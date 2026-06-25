"""Windowed aggregations for the streaming pipeline.

Implements requirement #7: **1-minute tumbling-window** aggregations per symbol,
computed with **event-time watermarking** so late-arriving events (up to the
configured delay) are still folded into the correct window, while state for
windows older than the watermark is dropped to bound memory.

Output metrics per (window, symbol, exchange):
    avg_price, max_price, min_price, total_volume, event_count, avg_change_percent

The function returns a DataFrame whose schema matches
:data:`~schemas.event_schema.AGGREGATE_TABLE_SCHEMA`, ready for the BigQuery
aggregates sink.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.config_loader import Config


def aggregate_tumbling_window(events_df: DataFrame, config: Config) -> DataFrame:
    """Compute 1-minute tumbling-window aggregates per symbol.

    The window duration / slide and the watermark delay are read from
    ``config.spark.streaming`` so the windowing policy is configuration-driven.
    Because ``window_slide == window_duration``, the windows are **tumbling**
    (non-overlapping).

    Args:
        events_df: A validated, enriched stream containing at least
            ``event_ts`` (timestamp), ``symbol``, ``exchange``, ``price``,
            ``volume`` and ``change_percent``.
        config: The loaded pipeline configuration.

    Returns:
        A streaming DataFrame matching ``AGGREGATE_TABLE_SCHEMA`` with one row
        per (window, symbol, exchange).
    """
    streaming_cfg = config.spark.streaming
    window_duration = streaming_cfg.window_duration   # e.g. "1 minute"
    window_slide = streaming_cfg.window_slide         # == duration -> tumbling
    watermark_delay = streaming_cfg.watermark_delay   # e.g. "2 minutes"

    aggregated = (
        events_df
        # Watermark on event time -> tolerate late data, bound state.
        .withWatermark("event_ts", watermark_delay)
        .groupBy(
            F.window(F.col("event_ts"), window_duration, window_slide),
            F.col("symbol"),
            F.col("exchange"),
        )
        .agg(
            F.round(F.avg("price"), 4).alias("avg_price"),
            F.round(F.max("price"), 4).alias("max_price"),
            F.round(F.min("price"), 4).alias("min_price"),
            F.sum("volume").cast("long").alias("total_volume"),
            F.count(F.lit(1)).cast("long").alias("event_count"),
            F.round(F.avg("change_percent"), 4).alias("avg_change_percent"),
        )
    )

    # Flatten the struct `window` column into explicit start/end + derive the
    # partition column and processing time, matching the BQ aggregate schema.
    return aggregated.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("symbol"),
        F.col("exchange"),
        F.col("avg_price"),
        F.col("max_price"),
        F.col("min_price"),
        F.col("total_volume"),
        F.col("event_count"),
        F.col("avg_change_percent"),
        F.to_date(F.col("window.start")).alias("event_date"),
        F.current_timestamp().alias("processing_time"),
    )


def deduplicate_events(events_df: DataFrame, config: Config) -> DataFrame:
    """Drop duplicate events within the watermark window (bonus: dedup).

    Uses ``dropDuplicatesWithinWatermark`` on ``event_id`` so a record replayed
    by an at-least-once upstream (or a producer retry) is counted exactly once,
    while keeping state bounded by the watermark.

    Args:
        events_df: A validated, enriched stream with ``event_id`` and
            ``event_ts``.
        config: The loaded pipeline configuration.

    Returns:
        The de-duplicated streaming DataFrame. If duplicate detection is
        disabled in config, the input is returned unchanged.
    """
    if not config.validation.enable_duplicate_detection:
        return events_df

    watermark_delay = config.spark.streaming.watermark_delay
    watermarked = events_df.withWatermark("event_ts", watermark_delay)

    # dropDuplicatesWithinWatermark (Spark 3.5+) avoids unbounded dedup state.
    try:
        return watermarked.dropDuplicatesWithinWatermark(["event_id"])
    except AttributeError:  # pragma: no cover - older Spark fallback
        return watermarked.dropDuplicates(["event_id"])
