"""Streaming transformations: parse, enrich and derive columns.

This module owns the *clean-path* shaping of the stream after validation:

1. :func:`parse_kafka_events` — decode the Kafka ``value`` bytes and apply the
   **explicit** :data:`~schemas.event_schema.STOCK_EVENT_SCHEMA` via
   ``from_json`` (requirement #3 — schema is never inferred), then cast the
   string timestamp into a real ``event_ts`` for watermarking.

2. :func:`add_derived_columns` — add the requirement-#6 derived columns:
   ``event_date``, ``event_hour``, ``event_day``, ``event_month``,
   ``event_year``, ``processing_time`` and a bucketed ``price_bucket``.

All transformations are pure Spark Column expressions (no Python UDFs) so they
execute entirely in the JVM and serialize cleanly to executors.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from schemas.event_schema import STOCK_EVENT_SCHEMA

# Price buckets for categorical analysis in Power BI (requirement #6).
# Boundaries are inclusive-low / exclusive-high.
_PRICE_BUCKETS = [
    (0.0, 50.0, "0-50"),
    (50.0, 100.0, "50-100"),
    (100.0, 250.0, "100-250"),
    (250.0, 500.0, "250-500"),
    (500.0, 1000.0, "500-1000"),
]


def parse_kafka_events(kafka_df: DataFrame) -> DataFrame:
    """Decode and parse raw Kafka rows into typed event columns.

    Applies the explicit Spark schema (no inference). Kafka metadata columns
    (``topic``, ``partition``, ``offset``, ``timestamp``) are preserved as
    ``kafka_*`` for offset/lineage debugging and exactly-once reasoning.

    Args:
        kafka_df: The raw streaming DataFrame from the ``kafka`` source, with
            the standard binary ``key`` / ``value`` columns.

    Returns:
        A DataFrame with the parsed event fields plus a true-typed ``event_ts``
        (timestamp) column derived from the ISO-8601 ``timestamp`` string, and
        the raw JSON retained as ``raw_value`` for DLQ purposes.
    """
    # Retain the raw payload string so invalid rows can be dead-lettered intact.
    decoded = kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    # Explicit-schema JSON parse. Malformed JSON yields an all-null struct,
    # which the validation layer classifies as `corrupted_json`.
    parsed = decoded.withColumn(
        "data", F.from_json(F.col("raw_value"), STOCK_EVENT_SCHEMA)
    )

    # Flatten the struct to top-level columns.
    flattened = parsed.select(
        "raw_value",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.col("data.event_id").alias("event_id"),
        F.col("data.symbol").alias("symbol"),
        F.col("data.company").alias("company"),
        F.col("data.price").alias("price"),
        F.col("data.volume").alias("volume"),
        F.col("data.change").alias("change"),
        F.col("data.change_percent").alias("change_percent"),
        F.col("data.timestamp").alias("timestamp"),
        F.col("data.exchange").alias("exchange"),
    )

    # Cast the ISO-8601 string to a real timestamp for watermarking. An
    # unparseable string becomes NULL -> flagged as `invalid_timestamp`.
    return flattened.withColumn(
        "event_ts", F.to_timestamp(F.col("timestamp"))
    )


def _price_bucket_expr() -> "F.Column":
    """Build the CASE-WHEN expression mapping price to a categorical bucket.

    Returns:
        A Spark Column producing the bucket label (or ``"unknown"``).
    """
    expr = F.lit("unknown")
    # Build from the top bucket down so the chained otherwise() reads cleanly.
    for low, high, label in reversed(_PRICE_BUCKETS):
        expr = F.when(
            (F.col("price") >= F.lit(low)) & (F.col("price") < F.lit(high)),
            F.lit(label),
        ).otherwise(expr)
    # Anything at/above the top boundary.
    top_low = _PRICE_BUCKETS[-1][1]
    return F.when(F.col("price") >= F.lit(top_low), F.lit("1000+")).otherwise(expr)


def add_derived_columns(events_df: DataFrame) -> DataFrame:
    """Add the requirement-#6 derived/enrichment columns to clean events.

    Args:
        events_df: A DataFrame of validated events containing at least
            ``event_ts`` and ``price``.

    Returns:
        The DataFrame with added columns: ``event_date``, ``event_hour``,
        ``event_day``, ``event_month``, ``event_year``, ``processing_time`` and
        ``price_bucket``.
    """
    return (
        events_df
        .withColumn("event_date", F.to_date(F.col("event_ts")))
        .withColumn("event_hour", F.hour(F.col("event_ts")))
        .withColumn("event_day", F.dayofmonth(F.col("event_ts")))
        .withColumn("event_month", F.month(F.col("event_ts")))
        .withColumn("event_year", F.year(F.col("event_ts")))
        .withColumn("price_bucket", _price_bucket_expr())
        # Wall-clock time the record was processed (lineage / latency analysis).
        .withColumn("processing_time", F.current_timestamp())
    )


def select_raw_event_columns(enriched_df: DataFrame) -> DataFrame:
    """Project the enriched stream to the BigQuery raw-events table columns.

    Aligns the column set + names exactly with
    :data:`~schemas.event_schema.RAW_EVENT_TABLE_SCHEMA` so the sink is a
    straight append with no surprises.

    Args:
        enriched_df: The output of :func:`add_derived_columns`.

    Returns:
        A DataFrame whose schema matches the raw-events BigQuery table.
    """
    return enriched_df.select(
        "event_id",
        "symbol",
        "company",
        "price",
        "volume",
        "change",
        "change_percent",
        F.col("event_ts").alias("event_timestamp"),
        "exchange",
        "event_date",
        "event_hour",
        "event_day",
        "event_month",
        "event_year",
        "price_bucket",
        "processing_time",
    )
