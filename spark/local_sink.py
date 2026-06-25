"""Offline sink writers — console and local Parquet.

These provide a drop-in alternative to :class:`~spark.bigquery_sink.BigQueryStreamWriter`
so the *entire* streaming pipeline (Kafka -> parse -> validate -> DLQ -> window
aggregate) can run end-to-end with **no GCP dependency**. Ideal for a local demo,
CI, or debugging the streaming logic before wiring up BigQuery.

Both writers expose the same interface as the BigQuery writer
(``raw_events_sink()`` / ``aggregates_sink()`` / ``dead_letter_sink()``), so
``streaming_job.py`` selects between them purely by ``config.sink.type`` without
any other code change (Strategy pattern).

* ``console`` — prints each micro-batch to stdout (truncated to N rows).
* ``parquet`` — appends each micro-batch to a partitioned Parquet dataset under
  ``config.sink.local_path`` (durable, queryable with pandas/DuckDB/Spark).
"""

from __future__ import annotations

from typing import Any, Callable

from config.config_loader import Config
from utils.logger import get_logger

_log = get_logger(__name__, component="pipeline")

# A foreachBatch sink callable: (batch_df, batch_id) -> None
SinkFn = Callable[[Any, int], None]


class NoOpTableManager:
    """Stand-in for BigQueryTableManager when no cloud tables are needed.

    Console/Parquet sinks create their targets implicitly, so the streaming job
    can call ``ensure_all_tables()`` uniformly regardless of sink type.
    """

    def __init__(self, config: Config) -> None:  # noqa: D107 - trivial
        self._sink_type = config.sink.type

    def ensure_all_tables(self) -> None:
        """No-op: offline sinks need no pre-provisioned tables."""
        _log.info(
            "Sink '%s' selected — skipping BigQuery table provisioning", self._sink_type
        )


class ConsoleStreamWriter:
    """Writes each micro-batch to stdout (offline demo / debugging).

    Args:
        config: The loaded pipeline configuration.
    """

    def __init__(self, config: Config) -> None:
        self._rows = config.sink.console_rows

    def _make_sink(self, label: str) -> SinkFn:
        """Build a foreachBatch function that prints the batch.

        Args:
            label: Human label for the stream (e.g. "raw_events").

        Returns:
            A ``(batch_df, batch_id)`` sink callable.
        """

        def _sink(batch_df: Any, batch_id: int) -> None:
            count = batch_df.count()
            if count == 0:
                _log.debug("[console:%s] batch %s empty", label, batch_id)
                return
            _log.info("[console:%s] batch %s — %d row(s)", label, batch_id, count)
            # show() prints a formatted table to the driver's stdout.
            batch_df.show(self._rows, truncate=False)

        return _sink

    def raw_events_sink(self) -> SinkFn:
        """Console sink for raw events."""
        return self._make_sink("raw_events")

    def aggregates_sink(self) -> SinkFn:
        """Console sink for 1-minute aggregates."""
        return self._make_sink("aggregates")

    def dead_letter_sink(self) -> SinkFn:
        """Console sink for DLQ records."""
        return self._make_sink("dead_letter")


class ParquetStreamWriter:
    """Appends each micro-batch to a local partitioned Parquet dataset.

    Args:
        config: The loaded pipeline configuration.
    """

    def __init__(self, config: Config) -> None:
        self._base = config.resolve_path(config.sink.local_path)
        self._base.mkdir(parents=True, exist_ok=True)
        _log.info("Parquet sink writing under %s", self._base)

    def _make_sink(self, subdir: str, partition_col: str) -> SinkFn:
        """Build a foreachBatch function that appends Parquet files.

        Args:
            subdir: Sub-directory (dataset name) under the base path.
            partition_col: Column to partition the Parquet output by.

        Returns:
            A ``(batch_df, batch_id)`` sink callable.
        """
        target = str(self._base / subdir)

        def _sink(batch_df: Any, batch_id: int) -> None:
            count = batch_df.count()
            if count == 0:
                return
            writer = batch_df.write.mode("append")
            # Partition by date when the column exists (mirrors BQ partitioning).
            if partition_col in batch_df.columns:
                writer = writer.partitionBy(partition_col)
            writer.parquet(target)
            _log.info("[parquet:%s] appended %d row(s) -> %s", subdir, count, target)

        return _sink

    def raw_events_sink(self) -> SinkFn:
        """Parquet sink for raw events (partitioned by event_date)."""
        return self._make_sink("events_raw", "event_date")

    def aggregates_sink(self) -> SinkFn:
        """Parquet sink for aggregates (partitioned by event_date)."""
        return self._make_sink("events_aggregates_1m", "event_date")

    def dead_letter_sink(self) -> SinkFn:
        """Parquet sink for DLQ records (partitioned by event_date)."""
        return self._make_sink("dead_letter_events", "event_date")


def build_sink(config: Config):
    """Factory: return the (table_manager, stream_writer) pair for the config.

    Selects the sink implementation from ``config.sink.type``:

    * ``bigquery`` -> :class:`~spark.bigquery_sink.BigQueryTableManager` +
      :class:`~spark.bigquery_sink.BigQueryStreamWriter`
    * ``console``  -> :class:`NoOpTableManager` + :class:`ConsoleStreamWriter`
    * ``parquet``  -> :class:`NoOpTableManager` + :class:`ParquetStreamWriter`

    Args:
        config: The loaded pipeline configuration.

    Returns:
        A ``(table_manager, stream_writer)`` tuple, both exposing the common
        interface used by ``streaming_job.py``.
    """
    sink_type = config.sink.type
    if sink_type == "bigquery":
        # Imported lazily so console/parquet runs never import google-cloud.
        from spark.bigquery_sink import BigQueryStreamWriter, BigQueryTableManager

        return BigQueryTableManager(config), BigQueryStreamWriter(config)
    if sink_type == "console":
        return NoOpTableManager(config), ConsoleStreamWriter(config)
    if sink_type == "parquet":
        return NoOpTableManager(config), ParquetStreamWriter(config)
    raise ValueError(f"Unsupported sink.type: {sink_type!r}")
