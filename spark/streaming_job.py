"""End-to-end PySpark Structured Streaming job (the consumer).

Implements the architecture's processing core::

    Kafka(events) -> parse(explicit schema) -> validate
        |-> valid  -> dedup -> enrich -> [raw events sink]  -> BigQuery
        |                         \\-> 1-min window aggregate -> BigQuery
        \\-> invalid -> DLQ envelope -> Kafka(dead-letter-events) + BigQuery

Key production properties:

* **Explicit schema** parsing (requirement #3 — never inferred).
* **Watermarking** + **1-minute tumbling windows** (requirements #3, #7).
* **Checkpointing** per query for fault-tolerant, exactly-once-ish recovery
  (requirements #3, #8, bonus #17).
* **Validation split** into a clean path and a DLQ path that never stops the
  stream (requirements #4, #5).
* **Streaming metrics listener** for observability (bonus #17).

Submit via ``spark-submit`` (see docker-compose) or run ``python -m
spark.streaming_job`` against a local master with the required packages.
"""

from __future__ import annotations

import sys
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from config.config_loader import Config, load_config
from dlq.dead_letter_queue import build_dlq_record  # noqa: F401  (schema parity ref)
from monitoring.metrics import StreamingMetricsListener
from spark.aggregations import aggregate_tumbling_window, deduplicate_events
from spark.bigquery_sink import BigQueryStreamWriter, BigQueryTableManager
from spark.transformations import (
    add_derived_columns,
    parse_kafka_events,
    select_raw_event_columns,
)
from utils.logger import get_logger
from validation.validator import build_spark_validation_expr

_log = get_logger("consumer", component="consumer")


class StreamingPipeline:
    """Orchestrates the full Kafka -> validate -> aggregate -> BigQuery stream.

    Args:
        config: The loaded pipeline configuration.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._spark = self._build_spark_session()
        self._bq_manager = BigQueryTableManager(config)
        self._bq_writer = BigQueryStreamWriter(config)
        self._queries: List[StreamingQuery] = []

    # ------------------------------------------------------------------
    # Spark session
    # ------------------------------------------------------------------
    def _build_spark_session(self) -> SparkSession:
        """Create the SparkSession with streaming + BigQuery configuration.

        Returns:
            A configured :class:`SparkSession`.
        """
        spark_cfg = self._config.spark
        builder = (
            SparkSession.builder.appName(spark_cfg.app_name)
            .master(spark_cfg.master)
            .config("spark.sql.shuffle.partitions", spark_cfg.shuffle_partitions)
            .config("spark.sql.streaming.schemaInference", "false")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            # BigQuery connector views/temp settings.
            .config("temporaryGcsBucket", self._config.bigquery.temporary_gcs_bucket)
        )
        # Credentials for the BigQuery connector (mounted in Docker).
        creds = self._config.resolve_path(self._config.bigquery.credentials_path)
        if creds.exists():
            builder = builder.config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                str(creds),
            )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(spark_cfg.log_level)
        _log.info(
            "SparkSession started (app=%s, master=%s)",
            spark_cfg.app_name,
            spark_cfg.master,
        )
        return spark

    # ------------------------------------------------------------------
    # Source
    # ------------------------------------------------------------------
    def _read_kafka_stream(self) -> DataFrame:
        """Create the streaming DataFrame reading from the ``events`` topic.

        Returns:
            The raw Kafka streaming DataFrame.
        """
        kc = self._config.kafka
        cc = kc.consumer
        reader = (
            self._spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kc.bootstrap_servers)
            .option("subscribe", kc.topics.events)
            .option("startingOffsets", self._config.spark.streaming.starting_offsets)
            .option("maxOffsetsPerTrigger", cc.max_offsets_per_trigger)
            .option("failOnDataLoss", str(cc.fail_on_data_loss).lower())
            .option("kafka.group.id", cc.group_id)
        )
        _log.info("Subscribed to Kafka topic '%s'", kc.topics.events)
        return reader.load()

    # ------------------------------------------------------------------
    # Pipeline graph
    # ------------------------------------------------------------------
    def _split_valid_invalid(self, parsed_df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Tag rows with a validation reason and split into clean / DLQ paths.

        Args:
            parsed_df: The parsed stream (output of :func:`parse_kafka_events`).

        Returns:
            A ``(valid_df, invalid_df)`` tuple. ``valid_df`` has the helper
            column removed; ``invalid_df`` carries ``raw_value`` + reason.
        """
        validation_expr = build_spark_validation_expr(self._config)
        tagged = parsed_df.withColumn("_error_reason", validation_expr)

        valid_df = tagged.filter(F.col("_error_reason").isNull()).drop("_error_reason")
        invalid_df = tagged.filter(F.col("_error_reason").isNotNull())
        return valid_df, invalid_df

    def _build_dlq_stream(self, invalid_df: DataFrame) -> DataFrame:
        """Shape invalid rows into the canonical DLQ envelope.

        Mirrors :func:`dlq.dead_letter_queue.build_dlq_record` so DLQ rows from
        Spark and from the producer are schema-identical.

        Args:
            invalid_df: Rows that failed validation (carry ``raw_value`` and
                ``_error_reason``).

        Returns:
            A DataFrame with the DLQ contract columns.
        """
        pipeline_name = self._config.app.pipeline_name
        return invalid_df.select(
            F.col("raw_value").alias("original_message"),
            F.col("_error_reason").alias("error_reason"),
            F.current_timestamp().alias("processing_timestamp"),
            F.lit(pipeline_name).alias("pipeline_name"),
            F.current_date().alias("event_date"),
        )

    # ------------------------------------------------------------------
    # Query construction / start
    # ------------------------------------------------------------------
    def _checkpoint_dir(self, name: str) -> str:
        """Return a per-query checkpoint path under the configured root.

        Args:
            name: A unique query name segment.

        Returns:
            An absolute checkpoint directory path string.
        """
        base = self._config.resolve_path(
            self._config.spark.streaming.checkpoint_location
        )
        return str(base / name)

    def start(self) -> None:
        """Build the streaming graph and start all queries."""
        # 1. Provision BigQuery dataset/tables up-front (idempotent).
        self._bq_manager.ensure_all_tables()

        # 2. Source -> parse (explicit schema) -> validate split.
        raw = self._read_kafka_stream()
        parsed = parse_kafka_events(raw)
        valid_df, invalid_df = self._split_valid_invalid(parsed)

        # 3. Clean path: dedup -> enrich.
        deduped = deduplicate_events(valid_df, self._config)
        enriched = add_derived_columns(deduped)

        # 4a. Raw events -> BigQuery (append).
        raw_events_df = select_raw_event_columns(enriched)
        self._queries.append(
            self._start_query(
                raw_events_df,
                self._bq_writer.raw_events_sink(),
                query_name="raw_events_to_bq",
                output_mode="append",
            )
        )

        # 4b. 1-minute tumbling aggregates -> BigQuery (append).
        aggregates_df = aggregate_tumbling_window(enriched, self._config)
        self._queries.append(
            self._start_query(
                aggregates_df,
                self._bq_writer.aggregates_sink(),
                query_name="aggregates_to_bq",
                output_mode="append",
            )
        )

        # 5. DLQ path -> Kafka(dead-letter-events) AND BigQuery dead_letter.
        dlq_df = self._build_dlq_stream(invalid_df)
        self._queries.append(self._start_dlq_to_kafka(dlq_df))
        self._queries.append(
            self._start_query(
                dlq_df,
                self._bq_writer.dead_letter_sink(),
                query_name="dlq_to_bq",
                output_mode="append",
            )
        )

        # 6. Attach the metrics listener (bonus: streaming metrics).
        self._spark.streams.addListener(
            StreamingMetricsListener(self._config)
        )

        _log.info("All %d streaming queries started", len(self._queries))
        self._await_termination()

    def _start_query(
        self,
        df: DataFrame,
        sink_fn,
        query_name: str,
        output_mode: str,
    ) -> StreamingQuery:
        """Start a foreachBatch streaming query with checkpointing.

        Args:
            df: The streaming DataFrame to sink.
            sink_fn: A ``foreachBatch`` callable ``(batch_df, batch_id)``.
            query_name: Unique query + checkpoint name.
            output_mode: ``append`` / ``update`` / ``complete``.

        Returns:
            The started :class:`StreamingQuery`.
        """
        trigger_interval = self._config.spark.streaming.trigger_interval
        query = (
            df.writeStream.queryName(query_name)
            .outputMode(output_mode)
            .foreachBatch(sink_fn)
            .option("checkpointLocation", self._checkpoint_dir(query_name))
            .trigger(processingTime=trigger_interval)
            .start()
        )
        _log.info("Started query '%s' (mode=%s)", query_name, output_mode)
        return query

    def _start_dlq_to_kafka(self, dlq_df: DataFrame) -> StreamingQuery:
        """Write DLQ envelopes back to the ``dead-letter-events`` Kafka topic.

        Args:
            dlq_df: The DLQ-shaped DataFrame.

        Returns:
            The started :class:`StreamingQuery`.
        """
        kafka_value = dlq_df.select(
            F.to_json(
                F.struct(
                    "original_message",
                    "error_reason",
                    "processing_timestamp",
                    "pipeline_name",
                )
            ).alias("value")
        )
        query = (
            kafka_value.writeStream.queryName("dlq_to_kafka")
            .format("kafka")
            .option("kafka.bootstrap.servers", self._config.kafka.bootstrap_servers)
            .option("topic", self._config.kafka.topics.dead_letter)
            .option("checkpointLocation", self._checkpoint_dir("dlq_to_kafka"))
            .trigger(processingTime=self._config.spark.streaming.trigger_interval)
            .start()
        )
        _log.info("Started DLQ->Kafka query (topic=%s)", self._config.kafka.topics.dead_letter)
        return query

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def _await_termination(self) -> None:
        """Block until any query terminates, then shut down gracefully."""
        try:
            self._spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            _log.info("Interrupt received — stopping streaming queries")
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop all queries and the Spark session gracefully."""
        for query in self._queries:
            try:
                if query.isActive:
                    query.stop()
                    _log.info("Stopped query '%s'", query.name)
            except Exception as exc:  # noqa: BLE001
                _log.warning("Error stopping query: %s", exc)
        try:
            self._spark.stop()
        except Exception:  # noqa: BLE001 - best-effort
            pass
        _log.info("Streaming pipeline shut down")


def main() -> int:
    """Entrypoint: build and start the streaming pipeline.

    Returns:
        Process exit code (0 = clean, 1 = fatal).
    """
    config = load_config()
    _log.info(
        "Launching streaming job | brokers=%s topic=%s -> %s.%s",
        config.kafka.bootstrap_servers,
        config.kafka.topics.events,
        config.bigquery.project_id,
        config.bigquery.dataset,
    )
    pipeline = StreamingPipeline(config)
    try:
        pipeline.start()
        return 0
    except Exception as exc:  # noqa: BLE001 - top-level fatal handler
        _log.exception("Fatal error in streaming job: %s", exc)
        pipeline.stop()
        return 1


if __name__ == "__main__":
    sys.exit(main())
