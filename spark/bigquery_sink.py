"""BigQuery sink: table lifecycle management + streaming writers.

Requirement #8 in full:

* **Auto-create** the dataset and tables if they do not exist
  (``CREATE_IF_NEEDED``), with explicit schemas (never inferred).
* **Append mode** (``WRITE_APPEND``) for the streaming inserts.
* **DATE partitioning** on ``event_date`` and **clustering** on
  ``symbol``/``exchange`` for cheap, fast analytical queries in Power BI.
* **Checkpointed** writes (the checkpoint location is configured on the query in
  ``streaming_job.py``).

Two layers:

1. :class:`BigQueryTableManager` — uses the ``google-cloud-bigquery`` client to
   ensure the dataset + partitioned/clustered tables exist *before* the stream
   starts. Idempotent and safe to call on every launch.

2. :class:`BigQueryStreamWriter` — builds ``foreachBatch`` sink functions that
   write each micro-batch via the Spark BigQuery connector. ``foreachBatch`` is
   used (rather than a direct ``format("bigquery")`` streaming sink) so we get
   per-batch control: we can write to a partition-aware table and log batch
   metrics for monitoring.
"""

from __future__ import annotations

from typing import Any, Dict, List

from config.config_loader import Config
from schemas.event_schema import (
    AGGREGATE_TABLE_SCHEMA,
    DLQ_TABLE_SCHEMA,
    RAW_EVENT_TABLE_SCHEMA,
)
from utils.logger import get_logger

_log = get_logger(__name__, component="pipeline")


# ===========================================================================
# Table lifecycle management (google-cloud-bigquery client)
# ===========================================================================
class BigQueryTableManager:
    """Ensures the BigQuery dataset and target tables exist.

    Args:
        config: The loaded pipeline configuration.
    """

    def __init__(self, config: Config) -> None:
        self._cfg = config.bigquery
        self._project = self._cfg.project_id
        self._dataset = self._cfg.dataset
        self._location = self._cfg.location
        self._client = self._build_client(config)

    def _build_client(self, config: Config) -> Any:
        """Construct an authenticated BigQuery client from the service account.

        Args:
            config: The loaded pipeline configuration.

        Returns:
            A ``google.cloud.bigquery.Client``.
        """
        from google.cloud import bigquery
        from google.oauth2 import service_account

        creds_path = config.resolve_path(self._cfg.credentials_path)
        if creds_path.exists():
            credentials = service_account.Credentials.from_service_account_file(
                str(creds_path)
            )
            client = bigquery.Client(
                project=self._project,
                credentials=credentials,
                location=self._location,
            )
            _log.info("BigQuery client authenticated via %s", creds_path.name)
        else:
            # Fall back to ADC (e.g. GOOGLE_APPLICATION_CREDENTIALS / metadata).
            client = bigquery.Client(project=self._project, location=self._location)
            _log.warning(
                "Service-account key not found at %s; using application-default "
                "credentials",
                creds_path,
            )
        return client

    # ------------------------------------------------------------------
    def ensure_dataset(self) -> None:
        """Create the dataset if it does not already exist (idempotent)."""
        from google.cloud import bigquery
        from google.api_core.exceptions import Conflict

        dataset_id = f"{self._project}.{self._dataset}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self._location
        try:
            self._client.create_dataset(dataset, exists_ok=True)
            _log.info("Ensured dataset %s (location=%s)", dataset_id, self._location)
        except Conflict:  # pragma: no cover - race with another launcher
            _log.debug("Dataset %s already exists", dataset_id)

    def ensure_all_tables(self) -> None:
        """Create the raw, aggregate and DLQ tables (partitioned + clustered)."""
        self.ensure_dataset()
        self._ensure_table(
            self._cfg.tables.raw_events,
            RAW_EVENT_TABLE_SCHEMA,
            clustering=self._cfg.clustering_fields,
        )
        self._ensure_table(
            self._cfg.tables.aggregates,
            AGGREGATE_TABLE_SCHEMA,
            clustering=["symbol", "exchange"],
        )
        self._ensure_table(
            self._cfg.tables.dead_letter,
            DLQ_TABLE_SCHEMA,
            clustering=["pipeline_name"],
        )

    def _ensure_table(
        self,
        table_name: str,
        schema_def: List[Dict[str, str]],
        clustering: List[str],
    ) -> None:
        """Create a single partitioned + clustered table if missing.

        Args:
            table_name: Bare table name (dataset/project are prefixed here).
            schema_def: List-of-dict schema (see ``schemas.event_schema``).
            clustering: Columns to cluster by.
        """
        from google.cloud import bigquery

        table_id = f"{self._project}.{self._dataset}.{table_name}"
        schema = [
            bigquery.SchemaField(f["name"], f["type"], mode=f.get("mode", "NULLABLE"))
            for f in schema_def
        ]
        table = bigquery.Table(table_id, schema=schema)

        # DATE partitioning on the configured partition field.
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=self._cfg.partition_field,
        )
        # Clustering (only fields actually present in this table's schema).
        present = {f["name"] for f in schema_def}
        table.clustering_fields = [c for c in clustering if c in present] or None

        created = self._client.create_table(table, exists_ok=True)
        _log.info(
            "Ensured table %s (partition=%s, cluster=%s)",
            table_id,
            self._cfg.partition_field,
            created.clustering_fields,
        )


# ===========================================================================
# Streaming writers (Spark BigQuery connector via foreachBatch)
# ===========================================================================
class BigQueryStreamWriter:
    """Builds ``foreachBatch`` writer functions for the BigQuery connector.

    Args:
        config: The loaded pipeline configuration.
    """

    def __init__(self, config: Config) -> None:
        self._cfg = config.bigquery
        self._dataset = self._cfg.dataset
        self._project = self._cfg.project_id

    def _table_path(self, table_name: str) -> str:
        """Return the fully-qualified ``project.dataset.table`` path."""
        return f"{self._project}.{self._dataset}.{table_name}"

    def _write_batch(self, batch_df: "Any", table_name: str, label: str) -> None:
        """Write one micro-batch DataFrame to a BigQuery table (append).

        Args:
            batch_df: The micro-batch DataFrame (bounded, not streaming).
            table_name: Target bare table name.
            label: Human label for logging (e.g. "raw_events").
        """
        count = batch_df.count()
        if count == 0:
            _log.debug("[%s] empty micro-batch — skipping write", label)
            return

        (
            batch_df.write.format("bigquery")
            .option("table", self._table_path(table_name))
            .option("temporaryGcsBucket", self._cfg.temporary_gcs_bucket)
            .option("writeMethod", self._cfg.write_method)
            .option("createDisposition", self._cfg.create_disposition)
            .option("partitionField", self._cfg.partition_field)
            .option("partitionType", self._cfg.partition_type)
            .option("clusteredFields", ",".join(self._cfg.clustering_fields))
            .mode("append")
            .save()
        )
        _log.info("[%s] wrote %d row(s) -> %s", label, count, table_name)

    # -- public foreachBatch factories --------------------------------------
    def raw_events_sink(self):
        """Return a ``foreachBatch`` function for the raw-events table."""

        def _sink(batch_df: "Any", batch_id: int) -> None:
            _log.debug("raw_events batch %s starting", batch_id)
            self._write_batch(batch_df, self._cfg.tables.raw_events, "raw_events")

        return _sink

    def aggregates_sink(self):
        """Return a ``foreachBatch`` function for the aggregates table."""

        def _sink(batch_df: "Any", batch_id: int) -> None:
            _log.debug("aggregates batch %s starting", batch_id)
            self._write_batch(batch_df, self._cfg.tables.aggregates, "aggregates")

        return _sink

    def dead_letter_sink(self):
        """Return a ``foreachBatch`` function for the dead-letter table."""

        def _sink(batch_df: "Any", batch_id: int) -> None:
            _log.debug("dead_letter batch %s starting", batch_id)
            self._write_batch(batch_df, self._cfg.tables.dead_letter, "dead_letter")

        return _sink
