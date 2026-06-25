"""Dead Letter Queue (DLQ) implementation.

A robust DLQ is what separates a toy pipeline from a production one: when a
record cannot be processed, it must be *captured with context* instead of
crashing the stream or being silently dropped.

This module provides:

* :func:`build_dlq_record` — the single canonical builder for a DLQ envelope,
  guaranteeing every dead-lettered record carries the required contract
  (requirement #5)::

      {
        "original_message":     <the raw payload that failed>,
        "error_reason":         <why it failed>,
        "processing_timestamp": <when we dead-lettered it, UTC ISO-8601>,
        "pipeline_name":        <which pipeline produced it>
      }

* :class:`DeadLetterQueue` — a thin, resilient Kafka producer dedicated to the
  ``dead-letter-events`` topic. It is used producer-side (and by any Python
  component) to off-load bad records. The Spark job writes its own DLQ rows via
  a Kafka sink using the *same* :func:`build_dlq_record` envelope, so the schema
  is identical regardless of origin.

The DLQ writer is deliberately fail-soft: if even the DLQ send fails (e.g. the
broker is down), it logs loudly and persists the record to an on-disk fallback
file so nothing is lost.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from confluent_kafka import Producer

from config.config_loader import Config, load_config
from utils.logger import get_logger

_log = get_logger(__name__, component="pipeline")


def build_dlq_record(
    original_message: str,
    error_reason: str,
    pipeline_name: str,
    processing_timestamp: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a canonical DLQ record envelope.

    Args:
        original_message: The raw payload (string) that failed processing.
        error_reason: A precise, greppable reason for the failure.
        pipeline_name: The pipeline that dead-lettered the record.
        processing_timestamp: Optional explicit UTC ISO-8601 timestamp; defaults
            to *now* in UTC.

    Returns:
        A dict matching the DLQ contract (requirement #5).
    """
    return {
        "original_message": original_message,
        "error_reason": error_reason,
        "processing_timestamp": processing_timestamp
        or datetime.now(timezone.utc).isoformat(),
        "pipeline_name": pipeline_name,
    }


class DeadLetterQueue:
    """Resilient writer for the ``dead-letter-events`` Kafka topic.

    Args:
        config: Loaded pipeline configuration. Defaults to :func:`load_config`.

    Notes:
        A dedicated producer (separate from the main one) keeps DLQ traffic
        isolated: a backlog of bad records can never apply backpressure to the
        healthy main stream, and vice-versa.
    """

    def __init__(self, config: Optional[Config] = None) -> None:
        self._config = config or load_config()
        self._topic = self._config.kafka.topics.dead_letter
        self._pipeline_name = self._config.app.pipeline_name
        self._fallback_path = self._resolve_fallback_path()
        self._producer = self._build_producer()
        self._sent = 0
        self._fallback_count = 0

    # ------------------------------------------------------------------
    def _resolve_fallback_path(self) -> Path:
        """Resolve (and create) the on-disk DLQ fallback file path."""
        log_dir = self._config.resolve_path(self._config.logging.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir / "dlq_fallback.jsonl"

    def _build_producer(self) -> Optional[Producer]:
        """Create the dedicated DLQ Kafka producer.

        Returns:
            A configured :class:`confluent_kafka.Producer`, or ``None`` if the
            broker is unreachable (the DLQ then uses the file fallback only).
        """
        try:
            producer = Producer(
                {
                    "bootstrap.servers": self._config.kafka.bootstrap_servers,
                    "client.id": f"{self._config.kafka.client_id}-dlq",
                    "acks": "all",
                    "enable.idempotence": True,
                    "retries": 5,
                    "retry.backoff.ms": 500,
                    "compression.type": "snappy",
                }
            )
            producer.list_topics(timeout=10)
            _log.info("DLQ producer connected (topic=%s)", self._topic)
            return producer
        except Exception as exc:  # noqa: BLE001 - DLQ must never crash the caller
            _log.error(
                "DLQ producer could not connect (%s); using file fallback at %s",
                exc,
                self._fallback_path,
            )
            return None

    # ------------------------------------------------------------------
    def send(self, original_message: str, error_reason: str) -> None:
        """Dead-letter a single failed record.

        Always succeeds from the caller's perspective: if the Kafka send fails
        for any reason, the record is appended to the on-disk fallback so it is
        never lost.

        Args:
            original_message: The raw payload that failed.
            error_reason: Why it failed (precise, greppable).
        """
        record = build_dlq_record(
            original_message=original_message,
            error_reason=error_reason,
            pipeline_name=self._pipeline_name,
        )
        payload = json.dumps(record).encode("utf-8")

        if self._producer is not None:
            try:
                self._producer.produce(
                    topic=self._topic,
                    value=payload,
                    on_delivery=self._delivery_callback,
                )
                self._producer.poll(0)
                self._sent += 1
                return
            except Exception as exc:  # noqa: BLE001 - degrade to file fallback
                _log.error("DLQ Kafka send failed (%s); writing to fallback file", exc)

        self._write_fallback(record)

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Delivery report for DLQ messages; falls back to disk on failure."""
        if err is not None:
            _log.error("DLQ delivery failed: %s — writing to fallback file", err)
            try:
                record = json.loads(msg.value().decode("utf-8"))
            except Exception:  # noqa: BLE001
                record = {"original_message": "<undecodable>", "error_reason": str(err)}
            self._write_fallback(record)

    def _write_fallback(self, record: Dict[str, Any]) -> None:
        """Append a DLQ record to the on-disk fallback (JSON Lines).

        Args:
            record: The DLQ envelope to persist.
        """
        try:
            with self._fallback_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record) + "\n")
            self._fallback_count += 1
        except OSError as exc:  # pragma: no cover - disk failure is catastrophic
            _log.critical("Failed to write DLQ fallback file: %s", exc)

    # ------------------------------------------------------------------
    def flush(self, timeout: float = 10.0) -> None:
        """Flush any buffered DLQ messages.

        Args:
            timeout: Max seconds to wait for outstanding sends.
        """
        if self._producer is not None:
            self._producer.flush(timeout)

    def close(self) -> None:
        """Flush and report DLQ statistics on shutdown."""
        self.flush()
        _log.info(
            "DLQ closed | sent_to_kafka=%d written_to_fallback=%d",
            self._sent,
            self._fallback_count,
        )

    @property
    def stats(self) -> Dict[str, int]:
        """Return DLQ counters (sent to Kafka vs. written to fallback)."""
        return {"sent": self._sent, "fallback": self._fallback_count}
