"""Resilient Kafka producer for the Real-Time Events Analytics Pipeline.

:class:`EventProducer` streams market events into the ``events`` topic with
production-grade guarantees:

* **Idempotent, ``acks=all`` delivery** (no duplicates, no silent loss).
* **Bounded in-flight + retries** configured from ``config.yaml``.
* **Per-record delivery callbacks** that update metrics and route permanent
  failures to the producer-side Dead Letter Queue.
* **Producer-side validation** so structurally invalid events are DLQ'd *before*
  they pollute the main topic.
* **Graceful shutdown** (SIGINT/SIGTERM) that flushes outstanding messages
  within a bounded timeout.
* **Kafka reconnect** handled by the underlying librdkafka client plus a
  retrying connection bootstrap.

The producer is deliberately decoupled from *how* events are generated: it takes
any :class:`~producer.data_source.DataSource`, so the same class works for stock
events or IoT sensors.
"""

from __future__ import annotations

import json
import signal
import time
from types import FrameType
from typing import Any, Dict, Optional

from confluent_kafka import KafkaError, KafkaException, Producer

from config.config_loader import Config, load_config
from dlq.dead_letter_queue import DeadLetterQueue
from producer.data_source import DataSource, MarketDataSource
from utils.logger import get_logger
from utils.retry import retry_with_backoff
from validation.validator import EventValidator

_log = get_logger(__name__, component="producer")


class EventProducer:
    """Streams events from a :class:`DataSource` to Kafka with resilience.

    Args:
        config: Loaded pipeline configuration. Defaults to :func:`load_config`.
        data_source: Event source. Defaults to a :class:`MarketDataSource`.
    """

    def __init__(
        self,
        config: Optional[Config] = None,
        data_source: Optional[DataSource] = None,
    ) -> None:
        self._config = config or load_config()
        self._data_source = data_source or MarketDataSource(self._config)
        self._validator = EventValidator(self._config)
        self._dlq = DeadLetterQueue(self._config)

        self._topic = self._config.kafka.topics.events
        self._shutdown_timeout = self._config.producer.graceful_shutdown_timeout_seconds
        self._running = False

        # Lightweight in-process metrics (also exported via monitoring layer).
        self._metrics = {"produced": 0, "delivered": 0, "failed": 0, "dlq": 0}

        self._producer = self._build_producer()
        self._install_signal_handlers()

    # ------------------------------------------------------------------
    # Construction / connection
    # ------------------------------------------------------------------
    @retry_with_backoff(
        max_attempts=5,
        base_seconds=2.0,
        max_seconds=30.0,
        exceptions=(KafkaException,),
    )
    def _build_producer(self) -> Producer:
        """Create and validate a confluent-kafka Producer.

        The constructor itself is cheap; we issue a metadata request inside the
        retry envelope so a broker that is still starting up triggers a backoff
        retry rather than a hard failure (Kafka reconnect on boot).

        Returns:
            A connected :class:`confluent_kafka.Producer`.

        Raises:
            KafkaException: If the broker cannot be reached after all retries.
        """
        pc = self._config.kafka.producer
        conf: Dict[str, Any] = {
            "bootstrap.servers": self._config.kafka.bootstrap_servers,
            "client.id": self._config.kafka.client_id,
            "acks": pc.acks,
            "enable.idempotence": pc.enable_idempotence,
            "retries": pc.retries,
            "retry.backoff.ms": pc.retry_backoff_ms,
            "linger.ms": pc.linger_ms,
            "batch.size": pc.batch_size,
            "compression.type": pc.compression_type,
            "max.in.flight.requests.per.connection": pc.max_in_flight_requests,
            "delivery.timeout.ms": pc.delivery_timeout_ms,
        }
        producer = Producer(conf)
        # Force a metadata fetch to verify connectivity (raises if unreachable).
        producer.list_topics(timeout=10)
        _log.info(
            "Kafka producer connected to %s (idempotence=%s, acks=%s)",
            self._config.kafka.bootstrap_servers,
            pc.enable_idempotence,
            pc.acks,
        )
        return producer

    def _install_signal_handlers(self) -> None:
        """Register SIGINT/SIGTERM handlers for graceful shutdown."""
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self._handle_signal)
            except ValueError:  # pragma: no cover - non-main thread (e.g. tests)
                _log.debug("Could not install handler for %s (non-main thread)", sig)

    def _handle_signal(self, signum: int, frame: Optional[FrameType]) -> None:
        """Signal handler that requests a graceful stop.

        Args:
            signum: The received signal number.
            frame: The current stack frame (unused).
        """
        _log.info("Received signal %s — initiating graceful shutdown", signum)
        self._running = False

    # ------------------------------------------------------------------
    # Delivery callback
    # ------------------------------------------------------------------
    def _delivery_callback(self, err: Optional[KafkaError], msg: Any) -> None:
        """librdkafka delivery report callback (success or permanent failure).

        Invoked from :meth:`poll`/:meth:`flush`. On permanent delivery failure
        the original payload is routed to the DLQ so nothing is silently lost.

        Args:
            err: A :class:`KafkaError` on failure, else ``None``.
            msg: The delivered/failed message handle.
        """
        if err is not None:
            self._metrics["failed"] += 1
            payload = msg.value().decode("utf-8") if msg.value() else ""
            _log.error("Delivery failed: %s", err)
            self._dlq.send(
                original_message=payload,
                error_reason=f"kafka_delivery_failed: {err}",
            )
            self._metrics["dlq"] += 1
        else:
            self._metrics["delivered"] += 1
            _log.debug(
                "Delivered to %s [partition %s] @ offset %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    # ------------------------------------------------------------------
    # Producing
    # ------------------------------------------------------------------
    def _produce_one(self, event: Dict[str, Any]) -> None:
        """Validate and produce a single event (or DLQ it on failure).

        Args:
            event: A candidate event dict from the data source.
        """
        # Producer-side validation: corrupted/invalid events never enter the
        # main topic — they go straight to the DLQ with a precise reason.
        result = self._validator.validate(event)
        if not result.is_valid:
            self._dlq.send(
                original_message=self._safe_dumps(event),
                error_reason=f"producer_validation_failed: {result.error_reason}",
            )
            self._metrics["dlq"] += 1
            return

        try:
            payload = json.dumps(event).encode("utf-8")
            self._producer.produce(
                topic=self._topic,
                key=str(event["symbol"]).encode("utf-8"),
                value=payload,
                on_delivery=self._delivery_callback,
            )
            self._metrics["produced"] += 1
            # Serve delivery callbacks without blocking the produce loop.
            self._producer.poll(0)
        except BufferError:
            # Local queue full: apply backpressure by flushing, then retry once.
            _log.warning("Producer local queue full — flushing to apply backpressure")
            self._producer.flush(5)
            self._producer.produce(
                topic=self._topic,
                key=str(event["symbol"]).encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                on_delivery=self._delivery_callback,
            )
        except (KafkaException, TypeError, ValueError) as exc:
            self._dlq.send(
                original_message=self._safe_dumps(event),
                error_reason=f"producer_serialize_or_send_error: {exc}",
            )
            self._metrics["dlq"] += 1
            _log.error("Failed to produce event %s: %s", event.get("event_id"), exc)

    @staticmethod
    def _safe_dumps(event: Dict[str, Any]) -> str:
        """JSON-encode an event defensively for DLQ storage.

        Args:
            event: The event dict (possibly containing non-serializable values).

        Returns:
            A JSON string, falling back to ``repr`` on encoding failure.
        """
        try:
            return json.dumps(event, default=str)
        except (TypeError, ValueError):
            return repr(event)

    # ------------------------------------------------------------------
    # Run loop
    # ------------------------------------------------------------------
    def run(self, max_ticks: Optional[int] = None) -> None:
        """Run the produce loop until stopped.

        Each tick generates a batch from the data source, produces it, then
        sleeps for the configured (jittered) interval. The loop exits on
        SIGINT/SIGTERM or after ``max_ticks`` iterations (used in tests).

        Args:
            max_ticks: Optional cap on the number of tick iterations. ``None``
                runs until a shutdown signal is received.
        """
        self._running = True
        tick = 0
        base = self._config.producer.event_interval_seconds
        jitter = self._config.producer.interval_jitter_seconds
        _log.info("Producer run loop started (topic=%s)", self._topic)

        try:
            while self._running:
                tick += 1
                batch = self._data_source.generate_events()
                for event in batch:
                    self._produce_one(event)

                if tick % 10 == 0:
                    self._log_metrics()

                if max_ticks is not None and tick >= max_ticks:
                    _log.info("Reached max_ticks=%s — stopping", max_ticks)
                    break

                self._sleep_interval(base, jitter)
        except Exception as exc:  # noqa: BLE001 - top-level safety net
            _log.exception("Unexpected error in producer run loop: %s", exc)
            raise
        finally:
            self.shutdown()

    def _sleep_interval(self, base: int, jitter: int) -> None:
        """Sleep for the configured base interval +/- jitter, interruptibly.

        Args:
            base: Base interval in seconds.
            jitter: Maximum +/- jitter in seconds.
        """
        import random

        delay = max(0.5, base + random.uniform(-jitter, jitter))
        # Sleep in small slices so a shutdown signal is honoured promptly.
        end = time.time() + delay
        while self._running and time.time() < end:
            time.sleep(min(0.25, end - time.time()))
            self._producer.poll(0)

    def _log_metrics(self) -> None:
        """Emit a periodic snapshot of producer metrics."""
        _log.info(
            "Producer metrics | produced=%d delivered=%d failed=%d dlq=%d",
            self._metrics["produced"],
            self._metrics["delivered"],
            self._metrics["failed"],
            self._metrics["dlq"],
        )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        """Flush outstanding messages and release resources gracefully."""
        if self._producer is None:
            return
        _log.info("Flushing producer (timeout=%ss)...", self._shutdown_timeout)
        remaining = self._producer.flush(self._shutdown_timeout)
        if remaining > 0:
            _log.warning("%d message(s) still undelivered after flush", remaining)
        self._dlq.close()
        self._log_metrics()
        _log.info("Producer shutdown complete")

    @property
    def metrics(self) -> Dict[str, int]:
        """Return a copy of the current in-process metrics."""
        return dict(self._metrics)
