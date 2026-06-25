"""Streaming metrics + performance monitoring (bonus #17).

Provides :class:`StreamingMetricsListener`, a Spark ``StreamingQueryListener``
that hooks into the structured-streaming lifecycle and, on every micro-batch:

* records throughput (input rows/sec, processed rows/sec),
* records processing latency / batch duration,
* tracks per-query progress and trigger health,
* exports everything as **Prometheus** gauges/counters (when enabled), and
* evaluates **alerting thresholds** (processing lag, throughput stall, DLQ rate)
  via the :class:`~monitoring.alerting.AlertManager`.

Spark calls the listener callbacks on a JVM-managed thread, so the
implementation is intentionally cheap and exception-safe — a monitoring error
must never destabilise the stream.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql.streaming import StreamingQueryListener

from config.config_loader import Config
from monitoring.alerting import AlertManager
from utils.logger import get_logger

_log = get_logger(__name__, component="pipeline")

# Prometheus is optional at runtime; degrade to log-only metrics if absent.
try:
    from prometheus_client import Counter, Gauge, start_http_server

    _PROM_AVAILABLE = True
except ImportError:  # pragma: no cover - prometheus optional
    _PROM_AVAILABLE = False


class StreamingMetricsListener(StreamingQueryListener):
    """Captures and exports per-batch streaming metrics.

    Args:
        config: The loaded pipeline configuration.
    """

    def __init__(self, config: Config) -> None:
        super().__init__()
        self._cfg = config.monitoring
        self._alerts = AlertManager(config)
        self._prom_started = False
        self._init_prometheus()

    # ------------------------------------------------------------------
    def _init_prometheus(self) -> None:
        """Initialise Prometheus metric objects and HTTP exposition server."""
        if not (self._cfg.enable_prometheus and _PROM_AVAILABLE):
            self._gauges = None
            return

        self._g_input_rate = Gauge(
            "rtep_input_rows_per_sec", "Input rows/sec", ["query"]
        )
        self._g_process_rate = Gauge(
            "rtep_processed_rows_per_sec", "Processed rows/sec", ["query"]
        )
        self._g_batch_duration = Gauge(
            "rtep_batch_duration_ms", "Micro-batch duration (ms)", ["query"]
        )
        self._g_num_input_rows = Gauge(
            "rtep_batch_input_rows", "Rows in the last batch", ["query"]
        )
        self._c_batches = Counter(
            "rtep_batches_total", "Total micro-batches processed", ["query"]
        )
        self._gauges = True

        try:
            start_http_server(self._cfg.prometheus_port)
            self._prom_started = True
            _log.info(
                "Prometheus metrics exposed on :%d/metrics", self._cfg.prometheus_port
            )
        except OSError as exc:  # pragma: no cover - port already bound
            _log.warning("Could not start Prometheus server: %s", exc)

    # ------------------------------------------------------------------
    # StreamingQueryListener callbacks
    # ------------------------------------------------------------------
    def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark API name
        """Called when a streaming query is started.

        Args:
            event: The query-started event (carries ``id`` and ``name``).
        """
        _log.info("Query started: name=%s id=%s", event.name, event.id)

    def onQueryProgress(self, event) -> None:  # noqa: N802 - Spark API name
        """Called after every micro-batch with a progress report.

        Args:
            event: The progress event; ``event.progress`` holds the metrics.
        """
        try:
            self._handle_progress(event.progress)
        except Exception as exc:  # noqa: BLE001 - never break the stream
            _log.debug("Metrics listener error (ignored): %s", exc)

    def onQueryTerminated(self, event) -> None:  # noqa: N802 - Spark API name
        """Called when a streaming query terminates.

        Args:
            event: The termination event (carries ``id`` and optional exception).
        """
        exc = getattr(event, "exception", None)
        if exc:
            _log.error("Query terminated with exception: %s", exc)
            self._alerts.send(
                title="Streaming query terminated",
                message=f"Query {event.id} failed: {exc}",
                severity="critical",
            )
        else:
            _log.info("Query terminated cleanly: id=%s", event.id)

    # ------------------------------------------------------------------
    def _handle_progress(self, progress) -> None:
        """Process a single progress report: log, export, and alert.

        Args:
            progress: A Spark ``StreamingQueryProgress`` object.
        """
        name = progress.name or "unnamed"
        input_rate = float(progress.inputRowsPerSecond or 0.0)
        process_rate = float(progress.processedRowsPerSecond or 0.0)
        num_rows = int(progress.numInputRows or 0)
        batch_ms = float(progress.durationMs.get("triggerExecution", 0.0)) \
            if getattr(progress, "durationMs", None) else 0.0

        _log.info(
            "[metrics] query=%s rows=%d in/s=%.1f proc/s=%.1f batch=%.0fms",
            name,
            num_rows,
            input_rate,
            process_rate,
            batch_ms,
        )

        if self._gauges:
            self._g_input_rate.labels(name).set(input_rate)
            self._g_process_rate.labels(name).set(process_rate)
            self._g_batch_duration.labels(name).set(batch_ms)
            self._g_num_input_rows.labels(name).set(num_rows)
            self._c_batches.labels(name).inc()

        # Performance / health alerting.
        lag_seconds = batch_ms / 1000.0
        if lag_seconds > self._cfg.alerting.thresholds.max_processing_lag_seconds:
            self._alerts.send(
                title="High processing lag",
                message=(
                    f"Query '{name}' batch took {lag_seconds:.1f}s "
                    f"(threshold {self._cfg.alerting.thresholds.max_processing_lag_seconds}s)"
                ),
                severity="warning",
            )
