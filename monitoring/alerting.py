"""Alerting framework (bonus #17).

Provides :class:`AlertManager`, a small, dependency-light alert dispatcher used
across the pipeline (metrics listener, DLQ-rate checks, fatal errors). It
supports pluggable delivery channels:

* ``log``     - structured log line at the mapped severity (always available).
* ``webhook`` - POST a Slack/Teams-compatible JSON payload to a configured
  incoming webhook URL.

Design goals:

* **Never throw** — alerting must not take down the component that is reporting
  a problem. All delivery is best-effort and failures are logged, not raised.
* **Rate-limited** — identical alerts are de-duplicated within a cooldown window
  so a sustained fault does not produce an alert storm.
* **Severity-aware** — ``info`` / ``warning`` / ``critical`` map to log levels
  and to Slack attachment colours.
"""

from __future__ import annotations

import json
import time
from typing import Dict, Optional

from config.config_loader import Config
from utils.logger import get_logger

_log = get_logger(__name__, component="pipeline")

# Severity -> (logging method name, Slack attachment colour).
_SEVERITY_MAP: Dict[str, tuple[str, str]] = {
    "info": ("info", "#36a64f"),       # green
    "warning": ("warning", "#ff9900"), # orange
    "critical": ("error", "#d00000"),  # red
}

#: Suppress duplicate alerts (same title) within this many seconds.
_DEFAULT_COOLDOWN_SECONDS = 60.0


class AlertManager:
    """Dispatches pipeline alerts to the configured channel.

    Args:
        config: The loaded pipeline configuration.
        cooldown_seconds: De-duplication window for identical alert titles.
    """

    def __init__(
        self, config: Config, cooldown_seconds: float = _DEFAULT_COOLDOWN_SECONDS
    ) -> None:
        alerting = config.monitoring.alerting
        self._enabled = alerting.enabled
        self._channel = alerting.channel
        self._webhook_url = alerting.webhook_url or None
        self._pipeline_name = config.app.pipeline_name
        self._environment = config.app.environment
        self._cooldown = cooldown_seconds
        self._last_sent: Dict[str, float] = {}

    # ------------------------------------------------------------------
    def send(
        self,
        title: str,
        message: str,
        severity: str = "warning",
    ) -> None:
        """Send an alert via the configured channel (best-effort, never raises).

        Args:
            title: Short alert title (also the de-duplication key).
            message: Human-readable detail.
            severity: One of ``info`` / ``warning`` / ``critical``.
        """
        if not self._enabled:
            return
        if self._is_rate_limited(title):
            return

        severity = severity if severity in _SEVERITY_MAP else "warning"
        log_method, _ = _SEVERITY_MAP[severity]
        getattr(_log, log_method)("ALERT [%s] %s — %s", severity.upper(), title, message)

        if self._channel == "webhook" and self._webhook_url:
            self._send_webhook(title, message, severity)

        self._last_sent[title] = time.monotonic()

    # ------------------------------------------------------------------
    def _is_rate_limited(self, title: str) -> bool:
        """Return True if an identical alert was sent within the cooldown.

        Args:
            title: The alert title used as the de-duplication key.

        Returns:
            ``True`` if the alert should be suppressed.
        """
        last = self._last_sent.get(title)
        if last is None:
            return False
        return (time.monotonic() - last) < self._cooldown

    def _send_webhook(self, title: str, message: str, severity: str) -> None:
        """POST a Slack/Teams-compatible payload to the configured webhook.

        Args:
            title: Alert title.
            message: Alert detail.
            severity: Severity level (maps to attachment colour).
        """
        import urllib.error
        import urllib.request

        _, colour = _SEVERITY_MAP[severity]
        payload = {
            "attachments": [
                {
                    "color": colour,
                    "title": f"[{self._environment}] {title}",
                    "text": message,
                    "footer": f"pipeline: {self._pipeline_name}",
                    "ts": int(time.time()),
                }
            ]
        }
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self._webhook_url,  # type: ignore[arg-type]
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as resp:
                if resp.status >= 300:
                    _log.warning("Webhook returned HTTP %s", resp.status)
        except (urllib.error.URLError, OSError) as exc:  # never raise
            _log.warning("Failed to deliver webhook alert: %s", exc)

    # ------------------------------------------------------------------
    def check_dlq_rate(self, processed: int, dead_lettered: int) -> None:
        """Evaluate the DLQ rate against the configured threshold and alert.

        Args:
            processed: Total records processed (valid + invalid).
            dead_lettered: Number of records sent to the DLQ.
        """
        if processed <= 0:
            return
        rate_pct = (dead_lettered / processed) * 100.0
        # Threshold lives on the config; read lazily to avoid storing it twice.
        # (AlertManager already holds enough context for the message.)
        if rate_pct >= self._dlq_threshold():
            self.send(
                title="High DLQ rate",
                message=(
                    f"{dead_lettered}/{processed} records dead-lettered "
                    f"({rate_pct:.1f}%) — exceeds threshold"
                ),
                severity="critical",
            )

    def _dlq_threshold(self) -> float:
        """Return the configured DLQ-rate alert threshold (percent)."""
        # Stored at construction would couple us to config internals; instead we
        # re-resolve from the cached singleton so tests can monkeypatch cleanly.
        from config.config_loader import load_config

        return load_config().monitoring.alerting.thresholds.dlq_rate_pct
