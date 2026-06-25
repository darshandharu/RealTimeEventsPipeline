"""Observability package: metrics, alerting and data-quality reporting.

Bonus features (#17): streaming metrics, performance monitoring, alerting
framework and a data-quality report generator.
"""

from monitoring.alerting import AlertManager
from monitoring.metrics import StreamingMetricsListener

__all__ = ["StreamingMetricsListener", "AlertManager"]
