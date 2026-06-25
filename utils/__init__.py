"""Shared utilities for the Real-Time Events Analytics Pipeline.

Exposes the logging factory and retry helpers used across producer, Spark,
validation, DLQ and monitoring components.
"""

from utils.logger import get_logger
from utils.retry import RetryError, retry_with_backoff

__all__ = ["get_logger", "retry_with_backoff", "RetryError"]
