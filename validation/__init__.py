"""Validation layer for the Real-Time Events Analytics Pipeline.

Exposes the reusable :class:`EventValidator` (used producer-side on Python
dicts) and the Spark-side validation expression builder (used inside the
Structured Streaming job).
"""

from validation.validator import (
    EventValidator,
    ValidationResult,
    build_spark_validation_expr,
)

__all__ = ["EventValidator", "ValidationResult", "build_spark_validation_expr"]
