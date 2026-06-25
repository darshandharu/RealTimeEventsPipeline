"""Data-quality validation layer.

Two complementary surfaces, one rule set (from ``config.validation``):

1. :class:`EventValidator` — validates a **Python dict** event. Used by the
   producer to reject bad records *before* Kafka, and reusable in unit tests.
   Returns a structured :class:`ValidationResult` rather than raising, so the
   caller decides whether to DLQ, drop or count it.

2. :func:`build_spark_validation_expr` — builds a **Spark SQL Column** that
   classifies each streamed row as valid/invalid with a precise
   ``error_reason``. Used inside the Structured Streaming job to split the
   stream into a clean path and a DLQ path *without* a Python UDF (keeping the
   work in the JVM for throughput).

Validation covers requirement #4 in full:
missing values, invalid timestamps, negative prices, invalid/out-of-range
values, corrupted JSON (NULL after ``from_json``) and wrong datatypes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config.config_loader import Config

# Spark imports are deferred to the function that needs them so this module is
# importable (for the producer / unit tests) without a Spark runtime present.


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of validating a single event.

    Attributes:
        is_valid: ``True`` if the event passed every rule.
        error_reason: A human-readable, machine-greppable failure reason
            (empty string when valid). Multiple failures are ``;``-joined.
    """

    is_valid: bool
    error_reason: str = ""


class EventValidator:
    """Validates dict-shaped events against the configured data-quality rules.

    Args:
        config: The loaded pipeline configuration.
    """

    def __init__(self, config: Config) -> None:
        self._cfg = config.validation
        self._rules = config.validation.rules
        self._required: List[str] = list(config.validation.required_fields)
        self._allowed_exchanges = set(self._rules.allowed_exchanges)

    # ------------------------------------------------------------------
    def validate(self, event: Optional[Dict[str, Any]]) -> ValidationResult:
        """Validate an event dict, collecting *all* failures.

        We accumulate every violation (rather than short-circuiting) so the DLQ
        record explains *everything* wrong with the payload — far more useful
        when triaging data-quality incidents.

        Args:
            event: The candidate event. ``None`` represents corrupted JSON that
                could not be parsed at all.

        Returns:
            A :class:`ValidationResult`.
        """
        if event is None:
            return ValidationResult(False, "corrupted_json: payload was not parseable")
        if not isinstance(event, dict):
            return ValidationResult(
                False, f"wrong_root_type: expected object, got {type(event).__name__}"
            )

        errors: List[str] = []
        errors.extend(self._check_required(event))
        errors.extend(self._check_types(event))
        errors.extend(self._check_business_rules(event))
        errors.extend(self._check_timestamp(event))
        errors.extend(self._check_exchange(event))

        if errors:
            return ValidationResult(False, "; ".join(errors))
        return ValidationResult(True, "")

    # ------------------------------------------------------------------
    # Individual rule groups
    # ------------------------------------------------------------------
    def _check_required(self, event: Dict[str, Any]) -> List[str]:
        """Check for missing or null required fields."""
        errors: List[str] = []
        for field in self._required:
            if field not in event:
                errors.append(f"missing_field: '{field}' is absent")
            elif event[field] is None:
                errors.append(f"null_value: '{field}' is null")
        return errors

    def _check_types(self, event: Dict[str, Any]) -> List[str]:
        """Check datatypes for the numeric fields that downstream maths needs."""
        errors: List[str] = []
        numeric_fields = ("price", "change", "change_percent")
        for field in numeric_fields:
            value = event.get(field)
            if value is not None and not _is_number(value):
                errors.append(
                    f"wrong_type: '{field}' must be numeric, got "
                    f"{type(value).__name__} ({value!r})"
                )
        volume = event.get("volume")
        if volume is not None and not _is_integer(volume):
            errors.append(
                f"wrong_type: 'volume' must be integer, got {type(volume).__name__}"
            )
        return errors

    def _check_business_rules(self, event: Dict[str, Any]) -> List[str]:
        """Apply numeric range / business-rule checks."""
        errors: List[str] = []
        price = event.get("price")
        if _is_number(price):
            if price <= self._rules.price_min:
                errors.append(f"invalid_price: {price} <= min {self._rules.price_min}")
            elif price > self._rules.price_max:
                errors.append(f"invalid_price: {price} > max {self._rules.price_max}")

        volume = event.get("volume")
        if _is_number(volume) and volume < self._rules.volume_min:
            errors.append(f"invalid_volume: {volume} < min {self._rules.volume_min}")

        change_pct = event.get("change_percent")
        if _is_number(change_pct):
            if not (
                self._rules.change_percent_min
                <= change_pct
                <= self._rules.change_percent_max
            ):
                errors.append(
                    f"invalid_change_percent: {change_pct} outside "
                    f"[{self._rules.change_percent_min}, "
                    f"{self._rules.change_percent_max}]"
                )
        return errors

    def _check_timestamp(self, event: Dict[str, Any]) -> List[str]:
        """Validate the event timestamp is parseable and not absurdly future."""
        raw = event.get("timestamp")
        if raw is None:
            return []  # already reported by required/null checks
        if not isinstance(raw, str):
            return [f"invalid_timestamp: must be ISO-8601 string, got {type(raw).__name__}"]
        parsed = _parse_iso(raw)
        if parsed is None:
            return [f"invalid_timestamp: '{raw}' is not valid ISO-8601"]

        skew = (parsed - datetime.now(timezone.utc)).total_seconds()
        if skew > self._rules.max_timestamp_skew_seconds:
            return [
                f"future_timestamp: '{raw}' is {int(skew)}s ahead of now "
                f"(max {self._rules.max_timestamp_skew_seconds}s)"
            ]
        return []

    def _check_exchange(self, event: Dict[str, Any]) -> List[str]:
        """Validate the exchange code against the allow-list."""
        exchange = event.get("exchange")
        if exchange is not None and exchange not in self._allowed_exchanges:
            return [f"invalid_exchange: '{exchange}' not in {sorted(self._allowed_exchanges)}"]
        return []


# ---------------------------------------------------------------------------
# Helpers (module-level so they are trivially unit-testable)
# ---------------------------------------------------------------------------
def _is_number(value: Any) -> bool:
    """Return True if ``value`` is a real number (not a bool, not a string)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_integer(value: Any) -> bool:
    """Return True if ``value`` is an integer (not a bool)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _parse_iso(raw: str) -> Optional[datetime]:
    """Parse an ISO-8601 string into a timezone-aware UTC datetime.

    Args:
        raw: Candidate timestamp string.

    Returns:
        A tz-aware :class:`datetime` in UTC, or ``None`` if unparseable.
    """
    try:
        # Support the trailing-Z form that fromisoformat rejected pre-3.11.
        normalised = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalised)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Spark-side validation expression (JVM-native, no Python UDF)
# ---------------------------------------------------------------------------
def build_spark_validation_expr(config: Config) -> "Any":
    """Build a Spark Column yielding the validation ``error_reason`` per row.

    The returned column is ``NULL`` for valid rows and a non-null reason string
    for invalid ones, letting the streaming job filter into clean vs. DLQ paths
    entirely in the JVM (no per-row Python serialization).

    Expects the parsed columns from ``from_json`` to be present (``event_id``,
    ``symbol``, ``price``, ``volume``, ``change_percent``, ``timestamp`` parsed
    to ``event_ts``, ``exchange``).

    Args:
        config: The loaded pipeline configuration.

    Returns:
        A :class:`pyspark.sql.Column` of type string (nullable).
    """
    from pyspark.sql import functions as F  # local import: Spark optional

    rules = config.validation.rules
    required = config.validation.required_fields
    allowed = config.validation.rules.allowed_exchanges

    # Build a chain of when() conditions; first match wins (most-fundamental
    # failure reported first), mirroring the dict validator's priority order.
    expr = F.lit(None).cast("string")

    # 1. corrupted JSON: every parsed field is null => parse failed entirely.
    all_null = F.col("event_id").isNull() & F.col("symbol").isNull() & F.col("price").isNull()
    expr = F.when(all_null, F.lit("corrupted_json: payload was not parseable")).otherwise(expr)

    # 2. missing/null required fields.
    for field in required:
        col = "event_ts" if field == "timestamp" else field
        expr = F.when(
            expr.isNull() & F.col(col).isNull(),
            F.concat(F.lit("missing_or_null: "), F.lit(field)),
        ).otherwise(expr)

    # 3. negative / out-of-range price.
    expr = F.when(
        expr.isNull() & (F.col("price") <= F.lit(rules.price_min)),
        F.lit("invalid_price: price <= min"),
    ).otherwise(expr)
    expr = F.when(
        expr.isNull() & (F.col("price") > F.lit(rules.price_max)),
        F.lit("invalid_price: price > max"),
    ).otherwise(expr)

    # 4. negative volume.
    expr = F.when(
        expr.isNull() & (F.col("volume") < F.lit(rules.volume_min)),
        F.lit("invalid_volume: volume < min"),
    ).otherwise(expr)

    # 5. change_percent out of range.
    expr = F.when(
        expr.isNull()
        & (
            (F.col("change_percent") < F.lit(rules.change_percent_min))
            | (F.col("change_percent") > F.lit(rules.change_percent_max))
        ),
        F.lit("invalid_change_percent: out of range"),
    ).otherwise(expr)

    # 6. invalid timestamp (string failed to cast to a timestamp).
    expr = F.when(
        expr.isNull() & F.col("timestamp").isNotNull() & F.col("event_ts").isNull(),
        F.lit("invalid_timestamp: not ISO-8601"),
    ).otherwise(expr)

    # 7. exchange not in allow-list.
    expr = F.when(
        expr.isNull()
        & F.col("exchange").isNotNull()
        & ~F.col("exchange").isin(list(allowed)),
        F.lit("invalid_exchange: not allowed"),
    ).otherwise(expr)

    return expr
