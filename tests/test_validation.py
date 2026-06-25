"""Unit tests for the validation layer (requirement #14).

Covers every requirement-#4 failure mode against :class:`EventValidator`:
missing values, invalid timestamps, negative prices, wrong datatypes, invalid
exchanges, future timestamps and corrupted (None) payloads — plus the happy
path.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from validation.validator import EventValidator, _is_number, _parse_iso


@pytest.fixture
def validator(config) -> EventValidator:
    """An EventValidator bound to the project config."""
    return EventValidator(config)


def test_valid_event_passes(validator, valid_event):
    """A well-formed event passes with no error reason."""
    result = validator.validate(valid_event)
    assert result.is_valid is True
    assert result.error_reason == ""


def test_none_payload_is_corrupted_json(validator):
    """A None payload (unparseable JSON) is flagged as corrupted_json."""
    result = validator.validate(None)
    assert result.is_valid is False
    assert "corrupted_json" in result.error_reason


def test_negative_price_rejected(validator, make_event):
    """A negative price fails the business-rule check."""
    result = validator.validate(make_event(price=-1.0))
    assert result.is_valid is False
    assert "invalid_price" in result.error_reason


def test_zero_price_rejected(validator, make_event):
    """A zero price (<= price_min) is rejected."""
    result = validator.validate(make_event(price=0.0))
    assert result.is_valid is False
    assert "invalid_price" in result.error_reason


def test_missing_required_field_rejected(validator, make_event):
    """A missing required field is reported precisely."""
    event = make_event()
    event.pop("volume")
    result = validator.validate(event)
    assert result.is_valid is False
    assert "missing_field" in result.error_reason and "volume" in result.error_reason


def test_null_required_field_rejected(validator, make_event):
    """A null required field is reported as null_value."""
    result = validator.validate(make_event(symbol=None))
    assert result.is_valid is False
    assert "null_value" in result.error_reason


def test_wrong_type_price_rejected(validator, make_event):
    """A string in the numeric price field is a wrong_type failure."""
    result = validator.validate(make_event(price="expensive"))
    assert result.is_valid is False
    assert "wrong_type" in result.error_reason


def test_invalid_timestamp_rejected(validator, make_event):
    """An unparseable timestamp is rejected."""
    result = validator.validate(make_event(timestamp="2026-99-99T99:99:99"))
    assert result.is_valid is False
    assert "invalid_timestamp" in result.error_reason


def test_future_timestamp_rejected(validator, make_event):
    """A timestamp far in the future (beyond skew) is rejected."""
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    result = validator.validate(make_event(timestamp=future))
    assert result.is_valid is False
    assert "future_timestamp" in result.error_reason


def test_invalid_exchange_rejected(validator, make_event):
    """An exchange outside the allow-list is rejected."""
    result = validator.validate(make_event(exchange="MARS"))
    assert result.is_valid is False
    assert "invalid_exchange" in result.error_reason


def test_multiple_errors_are_accumulated(validator, make_event):
    """All violations are collected, not short-circuited."""
    event = make_event(price=-1.0, exchange="MARS")
    result = validator.validate(event)
    assert result.is_valid is False
    assert "invalid_price" in result.error_reason
    assert "invalid_exchange" in result.error_reason


@pytest.mark.parametrize(
    "value,expected",
    [(1, True), (1.5, True), (True, False), ("1", False), (None, False)],
)
def test_is_number_rejects_bool_and_str(value, expected):
    """_is_number treats bools/strings as non-numeric."""
    assert _is_number(value) is expected


def test_parse_iso_handles_z_suffix():
    """ISO parser accepts the trailing-Z UTC form."""
    parsed = _parse_iso("2026-06-25T12:00:00Z")
    assert parsed is not None
    assert parsed.tzinfo is not None


def test_parse_iso_returns_none_for_garbage():
    """ISO parser returns None for unparseable input."""
    assert _parse_iso("not-a-date") is None
