"""Unit tests for the producer tier (requirement #14).

Validates the data-source generation, fault injection, and the producer's
validation/DLQ routing — all without a live Kafka broker by mocking the
confluent-kafka Producer and the DLQ.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from producer.data_source import MarketDataSource


# ---------------------------------------------------------------------------
# Data source
# ---------------------------------------------------------------------------
def test_data_source_generates_events_for_all_symbols(config):
    """The data source emits at least one event per configured symbol."""
    # Force mock mode so the test never hits the network.
    src = MarketDataSource(config)
    src._source_mode = "mock"  # noqa: SLF001 - deterministic offline test
    src.inject_bad_pct = 0
    events = src.generate_events()
    symbols = {e["symbol"] for e in events}
    assert symbols == {s.symbol for s in config.source.symbols}


def test_mock_price_is_always_positive(config):
    """The mock random walk never yields a non-positive price."""
    src = MarketDataSource(config)
    src._source_mode = "mock"  # noqa: SLF001
    for _ in range(500):
        price = src._mock_price("AAPL")  # noqa: SLF001
        assert price > 0


def test_fault_injection_produces_invalid_records(config):
    """With 100% injection, every record is corrupted in some way."""
    src = MarketDataSource(config)
    src._source_mode = "mock"  # noqa: SLF001
    src.inject_bad_pct = 100
    events = src.generate_events()

    from validation.validator import EventValidator

    validator = EventValidator(config)
    # Every event should now fail validation (each corruption strategy maps to
    # a distinct failure mode).
    assert all(not validator.validate(e).is_valid for e in events)


def test_zero_injection_produces_only_valid_records(config):
    """With 0% injection, all generated records pass validation."""
    src = MarketDataSource(config)
    src._source_mode = "mock"  # noqa: SLF001
    src.inject_bad_pct = 0

    from validation.validator import EventValidator

    validator = EventValidator(config)
    events = src.generate_events()
    assert all(validator.validate(e).is_valid for e in events)


# ---------------------------------------------------------------------------
# Producer routing (with mocked Kafka + DLQ)
# ---------------------------------------------------------------------------
class _FakeProducer:
    """Minimal stand-in for confluent_kafka.Producer."""

    def __init__(self) -> None:
        self.produced: List[Dict[str, Any]] = []

    def produce(self, topic, key, value, on_delivery=None):  # noqa: D401, ANN001
        self.produced.append({"topic": topic, "key": key, "value": value})
        if on_delivery is not None:
            on_delivery(None, _FakeMsg(value))

    def poll(self, timeout):  # noqa: ANN001
        return 0

    def flush(self, timeout=None):  # noqa: ANN001
        return 0

    def list_topics(self, timeout=None):  # noqa: ANN001
        return None


class _FakeMsg:
    """Minimal Kafka message stub for delivery callbacks."""

    def __init__(self, value: bytes) -> None:
        self._value = value

    def value(self):  # noqa: D401
        return self._value

    def topic(self):  # noqa: D401
        return "events"

    def partition(self):  # noqa: D401
        return 0

    def offset(self):  # noqa: D401
        return 1


class _FakeDLQ:
    """Captures DLQ sends for assertions."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, str]] = []

    def send(self, original_message: str, error_reason: str) -> None:
        self.sent.append({"msg": original_message, "reason": error_reason})

    def close(self) -> None:
        pass


@pytest.fixture
def producer_with_mocks(config, monkeypatch):
    """An EventProducer with Kafka + DLQ + validator boundaries mocked."""
    from producer import producer as producer_mod

    monkeypatch.setattr(producer_mod, "Producer", lambda conf: _FakeProducer())

    # Avoid touching the real DLQ (Kafka) — inject a fake post-construction.
    prod = producer_mod.EventProducer.__new__(producer_mod.EventProducer)
    prod._config = config  # noqa: SLF001
    prod._topic = config.kafka.topics.events  # noqa: SLF001
    prod._shutdown_timeout = 1  # noqa: SLF001
    prod._running = False  # noqa: SLF001
    prod._metrics = {"produced": 0, "delivered": 0, "failed": 0, "dlq": 0}  # noqa: SLF001
    from validation.validator import EventValidator

    prod._validator = EventValidator(config)  # noqa: SLF001
    prod._dlq = _FakeDLQ()  # noqa: SLF001
    prod._producer = _FakeProducer()  # noqa: SLF001
    return prod


def test_valid_event_is_produced(producer_with_mocks, valid_event):
    """A valid event is produced to Kafka, not dead-lettered."""
    producer_with_mocks._produce_one(valid_event)  # noqa: SLF001
    assert producer_with_mocks._metrics["produced"] == 1  # noqa: SLF001
    assert producer_with_mocks._dlq.sent == []  # noqa: SLF001


def test_invalid_event_is_dead_lettered(producer_with_mocks, make_event):
    """An invalid event is routed to the DLQ and not produced."""
    producer_with_mocks._produce_one(make_event(price=-9.0))  # noqa: SLF001
    assert producer_with_mocks._metrics["dlq"] == 1  # noqa: SLF001
    assert producer_with_mocks._metrics["produced"] == 0  # noqa: SLF001
    assert "producer_validation_failed" in producer_with_mocks._dlq.sent[0]["reason"]  # noqa: SLF001
