"""Shared pytest fixtures for the pipeline test suite.

Provides a loaded :class:`Config`, canonical valid/invalid event factories, and
a deterministic mock data source so tests run fast, offline and without Kafka,
Spark or BigQuery.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List

import pytest

from config.config_loader import load_config
from schemas.event_schema import event_to_dict


@pytest.fixture(scope="session")
def config():
    """Return the project configuration (loaded once per test session)."""
    return load_config()


@pytest.fixture
def make_event() -> Callable[..., Dict[str, Any]]:
    """Return a factory that builds a valid canonical event with overrides.

    Example:
        >>> event = make_event(price=123.45, symbol="AAPL")
    """

    def _factory(**overrides: Any) -> Dict[str, Any]:
        base = event_to_dict(
            event_id=str(uuid.uuid4()),
            symbol="AAPL",
            company="Apple Inc.",
            price=187.42,
            volume=1_250_000,
            change=1.23,
            change_percent=0.66,
            timestamp=datetime.now(timezone.utc).isoformat(),
            exchange="NASDAQ",
        )
        base.update(overrides)
        return base

    return _factory


@pytest.fixture
def valid_event(make_event) -> Dict[str, Any]:
    """A single well-formed event."""
    return make_event()


@pytest.fixture
def invalid_events(make_event) -> List[Dict[str, Any]]:
    """A list of events, each violating exactly one validation rule."""
    negative_price = make_event(price=-5.0)
    missing_volume = make_event()
    missing_volume.pop("volume")
    bad_timestamp = make_event(timestamp="not-a-timestamp")
    wrong_type = make_event(price="free")
    null_symbol = make_event(symbol=None)
    bad_exchange = make_event(exchange="MARS")
    return [
        negative_price,
        missing_volume,
        bad_timestamp,
        wrong_type,
        null_symbol,
        bad_exchange,
    ]


class _FakeDataSource:
    """Deterministic in-memory data source for producer tests."""

    def __init__(self, batches: List[List[Dict[str, Any]]]) -> None:
        self._batches = batches
        self._idx = 0

    def generate_events(self) -> List[Dict[str, Any]]:
        if self._idx >= len(self._batches):
            return []
        batch = self._batches[self._idx]
        self._idx += 1
        return batch


@pytest.fixture
def fake_data_source() -> Callable[[List[List[Dict[str, Any]]]], _FakeDataSource]:
    """Return a factory building a deterministic fake data source."""

    def _factory(batches: List[List[Dict[str, Any]]]) -> _FakeDataSource:
        return _FakeDataSource(batches)

    return _factory
