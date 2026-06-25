"""Unit tests for Spark transformations (requirement #14).

These tests run against a *local* SparkSession on small batch DataFrames (the
same Column logic used in streaming). They are skipped automatically if PySpark
or a working JVM is unavailable, so the rest of the suite still runs in minimal
CI environments.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from spark.transformations import add_derived_columns, parse_kafka_events  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    """A local single-threaded SparkSession for tests."""
    try:
        session = (
            SparkSession.builder.appName("rtep-tests")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover - no JVM available
        pytest.skip(f"Spark/JVM unavailable: {exc}")
    yield session
    session.stop()


def test_parse_kafka_events_applies_explicit_schema(spark):
    """Parsing decodes JSON into typed columns and a real event_ts."""
    raw = spark.createDataFrame(
        [
            (
                b"AAPL",
                b'{"event_id":"1","symbol":"AAPL","company":"Apple Inc.",'
                b'"price":187.4,"volume":1000,"change":1.2,"change_percent":0.6,'
                b'"timestamp":"2026-06-25T12:00:00Z","exchange":"NASDAQ"}',
                "events",
                0,
                10,
                None,
            )
        ],
        ["key", "value", "topic", "partition", "offset", "timestamp"],
    )
    parsed = parse_kafka_events(raw).collect()[0]
    assert parsed["symbol"] == "AAPL"
    assert abs(parsed["price"] - 187.4) < 1e-6
    assert parsed["event_ts"] is not None
    assert parsed["raw_value"] is not None  # retained for DLQ


def test_parse_kafka_events_corrupted_json_yields_nulls(spark):
    """Malformed JSON yields null fields (caught downstream as corrupted_json)."""
    raw = spark.createDataFrame(
        [(b"X", b"{not valid json", "events", 0, 1, None)],
        ["key", "value", "topic", "partition", "offset", "timestamp"],
    )
    parsed = parse_kafka_events(raw).collect()[0]
    assert parsed["event_id"] is None
    assert parsed["price"] is None
    assert parsed["raw_value"] == "{not valid json"


def test_add_derived_columns(spark):
    """Derived time-part columns and price_bucket are populated correctly."""
    raw = spark.createDataFrame(
        [
            (
                b"AAPL",
                b'{"event_id":"1","symbol":"AAPL","company":"Apple Inc.",'
                b'"price":187.4,"volume":1000,"change":1.2,"change_percent":0.6,'
                b'"timestamp":"2026-06-25T14:30:00Z","exchange":"NASDAQ"}',
                "events",
                0,
                10,
                None,
            )
        ],
        ["key", "value", "topic", "partition", "offset", "timestamp"],
    )
    enriched = add_derived_columns(parse_kafka_events(raw)).collect()[0]
    assert enriched["event_year"] == 2026
    assert enriched["event_month"] == 6
    assert enriched["event_day"] == 25
    assert enriched["event_hour"] == 14
    assert enriched["price_bucket"] == "100-250"
    assert enriched["event_date"] is not None
    assert enriched["processing_time"] is not None


@pytest.mark.parametrize(
    "price,bucket",
    [(25.0, "0-50"), (75.0, "50-100"), (300.0, "250-500"), (1500.0, "1000+")],
)
def test_price_buckets(spark, price, bucket):
    """price_bucket boundaries map prices to the expected label."""
    payload = (
        '{"event_id":"1","symbol":"AAPL","company":"A","price":%s,"volume":1,'
        '"change":0,"change_percent":0,"timestamp":"2026-06-25T14:30:00Z",'
        '"exchange":"NASDAQ"}' % price
    ).encode()
    raw = spark.createDataFrame(
        [(b"AAPL", payload, "events", 0, 1, None)],
        ["key", "value", "topic", "partition", "offset", "timestamp"],
    )
    enriched = add_derived_columns(parse_kafka_events(raw)).collect()[0]
    assert enriched["price_bucket"] == bucket
