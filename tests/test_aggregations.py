"""Unit tests for windowed aggregations (requirement #14).

Exercises the 1-minute tumbling-window aggregation logic on a static batch
DataFrame (the same aggregation expressions used in the streaming query).
Skipped automatically when PySpark / the JVM is unavailable.
"""

from __future__ import annotations

from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.aggregations import aggregate_tumbling_window, deduplicate_events  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    """A local SparkSession for aggregation tests."""
    try:
        session = (
            SparkSession.builder.appName("rtep-agg-tests")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Spark/JVM unavailable: {exc}")
    yield session
    session.stop()


_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("symbol", StringType()),
        StructField("exchange", StringType()),
        StructField("price", DoubleType()),
        StructField("volume", LongType()),
        StructField("change_percent", DoubleType()),
        StructField("event_ts", TimestampType()),
    ]
)


def _events_df(spark):
    """Three AAPL ticks within the same minute + one MSFT tick."""
    base = datetime(2026, 6, 25, 14, 30, 0)
    rows = [
        ("1", "AAPL", "NASDAQ", 100.0, 10, 1.0, base.replace(second=5)),
        ("2", "AAPL", "NASDAQ", 110.0, 20, 2.0, base.replace(second=25)),
        ("3", "AAPL", "NASDAQ", 120.0, 30, 3.0, base.replace(second=45)),
        ("4", "MSFT", "NASDAQ", 300.0, 5, 0.5, base.replace(second=10)),
    ]
    return spark.createDataFrame(rows, _SCHEMA)


def test_tumbling_window_aggregates(spark, config):
    """Per-symbol 1-minute aggregates compute correct stats."""
    df = _events_df(spark)
    result = {
        row["symbol"]: row
        for row in aggregate_tumbling_window(df, config).collect()
    }

    aapl = result["AAPL"]
    assert aapl["event_count"] == 3
    assert abs(aapl["avg_price"] - 110.0) < 1e-6
    assert aapl["max_price"] == 120.0
    assert aapl["min_price"] == 100.0
    assert aapl["total_volume"] == 60
    assert aapl["event_date"] is not None

    msft = result["MSFT"]
    assert msft["event_count"] == 1
    assert msft["total_volume"] == 5


def test_aggregate_output_schema_matches_bq(spark, config):
    """The aggregate output exposes exactly the BQ aggregate columns."""
    df = _events_df(spark)
    cols = set(aggregate_tumbling_window(df, config).columns)
    expected = {
        "window_start",
        "window_end",
        "symbol",
        "exchange",
        "avg_price",
        "max_price",
        "min_price",
        "total_volume",
        "event_count",
        "avg_change_percent",
        "event_date",
        "processing_time",
    }
    assert cols == expected


def test_deduplicate_drops_duplicate_event_ids(spark, config):
    """Duplicate event_ids within the watermark are collapsed to one."""
    df = _events_df(spark)
    duped = df.union(df.limit(1))  # replay the first row
    deduped = deduplicate_events(duped, config)
    counts = deduped.groupBy("event_id").count().collect()
    assert all(row["count"] == 1 for row in counts)
