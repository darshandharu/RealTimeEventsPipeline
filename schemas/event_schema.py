"""Explicit, versioned schemas for the stock-market event stream.

This module is the single source of truth for the *shape* of an event as it
flows through the pipeline. We declare schemas in four complementary forms so
each stage uses the right tool:

1. :data:`STOCK_EVENT_SCHEMA`   - PySpark ``StructType`` for parsing the Kafka
   value with ``from_json`` (requirement #3: schema is **explicit, never
   inferred**).
2. :data:`EVENT_JSON_SCHEMA`    - JSON Schema (draft-07) used by the producer /
   validation layer to validate payloads before they ever hit Kafka.
3. :data:`EVENT_AVRO_SCHEMA`    - Avro schema string registered with the
   Confluent Schema Registry (bonus: schema registry support).
4. ``*_TABLE_SCHEMA``           - BigQuery table schemas for the raw, aggregate
   and dead-letter sinks.

Keeping all representations side-by-side makes drift obvious in code review.

Event contract (the wire format the producer emits)
---------------------------------------------------
======================  =========  =================================================
field                   type       notes
======================  =========  =================================================
event_id                string     UUID, unique per event (dedup key)
symbol                  string     ticker, e.g. ``AAPL``
company                 string     human-readable company name
price                   double     last trade price (must be > 0)
volume                  long       traded volume (>= 0)
change                  double     absolute price change vs previous close
change_percent          double     percentage change
timestamp               string     ISO-8601 event time (UTC)
exchange                string     e.g. ``NASDAQ`` / ``NYSE``
======================  =========  =================================================
"""

from __future__ import annotations

from typing import Any, Dict

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# ===========================================================================
# 1. PySpark StructType - used with from_json() in the streaming job.
#    Declared field-by-field; `nullable=True` so malformed records yield NULLs
#    that the validation layer catches (rather than failing the whole batch).
# ===========================================================================
STOCK_EVENT_SCHEMA: StructType = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("symbol", StringType(), nullable=True),
        StructField("company", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("volume", LongType(), nullable=True),
        StructField("change", DoubleType(), nullable=True),
        StructField("change_percent", DoubleType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("exchange", StringType(), nullable=True),
    ]
)


# ===========================================================================
# 2. JSON Schema (draft-07) - producer-side & validation-layer payload check.
# ===========================================================================
EVENT_JSON_SCHEMA: Dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "StockMarketEvent",
    "type": "object",
    "additionalProperties": False,
    "required": [
        "event_id",
        "symbol",
        "company",
        "price",
        "volume",
        "change",
        "change_percent",
        "timestamp",
        "exchange",
    ],
    "properties": {
        "event_id": {"type": "string", "minLength": 1},
        "symbol": {"type": "string", "minLength": 1, "maxLength": 12},
        "company": {"type": "string", "minLength": 1},
        "price": {"type": "number", "exclusiveMinimum": 0},
        "volume": {"type": "integer", "minimum": 0},
        "change": {"type": "number"},
        "change_percent": {"type": "number"},
        "timestamp": {"type": "string", "format": "date-time"},
        "exchange": {"type": "string", "minLength": 1},
    },
}


# ===========================================================================
# 3. Avro schema (Confluent Schema Registry) - bonus: schema registry support.
#    Kept structurally identical to the JSON contract above.
# ===========================================================================
EVENT_AVRO_SCHEMA: str = (
    """
{
  "type": "record",
  "name": "StockMarketEvent",
  "namespace": "com.rtep.events",
  "doc": "A single real-time stock market trade/quote event.",
  "fields": [
    {"name": "event_id",       "type": "string", "doc": "UUID unique per event"},
    {"name": "symbol",         "type": "string", "doc": "Ticker symbol"},
    {"name": "company",        "type": "string", "doc": "Company name"},
    {"name": "price",          "type": "double", "doc": "Last trade price"},
    {"name": "volume",         "type": "long",   "doc": "Traded volume"},
    {"name": "change",         "type": "double", "doc": "Absolute price change"},
    {"name": "change_percent", "type": "double", "doc": "Percentage change"},
    {"name": "timestamp",      "type": "string", "doc": "ISO-8601 event time (UTC)"},
    {"name": "exchange",       "type": "string", "doc": "Exchange code"}
  ]
}
""".strip()
)


# ===========================================================================
# 4. BigQuery table schemas (list-of-dict form accepted by the BQ client).
# ===========================================================================
#: Raw, validated events table. Partitioned by `event_date`, clustered by
#: `symbol`, `exchange`. Mirrors STOCK_EVENT_SCHEMA + derived/transform columns.
RAW_EVENT_TABLE_SCHEMA: list[dict[str, str]] = [
    {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
    {"name": "company", "type": "STRING", "mode": "NULLABLE"},
    {"name": "price", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "volume", "type": "INT64", "mode": "NULLABLE"},
    {"name": "change", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "change_percent", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "exchange", "type": "STRING", "mode": "NULLABLE"},
    # --- derived / transformation columns (requirement #6) ---
    {"name": "event_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "event_hour", "type": "INT64", "mode": "NULLABLE"},
    {"name": "event_day", "type": "INT64", "mode": "NULLABLE"},
    {"name": "event_month", "type": "INT64", "mode": "NULLABLE"},
    {"name": "event_year", "type": "INT64", "mode": "NULLABLE"},
    {"name": "price_bucket", "type": "STRING", "mode": "NULLABLE"},
    {"name": "processing_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

#: 1-minute tumbling-window aggregates table.
AGGREGATE_TABLE_SCHEMA: list[dict[str, str]] = [
    {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
    {"name": "exchange", "type": "STRING", "mode": "NULLABLE"},
    {"name": "avg_price", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "max_price", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "min_price", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "total_volume", "type": "INT64", "mode": "NULLABLE"},
    {"name": "event_count", "type": "INT64", "mode": "NULLABLE"},
    {"name": "avg_change_percent", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "event_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "processing_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

#: Dead Letter Queue table (requirement #5 contract).
DLQ_TABLE_SCHEMA: list[dict[str, str]] = [
    {"name": "original_message", "type": "STRING", "mode": "NULLABLE"},
    {"name": "error_reason", "type": "STRING", "mode": "REQUIRED"},
    {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "pipeline_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "event_date", "type": "DATE", "mode": "REQUIRED"},
]


def event_to_dict(
    event_id: str,
    symbol: str,
    company: str,
    price: float,
    volume: int,
    change: float,
    change_percent: float,
    timestamp: str,
    exchange: str,
) -> Dict[str, Any]:
    """Assemble a canonical event dict matching the wire contract.

    Centralising construction here means the producer and tests build events
    the *same* way, so a contract change is a one-line edit.

    Args:
        event_id: Unique event UUID.
        symbol: Ticker symbol.
        company: Company name.
        price: Last trade price.
        volume: Traded volume.
        change: Absolute price change.
        change_percent: Percentage change.
        timestamp: ISO-8601 UTC event time.
        exchange: Exchange code.

    Returns:
        A dict with keys ordered to match :data:`EVENT_AVRO_SCHEMA`.
    """
    return {
        "event_id": event_id,
        "symbol": symbol,
        "company": company,
        "price": price,
        "volume": volume,
        "change": change,
        "change_percent": change_percent,
        "timestamp": timestamp,
        "exchange": exchange,
    }


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    print("Spark StructType fields:")
    for field in STOCK_EVENT_SCHEMA.fields:
        print(f"  {field.name:<16} {field.dataType.simpleString()}")
    print(f"\nJSON Schema required: {EVENT_JSON_SCHEMA['required']}")
    print("Avro record name    : StockMarketEvent")
    print(f"BQ raw columns      : {len(RAW_EVENT_TABLE_SCHEMA)}")
