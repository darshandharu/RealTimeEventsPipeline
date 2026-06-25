"""Explicit schema definitions for the Real-Time Events Analytics Pipeline.

Schemas are declared *explicitly* (never inferred) so the streaming contract is
versioned, reviewable and stable across producer and consumer.
"""

from schemas.event_schema import (
    AGGREGATE_TABLE_SCHEMA,
    DLQ_TABLE_SCHEMA,
    EVENT_AVRO_SCHEMA,
    EVENT_JSON_SCHEMA,
    RAW_EVENT_TABLE_SCHEMA,
    STOCK_EVENT_SCHEMA,
    event_to_dict,
)

__all__ = [
    "STOCK_EVENT_SCHEMA",
    "EVENT_JSON_SCHEMA",
    "EVENT_AVRO_SCHEMA",
    "RAW_EVENT_TABLE_SCHEMA",
    "AGGREGATE_TABLE_SCHEMA",
    "DLQ_TABLE_SCHEMA",
    "event_to_dict",
]
