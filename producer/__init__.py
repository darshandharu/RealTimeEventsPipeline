"""Kafka producer package for the Real-Time Events Analytics Pipeline.

Contains the data source (Yahoo Finance + mock fallback) and the resilient
Kafka producer that streams events into the ``events`` topic.
"""

from producer.data_source import DataSource, MarketDataSource

__all__ = ["DataSource", "MarketDataSource"]
