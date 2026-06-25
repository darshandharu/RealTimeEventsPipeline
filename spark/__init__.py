"""PySpark Structured Streaming package for the pipeline.

Modules:
    transformations  - derived columns (time parts, price buckets, processing time)
    aggregations     - 1-minute tumbling-window aggregations
    bigquery_sink    - BigQuery table management + streaming sink
    streaming_job    - the end-to-end streaming orchestration entrypoint
"""
