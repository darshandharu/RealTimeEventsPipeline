-- ============================================================================
-- Raw validated events table (events_raw)
-- ----------------------------------------------------------------------------
-- DATE-partitioned on `event_date` and clustered by `symbol`, `exchange` so the
-- Power BI queries (filter by date / company / exchange) prune partitions and
-- read only the relevant clusters — fast and cheap at scale.
--
-- The Spark job creates this automatically (CREATE_IF_NEEDED); kept here as the
-- canonical schema reference.
-- ============================================================================

CREATE TABLE IF NOT EXISTS `my-gcp-project.rtep_analytics.events_raw`
(
  event_id        STRING    NOT NULL OPTIONS(description = 'Unique event UUID'),
  symbol          STRING    NOT NULL OPTIONS(description = 'Ticker symbol'),
  company         STRING             OPTIONS(description = 'Company name'),
  price           FLOAT64   NOT NULL OPTIONS(description = 'Last trade price'),
  volume          INT64              OPTIONS(description = 'Traded volume'),
  change          FLOAT64            OPTIONS(description = 'Absolute price change'),
  change_percent  FLOAT64            OPTIONS(description = 'Percentage change'),
  event_timestamp TIMESTAMP NOT NULL OPTIONS(description = 'Event time (UTC)'),
  exchange        STRING             OPTIONS(description = 'Exchange code'),
  -- derived / transformation columns
  event_date      DATE      NOT NULL OPTIONS(description = 'Partition column'),
  event_hour      INT64              OPTIONS(description = 'Hour of event (0-23)'),
  event_day       INT64              OPTIONS(description = 'Day of month'),
  event_month     INT64              OPTIONS(description = 'Month (1-12)'),
  event_year      INT64              OPTIONS(description = 'Year'),
  price_bucket    STRING             OPTIONS(description = 'Categorical price band'),
  processing_time TIMESTAMP NOT NULL OPTIONS(description = 'Pipeline processing time')
)
PARTITION BY event_date
CLUSTER BY symbol, exchange
OPTIONS (
  description = 'Validated real-time stock events (append-only stream sink)',
  require_partition_filter = FALSE
);
