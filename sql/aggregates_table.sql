-- ============================================================================
-- 1-minute tumbling-window aggregates table (events_aggregates_1m)
-- + Dead Letter table (dead_letter_events)
-- + Power BI helper views
-- ----------------------------------------------------------------------------
-- The Spark job creates the tables automatically; the views below are pure
-- conveniences for the Power BI semantic model / direct query.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Aggregates (one row per 1-minute window / symbol / exchange)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `my-gcp-project.rtep_analytics.events_aggregates_1m`
(
  window_start       TIMESTAMP NOT NULL OPTIONS(description = 'Window start (UTC)'),
  window_end         TIMESTAMP NOT NULL OPTIONS(description = 'Window end (UTC)'),
  symbol             STRING    NOT NULL OPTIONS(description = 'Ticker symbol'),
  exchange           STRING             OPTIONS(description = 'Exchange code'),
  avg_price          FLOAT64            OPTIONS(description = 'Average price in window'),
  max_price          FLOAT64            OPTIONS(description = 'Maximum price in window'),
  min_price          FLOAT64            OPTIONS(description = 'Minimum price in window'),
  total_volume       INT64              OPTIONS(description = 'Summed volume in window'),
  event_count        INT64              OPTIONS(description = 'Events in window'),
  avg_change_percent FLOAT64            OPTIONS(description = 'Avg percentage change'),
  event_date         DATE      NOT NULL OPTIONS(description = 'Partition column'),
  processing_time    TIMESTAMP NOT NULL OPTIONS(description = 'Pipeline processing time')
)
PARTITION BY event_date
CLUSTER BY symbol, exchange
OPTIONS (
  description = '1-minute tumbling-window aggregates (append-only stream sink)'
);

-- ---------------------------------------------------------------------------
-- Dead Letter Queue table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `my-gcp-project.rtep_analytics.dead_letter_events`
(
  original_message     STRING             OPTIONS(description = 'Raw failed payload'),
  error_reason         STRING    NOT NULL OPTIONS(description = 'Why it failed'),
  processing_timestamp TIMESTAMP NOT NULL OPTIONS(description = 'When dead-lettered'),
  pipeline_name        STRING    NOT NULL OPTIONS(description = 'Source pipeline'),
  event_date           DATE      NOT NULL OPTIONS(description = 'Partition column')
)
PARTITION BY event_date
CLUSTER BY pipeline_name
OPTIONS (description = 'Dead Letter Queue — records that failed validation');

-- ---------------------------------------------------------------------------
-- Power BI helper view: latest price per symbol (KPI cards)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW `my-gcp-project.rtep_analytics.vw_latest_prices` AS
SELECT
  symbol,
  ANY_VALUE(company) AS company,
  ANY_VALUE(exchange) AS exchange,
  ARRAY_AGG(price ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS latest_price,
  MAX(event_timestamp) AS last_seen
FROM `my-gcp-project.rtep_analytics.events_raw`
WHERE event_date >= CURRENT_DATE() - 1
GROUP BY symbol;

-- ---------------------------------------------------------------------------
-- Power BI helper view: hourly event heatmap source
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW `my-gcp-project.rtep_analytics.vw_hourly_events` AS
SELECT
  event_date,
  event_hour,
  symbol,
  exchange,
  COUNT(*) AS event_count,
  ROUND(AVG(price), 2) AS avg_price,
  SUM(volume) AS total_volume
FROM `my-gcp-project.rtep_analytics.events_raw`
GROUP BY event_date, event_hour, symbol, exchange;

-- ---------------------------------------------------------------------------
-- Power BI helper view: DLQ rate per day (data-quality monitoring)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW `my-gcp-project.rtep_analytics.vw_dlq_rate` AS
WITH good AS (
  SELECT event_date, COUNT(*) AS valid_count
  FROM `my-gcp-project.rtep_analytics.events_raw`
  GROUP BY event_date
),
bad AS (
  SELECT event_date, COUNT(*) AS dlq_count
  FROM `my-gcp-project.rtep_analytics.dead_letter_events`
  GROUP BY event_date
)
SELECT
  COALESCE(good.event_date, bad.event_date) AS event_date,
  IFNULL(good.valid_count, 0) AS valid_count,
  IFNULL(bad.dlq_count, 0) AS dlq_count,
  SAFE_DIVIDE(
    IFNULL(bad.dlq_count, 0),
    IFNULL(good.valid_count, 0) + IFNULL(bad.dlq_count, 0)
  ) * 100 AS dlq_rate_pct
FROM good
FULL OUTER JOIN bad USING (event_date)
ORDER BY event_date;
