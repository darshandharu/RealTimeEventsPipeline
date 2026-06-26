# 🎓 Project Walkthrough & Explainer

> **Purpose of this file:** a plain-English guide to the *Real-Time Events Analytics
> Pipeline* — what each piece does, **why** it exists, and **how to explain it to
> someone else** (an interviewer, a teammate, a friend). Read it top-to-bottom once
> and you'll be able to talk about every stage with confidence.

---

## 1. The 30-second pitch (memorize this)

> "I built a **real-time data pipeline** that ingests live stock-market events,
> validates and cleans them as they stream in, computes rolling 1-minute analytics,
> and lands the results in a cloud data warehouse that powers a live dashboard.
> It's fully containerized with Docker, has automated tests and CI, and handles bad
> data gracefully with a Dead Letter Queue. The stack is **Python, Apache Kafka,
> PySpark Structured Streaming, Google BigQuery, and Power BI**."

That single paragraph hits: real-time, streaming, data quality, cloud, visualization,
testing, and resilience. Everything below is just *detail* behind that pitch.

---

## 2. The big picture — follow ONE event through the system

Imagine a single price tick for **AAPL** appears. Here's its journey:

```
[1] Data source        A price for AAPL is generated (Yahoo Finance, or a mock).
        │
        ▼
[2] Kafka Producer     It's wrapped in JSON, validated, and published to Kafka.
        │              (Bad records are side-lined here already.)
        ▼
[3] Kafka topic        It waits in the "events" topic — a durable, ordered log.
        │
        ▼
[4] PySpark Streaming  Spark reads it, applies an explicit schema, and runs checks:
        │                 • valid?   → keep going
        │                 • invalid? → send to the Dead Letter Queue
        ▼
[5] Transform          Add derived columns (hour, day, price bucket, processing time).
        │
        ▼
[6] Window aggregate   Group all AAPL ticks in the same 1-minute window and compute
        │              avg/max/min price, total volume, event count.
        ▼
[7] BigQuery           Append the row to a partitioned, clustered table in the cloud.
        │
        ▼
[8] Power BI           A dashboard reads BigQuery and shows it as a live chart.
```

**One sentence per stage** is enough to narrate the whole thing. That's the skill.

---

## 3. Stage-by-stage — what / why / how to say it

For each stage: **What it is**, **Why it matters**, and a **"Say this"** line you can
use verbatim.

### Stage 1 — The Data Source (`producer/data_source.py`)

- **What:** Produces stock events with fields like `symbol`, `price`, `volume`,
  `timestamp`, `exchange`. It tries the **live Yahoo Finance API** first and falls
  back to a **realistic mock generator** if the internet/API is unavailable.
- **Why:** Real pipelines must keep flowing even when an upstream source hiccups.
  The mock fallback also means the whole project can be demoed offline.
- **Say this:** *"My producer is resilient — it prefers live market data but
  automatically degrades to a mock generator if the API fails, so the pipeline never
  starves. It even injects a small % of deliberately bad records so I can prove the
  data-quality path works."*

### Stage 2 — The Kafka Producer (`producer/producer.py`)

- **What:** Publishes events to Kafka with strong delivery guarantees
  (`acks=all` + idempotence), retries with backoff, and a graceful shutdown.
- **Why:** This is the "front door" to the pipeline. If it loses or duplicates
  messages, everything downstream is wrong.
- **Say this:** *"The producer uses idempotent, acks=all delivery so messages aren't
  lost or duplicated, with exponential-backoff retries and a graceful shutdown that
  flushes in-flight messages."*

### Stage 3 — Apache Kafka (the message bus)

- **What:** A distributed, durable **log** of events. We use two topics:
  `events` (good data) and `dead-letter-events` (rejected data).
- **Why:** Kafka **decouples** the producer from the consumer. The producer can write
  fast while Spark reads at its own pace; if Spark restarts, the data is still in
  Kafka. It's the shock-absorber between "data arriving" and "data being processed."
- **Say this:** *"Kafka is the backbone — a durable, replayable log that decouples
  producers from consumers, so each side can scale and fail independently."*
- **Analogy:** Kafka is a **conveyor belt with memory**. Items keep their order, and
  if a worker steps away, the items wait on the belt instead of falling off.

### Stage 4 — PySpark Structured Streaming (`spark/streaming_job.py`)

This is the brain of the pipeline. It does several things in one continuous job:

**(a) Read from Kafka + apply an EXPLICIT schema** (`spark/transformations.py`)
- **What:** We define every field's type by hand and parse the JSON with it — we
  never let Spark "guess" the schema.
- **Why:** Inferred schemas drift and break silently in production. An explicit
  schema is a **contract**: malformed data becomes obvious instead of corrupting
  results.
- **Say this:** *"I declare the schema explicitly — no inference — so the data
  contract is versioned and bad data surfaces immediately."*

**(b) Validate every record** (`validation/validator.py`)
- **What:** Checks for missing fields, negative prices, bad timestamps, wrong types,
  corrupted JSON, invalid exchanges, etc.
- **Why:** "Garbage in, garbage out." Validation is what makes the analytics
  trustworthy.
- **Say this:** *"Each record is validated against business rules; anything that
  fails is quarantined, not silently dropped."*

**(c) Dead Letter Queue** (`dlq/dead_letter_queue.py`)
- **What:** Invalid records are wrapped with context — the **original message**, the
  **error reason**, a **timestamp**, and the **pipeline name** — and sent to a
  separate `dead-letter-events` topic (and a BigQuery table).
- **Why:** In a stream you can't stop the world for one bad record. The DLQ lets the
  pipeline keep running while preserving every failure for later inspection.
- **Say this:** *"Bad records go to a Dead Letter Queue with full context, so the
  stream never crashes on bad data and I can audit exactly what failed and why."*
- **Analogy:** It's the **"returns desk"** — defective items get set aside with a
  note explaining the defect, instead of jamming the assembly line.

**(d) Watermarking + de-duplication** (`spark/aggregations.py`)
- **What:** A **watermark** tells Spark "stop waiting for events older than N minutes."
  De-duplication removes repeated `event_id`s within that window.
- **Why:** Streams have **late-arriving** and **duplicate** data. Watermarks let us
  handle late data correctly *and* bound memory so the job doesn't grow forever.
- **Say this:** *"I use event-time watermarking to tolerate late data while keeping
  state bounded, plus de-duplication so retries are counted exactly once."*

**(e) Windowed aggregation** (`spark/aggregations.py`)
- **What:** Groups events into **1-minute tumbling windows** per symbol and computes
  avg/max/min price, total volume, and event count.
- **Why:** Raw ticks are noisy; per-minute summaries are what dashboards and humans
  actually use.
- **Say this:** *"I aggregate into 1-minute tumbling windows — non-overlapping
  buckets — to turn a firehose of ticks into clean per-minute metrics."*
- **Analogy:** Tumbling windows are like **counting customers per hour** — each
  customer falls into exactly one hour bucket.

### Stage 5 — BigQuery sink (`spark/bigquery_sink.py`)

- **What:** Writes results to Google BigQuery. Tables are **auto-created**,
  **append-only**, **partitioned by date**, and **clustered by symbol/exchange**.
- **Why:** Partitioning + clustering means queries only scan the data they need →
  fast and cheap at scale. Append-only keeps a full history.
- **Say this:** *"Results land in BigQuery in tables partitioned by date and
  clustered by symbol, so analytical queries prune to just the relevant data and
  stay cheap as the dataset grows."*
- **Analogy:** Partitioning is **filing documents by month**; clustering is **sorting
  each month's folder by customer**. You find things without reading everything.

### Stage 6 — Power BI dashboard (`RTEP_Dashboard.pbix`, `powerbi/`)

- **What:** Connects to BigQuery and visualizes the data: price-over-time line chart,
  volume-by-symbol bar chart, an event-count KPI card, and an interactive symbol
  slicer.
- **Why:** Data has no value until a human can *see* and *act* on it. This is the
  "last mile."
- **Say this:** *"Power BI sits on top of BigQuery — the same data the pipeline
  produced is now an interactive dashboard where one click filters every chart."*

### Stage 7 — Observability (`monitoring/`) — the "bonus" that impresses

- **What:** A streaming metrics listener (exports to **Prometheus**), an **alerting**
  framework (logs/webhooks on thresholds), and a **data-quality report** that scores
  completeness/validity/uniqueness/timeliness.
- **Why:** Production systems must be *observable* — you need to know throughput,
  lag, and failure rates without reading logs by hand.
- **Say this:** *"I added observability — Prometheus metrics, threshold-based
  alerting, and automated data-quality scorecards — because a pipeline you can't
  monitor is a pipeline you can't trust."*

---

## 4. Key concepts in one line each (the jargon decoder)

Use these when someone asks "what does X mean?"

| Term | One-line explanation |
|------|----------------------|
| **Streaming** | Processing data continuously as it arrives, instead of in nightly batches. |
| **Kafka topic** | A named, durable, ordered log that producers write to and consumers read from. |
| **Producer / Consumer** | Producer writes events into Kafka; consumer (Spark) reads them out. |
| **Schema** | The agreed shape/types of a record — the data "contract." |
| **Validation** | Rules that decide if a record is good enough to use. |
| **Dead Letter Queue (DLQ)** | A holding area for records that failed processing, kept with the reason. |
| **Watermark** | A cutoff that says "I'll wait this long for late data, then move on." |
| **Tumbling window** | Fixed, non-overlapping time buckets (e.g. every 1 minute). |
| **Idempotence** | Doing something twice has the same effect as once (no duplicates). |
| **Checkpointing** | Saving stream progress so it can resume exactly where it left off after a crash. |
| **Partitioning** | Splitting a table by a column (date) so queries scan less data. |
| **Clustering** | Sorting data within partitions (by symbol) for even faster filtering. |
| **Idempotent + acks=all** | The producer's "no message lost, no message duplicated" setting. |

---

## 5. How to DEMO it live (your script)

Run these in the project folder to show the pipeline working end-to-end.

**Offline demo (no cloud needed):**
```bash
# 1. Start everything in Docker with the console sink
SINK_TYPE=console docker compose up -d --build

# 2. Watch the pipeline process events live
docker compose logs -f spark-streaming     # parsed events, aggregates, DLQ
docker compose logs -f producer            # events being produced

# 3. (Optional) open the Kafka UI
docker compose --profile ui up -d kafka-ui   # http://localhost:8080
```

**Live BigQuery demo:**
```bash
# Switch the sink to BigQuery (needs config/gcp-key.json + .env set)
SINK_TYPE=bigquery docker compose up -d --build
docker compose logs -f spark-streaming      # watch "[raw_events] wrote N rows"
```

**Prove the Dead Letter Queue works (the wow moment):**
```bash
# Inject a deliberately broken record straight into the events topic
printf '%s\n' '{"event_id":"demo-bad","symbol":"AAPL","price":-99,"volume":1,"timestamp":"not-a-date","exchange":"MARS"}' \
  | docker exec -i rtep-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic events
# ...then show it appear in the dead_letter_events table / DLQ topic with a clear error reason.
```

**Stop everything when done:**
```bash
docker compose down
```

> **Talking point while it runs:** *"Notice the producer is falling back to mock data
> because the container has no internet — that's the resilience I built in, working
> live."*

---

## 6. Interview Q&A — likely questions, ready answers

**Q: Why Kafka instead of calling the database directly?**
A: Kafka decouples producers from consumers and buffers spikes. If the consumer or
database is slow or down, data waits durably in Kafka instead of being lost. It also
lets multiple consumers read the same stream independently.

**Q: What happens to bad data?**
A: It's caught by the validation layer and routed to a Dead Letter Queue with the
original message and the reason it failed — the stream keeps running, and nothing is
silently dropped. I demonstrated this by injecting malformed records and showing them
land in the DLQ table with precise error reasons.

**Q: How do you handle late or duplicate events?**
A: Event-time **watermarking** tolerates late events up to a configured delay while
bounding state, and **de-duplication** on `event_id` ensures retries are counted once.

**Q: Why partition and cluster the BigQuery tables?**
A: Partitioning by date lets queries scan only relevant days; clustering by symbol
sorts within each day. Together they cut the data scanned per query, which makes
dashboards fast and queries cheap as data grows.

**Q: How do you know it works / how is it tested?**
A: Unit tests cover the producer, validation rules, transformations, and aggregations,
and they run automatically in **GitHub Actions CI** (lint + type-check + tests +
Docker build). I also ran the whole thing end-to-end in Docker, in both offline and
live-BigQuery modes.

**Q: What was the hardest part?**
A: Real bugs only surface when you actually run it. For example, my Spark validation
expression grew exponentially and blew past the JVM's 64KB method limit — I rewrote it
with `coalesce` to make it linear. I also hit a "redefining watermark" crash and fixed
it by applying the watermark in exactly one place. Those are the kinds of issues you
only learn by running the pipeline for real.

**Q: How would you scale this?**
A: Add Kafka partitions and Spark executors for more throughput, run Spark on Dataproc
or Kubernetes, and use the BigQuery Storage Write API (already configured) for
high-throughput inserts. The architecture is already horizontally scalable because
Kafka and Spark both scale out.

---

## 7. The Power BI dashboard — what each visual proves

| Visual | What it shows | Why it's there |
|--------|---------------|----------------|
| **Line: avg price over time** | Market average price per minute | Demonstrates the streaming time-series result |
| **Bar: volume by symbol** | Total traded volume per ticker | Shows the aggregation + ranking |
| **KPI card: event count** | Total events processed (e.g. 756) | A headline number for the exec view |
| **Slicer: symbol** | Click a ticker → all visuals filter | Proves the dashboard is interactive |

**How I built it (so you can rebuild/extend it):**
1. **Get Data → Google BigQuery** → sign in → select `rtep_analytics` → load the
   `events_raw`, `events_aggregates_1m`, `dead_letter_events` tables (Import mode).
2. **Save the .pbix immediately** (don't lose work!).
3. Build each visual by **checking fields in the Data pane**, then converting to the
   chart type via the Visualizations icons (table → bar, etc.).
4. For the time axis, set the date field to the **continuous** `window_start`
   (not the date hierarchy) so you get a real per-minute line.
5. Set aggregations via the field's dropdown (e.g. `avg_price` → **Average**).
6. Add a **Slicer** with `symbol` for interactivity.

**Easy extensions:** an exchange **donut** (`exchange` + `event_count`), a **DLQ
card** (count of `dead_letter_events`), and a **Calendar** table + relationships so a
single date slicer filters every table.

---

## 8. The one-line summary of what you accomplished

> "I designed and built a production-style, end-to-end real-time analytics platform —
> from data generation through Kafka and Spark streaming with full data-quality
> handling, into a partitioned cloud warehouse, visualized in Power BI — containerized,
> tested, and running in CI."

That's a **Senior Data Engineer-level** story. You can back up every word of it.
</content>
