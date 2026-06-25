# Power BI Dashboard — Setup & Design Guide

This guide explains how to connect Power BI to the BigQuery sink and build the
**Real-Time Events Analytics** dashboard described in requirement #9.

> The pipeline writes three tables and several helper views into
> `your-project.rtep_analytics`. Power BI reads from those — it never touches
> Kafka or Spark directly.

---

## 1. Prerequisites

| Requirement | Notes |
|-------------|-------|
| Power BI Desktop (latest) | Free; Windows only. |
| BigQuery connector | Built in: **Get Data → Google BigQuery**. |
| GCP account with BigQuery access | Read access to the `rtep_analytics` dataset. |
| Pipeline has produced data | Run the producer + Spark job first (see root README). |

---

## 2. Connect to BigQuery

1. **Home → Get Data → Google BigQuery**.
2. Sign in with the Google account that can read the dataset.
3. Navigate to `your-project → rtep_analytics` and select:
   - `events_raw` (detailed events)
   - `events_aggregates_1m` (1-minute aggregates — preferred for time charts)
   - `dead_letter_events` (data-quality monitoring)
   - Views: `vw_latest_prices`, `vw_hourly_events`, `vw_dlq_rate`
4. **Storage mode**: choose **DirectQuery** for near-real-time refresh, or
   **Import** + a scheduled refresh (e.g. every 5–15 min) for snappier visuals.
   For a "real-time" feel, DirectQuery on `events_aggregates_1m` is the sweet
   spot (small, pre-aggregated table).

---

## 3. Data model

Build a simple star-ish model:

```
        vw_latest_prices ─┐
events_aggregates_1m ─────┼── (symbol) ── Dim_Symbol (optional)
events_raw ───────────────┘
dead_letter_events  (standalone, by event_date)
```

- Create a **Date table** (`Calendar`) and relate it to `event_date` on each
  fact for proper time intelligence:
  ```DAX
  Calendar = CALENDAR(MIN(events_raw[event_date]), MAX(events_raw[event_date]))
  ```
- Mark `Calendar` as the official date table.

---

## 4. Suggested measures (DAX)

```DAX
Avg Price        = AVERAGE(events_aggregates_1m[avg_price])
Max Price        = MAX(events_aggregates_1m[max_price])
Min Price        = MIN(events_aggregates_1m[min_price])
Total Volume     = SUM(events_aggregates_1m[total_volume])
Event Count      = SUM(events_aggregates_1m[event_count])
DLQ Count        = COUNTROWS(dead_letter_events)
DLQ Rate %       =
    DIVIDE([DLQ Count], [Event Count] + [DLQ Count]) * 100
Latest Price     = SELECTEDVALUE(vw_latest_prices[latest_price])
```

---

## 5. Report pages & visuals (maps to requirement #9)

### Page 1 — Real-Time Overview
| Visual | Type | Fields |
|--------|------|--------|
| **KPI cards** | Card | `Avg Price`, `Max Price`, `Event Count`, `Total Volume` |
| **Price over time** | Line chart | Axis: `window_start` · Values: `Avg Price` · Legend: `symbol` |
| **Top symbols** | Bar chart | Axis: `symbol` · Values: `Total Volume` (Top N filter = 10) |
| **Market distribution** | Pie / Donut | Legend: `exchange` · Values: `Event Count` |
| **Hourly events heatmap** | Matrix (conditional formatting) | Rows: `symbol` · Cols: `event_hour` · Values: `event_count` from `vw_hourly_events` |

### Page 2 — Data Quality
| Visual | Type | Fields |
|--------|------|--------|
| **DLQ rate trend** | Line | Axis: `event_date` · Values: `dlq_rate_pct` (`vw_dlq_rate`) |
| **Top error reasons** | Bar | Axis: `error_reason` · Values: `DLQ Count` |
| **DLQ table** | Table | `processing_timestamp`, `error_reason`, `original_message` |

### Slicers / filters (requirement #9)
Add slicers for **Date** (`Calendar[Date]`), **Company** (`company`) and
**Exchange** (`exchange`) on every page (use a sync slicer).

---

## 6. Auto-refresh (real-time feel)

- **DirectQuery**: Page → Format → **Page refresh** → ON, interval 1–5 min
  (requires admin enablement on the capacity for sub-30-min intervals).
- **Import mode**: publish to the Power BI Service and set a **Scheduled
  refresh** under dataset settings.

---

## 7. Publishing

1. **Home → Publish → My workspace**.
2. In the Service, configure the **BigQuery data source credentials** (OAuth).
3. Share the report or pin visuals to a dashboard for a tiled, always-on view.

---

## 8. Screenshots

Drop exported dashboard images into [`../screenshots/`](../screenshots/) and
reference them from the root `README.md`:

- `screenshots/overview.png`
- `screenshots/data_quality.png`

> Tip: **File → Export → Export to PDF** gives a quick shareable artifact for
> your GitHub README / portfolio.
