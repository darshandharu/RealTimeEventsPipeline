#!/usr/bin/env python
"""Verify the pipeline's data actually landed (correctly) in BigQuery.

Reads the project/dataset/credentials from config.yaml (+ .env), prints row
counts for each table, and spot-checks a few recent rows so you can confirm the
*content* is correct (real tickers, positive prices, sensible volumes).

Usage:
    python scripts/verify_bigquery.py
    make verify-bq
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make the project root importable when run as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import load_config  # noqa: E402


def main() -> int:
    cfg = load_config()
    project = cfg.bigquery.project_id
    dataset = cfg.bigquery.dataset
    creds_path = cfg.resolve_path(cfg.bigquery.credentials_path)

    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except ImportError:
        print(
            "google-cloud-bigquery not installed. Run: pip install -r requirements.txt"
        )
        return 1

    if not creds_path.exists():
        print(f"Service-account key not found at {creds_path}")
        print("Set GOOGLE_APPLICATION_CREDENTIALS in .env / place config/gcp-key.json.")
        return 1

    creds = service_account.Credentials.from_service_account_file(str(creds_path))
    client = bigquery.Client(
        project=project, credentials=creds, location=cfg.bigquery.location
    )
    ds = f"{project}.{dataset}"
    print(f"Checking dataset: {ds}\n")

    tables = {
        "raw events": cfg.bigquery.tables.raw_events,
        "aggregates": cfg.bigquery.tables.aggregates,
        "dead letter": cfg.bigquery.tables.dead_letter,
    }

    # --- row counts ---------------------------------------------------------
    print("Row counts:")
    any_data = False
    for label, table in tables.items():
        try:
            n = list(
                client.query(f"SELECT COUNT(*) AS c FROM `{ds}.{table}`").result()
            )[0].c
            any_data = any_data or n > 0
            print(f"  {label:12s} {table:24s} = {n} row(s)")
        except Exception as exc:  # noqa: BLE001
            print(f"  {label:12s} {table:24s} = ERROR: {str(exc)[:80]}")

    if not any_data:
        print("\nNo data yet. Make sure the pipeline is running in bigquery mode and")
        print("give it ~1 minute, then re-run this check.")
        return 0

    # --- spot-check recent raw events --------------------------------------
    print("\nLatest raw events (spot-check the content is correct):")
    q = f"""
        SELECT symbol, ROUND(price, 2) AS price, volume, exchange, event_hour
        FROM `{ds}.{cfg.bigquery.tables.raw_events}`
        ORDER BY processing_time DESC
        LIMIT 5
    """
    try:
        for r in client.query(q).result():
            print(
                f"  {r.symbol:6s} ${r.price:<10} vol={r.volume:<10} {r.exchange:6s} hr={r.event_hour}"
            )
    except Exception as exc:  # noqa: BLE001
        print(f"  (could not read sample: {str(exc)[:80]})")

    # --- top symbols by aggregated volume ----------------------------------
    print("\nTop symbols by 1-minute aggregate volume:")
    q2 = f"""
        SELECT symbol, SUM(total_volume) AS vol, COUNT(*) AS windows
        FROM `{ds}.{cfg.bigquery.tables.aggregates}`
        GROUP BY symbol ORDER BY vol DESC LIMIT 5
    """
    try:
        for r in client.query(q2).result():
            print(f"  {r.symbol:6s} total_volume={r.vol:<14} windows={r.windows}")
    except Exception as exc:  # noqa: BLE001
        print(f"  (could not read aggregates: {str(exc)[:80]})")

    print(
        "\nLooks healthy if: counts increase on re-run, tickers are real, prices > 0."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
