"""Data Quality Report generator (bonus #17).

Produces a point-in-time data-quality scorecard for the pipeline by analysing
the DLQ fallback file (and/or a supplied batch of records) across the classic
DQ dimensions:

* **Completeness** - share of records with all required fields populated.
* **Validity**     - share of records passing every validation rule.
* **Uniqueness**   - share of distinct ``event_id`` values (duplicate rate).
* **Timeliness**   - share of records whose event time is within freshness SLA.
* **Consistency**  - share of records with in-range numeric values.

The report is rendered to both **JSON** (machine-readable, for dashboards/CI
gates) and a self-contained **HTML** scorecard (human-readable), written under
``monitoring.data_quality_report.output_dir``.

This module is deliberately Spark-free: it operates on plain Python records so
it can run anywhere (a cron job, CI, or a post-run hook) and is trivially unit
-testable.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from config.config_loader import Config
from utils.logger import get_logger
from validation.validator import EventValidator

_log = get_logger(__name__, component="pipeline")


@dataclass
class DQDimensions:
    """Computed data-quality dimension scores (each 0-100)."""

    completeness: float = 100.0
    validity: float = 100.0
    uniqueness: float = 100.0
    timeliness: float = 100.0
    consistency: float = 100.0

    def overall(self) -> float:
        """Return the equally-weighted overall DQ score (0-100)."""
        scores = [
            self.completeness,
            self.validity,
            self.uniqueness,
            self.timeliness,
            self.consistency,
        ]
        return round(sum(scores) / len(scores), 2)


@dataclass
class DQReport:
    """A complete data-quality report payload."""

    generated_at: str
    pipeline_name: str
    environment: str
    total_records: int
    valid_records: int
    invalid_records: int
    duplicate_records: int
    dimensions: DQDimensions
    overall_score: float
    top_error_reasons: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Return the report as a JSON-serializable dict."""
        payload = asdict(self)
        payload["dimensions"] = asdict(self.dimensions)
        return payload


class DataQualityReporter:
    """Computes DQ dimension scores and renders JSON/HTML reports.

    Args:
        config: The loaded pipeline configuration.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._validator = EventValidator(config)
        self._required = config.validation.required_fields
        self._output_dir = config.resolve_path(
            config.monitoring.data_quality_report.output_dir
        )
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._freshness_seconds = (
            config.validation.rules.max_timestamp_skew_seconds
        )

    # ------------------------------------------------------------------
    def generate(self, records: Iterable[Dict[str, Any]]) -> DQReport:
        """Compute a data-quality report over an iterable of event records.

        Args:
            records: An iterable of event dicts to score.

        Returns:
            A populated :class:`DQReport`.
        """
        records = list(records)
        total = len(records)
        if total == 0:
            return self._empty_report()

        valid = 0
        complete = 0
        consistent = 0
        timely = 0
        error_counter: Counter[str] = Counter()
        seen_ids: Counter[str] = Counter()

        for rec in records:
            # Completeness.
            if all(rec.get(f) is not None for f in self._required):
                complete += 1
            # Validity (full rule set).
            result = self._validator.validate(rec)
            if result.is_valid:
                valid += 1
            else:
                # Capture the *first* reason for the top-error breakdown.
                error_counter[result.error_reason.split(";")[0].strip()] += 1
            # Consistency (price within configured bounds).
            if self._is_consistent(rec):
                consistent += 1
            # Timeliness.
            if self._is_timely(rec):
                timely += 1
            # Uniqueness.
            eid = rec.get("event_id")
            if eid is not None:
                seen_ids[eid] += 1

        duplicates = sum(c - 1 for c in seen_ids.values() if c > 1)
        unique_pct = self._pct(total - duplicates, total)

        dims = DQDimensions(
            completeness=self._pct(complete, total),
            validity=self._pct(valid, total),
            uniqueness=unique_pct,
            timeliness=self._pct(timely, total),
            consistency=self._pct(consistent, total),
        )

        report = DQReport(
            generated_at=datetime.now(timezone.utc).isoformat(),
            pipeline_name=self._config.app.pipeline_name,
            environment=self._config.app.environment,
            total_records=total,
            valid_records=valid,
            invalid_records=total - valid,
            duplicate_records=duplicates,
            dimensions=dims,
            overall_score=dims.overall(),
            top_error_reasons=[
                {"reason": reason, "count": count}
                for reason, count in error_counter.most_common(10)
            ],
        )
        _log.info(
            "DQ report: %d records, overall score %.1f%% (valid=%d, dup=%d)",
            total,
            report.overall_score,
            valid,
            duplicates,
        )
        return report

    # ------------------------------------------------------------------
    def from_dlq_fallback(self) -> Optional[DQReport]:
        """Generate a report from the on-disk DLQ fallback file, if present.

        Returns:
            A :class:`DQReport` summarising dead-lettered records, or ``None``
            if the fallback file does not exist.
        """
        fallback = self._config.resolve_path(
            self._config.logging.log_dir
        ) / "dlq_fallback.jsonl"
        if not fallback.exists():
            _log.info("No DLQ fallback file at %s — skipping DQ-from-DLQ", fallback)
            return None

        originals: List[Dict[str, Any]] = []
        for line in fallback.read_text(encoding="utf-8").splitlines():
            try:
                envelope = json.loads(line)
                originals.append(json.loads(envelope["original_message"]))
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
        return self.generate(originals)

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------
    def write(self, report: DQReport) -> Dict[str, Path]:
        """Write the report to timestamped JSON and HTML files.

        Args:
            report: The report to persist.

        Returns:
            A dict with ``{"json": path, "html": path}``.
        """
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        json_path = self._output_dir / f"dq_report_{stamp}.json"
        html_path = self._output_dir / f"dq_report_{stamp}.html"

        json_path.write_text(
            json.dumps(report.to_dict(), indent=2), encoding="utf-8"
        )
        html_path.write_text(self._render_html(report), encoding="utf-8")
        _log.info("DQ report written: %s | %s", json_path.name, html_path.name)
        return {"json": json_path, "html": html_path}

    def _render_html(self, report: DQReport) -> str:
        """Render a minimal, self-contained HTML scorecard.

        Args:
            report: The report to render.

        Returns:
            An HTML document string.
        """
        d = report.dimensions
        rows = "".join(
            f"<tr><td>{name}</td><td>{value:.1f}%</td></tr>"
            for name, value in [
                ("Completeness", d.completeness),
                ("Validity", d.validity),
                ("Uniqueness", d.uniqueness),
                ("Timeliness", d.timeliness),
                ("Consistency", d.consistency),
            ]
        )
        errors = "".join(
            f"<tr><td>{e['reason']}</td><td>{e['count']}</td></tr>"
            for e in report.top_error_reasons
        ) or "<tr><td colspan='2'>No errors 🎉</td></tr>"
        score_colour = (
            "#2e7d32" if report.overall_score >= 95
            else "#f9a825" if report.overall_score >= 80
            else "#c62828"
        )
        return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Data Quality Report — {report.pipeline_name}</title>
<style>
 body{{font-family:system-ui,Segoe UI,Arial,sans-serif;margin:2rem;color:#222}}
 h1{{margin-bottom:0}} .sub{{color:#666;margin-top:.25rem}}
 .score{{font-size:3rem;font-weight:700;color:{score_colour}}}
 table{{border-collapse:collapse;margin-top:1rem;min-width:320px}}
 td,th{{border:1px solid #ddd;padding:.5rem .75rem;text-align:left}}
 th{{background:#f5f5f5}}
</style></head><body>
<h1>Data Quality Report</h1>
<p class="sub">{report.pipeline_name} · env={report.environment} · {report.generated_at}</p>
<p class="score">{report.overall_score:.1f}%</p>
<p>{report.total_records} records · {report.valid_records} valid ·
   {report.invalid_records} invalid · {report.duplicate_records} duplicates</p>
<h2>Dimensions</h2>
<table><tr><th>Dimension</th><th>Score</th></tr>{rows}</table>
<h2>Top Error Reasons</h2>
<table><tr><th>Reason</th><th>Count</th></tr>{errors}</table>
</body></html>"""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _is_consistent(self, rec: Dict[str, Any]) -> bool:
        """Return True if numeric fields are within configured bounds."""
        rules = self._config.validation.rules
        price = rec.get("price")
        if not isinstance(price, (int, float)) or isinstance(price, bool):
            return False
        return rules.price_min < price <= rules.price_max

    def _is_timely(self, rec: Dict[str, Any]) -> bool:
        """Return True if the event timestamp is within the freshness SLA."""
        raw = rec.get("timestamp")
        if not isinstance(raw, str):
            return False
        try:
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return False
        age = abs((datetime.now(timezone.utc) - ts).total_seconds())
        return age <= max(self._freshness_seconds, 86_400)

    @staticmethod
    def _pct(numerator: int, denominator: int) -> float:
        """Return ``numerator/denominator`` as a rounded percentage."""
        if denominator <= 0:
            return 100.0
        return round((numerator / denominator) * 100.0, 2)

    def _empty_report(self) -> DQReport:
        """Return a neutral report for an empty record set."""
        return DQReport(
            generated_at=datetime.now(timezone.utc).isoformat(),
            pipeline_name=self._config.app.pipeline_name,
            environment=self._config.app.environment,
            total_records=0,
            valid_records=0,
            invalid_records=0,
            duplicate_records=0,
            dimensions=DQDimensions(),
            overall_score=100.0,
        )
