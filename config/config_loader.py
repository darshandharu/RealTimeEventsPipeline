"""Typed configuration loader for the Real-Time Events Analytics Pipeline.

Responsibilities
----------------
1. Read ``config.yaml`` from disk.
2. Resolve ``${ENV_VAR}`` / ``${ENV_VAR:-default}`` placeholders from the
   process environment (after loading ``.env`` if present).
3. Validate and coerce the result into strongly-typed, immutable Pydantic
   models so the rest of the codebase gets autocomplete, type-checking and a
   single fail-fast point for misconfiguration.

Design notes
------------
* The loader is intentionally dependency-light at import time and caches the
  parsed config with :func:`functools.lru_cache`, so repeated ``load_config()``
  calls across modules are effectively free and always return the same object.
* Environment interpolation happens on the *raw text* before YAML parsing,
  which keeps it simple and lets a single placeholder span any scalar type.

Usage
-----
>>> from config import load_config
>>> cfg = load_config()
>>> cfg.kafka.topics.events
'events'
"""

from __future__ import annotations

import functools
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator

try:  # python-dotenv is optional at runtime but listed in requirements.
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - defensive fallback
    load_dotenv = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
#: Project root = parent of the ``config`` package directory.
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

#: Default config file location (overridable via ``RTEP_CONFIG_PATH``).
DEFAULT_CONFIG_PATH: Path = PROJECT_ROOT / "config.yaml"

#: Matches ``${VAR}`` or ``${VAR:-default}`` placeholders inside the raw YAML.
_ENV_PLACEHOLDER: re.Pattern[str] = re.compile(
    r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}"
)


# ===========================================================================
# Typed configuration models
# ===========================================================================
class _Frozen(BaseModel):
    """Base model: immutable, forbids unknown keys to catch typos early."""

    model_config = {"frozen": True, "extra": "forbid"}


class AppConfig(_Frozen):
    """Top-level application metadata."""

    name: str
    pipeline_name: str
    environment: str = "local"
    version: str = "1.0.0"


class _SymbolConfig(_Frozen):
    """A single tracked instrument."""

    symbol: str
    company: str
    exchange: str


class _ApiConfig(_Frozen):
    """Outbound API resilience settings."""

    timeout_seconds: int = 10
    max_retries: int = 3
    backoff_base_seconds: int = 2
    backoff_max_seconds: int = 30


class SourceConfig(_Frozen):
    """Data-source configuration (Yahoo Finance with mock fallback)."""

    type: str = "yahoo_finance"
    use_mock_fallback: bool = True
    force_mock: bool = False
    symbols: List[_SymbolConfig]
    api: _ApiConfig

    @field_validator("type")
    @classmethod
    def _validate_type(cls, value: str) -> str:
        allowed = {"yahoo_finance", "mock_iot"}
        if value not in allowed:
            raise ValueError(f"source.type must be one of {allowed}, got {value!r}")
        return value


class _TopicsConfig(_Frozen):
    events: str
    dead_letter: str


class _KafkaProducerConfig(_Frozen):
    acks: str = "all"
    enable_idempotence: bool = True
    retries: int = 5
    retry_backoff_ms: int = 500
    linger_ms: int = 50
    batch_size: int = 32768
    compression_type: str = "snappy"
    max_in_flight_requests: int = 5
    delivery_timeout_ms: int = 120000


class _KafkaConsumerConfig(_Frozen):
    group_id: str = "rtep-spark-consumer"
    auto_offset_reset: str = "latest"
    max_offsets_per_trigger: int = 5000
    fail_on_data_loss: bool = False


class KafkaConfig(_Frozen):
    """Kafka cluster, topics and client tuning."""

    bootstrap_servers: str
    schema_registry_url: str
    client_id: str = "rtep-producer"
    topics: _TopicsConfig
    producer: _KafkaProducerConfig
    consumer: _KafkaConsumerConfig


class ProducerConfig(_Frozen):
    """Producer emission behaviour."""

    event_interval_seconds: int = 3
    interval_jitter_seconds: int = 2
    events_per_tick: int = 1
    inject_bad_records_pct: int = 0
    graceful_shutdown_timeout_seconds: int = 15

    @field_validator("inject_bad_records_pct")
    @classmethod
    def _validate_pct(cls, value: int) -> int:
        if not 0 <= value <= 100:
            raise ValueError("inject_bad_records_pct must be between 0 and 100")
        return value


class _SparkStreamingConfig(_Frozen):
    starting_offsets: str = "latest"
    trigger_interval: str = "30 seconds"
    watermark_delay: str = "2 minutes"
    window_duration: str = "1 minute"
    window_slide: str = "1 minute"
    checkpoint_location: str = "./checkpoints"
    max_files_per_trigger: int = 1


class SparkConfig(_Frozen):
    """Spark application + Structured Streaming settings."""

    app_name: str = "rtep-structured-streaming"
    master: str = "local[*]"
    log_level: str = "WARN"
    shuffle_partitions: int = 8
    streaming: _SparkStreamingConfig
    packages: List[str] = Field(default_factory=list)


class _ValidationRules(_Frozen):
    price_min: float = 0.0
    price_max: float = 1_000_000.0
    volume_min: int = 0
    change_percent_min: float = -100.0
    change_percent_max: float = 1000.0
    max_timestamp_skew_seconds: int = 300
    allowed_exchanges: List[str]


class ValidationConfig(_Frozen):
    """Data-quality validation rules."""

    required_fields: List[str]
    rules: _ValidationRules
    enable_duplicate_detection: bool = True
    duplicate_window: str = "5 minutes"


class DLQConfig(_Frozen):
    """Dead Letter Queue contract."""

    topic: str
    include_stacktrace: bool = True
    fields: List[str]


class _BigQueryTables(_Frozen):
    raw_events: str
    aggregates: str
    dead_letter: str


class BigQueryConfig(_Frozen):
    """BigQuery sink configuration."""

    project_id: str
    dataset: str
    location: str = "US"
    credentials_path: str
    temporary_gcs_bucket: str
    write_method: str = "direct"
    tables: _BigQueryTables
    partition_field: str = "event_date"
    partition_type: str = "DAY"
    clustering_fields: List[str]
    create_disposition: str = "CREATE_IF_NEEDED"
    write_disposition: str = "WRITE_APPEND"


class SinkConfig(_Frozen):
    """Selects and configures the streaming output sink."""

    type: str = "bigquery"
    local_path: str = "./output"
    console_rows: int = 20

    @field_validator("type")
    @classmethod
    def _validate_type(cls, value: str) -> str:
        allowed = {"bigquery", "console", "parquet"}
        if value not in allowed:
            raise ValueError(f"sink.type must be one of {allowed}, got {value!r}")
        return value


class _LogRotation(_Frozen):
    max_bytes: int = 10_485_760
    backup_count: int = 5


class _LogFiles(_Frozen):
    producer: str = "producer.log"
    consumer: str = "consumer.log"
    pipeline: str = "pipeline.log"


class LoggingConfig(_Frozen):
    """Centralised logging configuration."""

    level: str = "INFO"
    log_dir: str = "./logs"
    format: str = "json"
    rotation: _LogRotation
    files: _LogFiles


class _DQReportConfig(_Frozen):
    enabled: bool = True
    output_dir: str = "./logs/dq_reports"


class _AlertThresholds(_Frozen):
    dlq_rate_pct: float = 10.0
    min_throughput_per_min: int = 5
    max_processing_lag_seconds: int = 120


class _AlertingConfig(_Frozen):
    enabled: bool = True
    channel: str = "log"
    webhook_url: Optional[str] = None
    thresholds: _AlertThresholds


class MonitoringConfig(_Frozen):
    """Observability / alerting (bonus features)."""

    enable_prometheus: bool = True
    prometheus_port: int = 8000
    metrics_interval_seconds: int = 30
    data_quality_report: _DQReportConfig
    alerting: _AlertingConfig


class Config(_Frozen):
    """Root configuration aggregating every section of ``config.yaml``."""

    app: AppConfig
    source: SourceConfig
    kafka: KafkaConfig
    producer: ProducerConfig
    spark: SparkConfig
    validation: ValidationConfig
    dlq: DLQConfig
    bigquery: BigQueryConfig
    sink: SinkConfig
    logging: LoggingConfig
    monitoring: MonitoringConfig

    # -- convenience helpers -------------------------------------------------
    def resolve_path(self, value: str) -> Path:
        """Resolve a possibly-relative config path against the project root.

        Args:
            value: A path string from the config (e.g. ``./checkpoints``).

        Returns:
            An absolute :class:`pathlib.Path`.
        """
        path = Path(value)
        return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()


# ===========================================================================
# Loading / interpolation logic
# ===========================================================================
def _interpolate_env(raw_text: str) -> str:
    """Replace ``${VAR}`` / ``${VAR:-default}`` placeholders with env values.

    Full-line YAML comments (lines whose first non-whitespace character is
    ``#``) are skipped, so documentation that *mentions* the placeholder syntax
    (e.g. ``${ENV_VAR}`` in the file header) is not mistaken for a real value to
    resolve.

    Args:
        raw_text: The raw YAML file contents.

    Returns:
        The text with every placeholder resolved.

    Raises:
        KeyError: If a placeholder has no environment value and no default.
    """

    def _replace(match: re.Match[str]) -> str:
        name = match.group("name")
        default = match.group("default")
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise KeyError(
            f"Environment variable {name!r} is referenced in config.yaml but "
            f"is not set and has no default (use ${{{name}:-default}})."
        )

    # Process line-by-line so we can leave comment lines untouched.
    out_lines = []
    for line in raw_text.splitlines(keepends=True):
        if line.lstrip().startswith("#"):
            out_lines.append(line)  # documentation, not a value
        else:
            out_lines.append(_ENV_PLACEHOLDER.sub(_replace, line))
    return "".join(out_lines)


def _load_dotenv_if_present() -> None:
    """Load a ``.env`` file from the project root, if one exists."""
    if load_dotenv is None:
        return
    dotenv_path = PROJECT_ROOT / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path, override=False)


@functools.lru_cache(maxsize=1)
def load_config(config_path: Optional[str] = None) -> Config:
    """Load, interpolate and validate the pipeline configuration.

    The result is cached; subsequent calls (with the same path) return the
    same immutable :class:`Config` instance.

    Args:
        config_path: Optional explicit path to a YAML config. Falls back to
            the ``RTEP_CONFIG_PATH`` env var, then :data:`DEFAULT_CONFIG_PATH`.

    Returns:
        A fully-populated, validated :class:`Config`.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the YAML is malformed or fails schema validation.
    """
    _load_dotenv_if_present()

    path = Path(
        config_path or os.environ.get("RTEP_CONFIG_PATH") or DEFAULT_CONFIG_PATH
    )
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    raw_text = path.read_text(encoding="utf-8")
    interpolated = _interpolate_env(raw_text)

    try:
        data: Dict[str, Any] = yaml.safe_load(interpolated) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - surfaced to caller
        raise ValueError(f"Failed to parse YAML config at {path}: {exc}") from exc

    return Config(**data)


def reload_config(config_path: Optional[str] = None) -> Config:
    """Clear the cache and reload the configuration (useful in tests).

    Args:
        config_path: Optional explicit config path forwarded to
            :func:`load_config`.

    Returns:
        A freshly-parsed :class:`Config`.
    """
    load_config.cache_clear()
    return load_config(config_path)


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    cfg = load_config()
    print(f"Loaded config for app: {cfg.app.name} (env={cfg.app.environment})")
    print(f"Kafka brokers       : {cfg.kafka.bootstrap_servers}")
    print(f"Events topic        : {cfg.kafka.topics.events}")
    print(f"DLQ topic           : {cfg.kafka.topics.dead_letter}")
    print(f"BigQuery dataset    : {cfg.bigquery.project_id}.{cfg.bigquery.dataset}")
    print(f"Tracked symbols     : {[s.symbol for s in cfg.source.symbols]}")
