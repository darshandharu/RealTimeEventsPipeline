"""Centralised logging factory for the Real-Time Events Analytics Pipeline.

Every component obtains its logger via :func:`get_logger`, which guarantees:

* A single, idempotent configuration per logger name (no duplicate handlers
  when modules are re-imported, e.g. under Spark executors or pytest).
* **Dual output**: human-readable console + machine-parseable rotating file.
* **Structured JSON logs** on disk (when ``logging.format: json``) so the files
  can be shipped to ELK / Cloud Logging / Loki without re-parsing.
* Component-scoped files: ``producer.log``, ``consumer.log``, ``pipeline.log``
  (requirement #12), with size-based rotation and backups.

Example
-------
>>> from utils.logger import get_logger
>>> log = get_logger("producer", component="producer")
>>> log.info("producer started", extra={"symbols": 10})
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional

try:
    from pythonjsonlogger import jsonlogger
except ImportError:  # pragma: no cover - defensive
    jsonlogger = None  # type: ignore[assignment]

from config.config_loader import PROJECT_ROOT, load_config

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------
#: Maps a logical "component" to its dedicated log file, per config.yaml.
_COMPONENT_FILES: Dict[str, str] = {
    "producer": "producer.log",
    "consumer": "consumer.log",
    "pipeline": "pipeline.log",
}

#: Tracks which logger names we've already configured to keep `get_logger`
#: idempotent across repeated imports / executor restarts.
_CONFIGURED: set[str] = set()

_CONSOLE_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s | "
    "%(filename)s:%(lineno)d | %(message)s"
)
_JSON_FORMAT = (
    "%(asctime)s %(levelname)s %(name)s %(filename)s %(lineno)d "
    "%(funcName)s %(message)s"
)


def _build_file_formatter(fmt: str) -> logging.Formatter:
    """Return a JSON formatter when available/configured, else a plain one.

    Args:
        fmt: Either ``"json"`` or ``"plain"`` (from ``logging.format``).

    Returns:
        A configured :class:`logging.Formatter`.
    """
    if fmt == "json" and jsonlogger is not None:
        return jsonlogger.JsonFormatter(
            _JSON_FORMAT,
            rename_fields={"asctime": "timestamp", "levelname": "level"},
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    return logging.Formatter(_CONSOLE_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S%z")


def _resolve_log_dir(log_dir: str) -> Path:
    """Resolve and create the log directory.

    Args:
        log_dir: Directory path from config (possibly relative).

    Returns:
        The absolute, created log directory path.
    """
    path = Path(log_dir)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_logger(
    name: str,
    component: Optional[str] = None,
    level: Optional[str] = None,
) -> logging.Logger:
    """Get (or lazily configure) a logger for the given name.

    The logger writes to stdout *and* to a rotating component log file. Calling
    this repeatedly with the same ``name`` returns the same fully-configured
    logger without attaching duplicate handlers.

    Args:
        name: Logger name, typically ``__name__`` of the calling module.
        component: One of ``"producer"``, ``"consumer"`` or ``"pipeline"``.
            Determines which log file receives the records. Defaults to
            ``"pipeline"`` when omitted or unknown.
        level: Optional explicit level override (e.g. ``"DEBUG"``). Falls back
            to ``logging.level`` from config.

    Returns:
        A ready-to-use :class:`logging.Logger`.
    """
    logger = logging.getLogger(name)

    # Idempotency guard: configure each logger name exactly once.
    if name in _CONFIGURED:
        return logger

    try:
        cfg = load_config()
        log_cfg = cfg.logging
        configured_level = level or log_cfg.level
        log_dir = _resolve_log_dir(log_cfg.log_dir)
        fmt = log_cfg.format
        max_bytes = log_cfg.rotation.max_bytes
        backup_count = log_cfg.rotation.backup_count
    except Exception:  # pragma: no cover - bootstrap before config exists
        # Fallback so logging never crashes the app (e.g. during early import
        # or in unit tests without a config file).
        configured_level = level or "INFO"
        log_dir = _resolve_log_dir("./logs")
        fmt = "plain"
        max_bytes = 10_485_760
        backup_count = 5

    resolved_component = component if component in _COMPONENT_FILES else "pipeline"
    log_file = log_dir / _COMPONENT_FILES[resolved_component]

    logger.setLevel(configured_level.upper())
    logger.propagate = False  # avoid double-logging via the root logger

    # -- Console handler (human-readable) -----------------------------------
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(
        logging.Formatter(_CONSOLE_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S%z")
    )
    console_handler.setLevel(configured_level.upper())
    logger.addHandler(console_handler)

    # -- Rotating file handler (structured) ---------------------------------
    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(_build_file_formatter(fmt))
    file_handler.setLevel(configured_level.upper())
    logger.addHandler(file_handler)

    _CONFIGURED.add(name)
    logger.debug(
        "Logger initialised",
        extra={"component": resolved_component, "log_file": str(log_file)},
    )
    return logger


def reset_logging() -> None:
    """Tear down all configured handlers (primarily for test isolation)."""
    for name in list(_CONFIGURED):
        logger = logging.getLogger(name)
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)
    _CONFIGURED.clear()


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    for comp in ("producer", "consumer", "pipeline"):
        log = get_logger(f"smoke.{comp}", component=comp)
        log.info("hello from %s", comp, extra={"demo": True})
        log.warning("a warning on %s", comp)
    print("Wrote sample records to logs/{producer,consumer,pipeline}.log")
