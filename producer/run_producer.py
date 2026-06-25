"""CLI entrypoint for the Kafka event producer.

Wires together configuration, the market data source and the resilient
:class:`~producer.producer.EventProducer`, then runs the produce loop until a
shutdown signal (or an optional tick/duration cap) is reached.

Run locally::

    python -m producer.run_producer
    python -m producer.run_producer --max-ticks 20        # bounded run (CI/demo)
    python -m producer.run_producer --bad-pct 25          # stress the DLQ path
    python -m producer.run_producer --config ./config.yaml

Inside Docker this is the container's default command (see
``docker/Dockerfile.producer``).
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from config.config_loader import load_config, reload_config
from producer.data_source import MarketDataSource
from producer.producer import EventProducer
from utils.logger import get_logger

_log = get_logger("producer.run", component="producer")


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Optional explicit argument list (defaults to ``sys.argv``).

    Returns:
        The parsed :class:`argparse.Namespace`.
    """
    parser = argparse.ArgumentParser(
        prog="run_producer",
        description="Real-Time Events Analytics Pipeline — Kafka event producer.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config.yaml (defaults to project root / RTEP_CONFIG_PATH).",
    )
    parser.add_argument(
        "--max-ticks",
        type=int,
        default=None,
        help="Stop after N produce ticks (useful for CI / bounded demos).",
    )
    parser.add_argument(
        "--bad-pct",
        type=int,
        default=None,
        metavar="0-100",
        help="Override producer.inject_bad_records_pct to stress the DLQ path.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override the configured log level.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """Program entrypoint.

    Args:
        argv: Optional explicit argument list (for tests).

    Returns:
        Process exit code (0 = clean shutdown, 1 = fatal error).
    """
    args = _parse_args(argv)

    # Load config (reload so an explicit --config path bypasses any cache).
    config = reload_config(args.config) if args.config else load_config()

    # Apply CLI overrides on an immutable config via a validated copy.
    overrides: dict = {}
    if args.bad_pct is not None:
        overrides["producer"] = config.producer.model_copy(
            update={"inject_bad_records_pct": args.bad_pct}
        )
    if overrides:
        config = config.model_copy(update=overrides)

    if args.log_level:
        _log.setLevel(args.log_level)

    _log.info(
        "Starting producer | env=%s brokers=%s topic=%s symbols=%d",
        config.app.environment,
        config.kafka.bootstrap_servers,
        config.kafka.topics.events,
        len(config.source.symbols),
    )

    try:
        data_source = MarketDataSource(config)
        producer = EventProducer(config=config, data_source=data_source)
        producer.run(max_ticks=args.max_ticks)
        return 0
    except KeyboardInterrupt:
        _log.info("Interrupted by user — exiting cleanly")
        return 0
    except Exception as exc:  # noqa: BLE001 - top-level fatal handler
        _log.exception("Fatal error in producer: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
