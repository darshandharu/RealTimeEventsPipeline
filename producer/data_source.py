"""Market data source for the producer.

Provides :class:`MarketDataSource`, which yields stock-market events in the
canonical wire format. It prefers **live Yahoo Finance** quotes and transparently
falls back to a **realistic mock generator** when the API is unreachable or
``source.use_mock_fallback`` is enabled — so the pipeline always has a heartbeat,
which is essential for an offline demo or CI run.

It can also deliberately emit a configurable percentage of *corrupted* records
(``producer.inject_bad_records_pct``) so the downstream validation layer and
Dead Letter Queue are visibly exercised end-to-end.

Resilience
----------
Live fetches are wrapped with :func:`utils.retry.retry_with_backoff` (exponential
backoff + jitter) and a per-symbol price cache, so a transient API hiccup degrades
gracefully to the last-known price / mock rather than crashing the producer.
"""

from __future__ import annotations

import abc
import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config.config_loader import Config
from schemas.event_schema import event_to_dict
from utils.logger import get_logger
from utils.retry import RetryError, retry_with_backoff

try:  # yfinance is optional; absence simply forces the mock path.
    import yfinance as yf
except ImportError:  # pragma: no cover - mock-only environments
    yf = None  # type: ignore[assignment]

_log = get_logger(__name__, component="producer")


class DataSource(abc.ABC):
    """Abstract base for any event source (open/closed for new sources)."""

    @abc.abstractmethod
    def generate_events(self) -> List[Dict[str, Any]]:
        """Produce a batch of events for the current tick.

        Returns:
            A list of event dicts in the canonical wire format.
        """
        raise NotImplementedError


class MarketDataSource(DataSource):
    """Yahoo Finance-backed market data source with mock fallback.

    Args:
        config: The fully-loaded pipeline :class:`Config`.

    Attributes:
        symbols: The list of tracked instruments (symbol/company/exchange).
        use_mock_fallback: Whether to fall back to mock data on API failure.
        inject_bad_pct: Percentage of records to deliberately corrupt.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self.symbols = config.source.symbols
        self.use_mock_fallback = config.source.use_mock_fallback
        self.inject_bad_pct = config.producer.inject_bad_records_pct
        self._events_per_tick = config.producer.events_per_tick

        # Seed a plausible last-price per symbol so the mock generator produces
        # a coherent random walk rather than disconnected noise.
        self._last_price: Dict[str, float] = {
            s.symbol: round(random.uniform(50.0, 500.0), 2) for s in self.symbols
        }
        self._source_mode = (
            "yahoo_finance"
            if (
                config.source.type == "yahoo_finance"
                and yf is not None
                and not config.source.force_mock
            )
            else "mock"
        )
        _log.info(
            "MarketDataSource initialised (mode=%s, symbols=%d, bad_pct=%d)",
            self._source_mode,
            len(self.symbols),
            self.inject_bad_pct,
        )

    # -- public API ---------------------------------------------------------
    def generate_events(self) -> List[Dict[str, Any]]:
        """Generate this tick's batch of events across all tracked symbols.

        Returns:
            A list of event dicts (some possibly corrupted by design).
        """
        events: List[Dict[str, Any]] = []
        for instrument in self.symbols:
            for _ in range(self._events_per_tick):
                event = self._build_event(
                    symbol=instrument.symbol,
                    company=instrument.company,
                    exchange=instrument.exchange,
                )
                events.append(self._maybe_corrupt(event))
        return events

    # -- event construction -------------------------------------------------
    def _build_event(
        self, symbol: str, company: str, exchange: str
    ) -> Dict[str, Any]:
        """Build a single well-formed event for one symbol.

        Tries a live quote first (when in yahoo_finance mode); on any failure
        and with fallback enabled, synthesises a realistic mock event.

        Args:
            symbol: Ticker symbol.
            company: Company name.
            exchange: Exchange code.

        Returns:
            A canonical event dict.
        """
        price: Optional[float] = None
        if self._source_mode == "yahoo_finance":
            try:
                price = self._fetch_live_price(symbol)
            except (RetryError, Exception) as exc:  # noqa: BLE001 - degrade gracefully
                if not self.use_mock_fallback:
                    raise
                _log.warning(
                    "Live fetch failed for %s (%s); using mock fallback",
                    symbol,
                    exc,
                )

        if price is None:
            price = self._mock_price(symbol)

        previous = self._last_price.get(symbol, price)
        change = round(price - previous, 4)
        change_percent = round((change / previous * 100.0) if previous else 0.0, 4)
        self._last_price[symbol] = price

        return event_to_dict(
            event_id=str(uuid.uuid4()),
            symbol=symbol,
            company=company,
            price=round(price, 4),
            volume=random.randint(1_000, 5_000_000),
            change=change,
            change_percent=change_percent,
            timestamp=datetime.now(timezone.utc).isoformat(),
            exchange=exchange,
        )

    @retry_with_backoff(max_attempts=3, base_seconds=1.0, max_seconds=8.0)
    def _fetch_live_price(self, symbol: str) -> float:
        """Fetch the latest price for a symbol from Yahoo Finance.

        Decorated with exponential-backoff retry so transient network errors
        are absorbed before the mock fallback engages.

        Args:
            symbol: Ticker symbol.

        Returns:
            The most recent available price as a float.

        Raises:
            ValueError: If yfinance returns no usable price data.
            RuntimeError: If the yfinance library is unavailable.
        """
        if yf is None:  # pragma: no cover - guarded by _source_mode
            raise RuntimeError("yfinance is not installed")

        ticker = yf.Ticker(symbol)
        # `fast_info` is the low-latency path; fall back to recent history.
        last = getattr(ticker, "fast_info", {}).get("last_price")  # type: ignore[attr-defined]
        if last is None:
            hist = ticker.history(period="1d", interval="1m")
            if hist is None or hist.empty:
                raise ValueError(f"No price data returned for {symbol}")
            last = float(hist["Close"].iloc[-1])
        return float(last)

    # -- mock generation ----------------------------------------------------
    def _mock_price(self, symbol: str) -> float:
        """Generate the next mock price via a bounded random walk.

        Args:
            symbol: Ticker symbol (used to anchor the walk).

        Returns:
            A new positive price.
        """
        previous = self._last_price.get(symbol, 100.0)
        # +/- up to 2% drift per tick, floored so price never goes non-positive.
        drift = previous * random.uniform(-0.02, 0.02)
        return max(round(previous + drift, 4), 1.0)

    # -- fault injection ----------------------------------------------------
    def _maybe_corrupt(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Randomly corrupt an event to exercise the validation/DLQ path.

        Corruption strategies are chosen uniformly and each maps to a distinct
        validation failure mode (negative price, missing field, bad timestamp,
        wrong datatype) so the DLQ surfaces a variety of ``error_reason`` values.

        Args:
            event: A well-formed event dict.

        Returns:
            Either the original event or a deliberately corrupted copy.
        """
        if self.inject_bad_pct <= 0 or random.randint(1, 100) > self.inject_bad_pct:
            return event

        corrupt = dict(event)
        strategy = random.choice(
            ["negative_price", "missing_field", "bad_timestamp", "wrong_type", "null_symbol"]
        )
        if strategy == "negative_price":
            corrupt["price"] = round(-abs(event["price"]), 4)
        elif strategy == "missing_field":
            corrupt.pop("volume", None)
        elif strategy == "bad_timestamp":
            corrupt["timestamp"] = "not-a-real-timestamp"
        elif strategy == "wrong_type":
            corrupt["price"] = "NaN-dollars"  # string where double expected
        elif strategy == "null_symbol":
            corrupt["symbol"] = None

        _log.debug("Injected corrupt record (%s) for %s", strategy, event["symbol"])
        return corrupt


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    from config.config_loader import load_config

    src = MarketDataSource(load_config())
    batch = src.generate_events()
    print(f"Generated {len(batch)} events. Sample:")
    for ev in batch[:3]:
        print(f"  {ev['symbol']:<6} price={ev.get('price')} ts={ev.get('timestamp')}")
