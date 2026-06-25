"""Reusable retry / backoff utilities for the pipeline.

Provides a single decorator, :func:`retry_with_backoff`, used wherever the
pipeline talks to an unreliable boundary: the Yahoo Finance API, Kafka
connect/produce calls, and BigQuery client operations.

Why a hand-rolled decorator when ``tenacity`` is in requirements?
----------------------------------------------------------------
``tenacity`` is excellent and used in higher-level orchestration, but this
lightweight, dependency-free decorator keeps the *hot path* (per-event produce
retries) free of extra object allocation, gives us exact control over jitter
and structured logging, and remains importable in minimal environments (e.g.
Spark executors) where we may not want the full tenacity stack.

Features
--------
* Exponential backoff with configurable base, cap and full jitter.
* Selective retry on a tuple of exception types.
* Optional ``on_retry`` callback for metrics/alerting hooks.
* Preserves the wrapped function's signature, name and docstring.
* Raises :class:`RetryError` (chaining the last cause) when attempts exhaust.
"""

from __future__ import annotations

import functools
import random
import time
from typing import Any, Callable, Optional, Tuple, Type, TypeVar

from utils.logger import get_logger

_log = get_logger(__name__, component="pipeline")

# Generic return type so the decorator is transparent to type-checkers.
F = TypeVar("F", bound=Callable[..., Any])


class RetryError(RuntimeError):
    """Raised when all retry attempts are exhausted.

    Attributes:
        attempts: Number of attempts that were made.
        last_exception: The final exception that caused the failure.
    """

    def __init__(self, attempts: int, last_exception: BaseException) -> None:
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(
            f"Operation failed after {attempts} attempt(s): "
            f"{type(last_exception).__name__}: {last_exception}"
        )


def _compute_delay(
    attempt: int,
    base_seconds: float,
    max_seconds: float,
    jitter: bool,
) -> float:
    """Compute the backoff delay for a given attempt number.

    Uses exponential growth ``base * 2**(attempt-1)`` capped at ``max_seconds``.
    When ``jitter`` is enabled, applies *full jitter*
    (``random.uniform(0, delay)``) to avoid thundering-herd retry storms.

    Args:
        attempt: 1-based attempt number that just failed.
        base_seconds: Base backoff in seconds.
        max_seconds: Upper bound for any single delay.
        jitter: Whether to apply full jitter.

    Returns:
        The delay, in seconds, to sleep before the next attempt.
    """
    raw = base_seconds * (2 ** (attempt - 1))
    capped = min(raw, max_seconds)
    if jitter:
        return random.uniform(0, capped)
    return capped


def retry_with_backoff(
    max_attempts: int = 3,
    base_seconds: float = 2.0,
    max_seconds: float = 30.0,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    jitter: bool = True,
    on_retry: Optional[Callable[[int, BaseException, float], None]] = None,
) -> Callable[[F], F]:
    """Decorator that retries a callable with exponential backoff + jitter.

    Args:
        max_attempts: Maximum number of attempts (>= 1). ``1`` means no retry.
        base_seconds: Base delay for the exponential schedule.
        max_seconds: Maximum delay for any single backoff.
        exceptions: Exception types that trigger a retry. Anything outside this
            tuple propagates immediately (fail-fast on programmer errors).
        jitter: Apply full jitter to spread out concurrent retries.
        on_retry: Optional callback invoked as
            ``on_retry(attempt, exception, sleep_seconds)`` before each sleep —
            handy for incrementing a metrics counter or emitting an alert.

    Returns:
        A decorator that wraps the target callable.

    Raises:
        ValueError: If ``max_attempts`` is less than 1.
        RetryError: From the wrapped call, when all attempts are exhausted.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Optional[BaseException] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:  # type: ignore[misc]
                    last_exc = exc
                    if attempt == max_attempts:
                        _log.error(
                            "Call to %s failed permanently after %d attempts: %s",
                            func.__qualname__,
                            attempt,
                            exc,
                        )
                        break

                    sleep_for = _compute_delay(
                        attempt, base_seconds, max_seconds, jitter
                    )
                    _log.warning(
                        "Call to %s failed (attempt %d/%d): %s — retrying in %.2fs",
                        func.__qualname__,
                        attempt,
                        max_attempts,
                        exc,
                        sleep_for,
                    )
                    if on_retry is not None:
                        try:
                            on_retry(attempt, exc, sleep_for)
                        except Exception:  # pragma: no cover - hook must not break flow
                            _log.debug("on_retry callback raised; ignoring")
                    time.sleep(sleep_for)

            # All attempts exhausted.
            assert last_exc is not None  # for type-checkers; loop guarantees it
            raise RetryError(max_attempts, last_exc) from last_exc

        return wrapper  # type: ignore[return-value]

    return decorator


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    attempts_seen = {"n": 0}

    @retry_with_backoff(max_attempts=4, base_seconds=0.1, max_seconds=1.0)
    def flaky() -> str:
        attempts_seen["n"] += 1
        if attempts_seen["n"] < 3:
            raise ConnectionError("simulated transient failure")
        return "success"

    print(f"Result: {flaky()} (after {attempts_seen['n']} attempts)")
