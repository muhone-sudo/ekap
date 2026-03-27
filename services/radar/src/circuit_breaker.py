"""Circuit Breaker for EKAP scraper (handles 403 errors)."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Coroutine, Optional, TypeVar

from shared.logger import get_logger

log = get_logger(__name__)

SLEEP_SECONDS = 15 * 60  # 15 minutes

T = TypeVar("T")


class ScraperBlockedException(Exception):
    """Raised when the scraper receives a 403 / bot-detection response."""


@dataclass
class CircuitBreakerState:
    failure_count: int = 0
    total_requests: int = 0
    open_until: Optional[datetime] = None
    threshold: float = 0.15
    _open: bool = field(default=False, init=False)

    def record_failure(self) -> None:
        self.failure_count += 1

    def record_success(self) -> None:
        pass  # failures don't reset between windows in this simple model

    def record_request(self) -> None:
        self.total_requests += 1

    @property
    def error_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.failure_count / self.total_requests

    def should_open(self) -> bool:
        return self.error_rate > self.threshold

    def is_open(self) -> bool:
        if self.open_until is None:
            return False
        return datetime.now(timezone.utc) < self.open_until

    def trip(self) -> None:
        self.open_until = datetime.now(timezone.utc) + timedelta(seconds=SLEEP_SECONDS)

    def reset_counters(self) -> None:
        self.failure_count = 0
        self.total_requests = 0
        self.open_until = None


class CircuitBreaker:
    """Async-compatible circuit breaker.

    Opens after error rate exceeds *threshold* and sleeps for 15 minutes.
    """

    def __init__(self, threshold: float = 0.15) -> None:
        self.state = CircuitBreakerState(threshold=threshold)

    async def call(self, coro: Coroutine[Any, Any, T]) -> T:
        """Execute *coro* with circuit-breaker protection."""
        if self.state.is_open():
            remaining = (
                self.state.open_until - datetime.now(timezone.utc)  # type: ignore[operator]
            ).total_seconds()
            log.warning("circuit_breaker_open", sleep_seconds=remaining)
            await asyncio.sleep(remaining)
            self.state.reset_counters()

        self.state.record_request()
        try:
            result = await coro
            self.state.record_success()
            return result
        except ScraperBlockedException:
            self.state.record_failure()
            if self.state.should_open():
                self.state.trip()
                log.error(
                    "circuit_breaker_tripped",
                    error_rate=self.state.error_rate,
                    sleep_seconds=SLEEP_SECONDS,
                )
            raise
