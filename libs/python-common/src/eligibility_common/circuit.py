"""Minimal async circuit breaker. Half-open with single probe.

Not a full resilience library — just enough to stop cascading failure.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from .errors import InfraError, Codes

T = TypeVar("T")


@dataclass
class BreakerState:
    failures: int = 0
    opened_at: float = 0.0
    half_open: bool = False


class CircuitBreaker:
    def __init__(
        self,
        *,
        name: str,
        failure_threshold: int = 5,
        reset_after: float = 30.0,
        window: float = 10.0,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_after = reset_after
        self.window = window
        self._state = BreakerState()
        self._lock = asyncio.Lock()
        self._window_start = time.monotonic()

    async def call(self, fn: Callable[[], Awaitable[T]]) -> T:
        async with self._lock:
            now = time.monotonic()
            # time window rolls — failures older than `window` don't count
            if now - self._window_start > self.window:
                self._window_start = now
                self._state.failures = 0

            # open state — reject until reset_after
            if self._state.opened_at and not self._state.half_open:
                if now - self._state.opened_at >= self.reset_after:
                    self._state.half_open = True  # let one probe through
                else:
                    raise InfraError(Codes.DOWNSTREAM_UNAVAILABLE, f"circuit {self.name} open")

        try:
            result = await fn()
        except Exception:
            async with self._lock:
                self._state.failures += 1
                if self._state.half_open:
                    # probe failed — stay open
                    self._state.half_open = False
                    self._state.opened_at = time.monotonic()
                elif self._state.failures >= self.failure_threshold:
                    self._state.opened_at = time.monotonic()
            raise
        else:
            async with self._lock:
                if self._state.half_open:
                    # probe succeeded — close the circuit
                    self._state = BreakerState()
                else:
                    self._state.failures = 0
            return result
