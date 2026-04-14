"""Exponential-backoff-with-full-jitter retry helpers.

No silent infinite retries — every caller specifies `attempts` and `cap`.
"""
from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

from .errors import InfraError
from .logging import get_logger

T = TypeVar("T")
log = get_logger(__name__)


def _jittered(base: float, attempt: int, cap: float) -> float:
    # Full jitter per AWS Arch: sleep = random(0, min(cap, base * 2**attempt))
    return random.uniform(0, min(cap, base * (2**attempt)))


async def retry_async(
    fn: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    base: float = 0.05,
    cap: float = 2.0,
    retry_on: tuple[type[BaseException], ...] = (InfraError, ConnectionError, TimeoutError),
    op: str = "op",
) -> T:
    last: BaseException | None = None
    for i in range(attempts):
        try:
            return await fn()
        except retry_on as e:
            last = e
            if i == attempts - 1:
                break
            delay = _jittered(base, i, cap)
            log.warning("retry", op=op, attempt=i + 1, delay=round(delay, 3), error=str(e))
            await asyncio.sleep(delay)
    assert last is not None
    raise last


def retry_sync(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    base: float = 0.05,
    cap: float = 2.0,
    retry_on: tuple[type[BaseException], ...] = (ConnectionError, TimeoutError),
    op: str = "op",
) -> T:
    import time

    last: BaseException | None = None
    for i in range(attempts):
        try:
            return fn()
        except retry_on as e:
            last = e
            if i == attempts - 1:
                break
            delay = _jittered(base, i, cap)
            log.warning("retry", op=op, attempt=i + 1, delay=round(delay, 3), error=str(e))
            time.sleep(delay)
    assert last is not None
    raise last
