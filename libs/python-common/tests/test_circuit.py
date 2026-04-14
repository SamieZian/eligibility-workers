import asyncio

import pytest

from eligibility_common.circuit import CircuitBreaker
from eligibility_common.errors import InfraError


@pytest.mark.asyncio
async def test_opens_after_threshold() -> None:
    cb = CircuitBreaker(name="t", failure_threshold=3, reset_after=0.05, window=5)

    async def fail() -> None:
        raise RuntimeError("boom")

    for _ in range(3):
        with pytest.raises(RuntimeError):
            await cb.call(fail)
    # next call fails fast as InfraError
    with pytest.raises(InfraError):
        await cb.call(fail)


@pytest.mark.asyncio
async def test_half_open_then_close_on_success() -> None:
    cb = CircuitBreaker(name="t", failure_threshold=1, reset_after=0.02, window=5)

    async def fail() -> None:
        raise RuntimeError("boom")

    async def ok() -> str:
        return "ok"

    with pytest.raises(RuntimeError):
        await cb.call(fail)
    with pytest.raises(InfraError):
        await cb.call(fail)
    await asyncio.sleep(0.03)
    assert await cb.call(ok) == "ok"
    # closed again
    assert await cb.call(ok) == "ok"
