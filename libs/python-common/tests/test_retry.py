import asyncio

import pytest

from eligibility_common.errors import InfraError
from eligibility_common.retry import retry_async, retry_sync


@pytest.mark.asyncio
async def test_async_retry_succeeds_after_transient() -> None:
    calls = {"n": 0}

    async def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise InfraError("X", "transient")
        return "ok"

    result = await retry_async(flaky, attempts=5, base=0.001, cap=0.01)
    assert result == "ok"
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_async_retry_exhausts() -> None:
    async def always_fail() -> None:
        raise InfraError("X", "boom")

    with pytest.raises(InfraError):
        await retry_async(always_fail, attempts=3, base=0.001, cap=0.01)


def test_sync_retry_succeeds() -> None:
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 2:
            raise ConnectionError("transient")
        return "ok"

    assert retry_sync(flaky, attempts=3, base=0.001, cap=0.01) == "ok"


@pytest.mark.asyncio
async def test_async_retry_does_not_retry_non_allowed() -> None:
    async def other() -> None:
        raise ValueError("not allowed")

    with pytest.raises(ValueError):
        await retry_async(other, attempts=3, base=0.001, cap=0.01, retry_on=(InfraError,))
