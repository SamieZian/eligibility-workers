"""Liveness + readiness endpoints. Services register their own deps checks."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse


def make_health_router(
    readiness_checks: dict[str, Callable[[], Awaitable[Any]]] | None = None,
    *,
    is_ready: Callable[[], bool] | None = None,
) -> APIRouter:
    """Build /livez + /readyz.

    `is_ready` is a cheap flag the app can flip on SIGTERM so the load
    balancer stops routing traffic before pool drain starts; 503 from /readyz
    is the standard signal for that.
    """
    router = APIRouter()

    @router.get("/livez", include_in_schema=False)
    async def livez() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/readyz", include_in_schema=False)
    async def readyz() -> JSONResponse:
        if is_ready is not None and not is_ready():
            return JSONResponse({"status": "draining"}, status_code=503)
        if not readiness_checks:
            return JSONResponse({"status": "ok"})
        results: dict[str, str] = {}
        ok = True
        for name, fn in readiness_checks.items():
            try:
                await fn()
                results[name] = "ok"
            except Exception as e:
                ok = False
                results[name] = f"fail: {e}"
        return JSONResponse(results, status_code=200 if ok else 503)

    return router
