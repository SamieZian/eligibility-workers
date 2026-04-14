"""Liveness + readiness endpoints. Services register their own deps checks."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse


def make_health_router(
    readiness_checks: dict[str, Callable[[], Awaitable[Any]]] | None = None,
) -> APIRouter:
    router = APIRouter()

    @router.get("/livez", include_in_schema=False)
    async def livez() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/readyz", include_in_schema=False)
    async def readyz() -> JSONResponse:
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
