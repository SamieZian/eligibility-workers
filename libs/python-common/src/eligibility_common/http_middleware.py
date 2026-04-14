"""FastAPI middleware: correlation-id propagation + typed error envelope.

Mount with `app.middleware("http")(request_id_middleware)` and register the
exception handler via `register_exception_handlers(app)`.
"""
from __future__ import annotations

import uuid
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from .errors import AppError
from .logging import bind_context, get_logger

log = get_logger(__name__)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        cid = request.headers.get("X-Correlation-Id") or str(uuid.uuid4())
        tenant_id = request.headers.get("X-Tenant-Id")
        request.state.correlation_id = cid
        request.state.tenant_id = tenant_id
        with bind_context(correlation_id=cid, tenant_id=tenant_id, path=request.url.path, method=request.method):
            response = await call_next(request)
        response.headers["X-Correlation-Id"] = cid
        return response


def _envelope(err: AppError, cid: str) -> dict[str, Any]:
    return {
        "error": {
            "code": err.code,
            "message": err.message,
            "correlation_id": cid,
            "retryable": err.retryable,
            "details": err.details,
        }
    }


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _app_error(request: Request, exc: AppError) -> JSONResponse:
        cid = getattr(request.state, "correlation_id", "")
        log.warning("app.error", code=exc.code, message=exc.message)
        return JSONResponse(status_code=exc.http_status, content=_envelope(exc, cid))

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        from .errors import InfraError

        cid = getattr(request.state, "correlation_id", "")
        log.exception("unhandled", error=str(exc))
        wrapped = InfraError("UNHANDLED", "Internal server error")
        return JSONResponse(status_code=500, content=_envelope(wrapped, cid))
