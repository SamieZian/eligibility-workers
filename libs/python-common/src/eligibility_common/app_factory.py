"""One-liner FastAPI app factory so every service is consistent."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from .health import make_health_router
from .http_middleware import CorrelationIdMiddleware, register_exception_handlers
from .logging import configure_logging
from .tracing import init_tracing


def create_app(
    *,
    service_name: str,
    version: str = "0.1.0",
    lifespan: Callable[[FastAPI], Any] | None = None,
    readiness: dict[str, Callable[[], Awaitable[Any]]] | None = None,
) -> FastAPI:
    configure_logging(service_name=service_name)
    init_tracing(service_name)
    app = FastAPI(title=service_name, version=version, lifespan=lifespan)
    app.add_middleware(CorrelationIdMiddleware)
    register_exception_handlers(app)
    app.include_router(make_health_router(readiness))
    FastAPIInstrumentor.instrument_app(app)
    return app
