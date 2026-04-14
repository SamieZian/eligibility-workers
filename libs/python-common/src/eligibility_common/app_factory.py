"""One-liner FastAPI app factory so every service is consistent.

Also installs a graceful-shutdown hook: on SIGTERM (Cloud Run's termination
signal), we stop accepting new requests, wait up to `SHUTDOWN_GRACE_SECONDS`
for in-flight work to drain, then dispose DB pools and exit. Prevents mid-
write connection aborts on rollouts.
"""
from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from .health import make_health_router
from .http_middleware import CorrelationIdMiddleware, register_exception_handlers
from .logging import configure_logging, get_logger
from .tracing import init_tracing

log = get_logger(__name__)


def create_app(
    *,
    service_name: str,
    version: str = "0.1.0",
    lifespan: Callable[[FastAPI], Any] | None = None,
    readiness: dict[str, Callable[[], Awaitable[Any]]] | None = None,
    install_idempotency: bool = False,
) -> FastAPI:
    configure_logging(service_name=service_name)
    init_tracing(service_name)

    combined = _combine_lifespan(lifespan)
    app = FastAPI(title=service_name, version=version, lifespan=combined)
    app.state.ready = True
    app.add_middleware(CorrelationIdMiddleware)
    register_exception_handlers(app)
    app.include_router(make_health_router(readiness, is_ready=lambda: app.state.ready))
    FastAPIInstrumentor.instrument_app(app)

    if install_idempotency:
        from .db import session_scope
        from .idempotency import install_idempotency_middleware

        install_idempotency_middleware(app, session_scope)
    return app


def _combine_lifespan(
    user_lifespan: Callable[[FastAPI], Any] | None,
) -> Callable[[FastAPI], Any]:
    """Wrap the caller's lifespan with SIGTERM drain + DB pool disposal.

    Contract:
    - On startup: run caller's lifespan startup, install signal handlers.
    - On SIGTERM/SIGINT: flip readiness off so LB stops new traffic, then
      wait up to SHUTDOWN_GRACE_SECONDS for in-flight requests to finish
      (Starlette itself holds the lifespan open until the server drains).
    - On shutdown: dispose DB pool so connections close cleanly.
    """

    @asynccontextmanager
    async def _wrapped(app: FastAPI):
        grace = float(os.environ.get("SHUTDOWN_GRACE_SECONDS", "20"))
        loop = asyncio.get_running_loop()

        def _on_term(signum: int) -> None:
            log.info("shutdown.signal", signal=signum, grace_seconds=grace)
            app.state.ready = False

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _on_term, sig)
            except (NotImplementedError, RuntimeError):
                pass  # Windows / non-main-thread — best effort

        if user_lifespan is None:
            try:
                yield
            finally:
                await _dispose_engine_if_initialized()
            return

        async with _as_async_context(user_lifespan, app) as value:
            try:
                yield value
            finally:
                await _dispose_engine_if_initialized()

    return _wrapped


@asynccontextmanager
async def _as_async_context(user_lifespan: Callable[[FastAPI], Any], app: FastAPI):
    """Invoke the user's lifespan and yield whatever it yields.

    FastAPI lifespans are always async-context-managers; we just re-enter.
    """
    async with user_lifespan(app) as value:  # type: ignore[misc]
        yield value


async def _dispose_engine_if_initialized() -> None:
    try:
        from . import db as _db

        if _db._engine is not None:  # noqa: SLF001 - controlled access
            await _db._engine.dispose()  # noqa: SLF001
            log.info("db.pool.disposed")
    except Exception as e:  # noqa: BLE001
        log.warning("db.pool.dispose_failed", error=str(e))
