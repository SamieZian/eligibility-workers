"""Ingestion worker entrypoint.

Serves /livez + /readyz for k8s and runs a Pub/Sub subscriber in a
background asyncio task.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from functools import partial
from typing import Any, AsyncIterator

import httpx
import uvicorn
from fastapi import FastAPI

from eligibility_common.events import FileReceived, Topics  # noqa: F401  (re-export for tests)
from eligibility_common.health import make_health_router
from eligibility_common.logging import configure_logging, get_logger
from eligibility_common.pubsub import run_subscriber

from app.handler import handle_file
from app.settings import get_settings
from app.storage import ObjectStorage

log = get_logger(__name__)


def _default_client_factory(*, settings: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=httpx.Timeout(settings.http_timeout_seconds))


def build_app() -> FastAPI:
    settings = get_settings()
    configure_logging(service_name=settings.service_name)

    storage = ObjectStorage(
        endpoint=settings.minio_endpoint,
        bucket=settings.minio_bucket,
        user=settings.minio_user,
        password=settings.minio_password,
    )

    handler = partial(
        handle_file,
        storage=storage,
        settings=settings,
        client_factory=_default_client_factory,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await storage.ensure_bucket()

        task = asyncio.create_task(
            run_subscriber(
                sub="ingestion.file-received",
                topic=Topics.FILE_RECEIVED,
                handler=handler,
                dlq_topic=Topics.FILE_RECEIVED_DLQ,
            ),
            name="ingestion-subscriber",
        )
        log.info("ingestion.startup", bucket=settings.minio_bucket)
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            log.info("ingestion.shutdown")

    app = FastAPI(title="ingestion", lifespan=lifespan)
    app.include_router(make_health_router())
    return app


def main() -> None:
    uvicorn.run(
        "app.main:build_app",
        host="0.0.0.0",
        port=8000,
        factory=True,
        log_level="info",
    )


if __name__ == "__main__":
    main()
