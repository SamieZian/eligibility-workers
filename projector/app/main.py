"""Projector worker — CQRS read-model builder.

On startup:
  1. Apply read-model DDL (eligibility_view + lookup tables + pg_trgm) to the
     shared atlas db.
  2. Ensure the OpenSearch `eligibility` index exists with the right mapping.
  3. Launch a `run_subscriber` background task per domain topic. Each
     subscriber's handler dispatches by `event_type`.

On shutdown: cancel subscriber tasks and dispose the engine.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from eligibility_common.app_factory import create_app
from eligibility_common.events import Topics
from eligibility_common.logging import get_logger
from eligibility_common.pubsub import run_subscriber

from app.handlers import make_dispatch_handler
from app.os_index import ensure_index
from app.read_model import apply_ddl
from app.settings import settings

log = get_logger(__name__)


def _normalize_url(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return url.replace("postgresql+psycopg://", "postgresql+asyncpg://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


def _make_engine(url: str) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    eng = create_async_engine(
        _normalize_url(url),
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=1800,
        echo=False,
    )
    sm = async_sessionmaker(eng, expire_on_commit=False, class_=AsyncSession)
    return eng, sm


# Topics this worker subscribes to — one subscription per topic; a single
# dispatcher handler routes by event_type inside each callback.
_TOPICS: dict[str, tuple[str, str]] = {
    "enrollment": (Topics.ENROLLMENT, Topics.ENROLLMENT_DLQ),
    "member": (Topics.MEMBER, Topics.MEMBER_DLQ),
    "group": (Topics.GROUP, Topics.GROUP_DLQ),
    "plan": (Topics.PLAN, Topics.PLAN_DLQ),
}


@asynccontextmanager
async def lifespan(app_: FastAPI):  # type: ignore[no-untyped-def]
    engine, sm = _make_engine(settings.read_model_db_url)
    app_.state.engine = engine
    app_.state.sm = sm

    # 1. Read-model DDL.
    async with sm() as session:
        async with session.begin():
            await apply_ddl(session)

    # 2. OpenSearch index.
    await ensure_index(settings.opensearch_url)

    # 3. Pub/Sub subscribers.
    handler = make_dispatch_handler(sm, os_url=settings.opensearch_url)
    tasks: list[asyncio.Task[Any]] = []
    for name, (topic, dlq) in _TOPICS.items():
        sub_name = f"projector.{name}"
        task = asyncio.create_task(
            run_subscriber(sub_name, topic, handler, dlq_topic=dlq),
            name=f"subscriber-{name}",
        )
        tasks.append(task)
    app_.state.subscriber_tasks = tasks
    log.info("projector.started", topics=list(_TOPICS.keys()))

    try:
        yield
    finally:
        log.info("projector.shutdown")
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        await engine.dispose()


async def _ping_db() -> None:
    from sqlalchemy import text

    sm: async_sessionmaker[AsyncSession] = app.state.sm
    async with sm() as s:
        await s.execute(text("SELECT 1"))


app = create_app(
    service_name=settings.service_name,
    lifespan=lifespan,
    readiness={"db": _ping_db},
)


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
