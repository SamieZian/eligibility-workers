"""Outbox relay worker.

Polls unpublished rows from per-service `outbox` tables and publishes each to
Google Pub/Sub. After successful publish, marks the row published_at = now().
Publish failures are logged; the row is left unpublished so the next poll retries.
"""
from __future__ import annotations

import asyncio
import json
import signal
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from eligibility_common import pubsub
from eligibility_common.health import make_health_router
from eligibility_common.logging import configure_logging, get_logger
from eligibility_common.retry import retry_async
from eligibility_common.tracing import init_tracing

log = get_logger(__name__)


# SQL constants exposed so tests can assert on their shape.
CLAIM_SQL = (
    "SELECT id, aggregate, aggregate_id, event_type, payload, headers "
    "FROM outbox "
    "WHERE published_at IS NULL "
    "ORDER BY id "
    "LIMIT :batch_size "
    "FOR UPDATE SKIP LOCKED"
)

MARK_PUBLISHED_SQL = "UPDATE outbox SET published_at = now() WHERE id = :id"


def _normalize_url(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return url.replace("postgresql+psycopg://", "postgresql+asyncpg://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


def _make_sessionmaker(url: str) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(
        _normalize_url(url),
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=1800,
        echo=False,
    )
    sm = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    return engine, sm


def _row_to_dict(row: Any) -> dict[str, Any]:
    """Normalize an outbox row into a plain dict regardless of JSON(B) shape."""
    # SQLAlchemy Row mapping access
    m = row._mapping if hasattr(row, "_mapping") else row

    def _j(v: Any) -> Any:
        if isinstance(v, (dict, list)):
            return v
        if isinstance(v, (bytes, bytearray)):
            v = v.decode()
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                return v
        return v

    return {
        "id": m["id"],
        "aggregate": m["aggregate"],
        "aggregate_id": m["aggregate_id"],
        "event_type": m["event_type"],
        "payload": _j(m["payload"]),
        "headers": _j(m["headers"]),
    }


def _build_attributes(headers: dict[str, Any]) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for key in ("tenant_id", "correlation_id"):
        if key in headers and headers[key] is not None:
            attrs[key] = str(headers[key])
    return attrs


async def _publish_one(row: dict[str, Any]) -> None:
    headers = row.get("headers") or {}
    payload = dict(row.get("payload") or {})
    topic = headers.get("topic")
    if not topic:
        raise ValueError(f"outbox row {row.get('id')} missing headers.topic")

    # Consumers (projector, etc.) discriminate on `event_type` in the payload.
    # The outbox table stores it alongside the payload, so merge it in here.
    if row.get("event_type") and "event_type" not in payload:
        payload["event_type"] = row["event_type"]
    if row.get("aggregate_id") and "aggregate_id" not in payload:
        payload["aggregate_id"] = str(row["aggregate_id"])

    attributes = _build_attributes(headers)
    attributes.setdefault("event_type", str(row.get("event_type") or ""))

    async def _do() -> str:
        # pubsub.publish is sync; run in a thread to avoid blocking the loop.
        return await asyncio.to_thread(pubsub.publish, topic, payload, attributes)

    await retry_async(_do, attempts=5, base=0.1, cap=5.0, op=f"pubsub.publish:{topic}")


async def drain_once(
    sm: async_sessionmaker[AsyncSession],
    *,
    db_name: str,
    batch_size: int,
) -> int:
    """Claim one batch of unpublished rows, publish and mark each, return count."""
    published = 0
    async with sm() as session:
        async with session.begin():
            result = await session.execute(text(CLAIM_SQL), {"batch_size": batch_size})
            rows = [_row_to_dict(r) for r in result.fetchall()]
            if not rows:
                return 0

            for row in rows:
                try:
                    await _publish_one(row)
                except Exception as e:
                    log.error(
                        "outbox.publish.failed",
                        db=db_name,
                        outbox_id=row.get("id"),
                        event_type=row.get("event_type"),
                        error=str(e),
                    )
                    continue
                await session.execute(text(MARK_PUBLISHED_SQL), {"id": row["id"]})
                published += 1
                log.info(
                    "outbox.publish.ok",
                    db=db_name,
                    outbox_id=row.get("id"),
                    event_type=row.get("event_type"),
                    topic=(row.get("headers") or {}).get("topic"),
                )
    return published


async def _db_loop(
    db_name: str,
    sm: async_sessionmaker[AsyncSession],
    *,
    batch_size: int,
    poll_interval: float,
    stop: asyncio.Event,
) -> None:
    log.info("outbox.loop.start", db=db_name)
    while not stop.is_set():
        try:
            n = await drain_once(sm, db_name=db_name, batch_size=batch_size)
            if n == 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=poll_interval)
                except asyncio.TimeoutError:
                    pass
        except Exception as e:
            log.error("outbox.loop.error", db=db_name, error=str(e))
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)
            except asyncio.TimeoutError:
                pass
    log.info("outbox.loop.stop", db=db_name)


def _build_health_app() -> FastAPI:
    app = FastAPI(title="outbox-relay")
    app.include_router(make_health_router())
    return app


async def _serve_health(stop: asyncio.Event) -> None:
    app = _build_health_app()
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info", lifespan="on")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    await stop.wait()
    server.should_exit = True
    await task


async def run() -> None:
    # Import settings lazily so tests can import this module without env vars.
    from app.settings import settings

    configure_logging(service_name=settings.service_name)
    init_tracing(settings.service_name)

    db_urls = {
        "atlas": settings.atlas_db_url,
        "member": settings.member_db_url,
        "group": settings.group_db_url,
        "plan": settings.plan_db_url,
    }

    engines: list[AsyncEngine] = []
    sms: dict[str, async_sessionmaker[AsyncSession]] = {}
    for name, url in db_urls.items():
        e, sm = _make_sessionmaker(url)
        engines.append(e)
        sms[name] = sm

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _signal() -> None:
        log.info("outbox.shutdown.signal")
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal)
        except NotImplementedError:
            # e.g. on Windows; fall back to default handlers
            pass

    tasks = [
        asyncio.create_task(
            _db_loop(
                name,
                sm,
                batch_size=settings.batch_size,
                poll_interval=settings.poll_interval_seconds,
                stop=stop,
            ),
            name=f"outbox-loop-{name}",
        )
        for name, sm in sms.items()
    ]
    tasks.append(asyncio.create_task(_serve_health(stop), name="health-server"))

    try:
        await asyncio.gather(*tasks)
    finally:
        for e in engines:
            await e.dispose()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
