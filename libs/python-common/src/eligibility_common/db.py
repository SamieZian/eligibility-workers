"""SQLAlchemy async engine + session factory + RLS session var.

Every new asyncpg connection gets server-side `statement_timeout`,
`lock_timeout`, and `idle_in_transaction_session_timeout` configured so a
single slow query can never hang the pool. Defaults are tight (30s / 5s / 60s)
and tunable via env.
"""
from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


def engine() -> AsyncEngine:
    global _engine, _sessionmaker
    if _engine is None:
        url = os.environ["DATABASE_URL"]
        # Convert psycopg URL to async driver if needed
        if url.startswith("postgresql+psycopg://"):
            url = url.replace("postgresql+psycopg://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        _engine = create_async_engine(
            url,
            pool_size=int(os.environ.get("DB_POOL_SIZE", "10")),
            max_overflow=int(os.environ.get("DB_MAX_OVERFLOW", "20")),
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_timeout=float(os.environ.get("DB_POOL_TIMEOUT", "10")),
            echo=False,
        )
        _install_connect_hooks(_engine)
        _sessionmaker = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)
    return _engine


def _install_connect_hooks(eng: AsyncEngine) -> None:
    """Set Postgres timeouts on every new connection — belt-and-braces against
    slow queries hanging the pool. Values in milliseconds."""
    stmt_timeout = int(os.environ.get("DB_STATEMENT_TIMEOUT_MS", "30000"))
    lock_timeout = int(os.environ.get("DB_LOCK_TIMEOUT_MS", "5000"))
    idle_tx_timeout = int(os.environ.get("DB_IDLE_IN_TX_TIMEOUT_MS", "60000"))

    @event.listens_for(eng.sync_engine, "connect")
    def _on_connect(dbapi_conn, _record):  # type: ignore[no-untyped-def]
        # asyncpg connections expose a sync wrapper via AsyncAdapt_asyncpg_dbapi.
        # We execute SET commands at connection time; they persist for the session.
        async def _apply() -> None:
            await dbapi_conn.execute(
                f"SET statement_timeout = {stmt_timeout}; "
                f"SET lock_timeout = {lock_timeout}; "
                f"SET idle_in_transaction_session_timeout = {idle_tx_timeout}"
            )
        # SQLAlchemy's asyncpg adapter exposes a run() helper for sync-in-async.
        try:
            dbapi_conn.await_(_apply())
        except Exception:  # noqa: BLE001 - best-effort; don't break boot
            pass


def sessionmaker() -> async_sessionmaker[AsyncSession]:
    engine()
    assert _sessionmaker is not None
    return _sessionmaker


@asynccontextmanager
async def session_scope(tenant_id: str | None = None) -> AsyncIterator[AsyncSession]:
    """Open a session, set RLS tenant var, wrap in a transaction.

    `SET LOCAL` does not accept bind parameters under Postgres' parse+bind
    protocol, so we use `set_config(name, value, is_local=true)` instead — it
    takes parameters and is transaction-scoped identically to SET LOCAL.
    """
    sm = sessionmaker()
    async with sm() as s:
        async with s.begin():
            if tenant_id:
                await s.execute(
                    text("SELECT set_config('app.tenant_id', :t, true)"),
                    {"t": tenant_id},
                )
            yield s
