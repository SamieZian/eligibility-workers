"""Unit tests for the outbox relay drain loop.

We do not require a live Postgres — the session is fully mocked. We test:
  1. drain_once publishes each claimed row with the right topic+payload+attrs
     and marks the row published_at.
  2. The claim SQL uses FOR UPDATE SKIP LOCKED.
  3. A publish failure leaves published_at NULL (i.e. no UPDATE issued).
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.main import CLAIM_SQL, MARK_PUBLISHED_SQL, drain_once


def _row(id_: int, *, topic: str, tenant_id: str | None = "t-1",
         correlation_id: str | None = "c-1",
         payload: dict[str, Any] | None = None,
         event_type: str = "enrollment.created") -> Any:
    headers: dict[str, Any] = {"topic": topic, "event_id": f"evt-{id_}"}
    if tenant_id is not None:
        headers["tenant_id"] = tenant_id
    if correlation_id is not None:
        headers["correlation_id"] = correlation_id
    mapping = {
        "id": id_,
        "aggregate": "enrollment",
        "aggregate_id": "agg-1",
        "event_type": event_type,
        "payload": json.dumps(payload or {"foo": id_}),
        "headers": json.dumps(headers),
    }
    row = SimpleNamespace()
    row._mapping = mapping
    return row


class _FakeResult:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def fetchall(self) -> list[Any]:
        return list(self._rows)


class _FakeSession:
    """Minimal AsyncSession stand-in that records executed SQL.

    `session.begin()` returns an async context manager. `session.execute`
    dispatches on the SQL text prefix.
    """

    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows
        self.executed: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, clause: Any, params: dict[str, Any] | None = None) -> Any:
        sql = str(clause)
        self.executed.append((sql, params or {}))
        if sql.lstrip().upper().startswith("SELECT"):
            return _FakeResult(self._rows)
        return MagicMock()

    def begin(self) -> Any:
        @asynccontextmanager
        async def _cm() -> Any:
            yield self

        return _cm()

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


def _sessionmaker_for(session: _FakeSession) -> Any:
    def _mk() -> _FakeSession:
        return session

    return _mk


@pytest.mark.asyncio
async def test_drain_publishes_and_marks_rows() -> None:
    session = _FakeSession([
        _row(1, topic="enrollment.events", payload={"a": 1}),
        _row(2, topic="member.events", payload={"b": 2}),
    ])
    sm = _sessionmaker_for(session)

    with patch("app.main.pubsub.publish") as m_publish:
        m_publish.return_value = "msg-id"
        count = await drain_once(sm, db_name="atlas", batch_size=100)

    assert count == 2
    # publish called with (topic, payload, attributes)
    assert m_publish.call_count == 2
    call1 = m_publish.call_args_list[0]
    assert call1.args[0] == "enrollment.events"
    assert call1.args[1] == {"a": 1}
    assert call1.args[2] == {"tenant_id": "t-1", "correlation_id": "c-1"}

    call2 = m_publish.call_args_list[1]
    assert call2.args[0] == "member.events"
    assert call2.args[1] == {"b": 2}

    # The first execute must be the claim; the remaining must include two UPDATEs.
    assert session.executed[0][0].strip().startswith("SELECT")
    update_stmts = [e for e in session.executed if "UPDATE outbox" in e[0]]
    assert len(update_stmts) == 2
    ids_updated = sorted(e[1]["id"] for e in update_stmts)
    assert ids_updated == [1, 2]


@pytest.mark.asyncio
async def test_claim_sql_uses_for_update_skip_locked() -> None:
    session = _FakeSession([])
    sm = _sessionmaker_for(session)

    with patch("app.main.pubsub.publish"):
        await drain_once(sm, db_name="atlas", batch_size=50)

    assert session.executed, "expected the claim SELECT to be executed"
    sql = session.executed[0][0]
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "WHERE published_at IS NULL" in sql
    # Sanity: the module constant itself encodes the locking clause.
    assert "FOR UPDATE SKIP LOCKED" in CLAIM_SQL
    assert "UPDATE outbox SET published_at = now()" in MARK_PUBLISHED_SQL


@pytest.mark.asyncio
async def test_publish_failure_leaves_row_unpublished() -> None:
    session = _FakeSession([_row(7, topic="plan.events")])
    sm = _sessionmaker_for(session)

    with patch("app.main.pubsub.publish") as m_publish, \
         patch("app.main.asyncio.sleep", new=AsyncMock(return_value=None)):
        m_publish.side_effect = ConnectionError("boom")
        count = await drain_once(sm, db_name="plan", batch_size=10)

    assert count == 0
    # retry_async should have attempted 5 times before giving up.
    assert m_publish.call_count == 5
    # No UPDATE was executed — row stays with published_at NULL.
    assert not any("UPDATE outbox" in e[0] for e in session.executed)
