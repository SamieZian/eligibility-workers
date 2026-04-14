"""Transactional outbox helper.

Each service has an `outbox` table with the same schema. Writes happen inside
the service's domain transaction; the outbox_relay worker drains rows to Pub/Sub.
"""
from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


# DDL split into discrete statements — asyncpg rejects multi-statement prepared queries.
OUTBOX_DDL_STATEMENTS = [
    """
CREATE TABLE IF NOT EXISTS outbox (
  id BIGSERIAL PRIMARY KEY,
  aggregate TEXT NOT NULL,
  aggregate_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  headers JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
)
""",
    "CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL",
]
# Legacy alias — may still be used by sync drivers that accept multi-statement scripts.
OUTBOX_DDL = ";\n".join(OUTBOX_DDL_STATEMENTS)


async def emit(
    session: AsyncSession,
    *,
    aggregate: str,
    aggregate_id: str | uuid.UUID,
    event_type: str,
    payload: dict[str, Any],
    headers: dict[str, Any] | None = None,
) -> None:
    """Insert a row into the outbox in the *current* transaction.

    Never publish directly — the relay does that.
    """
    h = headers or {}
    h.setdefault("event_id", str(uuid.uuid4()))
    h.setdefault("emitted_at", datetime.now(UTC).isoformat())
    await session.execute(
        text(
            """
            INSERT INTO outbox (aggregate, aggregate_id, event_type, payload, headers)
            VALUES (:aggregate, :aggregate_id, :event_type, CAST(:payload AS JSONB), CAST(:headers AS JSONB))
            """
        ),
        {
            "aggregate": aggregate,
            "aggregate_id": str(aggregate_id),
            "event_type": event_type,
            "payload": json.dumps(payload),
            "headers": json.dumps(h),
        },
    )
