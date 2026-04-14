"""Idempotency-key store for mutating endpoints.

Caller supplies an Idempotency-Key header; we cache the response so duplicate
requests get the same reply. Keys expire after 24h by default.
"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

IDEMPOTENCY_DDL_STATEMENTS = [
    """
CREATE TABLE IF NOT EXISTS idempotency_keys (
  key TEXT PRIMARY KEY,
  request_hash TEXT NOT NULL,
  response JSONB NOT NULL,
  status INT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
)
""",
    "CREATE INDEX IF NOT EXISTS idempotency_expires ON idempotency_keys(expires_at)",
]
IDEMPOTENCY_DDL = ";\n".join(IDEMPOTENCY_DDL_STATEMENTS)


def request_hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


async def lookup(session: AsyncSession, key: str, body: bytes) -> tuple[int, dict[str, Any]] | None:
    row = (
        await session.execute(
            text(
                "SELECT request_hash, response, status, expires_at FROM idempotency_keys WHERE key=:k"
            ),
            {"k": key},
        )
    ).first()
    if not row:
        return None
    if row.request_hash != request_hash(body):
        # Same key, different body — client error
        from .errors import ValidationError

        raise ValidationError("IDEMPOTENCY_MISMATCH", "Idempotency-Key reused with different body")
    if row.expires_at < datetime.now(UTC):
        return None
    return int(row.status), dict(row.response)


async def save(
    session: AsyncSession,
    key: str,
    body: bytes,
    status: int,
    response: dict[str, Any],
    ttl: timedelta = timedelta(hours=24),
) -> None:
    await session.execute(
        text(
            """
            INSERT INTO idempotency_keys (key, request_hash, response, status, expires_at)
            VALUES (:k, :h, CAST(:r AS JSONB), :s, :exp)
            ON CONFLICT (key) DO NOTHING
            """
        ),
        {
            "k": key,
            "h": request_hash(body),
            "r": json.dumps(response),
            "s": status,
            "exp": datetime.now(UTC) + ttl,
        },
    )
