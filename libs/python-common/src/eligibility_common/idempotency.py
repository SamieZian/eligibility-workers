"""Idempotency-key store + FastAPI middleware for mutating endpoints.

Caller supplies an ``Idempotency-Key`` header; we cache the response so
duplicate requests get the same reply. Keys expire after 24h by default.

Wire via :func:`install_idempotency_middleware` in a service's ``main.py``:
the middleware runs before the route handler, short-circuits on a cache hit,
and otherwise stores the response body on success.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.types import ASGIApp, Message, Receive, Scope, Send

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


class IdempotencyMiddleware:
    """Pure ASGI middleware enforcing idempotency on mutating methods.

    Implemented at the raw ASGI layer (not ``BaseHTTPMiddleware``) because
    Starlette's ``BaseHTTPMiddleware`` does not allow safely overriding the
    ``receive`` callable the downstream app reads its body from. Raw ASGI is
    simpler here: we fully drain the request body, hash it, (optionally) short-
    circuit with a cached response, or call downstream with a replay-safe
    ``receive`` and buffer the response on success.

    - Only acts on ``POST`` / ``PUT`` / ``PATCH`` / ``DELETE``.
    - If the request carries an ``Idempotency-Key`` header and a matching row
      exists in ``idempotency_keys``, returns the cached response verbatim
      with an ``Idempotent-Replay: true`` header.
    - Else runs the handler, and on success (2xx) persists the response body
      keyed by the supplied key.
    - On ``Idempotency-Key`` reuse with a different request body, raises
      :class:`ValidationError` (HTTP 422, ``IDEMPOTENCY_MISMATCH``).

    Services without a DB (e.g. the BFF) should NOT install this — idempotency
    belongs on the writing system of record.
    """

    WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

    def __init__(self, app: ASGIApp, session_scope: Callable[..., Any]) -> None:
        self.app = app
        self._session_scope = session_scope

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("method") not in self.WRITE_METHODS:
            await self.app(scope, receive, send)
            return

        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        key = headers.get("idempotency-key")
        if not key:
            await self.app(scope, receive, send)
            return

        # Fully drain the body so we can hash it and replay it to the app.
        body = await _drain_body(receive)
        tenant_id = headers.get("x-tenant-id")

        # 1. Cache lookup.
        async with self._session_scope(tenant_id=tenant_id) as s:
            cached = await lookup(s, key, body)
        if cached is not None:
            status, payload = cached
            await _send_json(send, status, payload, replay=True)
            return

        # 2. Call downstream with a replay-safe receive + intercepting send.
        replayed = {"done": False}

        async def receive_replay() -> Message:
            if not replayed["done"]:
                replayed["done"] = True
                return {"type": "http.request", "body": body, "more_body": False}
            # After the single http.request, any further receive must block
            # until the client disconnects. We fall back to the original
            # `receive` so genuine disconnects propagate correctly.
            return await receive()

        captured = _ResponseCapture(send)
        await self.app(scope, receive_replay, captured.send)

        # 3. Persist on success.
        if captured.status is not None and 200 <= captured.status < 300:
            try:
                payload_json = json.loads(captured.body or b"{}")
            except json.JSONDecodeError:
                payload_json = {"raw": captured.body.decode(errors="replace")}
            async with self._session_scope(tenant_id=tenant_id) as s:
                await save(s, key, body, captured.status, payload_json)


async def _drain_body(receive: Receive) -> bytes:
    chunks: list[bytes] = []
    more = True
    while more:
        msg = await receive()
        if msg["type"] == "http.request":
            chunks.append(msg.get("body", b"") or b"")
            more = bool(msg.get("more_body"))
        else:  # http.disconnect
            break
    return b"".join(chunks)


async def _send_json(send: Send, status: int, payload: dict[str, Any], *, replay: bool) -> None:
    body = json.dumps(payload).encode("utf-8")
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode()),
    ]
    if replay:
        headers.append((b"idempotent-replay", b"true"))
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body, "more_body": False})


class _ResponseCapture:
    """Wraps ``send`` to tee the response to both the client and our cache."""

    def __init__(self, downstream_send: Send) -> None:
        self._send = downstream_send
        self.status: int | None = None
        self.body: bytes = b""

    async def send(self, message: Message) -> None:
        if message["type"] == "http.response.start":
            self.status = int(message["status"])
        elif message["type"] == "http.response.body":
            self.body += message.get("body", b"") or b""
        await self._send(message)


def install_idempotency_middleware(
    app: FastAPI,
    session_scope: Callable[..., Any],
) -> None:
    """Install :class:`IdempotencyMiddleware` on ``app``.

    ``session_scope`` is the service's ``session_scope`` async-contextmanager
    factory (from :mod:`eligibility_common.db`).
    """
    app.add_middleware(IdempotencyMiddleware, session_scope=session_scope)
