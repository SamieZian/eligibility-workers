"""Unit tests for the ingestion handler."""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from app.handler import handle_file


# ---------------------------------------------------------------------------
# 834 fixture helpers (small self-contained sample)
# ---------------------------------------------------------------------------

def _isa(ctrl: str = "000000001") -> str:
    return (
        "ISA*00*          *00*          *ZZ*SENDER         "
        "*ZZ*RECEIVER       *260414*0930*U*00501*"
        f"{ctrl}*0*P*:~"
    )


def _gs(ctrl: str = "1") -> str:
    return f"GS*BE*SENDER*RECEIVER*20260414*0930*{ctrl}*X*005010X220A1~"


def _st(ctrl: str = "0001") -> str:
    return f"ST*834*{ctrl}*005010X220A1~"


def _ins_body(
    *,
    maint: str = "021",
    sponsor: str = "EMP-EXT-42",
    ssn: str = "123456789",
    first: str = "PRIYA",
    last: str = "SHARMA",
    plan: str = "PLAN-GOLD",
    effective: str = "20260101",
    termination: str | None = None,
) -> list[str]:
    segs = [
        f"INS*Y*18*{maint}*20*A***FT~",
        f"REF*38*{sponsor}~",
        f"REF*0F*{ssn}~",
        "REF*1L*GRP-001~",
        f"NM1*IL*1*{last}*{first}****34*{ssn}~",
        f"DMG*D8*19900215*F~",
        f"HD*{maint}**HLT*{plan}*FAM~",
        f"DTP*348*D8*{effective}~",
    ]
    if termination:
        segs.append(f"DTP*349*D8*{termination}~")
    return segs


def _build_834(*bodies: list[str]) -> bytes:
    segs: list[str] = [_isa(), _gs(), _st()]
    total = 0
    for body in bodies:
        segs.extend(body)
        total += len(body)
    segs.append(f"SE*{total + 1}*0001~")
    segs.append("GE*1*1~")
    segs.append("IEA*1*000000001~")
    return "".join(segs).encode()


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeStorage:
    def __init__(self, blob: bytes) -> None:
        self.blob = blob
        self.calls: list[str] = []

    async def download(self, object_key: str) -> bytes:
        self.calls.append(object_key)
        return self.blob


class RecordedRequest:
    def __init__(self, method: str, url: str, params: dict[str, Any] | None, json_body: Any) -> None:
        self.method = method
        self.url = url
        self.params = params
        self.json = json_body


def _make_settings(**overrides: Any) -> Any:
    base = dict(
        atlas_url="http://atlas",
        member_url="http://member",
        group_url="http://group",
        plan_url="http://plan",
        http_timeout_seconds=5.0,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _file_received_payload(*, file_id: str | None = None, fmt: str = "X12_834") -> dict[str, Any]:
    return {
        "event_id": str(uuid4()),
        "event_type": "FileReceived",
        "tenant_id": str(uuid4()),
        "correlation_id": "corr-1",
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "file_id": file_id or str(uuid4()),
        "format": fmt,
        "object_key": "tenant/file.edi",
        "trading_partner_id": "TP-1",
    }


class _Router:
    """Tiny dispatch table used to back an httpx.MockTransport."""

    def __init__(self) -> None:
        self.handlers: list[tuple[str, str, Any]] = []
        self.calls: list[RecordedRequest] = []

    def on(self, method: str, path_prefix: str, responder: Any) -> None:
        self.handlers.append((method, path_prefix, responder))

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body: Any = None
        if request.content:
            try:
                body = json.loads(request.content.decode())
            except Exception:
                body = request.content
        params = dict(request.url.params)
        self.calls.append(
            RecordedRequest(request.method, str(request.url), params, body)
        )
        url_str = str(request.url)
        for method, prefix, responder in self.handlers:
            if method != request.method:
                continue
            if prefix in url_str:
                if callable(responder):
                    return responder(request, params, body)
                return responder
        return httpx.Response(500, json={"error": f"unhandled {request.method} {url_str}"})


def _client_factory_with(router: _Router) -> Any:
    def _factory(*, settings: Any) -> httpx.AsyncClient:
        transport = httpx.MockTransport(router)
        return httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(5.0))

    return _factory


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_happy_path_single_add_posts_atlas_command() -> None:
    blob = _build_834(_ins_body(maint="021", effective="20260101"))
    storage = FakeStorage(blob)

    employer_id = str(uuid4())
    member_id = str(uuid4())
    plan_id = str(uuid4())

    router = _Router()
    router.on(
        "GET",
        "http://group/employers",
        httpx.Response(
            200,
            json=[{"id": employer_id, "payer_id": str(uuid4()), "name": "Acme", "external_id": "EMP-EXT-42"}],
        ),
    )
    router.on(
        "GET",
        "http://member/members",
        httpx.Response(
            200,
            json={
                "id": member_id,
                "tenant_id": str(uuid4()),
                "employer_id": employer_id,
                "first_name": "PRIYA",
                "last_name": "SHARMA",
                "dob": "1990-02-15",
                "payer_member_id": None,
                "card_number": "123456789",
                "gender": "F",
                "ssn_last4": "6789",
                "address": None,
                "version": 1,
            },
        ),
    )
    router.on(
        "GET",
        "http://plan/plans",
        httpx.Response(
            200,
            json={
                "id": plan_id,
                "plan_code": "PLAN-GOLD",
                "name": "Gold",
                "type": "HLT",
                "metal_level": "G",
                "attributes": {},
                "version": 1,
            },
        ),
    )
    router.on(
        "POST",
        "http://atlas/commands",
        httpx.Response(200, json={"enrollment_ids": [str(uuid4())]}),
    )

    payload = _file_received_payload()
    settings = _make_settings()

    await handle_file(
        payload,
        {},
        storage=storage,
        settings=settings,
        client_factory=_client_factory_with(router),
    )

    # The atlas POST must have fired exactly once, and with a well-formed payload.
    atlas_posts = [c for c in router.calls if c.method == "POST" and "atlas/commands" in c.url]
    assert len(atlas_posts) == 1
    body = atlas_posts[0].json
    assert body["command_type"] == "ADD"
    assert body["employer_id"] == employer_id
    assert body["member_id"] == member_id
    assert body["plan_id"] == plan_id
    assert body["valid_from"] == "2026-01-01"
    assert body["source_file_id"] == payload["file_id"]
    assert body["source_segment_ref"]
    assert storage.calls == ["tenant/file.edi"]


# ---------------------------------------------------------------------------
# Dedup: duplicate POST returns 200 but handler must not raise.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_duplicate_file_does_not_raise() -> None:
    blob = _build_834(_ins_body(maint="021", effective="20260101"))
    storage = FakeStorage(blob)

    employer_id = str(uuid4())
    member_id = str(uuid4())
    plan_id = str(uuid4())

    router = _Router()
    router.on(
        "GET",
        "http://group/employers",
        httpx.Response(200, json=[{"id": employer_id, "payer_id": str(uuid4()), "name": "X", "external_id": "EMP-EXT-42"}]),
    )
    router.on(
        "GET",
        "http://member/members",
        httpx.Response(
            200,
            json={
                "id": member_id,
                "tenant_id": str(uuid4()),
                "employer_id": employer_id,
                "first_name": "PRIYA",
                "last_name": "SHARMA",
                "dob": "1990-02-15",
                "payer_member_id": None,
                "card_number": "123456789",
                "gender": "F",
                "ssn_last4": "6789",
                "address": None,
                "version": 1,
            },
        ),
    )
    router.on(
        "GET",
        "http://plan/plans",
        httpx.Response(
            200,
            json={
                "id": plan_id,
                "plan_code": "PLAN-GOLD",
                "name": "Gold",
                "type": "HLT",
                "metal_level": None,
                "attributes": {},
                "version": 1,
            },
        ),
    )
    # Atlas treats the replay as idempotent (200, same enrollment_ids).
    existing_enrollment = str(uuid4())
    router.on(
        "POST",
        "http://atlas/commands",
        httpx.Response(200, json={"enrollment_ids": [existing_enrollment]}),
    )

    payload = _file_received_payload(file_id=str(uuid4()))
    settings = _make_settings()

    # Second identical delivery — the handler is re-invoked with the same file
    # and must still complete cleanly.
    await handle_file(payload, {}, storage=storage, settings=settings, client_factory=_client_factory_with(router))
    await handle_file(payload, {}, storage=storage, settings=settings, client_factory=_client_factory_with(router))

    atlas_posts = [c for c in router.calls if c.method == "POST" and "atlas/commands" in c.url]
    # Two deliveries -> two POSTs, each idempotent on the server. What matters
    # is that neither raised.
    assert len(atlas_posts) == 2


# ---------------------------------------------------------------------------
# Per-instruction error does not abort the file.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_one_failed_instruction_does_not_abort_file() -> None:
    blob = _build_834(
        _ins_body(maint="021", sponsor="EMP-BAD", ssn="111111111", effective="20260101"),
        _ins_body(maint="021", sponsor="EMP-OK", ssn="222222222", effective="20260201"),
    )
    storage = FakeStorage(blob)

    employer_ok = str(uuid4())
    member_id = str(uuid4())
    plan_id = str(uuid4())

    def employer_responder(request: httpx.Request, params: dict[str, Any], body: Any) -> httpx.Response:
        ext = params.get("external_id")
        if ext == "EMP-BAD":
            return httpx.Response(404, json={"detail": "not found"})
        return httpx.Response(
            200,
            json=[{"id": employer_ok, "payer_id": str(uuid4()), "name": "Ok", "external_id": ext}],
        )

    router = _Router()
    router.on("GET", "http://group/employers", employer_responder)
    router.on(
        "GET",
        "http://member/members",
        httpx.Response(
            200,
            json={
                "id": member_id,
                "tenant_id": str(uuid4()),
                "employer_id": employer_ok,
                "first_name": "X",
                "last_name": "Y",
                "dob": "1990-02-15",
                "payer_member_id": None,
                "card_number": "222222222",
                "gender": "F",
                "ssn_last4": "2222",
                "address": None,
                "version": 1,
            },
        ),
    )
    router.on(
        "GET",
        "http://plan/plans",
        httpx.Response(
            200,
            json={
                "id": plan_id,
                "plan_code": "PLAN-GOLD",
                "name": "Gold",
                "type": "HLT",
                "metal_level": None,
                "attributes": {},
                "version": 1,
            },
        ),
    )
    router.on(
        "POST",
        "http://atlas/commands",
        httpx.Response(200, json={"enrollment_ids": [str(uuid4())]}),
    )

    payload = _file_received_payload()
    settings = _make_settings()

    await handle_file(
        payload,
        {},
        storage=storage,
        settings=settings,
        client_factory=_client_factory_with(router),
    )

    atlas_posts = [c for c in router.calls if c.method == "POST" and "atlas/commands" in c.url]
    # Only the good instruction reached atlas.
    assert len(atlas_posts) == 1
    assert atlas_posts[0].json["employer_id"] == employer_ok
