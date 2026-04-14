"""Unit tests for the projector event handlers.

Strategy:
- `_FakeSession` records every SQL statement + params executed. Text values are
  matched by substring so tests stay resilient to whitespace.
- OpenSearch is reached through `httpx.AsyncClient`; we monkeypatch that with a
  tiny fake that records (method, url, body) and can be made to raise.

We assert:
  * dispatch routes each event_type to the right handler
  * each handler issues the expected upsert with the expected fields
  * OS failures are swallowed (no exception propagates)
  * same event processed twice is idempotent — final state matches single-shot
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

import pytest

from app import handlers, os_index


# --------------------------------------------------------------------------
# Fake SQLAlchemy session
# --------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> "_FakeResult":
        return self

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    def all(self) -> list[dict[str, Any]]:
        return list(self._rows)


class _FakeSession:
    def __init__(self) -> None:
        self.executed: list[tuple[str, dict[str, Any]]] = []
        # Map substring → rows to return on matching SELECT.
        self.select_responses: dict[str, list[dict[str, Any]]] = {}

    def set_select(self, needle: str, rows: list[dict[str, Any]]) -> None:
        self.select_responses[needle] = rows

    async def execute(self, clause: Any, params: dict[str, Any] | None = None) -> Any:
        sql = str(clause)
        self.executed.append((sql, params or {}))
        upper = sql.strip().upper()
        if upper.startswith("SELECT"):
            for needle, rows in self.select_responses.items():
                if needle in sql:
                    return _FakeResult(list(rows))
            return _FakeResult([])
        return None

    def begin(self) -> Any:
        @asynccontextmanager
        async def _cm() -> Any:
            yield self

        return _cm()

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *a: Any) -> None:
        return None

    def statements_containing(self, needle: str) -> list[tuple[str, dict[str, Any]]]:
        return [e for e in self.executed if needle in e[0]]


def _sm_factory(session: _FakeSession) -> Any:
    def _mk() -> _FakeSession:
        return session

    return _mk


# --------------------------------------------------------------------------
# Fake httpx client for OpenSearch
# --------------------------------------------------------------------------


class _FakeResp:
    def __init__(self, status: int = 200, text: str = "") -> None:
        self.status_code = status
        self.text = text


class _FakeClient:
    """Records outbound PUT/DELETE/HEAD and optionally raises."""

    def __init__(self, *, raise_exc: Exception | None = None, statuses: dict[str, int] | None = None) -> None:
        self.calls: list[tuple[str, str, str | None]] = []
        self._raise = raise_exc
        self._statuses = statuses or {}

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *a: Any) -> None:
        return None

    async def head(self, url: str, **_: Any) -> _FakeResp:
        self.calls.append(("HEAD", url, None))
        if self._raise:
            raise self._raise
        return _FakeResp(self._statuses.get("HEAD", 200))

    async def put(self, url: str, content: str | None = None, **_: Any) -> _FakeResp:
        self.calls.append(("PUT", url, content))
        if self._raise:
            raise self._raise
        return _FakeResp(self._statuses.get("PUT", 200))

    async def delete(self, url: str, **_: Any) -> _FakeResp:
        self.calls.append(("DELETE", url, None))
        if self._raise:
            raise self._raise
        return _FakeResp(self._statuses.get("DELETE", 200))


@pytest.fixture
def fake_http(monkeypatch: pytest.MonkeyPatch) -> _FakeClient:
    client = _FakeClient()

    def _factory(*_a: Any, **_kw: Any) -> _FakeClient:
        return client

    monkeypatch.setattr(os_index.httpx, "AsyncClient", _factory)
    return client


@pytest.fixture
def failing_http(monkeypatch: pytest.MonkeyPatch) -> _FakeClient:
    client = _FakeClient(raise_exc=ConnectionError("os down"))

    def _factory(*_a: Any, **_kw: Any) -> _FakeClient:
        return client

    monkeypatch.setattr(os_index.httpx, "AsyncClient", _factory)
    return client


# --------------------------------------------------------------------------
# Test payloads
# --------------------------------------------------------------------------

TENANT = "11111111-1111-1111-1111-111111111111"
EMP = "22222222-2222-2222-2222-222222222222"
MEM = "33333333-3333-3333-3333-333333333333"
PLAN = "44444444-4444-4444-4444-444444444444"
ENR = "55555555-5555-5555-5555-555555555555"

OS_URL = "http://localhost:9200"


def _member_event() -> dict[str, Any]:
    return {
        "event_id": "e1",
        "event_type": "MemberUpserted",
        "tenant_id": TENANT,
        "emitted_at": "2026-04-14T00:00:00Z",
        "member_id": MEM,
        "employer_id": EMP,
        "card_number": "C-001",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "dob": "1815-12-10",
        "gender": "F",
        "ssn_last4": "1234",
    }


def _plan_event() -> dict[str, Any]:
    return {
        "event_id": "e2",
        "event_type": "PlanUpserted",
        "tenant_id": TENANT,
        "emitted_at": "2026-04-14T00:00:00Z",
        "plan_id": PLAN,
        "plan_code": "HMO-GOLD",
        "name": "Gold HMO",
        "type": "medical",
    }


def _employer_event() -> dict[str, Any]:
    return {
        "event_id": "e3",
        "event_type": "EmployerUpserted",
        "tenant_id": TENANT,
        "emitted_at": "2026-04-14T00:00:00Z",
        "employer_id": EMP,
        "payer_id": "P-99",
        "name": "Acme Corp",
    }


def _enrollment_added() -> dict[str, Any]:
    return {
        "event_id": "e4",
        "event_type": "EnrollmentAdded",
        "tenant_id": TENANT,
        "emitted_at": "2026-04-14T00:00:00Z",
        "enrollment_id": ENR,
        "employer_id": EMP,
        "member_id": MEM,
        "plan_id": PLAN,
        "relationship": "self",
        "valid_from": "2026-01-01",
        "valid_to": "2026-12-31",
        "status": "active",
    }


def _enrollment_changed() -> dict[str, Any]:
    return {
        "event_id": "e5",
        "event_type": "EnrollmentChanged",
        "tenant_id": TENANT,
        "emitted_at": "2026-04-14T00:00:00Z",
        "enrollment_id": ENR,
        "changes": {"valid_to": "2026-06-30", "plan_id": PLAN},
    }


def _enrollment_terminated() -> dict[str, Any]:
    return {
        "event_id": "e6",
        "event_type": "EnrollmentTerminated",
        "tenant_id": TENANT,
        "emitted_at": "2026-04-14T00:00:00Z",
        "enrollment_id": ENR,
        "valid_to": "2026-05-31",
    }


# --------------------------------------------------------------------------
# Dispatcher routing
# --------------------------------------------------------------------------


async def test_dispatch_routes_each_event_type_to_right_handler(
    fake_http: _FakeClient,
) -> None:
    sent: list[str] = []

    async def _fake_member(session: Any, payload: Any, *, os_url: Any) -> None:
        sent.append("MemberUpserted")

    async def _fake_plan(session: Any, payload: Any, *, os_url: Any) -> None:
        sent.append("PlanUpserted")

    async def _fake_employer(session: Any, payload: Any, *, os_url: Any) -> None:
        sent.append("EmployerUpserted")

    async def _fake_added(session: Any, payload: Any, *, os_url: Any) -> None:
        sent.append("EnrollmentAdded")

    async def _fake_changed(session: Any, payload: Any, *, os_url: Any) -> None:
        sent.append("EnrollmentChanged")

    async def _fake_term(session: Any, payload: Any, *, os_url: Any) -> None:
        sent.append("EnrollmentTerminated")

    original = dict(handlers.EVENT_HANDLERS)
    handlers.EVENT_HANDLERS.update(
        {
            "MemberUpserted": _fake_member,
            "PlanUpserted": _fake_plan,
            "EmployerUpserted": _fake_employer,
            "EnrollmentAdded": _fake_added,
            "EnrollmentChanged": _fake_changed,
            "EnrollmentTerminated": _fake_term,
        }
    )
    try:
        session = _FakeSession()
        dispatch = handlers.make_dispatch_handler(_sm_factory(session), os_url=OS_URL)
        for evt in (
            _member_event(),
            _plan_event(),
            _employer_event(),
            _enrollment_added(),
            _enrollment_changed(),
            _enrollment_terminated(),
        ):
            await dispatch(evt, {})
    finally:
        handlers.EVENT_HANDLERS.clear()
        handlers.EVENT_HANDLERS.update(original)

    assert sent == [
        "MemberUpserted",
        "PlanUpserted",
        "EmployerUpserted",
        "EnrollmentAdded",
        "EnrollmentChanged",
        "EnrollmentTerminated",
    ]


async def test_dispatch_unknown_event_type_is_noop() -> None:
    session = _FakeSession()
    dispatch = handlers.make_dispatch_handler(_sm_factory(session), os_url=None)
    await dispatch({"event_type": "Nonsense", "tenant_id": TENANT}, {})
    await dispatch({"no_type": True}, {})
    # Nothing routed through the EVENT_HANDLERS — so no session.begin() ran.
    assert session.executed == []


# --------------------------------------------------------------------------
# MemberUpserted
# --------------------------------------------------------------------------


async def test_member_upserted_writes_lookup_and_fans_out(fake_http: _FakeClient) -> None:
    session = _FakeSession()
    # Pretend there is one existing view row for this member, so fan-out hits OS.
    session.set_select(
        "FROM eligibility_view",
        [
            {
                "enrollment_id": ENR,
                "tenant_id": TENANT,
                "employer_id": EMP,
                "employer_name": "Acme Corp",
                "subgroup_name": None,
                "plan_id": PLAN,
                "plan_name": "Gold HMO",
                "plan_code": "HMO-GOLD",
                "member_id": MEM,
                "member_name": "Ada Lovelace",
                "first_name": "Ada",
                "last_name": "Lovelace",
                "dob": "1815-12-10",
                "gender": "F",
                "ssn_last4": "1234",
                "card_number": "C-001",
                "relationship": "self",
                "status": "active",
                "effective_date": "2026-01-01",
                "termination_date": "2026-12-31",
            }
        ],
    )

    await handlers.handle_member_upserted(session, _member_event(), os_url=OS_URL)

    # Lookup upsert executed.
    upserts = session.statements_containing("INSERT INTO members_lookup")
    assert len(upserts) == 1
    assert upserts[0][1]["member_id"] == MEM
    assert upserts[0][1]["first_name"] == "Ada"
    assert upserts[0][1]["ssn_last4"] == "1234"

    # Fan-out update on eligibility_view.
    assert session.statements_containing("UPDATE eligibility_view SET") != []

    # OpenSearch upsert called.
    puts = [c for c in fake_http.calls if c[0] == "PUT"]
    assert puts, "expected OpenSearch PUT"
    assert f"/_doc/{ENR}" in puts[0][1]
    body = json.loads(puts[0][2] or "{}")
    assert body["member_name"] == "Ada Lovelace"


# --------------------------------------------------------------------------
# PlanUpserted / EmployerUpserted
# --------------------------------------------------------------------------


async def test_plan_upserted_writes_lookup_and_fans_out(fake_http: _FakeClient) -> None:
    session = _FakeSession()
    await handlers.handle_plan_upserted(session, _plan_event(), os_url=OS_URL)
    inserts = session.statements_containing("INSERT INTO plans_lookup")
    assert len(inserts) == 1
    assert inserts[0][1]["plan_code"] == "HMO-GOLD"
    assert session.statements_containing("UPDATE eligibility_view SET") != []


async def test_employer_upserted_writes_lookup_and_fans_out(fake_http: _FakeClient) -> None:
    session = _FakeSession()
    await handlers.handle_employer_upserted(session, _employer_event(), os_url=OS_URL)
    inserts = session.statements_containing("INSERT INTO employers_lookup")
    assert len(inserts) == 1
    assert inserts[0][1]["payer_id"] == "P-99"


# --------------------------------------------------------------------------
# EnrollmentAdded
# --------------------------------------------------------------------------


async def test_enrollment_added_joins_lookups_and_upserts_view(fake_http: _FakeClient) -> None:
    session = _FakeSession()
    session.set_select(
        "FROM members_lookup",
        [
            {
                "member_id": MEM,
                "card_number": "C-001",
                "first_name": "Ada",
                "last_name": "Lovelace",
                "dob": "1815-12-10",
                "gender": "F",
                "ssn_last4": "1234",
                "employer_id": EMP,
            }
        ],
    )
    session.set_select(
        "FROM plans_lookup",
        [{"plan_id": PLAN, "plan_code": "HMO-GOLD", "name": "Gold HMO"}],
    )
    session.set_select(
        "FROM employers_lookup",
        [{"employer_id": EMP, "payer_id": "P-99", "name": "Acme Corp"}],
    )

    await handlers.handle_enrollment_added(session, _enrollment_added(), os_url=OS_URL)

    inserts = session.statements_containing("INSERT INTO eligibility_view")
    assert len(inserts) == 1
    params = inserts[0][1]
    assert params["enrollment_id"] == ENR
    assert params["employer_name"] == "Acme Corp"
    assert params["plan_code"] == "HMO-GOLD"
    assert params["member_name"] == "Ada Lovelace"
    assert params["status"] == "active"
    assert params["relationship"] == "self"
    # OS upsert called with the same enrollment id.
    puts = [c for c in fake_http.calls if c[0] == "PUT" and "/_doc/" in c[1]]
    assert puts
    body = json.loads(puts[0][2] or "{}")
    assert body["enrollment_id"] == ENR
    assert body["employer_name"] == "Acme Corp"


async def test_enrollment_added_tolerates_missing_lookups(fake_http: _FakeClient) -> None:
    """Out-of-order events: no lookup rows yet — row still inserted with NULLs."""
    session = _FakeSession()
    await handlers.handle_enrollment_added(session, _enrollment_added(), os_url=OS_URL)
    inserts = session.statements_containing("INSERT INTO eligibility_view")
    assert len(inserts) == 1
    params = inserts[0][1]
    assert params["employer_name"] is None
    assert params["plan_name"] is None
    assert params["first_name"] is None
    # member_name is the empty-string concatenation, not None.
    assert params["member_name"] == ""


# --------------------------------------------------------------------------
# EnrollmentChanged / Terminated
# --------------------------------------------------------------------------


async def test_enrollment_changed_updates_mapped_fields(fake_http: _FakeClient) -> None:
    session = _FakeSession()
    session.set_select(
        "FROM plans_lookup",
        [{"plan_id": PLAN, "plan_code": "HMO-GOLD", "name": "Gold HMO"}],
    )
    await handlers.handle_enrollment_changed(session, _enrollment_changed(), os_url=OS_URL)
    updates = session.statements_containing("UPDATE eligibility_view SET")
    assert updates, "expected eligibility_view UPDATE"
    merged_params = updates[0][1]
    assert "termination_date" in merged_params
    assert "plan_id" in merged_params
    assert merged_params["plan_name"] == "Gold HMO"


async def test_enrollment_terminated_marks_termed_not_deleted(fake_http: _FakeClient) -> None:
    session = _FakeSession()
    await handlers.handle_enrollment_terminated(
        session, _enrollment_terminated(), os_url=OS_URL
    )
    updates = session.statements_containing("UPDATE eligibility_view")
    assert updates, "expected termination update"
    assert "status = 'termed'" in updates[0][0]
    # No DELETE — row stays searchable historically.
    assert not any("DELETE FROM eligibility_view" in e[0] for e in session.executed)


# --------------------------------------------------------------------------
# OpenSearch graceful degradation
# --------------------------------------------------------------------------


async def test_os_failure_is_swallowed(failing_http: _FakeClient) -> None:
    """OS client raises → handler still completes successfully (pg is authoritative)."""
    session = _FakeSession()
    session.set_select(
        "FROM members_lookup",
        [
            {
                "member_id": MEM,
                "card_number": "C-001",
                "first_name": "Ada",
                "last_name": "Lovelace",
                "dob": "1815-12-10",
                "gender": "F",
                "ssn_last4": "1234",
                "employer_id": EMP,
            }
        ],
    )
    session.set_select(
        "FROM plans_lookup",
        [{"plan_id": PLAN, "plan_code": "HMO-GOLD", "name": "Gold HMO"}],
    )
    session.set_select(
        "FROM employers_lookup",
        [{"employer_id": EMP, "payer_id": "P-99", "name": "Acme Corp"}],
    )
    # Should NOT raise.
    await handlers.handle_enrollment_added(session, _enrollment_added(), os_url=OS_URL)
    # The pg upsert still happened.
    assert session.statements_containing("INSERT INTO eligibility_view")


# --------------------------------------------------------------------------
# Idempotency
# --------------------------------------------------------------------------


async def test_enrollment_added_is_idempotent(fake_http: _FakeClient) -> None:
    """Same event processed twice yields same final row params."""
    # Twice
    results: list[dict[str, Any]] = []
    for _ in range(2):
        session = _FakeSession()
        session.set_select(
            "FROM members_lookup",
            [
                {
                    "member_id": MEM,
                    "card_number": "C-001",
                    "first_name": "Ada",
                    "last_name": "Lovelace",
                    "dob": "1815-12-10",
                    "gender": "F",
                    "ssn_last4": "1234",
                    "employer_id": EMP,
                }
            ],
        )
        session.set_select(
            "FROM plans_lookup",
            [{"plan_id": PLAN, "plan_code": "HMO-GOLD", "name": "Gold HMO"}],
        )
        session.set_select(
            "FROM employers_lookup",
            [{"employer_id": EMP, "payer_id": "P-99", "name": "Acme Corp"}],
        )
        await handlers.handle_enrollment_added(session, _enrollment_added(), os_url=OS_URL)
        inserts = session.statements_containing("INSERT INTO eligibility_view")
        assert inserts
        results.append(inserts[0][1])

    assert results[0] == results[1]
    # And the upsert uses ON CONFLICT DO UPDATE so it's idempotent in SQL too.
    assert "ON CONFLICT (enrollment_id) DO UPDATE" in (
        [e[0] for e in session.statements_containing("INSERT INTO eligibility_view")][0]
    )


async def test_member_upserted_is_idempotent(fake_http: _FakeClient) -> None:
    runs: list[dict[str, Any]] = []
    for _ in range(2):
        session = _FakeSession()
        await handlers.handle_member_upserted(session, _member_event(), os_url=OS_URL)
        ins = session.statements_containing("INSERT INTO members_lookup")[0]
        runs.append(ins[1])
    assert runs[0] == runs[1]
