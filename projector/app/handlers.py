"""Domain-event handlers for the projector worker.

Each handler is idempotent: lookup tables use ON CONFLICT DO UPDATE, and the
eligibility_view upsert rewrites every column from the denormalized join. The
OpenSearch upsert is best-effort — if it raises, we log and continue (the
Postgres view is authoritative).

The dispatcher is driven by `event_type` on the pub/sub payload so a single
subscriber callback on a topic can route all event variants.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eligibility_common.logging import get_logger

from . import os_index, read_model
from .redis_bridge import publish_enrollment_update

log = get_logger(__name__)

# Sentinel date used when an enrollment has no termination — chosen for
# compatibility with the atlas domain which models bitemporal intervals.
OPEN_DATE = date(9999, 12, 31)


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _member_name(first: str | None, last: str | None) -> str:
    parts = [p for p in (first, last) if p]
    return " ".join(parts)


def _view_row_to_os_doc(row: dict[str, Any]) -> dict[str, Any]:
    """Project a SQL view row into the OpenSearch document shape."""
    return {
        "enrollment_id": str(row["enrollment_id"]),
        "tenant_id": str(row["tenant_id"]),
        "employer_id": str(row["employer_id"]),
        "employer_name": row.get("employer_name"),
        "plan_id": str(row["plan_id"]),
        "plan_code": row.get("plan_code"),
        "plan_name": row.get("plan_name"),
        "member_id": str(row["member_id"]),
        "member_name": row.get("member_name"),
        "first_name": row.get("first_name"),
        "last_name": row.get("last_name"),
        "card_number": row.get("card_number"),
        "ssn_last4": row.get("ssn_last4"),
        "dob": row.get("dob"),
        "status": row.get("status"),
        "effective_date": row.get("effective_date"),
        "termination_date": row.get("termination_date"),
        "relationship": row.get("relationship"),
    }


# --- Lookup upserts + fan-out ---------------------------------------------


async def handle_member_upserted(
    session: AsyncSession, payload: dict[str, Any], *, os_url: str | None
) -> None:
    """Update member lookup then refresh denormalized fields on existing view rows.

    Race note: when `addMember` fires both MemberUpserted and EnrollmentAdded
    near-simultaneously, the EnrollmentAdded handler may still be holding an
    uncommitted INSERT of the new view row when this handler's UPDATE fires.
    Under ``READ COMMITTED`` the UPDATE sees 0 rows. We mitigate with a
    bounded retry — sleep briefly, re-run the UPDATE, repeat up to N times.
    This keeps the system eventually-consistent without requiring Pub/Sub
    ordering keys or cross-session coordination.
    """
    member_id = payload["member_id"]
    first_name = payload["first_name"]
    last_name = payload["last_name"]
    member_name = _member_name(first_name, last_name)

    await read_model.upsert_member_lookup(
        session,
        member_id=member_id,
        card_number=payload.get("card_number"),
        first_name=first_name,
        last_name=last_name,
        dob=_parse_date(payload.get("dob")),
        gender=payload.get("gender"),
        ssn_last4=payload.get("ssn_last4"),
        employer_id=payload["employer_id"],
    )

    from sqlalchemy import text

    update_params = {
        "member_id": member_id,
        "first_name": first_name,
        "last_name": last_name,
        "member_name": member_name,
        "dob": _parse_date(payload.get("dob")),
        "gender": payload.get("gender"),
        "ssn_last4": payload.get("ssn_last4"),
        "card_number": payload.get("card_number"),
    }

    result = await session.execute(text(read_model.UPDATE_VIEW_MEMBER_SQL), update_params)
    rowcount = getattr(result, "rowcount", None) if result is not None else None
    if rowcount == 0:
        # Commit the lookup upsert so the EnrollmentAdded handler (which
        # reads members_lookup in its own transaction) can see it, then
        # retry the view update a few times as the view row lands.
        commit = getattr(session, "commit", None)
        if callable(commit):
            try:
                await commit()
            except Exception:  # noqa: BLE001 - fake sessions in tests may no-op
                pass
        for attempt in range(5):
            await asyncio.sleep(0.3 * (attempt + 1))
            retry = await session.execute(
                text(read_model.UPDATE_VIEW_MEMBER_SQL), update_params
            )
            retry_rc = getattr(retry, "rowcount", None) if retry is not None else None
            if retry_rc and retry_rc > 0:
                break
        else:
            log.info(
                "projector.member_upsert.no_view_row_yet",
                member_id=member_id,
                note="eligibility_view row not yet present — will be enriched by enrollment handler's COALESCE",
            )

    if os_url:
        for row in await read_model.fetch_views_by_member(session, member_id):
            await os_index.upsert(os_url, _view_row_to_os_doc(row))

    # Fan-out to Redis so any open frontend drawer can live-refresh.
    await publish_enrollment_update(
        member_id=str(member_id),
        payload={
            "event_type": "MemberUpserted",
            "occurred_at": datetime.utcnow().isoformat(),
        },
    )


async def handle_plan_upserted(
    session: AsyncSession, payload: dict[str, Any], *, os_url: str | None
) -> None:
    plan_id = payload["plan_id"]
    await read_model.upsert_plan_lookup(
        session,
        plan_id=plan_id,
        plan_code=payload["plan_code"],
        name=payload["name"],
    )
    from sqlalchemy import text

    await session.execute(
        text(read_model.UPDATE_VIEW_PLAN_SQL),
        {
            "plan_id": plan_id,
            "plan_code": payload["plan_code"],
            "name": payload["name"],
        },
    )
    if os_url:
        for row in await read_model.fetch_views_by_plan(session, plan_id):
            await os_index.upsert(os_url, _view_row_to_os_doc(row))


async def handle_employer_upserted(
    session: AsyncSession, payload: dict[str, Any], *, os_url: str | None
) -> None:
    employer_id = payload["employer_id"]
    await read_model.upsert_employer_lookup(
        session,
        employer_id=employer_id,
        payer_id=payload["payer_id"],
        name=payload["name"],
    )
    from sqlalchemy import text

    await session.execute(
        text(read_model.UPDATE_VIEW_EMPLOYER_SQL),
        {"employer_id": employer_id, "name": payload["name"]},
    )
    if os_url:
        for row in await read_model.fetch_views_by_employer(session, employer_id):
            await os_index.upsert(os_url, _view_row_to_os_doc(row))


# --- Enrollment lifecycle --------------------------------------------------


async def handle_enrollment_added(
    session: AsyncSession, payload: dict[str, Any], *, os_url: str | None
) -> None:
    """Insert a fully-denormalized view row by joining the three lookups.

    Missing lookup data (events out of order) is tolerated — fields are left
    NULL and will be backfilled when the upstream upsert arrives via the
    fan-out on `MemberUpserted` / `PlanUpserted` / `EmployerUpserted`.
    """
    enrollment_id = payload["enrollment_id"]
    member_id = payload["member_id"]
    plan_id = payload["plan_id"]
    employer_id = payload["employer_id"]

    member = await read_model.fetch_member(session, member_id) or {}
    plan = await read_model.fetch_plan(session, plan_id) or {}
    employer = await read_model.fetch_employer(session, employer_id) or {}

    first_name = member.get("first_name")
    last_name = member.get("last_name")

    effective_date = _parse_date(payload.get("valid_from"))
    termination_date = _parse_date(payload.get("valid_to")) or OPEN_DATE

    row: dict[str, Any] = {
        "enrollment_id": enrollment_id,
        "tenant_id": payload["tenant_id"],
        "employer_id": employer_id,
        "employer_name": employer.get("name"),
        "subgroup_name": payload.get("subgroup_name"),
        "plan_id": plan_id,
        "plan_name": plan.get("name"),
        "plan_code": plan.get("plan_code"),
        "member_id": member_id,
        "member_name": _member_name(first_name, last_name),
        "first_name": first_name,
        "last_name": last_name,
        "dob": member.get("dob"),
        "gender": member.get("gender"),
        "ssn_last4": member.get("ssn_last4"),
        "card_number": member.get("card_number"),
        "relationship": payload["relationship"],
        "status": payload.get("status", "active"),
        "effective_date": effective_date,
        "termination_date": termination_date,
    }
    await read_model.upsert_eligibility_view(session, row)

    # Backfill any still-null denormalized fields for THIS row. Handles the
    # race where MemberUpserted / PlanUpserted / EmployerUpserted may have
    # landed in parallel subscribers with a transaction that hadn't committed
    # when we fetched their lookup entries above.
    from sqlalchemy import text as _text

    await session.execute(
        _text(
            """
            UPDATE eligibility_view v SET
              first_name = COALESCE(v.first_name, m.first_name),
              last_name  = COALESCE(v.last_name,  m.last_name),
              member_name = COALESCE(NULLIF(v.member_name, ''), UPPER(m.first_name || ' ' || m.last_name)),
              card_number = COALESCE(v.card_number, m.card_number),
              dob = COALESCE(v.dob, m.dob),
              gender = COALESCE(v.gender, m.gender),
              ssn_last4 = COALESCE(v.ssn_last4, m.ssn_last4)
            FROM members_lookup m
            WHERE v.enrollment_id = CAST(:eid AS UUID) AND v.member_id = m.member_id
            """
        ),
        {"eid": enrollment_id},
    )
    await session.execute(
        _text(
            """
            UPDATE eligibility_view v SET
              plan_name = COALESCE(v.plan_name, p.name),
              plan_code = COALESCE(v.plan_code, p.plan_code)
            FROM plans_lookup p
            WHERE v.enrollment_id = CAST(:eid AS UUID) AND v.plan_id = p.plan_id
            """
        ),
        {"eid": enrollment_id},
    )
    await session.execute(
        _text(
            """
            UPDATE eligibility_view v SET
              employer_name = COALESCE(v.employer_name, e.name)
            FROM employers_lookup e
            WHERE v.enrollment_id = CAST(:eid AS UUID) AND v.employer_id = e.employer_id
            """
        ),
        {"eid": enrollment_id},
    )

    if os_url:
        await os_index.upsert(os_url, _view_row_to_os_doc(row))

    await publish_enrollment_update(
        member_id=str(member_id),
        payload={
            "event_type": "EnrollmentAdded",
            "occurred_at": datetime.utcnow().isoformat(),
        },
    )


async def handle_enrollment_changed(
    session: AsyncSession, payload: dict[str, Any], *, os_url: str | None
) -> None:
    """Apply a partial update to a view row; OS upserted afterwards."""
    enrollment_id = payload["enrollment_id"]
    changes = payload.get("changes") or {}

    mapped: dict[str, Any] = {}
    if "valid_from" in changes:
        mapped["effective_date"] = _parse_date(changes["valid_from"])
    if "valid_to" in changes:
        mapped["termination_date"] = _parse_date(changes["valid_to"]) or OPEN_DATE
    if "plan_id" in changes:
        mapped["plan_id"] = changes["plan_id"]
    if "status" in changes:
        mapped["status"] = changes["status"]
    if "relationship" in changes:
        mapped["relationship"] = changes["relationship"]

    if mapped:
        from sqlalchemy import text

        # If plan_id changed, also refresh plan_name/plan_code from lookup.
        if "plan_id" in mapped:
            plan = await read_model.fetch_plan(session, mapped["plan_id"]) or {}
            mapped["plan_name"] = plan.get("name")
            mapped["plan_code"] = plan.get("plan_code")

        set_clause = ", ".join(f"{k} = :{k}" for k in mapped)
        sql = (
            f"UPDATE eligibility_view SET {set_clause}, updated_at = now() "
            "WHERE enrollment_id = :enrollment_id"
        )
        params: dict[str, Any] = dict(mapped)
        params["enrollment_id"] = enrollment_id
        await session.execute(text(sql), params)

    row = await read_model.fetch_view_by_id(session, enrollment_id)
    if os_url and row:
        await os_index.upsert(os_url, _view_row_to_os_doc(row))

    if row:
        await publish_enrollment_update(
            member_id=str(row["member_id"]),
            payload={
                "event_type": "EnrollmentChanged",
                "occurred_at": datetime.utcnow().isoformat(),
            },
        )


async def handle_enrollment_terminated(
    session: AsyncSession, payload: dict[str, Any], *, os_url: str | None
) -> None:
    """Soft-terminate the enrollment — keep it searchable for historical queries."""
    from sqlalchemy import text

    enrollment_id = payload["enrollment_id"]
    termination_date = _parse_date(payload.get("valid_to"))
    await session.execute(
        text(
            """
            UPDATE eligibility_view SET
              status = 'termed',
              termination_date = :termination_date,
              updated_at = now()
            WHERE enrollment_id = :enrollment_id
            """
        ),
        {"enrollment_id": enrollment_id, "termination_date": termination_date},
    )
    row = await read_model.fetch_view_by_id(session, enrollment_id)
    if os_url and row:
        await os_index.upsert(os_url, _view_row_to_os_doc(row))

    if row:
        await publish_enrollment_update(
            member_id=str(row["member_id"]),
            payload={
                "event_type": "EnrollmentTerminated",
                "occurred_at": datetime.utcnow().isoformat(),
            },
        )


# --- Dispatcher ------------------------------------------------------------

HandlerFn = Callable[[AsyncSession, dict[str, Any]], Awaitable[None]]

EVENT_HANDLERS: dict[str, Callable[..., Awaitable[None]]] = {
    "MemberUpserted": handle_member_upserted,
    "PlanUpserted": handle_plan_upserted,
    "EmployerUpserted": handle_employer_upserted,
    "EnrollmentAdded": handle_enrollment_added,
    "EnrollmentChanged": handle_enrollment_changed,
    "EnrollmentTerminated": handle_enrollment_terminated,
}


def make_dispatch_handler(
    sm: async_sessionmaker[AsyncSession],
    *,
    os_url: str | None,
) -> Callable[[dict[str, Any], dict[str, str]], Awaitable[None]]:
    """Build a pub/sub handler that routes by `event_type` into the right handler.

    Raising propagates to `run_subscriber`, which nacks the message and triggers
    retry → DLQ. So: pg errors surface; OS errors are swallowed inside the
    individual handlers.
    """

    async def _handle(payload: dict[str, Any], _attributes: dict[str, str]) -> None:
        event_type = payload.get("event_type")
        if not event_type:
            log.warning("projector.event.missing_type", payload_keys=list(payload.keys()))
            return
        fn = EVENT_HANDLERS.get(event_type)
        if fn is None:
            log.debug("projector.event.unhandled", event_type=event_type)
            return
        async with sm() as session:
            async with session.begin():
                await fn(session, payload, os_url=os_url)
        log.info("projector.event.handled", event_type=event_type)

    return _handle
