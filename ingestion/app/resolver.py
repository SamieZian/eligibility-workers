"""Resolvers that translate external 834 references to internal UUIDs.

Every call goes through ``httpx.AsyncClient`` with a tight timeout and is
wrapped in ``retry_async`` so transient 5xx / connection hiccups don't
instantly abort the file.
"""
from __future__ import annotations

from datetime import date
from typing import Any
from uuid import UUID

import httpx

from eligibility_common.errors import InfraError, NotFoundError
from eligibility_common.logging import get_logger
from eligibility_common.retry import retry_async

log = get_logger(__name__)


_RETRIABLE_EXC: tuple[type[BaseException], ...] = (
    InfraError,
    ConnectionError,
    TimeoutError,
    httpx.TransportError,
    httpx.TimeoutException,
)


def _default_timeout(seconds: float) -> httpx.Timeout:
    return httpx.Timeout(seconds, connect=seconds, read=seconds, write=seconds, pool=seconds)


async def _get(
    client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None
) -> httpx.Response:
    async def _do() -> httpx.Response:
        resp = await client.get(url, params=params)
        if resp.status_code >= 500:
            raise InfraError("DOWNSTREAM_UNAVAILABLE", f"GET {url} -> {resp.status_code}")
        return resp

    return await retry_async(_do, attempts=3, op=f"GET {url}", retry_on=_RETRIABLE_EXC)


async def _post(
    client: httpx.AsyncClient, url: str, json: dict[str, Any]
) -> httpx.Response:
    async def _do() -> httpx.Response:
        resp = await client.post(url, json=json)
        if resp.status_code >= 500:
            raise InfraError("DOWNSTREAM_UNAVAILABLE", f"POST {url} -> {resp.status_code}")
        return resp

    return await retry_async(_do, attempts=3, op=f"POST {url}", retry_on=_RETRIABLE_EXC)


async def get_employer_id_by_external(
    client: httpx.AsyncClient, group_url: str, external_id: str
) -> UUID:
    """Resolve an employer UUID via the group service's ``external_id`` filter."""
    resp = await _get(client, f"{group_url.rstrip('/')}/employers", params={"external_id": external_id})
    if resp.status_code == 404:
        raise NotFoundError("EMPLOYER_NOT_FOUND", f"No employer for external_id={external_id}")
    resp.raise_for_status()
    body = resp.json()
    # Endpoint returns a list[EmployerOut]; tolerate a bare object too.
    if isinstance(body, list):
        if not body:
            raise NotFoundError("EMPLOYER_NOT_FOUND", f"No employer for external_id={external_id}")
        employer = body[0]
    else:
        employer = body
    return UUID(str(employer["id"]))


async def get_plan_id_by_code(client: httpx.AsyncClient, plan_url: str, plan_code: str) -> UUID:
    """Resolve a plan UUID via the plan service's code lookup."""
    resp = await _get(client, f"{plan_url.rstrip('/')}/plans", params={"code": plan_code})
    if resp.status_code == 404:
        raise NotFoundError("PLAN_NOT_FOUND", f"No plan for code={plan_code}")
    resp.raise_for_status()
    body = resp.json()
    return UUID(str(body["id"]))


async def get_or_create_member(
    client: httpx.AsyncClient,
    member_url: str,
    *,
    tenant_id: UUID,
    employer_id: UUID,
    instruction: Any,
) -> UUID:
    """Return the member UUID, upserting when the card lookup 404s."""
    base = member_url.rstrip("/")
    card = instruction.subscriber_ref or instruction.ssn
    if card:
        resp = await _get(
            client,
            f"{base}/members",
            params={"cardNumber": card, "tenantId": str(tenant_id)},
        )
        if resp.status_code == 200:
            body = resp.json()
            return UUID(str(body["id"]))
        if resp.status_code not in (404,):
            resp.raise_for_status()

    # Need to POST an upsert — member svc requires first/last/dob.
    if not (instruction.first_name and instruction.last_name and instruction.dob):
        raise NotFoundError(
            "MEMBER_NOT_FOUND",
            f"Member {card!r} not found and demographics insufficient to create",
        )

    dob = instruction.dob
    payload: dict[str, Any] = {
        "tenant_id": str(tenant_id),
        "employer_id": str(employer_id),
        "first_name": instruction.first_name,
        "last_name": instruction.last_name,
        "dob": dob.isoformat() if isinstance(dob, date) else dob,
        "gender": instruction.gender,
        "card_number": card,
        "ssn": instruction.ssn,
    }
    # Drop Nones so server defaults apply.
    payload = {k: v for k, v in payload.items() if v is not None}

    resp = await _post(client, f"{base}/members", json=payload)
    resp.raise_for_status()
    body = resp.json()
    return UUID(str(body["id"]))


async def post_atlas_command(
    client: httpx.AsyncClient, atlas_url: str, payload: dict[str, Any]
) -> dict[str, Any]:
    """POST a CommandIn to the atlas service. Returns the decoded response body."""
    resp = await _post(client, f"{atlas_url.rstrip('/')}/commands", json=payload)
    resp.raise_for_status()
    return resp.json()
