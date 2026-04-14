"""OpenSearch index + doc upsert helpers for the projector.

OpenSearch is a read-side secondary — failures are logged and swallowed; the
Postgres `eligibility_view` remains authoritative.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import httpx

from eligibility_common.logging import get_logger

log = get_logger(__name__)

INDEX_NAME = "eligibility"

INDEX_MAPPING: dict[str, Any] = {
    "mappings": {
        "properties": {
            "enrollment_id": {"type": "keyword"},
            "tenant_id": {"type": "keyword"},
            "employer_id": {"type": "keyword"},
            "employer_name": {"type": "text"},
            "plan_id": {"type": "keyword"},
            "plan_code": {"type": "keyword"},
            "plan_name": {"type": "text"},
            "member_id": {"type": "keyword"},
            "member_name": {
                "type": "text",
                "fields": {"suggest": {"type": "completion"}},
            },
            "first_name": {"type": "text"},
            "last_name": {"type": "text"},
            "card_number": {"type": "keyword"},
            "ssn_last4": {"type": "keyword"},
            "dob": {"type": "date"},
            "status": {"type": "keyword"},
            "effective_date": {"type": "date"},
            "termination_date": {"type": "date"},
            "relationship": {"type": "keyword"},
        }
    }
}


def _json_default(value: Any) -> Any:
    """Serialize date/datetime as ISO8601; other non-serializables as str."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _serialize(doc: dict[str, Any]) -> dict[str, Any]:
    """Convert date/datetime fields to ISO strings (OS accepts both, but this
    keeps the wire format predictable for tests)."""
    out: dict[str, Any] = {}
    for k, v in doc.items():
        if isinstance(v, (date, datetime)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


async def ensure_index(base_url: str) -> None:
    """Create the `eligibility` index if missing. Idempotent."""
    url = f"{base_url.rstrip('/')}/{INDEX_NAME}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            head = await client.head(url)
            if head.status_code == 200:
                return
            resp = await client.put(
                url,
                content=json.dumps(INDEX_MAPPING, default=_json_default),
                headers={"Content-Type": "application/json"},
            )
            # 200/201 create, 400 may mean resource_already_exists
            if resp.status_code in (200, 201):
                log.info("os.index.created", index=INDEX_NAME)
                return
            if resp.status_code == 400 and "resource_already_exists" in resp.text:
                return
            log.warning(
                "os.index.create_failed",
                status=resp.status_code,
                body=resp.text[:200],
            )
    except Exception as e:  # noqa: BLE001
        log.warning("os.index.create_error", error=str(e))


async def upsert(base_url: str, doc: dict[str, Any]) -> None:
    """Index (upsert) a single document by enrollment_id. Failure swallowed."""
    enrollment_id = doc.get("enrollment_id")
    if not enrollment_id:
        log.warning("os.upsert.missing_enrollment_id")
        return
    url = f"{base_url.rstrip('/')}/{INDEX_NAME}/_doc/{enrollment_id}"
    body = _serialize(doc)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.put(
                url,
                content=json.dumps(body, default=_json_default),
                headers={"Content-Type": "application/json"},
            )
            if resp.status_code >= 400:
                log.warning(
                    "os.upsert.non_2xx",
                    enrollment_id=str(enrollment_id),
                    status=resp.status_code,
                    body=resp.text[:200],
                )
    except Exception as e:  # noqa: BLE001
        log.warning("os.upsert.error", enrollment_id=str(enrollment_id), error=str(e))


async def delete(base_url: str, enrollment_id: str) -> None:
    """Delete a document by id. Failure swallowed."""
    url = f"{base_url.rstrip('/')}/{INDEX_NAME}/_doc/{enrollment_id}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(url)
            if resp.status_code >= 400 and resp.status_code != 404:
                log.warning(
                    "os.delete.non_2xx",
                    enrollment_id=enrollment_id,
                    status=resp.status_code,
                    body=resp.text[:200],
                )
    except Exception as e:  # noqa: BLE001
        log.warning("os.delete.error", enrollment_id=enrollment_id, error=str(e))
