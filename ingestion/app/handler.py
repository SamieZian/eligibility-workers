"""Handler for ``file.received`` Pub/Sub messages.

Flow per message:
    1. Validate the payload against the ``FileReceived`` schema.
    2. Download the referenced object from MinIO.
    3. Parse it (X12 834 or CSV) into a stream of ``EnrollmentInstruction``.
    4. For each instruction, resolve employer / member / plan IDs and POST a
       ``CommandIn`` to the atlas service. A failure on one instruction is
       logged and skipped — it must not bring down the whole file.

Global failures (bad payload shape, download error) are re-raised so the
Pub/Sub subscriber nacks the message; after ``max_delivery`` attempts it is
automatically DLQ'd.
"""
from __future__ import annotations

import csv
import io
from datetime import date, datetime
from typing import Any
from uuid import UUID

import httpx

from eligibility_common.events import FileReceived
from eligibility_common.logging import bind_context, get_logger

from x12_834 import EnrollmentInstruction, MaintenanceType, parse_834

from app.resolver import (
    get_employer_id_by_external,
    get_or_create_member,
    get_plan_id_by_code,
    post_atlas_command,
)

log = get_logger(__name__)


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        pass
    # Tolerate the YYYYMMDD compact form common in feeds.
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        return None


def _row_to_instruction(
    row: dict[str, str], *, position: int, trading_partner_id: str | None
) -> EnrollmentInstruction:
    """Map a CSV row to an EnrollmentInstruction."""
    maintenance = (row.get("maintenance") or "021").strip()
    tp = row.get("trading_partner_id") or trading_partner_id or ""
    subscriber_ref = row.get("subscriber_ref") or None
    group_ref = row.get("group_ref") or None
    seg_key = f"csv:{tp}:{subscriber_ref or ''}:{position}"
    return EnrollmentInstruction(
        sequence=position,
        isa_control="",
        gs_control="",
        st_control="",
        ins_position=position,
        segment_key=seg_key,
        subscriber_indicator="Y",
        relationship_code=(row.get("relationship") or "18").strip(),
        maintenance_type=MaintenanceType(maintenance),
        benefit_status_code="A",
        sponsor_ref=row.get("sponsor_ref") or None,
        subscriber_ref=subscriber_ref,
        group_ref=group_ref,
        first_name=row.get("first_name") or None,
        last_name=row.get("last_name") or None,
        dob=_parse_iso_date(row.get("dob")),
        gender=row.get("gender") or None,
        plan_code=row.get("plan_code") or None,
        coverage_level=row.get("coverage_level") or None,
        effective_date=_parse_iso_date(row.get("effective_date")),
        termination_date=_parse_iso_date(row.get("termination_date")),
        trading_partner_id=tp or None,
    )


def _iter_csv_instructions(
    blob: bytes, *, trading_partner_id: str | None
) -> list[EnrollmentInstruction]:
    text = blob.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    out: list[EnrollmentInstruction] = []
    for idx, row in enumerate(reader, start=1):
        out.append(
            _row_to_instruction(row, position=idx, trading_partner_id=trading_partner_id)
        )
    return out


def _build_command_payload(
    instruction: EnrollmentInstruction,
    *,
    tenant_id: UUID,
    employer_id: UUID,
    plan_id: UUID,
    member_id: UUID,
    source_file_id: UUID,
) -> dict[str, Any] | None:
    """Return the /commands JSON body for this instruction, or None to skip."""
    base: dict[str, Any] = {
        "tenant_id": str(tenant_id),
        "employer_id": str(employer_id),
        "plan_id": str(plan_id),
        "member_id": str(member_id),
        "source_file_id": str(source_file_id),
        "source_segment_ref": instruction.segment_key,
    }

    mt = instruction.maintenance_type
    if mt in (MaintenanceType.ADD, MaintenanceType.REINSTATE):
        if not instruction.effective_date:
            return None
        return {
            **base,
            "command_type": "ADD",
            "valid_from": instruction.effective_date.isoformat(),
            "valid_to": instruction.termination_date.isoformat()
            if instruction.termination_date
            else None,
        }
    if mt == MaintenanceType.CANCEL:
        if not instruction.termination_date:
            return None
        return {
            **base,
            "command_type": "TERMINATE",
            "valid_to": instruction.termination_date.isoformat(),
        }
    if mt in (MaintenanceType.CORRECTION, MaintenanceType.CHANGE):
        if not (instruction.effective_date and instruction.termination_date):
            return None
        return {
            **base,
            "command_type": "CORRECTION",
            "new_valid_from": instruction.effective_date.isoformat(),
            "new_valid_to": instruction.termination_date.isoformat(),
        }
    return None


async def _process_instruction(
    client: httpx.AsyncClient,
    instruction: EnrollmentInstruction,
    *,
    event: FileReceived,
    atlas_url: str,
    member_url: str,
    group_url: str,
    plan_url: str,
) -> None:
    tenant_id = UUID(event.tenant_id)
    file_id = UUID(event.file_id)

    external_employer = instruction.sponsor_ref or instruction.group_ref
    if not external_employer:
        raise ValueError("instruction missing sponsor_ref/group_ref")
    employer_id = await get_employer_id_by_external(client, group_url, external_employer)

    member_id = await get_or_create_member(
        client,
        member_url,
        tenant_id=tenant_id,
        employer_id=employer_id,
        instruction=instruction,
    )

    if not instruction.plan_code:
        raise ValueError("instruction missing plan_code")
    plan_id = await get_plan_id_by_code(client, plan_url, instruction.plan_code)

    payload = _build_command_payload(
        instruction,
        tenant_id=tenant_id,
        employer_id=employer_id,
        plan_id=plan_id,
        member_id=member_id,
        source_file_id=file_id,
    )
    if payload is None:
        log.info(
            "ingestion.instruction.skip",
            reason="unmapped_or_incomplete",
            maintenance=instruction.maintenance_type.value,
            segment_key=instruction.segment_key,
        )
        return

    body = await post_atlas_command(client, atlas_url, payload)
    log.info(
        "ingestion.instruction.ok",
        command_type=payload["command_type"],
        segment_key=instruction.segment_key,
        enrollment_ids=body.get("enrollment_ids"),
    )


async def handle_file(
    payload: dict[str, Any],
    attributes: dict[str, str],
    *,
    storage: Any,
    settings: Any,
    client_factory: Any | None = None,
) -> None:
    """Pub/Sub handler entry point.

    ``storage``, ``settings`` and ``client_factory`` are passed in explicitly
    so tests can substitute fakes without monkey-patching the module. The
    ``main`` module wires real instances via ``functools.partial``.
    """
    event = FileReceived.model_validate(payload)

    with bind_context(
        tenant_id=event.tenant_id,
        correlation_id=event.correlation_id or event.event_id,
        file_id=event.file_id,
    ):
        log.info("ingestion.file.start", object_key=event.object_key, format=event.format)

        # Global failure → nack (caller will retry / DLQ).
        blob = await storage.download(event.object_key)

        if event.format == "X12_834":
            stream = io.BytesIO(blob)
            instructions = list(
                parse_834(stream, trading_partner_id=event.trading_partner_id)
            )
        elif event.format == "CSV":
            instructions = _iter_csv_instructions(
                blob, trading_partner_id=event.trading_partner_id
            )
        else:
            raise ValueError(f"Unsupported format: {event.format}")

        total = len(instructions)
        ok = 0
        failed = 0

        async_client_cm = (
            client_factory(settings=settings)
            if client_factory is not None
            else httpx.AsyncClient(timeout=httpx.Timeout(settings.http_timeout_seconds))
        )

        async with async_client_cm as client:
            for instruction in instructions:
                try:
                    await _process_instruction(
                        client,
                        instruction,
                        event=event,
                        atlas_url=settings.atlas_url,
                        member_url=settings.member_url,
                        group_url=settings.group_url,
                        plan_url=settings.plan_url,
                    )
                    ok += 1
                except Exception as e:
                    failed += 1
                    log.warning(
                        "ingestion.instruction.failed",
                        segment_key=instruction.segment_key,
                        maintenance=instruction.maintenance_type.value,
                        error=str(e),
                    )
                    continue

        log.info(
            "ingestion.file.done",
            total=total,
            ok=ok,
            failed=failed,
            format=event.format,
        )
