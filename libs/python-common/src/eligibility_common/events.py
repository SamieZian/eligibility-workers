"""Shared event schemas and topic names. Single source of truth."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class Topics:
    FILE_RECEIVED = "file.received"
    FILE_RECEIVED_DLQ = "file.received.dlq"
    ENROLLMENT = "enrollment.events"
    ENROLLMENT_DLQ = "enrollment.events.dlq"
    MEMBER = "member.events"
    MEMBER_DLQ = "member.events.dlq"
    GROUP = "group.events"
    GROUP_DLQ = "group.events.dlq"
    PLAN = "plan.events"
    PLAN_DLQ = "plan.events.dlq"


class BaseEvent(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: str
    event_type: str
    tenant_id: str
    correlation_id: str | None = None
    emitted_at: datetime


class FileReceived(BaseEvent):
    event_type: Literal["FileReceived"] = "FileReceived"
    file_id: str
    format: Literal["X12_834", "CSV", "XLSX"]
    object_key: str
    trading_partner_id: str | None = None


class EnrollmentAdded(BaseEvent):
    event_type: Literal["EnrollmentAdded"] = "EnrollmentAdded"
    enrollment_id: str
    employer_id: str
    member_id: str
    plan_id: str
    relationship: str
    valid_from: date
    valid_to: date | None = None
    status: str = "active"
    source_file_id: str | None = None
    source_segment_ref: str | None = None


class EnrollmentChanged(BaseEvent):
    event_type: Literal["EnrollmentChanged"] = "EnrollmentChanged"
    enrollment_id: str
    changes: dict[str, Any] = Field(default_factory=dict)


class EnrollmentTerminated(BaseEvent):
    event_type: Literal["EnrollmentTerminated"] = "EnrollmentTerminated"
    enrollment_id: str
    valid_to: date


class MemberUpserted(BaseEvent):
    event_type: Literal["MemberUpserted"] = "MemberUpserted"
    member_id: str
    employer_id: str
    card_number: str | None = None
    first_name: str
    last_name: str
    dob: date
    gender: str | None = None
    ssn_last4: str | None = None


class PlanUpserted(BaseEvent):
    event_type: Literal["PlanUpserted"] = "PlanUpserted"
    plan_id: str
    plan_code: str
    name: str
    type: str


class EmployerUpserted(BaseEvent):
    event_type: Literal["EmployerUpserted"] = "EmployerUpserted"
    employer_id: str
    payer_id: str
    name: str
