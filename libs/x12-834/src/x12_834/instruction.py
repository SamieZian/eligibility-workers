"""Pydantic model + enum for a parsed 834 enrollment instruction."""

from __future__ import annotations

from datetime import date
from enum import Enum

from pydantic import BaseModel, ConfigDict


class MaintenanceType(str, Enum):
    """INS03 maintenance type codes used in 834 (5010X220A1)."""

    ADD = "021"
    CANCEL = "024"
    REINSTATE = "025"
    CHANGE = "001"
    CORRECTION = "030"


class EnrollmentInstruction(BaseModel):
    """A single INS loop (2000) flattened for downstream processing."""

    model_config = ConfigDict(frozen=False, extra="forbid")

    sequence: int
    isa_control: str
    gs_control: str
    st_control: str
    ins_position: int
    segment_key: str

    # INS segment fields
    subscriber_indicator: str
    relationship_code: str
    maintenance_type: MaintenanceType
    benefit_status_code: str

    # Reference identifiers (REF loop)
    sponsor_ref: str | None = None
    subscriber_ref: str | None = None
    group_ref: str | None = None

    # Name (NM1*IL) + identifier
    first_name: str | None = None
    last_name: str | None = None
    middle_name: str | None = None
    ssn: str | None = None

    # Demographics (DMG)
    dob: date | None = None
    gender: str | None = None

    # Health coverage (HD loop)
    plan_code: str | None = None
    coverage_level: str | None = None
    effective_date: date | None = None
    termination_date: date | None = None

    trading_partner_id: str | None = None
