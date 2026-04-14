"""Read-model DDL + upsert helpers for the projector worker.

The `eligibility_view` table is the denormalized row used by the BFF read API.
Three small lookup tables (`members_lookup`, `plans_lookup`, `employers_lookup`)
cache the latest known state of the upstream aggregates so that `EnrollmentAdded`
events can synthesize a full denormalized row without synchronous cross-service
lookups.

All helpers are idempotent — same event processed twice yields the same final
row state.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# --- Extension (pg_trgm) needs to run first since the GIN index depends on it.
ENABLE_TRGM_DDL = "CREATE EXTENSION IF NOT EXISTS pg_trgm;"

READ_MODEL_DDL_STATEMENTS = [
    """
CREATE TABLE IF NOT EXISTS eligibility_view (
  enrollment_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  employer_id UUID NOT NULL,
  employer_name TEXT,
  subgroup_name TEXT,
  plan_id UUID NOT NULL,
  plan_name TEXT,
  plan_code TEXT,
  member_id UUID NOT NULL,
  member_name TEXT,
  first_name TEXT,
  last_name TEXT,
  dob DATE,
  gender TEXT,
  ssn_last4 TEXT,
  card_number TEXT,
  relationship TEXT NOT NULL,
  status TEXT NOT NULL,
  effective_date DATE NOT NULL,
  termination_date DATE NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
""",
    "CREATE INDEX IF NOT EXISTS ev_tenant_employer_status ON eligibility_view (tenant_id, employer_id, status, effective_date DESC)",
    "CREATE INDEX IF NOT EXISTS ev_card ON eligibility_view (card_number)",
    "CREATE INDEX IF NOT EXISTS ev_name_trgm ON eligibility_view USING gin (lower(member_name) gin_trgm_ops)",
]

LOOKUP_DDL_STATEMENTS = [
    """
CREATE TABLE IF NOT EXISTS plans_lookup (
  plan_id UUID PRIMARY KEY,
  plan_code TEXT,
  name TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
""",
    """
CREATE TABLE IF NOT EXISTS employers_lookup (
  employer_id UUID PRIMARY KEY,
  payer_id TEXT,
  name TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
""",
    """
CREATE TABLE IF NOT EXISTS members_lookup (
  member_id UUID PRIMARY KEY,
  card_number TEXT,
  first_name TEXT,
  last_name TEXT,
  dob DATE,
  gender TEXT,
  ssn_last4 TEXT,
  employer_id UUID,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
""",
]

# Legacy single-string versions for tests / sync-driver callers.
READ_MODEL_DDL = ";\n".join(READ_MODEL_DDL_STATEMENTS)
LOOKUP_DDL = ";\n".join(LOOKUP_DDL_STATEMENTS)


async def apply_ddl(session: AsyncSession) -> None:
    """Run DDL idempotently. `pg_trgm` must exist before the GIN index."""
    await session.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    for stmt in READ_MODEL_DDL_STATEMENTS:
        await session.execute(text(stmt))
    for stmt in LOOKUP_DDL_STATEMENTS:
        await session.execute(text(stmt))


# --- Lookup upserts ---------------------------------------------------------

UPSERT_PLAN_SQL = """
INSERT INTO plans_lookup (plan_id, plan_code, name, updated_at)
VALUES (:plan_id, :plan_code, :name, now())
ON CONFLICT (plan_id) DO UPDATE SET
  plan_code = EXCLUDED.plan_code,
  name = EXCLUDED.name,
  updated_at = now();
"""

UPSERT_EMPLOYER_SQL = """
INSERT INTO employers_lookup (employer_id, payer_id, name, updated_at)
VALUES (:employer_id, :payer_id, :name, now())
ON CONFLICT (employer_id) DO UPDATE SET
  payer_id = EXCLUDED.payer_id,
  name = EXCLUDED.name,
  updated_at = now();
"""

UPSERT_MEMBER_SQL = """
INSERT INTO members_lookup (
  member_id, card_number, first_name, last_name, dob, gender, ssn_last4,
  employer_id, updated_at
) VALUES (
  :member_id, :card_number, :first_name, :last_name, :dob, :gender, :ssn_last4,
  :employer_id, now()
)
ON CONFLICT (member_id) DO UPDATE SET
  card_number = EXCLUDED.card_number,
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name,
  dob = EXCLUDED.dob,
  gender = EXCLUDED.gender,
  ssn_last4 = EXCLUDED.ssn_last4,
  employer_id = EXCLUDED.employer_id,
  updated_at = now();
"""


async def upsert_plan_lookup(session: AsyncSession, *, plan_id: str, plan_code: str, name: str) -> None:
    await session.execute(
        text(UPSERT_PLAN_SQL),
        {"plan_id": plan_id, "plan_code": plan_code, "name": name},
    )


async def upsert_employer_lookup(
    session: AsyncSession, *, employer_id: str, payer_id: str, name: str
) -> None:
    await session.execute(
        text(UPSERT_EMPLOYER_SQL),
        {"employer_id": employer_id, "payer_id": payer_id, "name": name},
    )


async def upsert_member_lookup(
    session: AsyncSession,
    *,
    member_id: str,
    card_number: str | None,
    first_name: str,
    last_name: str,
    dob: Any,
    gender: str | None,
    ssn_last4: str | None,
    employer_id: str,
) -> None:
    await session.execute(
        text(UPSERT_MEMBER_SQL),
        {
            "member_id": member_id,
            "card_number": card_number,
            "first_name": first_name,
            "last_name": last_name,
            "dob": dob,
            "gender": gender,
            "ssn_last4": ssn_last4,
            "employer_id": employer_id,
        },
    )


# --- Eligibility view upsert -----------------------------------------------

UPSERT_VIEW_SQL = """
INSERT INTO eligibility_view (
  enrollment_id, tenant_id, employer_id, employer_name, subgroup_name,
  plan_id, plan_name, plan_code, member_id, member_name, first_name, last_name,
  dob, gender, ssn_last4, card_number, relationship, status,
  effective_date, termination_date, updated_at
) VALUES (
  :enrollment_id, :tenant_id, :employer_id, :employer_name, :subgroup_name,
  :plan_id, :plan_name, :plan_code, :member_id, :member_name, :first_name, :last_name,
  :dob, :gender, :ssn_last4, :card_number, :relationship, :status,
  :effective_date, :termination_date, now()
)
ON CONFLICT (enrollment_id) DO UPDATE SET
  tenant_id = EXCLUDED.tenant_id,
  employer_id = EXCLUDED.employer_id,
  employer_name = EXCLUDED.employer_name,
  subgroup_name = EXCLUDED.subgroup_name,
  plan_id = EXCLUDED.plan_id,
  plan_name = EXCLUDED.plan_name,
  plan_code = EXCLUDED.plan_code,
  member_id = EXCLUDED.member_id,
  member_name = EXCLUDED.member_name,
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name,
  dob = EXCLUDED.dob,
  gender = EXCLUDED.gender,
  ssn_last4 = EXCLUDED.ssn_last4,
  card_number = EXCLUDED.card_number,
  relationship = EXCLUDED.relationship,
  status = EXCLUDED.status,
  effective_date = EXCLUDED.effective_date,
  termination_date = EXCLUDED.termination_date,
  updated_at = now();
"""


async def upsert_eligibility_view(session: AsyncSession, row: dict[str, Any]) -> None:
    await session.execute(text(UPSERT_VIEW_SQL), row)


# --- Fetch helpers for join-on-write ---------------------------------------

FETCH_MEMBER_SQL = "SELECT * FROM members_lookup WHERE member_id = :member_id"
FETCH_PLAN_SQL = "SELECT * FROM plans_lookup WHERE plan_id = :plan_id"
FETCH_EMPLOYER_SQL = "SELECT * FROM employers_lookup WHERE employer_id = :employer_id"

UPDATE_VIEW_MEMBER_SQL = """
UPDATE eligibility_view SET
  first_name = :first_name,
  last_name = :last_name,
  member_name = :member_name,
  dob = :dob,
  gender = :gender,
  ssn_last4 = :ssn_last4,
  card_number = :card_number,
  updated_at = now()
WHERE member_id = CAST(:member_id AS UUID)
"""

UPDATE_VIEW_PLAN_SQL = """
UPDATE eligibility_view SET
  plan_code = :plan_code,
  plan_name = :name,
  updated_at = now()
WHERE plan_id = CAST(:plan_id AS UUID)
"""

UPDATE_VIEW_EMPLOYER_SQL = """
UPDATE eligibility_view SET
  employer_name = :name,
  updated_at = now()
WHERE employer_id = CAST(:employer_id AS UUID)
"""

UPDATE_ENROLLMENT_CHANGES_SQL_PREFIX = "UPDATE eligibility_view SET updated_at = now()"

UPDATE_ENROLLMENT_TERMED_SQL = """
UPDATE eligibility_view SET
  status = 'termed',
  termination_date = :termination_date,
  updated_at = now()
WHERE enrollment_id = :enrollment_id
RETURNING enrollment_id, tenant_id, employer_id, employer_name, subgroup_name,
          plan_id, plan_name, plan_code, member_id, member_name, first_name,
          last_name, dob, gender, ssn_last4, card_number, relationship, status,
          effective_date, termination_date
"""

FETCH_VIEW_BY_ID_SQL = """
SELECT enrollment_id, tenant_id, employer_id, employer_name, subgroup_name,
       plan_id, plan_name, plan_code, member_id, member_name, first_name,
       last_name, dob, gender, ssn_last4, card_number, relationship, status,
       effective_date, termination_date
FROM eligibility_view
WHERE enrollment_id = :enrollment_id
"""

FETCH_VIEW_BY_MEMBER_SQL = """
SELECT enrollment_id, tenant_id, employer_id, employer_name, subgroup_name,
       plan_id, plan_name, plan_code, member_id, member_name, first_name,
       last_name, dob, gender, ssn_last4, card_number, relationship, status,
       effective_date, termination_date
FROM eligibility_view
WHERE member_id = :member_id
"""

FETCH_VIEW_BY_PLAN_SQL = """
SELECT enrollment_id, tenant_id, employer_id, employer_name, subgroup_name,
       plan_id, plan_name, plan_code, member_id, member_name, first_name,
       last_name, dob, gender, ssn_last4, card_number, relationship, status,
       effective_date, termination_date
FROM eligibility_view
WHERE plan_id = :plan_id
"""

FETCH_VIEW_BY_EMPLOYER_SQL = """
SELECT enrollment_id, tenant_id, employer_id, employer_name, subgroup_name,
       plan_id, plan_name, plan_code, member_id, member_name, first_name,
       last_name, dob, gender, ssn_last4, card_number, relationship, status,
       effective_date, termination_date
FROM eligibility_view
WHERE employer_id = :employer_id
"""


async def fetch_member(session: AsyncSession, member_id: str) -> dict[str, Any] | None:
    r = await session.execute(text(FETCH_MEMBER_SQL), {"member_id": member_id})
    row = r.mappings().first()
    return dict(row) if row else None


async def fetch_plan(session: AsyncSession, plan_id: str) -> dict[str, Any] | None:
    r = await session.execute(text(FETCH_PLAN_SQL), {"plan_id": plan_id})
    row = r.mappings().first()
    return dict(row) if row else None


async def fetch_employer(session: AsyncSession, employer_id: str) -> dict[str, Any] | None:
    r = await session.execute(text(FETCH_EMPLOYER_SQL), {"employer_id": employer_id})
    row = r.mappings().first()
    return dict(row) if row else None


async def fetch_view_by_id(session: AsyncSession, enrollment_id: str) -> dict[str, Any] | None:
    r = await session.execute(text(FETCH_VIEW_BY_ID_SQL), {"enrollment_id": enrollment_id})
    row = r.mappings().first()
    return dict(row) if row else None


async def fetch_views_by_member(session: AsyncSession, member_id: str) -> list[dict[str, Any]]:
    r = await session.execute(text(FETCH_VIEW_BY_MEMBER_SQL), {"member_id": member_id})
    return [dict(m) for m in r.mappings().all()]


async def fetch_views_by_plan(session: AsyncSession, plan_id: str) -> list[dict[str, Any]]:
    r = await session.execute(text(FETCH_VIEW_BY_PLAN_SQL), {"plan_id": plan_id})
    return [dict(m) for m in r.mappings().all()]


async def fetch_views_by_employer(session: AsyncSession, employer_id: str) -> list[dict[str, Any]]:
    r = await session.execute(text(FETCH_VIEW_BY_EMPLOYER_SQL), {"employer_id": employer_id})
    return [dict(m) for m in r.mappings().all()]
