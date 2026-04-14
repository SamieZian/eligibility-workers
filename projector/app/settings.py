"""Projector worker settings — CQRS read-model builder."""
from __future__ import annotations

from eligibility_common.settings import CommonSettings
from pydantic import Field


class Settings(CommonSettings):
    service_name: str = "projector"
    atlas_db_url: str = Field(..., alias="ATLAS_DB_URL")
    member_db_url: str = Field(..., alias="MEMBER_DB_URL")
    group_db_url: str = Field(..., alias="GROUP_DB_URL")
    plan_db_url: str = Field(..., alias="PLAN_DB_URL")
    opensearch_url: str = Field(..., alias="OPENSEARCH_URL")
    # We host the read-model view in atlas_db for simplicity (same physical DB as atlas).
    read_model_db_url: str = Field(..., alias="ATLAS_DB_URL")


settings = Settings()
