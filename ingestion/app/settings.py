"""Ingestion worker settings."""
from __future__ import annotations

from eligibility_common.settings import CommonSettings
from pydantic import Field


class Settings(CommonSettings):
    service_name: str = "ingestion"

    atlas_url: str = Field(..., alias="ATLAS_URL")
    member_url: str = Field(..., alias="MEMBER_URL")
    group_url: str = Field(..., alias="GROUP_URL")
    plan_url: str = Field(..., alias="PLAN_URL")

    minio_endpoint: str = Field(..., alias="MINIO_ENDPOINT")
    minio_bucket: str = Field(..., alias="MINIO_BUCKET")
    minio_user: str = Field("minio", alias="MINIO_ROOT_USER")
    minio_password: str = Field("minio12345", alias="MINIO_ROOT_PASSWORD")

    http_timeout_seconds: float = Field(5.0, alias="HTTP_TIMEOUT_SECONDS")


def get_settings() -> Settings:
    """Lazy Settings accessor so imports don't require env vars at test time."""
    return Settings()  # type: ignore[call-arg]
