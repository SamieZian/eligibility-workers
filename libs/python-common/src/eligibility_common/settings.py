"""Base Pydantic settings. Services subclass to add their own knobs."""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CommonSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = Field("unknown", alias="SERVICE_NAME")
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    otel_endpoint: str | None = Field(None, alias="OTEL_EXPORTER_OTLP_ENDPOINT")
    database_url: str | None = Field(None, alias="DATABASE_URL")
    redis_url: str = Field("redis://localhost:6379/0", alias="REDIS_URL")
    pubsub_project_id: str = Field("local-eligibility", alias="PUBSUB_PROJECT_ID")
    pubsub_emulator_host: str | None = Field(None, alias="PUBSUB_EMULATOR_HOST")
    tenant_default: str = Field("11111111-1111-1111-1111-111111111111", alias="TENANT_DEFAULT")
