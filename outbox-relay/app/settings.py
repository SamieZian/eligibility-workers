from pydantic import Field
from eligibility_common.settings import CommonSettings


class Settings(CommonSettings):
    service_name: str = "outbox-relay"
    poll_interval_seconds: float = Field(1.0, alias="POLL_INTERVAL_SECONDS")
    batch_size: int = Field(100, alias="BATCH_SIZE")
    atlas_db_url: str = Field(..., alias="ATLAS_DB_URL")
    member_db_url: str = Field(..., alias="MEMBER_DB_URL")
    group_db_url: str = Field(..., alias="GROUP_DB_URL")
    plan_db_url: str = Field(..., alias="PLAN_DB_URL")


settings = Settings()
