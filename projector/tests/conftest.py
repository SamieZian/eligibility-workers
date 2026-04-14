"""Populate the env vars the projector settings require BEFORE any app
module is imported. This keeps the test suite runnable on a clean clone
(no .env / docker-compose) — unit tests don't actually hit these URLs."""
from __future__ import annotations

import os

os.environ.setdefault("ATLAS_DB_URL", "postgresql+asyncpg://user:pw@localhost:5441/atlas_db")
os.environ.setdefault("MEMBER_DB_URL", "postgresql+asyncpg://user:pw@localhost:5442/member_db")
os.environ.setdefault("GROUP_DB_URL", "postgresql+asyncpg://user:pw@localhost:5443/group_db")
os.environ.setdefault("PLAN_DB_URL", "postgresql+asyncpg://user:pw@localhost:5444/plan_db")
os.environ.setdefault("OPENSEARCH_URL", "http://localhost:9200")
os.environ.setdefault("PUBSUB_PROJECT_ID", "local-test")
