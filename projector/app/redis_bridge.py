"""Redis Pub/Sub publisher for live frontend refresh.

Mirrors the publish contract in eligibility-bff/app/pubsub_bridge.py — same
channel name, same JSON schema. We intentionally duplicate this tiny module
rather than thread a new cross-repo shared dep through libs/python-common;
the whole file is ~30 lines and the coupling is just a channel string."""
from __future__ import annotations

import json
from typing import Any

import redis.asyncio as redis

from eligibility_common.logging import get_logger

from .settings import settings

log = get_logger(__name__)
CHANNEL_ENROLLMENT = "enrollment_updates"

_pool: redis.ConnectionPool | None = None


def _client() -> redis.Redis:
    global _pool
    if _pool is None:
        _pool = redis.ConnectionPool.from_url(settings.redis_url)
    return redis.Redis(connection_pool=_pool)


async def publish_enrollment_update(member_id: str, payload: dict[str, Any]) -> None:
    """Best-effort publish. Projections must NEVER block on the Redis bridge —
    if Redis is unavailable we log and continue."""
    try:
        await _client().publish(
            CHANNEL_ENROLLMENT,
            json.dumps({"member_id": str(member_id), **payload}),
        )
    except Exception as e:  # noqa: BLE001
        log.warning("projector.redis_bridge.publish_failed", error=str(e))
