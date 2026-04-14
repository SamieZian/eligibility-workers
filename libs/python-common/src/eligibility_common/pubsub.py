"""Thin Pub/Sub wrapper working against both the GCP emulator and prod.

- `publish(topic, payload, attributes)` creates topic on first use.
- `subscribe(sub, topic, callback)` creates subscription + DLQ on first use,
  auto-acks on success, nacks on exception (Pub/Sub retries with backoff).
"""
from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from typing import Any

from google.api_core import exceptions as gexc
from google.cloud import pubsub_v1

from .logging import get_logger

log = get_logger(__name__)

PROJECT_ID = os.environ.get("PUBSUB_PROJECT_ID", "local-eligibility")


def _publisher() -> pubsub_v1.PublisherClient:
    return pubsub_v1.PublisherClient()


def _subscriber() -> pubsub_v1.SubscriberClient:
    return pubsub_v1.SubscriberClient()


def ensure_topic(topic: str) -> str:
    path = _publisher().topic_path(PROJECT_ID, topic)
    try:
        _publisher().create_topic(request={"name": path})
        log.info("pubsub.topic.created", topic=topic)
    except gexc.AlreadyExists:
        pass
    return path


def ensure_subscription(sub: str, topic: str, *, dlq_topic: str | None = None, max_delivery: int = 7) -> str:
    s = _subscriber()
    sub_path = s.subscription_path(PROJECT_ID, sub)
    topic_path = ensure_topic(topic)
    req: dict[str, Any] = {"name": sub_path, "topic": topic_path, "ack_deadline_seconds": 60}
    if dlq_topic:
        dlq_path = ensure_topic(dlq_topic)
        req["dead_letter_policy"] = {"dead_letter_topic": dlq_path, "max_delivery_attempts": max_delivery}
    try:
        s.create_subscription(request=req)
        log.info("pubsub.subscription.created", sub=sub, topic=topic)
    except gexc.AlreadyExists:
        pass
    return sub_path


def publish(topic: str, payload: dict[str, Any], attributes: dict[str, str] | None = None) -> str:
    path = ensure_topic(topic)
    data = json.dumps(payload).encode()
    future = _publisher().publish(path, data, **(attributes or {}))
    return future.result(timeout=30.0)


async def run_subscriber(
    sub: str,
    topic: str,
    handler: Callable[[dict[str, Any], dict[str, str]], Awaitable[None]],
    *,
    dlq_topic: str | None = None,
    max_concurrent: int = 50,
) -> None:
    """Run forever, invoking `handler(payload, attributes)` per message.

    Handler MUST be idempotent. Exceptions cause a nack → Pub/Sub retries with
    exponential backoff; after `max_delivery` attempts the message goes to DLQ.
    """
    sub_path = ensure_subscription(sub, topic, dlq_topic=dlq_topic)
    s = _subscriber()
    sem = asyncio.Semaphore(max_concurrent)
    loop = asyncio.get_event_loop()

    def _callback(msg: pubsub_v1.subscriber.message.Message) -> None:
        async def _run() -> None:
            async with sem:
                try:
                    payload = json.loads(msg.data.decode())
                    await handler(payload, dict(msg.attributes))
                    msg.ack()
                except Exception as e:
                    log.exception("pubsub.handler.error", sub=sub, error=str(e))
                    msg.nack()

        asyncio.run_coroutine_threadsafe(_run(), loop)

    flow_control = pubsub_v1.types.FlowControl(max_messages=max_concurrent)
    streaming = s.subscribe(sub_path, callback=_callback, flow_control=flow_control)
    log.info("pubsub.subscriber.started", sub=sub, topic=topic)
    try:
        await asyncio.get_event_loop().run_in_executor(None, streaming.result)
    finally:
        streaming.cancel()
