"""Shared infrastructure for every eligibility service.

Keeps service code thin: logging, tracing, error envelope, retry, idempotency,
outbox, pubsub, redis, kms — all lives here behind narrow ports.
"""
from importlib.metadata import version

__version__ = "0.1.0"
_ = version  # keep import for tooling
