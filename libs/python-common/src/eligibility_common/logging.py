"""Structured JSON logging with automatic correlation-id + tenant-id binding.

Usage:
    from eligibility_common.logging import configure_logging, get_logger, bind_context
    configure_logging()
    log = get_logger(__name__)
    with bind_context(correlation_id="x", tenant_id="y"):
        log.info("hello", foo=1)
"""
from __future__ import annotations

import logging
import os
import sys
from contextlib import contextmanager
from typing import Any, Iterator

import structlog

_SENSITIVE_KEYS = {"ssn", "password", "authorization", "token", "secret"}


def _scrub_phi(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Remove obvious PHI before emission. Defense-in-depth; never rely on this alone."""
    for k in list(event_dict.keys()):
        lower = k.lower()
        if any(s in lower for s in _SENSITIVE_KEYS):
            event_dict[k] = "***"
    return event_dict


def configure_logging(service_name: str | None = None, level: str | None = None) -> None:
    level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(stream=sys.stdout, level=level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            _scrub_phi,
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level, 20)),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
    if service_name:
        structlog.contextvars.bind_contextvars(service=service_name)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)


@contextmanager
def bind_context(**kwargs: Any) -> Iterator[None]:
    """Temporarily bind kwargs into structlog contextvars. Cleans up on exit."""
    token = structlog.contextvars.bind_contextvars(**kwargs)
    try:
        yield
    finally:
        structlog.contextvars.unbind_contextvars(*kwargs.keys())
        _ = token
