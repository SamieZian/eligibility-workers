"""Typed error hierarchy used across every service.

Rules:
- DomainError and its children are *expected* failures the client caused (4xx).
- ValidationError for input-shape issues (422).
- AuthzError for authentication/authorization (401/403).
- InfraError for unexpected failures (5xx) — never echo details to clients.
- Every error has a stable `code` string so Codex/clients can branch on it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AppError(Exception):
    code: str
    message: str
    http_status: int = 500
    retryable: bool = False
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


class DomainError(AppError):
    def __init__(self, code: str, message: str, **kw: Any) -> None:
        super().__init__(code=code, message=message, http_status=kw.pop("http_status", 409), **kw)


class ValidationError(AppError):
    def __init__(self, code: str, message: str, **kw: Any) -> None:
        super().__init__(code=code, message=message, http_status=422, **kw)


class AuthzError(AppError):
    def __init__(self, code: str, message: str, **kw: Any) -> None:
        super().__init__(code=code, message=message, http_status=403, **kw)


class NotFoundError(AppError):
    def __init__(self, code: str, message: str, **kw: Any) -> None:
        super().__init__(code=code, message=message, http_status=404, **kw)


class ConflictError(AppError):
    """Optimistic-lock / version mismatch. Client should re-fetch and retry."""

    def __init__(self, code: str = "VERSION_CONFLICT", message: str = "Version mismatch", **kw: Any) -> None:
        super().__init__(code=code, message=message, http_status=412, retryable=True, **kw)


class InfraError(AppError):
    def __init__(self, code: str, message: str, **kw: Any) -> None:
        super().__init__(code=code, message=message, http_status=503, retryable=True, **kw)


# Stable error codes — extend, don't rename.
class Codes:
    ENROLLMENT_OVERLAP = "ENROLLMENT_OVERLAP"
    PLAN_NOT_VISIBLE = "PLAN_NOT_VISIBLE"
    MEMBER_NOT_FOUND = "MEMBER_NOT_FOUND"
    DUPLICATE_SEGMENT = "DUPLICATE_SEGMENT"
    INVALID_834 = "INVALID_834"
    TENANT_MISMATCH = "TENANT_MISMATCH"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    RATE_LIMITED = "RATE_LIMITED"
    DOWNSTREAM_UNAVAILABLE = "DOWNSTREAM_UNAVAILABLE"
