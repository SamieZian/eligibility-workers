"""Tests for the Vertex AI Document AI adapter + handler fallback.

These tests are designed to pass *without* ``google-cloud-documentai``
installed. The real client call is never made — we only exercise:

    1. ``is_enabled()`` returns False when env vars are missing.
    2. ``_normalize`` turns a fake Document proto-shaped object into rows.
    3. The handler logs ``pdf_scanned_without_docai`` and does not crash
       when a PDF arrives on an environment that hasn't opted in.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from app import document_ai
from app.handler import handle_file


# ---------------------------------------------------------------------------
# is_enabled / config gating
# ---------------------------------------------------------------------------

def test_disabled_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no processor id / project id, the feature is off by default."""
    monkeypatch.delenv("VERTEX_AI_DOCUMENT_PROCESSOR_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)

    assert document_ai.is_enabled() is False
    assert document_ai.DocumentAIConfig.from_env() is None


def test_is_scanned_mime_matches_pdf_and_images() -> None:
    assert document_ai.is_scanned_mime("application/pdf") is True
    assert document_ai.is_scanned_mime("image/png") is True
    assert document_ai.is_scanned_mime("image/tiff") is True
    assert document_ai.is_scanned_mime("text/csv") is False
    assert document_ai.is_scanned_mime(None) is False
    assert document_ai.is_scanned_mime("") is False


# ---------------------------------------------------------------------------
# _normalize — pragmatic skeleton mapping
# ---------------------------------------------------------------------------

def test_normalize_skeleton() -> None:
    """One ``enrollment_record`` entity → one row keyed by property ``type_``."""

    def prop(type_: str, mention_text: str) -> SimpleNamespace:
        return SimpleNamespace(type_=type_, mention_text=mention_text)

    doc = SimpleNamespace(
        entities=[
            SimpleNamespace(
                type_="enrollment_record",
                properties=[
                    prop("subscriber_ref", "SUB-001"),
                    prop("first_name", "PRIYA"),
                    prop("last_name", "SHARMA"),
                    prop("dob", "1990-02-15"),
                    prop("plan_code", "PLAN-GOLD"),
                    prop("effective_date", "2026-01-01"),
                ],
            ),
            # non-enrollment entity must be ignored
            SimpleNamespace(type_="page_header", properties=[prop("x", "y")]),
        ]
    )

    rows = document_ai._normalize(doc)
    assert len(rows) == 1
    row = rows[0]
    assert row["subscriber_ref"] == "SUB-001"
    assert row["first_name"] == "PRIYA"
    assert row["last_name"] == "SHARMA"
    assert row["dob"] == "1990-02-15"
    assert row["plan_code"] == "PLAN-GOLD"
    assert row["effective_date"] == "2026-01-01"


def test_normalize_empty_doc_returns_no_rows() -> None:
    assert document_ai._normalize(SimpleNamespace(entities=[])) == []
    assert document_ai._normalize(SimpleNamespace()) == []


# ---------------------------------------------------------------------------
# Handler fallback when Document AI is disabled
# ---------------------------------------------------------------------------

class _FakeStorage:
    def __init__(self, blob: bytes) -> None:
        self.blob = blob
        self.calls: list[str] = []

    async def download(self, object_key: str) -> bytes:
        self.calls.append(object_key)
        return self.blob


def _settings() -> Any:
    return SimpleNamespace(
        atlas_url="http://atlas",
        member_url="http://member",
        group_url="http://group",
        plan_url="http://plan",
        http_timeout_seconds=5.0,
    )


def _pdf_event(fmt: str = "X12_834") -> dict[str, Any]:
    # ``format`` must still validate against the FileReceived enum; the
    # scanned-PDF branch is triggered by the mime_type attribute, not format.
    return {
        "event_id": str(uuid4()),
        "event_type": "FileReceived",
        "tenant_id": str(uuid4()),
        "correlation_id": "corr-pdf",
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "file_id": str(uuid4()),
        "format": fmt,
        "object_key": "tenant/scan.pdf",
        "trading_partner_id": "TP-FAX",
    }


@pytest.mark.asyncio
async def test_handler_falls_back_gracefully(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """With Doc AI disabled, a scanned-PDF delivery must not crash the worker.

    The handler should emit the ``pdf_scanned_without_docai`` structured log
    and finish with ``total=0`` (nothing to send downstream).
    """
    monkeypatch.delenv("VERTEX_AI_DOCUMENT_PROCESSOR_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    assert document_ai.is_enabled() is False

    storage = _FakeStorage(b"%PDF-1.4 fake scanned 834 bytes")

    captured: list[tuple[str, dict[str, Any]]] = []

    def _spy(event: str, **kwargs: Any) -> None:
        captured.append((event, kwargs))

    # Patch the module-level logger used inside handler.py so we can assert
    # on structured events without depending on structlog's test harness.
    import app.handler as handler_mod

    monkeypatch.setattr(
        handler_mod.log,
        "warning",
        lambda event, **kw: _spy(event, **kw),
    )
    monkeypatch.setattr(
        handler_mod.log,
        "info",
        lambda event, **kw: _spy(event, **kw),
    )

    # Use an X12_834 *format* tag but a PDF mime_type attribute — a common
    # real-world pattern: the uploader knows it's an 834 "in principle" but
    # the blob is actually a fax scan. The branch is chosen by mime.
    # To force the Document-AI branch we pass format=CSV-lookalike... but
    # FileReceived only allows X12_834/CSV/XLSX. So we use XLSX which hits
    # the ``else`` branch — then the mime check runs.
    payload = _pdf_event(fmt="XLSX")

    await handle_file(
        payload,
        {"mime_type": "application/pdf"},
        storage=storage,
        settings=_settings(),
    )

    events = [e for e, _ in captured]
    assert "ingestion.pdf_scanned_without_docai" in events
    # File downloaded but no instructions processed (no downstream HTTP made).
    assert storage.calls == ["tenant/scan.pdf"]
