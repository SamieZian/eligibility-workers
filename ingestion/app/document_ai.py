"""
Vertex AI Document AI adapter.

Extracts enrollment fields from scanned 834 PDFs / image faxes. Returns a
normalized list of row dicts that are shape-compatible with the CSV path, so
the rest of the ingestion pipeline stays unchanged.

Feature-gated. ``is_enabled()`` returns True only when:
    * ``google-cloud-documentai`` is installed (optional ``ai`` extra), AND
    * ``VERTEX_AI_DOCUMENT_PROCESSOR_ID`` + ``GOOGLE_CLOUD_PROJECT`` env vars
      are both set.

If either is missing the worker still boots — the caller skips the AI branch
and either falls back to the structured parser or logs and drops the file.
The goal here is a designed opt-in seam, not a production ML pipeline; the
per-payer entity schema is a TODO (see ``_normalize``).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable

try:
    from google.cloud import documentai_v1 as documentai  # type: ignore[import-not-found]

    _AVAILABLE = True
except ImportError:  # pragma: no cover - exercised when extra not installed
    documentai = None  # type: ignore[assignment]
    _AVAILABLE = False

from eligibility_common.logging import get_logger
from eligibility_common.retry import retry_async

log = get_logger(__name__)


@dataclass(frozen=True)
class DocumentAIConfig:
    """Resolved Document AI configuration.

    Built from env vars at call time (never at import) so unit tests can
    flip the feature on/off with ``monkeypatch.setenv``.
    """

    project_id: str
    location: str
    processor_id: str

    @classmethod
    def from_env(cls) -> "DocumentAIConfig | None":
        pid = os.getenv("VERTEX_AI_DOCUMENT_PROCESSOR_ID")
        project = os.getenv("GOOGLE_CLOUD_PROJECT")
        location = os.getenv("VERTEX_AI_LOCATION", "us")
        if not pid or not project or not _AVAILABLE:
            return None
        return cls(project_id=project, location=location, processor_id=pid)


def is_enabled() -> bool:
    """Return True iff the Document AI branch should be taken for this file."""
    return DocumentAIConfig.from_env() is not None


def is_scanned_mime(mime_type: str | None) -> bool:
    """PDF or image uploads are candidates for Document AI extraction."""
    if not mime_type:
        return False
    mt = mime_type.lower()
    return mt == "application/pdf" or mt.startswith("image/")


async def extract_enrollment_rows(
    file_bytes: bytes, mime_type: str
) -> Iterable[dict[str, Any]]:
    """Call Document AI and return normalized enrollment row dicts.

    Raises ``RuntimeError`` if the feature is disabled; any transport error
    from the client bubbles up so the caller can decide whether to fall back
    to the regex / structured parser.
    """
    cfg = DocumentAIConfig.from_env()
    if cfg is None:
        raise RuntimeError(
            "document_ai disabled — check VERTEX_AI_DOCUMENT_PROCESSOR_ID / "
            "GOOGLE_CLOUD_PROJECT and that the `ai` poetry extra is installed"
        )

    assert documentai is not None  # narrowed by is_enabled / cfg
    client = documentai.DocumentProcessorServiceAsyncClient()
    name = (
        f"projects/{cfg.project_id}/locations/{cfg.location}"
        f"/processors/{cfg.processor_id}"
    )
    raw_doc = documentai.RawDocument(content=file_bytes, mime_type=mime_type)
    request = documentai.ProcessRequest(name=name, raw_document=raw_doc)

    log.info(
        "ingestion.document_ai.call",
        processor=cfg.processor_id,
        location=cfg.location,
        mime_type=mime_type,
        bytes=len(file_bytes),
    )

    response = await retry_async(
        lambda: client.process_document(request=request),
        attempts=3,
        op="document_ai.process_document",
    )
    rows = _normalize(response.document)
    log.info("ingestion.document_ai.extracted", rows=len(rows))
    return rows


def _normalize(doc: Any) -> list[dict[str, Any]]:
    """Map Document AI entities to ingestion-pipeline row dicts.

    Target row schema (same as the CSV reader):
        subscriber_ref, first_name, last_name, dob, gender,
        plan_code, coverage_level, effective_date, termination_date,
        relationship, maintenance, sponsor_ref, group_ref, trading_partner_id

    This is a pragmatic skeleton: one output row per top-level
    ``enrollment_record`` entity, with child ``properties`` mapped by
    ``type_`` -> ``mention_text``.

    TODO: a real payer-specific schema (trained per trading partner) should
    replace this once we have labeled training data. The shape of the return
    value must not change — consumers only depend on the row dict keys.
    """
    rows: list[dict[str, Any]] = []
    for ent in getattr(doc, "entities", []) or []:
        if getattr(ent, "type_", None) != "enrollment_record":
            continue
        row: dict[str, Any] = {}
        for prop in getattr(ent, "properties", []) or []:
            key = getattr(prop, "type_", None)
            if not key:
                continue
            row[key] = getattr(prop, "mention_text", None)
        if row:
            rows.append(row)
    return rows
