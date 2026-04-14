"""Streaming ANSI X12 834 (5010X220A1) parser.

Design notes
------------
* We never read the entire file — callers pass any iterable of bytes/str (or a
  text file object) and we pull chunks on demand through an internal character
  stream, then a segment stream, then a loop stream. Each layer is a generator.
* Delimiters are detected from the 106-byte ISA header as mandated by the X12
  standard: element separator is byte 3, component (composite) separator is
  byte 104, and segment terminator is byte 105. A line terminator may or may
  not follow the segment terminator; we tolerate CRLF, LF, or nothing.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import date, datetime
from typing import IO, Any, Union

from .instruction import EnrollmentInstruction, MaintenanceType


class Parse834Error(ValueError):
    """Raised for malformed 834 documents."""


StreamInput = Union[Iterable[bytes], Iterable[str], IO[str], IO[bytes]]


# ---------------------------------------------------------------------------
# Low-level character streaming
# ---------------------------------------------------------------------------

def _iter_chunks(src: StreamInput) -> Iterator[str]:
    """Yield ``str`` chunks from any supported source.

    Accepts raw iterables of bytes / str, or a file-like object with ``read``.
    Bytes are decoded as latin-1 (X12 is ASCII-safe; latin-1 guarantees
    byte-for-byte round-tripping for delimiter detection).
    """
    # File-like with .read() — prefer small reads so we stay streaming.
    if hasattr(src, "read") and callable(src.read):
        while True:
            chunk = src.read(4096)
            if not chunk:
                return
            if isinstance(chunk, bytes):
                yield chunk.decode("latin-1")
            else:
                yield chunk
        return

    # Iterable case
    for chunk in src:  # type: ignore[assignment]
        if isinstance(chunk, bytes):
            yield chunk.decode("latin-1")
        elif isinstance(chunk, str):
            yield chunk
        else:  # pragma: no cover — guarded by types
            raise Parse834Error(f"Unsupported stream element type: {type(chunk)!r}")


def _iter_chars(src: StreamInput) -> Iterator[str]:
    for chunk in _iter_chunks(src):
        for ch in chunk:
            yield ch


# ---------------------------------------------------------------------------
# Segment streaming
# ---------------------------------------------------------------------------

def _read_isa_header(chars: Iterator[str]) -> tuple[list[str], str, str, str]:
    """Read the 106-byte ISA header. Return (elements, element_sep, component_sep, segment_term)."""
    header_chars: list[str] = []
    for ch in chars:
        # Skip pure whitespace/newline padding that may precede the ISA.
        if not header_chars and ch in ("\r", "\n", " ", "\t"):
            continue
        header_chars.append(ch)
        if len(header_chars) == 106:
            break

    if len(header_chars) < 106:
        raise Parse834Error("Stream too short to contain an ISA header (need 106 chars)")

    if "".join(header_chars[:3]) != "ISA":
        raise Parse834Error("Stream does not begin with an ISA segment")

    element_sep = header_chars[3]
    component_sep = header_chars[104]
    segment_term = header_chars[105]

    if element_sep == segment_term or element_sep == component_sep:
        raise Parse834Error("ISA delimiters are not distinct")

    isa_body = "".join(header_chars[:105])  # exclude segment terminator
    elements = isa_body.split(element_sep)
    if len(elements) < 17 or elements[0] != "ISA":
        raise Parse834Error("Malformed ISA segment")

    return elements, element_sep, component_sep, segment_term


def _iter_segments(
    chars: Iterator[str], element_sep: str, segment_term: str
) -> Iterator[list[str]]:
    """Stream segments from the character iterator.

    Any CR/LF that appears around the segment terminator is stripped — those
    are cosmetic line breaks that some senders add after each segment.
    """
    buf: list[str] = []
    for ch in chars:
        if ch == segment_term:
            seg = "".join(buf).strip("\r\n")
            if seg:
                yield seg.split(element_sep)
            buf.clear()
        elif ch in ("\r", "\n") and not buf:
            # Skip line breaks between segments.
            continue
        else:
            buf.append(ch)
    # Trailing content without a terminator is ignored — real files always
    # terminate the last segment (IEA).


# ---------------------------------------------------------------------------
# Date / helpers
# ---------------------------------------------------------------------------

def _parse_date(fmt: str, value: str) -> date | None:
    if not value:
        return None
    if fmt == "D8" and len(value) == 8:
        try:
            return datetime.strptime(value, "%Y%m%d").date()
        except ValueError:
            return None
    if fmt == "RD8" and len(value) == 17 and "-" in value:
        # Range — take the start date.
        try:
            return datetime.strptime(value.split("-", 1)[0], "%Y%m%d").date()
        except ValueError:
            return None
    return None


def _get(seg: list[str], idx: int) -> str:
    """Safely fetch an element (1-indexed in X12 parlance)."""
    return seg[idx] if idx < len(seg) else ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_834(
    stream: StreamInput,
    *,
    trading_partner_id: str | None = None,
) -> Iterator[EnrollmentInstruction]:
    """Parse an ANSI X12 834 (5010X220A1) document as a stream.

    Yields one :class:`EnrollmentInstruction` per 2000 (INS) loop. The parser
    never buffers more than the current INS loop, so files with millions of
    members run in constant memory.
    """
    chars = _iter_chars(stream)
    isa_elements, element_sep, _component_sep, segment_term = _read_isa_header(chars)

    # Drain the segment terminator that followed the ISA header (if the
    # terminator is a newline, _read_isa_header already consumed it; otherwise
    # the 106th byte was the terminator itself, which we already captured).
    isa_control = isa_elements[13].strip() if len(isa_elements) > 13 else ""

    segments = _iter_segments(chars, element_sep, segment_term)

    gs_control = ""
    st_control = ""
    sequence = 0
    ins_position = 0

    # Per-instruction working state
    pending: dict[str, Any] | None = None

    def _finalize(state: dict[str, Any]) -> EnrollmentInstruction:
        seg_key = f"{trading_partner_id or ''}:{state['isa_control']}:{state['gs_control']}:{state['st_control']}:{state['ins_position']}"
        return EnrollmentInstruction(
            sequence=state["sequence"],
            isa_control=state["isa_control"],
            gs_control=state["gs_control"],
            st_control=state["st_control"],
            ins_position=state["ins_position"],
            segment_key=seg_key,
            subscriber_indicator=state["subscriber_indicator"],
            relationship_code=state["relationship_code"],
            maintenance_type=state["maintenance_type"],
            benefit_status_code=state["benefit_status_code"],
            sponsor_ref=state.get("sponsor_ref"),
            subscriber_ref=state.get("subscriber_ref"),
            group_ref=state.get("group_ref"),
            first_name=state.get("first_name"),
            last_name=state.get("last_name"),
            middle_name=state.get("middle_name"),
            ssn=state.get("ssn"),
            dob=state.get("dob"),
            gender=state.get("gender"),
            plan_code=state.get("plan_code"),
            coverage_level=state.get("coverage_level"),
            effective_date=state.get("effective_date"),
            termination_date=state.get("termination_date"),
            trading_partner_id=trading_partner_id,
        )

    in_hd_loop = False

    for seg in segments:
        tag = seg[0] if seg else ""
        if not tag:
            continue

        # Envelope tags that reset/track control numbers.
        if tag == "GS":
            # Flush any pending instruction before changing envelope scope.
            if pending is not None:
                yield _finalize(pending)
                pending = None
            gs_control = _get(seg, 6)
            ins_position = 0
            continue

        if tag == "GE":
            if pending is not None:
                yield _finalize(pending)
                pending = None
            gs_control = ""
            continue

        if tag == "ST":
            if pending is not None:
                yield _finalize(pending)
                pending = None
            st_control = _get(seg, 2)
            ins_position = 0
            continue

        if tag == "SE":
            if pending is not None:
                yield _finalize(pending)
                pending = None
            st_control = ""
            continue

        if tag == "IEA":
            if pending is not None:
                yield _finalize(pending)
                pending = None
            continue

        if tag == "INS":
            # New instruction — flush the previous one.
            if pending is not None:
                yield _finalize(pending)
            ins_position += 1
            sequence += 1
            maint_raw = _get(seg, 3)
            try:
                maint = MaintenanceType(maint_raw)
            except ValueError as exc:
                raise Parse834Error(
                    f"Unknown INS03 maintenance type {maint_raw!r} at ins_position={ins_position}"
                ) from exc
            pending = {
                "sequence": sequence,
                "isa_control": isa_control,
                "gs_control": gs_control,
                "st_control": st_control,
                "ins_position": ins_position,
                "subscriber_indicator": _get(seg, 1),
                "relationship_code": _get(seg, 2),
                "maintenance_type": maint,
                "benefit_status_code": _get(seg, 5),
            }
            in_hd_loop = False
            continue

        # Everything below is only meaningful inside an INS loop.
        if pending is None:
            continue

        if tag == "REF":
            qual = _get(seg, 1)
            val = _get(seg, 2)
            if qual == "38":
                pending["sponsor_ref"] = val
            elif qual == "0F":
                pending["subscriber_ref"] = val
            elif qual == "1L":
                pending["group_ref"] = val
            continue

        if tag == "NM1":
            entity = _get(seg, 1)
            if entity == "IL":  # Insured/Subscriber
                pending["last_name"] = _get(seg, 3) or None
                pending["first_name"] = _get(seg, 4) or None
                pending["middle_name"] = _get(seg, 5) or None
                id_qual = _get(seg, 8)
                id_val = _get(seg, 9)
                if id_qual == "34" and id_val:
                    pending["ssn"] = id_val
            continue

        if tag == "DMG":
            fmt = _get(seg, 1)
            pending["dob"] = _parse_date(fmt, _get(seg, 2))
            g = _get(seg, 3)
            pending["gender"] = g or None
            continue

        if tag == "HD":
            in_hd_loop = True
            # HD*maint_type_code*ref_id*ins_line_code*plan_coverage_desc*coverage_level
            # HD03 is the Insurance Line Code (HLT/DEN/VIS/...); HD04 is the
            # actual plan / coverage description. Senders differ in which
            # field they populate for the plan identifier, so prefer HD04
            # when it is present and fall back to HD03 otherwise.
            hd03 = _get(seg, 3)
            hd04 = _get(seg, 4)
            pending["plan_code"] = hd04 or hd03 or None
            pending["coverage_level"] = _get(seg, 5) or None
            continue

        if tag == "DTP":
            qual = _get(seg, 1)
            fmt = _get(seg, 2)
            val = _get(seg, 3)
            parsed = _parse_date(fmt, val)
            if qual == "348":
                pending["effective_date"] = parsed
            elif qual in ("349", "357"):
                pending["termination_date"] = parsed
            continue

        # Unhandled segment types are ignored but don't interrupt streaming.
        _ = in_hd_loop  # retained for future extensions

    if pending is not None:
        yield _finalize(pending)
