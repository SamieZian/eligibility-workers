"""Unit tests for the streaming 834 parser."""

from __future__ import annotations

from datetime import date
from io import BytesIO, StringIO

import pytest

from x12_834 import (
    EnrollmentInstruction,
    MaintenanceType,
    Parse834Error,
    parse_834,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _isa(ctrl: str = "000000001") -> str:
    # 16-char sender / receiver IDs, padded to spec.
    return (
        "ISA*00*          *00*          *ZZ*SENDER         "
        "*ZZ*RECEIVER       *260414*0930*U*00501*"
        f"{ctrl}*0*P*:~"
    )


def _gs(ctrl: str = "1") -> str:
    return f"GS*BE*SENDER*RECEIVER*20260414*0930*{ctrl}*X*005010X220A1~"


def _ge(ctrl: str = "1", count: str = "1") -> str:
    return f"GE*{count}*{ctrl}~"


def _iea(ctrl: str = "000000001", count: str = "1") -> str:
    return f"IEA*{count}*{ctrl}~"


def _st(ctrl: str = "0001") -> str:
    return f"ST*834*{ctrl}*005010X220A1~"


def _se(count: str, ctrl: str = "0001") -> str:
    return f"SE*{count}*{ctrl}~"


def _minimal_body(
    *,
    maint: str = "021",
    last: str = "SHARMA",
    first: str = "PRIYA",
    ssn: str = "123456789",
    dob: str = "19900215",
    gender: str = "F",
    plan: str = "PLAN-GOLD",
    level: str = "FAM",
    effective: str = "20260101",
    termination: str | None = None,
    relationship: str = "18",
    subscriber_ind: str = "Y",
) -> list[str]:
    segs = [
        f"INS*{subscriber_ind}*{relationship}*{maint}*20*A***FT~",
        "REF*38*SPONSOR-POLICY~",
        f"REF*0F*{ssn}~",
        "REF*1L*GRP-001~",
        f"NM1*IL*1*{last}*{first}****34*{ssn}~",
        f"DMG*D8*{dob}*{gender}~",
        f"HD*{maint}**HLT*{plan}*{level}~",
        f"DTP*348*D8*{effective}~",
    ]
    if termination:
        segs.append(f"DTP*349*D8*{termination}~")
    return segs


def _build_file(bodies: list[list[str]], isa_ctrl: str = "000000001") -> str:
    segs: list[str] = []
    segs.append(_isa(isa_ctrl))
    segs.append(_gs("1"))
    segs.append(_st("0001"))
    segs.append("BGN*00*TXN0001*20260414*0930****2~")
    segs.append("REF*38*SPONSOR-POLICY~")
    segs.append("DTP*303*D8*20260101~")
    inner: list[str] = []
    for b in bodies:
        inner.extend(b)
    segs.extend(inner)
    # SE count: ST + BGN + REF + DTP + inner + SE itself
    count = 2 + 1 + 1 + len(inner) + 1
    segs.append(_se(str(count)))
    segs.append(_ge("1", "1"))
    segs.append(_iea(isa_ctrl, "1"))
    return "".join(segs)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_parses_minimal_single_ins() -> None:
    doc = _build_file([_minimal_body()])
    results = list(parse_834(doc))
    assert len(results) == 1
    r = results[0]
    assert isinstance(r, EnrollmentInstruction)
    assert r.maintenance_type == MaintenanceType.ADD
    assert r.first_name == "PRIYA"
    assert r.last_name == "SHARMA"
    assert r.ssn == "123456789"
    assert r.dob == date(1990, 2, 15)
    assert r.gender == "F"
    assert r.plan_code == "PLAN-GOLD"
    assert r.coverage_level == "FAM"
    assert r.effective_date == date(2026, 1, 1)
    assert r.ins_position == 1
    assert r.isa_control == "000000001"
    assert r.gs_control == "1"
    assert r.st_control == "0001"


def test_detects_delimiters_from_isa() -> None:
    # Use alternate delimiters: element='|', component='^', segment='\n'.
    isa = (
        "ISA|00|          |00|          |ZZ|SENDER         "
        "|ZZ|RECEIVER       |260414|0930|U|00501|000000002|0|P|^\n"
    )
    body = (
        "GS|BE|SENDER|RECEIVER|20260414|0930|1|X|005010X220A1\n"
        "ST|834|0001|005010X220A1\n"
        "BGN|00|TXN0001|20260414|0930||||2\n"
        "INS|Y|18|021|20|A|||FT\n"
        "REF|0F|999000111\n"
        "NM1|IL|1|DOE|JANE||||34|999000111\n"
        "DMG|D8|19800101|F\n"
        "HD|021||HLT|PLAN-X|IND\n"
        "DTP|348|D8|20260201\n"
        "SE|8|0001\n"
        "GE|1|1\n"
        "IEA|1|000000002\n"
    )
    results = list(parse_834(isa + body))
    assert len(results) == 1
    assert results[0].last_name == "DOE"
    assert results[0].effective_date == date(2026, 2, 1)


def test_add_cancel_and_correction_in_same_file() -> None:
    bodies = [
        _minimal_body(maint="021", last="A", first="ADD", ssn="111111111"),
        _minimal_body(maint="024", last="B", first="CANCEL", ssn="222222222",
                      termination="20260331"),
        _minimal_body(maint="030", last="C", first="CORRECT", ssn="333333333",
                      effective="20260115"),
    ]
    results = list(parse_834(_build_file(bodies)))
    assert [r.maintenance_type for r in results] == [
        MaintenanceType.ADD,
        MaintenanceType.CANCEL,
        MaintenanceType.CORRECTION,
    ]
    assert results[1].termination_date == date(2026, 3, 31)
    assert results[2].effective_date == date(2026, 1, 15)


def test_incrementing_ins_position_and_stable_segment_key() -> None:
    bodies = [
        _minimal_body(ssn=f"{i:09d}") for i in range(1, 4)
    ]
    doc = _build_file(bodies)
    results = list(parse_834(doc, trading_partner_id="TP-42"))
    assert [r.ins_position for r in results] == [1, 2, 3]
    expected_keys = [
        f"TP-42:000000001:1:0001:{i}" for i in (1, 2, 3)
    ]
    assert [r.segment_key for r in results] == expected_keys

    # Re-parsing yields identical keys (stability).
    results2 = list(parse_834(doc, trading_partner_id="TP-42"))
    assert [r.segment_key for r in results2] == expected_keys


def test_invalid_isa_raises() -> None:
    with pytest.raises(Parse834Error):
        list(parse_834("NOT AN ISA HEADER AT ALL"))
    with pytest.raises(Parse834Error):
        # Starts with ISA but is too short.
        list(parse_834("ISA*short"))


def test_string_input() -> None:
    doc = _build_file([_minimal_body()])
    assert len(list(parse_834(doc))) == 1
    assert len(list(parse_834(StringIO(doc)))) == 1


def test_bytes_input() -> None:
    doc = _build_file([_minimal_body()]).encode("latin-1")

    def _chunks() -> object:
        # Chunked bytes iterable — exercises streaming seam.
        step = 37
        for i in range(0, len(doc), step):
            yield doc[i : i + step]

    results_iter = list(parse_834(_chunks()))
    assert len(results_iter) == 1

    results_file = list(parse_834(BytesIO(doc)))
    assert len(results_file) == 1
    assert results_file[0].last_name == "SHARMA"


def test_crlf_line_terminators_are_tolerated() -> None:
    # Insert a CRLF after every segment terminator.
    doc = _build_file([_minimal_body()])
    crlf_doc = doc.replace("~", "~\r\n")
    results = list(parse_834(crlf_doc))
    assert len(results) == 1
    assert results[0].first_name == "PRIYA"
