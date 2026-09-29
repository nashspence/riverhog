from __future__ import annotations

import base64

from riverhog_core.canonical_discovery_extraction import (
    assertion_clause_matches,
    assertion_postings,
    declared_profile_sections,
    literal_chunks,
)
from riverhog_protocol import AssertionClause, ProfilePin

PIN = ProfilePin(
    contract_id="urn:example:opaque",
    contract_sha256="a" * 64,
    schema_id="urn:example:opaque-schema",
)


def _profile(data: dict[str, object]) -> dict[str, object]:
    return {"profile": PIN.model_dump(mode="json"), "data": data}


def test_profile_predicates_correlate_within_one_schema_defined_section() -> None:
    row = {
        "type": "observation",
        "profiles": [
            _profile({"camera": "A", "serial": "x"}),
            _profile({"camera": "B", "serial": "y"}),
        ],
        "note": "unrelated",
    }
    clause = AssertionClause(
        profile=PIN,
        values=(
            {"pointer": "/profiles/0/data/camera", "operator": "equals", "value": "A"},
            {"operator": "equals", "value": "y"},
        ),
    )
    assert assertion_clause_matches(row, clause, assertion_state="effective") is None
    same_section = AssertionClause(
        profile=PIN,
        values=(
            {"operator": "equals", "value": "A"},
            {"operator": "equals", "value": "x"},
        ),
    )
    support = assertion_clause_matches(row, same_section, assertion_state="effective")
    assert support is not None
    assert {posting.pointer for posting in support} == {
        "/profiles/0/data/camera",
        "/profiles/0/data/serial",
    }


def test_nested_profile_lookalike_is_only_plain_json_and_scalar_types_stay_distinct() -> None:
    row = {
        "type": "extension",
        "value": {
            "type": "json",
            "value": _profile(
                {
                    "nested": _profile({"number": 23}),
                    "number": "23",
                }
            ),
        },
    }
    assert [section.pointer for section in declared_profile_sections(row)] == ["/value/value"]
    text_number = AssertionClause(
        profile=PIN,
        values=({"pointer": "/value/value/data/number", "operator": "equals", "value": "23"},),
    )
    int_number = AssertionClause(
        profile=PIN,
        values=({"pointer": "/value/value/data/number", "operator": "equals", "value": 23},),
    )
    assert assertion_clause_matches(row, text_number, assertion_state="effective")
    assert assertion_clause_matches(row, int_number, assertion_state="effective") is None
    assert assertion_clause_matches(row, text_number, assertion_state="retracted") is None


def test_exact_name_bytes_are_reversible_and_not_inferred_from_profile_data() -> None:
    raw = b"A\\\x00\xff"
    name = {
        "kind": "bytes",
        "bytes": {
            "encoding": "base64",
            "data": base64.b64encode(raw).decode("ascii"),
            "byte_length": str(len(raw)),
        },
        "encoding": "utf-8",
    }
    locator = {"type": "locator_binding", "locator": {"kind": "filesystem_path", "name": name}}
    postings = list(assertion_postings(locator))
    special = {item.representation: item for item in postings if item.pointer == "/locator/name"}
    assert special["exact-bytes"].value == "QVwA/w=="
    assert special["byte-escape"].value == "A\\\\\\x00\\xff"
    assert "declared-text" not in special

    lookalike = {"type": "extension", "value": {"type": "json", "value": _profile({"name": name})}}
    assert not any(item.representation == "exact-bytes" for item in assertion_postings(lookalike))


def test_chunk_windows_find_a_literal_crossing_the_4096_boundary() -> None:
    haystack = "a" * 4090 + "Z" * 250 + "b" * 4000
    needle = "a" * 6 + "Z" * 250
    chunks = list(literal_chunks(haystack))
    assert all(len(chunk) <= 4096 for _, chunk in chunks)
    assert any(needle in chunk for _, chunk in chunks)
    assert all(offset >= 0 for offset, _ in chunks)
