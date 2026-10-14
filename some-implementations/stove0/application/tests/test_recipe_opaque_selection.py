"""Opaque member and relation witnesses run against the retained compiled state."""

from pathlib import Path

import pytest
from pydantic import TypeAdapter
from stove0_protocol.predicates import RowPredicate, Truth, evaluate_row
from test_compiled_groups import _groups, _observed_state


def test_opaque_member_instances_never_collapse_by_equal_bytes(tmp_path: Path):
    state, url, work, subjects = _observed_state(tmp_path, ("primary", "primary"))
    assert len({member.sha256 for member in subjects}) == 1
    assert len({member.artifact_id for member in subjects}) == 2
    state, authority, _ = _groups(state, url, work)
    assert authority.group_count == 2
    assert {
        row.primary_id for row in state.compiled_groups.page(authority, start_ordinal=0).members
    } == {member.id for member in subjects}
    state.engine.dispose()


def test_complete_negative_relation_allows_next_declared_source(tmp_path: Path):
    state, url, work, subjects = _observed_state(
        tmp_path,
        ("primary", "associated"),
        fallback=((0, 1),),
    )
    state, authority, _ = _groups(state, url, work)
    assert [
        (row.primary_id, row.associated_id)
        for row in state.compiled_groups.page(authority, start_ordinal=0).members
    ] == [
        (subjects[0].id, None),
        (subjects[0].id, subjects[1].id),
    ]
    state.engine.dispose()


@pytest.mark.parametrize("status", ("insufficient", "ambiguous", "unsupported"))
def test_partial_relation_evidence_cannot_be_a_negative(tmp_path: Path, status: str):
    state, url, work, _ = _observed_state(
        tmp_path,
        ("primary", "associated"),
        fallback=((0, 1),),
        statuses={("preferred", 1): status},
    )
    state, authority, _ = _groups(state, url, work)
    assert authority.group_count == authority.association_count == 0
    state.engine.dispose()


def test_stronger_positive_relation_wins_even_when_weaker_tier_has_another_primary(tmp_path: Path):
    state, url, work, subjects = _observed_state(
        tmp_path,
        ("primary", "primary", "associated"),
        preferred=((0, 2),),
        fallback=((1, 2),),
    )
    state, authority, _ = _groups(state, url, work)
    assert [
        (row.primary_id, row.associated_id)
        for row in state.compiled_groups.page(authority, start_ordinal=0).members
    ] == [
        (subjects[0].id, None),
        (subjects[0].id, subjects[2].id),
        (subjects[1].id, None),
    ]
    state.engine.dispose()


def test_nested_metadata_rows_are_tested_without_position_or_filename_rules():
    document = {
        "subject_id": "a-" + "3" * 32,
        "facts": [
            {"name": "creator", "value": "Example"},
            {"name": "container-format", "value": "XMP"},
        ],
    }
    condition = TypeAdapter(RowPredicate).validate_python(
        {
            "items": {
                "path": "/facts",
                "quantifier": "any",
                "where": {
                    "all": [
                        {"test": {"path": "/name", "op": "eq", "value": "container-format"}},
                        {
                            "test": {
                                "path": "/value",
                                "op": "in",
                                "value": ["XMP", "application/rdf+xml"],
                            }
                        },
                    ]
                },
            },
        }
    )
    assert evaluate_row(condition, document) == Truth.TRUE
    assert evaluate_row(condition, {"facts": list(reversed(document["facts"]))}) == Truth.TRUE
    assert (
        evaluate_row(
            condition,
            {
                "facts": [
                    {"name": "container-format", "value": "MOV"},
                    {"name": "creator", "value": "XMP"},
                ]
            },
        )
        == Truth.FALSE
    )
