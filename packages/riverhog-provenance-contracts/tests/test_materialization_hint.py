"""Core optional field tests; publication policy is outside this schema."""

import pytest
from riverhog_provenance_contracts import PROFILE, ContractCatalog, schema_document

SCHEMA = PROFILE + "/materialization-hint.schema.json"


@pytest.mark.parametrize(
    "components",
    [
        ["clip.mkv"],
        ["Camera", "DCIM", "clip.mkv"],
        ["a", "a", "x"],
        ["CON"],
        ["C:"],
        ["trailing."],
        ["e\u0301"],
        ["é"],
        ["x" * 128] * 32,
    ],
)
def test_valid_hint(components):
    ContractCatalog().validate(SCHEMA, {"components": components})


@pytest.mark.parametrize(
    "components",
    [
        [],
        [""],
        ["."],
        [".."],
        ["/x"],
        ["a/b"],
        ["a\\b"],
        ["x\n"],
        ["\x7f"],
        ["\x85"],
        ["x" * 129],
        ["x"] * 33,
        [1],
        ["\ud800"],
    ],
)
def test_invalid_hint(components):
    with pytest.raises((ValueError, TypeError)):
        ContractCatalog().validate(SCHEMA, {"components": components})


@pytest.mark.parametrize(
    "value",
    [
        None,
        "x",
        {"components": ["x"], "path": "x"},
        {"components": ["x"], "allow_missing_materialization_hint": True},
        {},
    ],
)
def test_closed_hint(value):
    with pytest.raises((ValueError, TypeError)):
        ContractCatalog().validate(SCHEMA, value)


def test_occurrence_field_is_not_required_and_is_not_state_identity():
    defs = schema_document()["$defs"]
    assert "materialization_hint" in defs["occurrence"]["properties"]
    assert "materialization_hint" not in defs["occurrence"]["required"]
    assert "materialization_hint" not in defs["state"]["properties"]
    assert "materialization_hint" not in defs["deliveryAssociation"]["properties"]
