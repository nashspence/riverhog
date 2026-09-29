from __future__ import annotations

import pytest
from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_OBSERVER_CONTRACT,
    MATERIALIZATION_HINT_SEMANTIC_VALIDATOR,
    validate_materialization_hint_facts,
)
from a_stove0_materialization_hint_evidence_contract_lib.contracts import (
    MATERIALIZATION_HINT_CONFORMANCE_VECTORS,
    MATERIALIZATION_HINT_FACTS_SEMANTICS,
)
from stove0_observer_client import load_semantic_validator_registry


def test_exact_hint_evidence_vectors_are_controller_accepted() -> None:
    assert MATERIALIZATION_HINT_FACTS_SEMANTICS.conformance_vectors_sha256 == (
        MATERIALIZATION_HINT_CONFORMANCE_VECTORS.sha256
    )
    registry = load_semantic_validator_registry(("materialization-hint",))
    assert (
        registry.resolve(
            MATERIALIZATION_HINT_FACTS_SEMANTICS.id,
            MATERIALIZATION_HINT_FACTS_SEMANTICS.profile_sha256,
        )
        == MATERIALIZATION_HINT_SEMANTIC_VALIDATOR.validator
    )
    for vector in MATERIALIZATION_HINT_CONFORMANCE_VECTORS.vectors:
        if vector.accepted:
            validate_materialization_hint_facts(vector.facts, vector.subjects)
        else:
            with pytest.raises(ValueError):
                validate_materialization_hint_facts(vector.facts, vector.subjects)


def test_valid_missing_hint_is_distinct_from_missing_occurrence_support() -> None:
    vector = MATERIALIZATION_HINT_CONFORMANCE_VECTORS.vectors[0]
    row = dict(vector.facts["artifacts"][0])
    row["materialization_hint"] = None
    fact = validate_materialization_hint_facts({"artifacts": [row]}, vector.subjects)
    assert fact.artifacts[0].materialization_hint is None
    assert MATERIALIZATION_HINT_OBSERVER_CONTRACT.read_actions == ("read-provenance",)
    row.pop("occurrence")
    with pytest.raises(ValueError):
        validate_materialization_hint_facts({"artifacts": [row]}, vector.subjects)
