from __future__ import annotations

import pytest
from a_stove0_riverhog_provenance_evidence_contract_lib import (
    CORE_PROVENANCE_SEMANTIC_VALIDATOR,
    validate_core_provenance_facts,
)
from a_stove0_riverhog_provenance_evidence_contract_lib.contracts import (
    CORE_PROVENANCE_CONFORMANCE_VECTORS,
)
from stove0_observer_client import load_semantic_validator_registry


def test_controller_can_load_exact_core_provenance_validator() -> None:
    registry = load_semantic_validator_registry(("riverhog-provenance",))
    binding = CORE_PROVENANCE_SEMANTIC_VALIDATOR
    assert registry.resolve(binding.profile_id, binding.profile_sha256) == binding.validator
    for vector in CORE_PROVENANCE_CONFORMANCE_VECTORS.vectors:
        if vector.accepted:
            validate_core_provenance_facts(vector.facts, vector.subjects, {})
        else:
            with pytest.raises(ValueError):
                validate_core_provenance_facts(vector.facts, vector.subjects, {})
