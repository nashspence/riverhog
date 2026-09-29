from __future__ import annotations

import pytest
from a_riverhog_direct_relations_contract_lib import (
    DESCRIBES,
    RECONSTRUCTION_SUPPORT_FOR,
    VOCABULARY_SHA256,
    expected_output_edges,
    validate_selected_claim,
    verify_output_edges,
)


def _state(number: int) -> dict[str, str]:
    return {
        "scope": "local",
        "object_type": "state",
        "object_id": f"urn:uuid:00000000-0000-4000-8000-{number:012x}",
    }


def _claim(subject: dict[str, str], predicate: str, target: dict[str, str]) -> dict:
    return {
        "type": "extension",
        "subject": subject,
        "property": predicate,
        "value": {"type": "reference", "value": target},
    }


def test_selected_vocabulary_resolves_exact_operation_output_keys() -> None:
    products = (
        {"output_id": "media"},
        {"output_id": "xmp", "describes_output_id": "media"},
        {"output_id": "bundle", "reconstructs_output_id": "media"},
    )
    states = {"media": _state(1), "xmp": _state(2), "bundle": _state(3)}
    edges = expected_output_edges(products, states)
    assert edges == (
        (_state(2), DESCRIBES, _state(1)),
        (_state(3), RECONSTRUCTION_SUPPORT_FOR, _state(1)),
    )
    claims = tuple(_claim(*edge) for edge in edges)
    verify_output_edges(products, states, claims)
    assert len(VOCABULARY_SHA256) == 64
    with pytest.raises(ValueError, match="recorded direct relations differ"):
        verify_output_edges(products, states, claims[:1])
    with pytest.raises(ValueError, match="recorded direct relations differ"):
        verify_output_edges(products, states, claims + claims[:1])
    with pytest.raises(ValueError, match="recorded direct relations differ"):
        verify_output_edges(products, states, (_claim(_state(1), DESCRIBES, _state(2)), claims[1]))


def test_selected_shape_rejects_malformed_or_uninterpreted_claims() -> None:
    valid = _claim(_state(2), DESCRIBES, _state(1))
    assert validate_selected_claim(valid) == (_state(2), DESCRIBES, _state(1))
    with pytest.raises(ValueError, match="outside the selected"):
        validate_selected_claim({**valid, "property": "https://example.com/unknown"})
    with pytest.raises(ValueError, match="value must be"):
        validate_selected_claim({**valid, "value": {"type": "string", "value": "hello"}})
    with pytest.raises(ValueError, match="exact State"):
        validate_selected_claim({**valid, "subject": {**_state(2), "object_type": "artifact"}})


def test_output_correspondence_rejects_missing_duplicate_and_self_edges() -> None:
    with pytest.raises(ValueError, match="exactly"):
        expected_output_edges(({"output_id": "media"},), {"xmp": _state(1)})
    with pytest.raises(ValueError, match="distinct States"):
        expected_output_edges(
            ({"output_id": "media"}, {"output_id": "xmp"}),
            {"media": _state(1), "xmp": _state(1)},
        )
    with pytest.raises(ValueError, match="missing or same"):
        expected_output_edges(
            ({"output_id": "media", "describes_output_id": "media"},),
            {"media": _state(1)},
        )
