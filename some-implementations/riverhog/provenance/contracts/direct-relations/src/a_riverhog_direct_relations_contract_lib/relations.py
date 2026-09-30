"""Exact selected vocabulary and output-key to State-edge correspondence."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from pydantic import TypeAdapter
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance_contracts.references import EntityReference

_BASE = "https://nashspence.github.io/riverhog/v1/provenance/relations/"
DESCRIBES = _BASE + "describes"
RECONSTRUCTION_SUPPORT_FOR = _BASE + "reconstruction-support-for"
VOCABULARY: dict[str, object] = {
    "format": "riverhog-direct-description-relations/v1",
    "predicates": {
        DESCRIBES: {
            "domain": "state",
            "range": "state",
            "meaning": (
                "The asserting agent presents the subject State as descriptive metadata "
                "or documentation about the object State."
            ),
        },
        RECONSTRUCTION_SUPPORT_FOR: {
            "domain": "state",
            "range": "state",
            "meaning": (
                "The asserting agent presents the subject State as reconstruction support "
                "for the object under a separately stated operation or format contract."
            ),
        },
    },
    "direction": "subject-to-object",
    "cardinality": "many-to-many",
    "entailments": [],
    "uri_resolution": "never-network",
}
VOCABULARY_SHA256 = hashlib.sha256(canonical_json_bytes(VOCABULARY)).hexdigest()
_REFERENCE: TypeAdapter[EntityReference] = TypeAdapter(EntityReference)
type DirectEdge = tuple[dict[str, Any], str, dict[str, Any]]


def _state_reference(value: object) -> dict[str, Any]:
    reference = _REFERENCE.validate_python(value)
    if reference.object_type != "state":
        raise ValueError("direct relations require exact State references")
    return reference.model_dump(mode="json")


def validate_selected_claim(row: Mapping[str, Any]) -> DirectEdge:
    """Validate one known relation after generic canonical validation.

    Unknown extension predicates are not classified as absence by this validator.
    """

    predicate = row.get("property")
    if row.get("type") != "extension" or predicate not in {DESCRIBES, RECONSTRUCTION_SUPPORT_FOR}:
        raise ValueError("claim is outside the selected direct relation vocabulary")
    value = row.get("value")
    if not isinstance(value, dict) or value.get("type") != "reference":
        raise ValueError("direct relation value must be an exact State reference")
    return _state_reference(row.get("subject")), predicate, _state_reference(value.get("value"))


def resolve_output_relations(
    product: Mapping[str, Any], resolve_state: Callable[[str], object]
) -> tuple[DirectEdge, ...]:
    """Resolve one bounded accepted product against an exact operation State map."""
    output_id = product.get("output_id")
    if not isinstance(output_id, str) or not output_id:
        raise ValueError("accepted products require output IDs")
    subject = _state_reference(resolve_state(output_id))
    edges = []
    for field, predicate in (
        ("describes_output_id", DESCRIBES),
        ("reconstructs_output_id", RECONSTRUCTION_SUPPORT_FOR),
    ):
        target = product.get(field)
        if target is None:
            continue
        if not isinstance(target, str) or target == output_id:
            raise ValueError("direct relation targets a missing or same output")
        endpoint = _state_reference(resolve_state(target))
        if endpoint == subject:
            raise ValueError("distinct produced outputs need distinct States")
        edges.append((subject, predicate, endpoint))
    return tuple(edges)


def expected_output_edges(
    products: Sequence[Mapping[str, Any]], output_states: Mapping[str, object]
) -> tuple[DirectEdge, ...]:
    """Resolve only accepted operation-local keys to distinct produced State refs."""

    keys = [product.get("output_id") for product in products]
    if any(not isinstance(key, str) or not key for key in keys):
        raise ValueError("accepted products require output IDs")
    if len(keys) != len(set(keys)) or set(keys) != set(output_states):
        raise ValueError("output State map must cover accepted products exactly")
    states = {key: _state_reference(value) for key, value in output_states.items()}
    if len({canonical_json_bytes(value) for value in states.values()}) != len(states):
        raise ValueError("distinct produced outputs need distinct States")
    edges: list[DirectEdge] = []
    for product in products:
        output_id = product["output_id"]
        for field, predicate in (
            ("describes_output_id", DESCRIBES),
            ("reconstructs_output_id", RECONSTRUCTION_SUPPORT_FOR),
        ):
            if field not in product:
                continue
            target = product[field]
            if not isinstance(target, str) or target not in states or target == output_id:
                raise ValueError("direct relation targets a missing or same output")
            edges.append((states[output_id], predicate, states[target]))
    if len({canonical_json_bytes(list(edge)) for edge in edges}) != len(edges):
        raise ValueError("accepted operation repeats a direct relation")
    return tuple(edges)


def verify_output_edges(
    products: Sequence[Mapping[str, Any]],
    output_states: Mapping[str, object],
    claims: Sequence[Mapping[str, Any]],
) -> None:
    """Reject any missing, extra, substituted or duplicate published edge."""

    expected = sorted(
        canonical_json_bytes(list(edge)) for edge in expected_output_edges(products, output_states)
    )
    actual = sorted(canonical_json_bytes(list(validate_selected_claim(row))) for row in claims)
    if actual != expected:
        raise ValueError("recorded direct relations differ from accepted output relations")
