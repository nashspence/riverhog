"""Verification of an explicitly selected delivery association, without paths."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .common import fingerprint
from .errors import ProvenanceValidationError
from .journal import JournalSummary
from .model import BinaryReadable, ObservationPolicy
from .observer import measure


def selected_delivery_occurrence(
    summary: JournalSummary,
    *,
    binding: Mapping[str, Any],
    artifact_id: str,
    byte_count: int,
    sha256: str,
    member_role: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Resolve the Occurrence asserted by one exact root-selected delivery.

    A valid missing hint remains a valid Occurrence with no advice. An invalid
    anchor, association, State, or fixity observation fails instead of falling
    back to an ancestor, sibling, or source locator.
    """

    if binding.get("artifact_id") != artifact_id or binding.get("journal") != summary.anchor:
        raise ProvenanceValidationError("primary delivery binding differs from selected member")
    objects = summary.graph_validation.objects
    association_id = binding.get("delivery_association_id")
    if not isinstance(association_id, str):
        raise ProvenanceValidationError("primary delivery association identity is invalid")
    association = objects.get(association_id)
    if association is None or association.get("type") != "delivery_association":
        raise ProvenanceValidationError("primary delivery association is absent")
    if association.get("role") != member_role or association.get("slot") != {
        "kind": "text",
        "text": artifact_id,
    }:
        raise ProvenanceValidationError("primary delivery association names another member")
    state_reference = association.get("state")
    if not isinstance(state_reference, dict) or state_reference.get("scope") != "local":
        raise ProvenanceValidationError("primary delivery State is not local")
    state_id = state_reference.get("object_id")
    if not isinstance(state_id, str):
        raise ProvenanceValidationError("primary delivery State identity is invalid")
    state = objects.get(state_id)
    if (
        state is None
        or state.get("type") != "state"
        or state_reference.get("object_type") != "state"
    ):
        raise ProvenanceValidationError("primary delivery State is absent")
    occurrence_id = state.get("occurrence_id")
    if not isinstance(occurrence_id, str):
        raise ProvenanceValidationError("primary delivery Occurrence identity is invalid")
    occurrence = objects.get(occurrence_id)
    if occurrence is None or occurrence.get("type") != "occurrence":
        raise ProvenanceValidationError("primary delivery Occurrence is absent")
    observation_id = association.get("verification_observation_id")
    if not isinstance(observation_id, str):
        raise ProvenanceValidationError("primary delivery observation identity is invalid")
    observation = objects.get(observation_id)
    if (
        observation is None
        or observation.get("type") != "observation"
        or observation.get("state") != state_reference
        or int(observation["content"]["size_bytes"]) != byte_count
        or ("sha-256", sha256)
        not in {
            (digest["algorithm"], digest["value"]) for digest in observation["content"]["digests"]
        }
    ):
        raise ProvenanceValidationError("primary delivery observation differs from member fixity")
    return state, occurrence


def verify_delivery(
    summary: JournalSummary, association_id: str, reader: BinaryReadable
) -> dict[str, Any]:
    objects = summary.graph_validation.objects
    association = objects.get(association_id)
    if association is None or association["type"] != "delivery_association":
        raise ProvenanceValidationError("select an exact delivery association")
    observation = objects[association["verification_observation_id"]]
    expected = observation["content"]
    measured = measure(reader, ObservationPolicy(), expected_length=int(expected["size_bytes"]))
    if fingerprint(measured) != fingerprint(expected):
        raise ProvenanceValidationError(
            "supplied primary bytes do not match the selected observation"
        )
    return {
        "association_id": association_id,
        "observation_id": observation["id"],
        "result": "matching_fixity",
        "scope": "primary_bytes_only",
        "content": measured,
    }
