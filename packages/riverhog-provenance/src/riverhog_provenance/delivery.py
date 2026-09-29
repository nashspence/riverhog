"""Verification of an explicitly selected delivery association, without paths."""

from __future__ import annotations

from typing import Any

from .common import fingerprint
from .errors import ProvenanceValidationError
from .journal import JournalSummary
from .model import BinaryReadable, ObservationPolicy
from .observer import measure


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
