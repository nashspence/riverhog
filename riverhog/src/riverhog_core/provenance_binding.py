"""Exact canonical member binding checks shared by admission and recovery."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from riverhog_protocol.artifact_identity import ArtifactMemberIdentityDocument
from riverhog_protocol.provenance_transport import (
    CollectionArtifactProvenanceBindingDocument,
)
from riverhog_provenance import JournalSummary, ProvenanceValidationError, verify_delivery
from riverhog_provenance.model import BinaryReadable
from riverhog_provenance_contracts import PROFILE, ContractCatalog

_COLLECTION_PROFILE = PROFILE + "/profiles/collection-production"
_MEMBER_ROLE = _COLLECTION_PROFILE + "/member"
_MEMBER_HISTORY_ROLE = _COLLECTION_PROFILE + "/member-history"
_HINT_SCHEMA = PROFILE + "/materialization-hint.schema.json"


@dataclass(frozen=True, slots=True)
class VerifiedMemberBinding:
    artifact_id: str
    observation_id: str
    occurrence_id: str
    occurrence_assertion_id: str
    materialization_hint: tuple[str, ...] | None


def verify_member_binding(
    *,
    member: ArtifactMemberIdentityDocument,
    binding: CollectionArtifactProvenanceBindingDocument,
    summary: JournalSummary,
    delivery_context_id: str,
    reader: BinaryReadable | None = None,
) -> VerifiedMemberBinding:
    """Check exact primary history, selected observation and optional actual bytes.

    Corpus dependency closure is a separate whole-archive check. This function
    deliberately accepts no member pathname or latest-journal lookup.
    """

    def has_member_history(artifact_id: str) -> bool:
        subject = {"scope": "local", "object_id": artifact_id, "object_type": "artifact"}
        return any(
            row["journal_id"] == summary.journal_id
            and row["artifact"] == subject
            and row["role"] == _MEMBER_HISTORY_ROLE
            for row in summary.graph_validation.view.get("journal_subjects", ())
        )

    result = verify_selected_member_binding(
        member=member,
        binding=binding,
        journal_id=summary.journal_id,
        anchor=summary.anchor,
        objects=summary.graph_validation.objects,
        has_member_history=has_member_history,
        delivery_context_id=delivery_context_id,
    )
    if reader is not None:
        verify_delivery(summary, binding.delivery_association_id, reader)
    return result


def verify_selected_member_binding(
    *,
    member: ArtifactMemberIdentityDocument,
    binding: CollectionArtifactProvenanceBindingDocument,
    journal_id: str,
    anchor: Mapping[str, Any],
    objects: Mapping[str, dict[str, Any]],
    has_member_history: Callable[[str], bool],
    delivery_context_id: str,
) -> VerifiedMemberBinding:
    """Apply member policy to exact admitted historical Native selections.

    The caller owns current source admission and lifetime. Indexed existence
    supplies only the same journal/member-history predicate as the full view;
    it grants no independent archive or permission authority.
    """
    if member.artifact_id != binding.artifact_id:
        raise ProvenanceValidationError("member and binding artifact IDs differ")
    if journal_id != binding.journal.journal_id or anchor != binding.journal.model_dump(
        mode="json"
    ):
        raise ProvenanceValidationError("selected primary journal anchor differs")
    association = objects.get(binding.delivery_association_id)
    if association is None or association["type"] != "delivery_association":
        raise ProvenanceValidationError("binding does not select an effective delivery association")
    if (
        association["delivery_context_id"] != delivery_context_id
        or association["slot"] != {"kind": "text", "text": member.artifact_id}
        or association["role"] != _MEMBER_ROLE
    ):
        raise ProvenanceValidationError("delivery context, slot or role differs from the member")
    state_ref = association["state"]
    if state_ref.get("scope") != "local" or state_ref.get("object_type") != "state":
        raise ProvenanceValidationError("member delivery must select a local State")
    state = objects[state_ref["object_id"]]
    observation = objects[association["verification_observation_id"]]
    if observation["type"] != "observation" or observation["state"] != state_ref:
        raise ProvenanceValidationError("selected observation does not verify the delivery State")
    if not any(evidence["basis"] == "direct_measurement" for evidence in observation["evidence"]):
        raise ProvenanceValidationError("selected observation is not a direct measurement")
    content = observation["content"]
    sha256_values = [
        digest["value"] for digest in content["digests"] if digest["algorithm"] == "sha-256"
    ]
    if int(content["size_bytes"]) != member.bytes or sha256_values != [member.sha256]:
        raise ProvenanceValidationError("selected observation differs from member fixity")
    occurrence = objects[state["occurrence_id"]]
    if occurrence["type"] != "occurrence":
        raise ProvenanceValidationError("delivery State has no effective Occurrence")
    if not has_member_history(occurrence["artifact_id"]):
        raise ProvenanceValidationError("primary journal does not declare the member history")
    hint: tuple[str, ...] | None = None
    if "materialization_hint" in occurrence:
        value: Any = occurrence["materialization_hint"]
        ContractCatalog().validate(_HINT_SCHEMA, value)
        hint = tuple(value["components"])
    return VerifiedMemberBinding(
        artifact_id=member.artifact_id,
        observation_id=observation["id"],
        occurrence_id=occurrence["id"],
        occurrence_assertion_id=occurrence["assertion_id"],
        materialization_hint=hint,
    )


__all__ = ["VerifiedMemberBinding", "verify_member_binding", "verify_selected_member_binding"]
