from __future__ import annotations

import hashlib
from io import BytesIO

import pytest
from riverhog_core.canonical_discovery_relevance import member_relevance, relevance_row_keys
from riverhog_core.provenance_binding import verify_member_binding
from riverhog_protocol.artifact_identity import ArtifactMemberIdentityDocument
from riverhog_protocol.provenance_transport import (
    CollectionArtifactProvenanceBindingDocument,
)
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    ProvenanceValidationError,
    append_assertions,
    assertion,
    create_journal,
    evidence,
    reference,
    validate_journal,
)
from riverhog_provenance_contracts import PROFILE

_COLLECTION_PROFILE = PROFILE + "/profiles/collection-production"
_MEMBER_ID = "ab" * 32


def _bound(
    payload: bytes = b"same bytes can belong to two members",
    *,
    hint: tuple[str, ...] | None = None,
    slot: str = _MEMBER_ID,
):
    observed = BoundedSourceObserver().observe(BytesSource(payload))
    graph = observed.graph_fragment()
    if hint is not None:
        graph["occurrences"][0]["materialization_hint"] = {"components": list(hint)}
    who = observed.observer_agent_id
    raw = create_journal(graph, recorded_by_agent_id=who)
    initial = validate_journal(raw)
    context = assertion("context", who, kind="delivery")
    association = assertion(
        "delivery_association",
        who,
        delivery_context_id=context["id"],
        slot={"kind": "text", "text": slot},
        role=_COLLECTION_PROFILE + "/member",
        state=reference(observed.state_id, "state"),
        verification_observation_id=observed.observation_id,
        evidence_items=[evidence(who, "process_record")],
    )
    subject = assertion(
        "journal_subject",
        who,
        journal_id=initial.journal_id,
        artifact=reference(observed.artifact_id, "artifact"),
        role=_COLLECTION_PROFILE + "/member-history",
    )
    raw = append_assertions(
        raw,
        {
            "contexts": [context],
            "delivery_associations": [association],
            "journal_subjects": [subject],
        },
        recorded_by_agent_id=who,
    )
    summary = validate_journal(raw)
    member = ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": _MEMBER_ID,
            "bytes": str(len(payload)),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    )
    binding = CollectionArtifactProvenanceBindingDocument.model_validate(
        {
            "artifact_id": _MEMBER_ID,
            "journal": summary.anchor,
            "delivery_association_id": association["id"],
        }
    )
    return member, binding, summary, context["id"], payload


def test_exact_member_binding_and_hint() -> None:
    member, binding, summary, context_id, payload = _bound(hint=("Camera", "clip.mkv"))
    verified = verify_member_binding(
        member=member,
        binding=binding,
        summary=summary,
        delivery_context_id=context_id,
        reader=BytesIO(payload),
    )
    assert verified.artifact_id == _MEMBER_ID
    assert verified.materialization_hint == ("Camera", "clip.mkv")
    assert verified.occurrence_assertion_id.startswith("urn:uuid:")


def test_wrong_slot_or_anchor_never_selects_another_member() -> None:
    member, binding, summary, context_id, _ = _bound(slot="cd" * 32)
    with pytest.raises(ProvenanceValidationError, match="slot"):
        verify_member_binding(
            member=member, binding=binding, summary=summary, delivery_context_id=context_id
        )
    member, binding, summary, context_id, _ = _bound()
    wrong = binding.model_copy(
        update={"journal": binding.journal.model_copy(update={"prefix_sha256": "0" * 64})}
    )
    with pytest.raises(ProvenanceValidationError, match="anchor"):
        verify_member_binding(
            member=member, binding=wrong, summary=summary, delivery_context_id=context_id
        )


def test_member_fixity_and_actual_bytes_are_independently_checked() -> None:
    member, binding, summary, context_id, payload = _bound()
    wrong_member = member.model_copy(update={"sha256": "0" * 64})
    with pytest.raises(ProvenanceValidationError, match="fixity"):
        verify_member_binding(
            member=wrong_member,
            binding=binding,
            summary=summary,
            delivery_context_id=context_id,
        )
    with pytest.raises(ProvenanceValidationError):
        verify_member_binding(
            member=member,
            binding=binding,
            summary=summary,
            delivery_context_id=context_id,
            reader=BytesIO(payload[:-1] + b"!"),
        )


def test_unhinted_canonical_binding_is_valid() -> None:
    member, binding, summary, context_id, _ = _bound()
    assert (
        verify_member_binding(
            member=member,
            binding=binding,
            summary=summary,
            delivery_context_id=context_id,
        ).materialization_hint
        is None
    )


def test_member_discovery_relevance_does_not_import_unrelated_co_resident_facts() -> None:
    member, binding, summary, context_id, _ = _bound()
    recorder = summary.graph["agents"][0]["id"]
    unrelated_context = assertion("context", recorder, kind="opaque")
    unrelated = assertion(
        "extension",
        recorder,
        subject=reference(unrelated_context["id"], "context"),
        property="urn:test:unrelated",
        value={"type": "text", "value": "sibling-only"},
    )
    updated = append_assertions(
        b"".join(frame.encoded for frame in summary.frames),
        {"contexts": [unrelated_context], "extensions": [unrelated]},
        recorded_by_agent_id=recorder,
    )
    summary = validate_journal(updated)
    binding = CollectionArtifactProvenanceBindingDocument.model_validate(
        {
            "artifact_id": member.artifact_id,
            "journal": summary.anchor,
            "delivery_association_id": binding.delivery_association_id,
        }
    )
    relevance = member_relevance(
        member=member,
        binding=binding,
        primary=summary,
        corpus={summary.journal_id: summary},
        delivery_context_id=context_id,
    )
    assert (summary.journal_id, summary.journal_sha256, unrelated["assertion_id"]) not in relevance
    assert (
        summary.journal_id,
        summary.journal_sha256,
        unrelated_context["assertion_id"],
    ) not in relevance
    association = summary.graph_validation.objects[binding.delivery_association_id]
    assert (
        "member"
        in relevance[(summary.journal_id, summary.journal_sha256, association["assertion_id"])]
    )
    assert any(scope == "member" for _, scope in relevance_row_keys(relevance))
