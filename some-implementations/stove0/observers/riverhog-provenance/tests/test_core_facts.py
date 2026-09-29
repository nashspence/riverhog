from __future__ import annotations

import hashlib
from types import SimpleNamespace
from typing import Any, cast

import pytest
from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_OBSERVER_CONTRACT,
    validate_materialization_hint_facts,
)
from a_stove0_riverhog_provenance_observer import (
    RiverhogProvenanceObserver,
    extract_core_facts,
    extract_materialization_hint_fact,
)
from riverhog_protocol import CollectionArtifactProvenanceBindingDocument
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    assertion,
    create_journal,
    external_reference,
    reference,
    validate_journal,
)
from riverhog_provenance_contracts import SOURCE_NAMING_VIEW_SCHEME
from stove0_observer_protocol import (
    CollectionRootIdentityRef,
    ContentObservationRequest,
    ContentObservationRequestPayload,
    WorkArtifactSubject,
)
from stove0_observer_support import ContentObservationRuntime


def _fixture(
    *,
    name: str,
    view_id: str,
    describes: dict[str, object] | None = None,
    hint: dict[str, object] | None = None,
) -> tuple[WorkArtifactSubject, CollectionArtifactProvenanceBindingDocument, object]:
    observed = BoundedSourceObserver().observe(BytesSource(b"abc"))
    graph = observed.graph_fragment()
    if hint is not None:
        graph["occurrences"][0]["materialization_hint"] = hint
    graph["descriptions"][0]["address_status"] = "known"
    context = assertion(
        "context",
        observed.observer_agent_id,
        kind="filesystem_namespace",
        identifiers=[
            {
                "scheme": SOURCE_NAMING_VIEW_SCHEME,
                "scope": "global",
                "value": {"kind": "text", "text": view_id},
            }
        ],
    )
    delivery = assertion("context", observed.observer_agent_id, kind="delivery")
    locator = assertion(
        "locator_binding",
        observed.observer_agent_id,
        target=reference(observed.state_id, "state"),
        context_id=context["id"],
        locator={
            "kind": "filesystem_path",
            "syntax": "posix",
            "form": "absolute",
            "name": {"kind": "text", "text": name},
        },
        temporal_scope={"kind": "unknown", "reason": "fixture source view"},
        observation_id=observed.observation_id,
    )
    graph["contexts"] = [context, delivery]
    graph["locator_bindings"] = [locator]
    artifact_id = "c" * 64
    association = assertion(
        "delivery_association",
        observed.observer_agent_id,
        delivery_context_id=delivery["id"],
        slot={"kind": "text", "text": artifact_id},
        role=COLLECTION_MEMBER_ROLE,
        state=reference(observed.state_id, "state"),
        verification_observation_id=observed.observation_id,
    )
    graph["delivery_associations"] = [association]
    if describes is not None:
        graph["extensions"] = [
            assertion(
                "extension",
                observed.observer_agent_id,
                subject=reference(observed.state_id, "state"),
                property="https://nashspence.github.io/riverhog/v1/provenance/relations/describes",
                value={"type": "reference", "value": describes},
            )
        ]
    summary = validate_journal(
        create_journal(graph, recorded_by_agent_id=observed.observer_agent_id)
    )
    binding = CollectionArtifactProvenanceBindingDocument.model_validate(
        {
            "artifact_id": artifact_id,
            "journal": summary.anchor,
            "delivery_association_id": association["id"],
        }
    )
    subject = WorkArtifactSubject(
        id=artifact_id,
        role="stove0.source/v1",
        collection=CollectionRootIdentityRef(
            collection_id="1",
            archive_root_sha256="a" * 64,
            artifact_set_identity="b" * 64,
        ),
        artifact_id=artifact_id,
        bytes="3",
        sha256=hashlib.sha256(b"abc").hexdigest(),
    )
    return subject, binding, summary


def test_direct_locator_facts_preserve_context_identifier_and_exact_support() -> None:
    view_id = "urn:uuid:11111111-1111-4111-8111-111111111111"
    subject, binding, summary = _fixture(name="/camera/clip.mp4", view_id=view_id)
    facts = extract_core_facts(subject, binding, summary)
    assert facts["subject_id"] == subject.id
    assert facts["materialization_hint"] is None
    assert len(facts["locators"]) == 1
    locator = facts["locators"][0]
    assert locator["locator"]["name"]["text"] == "/camera/clip.mp4"
    assert locator["context_identifiers"][0]["value"]["text"] == view_id
    assert locator["context_endpoint"]["journal_id"] == summary.journal_id
    assert locator["context_support"]["assertion_id"] != locator["locator_support"]["assertion_id"]
    assert locator["observation_endpoint"]["object_type"] == "observation"


def test_hint_fact_uses_exact_delivered_occurrence_and_valid_missing_hint() -> None:
    fixture = _fixture(
        name="/camera/clip.mp4",
        view_id="urn:uuid:11111111-1111-4111-8111-111111111111",
        hint={"components": ["Album", "clip.mp4"]},
    )
    subject, binding, summary = fixture
    fact = extract_materialization_hint_fact(subject, binding, summary)
    assert fact["subject_id"] == subject.id
    assert fact["occurrence"]["scope"] == "external"
    assert fact["occurrence"]["object_type"] == "occurrence"
    assert fact["primary_binding"] == binding.model_dump(mode="json")
    assert fact["materialization_hint"] == {"components": ["Album", "clip.mp4"]}
    validate_materialization_hint_facts({"artifacts": [fact]}, (subject,))

    missing_subject, missing_binding, missing_summary = _fixture(
        name="/camera/other.mp4",
        view_id="urn:uuid:11111111-1111-4111-8111-111111111111",
    )
    missing = extract_materialization_hint_fact(missing_subject, missing_binding, missing_summary)
    assert missing["materialization_hint"] is None
    validate_materialization_hint_facts({"artifacts": [missing]}, (missing_subject,))


def test_observer_fails_when_exact_primary_provenance_is_unavailable() -> None:
    subject, binding, summary = _fixture(
        name="/camera/clip.mp4",
        view_id="urn:uuid:11111111-1111-4111-8111-111111111111",
    )
    observer = RiverhogProvenanceObserver(image_id="sha256:" + "f" * 64)
    support = observer.descriptor().support_for(MATERIALIZATION_HINT_OBSERVER_CONTRACT.id)
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id="a" * 64,
            observer_registration_id="canonical-hint",
            observer_descriptor_sha256=observer.descriptor().descriptor_sha256,
            observer_contract_id=support.contract_id,
            observer_contract_sha256=support.contract_sha256,
            read_actions=("read-provenance",),
            subjects=(subject,),
        )
    )

    class Runtime:
        def heartbeat(self) -> None:
            pass

        def open_provenance(self, _subject: WorkArtifactSubject) -> Any:
            return SimpleNamespace(binding=binding, bound_summary=lambda: summary)

    observed = observer.observe(request, cast(ContentObservationRuntime, Runtime()))
    assert observed.state == "observed"
    assert observed.facts is not None
    assert observed.facts["artifacts"][0]["materialization_hint"] is None

    class MissingRuntime(Runtime):
        def open_provenance(self, _subject: WorkArtifactSubject) -> Any:
            raise RuntimeError("root-selected primary anchor is absent")

    failed = observer.observe(request, cast(ContentObservationRuntime, MissingRuntime()))
    assert failed.state == "failed"
    assert failed.facts is None


def test_distinct_context_assertions_can_report_same_explicit_source_view() -> None:
    view_id = "urn:uuid:11111111-1111-4111-8111-111111111111"
    first = _fixture(name="/camera/clip.mp4", view_id=view_id)
    second = _fixture(name="/camera/clip.xmp", view_id=view_id)
    facts = [extract_core_facts(*item) for item in (first, second)]
    locators = [item["locators"][0] for item in facts]
    assert locators[0]["context_endpoint"] != locators[1]["context_endpoint"]
    assert locators[0]["context_identifiers"] == locators[1]["context_identifiers"]


def test_foreign_claim_requires_exact_resolved_assertion() -> None:
    view_id = "urn:uuid:11111111-1111-4111-8111-111111111111"
    _, _, source = _fixture(name="/camera/clip.mp4", view_id=view_id)
    target = external_reference(source, source.states[0]["id"])
    subject, binding, summary = _fixture(name="/camera/clip.xmp", view_id=view_id, describes=target)
    predicate = "https://nashspence.github.io/riverhog/v1/provenance/relations/describes"
    with pytest.raises(ValueError, match="exact corpus resolver"):
        extract_core_facts(subject, binding, summary, predicates=(predicate,))
    endpoint = {key: value for key, value in target.items() if key != "scope"}
    facts = extract_core_facts(
        subject, binding, summary, predicates=(predicate,), resolve_external=lambda _: endpoint
    )
    assert facts["claims"][0]["object"] == endpoint
    with pytest.raises(ValueError, match="differs"):
        extract_core_facts(
            subject,
            binding,
            summary,
            predicates=(predicate,),
            resolve_external=lambda _: {**endpoint, "object_id": summary.states[0]["id"]},
        )
