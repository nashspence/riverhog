from __future__ import annotations

import hashlib
from types import SimpleNamespace
from typing import Any, cast

import pytest
from a_stove0_filename_prefix_sidecar_evidence_contract_lib import (
    FILENAME_OBSERVER_CONTRACT,
    validate_filename_facts,
)
from a_stove0_filename_prefix_sidecar_observer import FilenamePrefixSidecarObserver
from a_stove0_riverhog_provenance_evidence_contract_lib import (
    CORE_PROVENANCE_OBSERVER_CONTRACT,
)
from a_stove0_riverhog_provenance_observer import extract_core_facts
from riverhog_protocol import CollectionArtifactProvenanceBindingDocument
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    assertion,
    create_journal,
    reference,
    validate_journal,
)
from riverhog_provenance_contracts import SOURCE_NAMING_VIEW_SCHEME
from stove0_observer_protocol import (
    CollectionRootIdentityRef,
    ContentObservationEvidence,
    ContentObservationRequest,
    ContentObservationRequestPayload,
    ObservationEvidenceSlot,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    WorkArtifactSubject,
)
from stove0_observer_support import ContentObservationResultBuilder, ContentObservationRuntime

_VIEW = "urn:uuid:11111111-1111-4111-8111-111111111111"


def _fact(artifact_id: str, name: str | None, *, hint: str | None = None) -> tuple[Any, Any]:
    observed = BoundedSourceObserver().observe(BytesSource(b"abc"))
    graph = observed.graph_fragment()
    if hint is not None:
        graph["occurrences"][0]["materialization_hint"] = {"components": [hint]}
    if name is not None:
        graph["descriptions"][0]["address_status"] = "known"
    delivery = assertion("context", observed.observer_agent_id, kind="delivery")
    graph.setdefault("contexts", []).append(delivery)
    if name is not None:
        context = assertion(
            "context",
            observed.observer_agent_id,
            kind="filesystem_namespace",
            identifiers=[
                {
                    "scheme": SOURCE_NAMING_VIEW_SCHEME,
                    "scope": "global",
                    "value": {"kind": "text", "text": _VIEW},
                }
            ],
        )
        graph["contexts"].append(context)
        graph["locator_bindings"] = [
            assertion(
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
        ]
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
            collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
        ),
        artifact_id=artifact_id,
        bytes="3",
        sha256=hashlib.sha256(b"abc").hexdigest(),
    )
    return subject, extract_core_facts(subject, binding, summary)


def _request(
    *,
    subjects: tuple[WorkArtifactSubject, ...],
    descriptor: ObserverDescriptor,
    options: dict[str, Any],
    evidence_slot: ObservationEvidenceSlot | None = None,
    core: bool = False,
) -> ContentObservationRequest:
    contract = CORE_PROVENANCE_OBSERVER_CONTRACT if core else FILENAME_OBSERVER_CONTRACT
    return ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id="a" * 64,
            observer_registration_id="core" if core else "filename",
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=contract.id,
            observer_contract_sha256=contract.contract_sha256,
            read_actions=("read-provenance",) if core else ("read-evidence",),
            subjects=subjects,
            evidence_slots=None if core else (evidence_slot,),
            options=options,
        )
    )


def test_observer_uses_only_accepted_exact_locator_evidence() -> None:
    primary, primary_fact = _fact("c" * 64, "/camera/clip.mov", hint="wrong.mov")
    sidecar, sidecar_fact = _fact("d" * 64, "/camera/clip.xmp", hint="wrong.xmp")
    subjects = (primary, sidecar)
    core_descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="core-fixture/v1",
            implementation_version="test",
            source_revision="test",
            image_id="sha256:" + "e" * 64,
            contracts=(ObserverContractSupport.from_contract(CORE_PROVENANCE_OBSERVER_CONTRACT),),
        )
    )
    core_request = _request(
        subjects=subjects, descriptor=core_descriptor, options={"predicates": []}, core=True
    )
    core_result = ContentObservationResultBuilder(core_descriptor, core_request).observed(
        {"artifacts": [primary_fact, sidecar_fact]}
    )
    evidence = ContentObservationEvidence(request=core_request, result=core_result)
    observer = FilenamePrefixSidecarObserver(image_id="sha256:" + "f" * 64)
    request = _request(
        subjects=subjects,
        descriptor=observer.descriptor(),
        options={
            "provenance_slot": "core",
            "primary_ids": [primary.id],
            "sidecar_ids": [sidecar.id],
            "sidecar_suffix": ".xmp",
        },
        evidence_slot=ObservationEvidenceSlot(
            slot="core",
            request_id=core_request.request_id,
            result_sha256=core_result.result_sha256,
            observer_contract_id=CORE_PROVENANCE_OBSERVER_CONTRACT.id,
        ),
    )
    runtime = SimpleNamespace(open_evidence=lambda slot: evidence)
    result = observer.observe(request, cast(ContentObservationRuntime, runtime))
    assert result.state == "observed"
    assert result.facts is not None
    accepted = validate_filename_facts(result.facts, subjects, request.options, request=request)
    assert [(row.primary_id, row.sidecar_id, row.rule) for row in accepted.candidates] == [
        (primary.id, sidecar.id, "stem")
    ]
    assert accepted.provenance_result_sha256 == core_result.result_sha256

    forged_support = accepted.model_dump(mode="json")
    forged_support["candidates"][0]["support"] = forged_support["candidates"][0][
        "support"
    ][:1]
    with pytest.raises(ValueError, match="support differs"):
        validate_filename_facts(forged_support, subjects, request.options, request=request)

    missing, missing_fact = _fact("d" * 64, None, hint="clip.xmp")
    missing_result = ContentObservationResultBuilder(core_descriptor, core_request).observed(
        {"artifacts": [primary_fact, missing_fact]}
    )
    missing_evidence = ContentObservationEvidence(request=core_request, result=missing_result)
    missing_request = _request(
        subjects=(primary, missing),
        descriptor=observer.descriptor(),
        options=request.options,
        evidence_slot=ObservationEvidenceSlot(
            slot="core",
            request_id=core_request.request_id,
            result_sha256=missing_result.result_sha256,
            observer_contract_id=CORE_PROVENANCE_OBSERVER_CONTRACT.id,
        ),
    )
    missing_runtime = SimpleNamespace(open_evidence=lambda slot: missing_evidence)
    missing_observed = observer.observe(
        missing_request, cast(ContentObservationRuntime, missing_runtime)
    )
    assert missing_observed.state == "observed"
    assert missing_observed.facts is not None
    assert missing_observed.facts["candidates"] == []
    assert missing_observed.facts["statuses"][1]["status"] == "no-locator"

    unavailable = SimpleNamespace(open_evidence=lambda slot: (_ for _ in ()).throw(RuntimeError()))
    failed = observer.observe(request, cast(ContentObservationRuntime, unavailable))
    assert failed.state == "failed"
    assert failed.facts is None
