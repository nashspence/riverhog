"""Exact public media evidence fixtures shared by maintained target tests."""

from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_OBSERVER_CONTRACT,
)
from a_stove0_media_archive_contract_lib import (
    SOURCE_ROLE,
    XMP_SOURCE_ROLE,
)
from a_stove0_media_metadata_contract_lib import (
    MEDIA_METADATA_FACTS_SCHEMA,
    MEDIA_METADATA_OBSERVER_CONTRACT,
    MediaArtifactFacts,
    MediaFactEvidence,
    MediaMetadataFact,
    MediaMetadataFacts,
)
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ContentObservationRequest,
    ContentObservationRequestPayload,
    ContentObservationResult,
    ContentObservationResultPayload,
    ObserverImplementation,
)
from stove0_protocol import (
    ArtifactSelection,
    CollectionRootIdentityRef,
    WorkArtifactSubject,
    WorkInputGroup,
    canonical_json_sha256,
)
from stove0_target_protocol import (
    InputArtifact,
    OperationContract,
    TargetInputAuthority,
    TargetPreflightRequest,
)


def sha(character: str) -> str:
    return character * 64


def media_preflight_request(
    operation: OperationContract,
    intent: dict[str, object],
    *,
    sidecar_capture_time: str = "2025:02:03 04:05:06-0800",
    target_options: dict[str, object] | None = None,
    primary_hint: tuple[str, ...] | None = ("Camera", "clip.mov"),
    sidecar_hint: tuple[str, ...] | None = ("Camera", "clip.xmp"),
) -> TargetPreflightRequest:
    root = CollectionRootIdentityRef(
        collection_id="11",
        archive_root_sha256=sha("1"),
        artifact_set_identity=sha("2"),
    )
    inputs = (
        InputArtifact(
            id="primary",
            role=SOURCE_ROLE,
            collection=root,
            artifact_id=sha("7"),
            bytes=str(100),
            sha256=sha("3"),
        ),
        InputArtifact(
            id="sidecar",
            role=XMP_SOURCE_ROLE,
            collection=root,
            artifact_id=sha("8"),
            bytes=str(20),
            sha256=sha("4"),
        ),
    )
    subjects = tuple(
        WorkArtifactSubject(
            id=item.id,
            role="stove0.source/v1",
            collection=item.collection,
            artifact_id=item.artifact_id,
            bytes=str(item.bytes),
            sha256=item.sha256,
        )
        for item in inputs
    )
    routed_subjects = tuple(
        subject.model_copy(update={"role": item.role})
        for subject, item in zip(subjects, inputs, strict=True)
    )
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id=sha("5"),
            observer_registration_id="exiftool",
            observer_descriptor_sha256=sha("6"),
            observer_contract_id=MEDIA_METADATA_OBSERVER_CONTRACT.id,
            observer_contract_sha256=MEDIA_METADATA_OBSERVER_CONTRACT.contract_sha256,
            subjects=subjects,
        )
    )
    facts = MediaMetadataFacts(
        artifacts=(
            MediaArtifactFacts(
                artifact_id="primary",
                state="observed",
                facts=(
                    MediaMetadataFact(
                        name="capture-time",
                        value="2025:02:03 04:05:01",
                        evidence=MediaFactEvidence(
                            artifact_id="primary",
                            field="EXIF:DateTimeOriginal",
                        ),
                    ),
                ),
            ),
            MediaArtifactFacts(
                artifact_id="sidecar",
                state="observed",
                facts=(
                    MediaMetadataFact(
                        name="capture-time",
                        value=sidecar_capture_time,
                        evidence=MediaFactEvidence(
                            artifact_id="sidecar",
                            field="XMP-xmp:CreateDate",
                        ),
                    ),
                ),
            ),
        )
    ).model_dump(mode="json")
    result = ContentObservationResult.seal(
        ContentObservationResultPayload(
            request_id=request.request_id,
            state="observed",
            observer=ObserverImplementation(
                id="fixture.exiftool/v1",
                version="1.0.0",
                source_revision="fixture",
                descriptor_sha256=sha("6"),
            ),
            observer_contract_id=MEDIA_METADATA_OBSERVER_CONTRACT.id,
            observer_contract_sha256=MEDIA_METADATA_OBSERVER_CONTRACT.contract_sha256,
            subjects=subjects,
            facts_schema=MEDIA_METADATA_FACTS_SCHEMA,
            facts=facts,
            facts_sha256=canonical_json_sha256(facts),
        )
    )
    hint_request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id=sha("5"),
            observer_registration_id="canonical-hint",
            observer_descriptor_sha256=sha("9"),
            observer_contract_id=MATERIALIZATION_HINT_OBSERVER_CONTRACT.id,
            observer_contract_sha256=MATERIALIZATION_HINT_OBSERVER_CONTRACT.contract_sha256,
            read_actions=("read-provenance",),
            subjects=subjects,
        )
    )
    entry = {
        "entry_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
        "sequence": "0",
        "json_sha256": sha("a"),
    }
    journal_id = "urn:uuid:22222222-2222-4222-8222-222222222222"
    hint_facts = {
        "artifacts": [
            {
                "subject_id": subject.id,
                "primary_binding": {
                    "artifact_id": str(subject.artifact_id),
                    "journal": {
                        "journal_id": journal_id,
                        "through": entry,
                        "prefix_sha256": sha("b"),
                        "prefix_bytes": "100",
                    },
                    "delivery_association_id": ("urn:uuid:33333333-3333-4333-8333-333333333333"),
                },
                "occurrence": {
                    "scope": "external",
                    "journal_id": journal_id,
                    "entry": entry,
                    "assertion_id": "urn:uuid:44444444-4444-4444-8444-444444444444",
                    "object_id": "urn:uuid:55555555-5555-4555-8555-555555555555",
                    "object_type": "occurrence",
                },
                "materialization_hint": ({"components": list(hint)} if hint is not None else None),
            }
            for subject, hint in zip(subjects, (primary_hint, sidecar_hint), strict=True)
        ]
    }
    hint_result = ContentObservationResult.seal(
        ContentObservationResultPayload(
            request_id=hint_request.request_id,
            state="observed",
            observer=ObserverImplementation(
                id="fixture.canonical-hint/v1",
                version="1.0.0",
                source_revision="fixture",
                descriptor_sha256=sha("9"),
            ),
            observer_contract_id=MATERIALIZATION_HINT_OBSERVER_CONTRACT.id,
            observer_contract_sha256=MATERIALIZATION_HINT_OBSERVER_CONTRACT.contract_sha256,
            subjects=subjects,
            facts_schema=MATERIALIZATION_HINT_OBSERVER_CONTRACT.facts_schema,
            facts=hint_facts,
            facts_sha256=canonical_json_sha256(hint_facts),
        )
    )
    evidence = tuple(
        sorted(
            (
                ContentObservationEvidence(request=request, result=result),
                ContentObservationEvidence(request=hint_request, result=hint_result),
            ),
            key=lambda item: item.request.request_id,
        )
    )
    return TargetPreflightRequest(
        invocation_sha256=sha("b"),
        operation_id=operation.id,
        operation_contract_sha256=operation.contract_sha256,
        inputs=TargetInputAuthority.from_selection(ArtifactSelection.seal(routed_subjects)),
        input_groups=(WorkInputGroup(primary_id="primary", associated_ids=("sidecar",)),),
        intent=intent,
        target_options=dict(target_options or {}),
        observations=evidence,
    )


__all__ = ["media_preflight_request", "sha"]
