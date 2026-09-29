"""Controller-selected predecessor evidence is a scoped observer read authority."""

from __future__ import annotations

from typing import Literal, cast

import pytest
from riverhog_protocol.artifact_identity import ArtifactId
from stove0_observer_protocol import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    CollectionRootIdentityRef,
    ContentObservationEvidence,
    ContentObservationInvocation,
    ContentObservationRequest,
    ContentObservationRequestPayload,
    JsonSchemaValidationProfile,
    ObservationEvidenceSlot,
    ObserverContract,
    ObserverContractPayload,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    ObserverRuntimeAuthority,
    WorkArtifactSubject,
)
from stove0_observer_support import ContentObservationResultBuilder, ContentObservationRuntime


def _contract(
    name: str, action: Literal["read-inputs", "read-provenance", "read-evidence"]
) -> ObserverContract:
    options = JsonSchemaValidationProfile.from_schema(
        name + "-options/v1",
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "additionalProperties": False,
        },
    )
    schema = JsonSchemaValidationProfile.from_schema(
        name + "-facts/v1",
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "additionalProperties": False,
            "properties": {"value": {"type": "string"}},
            "required": ["value"],
        },
    )
    return ObserverContract.seal(
        ObserverContractPayload(
            id=name + "/v1",
            read_actions=(action,),
            options_schema=options,
            facts_schema=schema,
            facts_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
        )
    )


def _descriptor(contract: ObserverContract) -> ObserverDescriptor:
    return ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="fixture.evidence-observer/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "a" * 64,
            contracts=(ObserverContractSupport.from_contract(contract),),
        )
    )


def _subject() -> WorkArtifactSubject:
    return WorkArtifactSubject(
        id="source",
        role="fixture.source/v1",
        collection=CollectionRootIdentityRef(
            collection_id=cast(int, "1"),
            archive_root_sha256="b" * 64,
            artifact_set_identity="c" * 64,
        ),
        artifact_id=ArtifactId("d" * 64),
        bytes=cast(int, "3"),
        sha256="e" * 64,
    )


def _evidence() -> ContentObservationEvidence:
    contract = _contract("fixture.predecessor", "read-inputs")
    descriptor = _descriptor(contract)
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id="f" * 64,
            observer_registration_id="predecessor",
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=contract.id,
            observer_contract_sha256=contract.contract_sha256,
            read_actions=contract.read_actions,
            subjects=(_subject(),),
        )
    )
    return ContentObservationEvidence(
        request=request,
        result=ContentObservationResultBuilder(descriptor, request).observed({"value": "exact"}),
    )


def _request(evidence: ContentObservationEvidence) -> ContentObservationRequest:
    contract = _contract("fixture.consumer", "read-evidence")
    descriptor = _descriptor(contract)
    return ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id=evidence.request.work_id,
            observer_registration_id="consumer",
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=contract.id,
            observer_contract_sha256=contract.contract_sha256,
            read_actions=contract.read_actions,
            subjects=evidence.request.subjects,
            evidence_slots=(
                ObservationEvidenceSlot(
                    slot="primary-provenance",
                    request_id=evidence.request.request_id,
                    result_sha256=evidence.result.result_sha256,
                    observer_contract_id=evidence.request.observer_contract_id,
                ),
            ),
        )
    )


class _Api:
    def __init__(self) -> None:
        self.archive_root = "b" * 64
        self.checks = 0

    def get_collection(self, collection_id: int) -> dict[str, str]:
        assert collection_id == 1
        self.checks += 1
        return {
            "id": "1",
            "archive_root_sha256": self.archive_root,
            "artifact_set_identity": "c" * 64,
        }


def test_exact_predecessor_evidence_is_read_only_after_current_root_check() -> None:
    evidence = _evidence()
    request = _request(evidence)
    authority = ObserverRuntimeAuthority(
        riverhog_base_url="https://riverhog.invalid",
        capability_token="fixture-token",
        declared_workspace_protection="memory-backed",
    )
    invocation = ContentObservationInvocation(
        request=request,
        claim_id="claim",
        fence=1,
        runtime=authority,
        evidence=(evidence,),
    )
    with pytest.raises(ValueError, match="differs from the sealed request slots"):
        ContentObservationInvocation(
            request=request,
            claim_id="claim",
            fence=1,
            runtime=authority,
            evidence=(),
        )
    api = _Api()
    with ContentObservationRuntime(
        api,
        request=request,
        claim_id=invocation.claim_id,
        fence=invocation.fence,
        evidence=invocation.evidence,
        declared_workspace_protection="memory-backed",
    ) as runtime:
        assert runtime.open_evidence("primary-provenance") == evidence
        assert api.checks == 1
        with pytest.raises(ValueError, match="not declared"):
            runtime.open_evidence("unknown")
        with pytest.raises(PermissionError, match="payload read authority"):
            runtime.read_bytes(request.subjects[0], maximum_bytes=3)
        api.archive_root = "0" * 64
        with pytest.raises(RuntimeError, match="root changed"):
            runtime.open_evidence("primary-provenance")
