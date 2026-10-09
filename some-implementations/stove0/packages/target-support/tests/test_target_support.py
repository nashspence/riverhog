from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from http_api_contracts import (
    http_operation_for_request,
    http_operation_inventory,
    operation_openapi,
)
from http_api_contracts.control import ControlBudgetExhausted
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import ValidationError
from riverhog_canonical_json import require_canonical_json
from riverhog_client import ProducerArtifactCustody, ProducerArtifactIdentity, ProducerFile
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_client.client import ApiClient
from riverhog_client.processing import (
    ClaimedCollectionRuntime,
    ClaimedCollectionRuntimeRegistry,
    CollectionTransformRuntime,
    DerivedCollectionReceipt,
    ProcessingWorkspace,
)
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
    CollectionUploadCustodyObjectDocument,
    CollectionUploadProvenanceCustodyObjectDocument,
    Conflict,
    DownloadAllowanceExceeded,
    Unauthorized,
)
from riverhog_protocol.collection_workflows import (
    ArtifactDispositionSetIdentity,
    CollectionDerivation,
)
from riverhog_protocol.collection_workflows import (
    canonical_json_sha256 as riverhog_canonical_json_sha256,
)
from riverhog_provenance import BoundedSourceObserver, BytesSource, new_id
from stove0_extension_support import ExecutionOwner, ExecutionPermit
from stove0_protocol import (
    ArtifactSelection,
    CollectionRootIdentityRef,
    ControllerEvidence,
    ControllerEvidencePayload,
    ExecutionEnvelope,
    ExecutionEnvelopePayload,
    JsonSchemaValidationProfile,
    OperationIdentityRef,
    RecipeIdentityRef,
    SemanticValidationProfile,
    SemanticValidationProfilePayload,
    TargetPlanBinding,
    WorkArtifactSubject,
    WorkflowPlan,
    WorkflowPlanPayload,
    WorkIdentity,
    WorkInputGroup,
    WorkPayload,
)
from stove0_target_client import TargetCallbackClient, TargetClient, TargetProtocolError
from stove0_target_protocol import (
    DEPARTURE_EFFECT_HTTP_OPERATIONS,
    OutputArtifactSetIdentity,
    SemanticIntentConformanceVector,
    SemanticIntentConformanceVectors,
    TargetCallbackAccess,
    TargetInputAuthority,
    TargetInputPage,
    TargetPreRootResult,
    TargetProductionAuthority,
    TargetProductionAuthorityPayload,
    TargetProductionSealResponse,
)
from stove0_target_support import (
    DEFAULT_TERMINAL_STATE_RETENTION_SECONDS,
    EFFECT_TARGET_PROTOCOL,
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    TARGET_HTTP_OPERATIONS,
    TARGET_TERMINAL_STATE_RETENTION_ENV,
    AcceptedTargetJob,
    EffectPlan,
    EffectPlanPayload,
    InputArtifact,
    InputArtifactContract,
    OperationContract,
    OperationContractPayload,
    OutputArtifact,
    OutputArtifactContract,
    OutputCollectionRef,
    PersistentTargetService,
    TargetCollectionPublication,
    TargetConformanceCase,
    TargetDescriptor,
    TargetDescriptorPayload,
    TargetEffectCommitUncertain,
    TargetExecutionCanceled,
    TargetExecutionEvidence,
    TargetExecutionFailure,
    TargetExecutionInapplicable,
    TargetExecutionRuntime,
    TargetExecutionSession,
    TargetHttpBinding,
    TargetJobDeclaration,
    TargetJobRequest,
    TargetJobStatus,
    TargetOperationSupport,
    TargetPreflightRequest,
    TargetPreflightResponse,
    TargetProgress,
    TargetRuntimeAuthority,
    TargetServiceError,
    TransformPlan,
    TransformPlanPayload,
    canonical_json_bytes,
    canonical_json_sha256,
    conformance_report,
    target_schema_bundle,
    terminal_state_retention_seconds,
    validate_preflight_response_against_request,
    validate_status_against_request,
)
from stove0_target_support.output_checkpoint import TargetOutputCheckpoint

REPO_ROOT = Path(__file__).resolve().parents[5]
_EXECUTION_PREIMAGE = canonical_json_bytes({"format": "fixture-execution/v1", "optional": None})
_EXECUTION_SHA256 = hashlib.sha256(_EXECUTION_PREIMAGE).hexdigest()


@pytest.fixture(autouse=True)
def fixture_dispatch_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    # These executors use synthetic claims. Live capability/root validation has
    # its own HTTP witnesses and is exercised by supplied component integration.
    monkeypatch.setattr(
        PersistentTargetService,
        "_validate_live_authority",
        lambda _self, _request, *, deadline: None,
    )


def _sha(character: str) -> str:
    return character * 64


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, b"null"),
        (True, b"true"),
        (False, b"false"),
        (0, b"0"),
        (-12, b"-12"),
        (1.0, b"1"),
        (-0.0, b"0"),
        (1e-6, b"0.000001"),
        (1e-7, b"1e-7"),
        (1e20, b"100000000000000000000"),
        (1e21, b"1e+21"),
        (333333333.3333333, b"333333333.3333333"),
        ('quotes " controls \n and unicode å', '"quotes \\" controls \\n and unicode å"'.encode()),
        (
            {"z": 1, "a": [3, 2, 1], "😀": "astral", "€": "bmp"},
            '{"a":[3,2,1],"z":1,"€":"bmp","😀":"astral"}'.encode(),
        ),
    ],
)
def test_local_jcs_matches_ecmascript_for_representative_i_json_values(
    value: Any,
    expected: bytes,
) -> None:
    assert canonical_json_bytes(value) == expected


def _operation() -> OperationContract:
    return OperationContract.seal(
        OperationContractPayload(
            id="fixture.copy/v1",
            intent_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.copy-intent/v1",
                {
                    "type": "object",
                    "properties": {"suffix": {"type": "string"}},
                    "required": ["suffix"],
                    "additionalProperties": False,
                },
            ),
            intent_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            inputs=(
                InputArtifactContract(
                    role="fixture.source/v1",
                    allowed_dispositions=("transformed",),
                ),
            ),
            outputs=(
                OutputArtifactContract(
                    role="fixture.output/v1",
                    derived_from_roles=("fixture.source/v1",),
                ),
            ),
            source_collection_retirement_permitted=True,
        )
    )


def _target(operation: OperationContract) -> TargetDescriptor:
    return TargetDescriptor.seal(
        TargetDescriptorPayload(
            implementation_id="fixture.target/v1",
            implementation_version="1.0.0",
            source_revision="fixture",
            image_id="sha256:" + _sha("9"),
            operations=(
                TargetOperationSupport(
                    operation_id=operation.id,
                    operation_contract_sha256=operation.contract_sha256,
                    options_schema=JsonSchemaValidationProfile.from_schema(
                        "fixture.target-options/v1",
                        {"type": "object", "additionalProperties": False},
                    ),
                ),
            ),
        )
    )


def test_target_descriptor_requires_an_oci_image_id() -> None:
    payload = _target(_operation()).model_dump(mode="json", exclude={"descriptor_sha256"})
    payload["image_id"] = _sha("9")
    with pytest.raises(ValidationError, match="image_id"):
        TargetDescriptorPayload.model_validate(payload)


def _input() -> InputArtifact:
    return InputArtifact(
        id="source",
        role="fixture.source/v1",
        collection=CollectionRootIdentityRef(
            collection_id=str(1),
            archive_root_sha256=_sha("1"),
            artifact_set_identity=_sha("2"),
        ),
        artifact_id=_sha("4"),
        bytes=str(12),
        sha256=_sha("3"),
    )


def _input_authority() -> TargetInputAuthority:
    value = _input()
    return TargetInputAuthority.from_selection(
        ArtifactSelection.seal(
            (
                WorkArtifactSubject.model_validate(
                    value.model_dump(mode="python", exclude_none=True)
                ),
            )
        )
    )


def _callback_access() -> TargetCallbackAccess:
    return TargetCallbackAccess(
        stove0_base_url="https://stove0.invalid",
        token="callback-secret",
    )


def _work() -> WorkIdentity:
    return WorkIdentity.seal(
        WorkPayload(
            recipe=RecipeIdentityRef(id="fixture.recipe/v1", revision="1", sha256=_sha("4")),
            inputs=(_input().collection,),
            effective_intent={"suffix": ".copy"},
        )
    )


def _workflow_for(operation: OperationContract, target: TargetDescriptor) -> WorkflowPlan:
    work = _work()
    workflow = WorkflowPlan.seal(
        WorkflowPlanPayload(
            work=work,
            operation=OperationIdentityRef(id=operation.id, sha256=operation.contract_sha256),
            target_registration_id="fixture-target",
            target_descriptor_sha256=target.descriptor_sha256,
            source_collection_retirement_policy="retain",
        )
    )
    return workflow


def _plan(
    operation: OperationContract,
    target: TargetDescriptor,
) -> TransformPlan:
    return TransformPlan.seal(
        TransformPlanPayload(
            invocation_sha256=_workflow_for(operation, target).workflow_plan_sha256,
            target_implementation_id=target.implementation_id,
            target_descriptor_sha256=target.descriptor_sha256,
            operation_id=operation.id,
            operation_contract_sha256=operation.contract_sha256,
            inputs=_input_authority(),
            intent={"suffix": ".copy"},
            target_options={},
        )
    )


def _controller_evidence(
    operation: OperationContract,
    target: TargetDescriptor,
    plan: TransformPlan,
) -> ControllerEvidence:
    work = _work()
    workflow = _workflow_for(operation, target)
    binding = TargetPlanBinding(
        protocol=target.protocol,
        target_implementation_id=target.implementation_id,
        target_descriptor_sha256=target.descriptor_sha256,
        operation_contract_sha256=operation.contract_sha256,
        plan=plan.binding_document(),
        plan_sha256=plan.plan_sha256,
    )
    envelope = ExecutionEnvelope.seal(
        ExecutionEnvelopePayload(
            claim_id=work.work_id,
            fence=2,
            workflow_plan=workflow,
            target_plan=binding,
        )
    )
    return ControllerEvidence.seal(ControllerEvidencePayload(execution_envelope=envelope))


def _request_for(
    operation: OperationContract,
    target: TargetDescriptor,
) -> TargetJobRequest:
    plan = _plan(operation, target)
    evidence = _controller_evidence(operation, target, plan)
    declaration = TargetJobDeclaration(
        job_id=evidence.execution_envelope.execution_envelope_sha256,
        claim_id=evidence.execution_envelope.workflow_plan.work.work_id,
        fence=2,
        controller_evidence=evidence,
        plan=plan,
        declared_workspace_protection="memory-backed",
    )
    request = TargetJobRequest.seal(
        declaration,
        TargetRuntimeAuthority(
            riverhog_base_url="https://riverhog.invalid",
            capability_token="first-secret",
        ),
        _callback_access(),
    )
    return request


def _request() -> tuple[OperationContract, TargetDescriptor, TargetJobRequest]:
    operation = _operation()
    target = _target(operation)
    return operation, target, _request_for(operation, target)


def _request_at_fence(request: TargetJobRequest, fence: int) -> TargetJobRequest:
    envelope = ExecutionEnvelope.seal(
        ExecutionEnvelopePayload.model_validate(
            request.declaration.controller_evidence.execution_envelope.model_dump(
                mode="python", exclude={"execution_envelope_sha256"}
            )
        ).model_copy(update={"fence": fence})
    )
    declaration = TargetJobDeclaration(
        job_id=envelope.execution_envelope_sha256,
        claim_id=envelope.claim_id,
        fence=fence,
        controller_evidence=ControllerEvidence.seal(
            ControllerEvidencePayload(execution_envelope=envelope)
        ),
        plan=request.declaration.plan,
        declared_workspace_protection=request.declaration.declared_workspace_protection,
    )
    return TargetJobRequest.seal(declaration, request.runtime, request.callback_access)


def _effect_request() -> tuple[OperationContract, TargetDescriptor, TargetJobRequest]:
    operation = OperationContract.seal(
        OperationContractPayload(
            id="fixture.record-index/v1",
            result_kind="external-effect",
            intent_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.record-index-intent/v1",
                {"type": "object", "additionalProperties": False},
            ),
            intent_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            inputs=(
                InputArtifactContract(
                    role="fixture.source/v1",
                    allowed_dispositions=None,
                ),
            ),
            effect_receipt_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.record-index-receipt/v1",
                {
                    "type": "object",
                    "required": ["format", "row_sha256"],
                    "properties": {
                        "format": {"const": "fixture-index-receipt/v1"},
                        "row_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                    },
                    "additionalProperties": False,
                },
            ),
        )
    )
    target = TargetDescriptor.seal(
        TargetDescriptorPayload(
            protocol=EFFECT_TARGET_PROTOCOL,
            implementation_id="fixture.index-target/v1",
            implementation_version="1.0.0",
            source_revision="fixture",
            image_id="sha256:" + _sha("8"),
            operations=(
                TargetOperationSupport(
                    operation_id=operation.id,
                    operation_contract_sha256=operation.contract_sha256,
                    result_kind="external-effect",
                    options_schema=JsonSchemaValidationProfile.from_schema(
                        "fixture.index-target-options/v1",
                        {"type": "object", "additionalProperties": False},
                    ),
                ),
            ),
        )
    )
    work = WorkIdentity.seal(
        WorkPayload(
            recipe=_work().recipe,
            inputs=_work().inputs,
            effective_intent={},
        )
    )
    workflow = WorkflowPlan.seal(
        WorkflowPlanPayload(
            work=work,
            result_kind="external-effect",
            operation=OperationIdentityRef(id=operation.id, sha256=operation.contract_sha256),
            target_registration_id="fixture-index-target",
            target_descriptor_sha256=target.descriptor_sha256,
            source_collection_retirement_policy="retain",
        )
    )
    plan = EffectPlan.seal(
        EffectPlanPayload(
            invocation_sha256=workflow.workflow_plan_sha256,
            target_implementation_id=target.implementation_id,
            target_descriptor_sha256=target.descriptor_sha256,
            operation_id=operation.id,
            operation_contract_sha256=operation.contract_sha256,
            inputs=_input_authority(),
            intent={},
            target_options={},
        )
    )
    binding = TargetPlanBinding(
        protocol=target.protocol,
        target_implementation_id=target.implementation_id,
        target_descriptor_sha256=target.descriptor_sha256,
        operation_contract_sha256=operation.contract_sha256,
        plan=plan.binding_document(),
        plan_sha256=plan.plan_sha256,
    )
    envelope = ExecutionEnvelope.seal(
        ExecutionEnvelopePayload(
            claim_id=work.work_id,
            fence=2,
            workflow_plan=workflow,
            target_plan=binding,
        )
    )
    evidence = ControllerEvidence.seal(ControllerEvidencePayload(execution_envelope=envelope))
    request = TargetJobRequest.seal(
        TargetJobDeclaration(
            job_id=envelope.execution_envelope_sha256,
            claim_id=work.work_id,
            fence=2,
            controller_evidence=evidence,
            plan=plan,
            declared_workspace_protection="memory-backed",
        ),
        TargetRuntimeAuthority(
            riverhog_base_url="https://riverhog.invalid",
            capability_token="effect-secret",
        ),
        _callback_access(),
    )
    return operation, target, request


def _effect_success_status(
    operation: OperationContract,
    request: TargetJobRequest,
    *,
    attempt: int = 1,
) -> TargetJobStatus:
    return TargetExecutionRuntime(request, object()).effect_success(  # type: ignore[arg-type]
        {"format": "fixture-index-receipt/v1", "row_sha256": _sha("7")},
        operation=operation,
        execution_sha256=_sha("6"),
        attempt=attempt,
        runtime_evidence={"implementation": "fixture"},
    )


def test_preflight_job_identity_excludes_refreshable_capability_secret() -> None:
    operation, target, request = _request()
    second = TargetJobRequest.seal(
        request.declaration,
        request.runtime.model_copy(update={"capability_token": "replacement-secret"}),
        request.callback_access,
    )
    assert second.request_sha256 == request.request_sha256

    preflight_request = TargetPreflightRequest(
        invocation_sha256=request.declaration.plan.invocation_sha256,
        operation_id=operation.id,
        operation_contract_sha256=operation.contract_sha256,
        inputs=request.declaration.plan.inputs,
        intent=request.declaration.plan.intent,
        target_options=request.declaration.plan.target_options,
    )
    response = TargetPreflightResponse(descriptor=target, plan=request.declaration.plan)
    validate_preflight_response_against_request(response, preflight_request)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("invocation_sha256", _sha("f")),
        ("input_groups", (WorkInputGroup(primary_id="source"),)),
        ("intent", {"count": True}),
        ("target_options", {"count": True}),
        ("target_options", {"count": 1, "injected": "option"}),
    ],
)
def test_preflight_cannot_change_the_recipe_invocation(
    field: str,
    replacement: Any,
) -> None:
    _operation, target, job = _request()
    payload = TransformPlanPayload.model_validate(
        job.declaration.plan.model_dump(exclude={"plan_sha256"})
    ).model_copy(update={"intent": {"count": 1}, "target_options": {"count": 1}})
    plan = TransformPlan.seal(payload)
    request = TargetPreflightRequest.model_validate(
        plan.model_dump(
            exclude={
                "plan_sha256",
                "target_implementation_id",
                "target_descriptor_sha256",
                "observation_result_sha256s",
                "execution_parameters",
            }
        )
    )
    changed = TransformPlan.seal(payload.model_copy(update={field: replacement}))
    with pytest.raises(ValueError, match="differs from the request"):
        validate_preflight_response_against_request(
            TargetPreflightResponse(descriptor=target, plan=changed), request
        )
    sealed = TransformPlan.seal(
        payload.model_copy(update={"execution_parameters": {"tool_plan": "exact-target-detail"}})
    )
    validate_preflight_response_against_request(
        TargetPreflightResponse(descriptor=target, plan=sealed), request
    )
    assert sealed.plan_sha256 != plan.plan_sha256


def _success_status(
    operation: OperationContract,
    request: TargetJobRequest,
) -> TargetJobStatus:
    output = OutputArtifact(
        id="output",
        role="fixture.output/v1",
        artifact_id=_sha("6"),
        bytes=str(12),
        sha256=_sha("5"),
    )
    declaration = request.declaration
    workflow = declaration.controller_evidence.execution_envelope.workflow_plan
    derivation = CollectionDerivation(
        execution_id=declaration.job_id,
        claim_id=declaration.claim_id,
        fence=declaration.fence,
        recipe=workflow.work.recipe.to_identity(),
        operation=workflow.operation.to_identity(),
        input_set_sha256=_sha("a"),
        artifact_set_sha256=_sha("b"),
        execution_envelope_sha256=declaration.job_id,
        execution_sha256=_EXECUTION_SHA256,
        controller_evidence=declaration.controller_evidence.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        controller_evidence_sha256=riverhog_canonical_json_sha256(
            declaration.controller_evidence.model_dump(
                mode="json", by_alias=True, exclude_none=True
            )
        ),
        disposition_set=ArtifactDispositionSetIdentity(
            disposition_count=1,
            output_edge_count=1,
            output_artifact_count=1,
            sha256=_sha("d"),
        ),
    )
    production = TargetProductionAuthority.seal(
        TargetProductionAuthorityPayload(
            job_id=declaration.job_id,
            plan_sha256=declaration.plan.plan_sha256,
            outputs=OutputArtifactSetIdentity.seal((output,)),
            disposition_count=1,
            disposition_sha256=_sha("e"),
            source_edge_count=1,
            source_edge_sha256=_sha("f"),
            riverhog_disposition_set=derivation.disposition_set,
        )
    )
    return TargetJobStatus(
        job_id=request.declaration.job_id,
        state="succeeded",
        attempt=1,
        request_sha256=request.request_sha256,
        plan_sha256=request.declaration.plan.plan_sha256,
        progress=TargetProgress(phase="done", completed=1, total=1, unit="artifacts"),
        production=production,
        output_collection=OutputCollectionRef(
            collection_id=str(7),
            archive_root_sha256=_sha("6"),
            artifact_set_identity=_sha("7"),
            derivation_sha256=derivation.sha256,
        ),
        execution_evidence=TargetExecutionEvidence(
            target_descriptor_sha256=request.declaration.plan.target_descriptor_sha256,
            operation_contract_sha256=operation.contract_sha256,
            plan_sha256=request.declaration.plan.plan_sha256,
            execution_sha256=_EXECUTION_SHA256,
            runtime={"tool": "fixture"},
        ),
        derivation=derivation.as_dict(),
    )


def test_success_status_is_operation_checked_and_failure_cannot_publish() -> None:
    operation, _target_descriptor, request = _request()
    status = _success_status(operation, request)
    validate_status_against_request(status, request, operation)

    with pytest.raises(ValidationError, match="failed target status cannot publish a result"):
        TargetJobStatus(
            **status.model_dump(mode="python", exclude={"state", "failure"}),
            state="failed",
            failure={"code": "fixture.failure/v1", "message": "failed", "retryable": False},
        )

    running_with_failure = {
        "job_id": request.declaration.job_id,
        "state": "running",
        "attempt": 1,
        "request_sha256": request.request_sha256,
        "plan_sha256": request.declaration.plan.plan_sha256,
        "progress": {"phase": "running", "completed": 0},
        "failure": {"code": "fixture.failure/v1", "message": "failed", "retryable": False},
    }
    with pytest.raises(ValidationError, match="nonterminal target status"):
        TargetJobStatus.model_validate(running_with_failure)
    with pytest.raises(JsonSchemaValidationError):
        Draft202012Validator(TargetJobStatus.model_json_schema()).validate(running_with_failure)


def test_persistent_status_preserves_embedded_authority_number_types(tmp_path: Path) -> None:
    operation, target, request = _request()
    base = _success_status(operation, request)
    assert base.derivation is not None
    assert base.output_collection is not None
    original = CollectionDerivation.from_mapping(base.derivation)
    controller_evidence = {
        **original.controller_evidence,
        "fixture_coordinate": 45.0,
    }
    derivation = CollectionDerivation(
        execution_id=original.execution_id,
        claim_id=original.claim_id,
        fence=original.fence,
        recipe=original.recipe,
        operation=original.operation,
        input_set_sha256=original.input_set_sha256,
        artifact_set_sha256=original.artifact_set_sha256,
        execution_envelope_sha256=original.execution_envelope_sha256,
        execution_sha256=original.execution_sha256,
        controller_evidence=controller_evidence,
        controller_evidence_sha256=riverhog_canonical_json_sha256(controller_evidence),
        disposition_set=original.disposition_set,
    )
    status = TargetJobStatus.model_validate(
        {
            **base.model_dump(mode="json", by_alias=True),
            "derivation": derivation.as_dict(),
            "output_collection": base.output_collection.model_copy(
                update={"derivation_sha256": derivation.sha256}
            ).model_dump(mode="json", by_alias=True),
        }
    )
    state_root = tmp_path / "state"
    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=lambda *_args: status,
    )
    try:
        service._write_model(service._status_path(status.job_id), status)
        assert service._load_status(status.job_id) == status
    finally:
        service.close()


def test_effect_success_is_canonical_bound_and_collection_free() -> None:
    operation, target, request = _effect_request()
    first = _effect_success_status(operation, request)
    second = _effect_success_status(operation, request)

    validate_status_against_request(first, request, operation)
    assert first == second
    assert first.protocol == EFFECT_TARGET_PROTOCOL
    assert first.production is None
    assert first.output_collection is None
    assert first.derivation is None
    assert first.effect_receipt is not None
    assert first.effect_receipt.receipt_sha256 == second.effect_receipt.receipt_sha256  # type: ignore[union-attr]
    assert first.effect_receipt.target_descriptor_sha256 == target.descriptor_sha256

    with pytest.raises(ValidationError, match="requires only execution evidence"):
        TargetJobStatus.model_validate(
            {
                **first.model_dump(mode="json"),
                "production": _success_status(_operation(), _request()[2]).production.model_dump(
                    mode="json"
                ),
            }
        )


def test_effect_execution_uses_only_generic_claimed_collection_read_custody() -> None:
    _operation_contract, _target_descriptor, request = _effect_request()
    execution = TargetExecutionRuntime.from_request(request)
    try:
        assert isinstance(execution.runtime, ClaimedCollectionRuntime)
        assert not isinstance(execution.runtime, CollectionTransformRuntime)
        assert not hasattr(execution.runtime, "spec")
        assert not hasattr(execution.runtime, "writer")
        with pytest.raises(RuntimeError, match="cannot publish"):
            execution.open_collection_publication(implementation=_target_descriptor)
    finally:
        execution.runtime.close()


def test_effect_receipt_is_operation_schema_checked() -> None:
    operation, _target, request = _effect_request()
    status = _effect_success_status(operation, request)
    assert status.effect_receipt is not None
    changed = status.model_copy(
        update={
            "effect_receipt": status.effect_receipt.model_copy(
                update={"result": {"format": "fixture-index-receipt/v1"}}
            )
        }
    )
    with pytest.raises(Exception, match="row_sha256"):
        validate_status_against_request(changed, request, operation)


def test_target_runtime_builds_complete_success_status() -> None:
    operation, _target_descriptor, request = _request()
    expected = _success_status(operation, request)
    assert expected.derivation is not None
    derivation = CollectionDerivation.from_mapping(expected.derivation)
    output_collection = expected.output_collection
    assert output_collection is not None
    session = TargetExecutionSession(request, 1, ClaimedCollectionRuntimeRegistry())
    assert expected.production is not None

    class Runtime:
        def finish_incremental_publication(
            self,
            _writer: object,
            *,
            execution_sha256: str,
            disposition_set: ArtifactDispositionSetIdentity,
            **_kwargs: object,
        ) -> DerivedCollectionReceipt:
            assert execution_sha256 == _EXECUTION_SHA256
            assert disposition_set == derivation.disposition_set
            records = {
                record.kind: b"".join(record.read()) for record in _kwargs["completion_records"]
            }
            assert records["target-execution"] == _EXECUTION_PREIMAGE
            assert b"null" in records["target-execution"]
            assert b"archive_root_sha256" not in records["target-result"]
            assert set(records) == {
                "invocation",
                "implementation",
                "target-execution",
                "target-output-declarations",
                "target-result",
            }
            return DerivedCollectionReceipt(
                collection_id=output_collection.collection_id,
                archive_root_sha256=output_collection.archive_root_sha256,
                artifact_set_identity=output_collection.artifact_set_identity,
                derivation=derivation,
            )

    class Callback:
        def iter_outputs(self, _production: object):
            yield OutputArtifact(
                id="output",
                role="fixture.output/v1",
                artifact_id=_sha("6"),
                bytes="12",
                sha256=_sha("5"),
            )

        def seal_target_execution_production(self, job_id: str) -> TargetProductionSealResponse:
            assert job_id == request.declaration.job_id
            return TargetProductionSealResponse(state="sealed", production=expected.production)

    class Writer:
        pass

    runtime = TargetExecutionRuntime(request, Runtime(), session=session)  # type: ignore[arg-type]
    runtime._input_client = Callback()  # type: ignore[assignment]
    publication = TargetCollectionPublication(
        runtime,
        Writer(),
        _target_descriptor,  # type: ignore[arg-type]
    )

    result = publication.finish_success(
        operation=operation,
        execution_sha256=_EXECUTION_SHA256,
        execution_preimage=_EXECUTION_PREIMAGE,
        runtime_evidence={"tool": "fixture"},
    )
    assert result == expected
    assert session.completed_status == expected


def test_incremental_publication_releases_local_output_only_after_exact_custody(
    tmp_path: Path,
) -> None:
    content = b"completed target output"
    local = tmp_path / "result.bin"
    local.write_bytes(content)
    output = OutputArtifact(
        id="output",
        role="fixture.output/v1",
        artifact_id=_sha("6"),
        bytes=str(len(content)),
        sha256=hashlib.sha256(content).hexdigest(),
    )

    _operation_contract, implementation, _request_document = _request()

    class Writer:
        pass

    writer = Writer()

    class Runtime:
        def append_incremental_output(
            self,
            _writer: object,
            _source: object,
            *,
            identity: ProducerArtifactIdentity,
            output_id: str,
            inputs: object,
            history_extent: str,
        ) -> tuple[ProducerArtifactCustody, ...]:
            assert local.exists()
            assert output_id == output.id
            assert history_extent == "bound-and-required-history"
            member = ArtifactMemberIdentityDocument.model_validate(
                {
                    "artifact_id": identity.artifact_id,
                    "bytes": str(identity.bytes),
                    "sha256": identity.sha256,
                }
            )
            primary = build_member_journal(
                member=member,
                observation=BoundedSourceObserver().observe(BytesSource(content)),
                delivery_context_id=new_id(),
                attribution=ProducerAttribution(
                    "fixture", "fixture", "1", "event", "fixture", {}, _sha("1")
                ),
                materialization_hint=None,
            )
            primary_sha256 = hashlib.sha256(primary.content).hexdigest()
            receipt = ProducerArtifactCustody(
                identity,
                CollectionUploadArtifactCustodyReceiptDocument.seal(
                    collection_id=7,
                    artifact_id=identity.artifact_id,
                    bytes=identity.bytes,
                    sha256=identity.sha256,
                    primary=primary.binding,
                    completion_requirement_sha256=None,
                    provenance_objects=(
                        CollectionUploadProvenanceCustodyObjectDocument.model_validate(
                            {
                                "object_id": "provenance-payload-" + primary_sha256,
                                "relative_path": "provenance/payloads/"
                                + primary_sha256[:2]
                                + "/"
                                + primary_sha256
                                + ".bin.age",
                                "plaintext_bytes": str(len(primary.content)),
                                "plaintext_sha256": primary_sha256,
                                "sealed_receipt_sha256": _sha("a"),
                            }
                        ),
                    ),
                    archive_objects=(
                        CollectionUploadCustodyObjectDocument(
                            volume_id="pack-" + "0" * 64,
                            sealed_receipt_sha256=_sha("a"),
                        ),
                    ),
                ),
            )
            return (receipt,)

    class Execution:
        runtime = Runtime()
        job_id = _sha("1")
        session = None
        _publications: list[Any] = []

        class Callback:
            def declare_target_execution_output(
                self, job_id: str, artifact: OutputArtifact
            ) -> None:
                assert job_id == _sha("1")
                assert artifact == output

            def declare_target_execution_source_edge(self, job_id: str, edge: object) -> None:
                assert job_id == _sha("1")

        _input_client = Callback()

        def resolve_input_ids(self, values):
            assert values == ("source",)
            return (object(),)

    publication = TargetCollectionPublication(Execution(), writer, implementation)  # type: ignore[arg-type]
    custody = publication.append(
        ProducerFile(local, output.artifact_id, allow_missing_materialization_hint=True),
        output,
        derived_from=("source",),
    )

    assert custody[0].artifact == ProducerArtifactIdentity(
        output.artifact_id,
        output.bytes,
        output.sha256,
    )
    assert not local.exists()


class FixtureTargetClient:
    def __init__(
        self,
        target: TargetDescriptor,
        request: TargetJobRequest,
        status: TargetJobStatus,
    ) -> None:
        self.target = target
        self.request = request
        self.status_value = status

    def descriptor(self) -> TargetDescriptor:
        return self.target

    def preflight(self, _request: TargetPreflightRequest) -> TargetPreflightResponse:
        return TargetPreflightResponse(descriptor=self.target, plan=self.request.declaration.plan)

    def put_job(
        self,
        _request: TargetJobRequest,
        *,
        operation: OperationContract,
    ) -> TargetJobStatus:
        assert operation.id == self.request.declaration.plan.operation_id
        return self.status_value

    def status(
        self,
        request: TargetJobRequest,
        *,
        operation: OperationContract,
    ) -> TargetJobStatus:
        assert request == self.request
        assert operation.id == self.request.declaration.plan.operation_id
        return self.status_value


def test_conformance_report_proves_preflight_and_idempotent_submission() -> None:
    operation, target, request = _request()
    status = _success_status(operation, request)
    report = conformance_report(
        FixtureTargetClient(target, request, status),
        cases=(TargetConformanceCase(operation=operation, job_request=request),),
    )
    assert report.format == "stove0-target-conformance-result/v1"
    assert report.status == "conformant"
    assert report.descriptor.transport == "riverhog-capability/v1"
    assert report.coverage.model_dump() == {"advertised": 1, "exercised": 1, "complete": True}
    assert report.operation_evidence[0].semantic_conformance.status == "schema-only"
    assert report.operations[0].semantic_conformance == "schema-only"
    assert report.operation_evidence[0].accepted_job == request.accepted()
    assert type(report).model_validate_json(report.model_dump_json()) == report

    changed = report.model_dump(mode="json")
    changed["operations"][0]["result_kind"] = "external-effect"
    with pytest.raises(ValidationError, match="differs from its descriptor"):
        type(report).model_validate(changed)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("intent", {"suffix": 42}, "intent violates its cited schema"),
        ("target_options", {"unexpected": True}, "options violates its cited schema"),
    ),
)
def test_target_conformance_result_revalidates_embedded_operation_schemas(
    field: str,
    value: dict[str, object],
    message: str,
) -> None:
    operation, target, request = _request()
    report = conformance_report(
        FixtureTargetClient(target, request, _success_status(operation, request)),
        cases=(TargetConformanceCase(operation=operation, job_request=request),),
    )
    changed = report.model_dump(mode="json")
    changed["operation_evidence"][0]["preflight_request"][field] = value

    with pytest.raises(ValidationError, match=message):
        type(report).model_validate(changed)


def test_conformance_report_uses_protocol_owned_external_effect_result_kind() -> None:
    operation, target, request = _effect_request()
    report = conformance_report(
        FixtureTargetClient(target, request, _effect_success_status(operation, request)),
        cases=(TargetConformanceCase(operation=operation, job_request=request),),
    )

    assert report.status == "conformant"
    assert report.operations[0].result_kind == "external-effect"
    assert report.operation_evidence[0].operation.result_kind == "external-effect"


def test_conformance_report_uses_one_exact_target_descriptor_snapshot() -> None:
    operation, target, request = _request()
    status = _success_status(operation, request)

    class SnapshotClient(FixtureTargetClient):
        def __init__(self) -> None:
            super().__init__(target, request, status)
            self.descriptor_calls = 0

        def descriptor(self) -> TargetDescriptor:
            self.descriptor_calls += 1
            if self.descriptor_calls > 1:
                raise AssertionError("conformance report reread its target descriptor")
            return super().descriptor()

    client = SnapshotClient()
    report = conformance_report(
        client,
        cases=(TargetConformanceCase(operation=operation, job_request=request),),
    )

    assert report.descriptor.descriptor_sha256 == target.descriptor_sha256
    assert report.coverage.model_dump() == {"advertised": 1, "exercised": 1, "complete": True}
    assert client.descriptor_calls == 1


def test_descriptor_only_target_report_does_not_claim_execution_conformance() -> None:
    operation, target, request = _request()
    report = conformance_report(
        FixtureTargetClient(target, request, _success_status(operation, request))
    )

    assert report.status == "inspected"
    assert report.coverage.model_dump() == {"advertised": 1, "exercised": 0, "complete": False}
    assert report.operations[0].semantic_conformance == "not-exercised"


def test_target_conformance_requires_every_advertised_operation() -> None:
    first = _operation()
    second_payload = first.model_dump(mode="python", exclude={"contract_sha256"})
    second_payload["id"] = "fixture.second/v1"
    second = OperationContract.seal(OperationContractPayload.model_validate(second_payload))
    options = JsonSchemaValidationProfile.from_schema(
        "fixture.multi-target-options/v1",
        {"type": "object", "additionalProperties": False},
    )
    target = TargetDescriptor.seal(
        TargetDescriptorPayload(
            implementation_id="fixture.multi-target/v1",
            implementation_version="1.0.0",
            source_revision="fixture",
            image_id="sha256:" + _sha("9"),
            operations=tuple(
                TargetOperationSupport(
                    operation_id=operation.id,
                    operation_contract_sha256=operation.contract_sha256,
                    options_schema=options,
                )
                for operation in (first, second)
            ),
        )
    )
    requests = {operation.id: _request_for(operation, target) for operation in (first, second)}
    statuses = {
        operation.id: _success_status(operation, requests[operation.id])
        for operation in (first, second)
    }

    class MultiOperationClient:
        def descriptor(self) -> TargetDescriptor:
            return target

        def preflight(self, request: TargetPreflightRequest) -> TargetPreflightResponse:
            return TargetPreflightResponse(
                descriptor=target,
                plan=requests[request.operation_id].declaration.plan,
            )

        def put_job(
            self,
            request: TargetJobRequest,
            *,
            operation: OperationContract,
        ) -> TargetJobStatus:
            assert request == requests[operation.id]
            return statuses[operation.id]

        def status(
            self,
            request: TargetJobRequest,
            *,
            operation: OperationContract,
        ) -> TargetJobStatus:
            return self.put_job(request, operation=operation)

    client = MultiOperationClient()
    partial = conformance_report(
        client,
        cases=(TargetConformanceCase(operation=first, job_request=requests[first.id]),),
    )
    assert partial.status == "partially-exercised"
    assert partial.coverage.model_dump() == {"advertised": 2, "exercised": 1, "complete": False}

    complete = conformance_report(
        client,
        cases=tuple(
            TargetConformanceCase(operation=operation, job_request=requests[operation.id])
            for operation in (first, second)
        ),
    )
    assert complete.status == "conformant"
    assert complete.coverage.model_dump() == {"advertised": 2, "exercised": 2, "complete": True}


def test_target_conformance_executes_the_exact_advertised_semantic_vectors() -> None:
    vectors = SemanticIntentConformanceVectors(
        profile_id="fixture.copy-intent-semantics/v1",
        vectors=(
            SemanticIntentConformanceVector(
                id="accepted",
                accepted=True,
                intent={"suffix": ".accepted"},
            ),
            SemanticIntentConformanceVector(
                id="rejected",
                accepted=False,
                intent={"suffix": ".rejected"},
            ),
        ),
    )
    base = _operation()
    semantics = SemanticValidationProfile.seal(
        SemanticValidationProfilePayload(
            id=vectors.profile_id,
            rules=("fixture.copy-intent.suffix-policy/v1",),
            conformance_vectors_sha256=vectors.sha256,
        )
    )
    payload = base.model_dump(mode="python", exclude={"contract_sha256"})
    payload["intent_semantics"] = semantics
    operation = OperationContract.seal(OperationContractPayload.model_validate(payload))
    target = _target(operation)
    request = _request_for(operation, target)
    status = _success_status(operation, request)

    class SemanticClient(FixtureTargetClient):
        def __init__(self) -> None:
            super().__init__(target, request, status)
            self.preflight_intents: list[dict[str, object]] = []
            self.job_submissions = 0

        def preflight(
            self,
            received: TargetPreflightRequest,
        ) -> TargetPreflightResponse:
            self.preflight_intents.append(dict(received.intent))
            if received.intent["suffix"] == ".rejected":
                raise TargetProtocolError(
                    "fixture semantic rejection",
                    failure_kind="remote_rejection",
                    code="invalid_target_request",
                    observed_status=400,
                )
            plan = TransformPlan.seal(
                TransformPlanPayload(
                    input_groups=received.input_groups,
                    invocation_sha256=received.invocation_sha256,
                    target_implementation_id=target.implementation_id,
                    target_descriptor_sha256=target.descriptor_sha256,
                    operation_id=received.operation_id,
                    operation_contract_sha256=received.operation_contract_sha256,
                    inputs=received.inputs,
                    intent=received.intent,
                    target_options=received.target_options,
                    observation_result_sha256s=tuple(
                        sorted(item.result.result_sha256 for item in received.observations)
                    ),
                )
            )
            return TargetPreflightResponse(descriptor=target, plan=plan)

        def put_job(
            self,
            received: TargetJobRequest,
            *,
            operation: OperationContract,
        ) -> TargetJobStatus:
            self.job_submissions += 1
            return super().put_job(received, operation=operation)

    client = SemanticClient()
    report = conformance_report(
        client,
        cases=(
            TargetConformanceCase(
                operation=operation,
                job_request=request,
                semantic_vectors=vectors,
            ),
        ),
    )

    assert report.status == "conformant"
    assert report.operation_evidence[0].semantic_conformance.model_dump(mode="json") == {
        "profile_id": semantics.id,
        "profile_sha256": semantics.profile_sha256,
        "conformance_vectors_sha256": vectors.sha256,
        "vectors": vectors.model_dump(mode="json"),
        "accepted_vector_ids": ["accepted"],
        "rejected_vector_ids": ["rejected"],
        "status": "exercised",
    }
    assert client.preflight_intents == [
        {"suffix": ".accepted"},
        {"suffix": ".rejected"},
        {"suffix": ".copy"},
    ]
    assert client.job_submissions == 2


def test_target_conformance_requires_semantic_vectors_before_any_job() -> None:
    vectors = SemanticIntentConformanceVectors(
        profile_id="fixture.copy-intent-semantics/v1",
        vectors=(
            SemanticIntentConformanceVector(
                id="accepted",
                accepted=True,
                intent={"suffix": ".copy"},
            ),
            SemanticIntentConformanceVector(
                id="rejected",
                accepted=False,
                intent={"suffix": ".rejected"},
            ),
        ),
    )
    base = _operation()
    payload = base.model_dump(mode="python", exclude={"contract_sha256"})
    payload["intent_semantics"] = SemanticValidationProfile.seal(
        SemanticValidationProfilePayload(
            id=vectors.profile_id,
            rules=("fixture.copy-intent.suffix-policy/v1",),
            conformance_vectors_sha256=vectors.sha256,
        )
    )
    operation = OperationContract.seal(OperationContractPayload.model_validate(payload))
    target = _target(operation)
    request = _request_for(operation, target)
    client = FixtureTargetClient(target, request, _success_status(operation, request))

    with pytest.raises(ValueError, match="requires its exact semantic conformance vectors"):
        conformance_report(
            client,
            cases=(TargetConformanceCase(operation=operation, job_request=request),),
        )


def test_target_client_rejects_remote_plain_http_by_default() -> None:
    with pytest.raises(ValueError, match="must use HTTPS"):
        TargetClient("http://target.example")
    assert TargetClient("http://localhost:8000").base_url.startswith("http://")


def test_target_client_rejects_noncanonical_job_ids_before_transport() -> None:
    operation, _target, _job_request = _request()
    client = TargetClient("https://target.example")
    with pytest.raises(ValueError, match="accepted-request context"):
        client.status("not-a-job-id", operation=operation)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="accepted-request context"):
        client.cancel("A" * 64, operation=operation)  # type: ignore[arg-type]


def test_target_client_sends_exact_nonsecret_cancellation_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, _target, request = _request()
    expected = _success_status(operation, request)
    real_client = httpx.Client

    def respond(received: httpx.Request) -> httpx.Response:
        assert received.method == "POST"
        assert received.url.path == f"/v1/jobs/{request.declaration.job_id}/cancel"
        assert AcceptedTargetJob.model_validate_json(received.content) == request.accepted()
        assert b"first-secret" not in received.content
        assert b"callback-secret" not in received.content
        return httpx.Response(200, json=expected.model_dump(mode="json"))

    monkeypatch.setattr(
        httpx,
        "Client",
        lambda **_kwargs: real_client(transport=httpx.MockTransport(respond)),
    )

    assert TargetClient("https://target.example").cancel(request, operation=operation) == expected


def test_target_client_rejects_a_well_formed_status_for_different_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation, _target, request = _request()
    mismatched = {
        **_success_status(operation, request).model_dump(mode="json"),
        "request_sha256": _sha("f"),
    }
    real_client = httpx.Client

    def respond(_received: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=mismatched)

    monkeypatch.setattr(
        httpx,
        "Client",
        lambda **_kwargs: real_client(transport=httpx.MockTransport(respond)),
    )

    with pytest.raises(TargetProtocolError, match="inconsistent with the request"):
        TargetClient("https://target.example").put_job(request, operation=operation)


def test_target_http_operations_publish_exact_job_paths_and_bound_cancel() -> None:
    jobs = [operation for operation in TARGET_HTTP_OPERATIONS if "{job_id}" in operation.path]
    assert len(jobs) == 3
    for operation in jobs:
        extra = operation_openapi(operation)["openapi_extra"]
        assert extra["parameters"] == [
            {
                "name": "job_id",
                "in": "path",
                "required": True,
                "schema": {"pattern": "^[0-9a-f]{64}$", "type": "string"},
            }
        ]
    cancel = next(operation for operation in jobs if operation.path.endswith("/cancel"))
    assert cancel.request_type is AcceptedTargetJob
    assert "requestBody" in operation_openapi(cancel)["openapi_extra"]


class BindingTargetService:
    def __init__(
        self,
        target: TargetDescriptor,
        request: TargetJobRequest,
        status: TargetJobStatus,
    ) -> None:
        self.target = target
        self.request = request
        self.status_value = status

    def descriptor(self) -> TargetDescriptor:
        return self.target

    def preflight(self, _request: TargetPreflightRequest) -> TargetPreflightResponse:
        return TargetPreflightResponse(
            descriptor=self.target,
            plan=self.request.declaration.plan,
        )

    def put_job(self, request: TargetJobRequest) -> TargetJobStatus:
        assert request.accepted() == self.request.accepted()
        self.request = request
        return self.status_value

    def get_job(self, job_id: str) -> TargetJobStatus:
        assert job_id == self.request.declaration.job_id
        return self.status_value

    def cancel_job(self, request: AcceptedTargetJob) -> TargetJobStatus:
        job_id = request.declaration.job_id
        assert job_id == self.request.declaration.job_id
        return self.status_value


def test_framework_neutral_target_http_binding() -> None:
    operation, target, request = _request()
    status = _success_status(operation, request)
    binding = TargetHttpBinding(BindingTargetService(target, request, status))

    contract_response = binding.handle("GET", "/v1/target")
    assert contract_response.status == 200
    assert TargetDescriptor.model_validate_json(contract_response.body) == target

    preflight_request = TargetPreflightRequest(
        invocation_sha256=request.declaration.plan.invocation_sha256,
        operation_id=operation.id,
        operation_contract_sha256=operation.contract_sha256,
        inputs=request.declaration.plan.inputs,
        intent=request.declaration.plan.intent,
        target_options=request.declaration.plan.target_options,
    )
    preflight_response = binding.handle(
        "POST",
        "/v1/preflight",
        preflight_request.model_dump_json(exclude_none=True).encode(),
    )
    assert preflight_response.status == 200
    assert TargetPreflightResponse.model_validate_json(preflight_response.body).descriptor == target
    duplicate = (
        b'{"operation_id":"'
        + operation.id.encode()
        + b'",'
        + preflight_request.model_dump_json(exclude_none=True).encode()[1:]
    )
    assert binding.handle("POST", "/v1/preflight", duplicate).status == 400

    job_response = binding.handle(
        "PUT",
        f"/v1/jobs/{request.declaration.job_id}",
        request.model_dump_json(exclude_none=True).encode(),
    )
    assert job_response.status == 200
    assert TargetJobStatus.model_validate_json(job_response.body) == status
    assert (
        binding.handle(
            "POST",
            f"/v1/jobs/{request.declaration.job_id}/cancel",
            b'{"reason":"discarded"}',
        ).status
        == 400
    )
    assert binding.handle("PATCH", "/v1/target").status == 405


def test_target_http_binding_treats_untyped_service_schema_faults_as_server_faults() -> None:
    operation, target, request = _request()
    status = _success_status(operation, request)

    class SchemaRejectingService(BindingTargetService):
        def preflight(self, request: TargetPreflightRequest) -> TargetPreflightResponse:
            Draft202012Validator({"type": "integer"}).validate(request.intent)
            raise AssertionError("schema rejection must stop preflight")

    binding = TargetHttpBinding(SchemaRejectingService(target, request, status))
    preflight_request = TargetPreflightRequest(
        invocation_sha256=request.declaration.plan.invocation_sha256,
        operation_id=operation.id,
        operation_contract_sha256=operation.contract_sha256,
        inputs=request.declaration.plan.inputs,
        intent=request.declaration.plan.intent,
        target_options=request.declaration.plan.target_options,
    )

    response = binding.handle(
        "POST",
        "/v1/preflight",
        preflight_request.model_dump_json(exclude_none=True).encode(),
    )

    assert response.status == 500
    assert b'"code":"target_failed"' in response.body


def test_target_operation_matching_enforces_the_declared_job_identity() -> None:
    valid_path = "/v1/jobs/" + _sha("a")

    assert http_operation_for_request(TARGET_HTTP_OPERATIONS, "GET", valid_path) is not None
    assert (
        http_operation_for_request(
            TARGET_HTTP_OPERATIONS,
            "GET",
            "/v1/jobs/not-a-sha",
        )
        is None
    )

    operation, target, request = _request()
    status = _success_status(operation, request)
    response = TargetHttpBinding(BindingTargetService(target, request, status)).handle(
        "GET",
        "/v1/jobs/not-a-sha",
    )
    assert response.status == 404
    assert b'"code":"not_found"' in response.body


def test_persistent_target_requires_and_executes_advertised_semantic_validation(
    tmp_path: Path,
) -> None:
    base = _operation()
    semantics = SemanticValidationProfile.seal(
        SemanticValidationProfilePayload(
            id="fixture.copy-intent-semantics/v1",
            rules=("fixture.copy-intent.suffix-policy/v1",),
            conformance_vectors_sha256=_sha("e"),
        )
    )
    payload = base.model_dump(mode="python", exclude={"contract_sha256"})
    payload["intent_semantics"] = semantics
    operation = OperationContract.seal(OperationContractPayload.model_validate(payload))
    target = _target(operation)

    with pytest.raises(ValueError, match="semantic validators"):
        PersistentTargetService(
            descriptor=target,
            operations={operation.id: operation},
            state_root=tmp_path / "missing-validator",
            execute=lambda *_args: pytest.fail("target execution must not start"),
        )

    def validate_intent(intent: Mapping[str, object]) -> None:
        if intent.get("suffix") != ".accepted":
            raise ValueError("fixture suffix policy rejected the intent")

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path / "validated",
        execute=lambda *_args: pytest.fail("target execution must not start"),
        intent_semantic_validators={operation.intent_semantics.profile_sha256: validate_intent},
    )
    try:
        with pytest.raises(TargetServiceError, match="suffix policy"):
            service.preflight(
                TargetPreflightRequest(
                    invocation_sha256=_sha("b"),
                    operation_id=operation.id,
                    operation_contract_sha256=operation.contract_sha256,
                    inputs=_input_authority(),
                    intent={"suffix": ".rejected"},
                    target_options={},
                )
            )
    finally:
        service.close()


def test_persistent_target_service_uses_canonical_public_error_codes(tmp_path: Path) -> None:
    operation, target, request = _request()
    status = _success_status(operation, request)
    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path / "target-state",
        execute=lambda *_args: status,
    )
    preflight = TargetPreflightRequest(
        invocation_sha256=request.declaration.plan.invocation_sha256,
        operation_id=operation.id,
        operation_contract_sha256=operation.contract_sha256,
        inputs=request.declaration.plan.inputs,
        intent=request.declaration.plan.intent,
        target_options=request.declaration.plan.target_options,
    )
    try:
        with pytest.raises(TargetServiceError) as protocol_error:
            service.preflight(preflight.model_copy(update={"protocol": EFFECT_TARGET_PROTOCOL}))
        with pytest.raises(TargetServiceError) as contract_error:
            service.preflight(preflight.model_copy(update={"operation_contract_sha256": _sha("0")}))
        with pytest.raises(TargetServiceError) as operation_error:
            service.preflight(preflight.model_copy(update={"operation_id": "unknown/v1"}))
        with pytest.raises(TargetServiceError) as request_error:
            service.preflight(preflight.model_copy(update={"intent": {}}))
        with pytest.raises(TargetServiceError) as absence_error:
            service.get_job(_sha("0"))
    finally:
        service.close()

    assert {
        protocol_error.value.code,
        contract_error.value.code,
        operation_error.value.code,
        request_error.value.code,
        absence_error.value.code,
    } == {
        "target_protocol_mismatch",
        "operation_contract_mismatch",
        "unsupported_operation",
        "job_not_found",
        "invalid_target_request",
    }


def test_target_schema_bundle_is_deterministic_and_self_validating() -> None:
    first = target_schema_bundle()
    second = target_schema_bundle()
    assert first == second
    digest = first.pop("bundle_sha256")
    assert canonical_json_sha256(first) == digest
    assert first["http_binding"]["operations"] == http_operation_inventory(TARGET_HTTP_OPERATIONS)
    departure = first["departure_effect"]
    assert departure["http_binding"]["operations"] == http_operation_inventory(
        DEPARTURE_EFFECT_HTTP_OPERATIONS
    )
    assert first["authorities"] == {
        "structural_models": "schemas",
        "http_operations": "http_binding.operations",
        "departure_structural_models": "departure_effect.schemas",
        "departure_http_operations": "departure_effect.http_binding.operations",
        "semantic_acceptance": "semantic_acceptance",
    }
    assert first["semantic_acceptance"]["identity"] == ["id", "profile_sha256"]
    referenced = {
        value
        for operation in first["http_binding"]["operations"]
        for value in (
            operation["request"]["schema"],
            operation["response"]["schema"],
            operation["error_schema"],
        )
        if value is not None
    }
    assert referenced <= set(first["schemas"])
    assert "ErrorOut" in referenced
    departure_referenced = {
        value
        for operation in departure["http_binding"]["operations"]
        for value in (
            operation["request"]["schema"],
            operation["response"]["schema"],
            operation["error_schema"],
        )
        if value is not None
    }
    assert departure_referenced <= set(departure["schemas"])
    assert {
        "DepartureEffectTargetDescriptorPayload",
        "DepartureEffectIntentPayload",
        "DepartureEffectReceiptPayload",
    } <= set(departure["schemas"])
    assert first["schemas"]["TargetConformanceResult"]["properties"]["format"]["const"] == (
        "stove0-target-conformance-result/v1"
    )
    for schema in first["schemas"].values():
        Draft202012Validator.check_schema(schema)
    for schema in departure["schemas"].values():
        Draft202012Validator.check_schema(schema)


def _write_model(path: Path, value: Any) -> None:
    path.write_bytes(
        canonical_json_bytes(value.model_dump(mode="json", by_alias=True, exclude_none=True))
    )


@pytest.mark.parametrize(
    ("kind", "request_factory"),
    [("transform", _request), ("effect", _effect_request)],
)
def test_v1_persisted_target_fixture_recovers_active_custody_without_repeating_effect(
    kind: str,
    request_factory: Any,
    tmp_path: Path,
) -> None:
    operation, target, request = request_factory()
    fixture_root = REPO_ROOT / "tests/fixtures/state/v1_0001/stove0-target"
    accepted_fixture = fixture_root / f"{kind}.accepted.json"
    status_fixture = fixture_root / f"{kind}.status.json"
    assert AcceptedTargetJob.model_validate_json(accepted_fixture.read_text()) == request.accepted()
    persisted_status = TargetJobStatus.model_validate_json(status_fixture.read_text())
    assert persisted_status.state == "running"
    assert persisted_status.job_id == request.declaration.job_id

    state_root = tmp_path / kind
    state_root.mkdir(mode=0o700)
    shutil.copyfile(
        accepted_fixture,
        state_root / f"{request.declaration.job_id}.accepted.json",
    )
    shutil.copyfile(
        status_fixture,
        state_root / f"{request.declaration.job_id}.status.json",
    )
    executions = 0

    def execute(*_args: object) -> TargetJobStatus:
        nonlocal executions
        executions += 1
        raise AssertionError("fixture recovery must not execute during restart")

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=execute,  # type: ignore[arg-type]
    )
    try:
        recovered = service.get_job(request.declaration.job_id)
        assert recovered.state == "interrupted"
        assert recovered.request_sha256 == request.request_sha256
        if kind == "effect":
            assert service.put_job(request) == recovered
    finally:
        service.close()
    assert executions == 0


def test_terminal_state_retention_configuration_is_connected_and_fail_closed() -> None:
    assert terminal_state_retention_seconds({}) == DEFAULT_TERMINAL_STATE_RETENTION_SECONDS
    assert terminal_state_retention_seconds({TARGET_TERMINAL_STATE_RETENTION_ENV: "3600"}) == 3600
    with pytest.raises(ValueError, match="must be an integer"):
        terminal_state_retention_seconds({TARGET_TERMINAL_STATE_RETENTION_ENV: "one-day"})
    with pytest.raises(ValueError, match="must be positive"):
        terminal_state_retention_seconds({TARGET_TERMINAL_STATE_RETENTION_ENV: "0"})


def test_persistent_target_prunes_only_expired_terminal_request_pairs(
    tmp_path: Path,
) -> None:
    operation, target, request = _request()
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    expired_status = _success_status(operation, request)
    _write_model(
        state_root / f"{request.declaration.job_id}.accepted.json",
        request.accepted(),
    )
    expired_path = state_root / f"{request.declaration.job_id}.status.json"
    _write_model(expired_path, expired_status)

    interrupted_request = _request_at_fence(request, 3)
    _write_model(
        state_root / f"{interrupted_request.declaration.job_id}.accepted.json",
        interrupted_request.accepted(),
    )
    interrupted = TargetJobStatus(
        job_id=interrupted_request.declaration.job_id,
        state="interrupted",
        attempt=1,
        request_sha256=interrupted_request.request_sha256,
        plan_sha256=interrupted_request.declaration.plan.plan_sha256,
        progress=TargetProgress(phase="interrupted", completed=0),
    )
    interrupted_path = state_root / f"{interrupted.job_id}.status.json"
    _write_model(interrupted_path, interrupted)
    fresh_request = _request_at_fence(request, 4)
    _write_model(
        state_root / f"{fresh_request.declaration.job_id}.accepted.json",
        fresh_request.accepted(),
    )
    fresh = TargetJobStatus(
        job_id=fresh_request.declaration.job_id,
        state="canceled",
        attempt=1,
        request_sha256=fresh_request.request_sha256,
        plan_sha256=fresh_request.declaration.plan.plan_sha256,
        progress=TargetProgress(phase="canceled", completed=0),
    )
    fresh_path = state_root / f"{fresh.job_id}.status.json"
    _write_model(fresh_path, fresh)

    observed_now = time.time()
    expired_mtime = observed_now - 101
    for path in (expired_path, interrupted_path):
        path.touch()
        path.chmod(0o600)
        os.utime(path, (expired_mtime, expired_mtime))

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=lambda *_args: expired_status,
        terminal_state_retention_seconds=100,
    )
    try:
        assert service.prune_terminal_state(now=observed_now) == {"jobs": 0, "bytes": 0}
        assert sorted(path.name for path in state_root.iterdir()) == [
            ".owner.lock",
            f"{interrupted.job_id}.accepted.json",
            f"{interrupted.job_id}.status.json",
            f"{fresh.job_id}.accepted.json",
            f"{fresh.job_id}.status.json",
        ]
    finally:
        service.close()


def test_persistent_target_resumes_exact_declaration_without_storing_authority(
    tmp_path: Path,
) -> None:
    operation, target, request = _request()
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    job_id = request.declaration.job_id
    _write_model(state_root / f"{job_id}.accepted.json", request.accepted())
    _write_model(
        state_root / f"{job_id}.status.json",
        TargetJobStatus(
            job_id=job_id,
            state="running",
            attempt=1,
            request_sha256=request.request_sha256,
            plan_sha256=request.declaration.plan.plan_sha256,
            progress=TargetProgress(phase="transforming", completed=0),
        ),
    )

    def execute(
        _request: TargetJobRequest,
        _attempt: int,
        _cancellation: threading.Event,
        _session: TargetExecutionSession,
    ) -> TargetJobStatus:
        raise TargetExecutionInapplicable("unsupported-content", "fixture input")

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=execute,
    )
    try:
        assert service.get_job(job_id).state == "interrupted"
        assert service.put_job(request).attempt == 2
        deadline = time.monotonic() + 5
        while service.get_job(job_id).state != "inapplicable":
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert service.put_job(request) == service.get_job(job_id)
    finally:
        service.close()

    persisted = b"\n".join(path.read_bytes() for path in state_root.iterdir())
    assert b"first-secret" not in persisted
    assert b"riverhog.invalid" not in persisted


def test_target_waiters_have_no_payload_slots_and_cancel_without_execution(tmp_path: Path) -> None:
    operation, target, request = _request()
    started = threading.Event()
    calls: list[str] = []

    def block(
        request: TargetJobRequest,
        _attempt: int,
        canceled: threading.Event,
        _session: TargetExecutionSession,
    ) -> TargetJobStatus:
        calls.append(request.declaration.job_id)
        started.set()
        assert canceled.wait(5)
        raise TargetExecutionCanceled

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path,
        execute=block,
        maximum_workers=1,
    )
    waiting = [_request_at_fence(request, fence) for fence in range(3, 35)]
    try:
        service.put_job(request)
        assert started.wait(5)
        for queued in waiting:
            assert service.put_job(queued).state == "queued"
        assert service._dispatch.payload_count == 1
        assert len(service._sessions) == 1
        for queued in waiting:
            canceled = service.cancel_job(queued.accepted())
            assert canceled.state == "canceled"
            assert canceled.attempt == 1
            assert service.put_job(queued) == canceled
        assert calls == [request.declaration.job_id]
    finally:
        service.close()


@pytest.mark.parametrize("effect", [False, True])
def test_unstarted_jobs_survive_restart_and_need_fresh_authority(
    tmp_path: Path, effect: bool
) -> None:
    operation, target, request = _effect_request() if effect else _request()
    state_root = tmp_path / "state"
    state_root.mkdir()
    # Crash between accepted declaration and initial status, before dispatch.
    _write_model(state_root / f"{request.declaration.job_id}.accepted.json", request.accepted())
    calls: list[str] = []
    finished = threading.Event()

    def execute(
        request: TargetJobRequest,
        _attempt: int,
        _canceled: threading.Event,
        _session: TargetExecutionSession,
    ) -> TargetJobStatus:
        calls.append(request.runtime.capability_token)
        finished.set()
        raise TargetExecutionInapplicable("fixture.unsupported/v1", "fixture")

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=execute,
    )
    try:
        assert service.get_job(request.declaration.job_id).state == "queued"
        assert service.get_job(request.declaration.job_id).attempt == 1
        assert not calls
        refreshed = request.model_copy(
            update={
                "runtime": request.runtime.model_copy(
                    update={"capability_token": "refreshed-secret"}
                )
            }
        )
        assert service.put_job(refreshed).attempt == 1
        assert finished.wait(5)
        assert calls == ["refreshed-secret"]
    finally:
        service.close()
    assert b"refreshed-secret" not in b"".join(p.read_bytes() for p in state_root.iterdir())


def test_unknown_cancellation_survives_restart_and_fences_late_put(tmp_path: Path) -> None:
    operation, target, request = _request()
    calls: list[str] = []

    def execute(*_args: object) -> TargetJobStatus:
        calls.append("executed")
        raise AssertionError("a canceled invocation must not start")

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path,
        execute=execute,
    )
    binding = TargetHttpBinding(service)
    try:
        response = binding.handle(
            "POST",
            f"/v1/jobs/{request.declaration.job_id}/cancel",
            request.accepted().model_dump_json().encode(),
        )
        assert response.status == 200
        terminal = TargetJobStatus.model_validate_json(response.body)
        assert terminal.state == "canceled"
        assert service.put_job(request) == terminal
    finally:
        service.close()
    restarted = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path,
        execute=execute,
    )
    try:
        assert restarted.put_job(request) == terminal
        assert restarted.cancel_job(request.accepted()) == terminal
        assert calls == []
    finally:
        restarted.close()


def test_cancellation_tombstone_recovers_before_accepted_status_writes(tmp_path: Path) -> None:
    operation, target, request = _request()
    _write_model(tmp_path / f"{request.declaration.job_id}.cancel.json", request.accepted())
    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path,
        execute=lambda *_args: _success_status(operation, request),
    )
    try:
        assert service.put_job(request).state == "canceled"
        assert service.get_job(request.declaration.job_id).attempt == 1
        assert not service._dispatch.payload_count
    finally:
        service.close()


def test_queue_pressure_refuses_before_acceptance_and_preserves_refresh(tmp_path: Path) -> None:
    operation, target, request = _request()
    started = threading.Event()

    def block(
        _request: TargetJobRequest,
        _attempt: int,
        canceled: threading.Event,
        _session: TargetExecutionSession,
    ) -> TargetJobStatus:
        started.set()
        assert canceled.wait(5)
        raise TargetExecutionCanceled

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path,
        execute=block,
        maximum_pending_jobs=2,
    )
    queued = _request_at_fence(request, 3)
    refused = _request_at_fence(request, 4)
    try:
        service.put_job(request)
        assert started.wait(5)
        assert service.put_job(queued).state == "queued"
        assert service.put_job(queued).attempt == 1
        response = TargetHttpBinding(service).handle(
            "PUT", f"/v1/jobs/{refused.declaration.job_id}", refused.model_dump_json().encode()
        )
        assert response.status == 503
        assert json.loads(response.body)["error"]["code"] == "admission_unavailable"
        assert not (tmp_path / f"{refused.declaration.job_id}.accepted.json").exists()
        assert service.cancel_job(queued.accepted()).state == "canceled"
        assert service.put_job(refused).state == "queued"
    finally:
        service.close()


class _AdmissionPermit:
    def __init__(self, owner: ExecutionOwner) -> None:
        self.owner = owner
        self.activations = 0
        self.releases = 0
        self.released = threading.Event()

    def activate(self, cancellation: threading.Event, *, deadline: float) -> None:
        assert not cancellation.is_set()
        assert time.monotonic() < deadline
        self.activations += 1

    def release(self, *, deadline: float) -> None:
        self.releases += 1
        self.released.set()


def test_deferred_admission_uses_no_payload_and_other_invocations_progress(tmp_path: Path) -> None:
    operation, target, request = _request()
    allowed = _request_at_fence(request, 3)
    probed = threading.Event()
    finished = threading.Event()
    probes: list[ExecutionOwner] = []
    permits: list[_AdmissionPermit] = []
    executions: list[str] = []

    class Admission:
        def probe(self, owner: ExecutionOwner, *, deadline: float) -> ExecutionPermit | None:
            probes.append(owner)
            if owner.invocation_id == request.request_sha256:
                probed.set()
                return None
            permit = _AdmissionPermit(owner)
            permits.append(permit)
            return permit

        def withdraw(self, owner: ExecutionOwner, *, deadline: float) -> None:
            pass

    def execute(
        request: TargetJobRequest,
        _attempt: int,
        _canceled: threading.Event,
        _session: TargetExecutionSession,
    ) -> TargetJobStatus:
        executions.append(request.declaration.job_id)
        finished.set()
        return _success_status(operation, request)

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path,
        execute=execute,
        execution_admission=Admission(),
        admission_retry_seconds=0.01,
    )
    try:
        service.put_job(request)
        assert probed.wait(5)
        assert service.get_job(request.declaration.job_id).state == "queued"
        assert not service._dispatch.payload_count
        assert not service._sessions
        assert service.put_job(request).attempt == 1
        service.put_job(allowed)
        assert finished.wait(5)
        assert permits[0].released.wait(5)
        assert service.get_job(allowed.declaration.job_id).state == "succeeded"
        assert service.get_job(request.declaration.job_id).attempt == 1
        denied_owners = {owner for owner in probes if owner.invocation_id == request.request_sha256}
        assert len(denied_owners) == 1
        assert executions == [allowed.declaration.job_id]
        assert service.cancel_job(request.accepted()).state == "canceled"
    finally:
        service.close()


@pytest.mark.parametrize("foreign", [False, True])
def test_cancel_during_probe_releases_only_its_late_owned_grant(
    tmp_path: Path, foreign: bool
) -> None:
    operation, target, request = _request()
    probing = threading.Event()
    deliver = threading.Event()
    permits: list[_AdmissionPermit] = []
    withdrawn = threading.Event()
    executions: list[str] = []

    class Admission:
        def probe(self, owner: ExecutionOwner, *, deadline: float) -> ExecutionPermit:
            probing.set()
            assert deliver.wait(5)
            returned = (
                ExecutionOwner(owner.invocation_id, owner.attempt, "another-owner", "foreign")
                if foreign
                else owner
            )
            permit = _AdmissionPermit(returned)
            permits.append(permit)
            return permit

        def withdraw(self, owner: ExecutionOwner, *, deadline: float) -> None:
            withdrawn.set()

    def execute(*_args: object) -> TargetJobStatus:
        executions.append("ran")
        raise AssertionError("late grants cannot revive canceled work")

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path,
        execute=execute,
        execution_admission=Admission(),
        admission_retry_seconds=0.01,
    )
    try:
        service.put_job(request)
        assert probing.wait(5)
        # Probe never holds the control lock. The cancel is durable before the
        # external application supplies its response.
        assert service.cancel_job(request.accepted()).state == "canceled"
        assert not deliver.is_set()
        deliver.set()
        assert withdrawn.wait(5)
        assert permits[0].activations == 0
        assert permits[0].releases == (0 if foreign else 1)
        assert executions == []
        assert service.put_job(request).state == "canceled"
    finally:
        deliver.set()
        service.close()


def test_active_target_state_has_one_exclusive_process_owner(tmp_path: Path) -> None:
    operation, target, request = _request()
    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path,
        execute=lambda *_args: _success_status(operation, request),
    )
    try:
        with pytest.raises(RuntimeError, match="active owner"):
            PersistentTargetService(
                descriptor=target,
                operations={operation.id: operation},
                state_root=tmp_path,
                execute=lambda *_args: _success_status(operation, request),
            )
    finally:
        service.close()
    replacement = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path,
        execute=lambda *_args: _success_status(operation, request),
    )
    replacement.close()


def test_persistent_target_shutdown_and_operator_cancel_have_distinct_state(
    tmp_path: Path,
) -> None:
    operation, target, request = _request()
    started = threading.Event()

    def block(
        _request: TargetJobRequest,
        _attempt: int,
        cancellation: threading.Event,
        _session: TargetExecutionSession,
    ) -> TargetJobStatus:
        started.set()
        assert cancellation.wait(timeout=5)
        raise TargetExecutionCanceled

    interrupted = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path / "interrupted",
        execute=block,
    )
    interrupted.put_job(request)
    assert started.wait(timeout=5)
    interrupted.close()
    assert interrupted.get_job(request.declaration.job_id).state == "interrupted"

    started.clear()
    canceled = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path / "canceled",
        execute=block,
    )
    try:
        canceled.put_job(request)
        assert started.wait(timeout=5)
        assert canceled.cancel_job(request.accepted()).state == "canceling"
        deadline = time.monotonic() + 5
        while canceled.get_job(request.declaration.job_id).state != "canceled":
            assert time.monotonic() < deadline
            time.sleep(0.01)
    finally:
        canceled.close()


def test_running_target_receives_capability_refresh_without_persisting_secrets(
    tmp_path: Path,
) -> None:
    operation, target, request = _request()
    state_root = tmp_path / "state"
    bound = threading.Event()
    refreshed = threading.Event()
    finish = threading.Event()
    tokens: list[str] = []

    class Runtime:
        def refresh_capability(self, token: str) -> None:
            tokens.append(token)
            if token == "replacement-secret":
                refreshed.set()

        def close(self) -> None:
            pass

    def execute(
        _request: TargetJobRequest,
        _attempt: int,
        _cancellation: threading.Event,
        session: TargetExecutionSession,
    ) -> TargetJobStatus:
        with session.runtime_registry.bind(request.declaration.job_id, Runtime()):  # type: ignore[arg-type]
            bound.set()
            assert finish.wait(timeout=5)
        raise TargetExecutionInapplicable("fixture-inapplicable", "fixture input")

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=execute,
    )
    try:
        service.put_job(request)
        assert bound.wait(timeout=5)
        replacement = TargetJobRequest.seal(
            request.declaration,
            request.runtime.model_copy(update={"capability_token": "replacement-secret"}),
            request.callback_access,
        )
        assert service.put_job(replacement).state == "running"
        assert refreshed.wait(timeout=5)
        assert service.put_job(replacement).state == "running"
        finish.set()
        deadline = time.monotonic() + 5
        while service.get_job(request.declaration.job_id).state != "inapplicable":
            assert time.monotonic() < deadline
            time.sleep(0.01)
    finally:
        finish.set()
        service.close()

    assert tokens == ["first-secret", "replacement-secret"]
    persisted = b"\n".join(path.read_bytes() for path in state_root.iterdir())
    assert b"first-secret" not in persisted
    assert b"replacement-secret" not in persisted
    assert b"riverhog.invalid" not in persisted


def test_queued_target_constructs_runtime_with_refreshed_authority_before_claim_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation, target, request = _request()
    waiting = threading.Event()
    start = threading.Event()
    claims: list[str] = []
    callback_tokens: list[str] = []
    workflow = request.declaration.controller_evidence.execution_envelope.workflow_plan

    def callback(http_request: httpx.Request) -> httpx.Response:
        token = http_request.headers["Authorization"]
        callback_tokens.append(token)
        assert token == "Bearer refreshed-queued-callback-secret"
        return httpx.Response(
            200,
            json=TargetInputPage(
                authority=request.declaration.plan.inputs, complete=True, artifacts=(_input(),)
            ).model_dump(mode="json"),
        )

    original_client_init = TargetCallbackClient.__init__

    def callback_client_init(self, access, **kwargs):
        original_client_init(self, access, **kwargs)
        self._client.close()
        self._client = httpx.Client(transport=httpx.MockTransport(callback))

    monkeypatch.setattr(TargetCallbackClient, "__init__", callback_client_init)

    def claim(api: ApiClient, claim_id: str):
        assert claim_id == request.declaration.claim_id
        claims.append(api.token)
        if api.token != "refreshed-queued-secret":
            raise Unauthorized("original queued bearer expired")
        return SimpleNamespace(
            plan=SimpleNamespace(
                execution_id=request.declaration.job_id,
                output_policy=workflow.output_policy,
                inputs=SimpleNamespace(sha256=_sha("a")),
                artifacts=SimpleNamespace(sha256=_sha("b")),
            )
        )

    monkeypatch.setattr(ApiClient, "get_processing_claim", claim)

    def execute(original, _attempt, _cancellation, session):
        waiting.set()
        assert start.wait(timeout=5)
        with TargetExecutionRuntime.from_request(original, session=session) as execution:
            assert execution.runtime.api.current.token == "refreshed-queued-secret"
            assert tuple(item for item, _claimed in execution.iter_inputs()) == (_input(),)
        raise TargetExecutionInapplicable("fixture-inapplicable", "fixture input")

    state = tmp_path / "state"
    service = PersistentTargetService(
        descriptor=target, operations={operation.id: operation}, state_root=state, execute=execute
    )
    try:
        service.put_job(request)
        assert waiting.wait(timeout=5)
        replacement = TargetJobRequest.seal(
            request.declaration,
            request.runtime.model_copy(update={"capability_token": "refreshed-queued-secret"}),
            request.callback_access.model_copy(
                update={"token": "refreshed-queued-callback-secret"}
            ),
        )
        assert replacement.request_sha256 == request.request_sha256
        service.put_job(replacement)
        start.set()
        deadline = time.monotonic() + 5
        while service.get_job(request.declaration.job_id).state not in {"failed", "inapplicable"}:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        status = service.get_job(request.declaration.job_id)
        assert (status.state, status.attempt) == ("inapplicable", 1)
        assert claims == ["refreshed-queued-secret"]
        assert callback_tokens == ["Bearer refreshed-queued-callback-secret"]
    finally:
        start.set()
        service.close()
    persisted = b"\n".join(path.read_bytes() for path in state.iterdir())
    assert b"first-secret" not in persisted
    assert b"refreshed-queued-secret" not in persisted
    assert b"callback-secret" not in persisted
    assert b"refreshed-queued-callback-secret" not in persisted


def test_running_target_refreshes_callback_access_and_rejects_endpoint_changes(
    tmp_path: Path,
) -> None:
    operation, target, request = _request()
    started = threading.Event()
    proceed = threading.Event()
    tokens: list[str] = []

    def callback(http_request: httpx.Request) -> httpx.Response:
        tokens.append(http_request.headers["Authorization"])
        return httpx.Response(
            200,
            json=TargetInputPage(
                authority=request.declaration.plan.inputs, complete=True, artifacts=(_input(),)
            ).model_dump(mode="json"),
        )

    def execute(original, _attempt, _cancellation, session):
        client = session.callback_client()
        client._client.close()
        client._client = httpx.Client(transport=httpx.MockTransport(callback))
        try:
            assert tuple(client.iter_inputs(original.declaration.job_id)) == (_input(),)
            started.set()
            assert proceed.wait(timeout=5)
            assert tuple(client.iter_inputs(original.declaration.job_id)) == (_input(),)
        finally:
            client.close()
        raise TargetExecutionInapplicable("fixture-inapplicable", "fixture input")

    state_root = tmp_path / "state"
    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=execute,
    )
    try:
        service.put_job(request)
        assert started.wait(timeout=5)
        replacement = TargetJobRequest.seal(
            request.declaration,
            request.runtime,
            request.callback_access.model_copy(update={"token": "renewed-callback-secret"}),
        )
        assert replacement.request_sha256 == request.request_sha256
        assert service.put_job(replacement).state == "running"
        for changed in (
            {"stove0_base_url": "https://different-stove0.invalid"},
            {"allow_insecure_http": True},
        ):
            redirected = TargetJobRequest.seal(
                replacement.declaration,
                replacement.runtime,
                replacement.callback_access.model_copy(update={**changed, "token": "wrong-secret"}),
            )
            with pytest.raises(TargetServiceError, match="callback endpoint changed") as failure:
                service.put_job(redirected)
            assert failure.value.status == 409
        proceed.set()
        deadline = time.monotonic() + 5
        while service.get_job(request.declaration.job_id).state not in {"failed", "inapplicable"}:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert service.get_job(request.declaration.job_id).state == "inapplicable"
    finally:
        proceed.set()
        service.close()
    assert tokens == ["Bearer callback-secret", "Bearer renewed-callback-secret"]
    persisted = b"\n".join(path.read_bytes() for path in state_root.iterdir())
    for secret in (b"callback-secret", b"renewed-callback-secret", b"wrong-secret"):
        assert secret not in persisted


def test_restart_before_publication_preserves_semantic_execution_identity(
    tmp_path: Path,
) -> None:
    operation, target, request = _request()
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    job_id = request.declaration.job_id
    _write_model(state_root / f"{job_id}.accepted.json", request.accepted())
    _write_model(
        state_root / f"{job_id}.status.json",
        TargetJobStatus(
            job_id=job_id,
            state="running",
            attempt=1,
            request_sha256=request.request_sha256,
            plan_sha256=request.declaration.plan.plan_sha256,
            progress=TargetProgress(phase="publishing", completed=0),
        ),
    )
    first_attempt = _success_status(operation, request)

    def execute(
        _request: TargetJobRequest,
        attempt: int,
        _cancellation: threading.Event,
        session: TargetExecutionSession,
    ) -> TargetJobStatus:
        restarted = first_attempt.model_copy(update={"attempt": attempt})
        session.record_completed(restarted)
        return restarted

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=execute,
    )
    try:
        assert service.get_job(job_id).state == "interrupted"
        assert service.put_job(request).attempt == 2
        deadline = time.monotonic() + 5
        restarted = service.get_job(job_id)
        while restarted.state != "succeeded":
            assert time.monotonic() < deadline
            time.sleep(0.01)
            restarted = service.get_job(job_id)
    finally:
        service.close()

    assert restarted.attempt == 2
    assert restarted.request_sha256 == first_attempt.request_sha256
    assert restarted.execution_evidence == first_attempt.execution_evidence
    assert restarted.output_collection == first_attempt.output_collection
    assert restarted.derivation == first_attempt.derivation


def test_persisted_publication_survives_lost_response_and_process_restart(
    tmp_path: Path,
) -> None:
    operation, target, request = _request()
    state_root = tmp_path / "state"
    finished = threading.Event()
    success = _success_status(operation, request)

    def publish(
        _request: TargetJobRequest,
        _attempt: int,
        _cancellation: threading.Event,
        session: TargetExecutionSession,
    ) -> TargetJobStatus:
        session.record_completed(success)
        finished.set()
        return success

    first = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=publish,
    )
    first.put_job(request)
    assert finished.wait(timeout=5)
    deadline = time.monotonic() + 5
    while first.get_job(request.declaration.job_id).state != "succeeded":
        assert time.monotonic() < deadline
        time.sleep(0.01)
    first.close()

    called = False

    def unexpected_execution(
        _request: TargetJobRequest,
        _attempt: int,
        _cancellation: threading.Event,
        _session: TargetExecutionSession,
    ) -> TargetJobStatus:
        nonlocal called
        called = True
        raise AssertionError("published target output must not execute again")

    restarted = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=unexpected_execution,
    )
    try:
        replacement = TargetJobRequest.seal(
            request.declaration,
            request.runtime.model_copy(update={"capability_token": "replacement-secret"}),
            request.callback_access,
        )
        assert restarted.put_job(replacement) == success
        assert not called
    finally:
        restarted.close()

    persisted = b"\n".join(path.read_bytes() for path in state_root.iterdir())
    assert b"first-secret" not in persisted
    assert b"replacement-secret" not in persisted


def test_persisted_effect_receipt_replays_without_repeating_external_effect(
    tmp_path: Path,
) -> None:
    operation, target, request = _effect_request()
    state_root = tmp_path / "effect-state"
    committed = threading.Event()
    calls = 0

    def execute(
        _request: TargetJobRequest,
        attempt: int,
        _cancellation: threading.Event,
        session: TargetExecutionSession,
    ) -> TargetJobStatus:
        nonlocal calls
        calls += 1
        status = _effect_success_status(operation, request, attempt=attempt)
        session.record_completed(status)
        committed.set()
        return status

    first = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=execute,
    )
    first.put_job(request)
    assert committed.wait(timeout=5)
    deadline = time.monotonic() + 5
    while first.get_job(request.declaration.job_id).state != "succeeded":
        assert time.monotonic() < deadline
        time.sleep(0.01)
    expected = first.get_job(request.declaration.job_id)
    first.close()
    expired = time.time() - 100
    for path in state_root.iterdir():
        os.utime(path, (expired, expired))

    def repeat_forbidden(*_args: object) -> TargetJobStatus:
        raise AssertionError("a persisted external effect must not execute again")

    restarted = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=state_root,
        execute=repeat_forbidden,  # type: ignore[arg-type]
        terminal_state_retention_seconds=1,
    )
    try:
        assert sorted(path.suffix for path in state_root.iterdir()) == [".json", ".json", ".lock"]
        assert restarted.put_job(request) == expected
    finally:
        restarted.close()
    assert calls == 1


def test_uncertain_effect_commit_stays_interrupted_and_never_auto_repeats(
    tmp_path: Path,
) -> None:
    operation, target, request = _effect_request()
    attempted = threading.Event()
    calls = 0

    def uncertain(
        _request: TargetJobRequest,
        _attempt: int,
        _cancellation: threading.Event,
        _session: TargetExecutionSession,
    ) -> TargetJobStatus:
        nonlocal calls
        calls += 1
        attempted.set()
        raise TargetEffectCommitUncertain("fixture external commit is uncertain")

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path / "uncertain-effect-state",
        execute=uncertain,
    )
    try:
        service.put_job(request)
        assert attempted.wait(timeout=5)
        deadline = time.monotonic() + 5
        while service.get_job(request.declaration.job_id).state != "interrupted":
            assert time.monotonic() < deadline
            time.sleep(0.01)
        interrupted = service.get_job(request.declaration.job_id)
        assert interrupted.progress.phase == "external-commit-uncertain"
        assert service.put_job(request) == interrupted
        assert service.cancel_job(request.accepted()) == interrupted
        assert calls == 1
    finally:
        service.close()


def test_terminal_target_jobs_release_process_local_bookkeeping(tmp_path: Path) -> None:
    operation, target, request = _request()
    finished = threading.Event()

    def execute(
        _request: TargetJobRequest,
        _attempt: int,
        _cancellation: threading.Event,
        _session: TargetExecutionSession,
    ) -> TargetJobStatus:
        finished.set()
        raise TargetExecutionInapplicable("fixture-content", "fixture input")

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path / "state",
        execute=execute,
    )
    try:
        service.put_job(request)
        assert finished.wait(timeout=5)
        deadline = time.monotonic() + 5
        while service._dispatch.active_keys:  # noqa: SLF001 - white-box bounded-state proof
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert service._cancel == {}  # noqa: SLF001 - white-box bounded-state proof
        assert service._operator_canceled == set()  # noqa: SLF001
        assert service._shutdown_interrupted == set()  # noqa: SLF001
    finally:
        service.close()


def test_published_success_wins_late_cancel_and_cleanup_failure(tmp_path: Path) -> None:
    operation, target, request = _request()
    published = threading.Event()
    release = threading.Event()
    success = _success_status(operation, request)

    def execute(
        _request: TargetJobRequest,
        _attempt: int,
        _cancellation: threading.Event,
        session: TargetExecutionSession,
    ) -> TargetJobStatus:
        session.record_completed(success)
        published.set()
        assert release.wait(timeout=5)
        raise RuntimeError("post-publication cleanup failed")

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path / "state",
        execute=execute,
    )
    try:
        service.put_job(request)
        assert published.wait(timeout=5)
        assert service.cancel_job(request.accepted()) == success
        release.set()
        deadline = time.monotonic() + 5
        while service.get_job(request.declaration.job_id).state != "succeeded":
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert service.cancel_job(request.accepted()) == success
    finally:
        release.set()
        service.close()


@pytest.mark.parametrize(
    ("failure", "state", "code", "retryable"),
    [
        (
            TargetExecutionInapplicable("fixture-content", "unsupported fixture"),
            "inapplicable",
            "fixture-content",
            None,
        ),
        (
            TargetExecutionFailure("fixture-tool", "tool unavailable", retryable=True),
            "failed",
            "fixture-tool",
            True,
        ),
        (
            Unauthorized("expired capability", observed_status=401),
            "failed",
            "target-authorization",
            True,
        ),
        (
            Conflict("stale fence", observed_status=409),
            "failed",
            "target-conflict",
            True,
        ),
        (
            DownloadAllowanceExceeded("quota resets later", observed_status=429),
            "failed",
            "target-download-allowance",
            True,
        ),
        (OSError("storage unavailable"), "failed", "target-infrastructure", True),
        (ValueError("unexpected implementation defect"), "failed", "target-software", True),
    ],
)
def test_target_failure_classes_remain_distinct_from_content_inapplicability(
    tmp_path: Path,
    failure: Exception,
    state: str,
    code: str,
    retryable: bool | None,
) -> None:
    operation, target, request = _request()

    def execute(
        _request: TargetJobRequest,
        _attempt: int,
        _cancellation: threading.Event,
        _session: TargetExecutionSession,
    ) -> TargetJobStatus:
        raise failure

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path / code,
        execute=execute,
    )
    try:
        service.put_job(request)
        deadline = time.monotonic() + 5
        status = service.get_job(request.declaration.job_id)
        while status.state not in {"inapplicable", "failed"}:
            assert time.monotonic() < deadline
            time.sleep(0.01)
            status = service.get_job(request.declaration.job_id)
    finally:
        service.close()

    assert status.state == state
    if state == "inapplicable":
        assert status.inapplicable is not None
        assert status.inapplicable.code == code
    else:
        assert status.failure is not None
        assert (status.failure.code, status.failure.retryable) == (code, retryable)


def _retain_publication_checkpoint(state_root: Path):
    from riverhog_client.canonical_completion import CompletionRecord
    from stove0_target_support.completion_checkpoint import TargetCompletionCheckpoint

    operation, target, request = _request()
    expected = _success_status(operation, request)
    assert expected.production is not None
    assert expected.execution_evidence is not None
    pre_root = TargetPreRootResult(
        job_id=request.declaration.job_id,
        attempt=1,
        request_sha256=request.request_sha256,
        plan_sha256=request.declaration.plan.plan_sha256,
        production=expected.production,
        execution_evidence=expected.execution_evidence,
    )
    checkpoint = TargetCompletionCheckpoint.retain(
        state_root,
        request=request,
        implementation=target,
        operation=operation,
        pre_root=pre_root,
        execution=CompletionRecord.from_bytes(
            "target-execution",
            _EXECUTION_PREIMAGE,
        ),
        source_context={},
    )
    return operation, target, request, expected, checkpoint


def test_publication_checkpoint_keeps_exact_evidence_and_original_attempt(tmp_path: Path) -> None:
    from riverhog_client.canonical_completion import CompletionRecord
    from stove0_target_support.completion_checkpoint import TargetCompletionCheckpoint

    operation, target, request, _expected, original = _retain_publication_checkpoint(tmp_path)
    retry = TargetCompletionCheckpoint.retain(
        tmp_path,
        request=request,
        implementation=target,
        operation=operation,
        pre_root=original.pre_root.model_copy(update={"attempt": 2}),
        execution=CompletionRecord.from_bytes("target-execution", _EXECUTION_PREIMAGE),
        source_context={},
    )
    assert retry.pre_root.attempt == 1
    assert retry.pre_root == original.pre_root
    assert b"".join(retry.execution.read()) == _EXECUTION_PREIMAGE
    persisted = b"\n".join(path.read_bytes() for path in tmp_path.iterdir())
    assert b"first-secret" not in persisted
    assert b"callback-secret" not in persisted
    assert b"riverhog.invalid" not in persisted
    with pytest.raises(ValueError, match="identity differs|evidence changed"):
        TargetCompletionCheckpoint.retain(
            tmp_path,
            request=request,
            implementation=target,
            operation=operation,
            pre_root=original.pre_root,
            execution=CompletionRecord.from_bytes("target-execution", b"changed"),
            source_context={},
        )


def test_publication_checkpoint_rejects_corrupt_exact_preimage(tmp_path: Path) -> None:
    from stove0_target_support.completion_checkpoint import TargetCompletionCheckpoint

    _operation, _target, request, _expected, checkpoint = _retain_publication_checkpoint(tmp_path)
    content_path = tmp_path / (
        f"{request.declaration.job_id}.execution-{checkpoint.execution.sha256}.bin"
    )
    content_path.write_bytes(b"x" * checkpoint.execution.bytes)
    loaded = TargetCompletionCheckpoint.load(tmp_path, request=request)
    assert loaded is not None
    with pytest.raises(ValueError, match="preimage changed"):
        b"".join(loaded.execution.read())


@pytest.mark.parametrize("retry_outcome", ["resume", "cancel", "shutdown"])
def test_persistent_target_resumes_sealed_publication_without_rerunning_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    retry_outcome: str,
) -> None:
    from riverhog_protocol import ServiceUnavailable

    operation, target, request, expected, checkpoint = _retain_publication_checkpoint(tmp_path)
    job_id = request.declaration.job_id
    _write_model(tmp_path / f"{job_id}.accepted.json", request.accepted())
    _write_model(
        tmp_path / f"{job_id}.status.json",
        TargetJobStatus(
            job_id=job_id,
            state="running",
            attempt=1,
            request_sha256=request.request_sha256,
            plan_sha256=request.declaration.plan.plan_sha256,
            progress=TargetProgress(phase="publishing", completed=1),
        ),
    )
    calls = []

    def resume(
        _cls: object, fresh: TargetJobRequest, *, session: TargetExecutionSession, **_kwargs
    ):
        assert fresh.request_sha256 == request.request_sha256
        if calls:
            assert fresh.runtime.capability_token == "newest-retry-secret"

        class Execution:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                pass

            def open_collection_publication(self, *, implementation, source_context):
                assert implementation == target
                assert source_context == checkpoint.source_context
                return self

            def finish_success(self, *, execution_preimage, attempt, runtime_evidence, **_kwargs):
                assert b"".join(execution_preimage.read()) == _EXECUTION_PREIMAGE
                assert runtime_evidence == checkpoint.pre_root.execution_evidence.runtime
                calls.append(attempt)
                if len(calls) == 1:
                    raise ServiceUnavailable("publication transport lost private-token-value")
                result = expected.model_copy(update={"attempt": attempt})
                session.record_completed(result)
                return result

        return Execution()

    monkeypatch.setattr(TargetExecutionRuntime, "from_request", classmethod(resume))

    def execute(*_args):
        raise AssertionError("sealed publication must not invoke the target operation")

    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path,
        execute=execute,
    )
    finished_attempt = threading.Event()
    release_future = threading.Event()
    run = service._run

    def held_run(fresh, attempt, cancellation, session, permit):
        status = run(fresh, attempt, cancellation, session, permit)
        if attempt == 2:
            finished_attempt.set()
            assert release_future.wait(timeout=5)
        return status

    monkeypatch.setattr(service, "_run", held_run)
    shutdown_started = threading.Event()
    shutdown = service._dispatch.pool.shutdown

    def signaled_shutdown(**kwargs):
        shutdown_started.set()
        shutdown(**kwargs)

    monkeypatch.setattr(service._dispatch.pool, "shutdown", signaled_shutdown)
    closing = None
    try:
        assert service.get_job(job_id).state == "interrupted"
        service.put_job(request)
        assert finished_attempt.wait(timeout=5)
        deadline = time.monotonic() + 5
        while service.get_job(job_id).state != "interrupted":
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert service.get_job(job_id).progress.phase == "publication-interrupted"
        assert "Target publication interrupted" in caplog.text
        assert f"job={job_id} attempt=2" in caplog.text
        assert "cause=ServiceUnavailable" in caplog.text
        assert "finish_success" in caplog.text
        assert "private-token-value" not in caplog.text
        assert service.put_job(request).state == "queued"
        refreshed = TargetJobRequest.seal(
            request.declaration,
            request.runtime.model_copy(update={"capability_token": "newest-retry-secret"}),
            request.callback_access,
        )
        assert service.put_job(refreshed).state == "queued"
        if retry_outcome == "cancel":
            assert service.cancel_job(request.accepted()).state == "canceling"
        elif retry_outcome == "shutdown":
            closing = threading.Thread(target=service.close)
            closing.start()
            assert shutdown_started.wait(timeout=5)
        release_future.set()
        expected_state = {"cancel": "canceled", "shutdown": "queued", "resume": "succeeded"}[
            retry_outcome
        ]
        while service.get_job(job_id).state != expected_state:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert service.get_job(job_id).attempt == 3
        if retry_outcome != "resume":
            assert service.get_job(job_id).output_collection is None
            assert calls == [2]
        else:
            assert service.get_job(job_id).output_collection == expected.output_collection
            assert calls == [2, 3]
        assert service.prune_terminal_state(now=time.time()) == {"jobs": 0, "bytes": 0}
    finally:
        release_future.set()
        if closing is not None:
            closing.join(timeout=5)
            assert not closing.is_alive()
        service.close()
    assert checkpoint.pre_root.attempt == 1


def test_checkpoint_retention_keeps_failed_unpublished_evidence(tmp_path: Path) -> None:
    operation, target, request, expected, _checkpoint = _retain_publication_checkpoint(tmp_path)
    job_id = request.declaration.job_id
    _write_model(tmp_path / f"{job_id}.accepted.json", request.accepted())
    failed = TargetJobStatus(
        job_id=job_id,
        state="failed",
        attempt=1,
        request_sha256=request.request_sha256,
        plan_sha256=request.declaration.plan.plan_sha256,
        progress=TargetProgress(phase="publishing", completed=1),
        failure={"code": "fixture-publish", "message": "publishing failed", "retryable": False},
    )
    _write_model(tmp_path / f"{job_id}.status.json", failed)
    service = PersistentTargetService(
        descriptor=target,
        operations={operation.id: operation},
        state_root=tmp_path,
        execute=lambda *_args: expected,
    )
    try:
        assert service.prune_terminal_state(now=time.time() + 10**9) == {"jobs": 0, "bytes": 0}
        assert (tmp_path / f"{job_id}.completion.json").exists()
        for suffix in ("outputs", "output-members"):
            directory = tmp_path / f"{job_id}.{suffix}"
            directory.mkdir(mode=0o700)
            (directory / "fixture.json").write_bytes(b"retained restart state")
        (tmp_path / f"{job_id}.step-fixture.bin").write_bytes(b"original tool evidence")
        assert service.prune_terminal_state(now=time.time() + 10**9) == {"jobs": 0, "bytes": 0}
        service._write_model(tmp_path / f"{job_id}.status.json", expected)
        sizes = sum(path.stat().st_size for path in tmp_path.rglob("*") if path.is_file())
        assert service.prune_terminal_state(now=time.time() + 10**9) == {"jobs": 1, "bytes": sizes}
        assert list(tmp_path.iterdir()) == [tmp_path / ".owner.lock"]
    finally:
        service.close()


def test_partial_output_restart_keeps_pending_bytes_and_skips_receipted_bytes(
    tmp_path: Path,
) -> None:
    operation, implementation, request = _request()
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    base = tmp_path / "workspace"
    base.mkdir(mode=0o700)
    workspace = ProcessingWorkspace.open(
        base,
        execution_id=request.declaration.job_id,
        declared_protection=request.declaration.declared_workspace_protection,
    )
    local = workspace.resolve("output/disposable.bin")
    local.parent.mkdir(mode=0o700)
    local.write_bytes(b"exact pending output")
    artifact = OutputArtifact(
        id="output",
        role="fixture.output/v1",
        artifact_id=_sha("6"),
        bytes=str(len(local.read_bytes())),
        sha256=hashlib.sha256(local.read_bytes()).hexdigest(),
    )
    appended = []
    declared = []
    custodied = False

    class Producer:
        def set_pending_source_resolver(self, resolver):
            self.resolver = resolver

        def resume_artifact_custody(self, identity):
            assert identity.artifact_id == artifact.artifact_id
            return object() if custodied else None

    class Writer:
        producer = Producer()

    class Runtime:
        def append_incremental_output(self, _writer, source, **_kwargs):
            assert source.source.read_bytes() == b"exact pending output"
            appended.append(source.source)
            return ()

        def __exit__(self, *_args):
            pass

    class Callback:
        def declare_target_execution_output(self, job_id, output):
            assert job_id == request.declaration.job_id
            assert output == artifact
            declared.append(output)

        def declare_target_execution_source_edge(self, _job_id, edge):
            assert edge.output_id == artifact.id and edge.input_id == "source"

        def close(self):
            pass

    def open_execution():
        session = TargetExecutionSession(
            request, 1, ClaimedCollectionRuntimeRegistry(), state_root=state
        )
        execution = TargetExecutionRuntime(request, Runtime(), session=session)
        execution._workspaces.append(workspace)
        execution._input_client = Callback()
        execution.resolve_input_ids = lambda ids: tuple(ids)
        return execution, TargetCollectionPublication(execution, Writer(), implementation)

    execution, publication = open_execution()
    publication.prepare_output(
        ProducerFile(local, artifact.artifact_id, allow_missing_materialization_hint=True),
        artifact,
        derived_from=("source",),
    )
    execution.__exit__(None, None, None)
    assert local.read_bytes() == b"exact pending output"

    restarted, resumed = open_execution()
    assert (
        resumed.resume_output(
            "output",
            derived_from=("source",),
            materialization_hint=None,
            allow_missing_materialization_hint=True,
        )
        == artifact
    )
    assert appended == [local]
    assert resumed.writer.producer.resolver(artifact.artifact_id).source == local
    restarted.__exit__(None, None, None)
    assert local.exists()

    # Simulate a receipt followed by release and process loss. A new attempt
    # must consult Riverhog custody and never touch the released payload.
    custodied = True
    local.unlink()
    restarted, resumed = open_execution()
    assert (
        resumed.resume_output(
            "output",
            derived_from=("source",),
            materialization_hint=None,
            allow_missing_materialization_hint=True,
        )
        == artifact
    )
    assert appended == [local]
    with pytest.raises(ValueError, match="source edges"):
        resumed.resume_output(
            "output",
            derived_from=("another-source",),
            materialization_hint=None,
            allow_missing_materialization_hint=True,
        )
    with pytest.raises(ValueError, match="publication decision"):
        resumed.resume_output(
            "output",
            derived_from=("source",),
            materialization_hint=("unaccepted-name",),
            allow_missing_materialization_hint=True,
        )
    assert len(declared) == 3


def test_output_checkpoint_rejects_replacement_and_changed_workspace(tmp_path: Path) -> None:
    _operation, _implementation, request = _request()
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    base = tmp_path / "workspace"
    base.mkdir(mode=0o700)
    workspace = ProcessingWorkspace.open(
        base,
        execution_id=request.declaration.job_id,
        declared_protection=request.declaration.declared_workspace_protection,
    )
    local = workspace.resolve("pending.bin")
    local.write_bytes(b"original")
    artifact = OutputArtifact(
        id="output",
        role="fixture.output/v1",
        artifact_id=_sha("6"),
        bytes="8",
        sha256=hashlib.sha256(b"original").hexdigest(),
    )
    arguments = dict(
        request=request,
        output=artifact,
        source_edges_sha256=_sha("1"),
        source=ProducerFile(local, artifact.artifact_id, allow_missing_materialization_hint=True),
        workspace=workspace,
    )
    first = TargetOutputCheckpoint.retain(state, **arguments)
    assert TargetOutputCheckpoint.retain(state, **arguments) == first
    assert (
        TargetOutputCheckpoint.load_member(state, request=request, artifact_id=artifact.artifact_id)
        == first
    )
    index = state / f"{request.declaration.job_id}.output-members" / f"{artifact.artifact_id}.json"
    index.unlink()
    assert TargetOutputCheckpoint.load(state, request=request, output_id=artifact.id) == first
    assert (
        TargetOutputCheckpoint.load_member(state, request=request, artifact_id=artifact.artifact_id)
        == first
    )
    with pytest.raises(ValueError, match="checkpoint changed"):
        TargetOutputCheckpoint.retain(state, **{**arguments, "source_edges_sha256": _sha("2")})
    wrong_workspace = ProcessingWorkspace(
        workspace.root, workspace.execution_id, "encrypted-at-rest"
    )
    with pytest.raises(ValueError, match="workspace differs"):
        first.producer_file(wrong_workspace)
    local.unlink()
    local.symlink_to(tmp_path / "foreign.bin")
    with pytest.raises(ValueError, match="symlinks"):
        first.producer_file(workspace)


def test_step_checkpoint_preserves_original_evidence_after_restart(tmp_path: Path) -> None:
    _operation, _target, request = _request()
    session = TargetExecutionSession(
        request, 1, ClaimedCollectionRuntimeRegistry(), state_root=tmp_path
    )
    first = session.step_value("toolchain", lambda: {"tool": "original"})
    assert first == {"tool": "original"}
    restarted = TargetExecutionSession(
        request, 2, ClaimedCollectionRuntimeRegistry(), state_root=tmp_path
    )
    assert restarted.step_value("toolchain", lambda: {"tool": "changed"}) == first
    assert restarted.stored_step_value("toolchain") == first
    assert restarted.stored_step_value("not-recorded") is None
    with pytest.raises(ValueError, match="checkpoint changed"):
        restarted.retain_step("toolchain", b"replacement")
    with pytest.raises(ValueError, match="record budget"):
        restarted.retain_step("oversized", b"too big", maximum_bytes=2)
    path = next(tmp_path.glob("*.step-*.bin"))
    value = require_canonical_json(path.read_bytes())
    value["request_sha256"] = _sha("f")
    path.write_bytes(canonical_json_bytes(value))
    with pytest.raises(ValueError, match="accepted execution"):
        restarted.stored_step_value("toolchain")


def test_staged_preflight_preserves_complete_sealed_plan_and_native_invocation(
    tmp_path, monkeypatch
):
    operation, target, job = _request()
    base = job.declaration.plan
    records = [f"{index:05d}:" + "x" * 600 for index in range(8192)]
    plan = TransformPlan.seal(
        TransformPlanPayload.model_validate(
            base.model_dump(mode="json", exclude={"plan_sha256"})
        ).model_copy(update={"execution_parameters": {"records": records}})
    )
    preflight = TargetPreflightRequest(
        invocation_sha256=base.invocation_sha256,
        operation_id=operation.id,
        operation_contract_sha256=operation.contract_sha256,
        inputs=base.inputs,
        intent=base.intent,
        target_options=base.target_options,
    )
    received = []

    class Target:
        def descriptor(self):
            return target

        def preflight(self, request):
            assert request == preflight
            received.append(request)
            return TargetPreflightResponse(descriptor=target, plan=plan)

    binding = TargetHttpBinding(Target(), maximum_request_bytes=256, metadata_root=tmp_path)
    assert (
        binding.handle(
            "POST", "/v1/preflight", canonical_json_bytes(preflight.model_dump(mode="json"))
        ).status
        == 413
    )
    real_client = httpx.Client
    wires = []

    def respond(request):
        assert request.headers["Authorization"] == "Bearer test-target-token"
        wires.append((request.url.path, len(request.content)))
        response = binding.handle(request.method, request.url.path, request.content)
        return httpx.Response(
            response.status, content=response.body, headers=dict(response.headers)
        )

    monkeypatch.setattr(
        httpx, "Client", lambda **kwargs: real_client(transport=httpx.MockTransport(respond))
    )
    client = TargetClient("https://target.invalid", token="test-target-token", staged_metadata=True)
    try:
        deadline = time.monotonic() + 30
        while True:
            assert time.monotonic() < deadline
            try:
                result = client.preflight(preflight)
                break
            except ControlBudgetExhausted:
                time.sleep(0.01)
        assert result.plan.plan_sha256 == plan.plan_sha256
        assert result.plan.invocation_sha256 == preflight.invocation_sha256
        assert result.plan.execution_parameters == {"records": records}
        assert canonical_json_bytes(result.plan.model_dump(mode="json")) == canonical_json_bytes(
            plan.model_dump(mode="json")
        )
        assert received == [preflight]
        assert all(path.startswith("/v1/metadata/") and size < 100000 for path, size in wires)
    finally:
        client.close()
        binding.metadata.close()


def test_live_target_retention_prunes_expired_terminal_records_without_restart(tmp_path):
    import os
    import threading
    import time

    operation, descriptor, request = _request()
    service = PersistentTargetService(
        descriptor=descriptor,
        operations={operation.id: operation},
        state_root=tmp_path / "state",
        execute=lambda *_args: pytest.fail("must not execute"),
        terminal_state_retention_seconds=1,
    )
    try:
        accepted = service.state_root / f"{request.declaration.job_id}.accepted.json"
        status = service.state_root / f"{request.declaration.job_id}.status.json"
        with service._lock:
            _write_model(accepted, request.declaration)
            _write_model(status, _success_status(operation, request))
            os.utime(status, (time.time() - 10, time.time() - 10))
        service._retention.wake()
        deadline = time.monotonic() + 5
        while status.exists():
            assert time.monotonic() < deadline
            threading.Event().wait(0.01)
        assert not accepted.exists()
    finally:
        service.close()
