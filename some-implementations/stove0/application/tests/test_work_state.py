from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
from planning_fixture import fixture_interface, observation_headers
from pydantic import ValidationError
from riverhog_client.processing import ClaimedCollectionRuntimeRegistry
from riverhog_protocol.collection_workflows import (
    ArtifactDisposition,
    ArtifactDispositionOutput,
    ArtifactDispositionSetIdentity,
    CollectionDerivation,
)
from riverhog_protocol.collection_workflows import (
    canonical_json_sha256 as riverhog_canonical_json_sha256,
)
from sqlalchemy import text
from stove0_core import (
    ClaimBinding,
    ConcurrentWorkUpdate,
    InMemoryWorkStore,
    Stove0StateError,
    Stove0WorkService,
    TargetCallbackAuthority,
    WorkFailure,
    WorkInapplicable,
    WorkRecord,
)
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ContentObservationFailure,
    ContentObservationInapplicable,
    ContentObservationRequest,
    ContentObservationRequestPayload,
    ContentObservationResult,
    ContentObservationResultPayload,
    ObserverContract,
    ObserverContractPayload,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    ObserverImplementation,
)
from stove0_protocol import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    ArtifactSelection,
    BranchPlan,
    BranchSetDecision,
    BranchSetPlan,
    CollectionRootIdentityRef,
    CoordinationBranchPlan,
    JsonSchemaValidationProfile,
    OperationIdentityRef,
    RecipeIdentityRef,
    WorkArtifactSubject,
    WorkflowPlan,
    WorkflowPlanIntent,
    WorkflowPlanPayload,
    WorkIdentity,
    WorkPayload,
    canonical_json_sha256,
)
from stove0_target_protocol import (
    InputDispositionDeclaration,
    OutputArtifactSetIdentity,
    OutputSourceEdge,
    TargetCallbackAccess,
    TargetInputAuthority,
    TargetOutputBindingSetIdentity,
    TargetProductionAuthority,
    TargetProductionAuthorityPayload,
    TargetSettlementAuthority,
    TargetSettlementAuthorityPayload,
)
from stove0_target_support import (
    InputArtifactContract,
    OperationContract,
    OperationContractPayload,
    OutputArtifact,
    OutputArtifactContract,
    OutputCollectionRef,
    TargetDescriptor,
    TargetDescriptorPayload,
    TargetExecutionEvidence,
    TargetExecutionSession,
    TargetJobDeclaration,
    TargetJobRequest,
    TargetJobStatus,
    TargetOperationSupport,
    TargetProgress,
    TargetRuntimeAuthority,
    TransformPlan,
    TransformPlanPayload,
)


def _sha(character: str) -> str:
    return character * 64


def _member_id(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _root() -> CollectionRootIdentityRef:
    return CollectionRootIdentityRef(
        collection_id=str(1),
        archive_root_sha256=_sha("1"),
        artifact_set_identity=_sha("2"),
    )


def _work() -> WorkIdentity:
    return WorkIdentity.seal(
        WorkPayload(
            recipe=RecipeIdentityRef(id="fixture.recipe/v1", revision="1", sha256=_sha("3")),
            inputs=(_root(),),
            effective_intent={"suffix": ".copy"},
        )
    )


def _observer() -> tuple[ObserverContract, ObserverDescriptor]:
    contract = ObserverContract.seal(
        ObserverContractPayload(
            id="fixture.kind/v1",
            facts_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            options_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.kind-options/v1",
                {"type": "object", "additionalProperties": False},
            ),
            facts_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.kind-facts/v1",
                {
                    "type": "object",
                    "properties": {"kind": {"type": "string"}},
                    "required": ["kind"],
                    "additionalProperties": False,
                },
            ),
        )
    )
    descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="fixture.observer/v1",
            implementation_version="1.0.0",
            source_revision="fixture",
            image_id="sha256:" + _sha("9"),
            contracts=(
                ObserverContractSupport.from_contract(
                    contract, interfaces=(fixture_interface(contract).ref,)
                ),
            ),
        )
    )
    return contract, descriptor


def _observation(
    work: WorkIdentity,
    contract: ObserverContract,
    descriptor: ObserverDescriptor,
) -> tuple[ContentObservationRequest, ContentObservationResult]:
    subject = WorkArtifactSubject(
        id="source",
        role="fixture.source/v1",
        collection=_root(),
        artifact_id=_member_id("source/input.bin"),
        bytes=str(12),
        sha256=_sha("4"),
    )
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            **observation_headers(work_id=work.work_id, contract=contract, subjects=(subject,)),
            work_id=work.work_id,
            observer_registration_id="fixture-observer",
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=contract.id,
            observer_contract_sha256=contract.contract_sha256,
            subjects=(subject,),
            options={},
        )
    )
    facts = {"kind": "fixture"}
    result = ContentObservationResult.seal(
        ContentObservationResultPayload(
            request_id=request.request_id,
            state="observed",
            observer=ObserverImplementation(
                id=descriptor.implementation_id,
                version=descriptor.implementation_version,
                source_revision=descriptor.source_revision,
                descriptor_sha256=descriptor.descriptor_sha256,
            ),
            observer_contract_id=contract.id,
            observer_contract_sha256=contract.contract_sha256,
            subjects=request.subjects,
            facts_schema=contract.facts_schema,
            facts=facts,
            facts_sha256=canonical_json_sha256(facts),
        )
    )
    return request, result


def _operation() -> OperationContract:
    return OperationContract.seal(
        OperationContractPayload(
            id="fixture.copy/v1",
            intent_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            intent_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.copy-intent/v1",
                {
                    "type": "object",
                    "properties": {"suffix": {"type": "string"}},
                    "required": ["suffix"],
                    "additionalProperties": False,
                },
            ),
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


def _target_plan(
    operation: OperationContract,
    target: TargetDescriptor,
    *,
    invocation_sha256: str,
    observation_result_sha256s: tuple[str, ...] = (),
    selection: ArtifactSelection | None = None,
) -> TransformPlan:
    if selection is None:
        selection = ArtifactSelection.seal(
            (
                WorkArtifactSubject(
                    id="source",
                    role="fixture.source/v1",
                    collection=_root(),
                    artifact_id=_member_id("source/input.bin"),
                    bytes=str(12),
                    sha256=_sha("4"),
                ),
            )
        )
    return TransformPlan.seal(
        TransformPlanPayload(
            invocation_sha256=invocation_sha256,
            target_implementation_id=target.implementation_id,
            target_descriptor_sha256=target.descriptor_sha256,
            operation_id=operation.id,
            operation_contract_sha256=operation.contract_sha256,
            inputs=TargetInputAuthority.from_selection(selection),
            intent={"suffix": ".copy"},
            target_options={},
            observation_result_sha256s=observation_result_sha256s,
        )
    )


def _branch_decision(
    work: WorkIdentity,
    selection: ArtifactSelection | None = None,
    *,
    observations=(),
) -> BranchSetDecision:
    operation = _operation()
    target = _target(operation)
    if selection is None:
        selection = ArtifactSelection.seal(
            (
                WorkArtifactSubject(
                    id="source",
                    role="fixture.source/v1",
                    collection=_root(),
                    artifact_id=_member_id("source/input.bin"),
                    bytes=str(12),
                    sha256=_sha("4"),
                ),
            )
        )
    branch = BranchPlan.build(
        parent_work=work,
        branch_id="fixture",
        decision_sha256=_sha("d"),
        selection=selection,
        recipe=work.recipe,
        effective_intent=work.effective_intent,
        workflow_intent=WorkflowPlanIntent(
            operation=OperationIdentityRef(id=operation.id, sha256=operation.contract_sha256),
            target_registration_id="fixture-target",
            target_descriptor_sha256=target.descriptor_sha256,
            source_collection_retirement_policy="retain",
        ),
        observations=observations,
    )
    return BranchSetDecision(
        plan=BranchSetPlan.seal(
            parent_work=work,
            decision_sha256=_sha("d"),
            evidence_sha256s=tuple(item.result.result_sha256 for item in observations),
            branches=(branch,),
            selections={selection.selection_sha256: selection},
        ),
        selections=(selection,),
    )


def _queued_target_callback_execution(
    selection: ArtifactSelection | None = None,
    *,
    seal_batch_size: int = 100,
    observations=(),
) -> tuple[
    InMemoryWorkStore,
    Stove0WorkService,
    WorkRecord,
    OperationContract,
    TargetCallbackAuthority,
    TargetCallbackAccess,
]:
    store = InMemoryWorkStore()
    service = Stove0WorkService(store)
    parent = _work()
    parent_record = service.create_or_resume(parent)
    parent_record = service.bind_claim(
        parent.work_id,
        claim_id=parent.work_id,
        fence=1,
        expected_revision=parent_record.revision,
    )
    parent_record = service.begin_planning(
        parent.work_id,
        expected_revision=parent_record.revision,
    )
    decision = _branch_decision(parent, selection, observations=observations)
    service.admit_branch_set(
        parent.work_id,
        decision,
        expected_revision=parent_record.revision,
    )
    branch = decision.leaf_branches()[0]
    child_id = branch.workflow_plan.work.work_id
    record = store.load(child_id)
    assert record is not None
    record = service.bind_claim(
        child_id,
        claim_id=child_id,
        fence=1,
        expected_revision=record.revision,
    )
    record = service.activate_preplanned(child_id, expected_revision=record.revision)
    operation = _operation()
    target = _target(operation)
    record = service.seal_target_plan(
        child_id,
        target=target,
        plan=_target_plan(
            operation,
            target,
            invocation_sha256=record.workflow_plan.workflow_plan_sha256,
            selection=decision.selections[0],
            observation_result_sha256s=tuple(
                sorted(item.result.result_sha256 for item in observations)
            ),
        ),
        expected_revision=record.revision,
    )

    class Operations:
        def operation_contract(self, _operation: object) -> OperationContract:
            return operation

    class Projector:
        dispositions: list[ArtifactDisposition] = []
        edges: list[ArtifactDispositionOutput] = []

        def project_target_dispositions(
            self,
            _record: WorkRecord,
            dispositions: object,
        ) -> None:
            self.dispositions.extend(dispositions)  # type: ignore[arg-type]

        def project_target_source_edges(
            self,
            _record: WorkRecord,
            edges: object,
        ) -> None:
            self.edges.extend(edges)  # type: ignore[arg-type]

        def seal_target_projection(
            self,
            _record: WorkRecord,
        ) -> ArtifactDispositionSetIdentity:
            assert all(isinstance(item, ArtifactDisposition) for item in self.dispositions)
            assert all(isinstance(item, ArtifactDispositionOutput) for item in self.edges)
            return ArtifactDispositionSetIdentity(
                disposition_count=len(self.dispositions),
                output_edge_count=len(self.edges),
                output_artifact_count=len({item.output_artifact_id for item in self.edges}),
                sha256=_sha("e"),
            )

    callbacks = TargetCallbackAuthority(
        store,
        signing_key="fixture target callback signing key",
        base_url="https://stove0.invalid",
        allow_insecure_http=False,
        ttl_seconds=900,
        operations=Operations(),
        projector=Projector(),
        seal_batch_size=seal_batch_size,
    )
    access = callbacks.issue_access(record, "fixture-target")
    assert record.controller_evidence is not None
    declaration = TargetJobDeclaration(
        job_id=record.controller_evidence.execution_envelope.execution_envelope_sha256,
        claim_id=child_id,
        fence=1,
        controller_evidence=record.controller_evidence,
        plan=record.target_plan,
        declared_workspace_protection="memory-backed",
    )
    request = TargetJobRequest.seal(
        declaration,
        TargetRuntimeAuthority(
            riverhog_base_url="https://riverhog.invalid",
            capability_token="secret",
        ),
        access,
    )
    record = service.bind_target_request(
        child_id,
        request,
        expected_revision=record.revision,
    )
    return store, service, record, operation, callbacks, access


def _seal_production(
    callbacks: TargetCallbackAuthority,
    token: str,
    job_id: str,
) -> TargetProductionAuthority:
    for _ in range(32):
        response = callbacks.seal_production(token, job_id=job_id)
        if response.state == "sealed":
            assert response.production is not None
            return response.production
    raise AssertionError("target production did not seal")


def test_target_callback_authority_seals_exact_production_and_is_idempotent() -> None:
    _store, _service, record, _operation_contract, callbacks, access = (
        _queued_target_callback_execution()
    )
    assert record.controller_evidence is not None
    job_id = record.controller_evidence.execution_envelope.execution_envelope_sha256
    page = callbacks.input_page(
        access.token,
        job_id=job_id,
        continuation=None,
        limit=256,
    )
    assert page.complete is True
    assert len(page.artifacts) == 1
    source = page.artifacts[0]
    output = OutputArtifact(
        id="output",
        role="fixture.output/v1",
        artifact_id=_member_id("output/result.bin"),
        bytes=str(12),
        sha256=_sha("5"),
    )
    disposition = InputDispositionDeclaration(input_id=source.id, status="transformed")
    edge = OutputSourceEdge(output_id=output.id, input_id=source.id)
    for _ in range(2):
        callbacks.declare_output(access.token, job_id=job_id, output=output)
        callbacks.declare_disposition(access.token, job_id=job_id, disposition=disposition)
        callbacks.declare_source_edge(access.token, job_id=job_id, edge=edge)
    sealed = _seal_production(callbacks, access.token, job_id)
    assert sealed.outputs == OutputArtifactSetIdentity.seal((output,))
    assert sealed.disposition_count == 1
    assert sealed.source_edge_count == 1


@pytest.mark.parametrize("started", [False, True], ids=["queued", "running"])
def test_callback_refresh_after_real_expiry_preserves_job_and_claim_scope(started: bool) -> None:
    with patch("stove0_core.target_callbacks.time.time", return_value=1000):
        _store, service, record, _operation_contract, callbacks, access = (
            _queued_target_callback_execution()
        )
    assert record.target_request is not None
    request = TargetJobRequest.seal(
        record.target_request.declaration,
        TargetRuntimeAuthority(
            riverhog_base_url="https://riverhog.invalid", capability_token="fixture-secret"
        ),
        access,
    )
    session = TargetExecutionSession(request, 1, ClaimedCollectionRuntimeRegistry())
    client = session.callback_client() if started else None
    job_id = request.declaration.job_id

    def callback(http_request: httpx.Request) -> httpx.Response:
        token = http_request.headers["Authorization"].removeprefix("Bearer ")
        page = callbacks.input_page(token, job_id=job_id, continuation=None, limit=256)
        return httpx.Response(200, json=page.model_dump(mode="json"))

    with patch("stove0_core.target_callbacks.time.time", return_value=2000):
        with pytest.raises(PermissionError, match="unavailable"):
            callbacks.input_page(access.token, job_id=job_id, continuation=None, limit=256)
        renewed = callbacks.issue_access(record, "fixture-target")
        session.refresh_callback_access(renewed)
        current = session.callback_client()
        assert client is None or current is client
        current._client.close()
        current._client = httpx.Client(transport=httpx.MockTransport(callback))
        try:
            assert len(tuple(current.iter_inputs(job_id))) == 1
            with pytest.raises(ValueError, match="callback endpoint changed"):
                current.refresh_access(
                    renewed.model_copy(update={"stove0_base_url": "https://different.invalid"})
                )
            with pytest.raises(PermissionError, match="unavailable"):
                callbacks.input_page(renewed.token, job_id=_sha("9"), continuation=None, limit=256)
            assert record.claim is not None
            service.rebind_claim(
                record.work_id,
                claim_id=record.claim.claim_id,
                fence=record.claim.fence + 1,
                expected_revision=record.revision,
            )
            with pytest.raises(PermissionError, match="stale"):
                tuple(current.iter_inputs(job_id))
        finally:
            current.close()


def test_target_production_seal_is_segmented_closes_declarations_and_replays() -> None:
    selection = ArtifactSelection.seal(
        tuple(
            WorkArtifactSubject(
                id=f"source-{ordinal}",
                role="fixture.source/v1",
                collection=_root(),
                artifact_id=_member_id(f"source/{ordinal}.bin"),
                bytes=str(ordinal + 1),
                sha256=_sha(str(ordinal + 1)),
            )
            for ordinal in range(3)
        )
    )
    _store, _service, record, _operation, callbacks, access = _queued_target_callback_execution(
        selection, seal_batch_size=1
    )
    assert record.controller_evidence is not None
    job_id = record.controller_evidence.execution_envelope.execution_envelope_sha256
    inputs = callbacks.input_page(
        access.token,
        job_id=job_id,
        continuation=None,
        limit=256,
    ).artifacts
    outputs = tuple(
        OutputArtifact(
            id=f"output-{ordinal}",
            role="fixture.output/v1",
            artifact_id=_member_id(f"output/{ordinal}.bin"),
            bytes=str(source.bytes),
            sha256=_sha(str(ordinal + 4)),
        )
        for ordinal, source in enumerate(inputs)
    )
    for source, output in zip(inputs, outputs, strict=True):
        callbacks.declare_output(access.token, job_id=job_id, output=output)
        callbacks.declare_disposition(
            access.token,
            job_id=job_id,
            disposition=InputDispositionDeclaration(
                input_id=source.id,
                status="transformed",
            ),
        )
        callbacks.declare_source_edge(
            access.token,
            job_id=job_id,
            edge=OutputSourceEdge(output_id=output.id, input_id=source.id),
        )

    first = callbacks.seal_production(access.token, job_id=job_id)
    assert first.state == "sealing"
    with pytest.raises(Stove0StateError, match="declarations are closed"):
        callbacks.declare_output(
            access.token,
            job_id=job_id,
            output=outputs[0],
        )

    calls = 1
    while True:
        response = callbacks.seal_production(access.token, job_id=job_id)
        calls += 1
        if response.state == "sealed":
            break
        assert calls < 64
    assert calls > 12
    assert response.production is not None
    replay = callbacks.seal_production(access.token, job_id=job_id)
    assert replay.state == "sealed"
    assert replay.production == response.production


def test_target_callback_dispositions_cover_multi_input_selection_by_identity() -> None:
    selection = ArtifactSelection.seal(
        (
            WorkArtifactSubject(
                id="a-request-first",
                role="fixture.source/v1",
                collection=_root(),
                artifact_id=_member_id("z-collection-last.bin"),
                bytes=str(1),
                sha256=_sha("4"),
            ),
            WorkArtifactSubject(
                id="z-request-last",
                role="fixture.source/v1",
                collection=_root(),
                artifact_id=_member_id("a-collection-first.bin"),
                bytes=str(1),
                sha256=_sha("5"),
            ),
        )
    )
    _store, _service, record, _operation, callbacks, access = _queued_target_callback_execution(
        selection
    )
    assert record.controller_evidence is not None
    job_id = record.controller_evidence.execution_envelope.execution_envelope_sha256
    output = OutputArtifact(
        id="output",
        role="fixture.output/v1",
        artifact_id=_member_id("output/result.bin"),
        bytes=str(2),
        sha256=_sha("6"),
    )
    callbacks.declare_output(access.token, job_id=job_id, output=output)
    for source in reversed(selection.artifacts):
        callbacks.declare_disposition(
            access.token,
            job_id=job_id,
            disposition=InputDispositionDeclaration(
                input_id=source.id,
                status="transformed",
            ),
        )
        callbacks.declare_source_edge(
            access.token,
            job_id=job_id,
            edge=OutputSourceEdge(output_id=output.id, input_id=source.id),
        )

    sealed = _seal_production(callbacks, access.token, job_id)

    assert sealed.disposition_count == 2
    assert sealed.source_edge_count == 2


def test_target_callback_authority_rejects_unpermitted_disposition_and_stale_fence() -> None:
    _store, service, record, _operation_contract, callbacks, access = (
        _queued_target_callback_execution()
    )
    assert record.controller_evidence is not None
    job_id = record.controller_evidence.execution_envelope.execution_envelope_sha256
    source = callbacks.input_page(
        access.token,
        job_id=job_id,
        continuation=None,
        limit=256,
    ).artifacts[0]
    output = OutputArtifact(
        id="output",
        role="fixture.output/v1",
        artifact_id=_member_id("output/result.bin"),
        bytes=str(12),
        sha256=_sha("5"),
    )
    callbacks.declare_output(access.token, job_id=job_id, output=output)
    callbacks.declare_disposition(
        access.token,
        job_id=job_id,
        disposition=InputDispositionDeclaration(input_id=source.id, status="preserved"),
    )
    callbacks.declare_source_edge(
        access.token,
        job_id=job_id,
        edge=OutputSourceEdge(output_id=output.id, input_id=source.id),
    )
    with pytest.raises(ValueError, match="disposition is not permitted"):
        _seal_production(callbacks, access.token, job_id)

    rebound = service.rebind_claim(
        record.work_id,
        claim_id=record.claim.claim_id,  # type: ignore[union-attr]
        fence=2,
        expected_revision=record.revision,
    )
    assert rebound.claim is not None and rebound.claim.fence == 2
    with pytest.raises(PermissionError, match="stale"):
        callbacks.input_page(
            access.token,
            job_id=job_id,
            continuation=None,
            limit=256,
        )


def _nested_branch_decision(work: WorkIdentity) -> BranchSetDecision:
    operation = _operation()
    target = _target(operation)
    selection = ArtifactSelection.seal(
        (
            WorkArtifactSubject(
                id="source",
                role="fixture.source/v1",
                collection=_root(),
                artifact_id=_member_id("source/input.bin"),
                bytes=str(12),
                sha256=_sha("4"),
            ),
        )
    )
    child_work = CoordinationBranchPlan.build_work(
        parent_work=work,
        branch_id="nested",
        decision_sha256=_sha("d"),
        selection=selection,
        recipe=RecipeIdentityRef(id="fixture.child/v1", revision="1", sha256=_sha("5")),
        effective_intent={"scope": "child"},
    )
    leaf = BranchPlan.build(
        parent_work=child_work,
        branch_id="leaf",
        decision_sha256=_sha("e"),
        selection=selection,
        recipe=child_work.recipe,
        effective_intent=child_work.effective_intent,
        workflow_intent=WorkflowPlanIntent(
            operation=OperationIdentityRef(id=operation.id, sha256=operation.contract_sha256),
            target_registration_id="fixture-target",
            target_descriptor_sha256=target.descriptor_sha256,
            source_collection_retirement_policy="retain",
        ),
    )
    child_plan = BranchSetPlan.seal(
        parent_work=child_work,
        decision_sha256=_sha("e"),
        branches=(leaf,),
        selections={selection.selection_sha256: selection},
    )
    nested = CoordinationBranchPlan(
        branch_id="nested",
        artifact_selection=selection.ref(),
        work=child_work,
        branch_set_sha256=child_plan.branch_set_sha256,
    )
    root_plan = BranchSetPlan.seal(
        parent_work=work,
        decision_sha256=_sha("d"),
        branches=(nested,),
        selections={selection.selection_sha256: selection},
        branch_sets={child_plan.branch_set_sha256: child_plan},
    )
    return BranchSetDecision(
        plan=root_plan,
        selections=(selection,),
        branch_sets=(child_plan,),
    )


def test_one_record_carries_observation_plan_execution_verification_and_completion() -> None:
    parent = _work()
    contract, descriptor = _observer()
    request, result = _observation(parent, contract, descriptor)
    evidence = ContentObservationEvidence(request=request, result=result)
    store, service, record, operation, _callbacks, _access = _queued_target_callback_execution(
        ArtifactSelection.seal(request.subjects),
        observations=(evidence,),
    )
    work, workflow = record.work, record.workflow_plan
    assert workflow.observations == (evidence,)
    assert service.create_or_resume(work) == record
    plan, target_request = record.target_plan, record.target_request
    target = _target(operation)
    declaration = target_request.declaration
    running = TargetJobStatus(
        job_id=declaration.job_id,
        state="running",
        attempt=1,
        request_sha256=target_request.request_sha256,
        plan_sha256=plan.plan_sha256,
        progress=TargetProgress(phase="transform", completed=1, total=2),
    )
    record = service.record_target_status(
        work.work_id,
        running,
        operation=operation,
        expected_revision=record.revision,
    )
    output = OutputArtifact(
        id="output",
        role="fixture.output/v1",
        artifact_id=_member_id("output/result.bin"),
        bytes=str(12),
        sha256=_sha("5"),
    )
    assert record.controller_evidence is not None
    workflow = record.controller_evidence.execution_envelope.workflow_plan
    disposition_set = ArtifactDispositionSetIdentity(
        disposition_count=1,
        output_edge_count=1,
        output_artifact_count=1,
        sha256=_sha("8"),
    )
    derivation = CollectionDerivation(
        execution_id=declaration.job_id,
        claim_id=declaration.claim_id,
        fence=declaration.fence,
        recipe=workflow.work.recipe.to_identity(),
        operation=workflow.operation.to_identity(),
        input_set_sha256=_sha("a"),
        artifact_set_sha256=_sha("b"),
        execution_envelope_sha256=declaration.job_id,
        execution_sha256=_sha("9"),
        controller_evidence=record.controller_evidence.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        controller_evidence_sha256=riverhog_canonical_json_sha256(
            record.controller_evidence.model_dump(mode="json", by_alias=True, exclude_none=True)
        ),
        disposition_set=disposition_set,
    )
    output_collection = OutputCollectionRef(
        collection_id=str(7),
        archive_root_sha256=_sha("6"),
        artifact_set_identity=_sha("7"),
        derivation_sha256=derivation.sha256,
    )
    production = TargetProductionAuthority.seal(
        TargetProductionAuthorityPayload(
            job_id=declaration.job_id,
            plan_sha256=plan.plan_sha256,
            outputs=OutputArtifactSetIdentity.seal((output,)),
            disposition_count=1,
            disposition_sha256=_sha("c"),
            source_edge_count=1,
            source_edge_sha256=_sha("d"),
            riverhog_disposition_set=disposition_set,
        )
    )
    succeeded = TargetJobStatus(
        job_id=declaration.job_id,
        state="succeeded",
        attempt=1,
        request_sha256=target_request.request_sha256,
        plan_sha256=plan.plan_sha256,
        progress=TargetProgress(phase="done", completed=2, total=2),
        production=production,
        output_collection=output_collection,
        execution_evidence=TargetExecutionEvidence(
            target_descriptor_sha256=target.descriptor_sha256,
            operation_contract_sha256=operation.contract_sha256,
            plan_sha256=plan.plan_sha256,
            execution_sha256=_sha("9"),
        ),
        derivation=derivation.as_dict(),
    )
    record = service.record_target_status(
        work.work_id,
        succeeded,
        operation=operation,
        expected_revision=record.revision,
    )
    assert record.phase == "verifying"
    settlement = TargetSettlementAuthority.seal(
        TargetSettlementAuthorityPayload(
            job_id=declaration.job_id,
            production_sha256=production.production_sha256,
            output_collection=output_collection,
            output_bindings=TargetOutputBindingSetIdentity(
                artifact_count=1,
                total_bytes=str(output.bytes),
                sha256=_sha("e"),
            ),
        )
    )
    record = service.verify_output(
        work.work_id,
        output_collection,
        settlement,
        expected_revision=record.revision,
    )
    assert record.phase == "settled"
    record = service.begin_source_collection_retirement(
        work.work_id,
        (),
        expected_revision=record.revision,
    )
    assert record.phase == "complete"


def test_new_claim_fence_resets_unsettled_execution_authorities() -> None:
    store, service, record, operation, _callbacks, _access = _queued_target_callback_execution()
    work, workflow = record.work, record.workflow_plan
    target = _target(operation)
    stale_execution_id = record.controller_evidence.execution_envelope.execution_envelope_sha256
    stale_output = OutputArtifact(
        id="result",
        role="fixture.output/v1",
        artifact_id=_member_id("output/result.bin"),
        bytes=str(1),
        sha256=_sha("1"),
    )
    store.ensure_target_production_receiving(work.work_id, stale_execution_id)
    store.record_target_output(work.work_id, stale_execution_id, stale_output)

    rebound = service.rebind_claim(
        work.work_id,
        claim_id=work.work_id,
        fence=2,
        expected_revision=record.revision,
    )

    assert rebound.phase == "claimed"
    assert rebound.claim == ClaimBinding(claim_id=work.work_id, fence=2)
    assert rebound.workflow_plan == workflow
    assert rebound.target_plan is None
    assert rebound.controller_evidence is None

    rebound = service.activate_preplanned(work.work_id, expected_revision=rebound.revision)
    rebound = service.seal_target_plan(
        work.work_id,
        target=target,
        plan=_target_plan(operation, target, invocation_sha256=workflow.workflow_plan_sha256),
        expected_revision=rebound.revision,
    )
    assert rebound.controller_evidence is not None
    assert rebound.controller_evidence.execution_envelope.fence == 2
    assert (
        rebound.controller_evidence.execution_envelope.execution_envelope_sha256
        != stale_execution_id
    )
    current_execution_id = rebound.controller_evidence.execution_envelope.execution_envelope_sha256
    current_output = stale_output.model_copy(update={"bytes": 2, "sha256": _sha("2")})
    store.ensure_target_production_receiving(work.work_id, current_execution_id)
    store.record_target_output(work.work_id, current_execution_id, current_output)

    assert store.load_target_output(work.work_id, stale_execution_id, "result") == stale_output
    assert store.load_target_output(work.work_id, current_execution_id, "result") == current_output
    with pytest.raises(Stove0StateError, match="generation is stale"):
        store.record_target_output(work.work_id, stale_execution_id, stale_output)


@pytest.mark.parametrize(
    ("state", "outcome", "expected_phase", "expected_abandon"),
    [
        (
            "inapplicable",
            {
                "inapplicable": ContentObservationInapplicable(
                    code="unsupported", message="No match"
                )
            },
            "abandon_pending",
            "inapplicable",
        ),
        (
            "failed",
            {
                "failure": ContentObservationFailure(
                    code="temporary", message="Try again", retryable=True
                )
            },
            "failed",
            None,
        ),
        (
            "failed",
            {
                "failure": ContentObservationFailure(
                    code="invalid", message="Cannot inspect", retryable=False
                )
            },
            "abandon_pending",
            "failed",
        ),
        ("canceled", {}, "abandon_pending", "canceled"),
    ],
)
def test_terminal_observation_results_converge_without_target_execution(
    state: str,
    outcome: dict[str, object],
    expected_phase: str,
    expected_abandon: str | None,
) -> None:
    from test_coordinator import (
        FixtureObservers,
        FixturePlanning,
        FixtureRiverhog,
        FixtureTarget,
        FixtureTargetCallbacks,
        _coordinator,
    )
    from test_coordinator import (
        _observer as controller_observer,
    )
    from test_coordinator import (
        _operation as controller_operation,
    )
    from test_coordinator import (
        _target as controller_target,
    )

    operation, observer = controller_operation(), controller_observer()
    descriptor = controller_target(operation)
    store = InMemoryWorkStore()
    service = Stove0WorkService(store)

    class TerminalObserver(FixtureObservers):
        def put_job(self, registration_id, invocation, *, descriptor):
            status = super().put_job(registration_id, invocation, descriptor=descriptor)
            result = ContentObservationResult.seal(
                ContentObservationResultPayload(
                    **status.result.model_dump(
                        mode="python",
                        exclude_none=True,
                        exclude={
                            "result_sha256",
                            "state",
                            "facts_schema",
                            "facts",
                            "facts_sha256",
                        },
                    ),
                    state=state,
                    **outcome,
                )
            )
            return status.model_copy(update={"result": result})

    coordinator = _coordinator(
        service,
        riverhog=FixtureRiverhog(),
        planning=FixturePlanning(operation, descriptor, observer),
        observers=TerminalObserver(observer),
        targets=FixtureTarget(operation, descriptor),
        target_callbacks=FixtureTargetCallbacks(store),
    )
    record = coordinator.create_or_resume(_work())
    for _ in range(8):
        record = coordinator.step(record.work_id)
        if record.phase == expected_phase:
            break
    assert record.phase == expected_phase and record.abandon_outcome == expected_abandon
    deliveries = store.scan_observation_deliveries("work", record.work_id, limit=100)
    assert len(deliveries) == 1 and deliveries[0].status.result.state == state
    assert record.target_plan is None


def test_stale_revision_and_invalid_success_order_fail_closed() -> None:
    service = Stove0WorkService(InMemoryWorkStore())
    record = service.create_or_resume(_work())
    claimed = service.bind_claim(
        record.work_id,
        claim_id=record.work_id,
        fence=1,
        expected_revision=record.revision,
    )
    with pytest.raises(ConcurrentWorkUpdate):
        service.begin_planning(record.work_id, expected_revision=record.revision)
    with pytest.raises(Stove0StateError, match="verify output"):
        service.verify_output(
            record.work_id,
            OutputCollectionRef(
                collection_id=str(7),
                archive_root_sha256=_sha("6"),
                artifact_set_identity=_sha("7"),
                derivation_sha256=_sha("8"),
            ),
            TargetSettlementAuthority.seal(
                TargetSettlementAuthorityPayload(
                    job_id=record.work_id,
                    production_sha256=_sha("9"),
                    output_collection=OutputCollectionRef(
                        collection_id=str(7),
                        archive_root_sha256=_sha("6"),
                        artifact_set_identity=_sha("7"),
                        derivation_sha256=_sha("8"),
                    ),
                    output_bindings=TargetOutputBindingSetIdentity(
                        artifact_count=1,
                        total_bytes=str(1),
                        sha256=_sha("a"),
                    ),
                )
            ),
            expected_revision=claimed.revision,
        )


@pytest.mark.parametrize(
    ("transition", "terminal"),
    [
        ("cancel", "canceled"),
        ("inapplicable", "inapplicable"),
        ("failed", "failed"),
    ],
)
def test_no_output_terminal_work_is_crash_safe_through_abandon_pending(
    transition: str,
    terminal: str,
) -> None:
    service = Stove0WorkService(InMemoryWorkStore())
    record = service.create_or_resume(_work())
    record = service.bind_claim(
        record.work_id,
        claim_id=record.work_id,
        fence=1,
        expected_revision=record.revision,
    )
    if transition == "cancel":
        record = service.cancel(record.work_id, expected_revision=record.revision)
    elif transition == "inapplicable":
        record = service.mark_inapplicable(
            record.work_id,
            WorkInapplicable(code="not-applicable", message="fixture outcome"),
            expected_revision=record.revision,
        )
    else:
        record = service.fail(
            record.work_id,
            WorkFailure(code="terminal", message="fixture failure", retryable=False),
            expected_revision=record.revision,
        )

    assert record.phase == "abandon_pending"
    assert record.abandon_outcome == terminal
    completed = service.complete_abandon(
        record.work_id,
        expected_revision=record.revision,
    )
    assert completed.phase == terminal
    assert completed.abandon_outcome is None


def test_retryable_failed_work_requires_a_new_fencing_generation() -> None:
    service = Stove0WorkService(InMemoryWorkStore())
    record = service.create_or_resume(_work())
    record = service.bind_claim(
        record.work_id,
        claim_id=record.work_id,
        fence=1,
        expected_revision=record.revision,
    )
    record = service.fail(
        record.work_id,
        WorkFailure(code="temporary", message="retry later", retryable=True),
        expected_revision=record.revision,
    )
    with pytest.raises(Stove0StateError, match="advance the Riverhog claim fence"):
        service.retry_failed(
            record.work_id,
            claim_id=record.work_id,
            fence=1,
            expected_revision=record.revision,
        )

    retried = service.retry_failed(
        record.work_id,
        claim_id=record.work_id,
        fence=2,
        expected_revision=record.revision,
    )
    assert retried.phase == "claimed"
    assert retried.claim == ClaimBinding(claim_id=record.work_id, fence=2)
    assert retried.failure is None


def test_retryable_failed_work_can_be_canceled_and_abandoned() -> None:
    service = Stove0WorkService(InMemoryWorkStore())
    record = service.create_or_resume(_work())
    record = service.bind_claim(
        record.work_id,
        claim_id=record.work_id,
        fence=1,
        expected_revision=record.revision,
    )
    record = service.fail(
        record.work_id,
        WorkFailure(code="temporary", message="retry later", retryable=True),
        expected_revision=record.revision,
    )
    assert record.phase == "failed"
    assert record.failure is not None and record.failure.retryable

    pending = service.cancel(record.work_id, expected_revision=record.revision)
    assert pending.phase == "abandon_pending"
    assert pending.failure is None
    assert pending.abandon_outcome == "canceled"

    terminal = service.complete_abandon(
        pending.work_id,
        expected_revision=pending.revision,
    )
    assert terminal.phase == "canceled"


def test_sql_work_reads_reuse_exact_validation_without_sharing_mutable_facts(
    tmp_path: Path,
) -> None:
    from stove0_core import SqlAlchemyStateStore
    from stove0_core.persistence import _WORK_RECORD_VALIDATION_CACHE

    _WORK_RECORD_VALIDATION_CACHE.clear()
    store = SqlAlchemyStateStore(f"sqlite+pysqlite:///{tmp_path / 'read-cache.sqlite3'}")
    record = store.create(WorkRecord(work=_work()))
    with patch.object(
        WorkRecord, "model_validate_json", wraps=WorkRecord.model_validate_json
    ) as parse:
        loaded = store.load(record.work_id)
        assert loaded == record
        assert loaded is not None
        loaded.work.effective_intent["suffix"] = ".changed"
        assert store.load(record.work_id) == record
        assert parse.call_count == 1

        # The database remains authoritative: an altered document is validated
        # afresh even when its primary key and revision did not change.
        invalid = record.model_dump(mode="json", by_alias=True, exclude_none=True)
        invalid["work"]["work_id"] = "f" * 64
        with store.engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE stove0_work_records SET document_json = :document "
                    "WHERE work_id = :work_id"
                ),
                {"document": json.dumps(invalid), "work_id": record.work_id},
            )
        with pytest.raises(ValidationError):
            store.load(record.work_id)
        assert parse.call_count == 2


def test_work_larger_than_validation_cache_budget_is_accepted_without_caching() -> None:
    from stove0_core.persistence import _WORK_RECORD_VALIDATION_CACHE, _decode_work_record

    _WORK_RECORD_VALIDATION_CACHE.clear()
    work = WorkIdentity.seal(
        WorkPayload(
            recipe=_work().recipe,
            inputs=_work().inputs,
            effective_intent={"evidence": "a" * (8 * 1024 * 1024 + 1)},
        )
    )
    record = WorkRecord(work=work)
    document = record.model_dump_json(by_alias=True, exclude_none=True)
    with patch.object(
        WorkRecord, "model_validate_json", wraps=WorkRecord.model_validate_json
    ) as parse:
        assert _decode_work_record(document) == record
        assert _decode_work_record(document) == record
        assert parse.call_count == 2


def test_repeated_six_record_browse_reuses_validation_and_isolates_facts(tmp_path: Path) -> None:
    from stove0_core import SqlAlchemyStateStore
    from stove0_core.persistence import _WORK_RECORD_VALIDATION_CACHE

    _WORK_RECORD_VALIDATION_CACHE.clear()
    store = SqlAlchemyStateStore(f"sqlite+pysqlite:///{tmp_path / 'six-records.sqlite3'}")
    records = [
        store.create(
            WorkRecord(
                work=WorkIdentity.seal(
                    WorkPayload(
                        recipe=_work().recipe,
                        inputs=_work().inputs,
                        effective_intent={"index": str(index), "evidence": "a" * 65536},
                    )
                )
            )
        )
        for index in range(6)
    ]
    with patch.object(
        WorkRecord, "model_validate_json", wraps=WorkRecord.model_validate_json
    ) as parse:
        for _ in range(3):
            rows = store.list_work(page_size=100, sort="work_id")["work"]
            assert isinstance(rows, list) and len(rows) == 6
            assert all(isinstance(row, WorkRecord) for row in rows)
            rows[0].work.effective_intent["evidence"] = "local edit"
            assert store.load(records[0].work_id) == records[0]
        assert parse.call_count == 6


@pytest.mark.parametrize("max_entries", [1, 128])
def test_work_validation_eviction_bounds_documents_without_rejecting_them(max_entries: int) -> None:
    from stove0_core.persistence import _WorkRecordValidationCache

    records = [
        WorkRecord(
            work=WorkIdentity.seal(
                WorkPayload(
                    recipe=_work().recipe,
                    inputs=_work().inputs,
                    effective_intent={"index": str(index), "evidence": "é" * 500},
                )
            )
        )
        for index in range(3)
    ]
    documents = [record.model_dump_json(by_alias=True, exclude_none=True) for record in records]
    budget = 2 * len(documents[0].encode("utf-8"))
    cache = _WorkRecordValidationCache(max_bytes=budget, max_entries=max_entries)
    with patch.object(
        WorkRecord, "model_validate_json", wraps=WorkRecord.model_validate_json
    ) as parse:
        for document, record in zip(documents, records, strict=True):
            assert cache.decode(document) == record
        assert cache.decode(documents[-1]) == records[-1]
        assert parse.call_count == 3
        assert cache.decode(documents[0]) == records[0]
        assert parse.call_count == 4


def test_sql_browse_preserves_accepted_facts_and_validates_current_documents(
    tmp_path: Path,
) -> None:
    from stove0_core import SqlAlchemyStateStore
    from stove0_operator_contracts import WorkPage, WorkView

    store = SqlAlchemyStateStore(f"sqlite+pysqlite:///{tmp_path / 'browse.sqlite3'}")
    work = _work()
    contract, descriptor = _observer()
    request, result = _observation(work, contract, descriptor)
    workflow = WorkflowPlan.seal(
        WorkflowPlanPayload(
            work=work,
            observations=(ContentObservationEvidence(request=request, result=result),),
            operation=OperationIdentityRef(id=_operation().id, sha256=_operation().contract_sha256),
            target_registration_id="fixture-target",
            target_descriptor_sha256=_target(_operation()).descriptor_sha256,
        )
    )
    record = store.create(
        WorkRecord(
            work=work,
            phase="planning",
            claim=ClaimBinding(claim_id=work.work_id, fence=1),
            workflow_plan=workflow,
        )
    )
    payload = store.list_work()
    payload.pop("_next_position")
    payload["next_page_token"] = None
    page = WorkPage.from_page(payload)
    projected = page.work[0]
    assert projected == WorkView.from_record(record)
    assert WorkPage.model_validate_json(page.model_dump_json()) == page
    # The projection retains accepted models rather than reconstructing their
    # complete fact corpus at each browse. Returned nested facts remain isolated.
    rows = payload["work"]
    assert isinstance(rows, list) and isinstance(rows[0], WorkRecord)
    assert projected.workflow_plan.observations[0] is rows[0].workflow_plan.observations[0]
    assert projected.workflow_plan.observations[0].result.facts is not None
    projected.workflow_plan.observations[0].result.facts["kind"] = "changed locally"
    assert store.load(record.work_id) == record

    # An unchanged row identity cannot make altered sealed evidence trustworthy.
    invalid = record.model_dump(mode="json", by_alias=True, exclude_none=True)
    invalid["workflow_plan"]["observations"][0]["result"]["facts"]["kind"] = "changed durably"
    with store.engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE stove0_work_records SET document_json = :document WHERE work_id = :work_id"
            ),
            {"document": json.dumps(invalid), "work_id": record.work_id},
        )
    with pytest.raises(ValidationError, match="observation facts digest"):
        store.list_work()


def test_unified_state_store_is_restart_safe_and_compare_and_swap(tmp_path: Path) -> None:
    from stove0_core import SqlAlchemyStateStore

    path = tmp_path / "private" / "stove0.sqlite3"
    store = SqlAlchemyStateStore(f"sqlite+pysqlite:///{path}")
    service = Stove0WorkService(store)
    created = service.create_or_resume(_work())
    claimed = service.bind_claim(
        created.work_id,
        claim_id=created.work_id,
        fence=1,
        expected_revision=created.revision,
    )

    restarted = SqlAlchemyStateStore(f"sqlite+pysqlite:///{path}")
    assert restarted.load(created.work_id) == claimed
    events = restarted.list_events().events
    assert [event.type for event in events] == [
        "io.riverhog.stove0.work.created",
        "io.riverhog.stove0.work.updated",
    ]
    assert events[0].payload["work_id"] == created.work_id
    assert events[1].payload["revision"] == claimed.revision

    with pytest.raises(ConcurrentWorkUpdate, match="stale stove0 work revision"):
        store.compare_and_swap(
            created.work_id,
            expected_revision=created.revision,
            replacement=claimed,
        )


def test_sql_runnable_scan_ignores_terminal_history_and_uses_a_keyset(
    tmp_path: Path,
) -> None:
    from stove0_core import SqlAlchemyStateStore

    store = SqlAlchemyStateStore(f"sqlite+pysqlite:///{tmp_path / 'scan.sqlite3'}")
    for index in range(250):
        identity = WorkIdentity.seal(
            WorkPayload(
                recipe=RecipeIdentityRef(id="fixture.recipe/v1", revision="1", sha256=_sha("3")),
                inputs=(
                    CollectionRootIdentityRef(
                        collection_id=str(index + 1),
                        archive_root_sha256=f"{index + 1:064x}",
                        artifact_set_identity=_sha("2"),
                    ),
                ),
            )
        )
        store.create(WorkRecord(work=identity, phase="canceled"))
    runnable = store.create(WorkRecord(work=_work()))

    records, cursor = store.scan_work(
        phases=("eligible",),
        after_work_id="",
        limit=1,
    )

    assert [(item.work_id, item.phase, item.revision) for item in records] == [
        (runnable.work_id, runnable.phase, runnable.revision)
    ]
    assert cursor == runnable.work_id


def test_sql_operational_retention_prunes_only_complete_expired_components(
    tmp_path: Path,
) -> None:
    from stove0_core import SqlAlchemyStateStore

    store = SqlAlchemyStateStore(f"sqlite+pysqlite:///{tmp_path / 'retention.sqlite3'}")
    service = Stove0WorkService(store)
    work = _work()
    parent = service.create_or_resume(work)
    parent = service.bind_claim(
        parent.work_id,
        claim_id=parent.work_id,
        fence=1,
        expected_revision=parent.revision,
    )
    parent = service.begin_planning(parent.work_id, expected_revision=parent.revision)
    decision = _branch_decision(work)
    parent = service.admit_branch_set(
        parent.work_id,
        decision,
        expected_revision=parent.revision,
    )
    child_id = decision.plan.branches[0].workflow_plan.work.work_id
    child = store.load(child_id)
    assert child is not None
    child = WorkRecord.model_validate(
        child.model_copy(update={"phase": "canceled", "revision": 2}).model_dump(mode="python")
    )
    store.compare_and_swap(
        child_id,
        expected_revision=1,
        replacement=child,
    )
    parent = WorkRecord.model_validate(
        parent.model_copy(update={"phase": "complete", "revision": parent.revision + 1}).model_dump(
            mode="python"
        )
    )
    store.compare_and_swap(
        parent.work_id,
        expected_revision=parent.revision - 1,
        replacement=parent,
    )
    old = "2000-01-01T00:00:00.000000000Z"
    cutoff = "2001-01-01T00:00:00.000000000Z"
    with store.engine.begin() as connection:
        connection.execute(
            text("UPDATE stove0_work_records SET updated_at = :old WHERE work_id = :work_id"),
            {"old": old, "work_id": parent.work_id},
        )

    retained = store.prune_operational_state(cutoff=cutoff)
    assert retained["work"] == 0
    assert store.load(parent.work_id) == parent
    assert store.load(child_id) == child
    selection = decision.selections[0]
    assert store.load_selection(selection.selection_sha256) == selection

    with store.engine.begin() as connection:
        connection.execute(
            text("UPDATE stove0_work_records SET updated_at = :old WHERE work_id = :work_id"),
            {"old": old, "work_id": child_id},
        )
        connection.execute(
            text("UPDATE stove0_lifecycle_events SET created_at = :old"),
            {"old": old},
        )

    pruned = store.prune_operational_state(cutoff=cutoff)
    assert pruned["work"] == 2
    assert pruned["work_bytes"] > 0
    assert pruned["selections"] == 1
    assert pruned["events"] > 0
    assert store.list_work()["work"] == []
    assert store.load_selection(selection.selection_sha256) is None


def test_sql_operational_retention_scans_bounded_pages_without_parsing_all_work(
    tmp_path: Path,
) -> None:
    from stove0_core import SqlAlchemyStateStore

    store = SqlAlchemyStateStore(f"sqlite+pysqlite:///{tmp_path / 'bounded-retention.sqlite3'}")
    for ordinal in range(250):
        work = WorkIdentity.seal(
            WorkPayload(
                recipe=RecipeIdentityRef(id="fixture.recipe/v1", revision="1", sha256=_sha("3")),
                inputs=(_root(),),
                effective_intent={"ordinal": ordinal},
            )
        )
        store.create(WorkRecord(work=work, phase="canceled"))
    old = "2000-01-01T00:00:00.000000000Z"
    cutoff = "2001-01-01T00:00:00.000000000Z"
    with store.engine.begin() as connection:
        connection.execute(text("UPDATE stove0_work_records SET updated_at = :old"), {"old": old})

    removed: list[int] = []
    with patch.object(
        WorkRecord,
        "model_validate_json",
        side_effect=AssertionError("retention must use relational projections"),
    ):
        for _ in range(3):
            result = store.prune_operational_state(cutoff=cutoff)
            assert result["work"] <= 100
            removed.append(result["work"])

    assert removed == [100, 100, 50]
    assert store.list_work()["work"] == []


def test_sql_branch_set_admission_is_restart_safe_and_exposes_exact_children(
    tmp_path: Path,
) -> None:
    from stove0_core import SqlAlchemyStateStore

    path = tmp_path / "private" / "stove0.sqlite3"
    store = SqlAlchemyStateStore(f"sqlite+pysqlite:///{path}")
    service = Stove0WorkService(store)
    work = _work()
    record = service.create_or_resume(work)
    record = service.bind_claim(
        record.work_id,
        claim_id=record.work_id,
        fence=1,
        expected_revision=record.revision,
    )
    record = service.begin_planning(record.work_id, expected_revision=record.revision)
    decision = _branch_decision(work)

    admitted = service.admit_branch_set(
        record.work_id,
        decision,
        expected_revision=record.revision,
    )

    assert admitted.phase == "coordinating"
    assert admitted.branch_set_plan == decision.plan
    branch = decision.plan.branches[0]
    child = store.load(branch.workflow_plan.work.work_id)
    assert child == WorkRecord(
        work=branch.workflow_plan.work,
        workflow_plan=branch.workflow_plan,
    )
    selection = decision.selections[0]
    assert store.load_selection(selection.selection_sha256) == selection

    restarted = SqlAlchemyStateStore(f"sqlite+pysqlite:///{path}")
    assert restarted.load(work.work_id) == admitted
    assert restarted.load(child.work_id) == child
    assert restarted.load_selection(selection.selection_sha256) == selection


def test_sql_selection_restart_preserves_canonical_artifact_order(tmp_path: Path) -> None:
    from stove0_core import SqlAlchemyStateStore

    path = tmp_path / "private" / "stove0.sqlite3"
    selection = ArtifactSelection.seal(
        (
            WorkArtifactSubject(
                id="a-request-first",
                role="fixture.source/v1",
                collection=_root(),
                artifact_id=_member_id("z-collection-last.bin"),
                bytes=str(1),
                sha256=_sha("4"),
            ),
            WorkArtifactSubject(
                id="z-request-last",
                role="fixture.source/v1",
                collection=_root(),
                artifact_id=_member_id("a-collection-first.bin"),
                bytes=str(1),
                sha256=_sha("5"),
            ),
        )
    )
    store = SqlAlchemyStateStore(f"sqlite+pysqlite:///{path}")
    store.retain_selection(selection)

    restarted = SqlAlchemyStateStore(f"sqlite+pysqlite:///{path}")

    assert restarted.load_selection(selection.selection_sha256) == selection


def test_sql_nested_admission_atomically_normalizes_coordinator_and_leaf_records(
    tmp_path: Path,
) -> None:
    from stove0_core import SqlAlchemyStateStore

    path = tmp_path / "private" / "stove0.sqlite3"
    store = SqlAlchemyStateStore(f"sqlite+pysqlite:///{path}")
    service = Stove0WorkService(store)
    work = _work()
    record = service.create_or_resume(work)
    record = service.bind_claim(
        record.work_id,
        claim_id=record.work_id,
        fence=1,
        expected_revision=record.revision,
    )
    record = service.begin_planning(record.work_id, expected_revision=record.revision)
    decision = _nested_branch_decision(work)

    admitted = service.admit_branch_set(
        record.work_id,
        decision,
        expected_revision=record.revision,
    )

    nested = decision.plan.branches[0]
    assert isinstance(nested, CoordinationBranchPlan)
    child_plan = decision.branch_set_documents[nested.branch_set_sha256]
    coordinator = store.load(nested.work.work_id)
    assert coordinator == WorkRecord(work=nested.work, branch_set_plan=child_plan)
    leaf = child_plan.branches[0]
    assert isinstance(leaf, BranchPlan)
    leaf_record = store.load(leaf.workflow_plan.work.work_id)
    assert leaf_record == WorkRecord(
        work=leaf.workflow_plan.work,
        workflow_plan=leaf.workflow_plan,
    )
    coordinator = service.bind_claim(
        coordinator.work_id,
        claim_id=coordinator.work_id,
        fence=1,
        expected_revision=coordinator.revision,
    )
    coordinator = service.activate_preplanned_coordination(
        coordinator.work_id,
        expected_revision=coordinator.revision,
    )
    assert coordinator.phase == "coordinating"

    restarted = SqlAlchemyStateStore(f"sqlite+pysqlite:///{path}")
    assert restarted.load(work.work_id) == admitted
    assert restarted.load(coordinator.work_id) == coordinator
    assert restarted.load(leaf_record.work_id) == leaf_record
    assert (
        restarted.load_selection(decision.selections[0].selection_sha256)
        == (decision.selections[0])
    )


def test_sql_branch_set_admission_rolls_back_every_document_on_child_conflict(
    tmp_path: Path,
) -> None:
    from stove0_core import SqlAlchemyStateStore

    path = tmp_path / "private" / "stove0.sqlite3"
    store = SqlAlchemyStateStore(f"sqlite+pysqlite:///{path}")
    service = Stove0WorkService(store)
    work = _work()
    record = service.create_or_resume(work)
    record = service.bind_claim(
        record.work_id,
        claim_id=record.work_id,
        fence=1,
        expected_revision=record.revision,
    )
    record = service.begin_planning(record.work_id, expected_revision=record.revision)
    decision = _branch_decision(work)
    branch = decision.plan.branches[0]
    conflicting_plan = WorkflowPlanIntent(
        operation=branch.workflow_plan.operation,
        target_registration_id=branch.workflow_plan.target_registration_id,
        target_descriptor_sha256=branch.workflow_plan.target_descriptor_sha256,
        requested_target_options={"conflict": True},
        source_collection_retirement_policy="retain",
    ).materialize(work=branch.workflow_plan.work)
    store.create(WorkRecord(work=branch.workflow_plan.work, workflow_plan=conflicting_plan))

    with pytest.raises(ConcurrentWorkUpdate, match="branch child identity was reused"):
        service.admit_branch_set(
            record.work_id,
            decision,
            expected_revision=record.revision,
        )

    parent = store.load(record.work_id)
    assert parent is not None and parent.phase == "planning"
    assert parent.branch_set_plan is None
    selection = decision.selections[0]
    assert store.load_selection(selection.selection_sha256) is None


def test_sealed_output_pages_bind_exact_relationships_and_current_fence() -> None:
    store, service, record, _operation, callbacks, access = _queued_target_callback_execution()
    assert record.controller_evidence is not None
    job_id = record.controller_evidence.execution_envelope.execution_envelope_sha256
    source = callbacks.input_page(
        access.token, job_id=job_id, continuation=None, limit=1
    ).artifacts[0]
    outputs = tuple(
        OutputArtifact.model_validate(
            {
                "id": key,
                "role": "fixture.output/v1",
                "artifact_id": _member_id(key),
                "bytes": "1",
                "sha256": _sha("5"),
                **relations,
            }
        )
        for key, relations in (
            ("bundle", {"reconstructs_output_id": "media"}),
            ("media", {}),
            ("xmp", {"describes_output_id": "media"}),
        )
    )
    for output in outputs:
        callbacks.declare_output(access.token, job_id=job_id, output=output)
        callbacks.declare_source_edge(
            access.token,
            job_id=job_id,
            edge=OutputSourceEdge(output_id=output.id, input_id=source.id),
        )
    callbacks.declare_disposition(
        access.token,
        job_id=job_id,
        disposition=InputDispositionDeclaration(
            input_id=source.id,
            status="transformed",
        ),
    )
    with pytest.raises(ValueError, match="exact sealed"):
        callbacks.output_page(
            access.token,
            job_id=job_id,
            production_sha256=_sha("f"),
            after_id=None,
            limit=1,
        )
    production = _seal_production(callbacks, access.token, job_id)
    after_id = None
    recovered = []
    while True:
        page = callbacks.output_page(
            access.token,
            job_id=job_id,
            production_sha256=production.production_sha256,
            after_id=after_id,
            limit=1,
        )
        assert page.after_id == after_id
        assert page.production_sha256 == production.production_sha256
        recovered.extend(page.artifacts)
        if page.complete:
            break
        after_id = page.next_after_id
    assert tuple(recovered) == outputs
    assert production.outputs == OutputArtifactSetIdentity.seal(tuple(recovered))
    with pytest.raises(ValueError, match="exact sealed"):
        callbacks.output_page(
            access.token,
            job_id=job_id,
            production_sha256=_sha("f"),
            after_id=None,
            limit=1,
        )
    current = store.load(record.work_id)
    assert current is not None
    service.fail(
        record.work_id,
        failure=WorkFailure(code="fixture-revoked", message="revoked", retryable=False),
        expected_revision=current.revision,
    )
    with pytest.raises(PermissionError, match="stale"):
        callbacks.output_page(
            access.token,
            job_id=job_id,
            production_sha256=production.production_sha256,
            after_id="media",
            limit=1,
        )


def test_sealed_production_rejects_relationship_to_unproduced_output() -> None:
    _store, _service, record, _operation, callbacks, access = _queued_target_callback_execution()
    assert record.controller_evidence is not None
    job_id = record.controller_evidence.execution_envelope.execution_envelope_sha256
    output = OutputArtifact.model_validate(
        {
            "id": "metadata",
            "role": "fixture.output/v1",
            "artifact_id": _member_id("metadata"),
            "bytes": "1",
            "sha256": _sha("5"),
            "describes_output_id": "missing-output",
        }
    )
    callbacks.declare_output(access.token, job_id=job_id, output=output)
    with pytest.raises(ValueError, match="unproduced"):
        _seal_production(callbacks, access.token, job_id)
