from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from riverhog_protocol import (
    Conflict,
    ImmutableFileIdentityDocument,
    NotFound,
    PortableCollectionHeader,
    PortableCollectionInventoryAuthority,
    PortableCollectionInventoryPage,
)
from riverhog_protocol.collection_workflow_transport import (
    ArtifactDispositionSetDocument,
    ArtifactDispositionSetIdentityDocument,
    ConsiderationEvidenceOutDocument,
    ExactSetIdentityDocument,
    ProcessingClaimPlanDocument,
    ProcessingOutcomeIdentityDocument,
)
from riverhog_protocol.collection_workflows import (
    ArtifactDispositionSetIdentity,
    CollectionArtifactIdentity,
    CollectionDerivation,
    CollectionProcessingOutcomeIdentity,
    CollectionRootIdentity,
    processing_outcome_set_identity,
)
from riverhog_protocol.collection_workflows import (
    canonical_json_sha256 as riverhog_canonical_json_sha256,
)
from stove0_core import (
    ClaimBinding,
    InMemoryWorkStore,
    Stove0Coordinator,
    Stove0RiverhogClient,
    Stove0WorkService,
    WorkRecord,
)
from stove0_core.riverhog import _no_output_discard_approval
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ContentObservationRequest,
    ContentObservationRequestPayload,
    ContentObservationResult,
    ContentObservationResultPayload,
    ObserverImplementation,
)
from stove0_operator_contracts import WorkView
from stove0_protocol import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    ArtifactSelection,
    BranchPlan,
    BranchSetPlan,
    BranchSettlement,
    CollectionRootIdentityRef,
    ControllerEvidence,
    ControllerEvidencePayload,
    ExecutionEnvelope,
    ExecutionEnvelopePayload,
    JsonSchemaValidationProfile,
    OperationIdentityRef,
    PreviewOutcome,
    RecipeIdentityRef,
    TargetPlanBinding,
    WorkArtifactSubject,
    WorkflowPlan,
    WorkflowPlanIntent,
    WorkflowPlanPayload,
    WorkflowPreview,
    WorkflowPreviewPayload,
    WorkflowPreviewRequest,
    WorkflowPreviewRequestPayload,
    WorkIdentity,
    WorkPayload,
    canonical_json_sha256,
    evaluate_branch_set,
)
from stove0_recipe_config import (
    ArtifactFactBinding,
    FactPredicate,
    RecipeNoAction,
    RecipeSourceLossEvidenceSlot,
    RecipeSourceLossRule,
)
from stove0_target_protocol import (
    AcceptedTargetJob,
    ExternalEffectReceipt,
    ExternalEffectReceiptPayload,
    OutputArtifactSetIdentity,
    TargetInputAuthority,
    TargetJobDeclaration,
    TargetOutputBindingSetIdentity,
    TargetProductionAuthority,
    TargetProductionAuthorityPayload,
    TargetSettlementAuthority,
    TargetSettlementAuthorityPayload,
)
from stove0_target_support import (
    EffectPlan,
    EffectPlanPayload,
    InputArtifactContract,
    OperationContract,
    OperationContractPayload,
    OutputArtifact,
    OutputArtifactContract,
    OutputCollectionRef,
    TargetExecutionEvidence,
    TargetFailure,
    TargetJobStatus,
    TargetProgress,
    TransformPlan,
    TransformPlanPayload,
)


def _sha(character: str) -> str:
    return character * 64


def _claim_id() -> str:
    return _sha("c")


class _AttrDict(dict[str, Any]):
    def __getattr__(self, name: str) -> Any:
        return self[name]


def _input_selection(work: WorkIdentity) -> ArtifactSelection:
    return ArtifactSelection.seal(
        (
            WorkArtifactSubject(
                id="source",
                role="fixture.source/v1",
                collection=work.inputs[0],
                path="source/input.bin",
                bytes=str(12),
                sha256=_sha("e"),
            ),
        )
    )


def _operation() -> OperationContract:
    schema = JsonSchemaValidationProfile.from_schema(
        "fixture.empty/v1",
        {"type": "object", "additionalProperties": False},
    )
    return OperationContract.seal(
        OperationContractPayload(
            id="fixture.copy/v1",
            source_collection_retirement_permitted=True,
            intent_schema=schema,
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
        )
    )


def _output_artifact() -> OutputArtifact:
    return OutputArtifact(
        id="output",
        role="fixture.output/v1",
        path="output/result.bin",
        bytes=str(12),
        sha256=_sha("9"),
    )


def _disposition_set() -> ArtifactDispositionSetIdentity:
    return ArtifactDispositionSetIdentity(
        disposition_count=1,
        output_edge_count=1,
        output_artifact_count=1,
        sha256=_sha("6"),
    )


def _authorities(
    source_collection_retirement_policy: str = "retain",
) -> tuple[WorkIdentity, WorkflowPlan, TransformPlan, ControllerEvidence]:
    work = WorkIdentity.seal(
        WorkPayload(
            recipe=RecipeIdentityRef(id="fixture.recipe/v1", revision="1", sha256=_sha("1")),
            inputs=(
                CollectionRootIdentityRef(
                    collection_id=str(1),
                    archive_root_sha256=_sha("2"),
                    artifact_set_identity=_sha("3"),
                ),
            ),
        )
    )
    workflow = WorkflowPlan.seal(
        WorkflowPlanPayload(
            work=work,
            operation=OperationIdentityRef(
                id="fixture.copy/v1", sha256=_operation().contract_sha256
            ),
            target_registration_id="fixture-target",
            target_descriptor_sha256=_sha("5"),
            source_collection_retirement_policy=source_collection_retirement_policy,
        )
    )
    selection = _input_selection(work)
    target_plan = TransformPlan.seal(
        TransformPlanPayload(
            invocation_sha256=workflow.workflow_plan_sha256,
            target_implementation_id="fixture.target/v1",
            target_descriptor_sha256=_sha("5"),
            operation_id="fixture.copy/v1",
            operation_contract_sha256=_operation().contract_sha256,
            inputs=TargetInputAuthority.from_selection(selection),
            intent={},
        )
    )
    binding = TargetPlanBinding(
        protocol=target_plan.protocol,
        target_implementation_id="fixture.target/v1",
        target_descriptor_sha256=_sha("5"),
        operation_contract_sha256=_operation().contract_sha256,
        plan=target_plan.binding_document(),
        plan_sha256=target_plan.plan_sha256,
    )
    envelope = ExecutionEnvelope.seal(
        ExecutionEnvelopePayload(
            claim_id=_claim_id(),
            fence=1,
            workflow_plan=workflow,
            target_plan=binding,
        )
    )
    evidence = ControllerEvidence.seal(ControllerEvidencePayload(execution_envelope=envelope))
    return work, workflow, target_plan, evidence


def _effect_operation() -> OperationContract:
    schema = JsonSchemaValidationProfile.from_schema(
        "fixture.empty/v1", {"type": "object", "additionalProperties": False}
    )
    return OperationContract.seal(
        OperationContractPayload(
            id="fixture.effect/v1",
            result_kind="external-effect",
            source_collection_retirement_permitted=True,
            intent_schema=schema,
            intent_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            effect_receipt_schema=schema,
            inputs=(InputArtifactContract(role="fixture.source/v1"),),
            outputs=(),
        )
    )


def _effect_authorities(
    policy: str = "retain",
) -> tuple[WorkIdentity, WorkflowPlan, EffectPlan, ControllerEvidence]:
    work, _workflow, _target_plan, _evidence = _authorities()
    workflow = WorkflowPlan.seal(
        WorkflowPlanPayload(
            work=work,
            result_kind="external-effect",
            operation=OperationIdentityRef(
                id="fixture.effect/v1", sha256=_effect_operation().contract_sha256
            ),
            target_registration_id="fixture-effect-target",
            target_descriptor_sha256=_sha("5"),
            source_collection_retirement_policy=policy,
        )
    )
    target_plan = EffectPlan.seal(
        EffectPlanPayload(
            invocation_sha256=workflow.workflow_plan_sha256,
            target_implementation_id="fixture.effect-target/v1",
            target_descriptor_sha256=_sha("5"),
            operation_id="fixture.effect/v1",
            operation_contract_sha256=_effect_operation().contract_sha256,
            inputs=TargetInputAuthority.from_selection(_input_selection(work)),
            intent={},
        )
    )
    binding = TargetPlanBinding(
        protocol=target_plan.protocol,
        target_implementation_id=target_plan.target_implementation_id,
        target_descriptor_sha256=target_plan.target_descriptor_sha256,
        operation_contract_sha256=target_plan.operation_contract_sha256,
        plan=target_plan.binding_document(),
        plan_sha256=target_plan.plan_sha256,
    )
    envelope = ExecutionEnvelope.seal(
        ExecutionEnvelopePayload(
            claim_id=_claim_id(),
            fence=1,
            workflow_plan=workflow,
            target_plan=binding,
        )
    )
    evidence = ControllerEvidence.seal(ControllerEvidencePayload(execution_envelope=envelope))
    return work, workflow, target_plan, evidence


class FixtureApi:
    base_url = "https://riverhog.invalid"
    allow_insecure_http = False

    def __init__(self) -> None:
        self.execution_id: str | None = None
        self.derivation: CollectionDerivation | None = None
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.fence = 1
        self.claim_state = "active"
        self.retirement_state = "retiring"
        self.deletion_blockers: list[str] = []
        self.expire_renewal = False
        self.deleted: set[int] = set()
        self.processing_outcomes: list[dict[str, object]] = []
        self.plan: ProcessingClaimPlanDocument | None = None
        self.effect_settlement_sha256: str | None = None
        self.effect_document: dict[str, object] | None = None
        self.lose_effect_ack = False
        self.dispositions: list[dict[str, object]] = []

    def create_or_resume_processing_claim(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("claim", kwargs))
        if self.expire_renewal:
            self.fence += 1
            self.expire_renewal = False
        self.deleted: set[int] = set()
        return {
            "id": _claim_id(),
            "fence": self.fence,
            "work_id": kwargs["work_id"],
            "state": self.claim_state,
        }

    def record_processing_claim_consideration_evidence(
        self, claim_id: str, **kwargs: Any
    ) -> ConsiderationEvidenceOutDocument:
        self.calls.append(("consideration_evidence", {"claim_id": claim_id, **kwargs}))
        assert riverhog_canonical_json_sha256(kwargs["document"]) == kwargs["sha256"]
        return ConsiderationEvidenceOutDocument(sha256=kwargs["sha256"])

    def renew_processing_claim(self, claim_id: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("renew", {"claim_id": claim_id, **kwargs}))
        if self.expire_renewal:
            raise Conflict("claim lease expired")
        return {"id": claim_id, "fence": kwargs["fence"]}

    def restart_processing_claim(self, claim_id: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("restart", {"claim_id": claim_id, **kwargs}))
        self.fence += 1
        self.execution_id = None
        return {"id": claim_id, "fence": self.fence, "state": "active"}

    def create_processing_capability(self, claim_id: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("capability", {"claim_id": claim_id, **kwargs}))
        actions = tuple(sorted(kwargs["actions"]))
        principal = (
            f"processing:{self.execution_id}"
            if "write-output" in actions
            else f"claim:{claim_id}"
            if str(kwargs["audience"]).startswith("stove0.target/")
            else f"observe:{claim_id}:{kwargs['fence']}"
        )
        return {
            "claim_id": claim_id,
            "fence": str(kwargs["fence"]),
            "audience": kwargs["audience"],
            "actions": list(actions),
            "principal_id": principal,
            "token": f"secret-{len(self.calls)}",
        }

    def seal_processing_claim_plan(self, claim_id: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("seal", {"claim_id": claim_id, **kwargs}))
        self.execution_id = kwargs["execution_id"]
        self.plan = ProcessingClaimPlanDocument(
            execution_id=self.execution_id,
            controller_evidence=kwargs["controller_evidence"],
            controller_evidence_sha256=kwargs["controller_evidence_sha256"],
            operation={"id": kwargs["operation_id"], "sha256": kwargs["operation_sha256"]},
            result_kind=kwargs["result_kind"],
            operation_contract=kwargs["operation_contract"],
            inputs={"count": "1", "sha256": _sha("d")},
            artifacts={"count": "1", "total_bytes": "12", "sha256": _sha("e")},
            source_collection_retirement_policy=kwargs["source_collection_retirement_policy"],
            source_collection_retirement_grace_seconds=str(
                kwargs["source_collection_retirement_grace_seconds"]
            ),
            sealed_at="2026-09-26T00:00:00.000000000Z",
        )
        return {
            "id": claim_id,
            "fence": kwargs["fence"],
            "plan": {"execution_id": self.execution_id},
        }

    def settle_processing_claim(self, claim_id: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("settle", {"claim_id": claim_id, **kwargs}))
        self.derivation = CollectionDerivation.from_mapping(kwargs["derivation"])
        return {
            "id": claim_id,
            "fence": kwargs["fence"],
            "state": "settled",
        }

    def get_processing_claim(self, claim_id: str) -> _AttrDict:
        self.calls.append(("get-claim", {"claim_id": claim_id}))
        return _AttrDict(
            id=claim_id,
            fence=self.fence,
            state=self.claim_state,
            outcomes=self.processing_outcomes,
            plan=self.plan,
            effect_settlement_sha256=self.effect_settlement_sha256,
        )

    def settle_processing_claim_effect(self, claim_id: str, **kwargs: Any) -> _AttrDict:
        self.calls.append(("settle-effect", {"claim_id": claim_id, **kwargs}))
        document = kwargs["settlement"]
        if self.effect_document is not None:
            assert document == self.effect_document
        self.effect_document = document
        self.effect_settlement_sha256 = canonical_json_sha256(document)
        self.claim_state = "settled"
        if self.lose_effect_ack:
            self.lose_effect_ack = False
            raise ConnectionError("lost ACK after durable Riverhog commit")
        return self.get_processing_claim(claim_id)

    def append_processing_claim_outcomes(self, claim_id: str, **kwargs: Any) -> _AttrDict:
        self.calls.append(("append-outcomes", {"claim_id": claim_id, **kwargs}))
        assert kwargs["outcomes"] == self.processing_outcomes
        return self.get_processing_claim(claim_id)

    def settle_processing_claim_outcomes(self, claim_id: str, **kwargs: Any) -> _AttrDict:
        self.calls.append(("settle-outcomes", {"claim_id": claim_id, **kwargs}))
        identity = ExactSetIdentityDocument(
            count=str(kwargs["outcomes_count"]), sha256=kwargs["outcomes_sha256"]
        )
        return _AttrDict(
            id=claim_id,
            fence=kwargs["fence"],
            state="settled",
            outcomes=_AttrDict(identity=identity),
        )

    def list_processing_claim_outcomes(
        self, claim_id: str, *, identity_sha256: str, start_ordinal: int
    ) -> SimpleNamespace:
        assert claim_id == _claim_id() and start_ordinal == 0
        items = [
            CollectionProcessingOutcomeIdentity.from_mapping(item)
            for item in self.processing_outcomes
        ]
        identity = processing_outcome_set_identity(items)
        assert identity_sha256 == identity["sha256"]
        return SimpleNamespace(
            identity=ExactSetIdentityDocument.model_validate(identity),
            start_ordinal=0,
            outcomes=[
                ProcessingOutcomeIdentityDocument.model_validate(item)
                for item in self.processing_outcomes
            ],
            next_ordinal=None,
        )

    def abandon_processing_claim(self, claim_id: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("abandon", {"claim_id": claim_id, **kwargs}))
        return {
            "id": claim_id,
            "fence": kwargs["fence"],
            "state": "abandoned",
        }

    def get_collection(self, collection_id: int) -> dict[str, Any]:
        assert self.derivation is not None
        return {
            "id": str(collection_id),
            "archive_root_sha256": _sha("7"),
            "artifact_set_identity": _sha("8"),
        }

    def get_processing_claim_dispositions(self, claim_id: str) -> ArtifactDispositionSetDocument:
        identity = (
            ArtifactDispositionSetIdentity(1, 0, 0, _sha("6"))
            if self.plan is not None and self.plan.result_kind != "collection"
            else _disposition_set()
        )
        return ArtifactDispositionSetDocument(
            claim_id=claim_id,
            state="sealed",
            disposition_count=str(identity.disposition_count),
            output_edge_count=str(identity.output_edge_count),
            output_artifact_count=str(identity.output_artifact_count),
            identity=ArtifactDispositionSetIdentityDocument.model_validate(identity.as_dict()),
        )

    def list_processing_claim_artifacts(self, claim_id: str, **kwargs: Any) -> Any:
        assert self.plan is not None and kwargs["start_ordinal"] == 0
        assert kwargs["identity_sha256"] == self.plan.artifacts.sha256
        return SimpleNamespace(
            start_ordinal=0,
            identity=SimpleNamespace(sha256=self.plan.artifacts.sha256),
            artifacts=(
                SimpleNamespace(
                    collection=SimpleNamespace(
                        collection_id=1,
                        archive_root_sha256=_sha("2"),
                        artifact_set_identity=_sha("3"),
                    ),
                    path="source/input.bin",
                    bytes=12,
                    sha256=_sha("e"),
                ),
            ),
            next_ordinal=None,
        )

    def record_processing_claim_dispositions(self, claim_id: str, **kwargs: Any) -> None:
        self.dispositions.extend(kwargs["dispositions"])

    def seal_processing_claim_dispositions(
        self, claim_id: str, **kwargs: Any
    ) -> ArtifactDispositionSetDocument:
        return self.get_processing_claim_dispositions(claim_id)

    def get_portable_collection_inventory(
        self,
        collection_id: int,
        *,
        cursor: str | None,
        limit: int,
        inventory_identity: str | None,
    ) -> PortableCollectionInventoryPage:
        assert cursor is None
        assert limit == 100
        assert inventory_identity is None
        return PortableCollectionInventoryPage(
            authority=PortableCollectionInventoryAuthority(
                header=PortableCollectionHeader(
                    collection=str(collection_id),
                    artifact_set_identity=_sha("8"),
                    encryption_format="age/v1",
                    passphrase_id="fixture-passphrase",
                    provenance_mode="omitted",
                ),
                inventory_identity=_sha("5"),
                file_count="1",
                file_bytes="12",
            ),
            files=[
                ImmutableFileIdentityDocument(
                    path="output/result.bin",
                    bytes="12",
                    sha256=_sha("9"),
                )
            ],
            complete=True,
        )

    def get_collection_derivation(self, collection_id: int) -> dict[str, Any]:
        assert self.derivation is not None
        return {
            "collection_id": str(collection_id),
            "document_sha256": self.derivation.sha256,
            "derivation": self.derivation.as_dict(),
        }

    def begin_source_collection_retirement(self, claim_id: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("retirement", {"claim_id": claim_id, **kwargs}))
        return {"id": claim_id, "fence": kwargs["fence"], "state": self.retirement_state}

    def plan_collection_deletion(self, collection_id: int, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("deletion-plan", {"collection_id": collection_id, **kwargs}))
        if collection_id in self.deleted:
            raise NotFound("collection is already absent")
        return {
            "status": "blocked" if self.deletion_blockers else "ready",
            "blockers": self.deletion_blockers,
            "challenge": None if self.deletion_blockers else "delete-me",
        }

    def delete_collection(self, collection_id: int, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("delete", {"collection_id": collection_id, **kwargs}))
        self.deleted.add(collection_id)
        return {"status": "deleted"}

    def release_processing_claim(self, claim_id: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("release", {"claim_id": claim_id, **kwargs}))
        return {"id": claim_id, "fence": kwargs["fence"], "state": "released"}


class PagedInventoryFixtureApi(FixtureApi):
    def get_portable_collection_inventory(
        self,
        collection_id: int,
        *,
        cursor: str | None,
        limit: int,
        inventory_identity: str | None,
    ) -> PortableCollectionInventoryPage:
        assert limit == 1
        authority = PortableCollectionInventoryAuthority(
            header=PortableCollectionHeader(
                collection=str(collection_id),
                artifact_set_identity=_sha("8"),
                encryption_format="age/v1",
                passphrase_id="fixture-passphrase",
                provenance_mode="omitted",
            ),
            inventory_identity=_sha("5"),
            file_count="2",
            file_bytes="13",
        )
        if cursor is None:
            assert inventory_identity is None
            return PortableCollectionInventoryPage(
                authority=authority,
                files=[
                    ImmutableFileIdentityDocument(
                        path="riverhog/derivation.json",
                        bytes="1",
                        sha256=_sha("1"),
                    )
                ],
                next_cursor="second-page",
                complete=False,
            )
        assert cursor == "second-page"
        assert inventory_identity == authority.inventory_identity
        return PortableCollectionInventoryPage(
            authority=authority,
            files=[
                ImmutableFileIdentityDocument(
                    path="output/result.bin",
                    bytes="12",
                    sha256=_sha("9"),
                )
            ],
            complete=True,
        )


def _verifying_record(
    work: WorkIdentity,
    workflow: WorkflowPlan,
    evidence: ControllerEvidence,
) -> WorkRecord:
    execution_id = evidence.execution_envelope.execution_envelope_sha256
    controller_document = evidence.model_dump(mode="json", by_alias=True, exclude_none=True)
    output = _output_artifact()
    disposition_set = _disposition_set()
    derivation = CollectionDerivation(
        execution_id=execution_id,
        claim_id=evidence.execution_envelope.claim_id,
        fence=1,
        recipe=work.recipe.to_identity(),
        operation=workflow.operation.to_identity(),
        input_set_sha256=_sha("d"),
        artifact_set_sha256=_sha("e"),
        execution_envelope_sha256=execution_id,
        execution_sha256=_sha("a"),
        controller_evidence=controller_document,
        controller_evidence_sha256=riverhog_canonical_json_sha256(controller_document),
        disposition_set=disposition_set,
    )
    output_ref = OutputCollectionRef(
        collection_id=str(7),
        archive_root_sha256=_sha("7"),
        artifact_set_identity=_sha("8"),
        derivation_sha256=derivation.sha256,
    )
    production = TargetProductionAuthority.seal(
        TargetProductionAuthorityPayload(
            job_id=execution_id,
            plan_sha256=evidence.execution_envelope.target_plan.plan_sha256,
            outputs=OutputArtifactSetIdentity.seal((output,)),
            disposition_count=1,
            disposition_sha256=_sha("f"),
            source_edge_count=1,
            source_edge_sha256=_sha("0"),
            riverhog_disposition_set=disposition_set,
        )
    )
    status = TargetJobStatus(
        job_id=execution_id,
        state="succeeded",
        attempt=1,
        request_sha256=_sha("b"),
        plan_sha256=evidence.execution_envelope.target_plan.plan_sha256,
        progress=TargetProgress(phase="done", completed=1, total=1),
        production=production,
        output_collection=output_ref,
        execution_evidence=TargetExecutionEvidence(
            target_descriptor_sha256=_sha("5"),
            operation_contract_sha256=workflow.operation.sha256,
            plan_sha256=evidence.execution_envelope.target_plan.plan_sha256,
            execution_sha256=_sha("a"),
        ),
        derivation=derivation.as_dict(),
    )
    return WorkRecord(
        work=work,
        phase="verifying",
        claim=ClaimBinding(claim_id=evidence.execution_envelope.claim_id, fence=1),
        workflow_plan=workflow,
        controller_evidence=evidence,
        target_status=status,
        output=output_ref,
    )


def test_riverhog_adapter_uses_scoped_capabilities_and_verifies_settlement() -> None:
    work, workflow, target_plan, evidence = _authorities()
    api = FixtureApi()
    state = InMemoryWorkStore()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed", state=state)

    claim = client.acquire_claim(work)
    assert claim == ClaimBinding(claim_id=_claim_id(), fence=1)
    assert client.renew_claim(work, claim) == claim
    inputs = _input_selection(work).artifacts
    client.seal_execution(claim, evidence, workflow, target_plan, inputs, _operation())
    authority = client.target_authority(claim, evidence, target_plan, inputs)
    assert authority.declared_workspace_protection == "memory-backed"
    assert authority.runtime.capability_token.startswith("secret-")

    record = _verifying_record(work, workflow, evidence)
    state.create(record)
    assert record.target_status is not None and record.target_status.production is not None
    job_id = record.target_status.production.job_id
    state.record_target_output(record.work_id, job_id, _output_artifact())
    output, settlement = client.verify_and_settle(record)
    assert output == record.output
    assert settlement is not None
    assert settlement.production_sha256 == record.target_status.production.production_sha256
    assert any(name == "settle" for name, _payload in api.calls)


def test_post_root_settlement_restarts_from_bounded_portable_inventory_progress() -> None:
    work, workflow, _target_plan, evidence = _authorities()
    api = PagedInventoryFixtureApi()
    state = InMemoryWorkStore()
    record = _verifying_record(work, workflow, evidence)
    state.create(record)
    assert record.target_status is not None and record.target_status.production is not None
    job_id = record.target_status.production.job_id
    state.record_target_output(record.work_id, job_id, _output_artifact())

    first = Stove0RiverhogClient(
        api, declared_workspace_protection="memory-backed", state=state, authority_batch_size=1
    )
    output, settlement = first.verify_and_settle(record)

    assert output == record.output
    assert settlement is None
    checkpoint = state.load_target_settlement_seal(record.work_id, job_id)
    assert checkpoint is not None and checkpoint.checkpoint is not None
    assert checkpoint.checkpoint.inventory_cursor == "second-page"
    assert checkpoint.checkpoint.artifact_count == 0

    restarted = Stove0RiverhogClient(
        api, declared_workspace_protection="memory-backed", state=state, authority_batch_size=1
    )
    replayed_output, replayed_settlement = restarted.verify_and_settle(record)

    assert replayed_output == output
    assert replayed_settlement is not None
    assert replayed_settlement.output_collection == output
    sealed = state.load_target_settlement_seal(record.work_id, job_id)
    assert sealed is not None and sealed.state == "sealed"
    assert sealed.settlement == replayed_settlement


def test_post_root_settlement_fails_closed_on_non_bijective_output() -> None:
    work, workflow, _target_plan, evidence = _authorities()
    api = FixtureApi()
    state = InMemoryWorkStore()
    record = _verifying_record(work, workflow, evidence)
    state.create(record)
    assert record.target_status is not None and record.target_status.production is not None
    state.record_target_output(
        record.work_id,
        record.target_status.production.job_id,
        _output_artifact().model_copy(update={"sha256": _sha("a")}),
    )
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed", state=state)

    with pytest.raises(RuntimeError, match="artifact differs"):
        client.verify_and_settle(record)


def _effect_record(
    work: WorkIdentity,
    workflow: WorkflowPlan,
    target_plan: EffectPlan,
    evidence: ControllerEvidence,
) -> WorkRecord:
    declaration = TargetJobDeclaration(
        job_id=evidence.execution_envelope.execution_envelope_sha256,
        claim_id=evidence.execution_envelope.claim_id,
        fence=1,
        controller_evidence=evidence,
        plan=target_plan,
        declared_workspace_protection="memory-backed",
    )
    accepted = AcceptedTargetJob(
        declaration=declaration,
        request_sha256=canonical_json_sha256(
            declaration.model_dump(mode="json", exclude_none=True)
        ),
    )
    receipt = ExternalEffectReceipt.seal(
        ExternalEffectReceiptPayload(
            job_id=declaration.job_id,
            request_sha256=accepted.request_sha256,
            target_descriptor_sha256=target_plan.target_descriptor_sha256,
            operation_contract_sha256=target_plan.operation_contract_sha256,
            plan_sha256=target_plan.plan_sha256,
            execution_sha256=_sha("a"),
            result={},
        )
    )
    status = TargetJobStatus(
        protocol=target_plan.protocol,
        job_id=declaration.job_id,
        state="succeeded",
        attempt=1,
        request_sha256=accepted.request_sha256,
        plan_sha256=target_plan.plan_sha256,
        progress=TargetProgress(phase="done", completed=1, total=1),
        effect_receipt=receipt,
        execution_evidence=TargetExecutionEvidence(
            target_descriptor_sha256=target_plan.target_descriptor_sha256,
            operation_contract_sha256=target_plan.operation_contract_sha256,
            plan_sha256=target_plan.plan_sha256,
            execution_sha256=_sha("a"),
        ),
    )
    return WorkRecord(
        work=work,
        phase="verifying",
        claim=ClaimBinding(claim_id=declaration.claim_id, fence=1),
        workflow_plan=workflow,
        target_plan=target_plan,
        controller_evidence=evidence,
        target_request=accepted,
        target_status=status,
    )


def test_effect_target_has_only_read_authority_and_requires_replayable_riverhog_commit() -> None:
    work, workflow, target_plan, evidence = _effect_authorities()
    api = FixtureApi()
    state = InMemoryWorkStore()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed", state=state)
    claim = client.acquire_claim(work)
    inputs = _input_selection(work).artifacts
    client.seal_execution(claim, evidence, workflow, target_plan, inputs, _effect_operation())
    authority = client.target_authority(claim, evidence, target_plan, inputs)
    assert authority.runtime.capability_token.startswith("secret-")
    capability = next(payload for name, payload in api.calls if name == "capability")
    assert capability["actions"] == ("read-inputs",)
    assert capability["audience"] == "stove0.target/fixture-effect-target"
    record = _effect_record(work, workflow, target_plan, evidence)
    state.create(record)
    with pytest.raises(ValueError, match="Riverhog acknowledgment"):
        WorkRecord.model_validate({**record.model_dump(), "phase": "settled"})
    api.lose_effect_ack = True
    with pytest.raises(ConnectionError, match="after durable"):
        client.verify_and_settle_effect(record, _effect_operation())
    # Controller restarted with only the pre-ACK record; must replay settlement,
    # not invoke the target or infer successful settlement from the local receipt.
    restarted = Stove0RiverhogClient(
        api, declared_workspace_protection="memory-backed", state=state
    )
    restored = WorkRecord.model_validate_json(record.model_dump_json())
    identity = restarted.verify_and_settle_effect(restored, _effect_operation())
    assert identity == api.effect_settlement_sha256
    assert [name for name, _ in api.calls].count("settle-effect") == 2
    completed = WorkRecord.model_validate(
        {**record.model_dump(), "phase": "settled", "effect_settlement_sha256": identity}
    )
    assert WorkView.from_record(completed).effect_settlement_sha256 == identity
    client.release_claim(completed)
    assert api.calls[-1][0] == "release"
    assert not any(name in {"delete", "deletion-plan"} for name, _ in api.calls)


def test_riverhog_adapter_closes_only_the_exact_generic_outcome_set() -> None:
    work, workflow, _target_plan, _evidence = _authorities()
    source_selection = ArtifactSelection.seal(
        (
            WorkArtifactSubject(
                id="source",
                role="fixture.source/v1",
                collection=work.inputs[0],
                path="source/input.bin",
                bytes=str(12),
                sha256=_sha("e"),
            ),
        )
    )
    branch = BranchPlan.build(
        parent_work=work,
        branch_id="copy",
        decision_sha256=_sha("c"),
        selection=source_selection,
        recipe=work.recipe,
        effective_intent={},
        workflow_intent=WorkflowPlanIntent.from_plan(workflow),
    )
    plan = BranchSetPlan.seal(
        parent_work=work,
        decision_sha256=_sha("c"),
        branches=(branch,),
        selections={source_selection.selection_sha256: source_selection},
    )
    output_root = CollectionRootIdentityRef(
        collection_id=str(7),
        archive_root_sha256=_sha("7"),
        artifact_set_identity=_sha("8"),
    )
    output_selection = ArtifactSelection.seal(
        (
            WorkArtifactSubject(
                id="output",
                role="fixture.output/v1",
                collection=output_root,
                path="output/result.bin",
                bytes=str(12),
                sha256=_sha("9"),
            ),
        )
    )
    child_work = branch.workflow_plan.work
    child_envelope = ExecutionEnvelope.seal(
        ExecutionEnvelopePayload(
            claim_id=_sha("f"),
            fence=1,
            workflow_plan=branch.workflow_plan,
            target_plan=_evidence.execution_envelope.target_plan,
        )
    )
    child_evidence = ControllerEvidence.seal(
        ControllerEvidencePayload(execution_envelope=child_envelope)
    )
    child = _verifying_record(child_work, branch.workflow_plan, child_evidence)
    assert (
        child.output is not None
        and child.target_status is not None
        and child.target_status.production is not None
    )
    target_settlement = TargetSettlementAuthority.seal(
        TargetSettlementAuthorityPayload(
            job_id=child_envelope.execution_envelope_sha256,
            production_sha256=child.target_status.production.production_sha256,
            output_collection=child.output,
            output_bindings=TargetOutputBindingSetIdentity(
                artifact_count=1, total_bytes="12", sha256=_sha("b")
            ),
        )
    )
    child = WorkRecord.model_validate(
        {**child.model_dump(), "phase": "complete", "target_settlement": target_settlement}
    )
    settlement = BranchSettlement.seal(
        branch=branch,
        producer_settlement_sha256=target_settlement.settlement_sha256,
        derivation_sha256=child.output.derivation_sha256,
        output_collection=output_root,
        output_selection=output_selection,
    )
    selections = {
        source_selection.selection_sha256: source_selection,
        output_selection.selection_sha256: output_selection,
    }
    evaluation = evaluate_branch_set(
        plan,
        selections,
        branch_settlements=(settlement,),
    )
    outcome = CollectionProcessingOutcomeIdentity(
        outcome_id="branch/copy",
        source_claim_id=_sha("f"),
        source_fence=1,
        execution_id=child_envelope.execution_envelope_sha256,
        result_kind="collection",
        output_collection=CollectionRootIdentity(
            collection_id=7, archive_root_sha256=_sha("7"), artifact_set_identity=_sha("8")
        ),
        derivation_sha256=child.output.derivation_sha256,
    )
    api = FixtureApi()
    api.processing_outcomes = [outcome.as_dict()]
    state = InMemoryWorkStore()
    state.create(child)
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed", state=state)
    parent = WorkRecord(
        work=work,
        phase="coordinating",
        claim=ClaimBinding(claim_id=_claim_id(), fence=1),
        branch_set_plan=plan,
    )

    client.settle_outcomes(parent, evaluation)

    payload = next(payload for name, payload in api.calls if name == "settle-outcomes")
    assert payload == {
        "claim_id": _claim_id(),
        "fence": 1,
        "source_collection_retirement_policy": "retain",
        "source_collection_retirement_grace_seconds": 0,
        "outcomes_count": 1,
        "outcomes_sha256": processing_outcome_set_identity((outcome,))["sha256"],
    }


def test_riverhog_adapter_recovers_an_expired_claim_with_a_new_fence() -> None:
    work, _workflow, _target_plan, _evidence = _authorities()
    api = FixtureApi()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed")
    claim = client.acquire_claim(work)
    api.expire_renewal = True

    recovered = client.renew_claim(work, claim)

    assert recovered == ClaimBinding(claim_id=_claim_id(), fence=2)
    assert [name for name, _payload in api.calls][-3:] == ["renew", "get-claim", "claim"]


def test_riverhog_adapter_replays_a_remotely_retiring_claim() -> None:
    work, _workflow, _target_plan, _evidence = _authorities()
    api = FixtureApi()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed")
    claim = client.acquire_claim(work)
    api.claim_state = "retiring"
    api.expire_renewal = True

    assert client.renew_claim(work, claim) == claim
    assert [name for name, _payload in api.calls][-2:] == ["renew", "get-claim"]


def test_riverhog_adapter_refuses_to_resume_terminal_work() -> None:
    work, _workflow, _target_plan, _evidence = _authorities()
    api = FixtureApi()
    api.claim_state = "abandoned"
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed")

    with pytest.raises(RuntimeError, match="terminal: abandoned"):
        client.acquire_claim(work)


def test_riverhog_adapter_restarts_retryable_work_with_a_new_fence() -> None:
    work, _workflow, _target_plan, _evidence = _authorities()
    api = FixtureApi()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed")
    claim = client.acquire_claim(work)

    restarted = client.restart_claim(work, claim)

    assert restarted == ClaimBinding(claim_id=_claim_id(), fence=2)
    assert api.calls[-1] == (
        "restart",
        {"claim_id": _claim_id(), "fence": 1, "lease_seconds": 1800},
    )


def test_riverhog_adapter_retirement_is_fenced_and_challenge_bound() -> None:
    work, workflow, _target_plan, evidence = _authorities("retire-after-settlement")
    api = FixtureApi()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed")
    record = _verifying_record(work, workflow, evidence).model_copy(update={"phase": "settled"})

    assert client.begin_source_collection_retirement(record) is True
    assert client.retire_source_collection(record, 1) is True
    assert client.retire_source_collection(record, 1) is True
    client.release_claim(record)

    delete_call = next(payload for name, payload in api.calls if name == "delete")
    assert delete_call["source_collection_retirement_claim_id"] == _claim_id()
    assert delete_call["challenge"] == "delete-me"


def test_riverhog_adapter_reports_grace_and_deletion_blockers_as_waiting() -> None:
    work, workflow, _target_plan, evidence = _authorities("retire-after-settlement")
    api = FixtureApi()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed")
    record = _verifying_record(work, workflow, evidence).model_copy(update={"phase": "settled"})

    api.retirement_state = "settled"
    assert client.begin_source_collection_retirement(record) is False

    api.retirement_state = "retiring"
    api.deletion_blockers = ["active retrieval"]
    assert client.begin_source_collection_retirement(record) is True
    assert client.retire_source_collection(record, 1) is False
    assert not any(name == "delete" for name, _payload in api.calls)


def test_riverhog_adapter_abandons_the_exact_claim_generation() -> None:
    work, _workflow, _target_plan, _evidence = _authorities()
    api = FixtureApi()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed")
    record = WorkRecord(
        work=work,
        phase="abandon_pending",
        claim=ClaimBinding(claim_id=_claim_id(), fence=1),
        abandon_outcome="canceled",
    )

    client.abandon_claim(record)

    assert api.calls[-1] == (
        "abandon",
        {
            "claim_id": _claim_id(),
            "fence": 1,
            "reason": "canceled: stove0 work was canceled before Riverhog settlement",
        },
    )


def test_synchronous_observation_must_fit_claim_and_capability_lifetime() -> None:
    work, _workflow, _target_plan, _evidence = _authorities()
    api = FixtureApi()
    client = Stove0RiverhogClient(
        api,
        declared_workspace_protection="memory-backed",
        claim_lease_seconds=30,
        capability_ttl_seconds=30,
    )
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id=work.work_id,
            observer_registration_id="fixture-observer",
            observer_descriptor_sha256=_sha("c"),
            observer_contract_id="fixture.observe/v1",
            observer_contract_sha256=_sha("d"),
            subjects=(
                WorkArtifactSubject(
                    id="source",
                    role="fixture.source/v1",
                    collection=work.inputs[0],
                    path="source/input.bin",
                    bytes=str(12),
                    sha256=_sha("e"),
                ),
            ),
            timeout_seconds=31,
        )
    )
    with pytest.raises(ValueError, match="timeout exceeds"):
        client.observation_authority(
            ClaimBinding(claim_id=_claim_id(), fence=1),
            request,
        )


def test_observation_capability_projects_subjects_into_riverhog_artifact_order() -> None:
    work, _workflow, _target_plan, _evidence = _authorities()
    api = FixtureApi()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed")
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id=work.work_id,
            observer_registration_id="fixture-observer",
            observer_descriptor_sha256=_sha("c"),
            observer_contract_id="fixture.observe/v1",
            observer_contract_sha256=_sha("d"),
            subjects=(
                WorkArtifactSubject(
                    id="a-request-id",
                    role="fixture.source/v1",
                    collection=work.inputs[0],
                    path="source/z.bin",
                    bytes=str(12),
                    sha256=_sha("e"),
                ),
                WorkArtifactSubject(
                    id="z-request-id",
                    role="fixture.source/v1",
                    collection=work.inputs[0],
                    path="source/a.bin",
                    bytes=str(13),
                    sha256=_sha("f"),
                ),
            ),
        )
    )

    client.observation_authority(ClaimBinding(claim_id=_claim_id(), fence=1), request)

    capability = next(payload for name, payload in api.calls if name == "capability")
    assert [item["path"] for item in capability["artifacts"]] == [
        "source/a.bin",
        "source/z.bin",
    ]


def test_preview_claim_is_separate_read_only_authority_and_is_abandoned() -> None:
    work, _workflow, _target_plan, _evidence = _authorities()
    api = FixtureApi()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed")
    request = WorkflowPreviewRequest.seal(WorkflowPreviewRequestPayload(work=work))

    first = client.acquire_preview_claim(request)
    client.abandon_preview_claim(request, first)
    second = client.acquire_preview_claim(request)
    client.abandon_preview_claim(request, second)

    claim_calls = [payload for name, payload in api.calls if name == "claim"]
    assert len(claim_calls) == 2
    assert claim_calls[0]["work_id"] != claim_calls[1]["work_id"]
    assert all(payload["work_id"] != request.preview_id for payload in claim_calls)
    assert {payload["purpose"] for payload in claim_calls} == {"stove0-workflow-preview/v1"}
    assert {payload["work_document"]["preview_id"] for payload in claim_calls} == {
        request.preview_id
    }
    abandon_calls = [payload for name, payload in api.calls if name == "abandon"]
    assert len(abandon_calls) == 2
    assert {payload["reason"] for payload in abandon_calls} == {
        f"preview-complete:{request.preview_id}"
    }


def test_effect_controller_waits_for_riverhog_ack_before_persisting_success() -> None:
    work, workflow, plan, evidence = _effect_authorities()
    api = FixtureApi()
    store = InMemoryWorkStore()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed", state=store)
    claim = client.acquire_claim(work)
    client.seal_execution(
        claim, evidence, workflow, plan, _input_selection(work).artifacts, _effect_operation()
    )
    record = _effect_record(work, workflow, plan, evidence)
    store.create(record)

    def controller(current_store: InMemoryWorkStore) -> Stove0Coordinator:
        return Stove0Coordinator(
            Stove0WorkService(current_store),
            riverhog=Stove0RiverhogClient(
                api, declared_workspace_protection="memory-backed", state=current_store
            ),
            planning=cast(
                Any, SimpleNamespace(operation_contract=lambda _identity: _effect_operation())
            ),
            observers=cast(Any, object()),
            targets=cast(Any, object()),
            target_callbacks=cast(Any, object()),
        )

    api.lose_effect_ack = True
    with pytest.raises(ConnectionError):
        controller(store).step(work.work_id)
    unchanged = store.load(work.work_id)
    assert unchanged is not None and unchanged.phase == "verifying"
    assert unchanged.effect_settlement_sha256 is None and api.effect_settlement_sha256 is not None
    # Model a controller restart by decoding the durable record into a fresh store.
    restarted = InMemoryWorkStore()
    restarted.create(WorkRecord.model_validate_json(unchanged.model_dump_json()))
    settled = controller(restarted).step(work.work_id)
    assert (
        settled.phase == "settled"
        and settled.effect_settlement_sha256 == api.effect_settlement_sha256
    )
    assert controller(restarted).step(work.work_id).phase == "complete"
    assert not any(name in {"delete", "deletion-plan", "retirement"} for name, _ in api.calls)


@pytest.mark.parametrize("outcome", ["failed", "commit-uncertain", "interrupted"])
def test_failed_or_uncertain_effect_work_cannot_request_source_retirement(outcome: str) -> None:
    work, workflow, plan, evidence = _effect_authorities("retire-after-settlement")
    verifying = _effect_record(work, workflow, plan, evidence)
    assert verifying.target_request is not None
    executing = WorkRecord.model_validate(
        {**verifying.model_dump(), "phase": "executing", "target_status": None}
    )
    store = InMemoryWorkStore()
    store.create(executing)
    state = Stove0WorkService(store)
    status = TargetJobStatus(
        protocol=plan.protocol,
        job_id=verifying.target_request.declaration.job_id,
        state="interrupted" if outcome == "interrupted" else "failed",
        attempt=1,
        request_sha256=verifying.target_request.request_sha256,
        plan_sha256=plan.plan_sha256,
        progress=TargetProgress(phase=outcome, completed=0, total=1),
        failure=None
        if outcome == "interrupted"
        else TargetFailure(code=outcome, message=outcome, retryable=False),
    )
    unresolved = state.record_target_status(
        work.work_id, status, operation=_effect_operation(), expected_revision=executing.revision
    )
    assert unresolved.effect_settlement_sha256 is None
    assert unresolved.source_collection_retirement_remaining == ()
    with pytest.raises(RuntimeError):
        state.verify_effect(work.work_id, _sha("f"), expected_revision=unresolved.revision)
    with pytest.raises(RuntimeError):
        state.begin_source_collection_retirement(
            work.work_id, (1,), expected_revision=unresolved.revision
        )


@pytest.mark.parametrize(
    ("observed_verdict", "required_verdict", "approved"),
    [
        pytest.param(True, True, True, id="matching-boolean"),
        pytest.param({"safe": [True, 2]}, {"safe": [True, 2]}, True, id="matching-nested"),
        pytest.param(1, True, False, id="number-for-true"),
        pytest.param(0, False, False, id="number-for-false"),
        pytest.param(True, 1, False, id="true-for-number"),
        pytest.param({"safe": True}, {"safe": 1}, False, id="object-mismatch"),
        pytest.param([{"safe": False}], [{"safe": 0}], False, id="array-mismatch"),
    ],
)
def test_no_output_source_loss_requires_exact_per_artifact_observer_verdict(
    observed_verdict: Any, required_verdict: Any, approved: bool
) -> None:
    work, _workflow, _plan, _evidence = _authorities()
    source = _input_selection(work).artifacts[0]
    schema = JsonSchemaValidationProfile.from_schema("fixture.consideration/v1", {"type": "object"})
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id=work.work_id,
            observer_registration_id="fixture-observer",
            observer_descriptor_sha256=_sha("4"),
            observer_contract_id="fixture.consideration/v1",
            observer_contract_sha256=_sha("5"),
            subjects=(source,),
        )
    )
    facts = {
        "records": [{"artifact_id": source.id, "discard": observed_verdict}],
        "unrelated_blob": "x" * 10_000,
    }
    result = ContentObservationResult.seal(
        ContentObservationResultPayload(
            request_id=request.request_id,
            state="observed",
            observer=ObserverImplementation(
                id="fixture-observer/v1",
                version="1",
                source_revision="fixture",
                descriptor_sha256=request.observer_descriptor_sha256,
            ),
            observer_contract_id=request.observer_contract_id,
            observer_contract_sha256=request.observer_contract_sha256,
            subjects=request.subjects,
            facts_schema=schema,
            facts=facts,
            facts_sha256=canonical_json_sha256(facts),
        )
    )
    preview_request = WorkflowPreviewRequest.seal(WorkflowPreviewRequestPayload(work=work))
    preview = WorkflowPreview.seal(
        WorkflowPreviewPayload(
            preview_id=preview_request.preview_id,
            state="no_action",
            work=work,
            observations=(ContentObservationEvidence(request=request, result=result),),
            outcome=PreviewOutcome(code="fixture.no-action/v1", message="Discard selected bytes."),
        )
    )
    slot = RecipeSourceLossEvidenceSlot(
        observation_contract_id=request.observer_contract_id,
        observation_contract_sha256=request.observer_contract_sha256,
        facts_profile_sha256=schema.profile_sha256,
        artifact_facts=ArtifactFactBinding(records_pointer="/records"),
        verdict_pointer="/discard",
        verdict_value=required_verdict,
    )
    rule = RecipeSourceLossRule(id="fixture.discard/v1", evidence_slots=(slot,))
    identity = CollectionArtifactIdentity(
        collection=CollectionRootIdentity(
            source.collection.collection_id,
            source.collection.archive_root_sha256,
            source.collection.artifact_set_identity,
        ),
        path=source.path,
        bytes=source.bytes,
        sha256=source.sha256,
    )
    approval = _no_output_discard_approval(
        identity, rule, preview, controller_id="stove0", reason="Discard selected bytes."
    )
    if not approved:
        assert approval is None
        return
    assert approval is not None and approval.rule_sha256 == rule.sha256
    assert len(approval.evidence_json) < 1000
    assert (
        _no_output_discard_approval(
            CollectionArtifactIdentity(
                collection=identity.collection,
                path="source/other.bin",
                bytes=identity.bytes,
                sha256=identity.sha256,
            ),
            rule,
            preview,
            controller_id="stove0",
            reason="Discard selected bytes.",
        )
        is None
    )
    weaker = RecipeSourceLossRule(
        id=rule.id,
        evidence_slots=(slot.model_copy(update={"facts_profile_sha256": _sha("a")}),),
    )
    assert (
        _no_output_discard_approval(
            identity, weaker, preview, controller_id="stove0", reason="Discard selected bytes."
        )
        is None
    )


@pytest.mark.parametrize(
    ("source_loss_enabled", "observed_verdict", "required_verdict", "approval_expected"),
    [
        pytest.param(False, None, None, False, id="no-source-loss-rule"),
        pytest.param(True, True, True, True, id="matching-verdict"),
        pytest.param(True, 1, True, False, id="boolean-number-mismatch"),
        pytest.param(
            True, {"safe": [1]}, {"safe": [True]}, False, id="nested-boolean-number-mismatch"
        ),
    ],
)
def test_no_output_adapter_seals_disposition_and_replays_lost_ack(
    source_loss_enabled: bool,
    observed_verdict: Any,
    required_verdict: Any,
    approval_expected: bool,
) -> None:
    work, _workflow, _plan, _evidence = _authorities()
    observations: tuple[ContentObservationEvidence, ...] = ()
    source_loss: RecipeSourceLossRule | None = None
    if source_loss_enabled:
        source = _input_selection(work).artifacts[0]
        schema = JsonSchemaValidationProfile.from_schema(
            "fixture.consideration/v1", {"type": "object"}
        )
        request = ContentObservationRequest.seal(
            ContentObservationRequestPayload(
                work_id=work.work_id,
                observer_registration_id="fixture-observer",
                observer_descriptor_sha256=_sha("4"),
                observer_contract_id="fixture.consideration/v1",
                observer_contract_sha256=_sha("5"),
                subjects=(source,),
            )
        )
        facts = {
            "done": True,
            "records": [{"artifact_id": source.id, "discard": observed_verdict}],
        }
        result = ContentObservationResult.seal(
            ContentObservationResultPayload(
                request_id=request.request_id,
                state="observed",
                observer=ObserverImplementation(
                    id="fixture-observer/v1",
                    version="1",
                    source_revision="fixture",
                    descriptor_sha256=request.observer_descriptor_sha256,
                ),
                observer_contract_id=request.observer_contract_id,
                observer_contract_sha256=request.observer_contract_sha256,
                subjects=request.subjects,
                facts_schema=schema,
                facts=facts,
                facts_sha256=canonical_json_sha256(facts),
            )
        )
        observations = (ContentObservationEvidence(request=request, result=result),)
        source_loss = RecipeSourceLossRule(
            id="fixture.discard/v1",
            evidence_slots=(
                RecipeSourceLossEvidenceSlot(
                    observation_contract_id=request.observer_contract_id,
                    observation_contract_sha256=request.observer_contract_sha256,
                    facts_profile_sha256=schema.profile_sha256,
                    artifact_facts=ArtifactFactBinding(records_pointer="/records"),
                    verdict_pointer="/discard",
                    verdict_value=required_verdict,
                ),
            ),
        )
    preview_request = WorkflowPreviewRequest.seal(WorkflowPreviewRequestPayload(work=work))
    preview = WorkflowPreview.seal(
        WorkflowPreviewPayload(
            preview_id=preview_request.preview_id,
            state="no_action",
            work=work,
            observations=observations,
            outcome=PreviewOutcome(code="fixture.no-action/v1", message="No output is needed."),
        )
    )
    no_action = RecipeNoAction(
        code="fixture.no-action/v1",
        message="No output is needed.",
        when=(
            FactPredicate(
                observation_contract_id=(
                    "fixture.consideration/v1" if source_loss_enabled else "fixture.observation/v1"
                ),
                pointer="/done",
                value=True,
            ),
        ),
        source_loss=source_loss,
    )
    record = WorkRecord(
        work=work,
        phase="no_output_pending",
        claim=ClaimBinding(claim_id=_claim_id(), fence=1),
        no_action_preview=preview,
        no_output_retirement_policy=(
            "retire-after-settlement" if source_loss_enabled else "retain"
        ),
    )

    class NoOutputApi(FixtureApi):
        no_output_sha256: str | None = None
        dispositions: list[dict[str, object]]

        def __init__(self) -> None:
            super().__init__()
            self.dispositions = []

        def get_processing_claim(self, claim_id: str) -> _AttrDict:
            return _AttrDict(
                id=claim_id,
                fence=self.fence,
                state=self.claim_state,
                plan=self.plan,
                consumer=_AttrDict(app="stove0"),
                no_output_settlement_sha256=self.no_output_sha256,
            )

        def seal_processing_claim_plan(self, claim_id: str, **kwargs: Any) -> _AttrDict:
            super().seal_processing_claim_plan(claim_id, **kwargs)
            return self.get_processing_claim(claim_id)

        def get_collection(self, collection_id: int) -> dict[str, Any]:
            return {
                "id": str(collection_id),
                "archive_root_sha256": _sha("2"),
                "artifact_set_identity": _sha("3"),
            }

        def get_portable_collection_inventory(
            self,
            collection_id: int,
            *,
            cursor: str | None,
            limit: int,
            inventory_identity: str | None,
        ) -> PortableCollectionInventoryPage:
            assert cursor is None and limit == 1000 and inventory_identity is None
            return PortableCollectionInventoryPage(
                authority=PortableCollectionInventoryAuthority(
                    header=PortableCollectionHeader(
                        collection=str(collection_id),
                        artifact_set_identity=_sha("3"),
                        encryption_format="age/v1",
                        passphrase_id="fixture-passphrase",
                        provenance_mode="omitted",
                    ),
                    inventory_identity=_sha("5"),
                    file_count="1",
                    file_bytes="12",
                ),
                files=[
                    ImmutableFileIdentityDocument(
                        path="source/input.bin", bytes="12", sha256=_sha("e")
                    )
                ],
                complete=True,
            )

        def list_processing_claim_artifacts(self, claim_id: str, **kwargs: Any) -> Any:
            assert self.plan is not None
            return SimpleNamespace(
                start_ordinal=0,
                identity=SimpleNamespace(sha256=self.plan.artifacts.sha256),
                artifacts=(
                    SimpleNamespace(
                        collection=SimpleNamespace(
                            collection_id=1,
                            archive_root_sha256=_sha("2"),
                            artifact_set_identity=_sha("3"),
                        ),
                        path="source/input.bin",
                        bytes=12,
                        sha256=_sha("e"),
                    ),
                ),
                next_ordinal=None,
            )

        def record_processing_claim_dispositions(self, claim_id: str, **kwargs: Any) -> None:
            self.dispositions.extend(kwargs["dispositions"])

        def get_processing_claim_dispositions(
            self, claim_id: str
        ) -> ArtifactDispositionSetDocument:
            identity = ArtifactDispositionSetIdentity(1, 0, 0, _sha("6"))
            return ArtifactDispositionSetDocument(
                claim_id=claim_id,
                state="sealed",
                disposition_count="1",
                output_edge_count="0",
                output_artifact_count="0",
                identity=ArtifactDispositionSetIdentityDocument.model_validate(identity.as_dict()),
            )

        def seal_processing_claim_dispositions(
            self, claim_id: str, **kwargs: Any
        ) -> ArtifactDispositionSetDocument:
            return self.get_processing_claim_dispositions(claim_id)

        def settle_processing_claim_no_output(self, claim_id: str, **kwargs: Any) -> _AttrDict:
            self.calls.append(("settle-no-output", kwargs))
            digest = riverhog_canonical_json_sha256(kwargs["settlement"])
            if self.no_output_sha256 is not None:
                assert self.no_output_sha256 == digest
            self.no_output_sha256 = digest
            self.claim_state = "settled"
            return self.get_processing_claim(claim_id)

    api = NoOutputApi()
    client = Stove0RiverhogClient(api, declared_workspace_protection="memory-backed")
    policy = "retire-after-settlement" if source_loss_enabled else "retain"
    first = client.verify_and_settle_no_output(record, no_action, policy, 0)
    assert first == api.no_output_sha256
    assert len(api.dispositions) == 1
    assert api.dispositions[0]["status"] == "not-carried-forward"
    assert ("discard_approval" in api.dispositions[0]) is approval_expected
    assert len([name for name, _ in api.calls if name == "consideration_evidence"]) == int(
        source_loss_enabled
    )
    assert client.verify_and_settle_no_output(record, no_action, policy, 0) == first
    assert len(api.dispositions) == 1
