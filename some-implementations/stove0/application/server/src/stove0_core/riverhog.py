"""Concrete stove0 controller adapter for Riverhog's generic work API.

The adapter is intentionally narrow. It translates stove0-owned documents into
Riverhog's content-opaque claim/capability primitives and performs independent
settlement verification. It never receives or exposes archive credentials.
"""

from __future__ import annotations

import secrets
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Literal, Protocol

from riverhog_canonical_json import canonical_json_bytes as riverhog_canonical_json_bytes
from riverhog_protocol import Conflict, NotFound, OutputCollectionPolicy
from riverhog_protocol.collection_workflow_transport import (
    DISPOSITION_BATCH_MAX,
    ArtifactDispositionOutputPageDocument,
    ArtifactDispositionPageDocument,
    ArtifactDispositionSetDocument,
    CapabilityAction,
    CollectionArtifactPageDocument,
    CollectionDerivationResponseDocument,
    ConsiderationEvidenceOutDocument,
    ProcessingCapabilityDocument,
    ProcessingClaimDocument,
    ProcessingOutcomePageDocument,
)
from riverhog_protocol.collection_workflows import (
    ArtifactDiscardApproval,
    ArtifactDisposition,
    ArtifactDispositionOutput,
    ArtifactDispositionSetIdentity,
    CollectionArtifactIdentity,
    CollectionDerivation,
    CollectionProcessingOutcomeIdentity,
    CollectionRootIdentity,
    OperationIdentity,
    SourceCollectionRetirementPolicy,
    processing_outcome_set_identity,
)
from riverhog_protocol.collection_workflows import (
    canonical_json_sha256 as riverhog_canonical_json_sha256,
)
from riverhog_protocol.effect_settlement import ExternalEffectSettlement
from riverhog_protocol.no_output_settlement import NoOutputSettlement
from riverhog_protocol.portable_collection import PortableCollectionInventoryPage
from riverhog_protocol.workspace_protection import DeclaredWorkspaceProtection
from stove0_observer_protocol import (
    ContentObservationRequest,
    ObserverRuntimeAuthority,
)
from stove0_protocol import (
    BranchSetEvaluation,
    ControllerEvidence,
    TargetPlanBinding,
    WorkArtifactSubject,
    WorkflowPlan,
    WorkflowPreview,
    WorkflowPreviewRequest,
    WorkIdentity,
)
from stove0_recipe_config import RecipeNoAction, RecipeSourceLossRule
from stove0_target_protocol import (
    InputArtifact,
    OperationContract,
    OutputCollectionRef,
    TargetOutputBinding,
    TargetOutputBindingSetIdentity,
    TargetPlan,
    TargetRuntimeAuthority,
    TargetSettlementAuthority,
    TargetSettlementAuthorityPayload,
    update_target_output_binding_commitment,
    validate_status_against_request,
)

from stove0_core._checkpoint_sha256 import CheckpointSHA256
from stove0_core.coordinator import (
    ParentOutcomeBinding,
    TargetInvocationAuthority,
)
from stove0_core.work_state import (
    ClaimBinding,
    ConcurrentWorkUpdate,
    TargetSettlementSealCheckpoint,
    TargetSettlementSealRecord,
    WorkRecord,
    WorkStore,
)


class RiverhogApi(Protocol):
    base_url: str
    allow_insecure_http: bool

    def create_or_resume_processing_claim(
        self,
        *,
        work_id: str,
        work_document: Mapping[str, Any],
        work_document_sha256: str,
        inputs: Iterable[Mapping[str, Any]],
        lease_seconds: int = 1800,
        purpose: str = "collection-work/v1",
    ) -> ProcessingClaimDocument: ...

    def renew_processing_claim(
        self, claim_id: str, *, fence: int, lease_seconds: int = 1800
    ) -> ProcessingClaimDocument: ...

    def restart_processing_claim(
        self, claim_id: str, *, fence: int, lease_seconds: int = 1800
    ) -> ProcessingClaimDocument: ...

    def get_processing_claim(self, claim_id: str) -> ProcessingClaimDocument: ...

    def list_processing_claim_artifacts(
        self, claim_id: str, *, identity_sha256: str, start_ordinal: int = 0
    ) -> CollectionArtifactPageDocument: ...

    def create_processing_capability(
        self,
        claim_id: str,
        *,
        fence: int,
        audience: str,
        actions: Sequence[CapabilityAction] = ("read-inputs",),
        artifacts: Iterable[Mapping[str, Any]],
        ttl_seconds: int = 900,
    ) -> ProcessingCapabilityDocument: ...

    def seal_processing_claim_plan(
        self,
        claim_id: str,
        *,
        fence: int,
        execution_id: str,
        controller_evidence: Mapping[str, Any],
        controller_evidence_sha256: str,
        operation_id: str,
        operation_sha256: str,
        input_artifacts: Iterable[Mapping[str, Any]],
        result_kind: Literal["collection", "external-effect", "no-output"] = "collection",
        operation_contract: Mapping[str, Any] | None = None,
        output_policy: OutputCollectionPolicy | None = None,
        source_collection_retirement_policy: SourceCollectionRetirementPolicy = "retain",
        source_collection_retirement_grace_seconds: int = 0,
    ) -> ProcessingClaimDocument: ...

    def settle_processing_claim(
        self,
        claim_id: str,
        *,
        fence: int,
        output_collection_id: int,
        derivation: Mapping[str, Any],
        outcome_claim_id: str | None = None,
        outcome_fence: int | None = None,
        outcome_id: str | None = None,
    ) -> ProcessingClaimDocument: ...

    def settle_processing_claim_effect(
        self,
        claim_id: str,
        *,
        fence: int,
        settlement: Mapping[str, Any],
        outcome_claim_id: str | None = None,
        outcome_fence: int | None = None,
        outcome_id: str | None = None,
    ) -> ProcessingClaimDocument: ...

    def settle_processing_claim_no_output(
        self,
        claim_id: str,
        *,
        fence: int,
        settlement: Mapping[str, Any],
        outcome_claim_id: str | None = None,
        outcome_fence: int | None = None,
        outcome_id: str | None = None,
    ) -> ProcessingClaimDocument: ...

    def append_processing_claim_outcomes(
        self,
        claim_id: str,
        *,
        fence: int,
        outcomes: Sequence[Mapping[str, Any]],
    ) -> ProcessingClaimDocument: ...

    def settle_processing_claim_outcomes(
        self,
        claim_id: str,
        *,
        fence: int,
        outcomes_count: int,
        outcomes_sha256: str,
        source_collection_retirement_policy: SourceCollectionRetirementPolicy = "retain",
        source_collection_retirement_grace_seconds: int = 0,
    ) -> ProcessingClaimDocument: ...

    def list_processing_claim_outcomes(
        self,
        claim_id: str,
        *,
        identity_sha256: str,
        start_ordinal: int = 0,
    ) -> ProcessingOutcomePageDocument: ...

    def abandon_processing_claim(
        self, claim_id: str, *, fence: int, reason: str
    ) -> ProcessingClaimDocument: ...

    def get_collection(self, collection_id: int) -> dict[str, Any]: ...

    def get_collection_derivation(
        self, collection_id: int
    ) -> CollectionDerivationResponseDocument: ...

    def get_portable_collection_inventory(
        self,
        collection_id: int,
        *,
        cursor: str | None = None,
        limit: int = 100,
        inventory_identity: str | None = None,
    ) -> PortableCollectionInventoryPage: ...

    def list_processing_claim_dispositions(
        self,
        claim_id: str,
        *,
        identity_sha256: str,
        start_ordinal: int = 0,
    ) -> ArtifactDispositionPageDocument: ...

    def list_processing_claim_disposition_outputs(
        self,
        claim_id: str,
        *,
        identity_sha256: str,
        start_ordinal: int = 0,
    ) -> ArtifactDispositionOutputPageDocument: ...

    def record_processing_claim_dispositions(
        self,
        claim_id: str,
        *,
        fence: int,
        dispositions: Sequence[Mapping[str, Any]],
    ) -> ArtifactDispositionSetDocument: ...

    def record_processing_claim_consideration_evidence(
        self,
        claim_id: str,
        *,
        fence: int,
        document: Mapping[str, Any],
        sha256: str,
    ) -> ConsiderationEvidenceOutDocument: ...

    def record_processing_claim_disposition_outputs(
        self,
        claim_id: str,
        *,
        fence: int,
        outputs: Sequence[Mapping[str, Any]],
    ) -> ArtifactDispositionSetDocument: ...

    def seal_processing_claim_dispositions(
        self, claim_id: str, *, fence: int
    ) -> ArtifactDispositionSetDocument: ...

    def get_processing_claim_dispositions(
        self, claim_id: str
    ) -> ArtifactDispositionSetDocument: ...

    def begin_source_collection_retirement(
        self, claim_id: str, *, fence: int
    ) -> ProcessingClaimDocument: ...

    def plan_collection_deletion(
        self, collection_id: int, *, source_collection_retirement_claim_id: str | None = None
    ) -> dict[str, Any]: ...

    def delete_collection(
        self,
        collection_id: int,
        *,
        challenge: str,
        source_collection_retirement_claim_id: str | None = None,
        event_context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    def release_processing_claim(self, claim_id: str, *, fence: int) -> ProcessingClaimDocument: ...


def _verify_disposition_identity(
    api: RiverhogApi,
    record: WorkRecord,
    derivation: CollectionDerivation,
) -> None:
    assert record.claim is not None
    assert record.target_status is not None
    assert record.target_status.production is not None
    production = record.target_status.production
    if derivation.disposition_set != production.riverhog_disposition_set:
        raise RuntimeError("target derivation differs from its production authority")
    sealed = api.get_processing_claim_dispositions(record.claim.claim_id)
    if (
        sealed.state != "sealed"
        or sealed.identity is None
        or ArtifactDispositionSetIdentity.from_mapping(sealed.identity.model_dump(mode="json"))
        != production.riverhog_disposition_set
    ):
        raise RuntimeError("Riverhog generic derivation identity changed")


class Stove0RiverhogClient:
    """Riverhog authority used by stove0 controller and worker roles."""

    def __init__(
        self,
        api: RiverhogApi,
        *,
        claim_lease_seconds: int = 30 * 60,
        capability_ttl_seconds: int = 15 * 60,
        declared_workspace_protection: DeclaredWorkspaceProtection,
        claim_purpose: str = "stove0-collection-work/v1",
        state: WorkStore | None = None,
        authority_batch_size: int = 100,
    ) -> None:
        if claim_lease_seconds < 30 or capability_ttl_seconds < 30:
            raise ValueError("Riverhog claim and capability lifetimes must be at least 30 seconds")
        if declared_workspace_protection not in {"encrypted-at-rest", "memory-backed"}:
            raise ValueError("stove0 workspace protection declaration is invalid")
        purpose = claim_purpose.strip()
        if not purpose:
            raise ValueError("Riverhog claim purpose must be visible")
        if authority_batch_size < 1 or authority_batch_size > DISPOSITION_BATCH_MAX:
            raise ValueError(
                f"Stove0 authority batch size must be between 1 and {DISPOSITION_BATCH_MAX}"
            )
        self.api = api
        self.claim_lease_seconds = claim_lease_seconds
        self.capability_ttl_seconds = capability_ttl_seconds
        self.declared_workspace_protection = declared_workspace_protection
        self.claim_purpose = purpose
        self.state = state
        self.authority_batch_size = authority_batch_size

    def project_target_dispositions(
        self,
        record: WorkRecord,
        dispositions: Sequence[ArtifactDisposition],
    ) -> None:
        if record.claim is None:
            raise ValueError("target production requires an active Riverhog claim")
        if not dispositions or len(dispositions) > DISPOSITION_BATCH_MAX:
            raise ValueError("target disposition projection batch is invalid")
        self.api.record_processing_claim_dispositions(
            record.claim.claim_id,
            fence=record.claim.fence,
            dispositions=[item.as_dict() for item in dispositions],
        )

    def project_target_source_edges(
        self,
        record: WorkRecord,
        edges: Sequence[ArtifactDispositionOutput],
    ) -> None:
        if record.claim is None:
            raise ValueError("target production requires an active Riverhog claim")
        if not edges or len(edges) > DISPOSITION_BATCH_MAX:
            raise ValueError("target source-edge projection batch is invalid")
        self.api.record_processing_claim_disposition_outputs(
            record.claim.claim_id,
            fence=record.claim.fence,
            outputs=[item.as_dict() for item in edges],
        )

    def seal_target_projection(
        self,
        record: WorkRecord,
    ) -> ArtifactDispositionSetIdentity | None:
        if record.claim is None:
            raise ValueError("target production requires an active Riverhog claim")
        status = self.api.seal_processing_claim_dispositions(
            record.claim.claim_id,
            fence=record.claim.fence,
        )
        if status.state == "sealing":
            return None
        if status.state != "sealed" or status.identity is None:
            raise RuntimeError(status.failure or "Riverhog did not seal derivation evidence")
        return ArtifactDispositionSetIdentity.from_mapping(status.identity.model_dump(mode="json"))

    def acquire_claim(self, work: WorkIdentity) -> ClaimBinding:
        return self._acquire_document_claim(
            identity=work.work_id,
            document=work.model_dump(mode="json", by_alias=True, exclude_none=True),
            inputs=[item.model_dump(mode="json") for item in work.inputs],
            purpose=self.claim_purpose,
        )

    def acquire_preview_claim(self, request: WorkflowPreviewRequest) -> ClaimBinding:
        # The preview result identity is semantic and repeatable, but each read-only
        # execution receives a distinct Riverhog claim. A completed preview abandons
        # its claim, so reusing the semantic preview ID as claim work identity would
        # collide with that terminal claim on a later preview invocation.
        attempt_id = secrets.token_hex(16)
        claim_work_id = riverhog_canonical_json_sha256(
            {
                "format": "stove0-workflow-preview-claim/v1",
                "preview_id": request.preview_id,
                "attempt_id": attempt_id,
            }
        )
        return self._acquire_document_claim(
            identity=claim_work_id,
            document=request.model_dump(mode="json", by_alias=True, exclude_none=True),
            inputs=[item.model_dump(mode="json") for item in request.work.inputs],
            purpose="stove0-workflow-preview/v1",
        )

    def renew_claim(
        self,
        work: WorkIdentity,
        claim: ClaimBinding,
    ) -> ClaimBinding:
        try:
            payload = self.api.renew_processing_claim(
                claim.claim_id,
                fence=claim.fence,
                lease_seconds=self.claim_lease_seconds,
            )
        except Conflict as exc:
            current = self.api.get_processing_claim(claim.claim_id)
            if _claim_binding(current) == claim and current.state in {
                "settled",
                "retiring",
                "released",
            }:
                # A prior step may have settled the claim remotely before its
                # local work phase advanced. Let that step replay its exact
                # settlement instead of trying to reacquire a terminal claim.
                return claim
            recovered = self.acquire_claim(work)
            if recovered.claim_id != claim.claim_id or recovered.fence <= claim.fence:
                raise RuntimeError(
                    "Riverhog did not recover the expired claim with a newer fence"
                ) from exc
            return recovered
        renewed = _claim_binding(payload)
        if renewed != claim:
            raise RuntimeError("Riverhog renewed a different claim generation")
        return renewed

    def restart_claim(
        self,
        work: WorkIdentity,
        claim: ClaimBinding,
    ) -> ClaimBinding:
        try:
            payload = self.api.restart_processing_claim(
                claim.claim_id,
                fence=claim.fence,
                lease_seconds=self.claim_lease_seconds,
            )
        except Conflict as exc:
            recovered = self.acquire_claim(work)
            if recovered.claim_id != claim.claim_id or recovered.fence <= claim.fence:
                raise RuntimeError(
                    "Riverhog did not reconcile the restarted claim with a newer fence"
                ) from exc
            return recovered
        restarted = _claim_binding(payload)
        if restarted.claim_id != claim.claim_id or restarted.fence != claim.fence + 1:
            raise RuntimeError("Riverhog restarted an unexpected claim generation")
        return restarted

    def observation_authority(
        self,
        claim: ClaimBinding,
        request: ContentObservationRequest,
    ) -> ObserverRuntimeAuthority:
        if request.timeout_seconds > min(
            self.claim_lease_seconds,
            self.capability_ttl_seconds,
        ):
            raise ValueError(
                "synchronous observation timeout exceeds its claim/capability lifetime"
            )
        audience = f"stove0.observer/{request.observer_registration_id}"
        capability = self._capability(
            claim,
            audience=audience,
            actions=("read-inputs",),
            # Observation subjects are semantically ordered by their request-scoped
            # IDs.  Capability scope is a different, generic Riverhog authority and
            # must be projected into immutable collection-artifact order.
            artifacts=tuple(sorted(_artifact_identity(item) for item in request.subjects)),
        )
        return ObserverRuntimeAuthority(
            riverhog_base_url=self.api.base_url,
            capability_token=_token(capability),
            allow_insecure_http=bool(self.api.allow_insecure_http),
            declared_workspace_protection=self.declared_workspace_protection,
        )

    def seal_execution(
        self,
        claim: ClaimBinding,
        evidence: ControllerEvidence,
        plan: WorkflowPlan,
        target_plan: TargetPlan,
        inputs: Iterable[WorkArtifactSubject],
        operation: OperationContract,
    ) -> None:
        envelope = evidence.execution_envelope
        if envelope.workflow_plan != plan:
            raise ValueError("controller evidence does not contain the selected workflow plan")
        if envelope.claim_id != claim.claim_id or envelope.fence != claim.fence:
            raise ValueError("controller evidence differs from the current Riverhog claim")
        if _target_binding(target_plan) != envelope.target_plan:
            raise ValueError("controller evidence differs from the exact target plan")
        if (
            operation.id != plan.operation.id
            or operation.contract_sha256 != plan.operation.sha256
            or operation.result_kind != plan.result_kind
        ):
            raise ValueError("selected operation declaration differs from the sealed workflow")
        document = evidence.model_dump(mode="json", by_alias=True, exclude_none=True)
        payload = self.api.seal_processing_claim_plan(
            claim.claim_id,
            fence=claim.fence,
            execution_id=envelope.execution_envelope_sha256,
            controller_evidence=document,
            controller_evidence_sha256=riverhog_canonical_json_sha256(document),
            result_kind=plan.result_kind,
            operation_contract=operation.model_dump(
                mode="json", by_alias=True, exclude_none=True, exclude={"contract_sha256"}
            ),
            output_policy=plan.output_policy,
            operation_id=plan.operation.id,
            operation_sha256=plan.operation.sha256,
            input_artifacts=(_artifact_identity(item).as_dict() for item in inputs),
            source_collection_retirement_policy=plan.source_collection_retirement_policy,
            source_collection_retirement_grace_seconds=plan.source_collection_retirement_grace_seconds,
        )
        binding = _claim_binding(payload)
        if binding != claim:
            raise RuntimeError("Riverhog sealed a different claim generation")
        sealed = payload.get("plan")
        if sealed is None or sealed.get("execution_id") != envelope.execution_envelope_sha256:
            raise RuntimeError("Riverhog did not retain the sealed execution identity")

    def target_authority(
        self,
        claim: ClaimBinding,
        evidence: ControllerEvidence,
        target_plan: TargetPlan,
        inputs: Iterable[WorkArtifactSubject],
    ) -> TargetInvocationAuthority:
        envelope = evidence.execution_envelope
        if envelope.claim_id != claim.claim_id or envelope.fence != claim.fence:
            raise ValueError("target evidence differs from the current Riverhog claim")
        if _target_binding(target_plan) != envelope.target_plan:
            raise ValueError("target evidence differs from the exact target plan")
        execution_id = envelope.execution_envelope_sha256
        actions: tuple[CapabilityAction, ...] = (
            ("read-inputs",)
            if envelope.workflow_plan.result_kind == "external-effect"
            else ("read-inputs", "write-output")
        )
        capability = self._capability(
            claim,
            audience=("stove0.target/" + envelope.workflow_plan.target_registration_id),
            actions=actions,
            artifacts=(_artifact_identity(item) for item in inputs),
        )
        principal = capability.get("principal_id")
        expected_principal = (
            f"claim:{claim.claim_id}"
            if envelope.workflow_plan.result_kind == "external-effect"
            else f"processing:{execution_id}"
        )
        if principal != expected_principal:
            raise RuntimeError("Riverhog target capability has an unexpected principal")
        return TargetInvocationAuthority(
            runtime=TargetRuntimeAuthority(
                riverhog_base_url=self.api.base_url,
                capability_token=_token(capability),
                allow_insecure_http=bool(self.api.allow_insecure_http),
            ),
            declared_workspace_protection=self.declared_workspace_protection,
        )

    def verify_and_settle(
        self,
        record: WorkRecord,
        parent_outcome: ParentOutcomeBinding | None = None,
    ) -> tuple[OutputCollectionRef, TargetSettlementAuthority | None]:
        if (
            record.phase != "verifying"
            or record.claim is None
            or record.workflow_plan is None
            or record.controller_evidence is None
            or record.target_status is None
            or record.target_status.state != "succeeded"
            or record.target_status.output_collection is None
            or record.target_status.derivation is None
            or record.target_status.production is None
        ):
            raise ValueError("stove0 work is not ready for Riverhog settlement")
        target_output = record.target_status.output_collection
        derivation = CollectionDerivation.from_mapping(record.target_status.derivation)
        _verify_disposition_identity(
            self.api,
            record,
            derivation,
        )
        self.api.settle_processing_claim(
            record.claim.claim_id,
            fence=record.claim.fence,
            output_collection_id=target_output.collection_id,
            derivation=derivation.as_dict(),
            outcome_claim_id=(parent_outcome.claim.claim_id if parent_outcome else None),
            outcome_fence=(parent_outcome.claim.fence if parent_outcome else None),
            outcome_id=(parent_outcome.outcome_id if parent_outcome else None),
        )
        collection = self.api.get_collection(target_output.collection_id)
        stored = self.api.get_collection_derivation(target_output.collection_id)
        stored_document = stored.get("derivation")
        if not isinstance(stored_document, Mapping):
            raise RuntimeError("Riverhog returned no immutable derivation document")
        verified = CollectionDerivation.from_mapping(stored_document)
        if verified != derivation or stored.get("document_sha256") != derivation.sha256:
            raise RuntimeError("Riverhog derivation differs from the target publication evidence")
        output = OutputCollectionRef.model_validate(
            {
                "collection_id": str(_positive_int(collection.get("id"), "collection id")),
                "archive_root_sha256": _text(
                    collection.get("archive_root_sha256"),
                    "archive-root identity",
                ),
                "content_identity": _text(collection.get("content_identity"), "content identity"),
                "derivation_sha256": derivation.sha256,
            }
        )
        if output != target_output:
            raise RuntimeError("Riverhog output root differs from the target publication receipt")
        settlement = self._advance_settlement(record, output)
        return output, settlement

    def _advance_settlement(
        self,
        record: WorkRecord,
        output: OutputCollectionRef,
    ) -> TargetSettlementAuthority | None:
        assert record.target_status is not None
        assert record.target_status.production is not None
        production = record.target_status.production
        if self.state is None:
            raise RuntimeError("Stove0 settlement requires its durable state authority")
        seal = self.state.ensure_target_settlement_binding(
            TargetSettlementSealRecord(
                work_id=record.work_id,
                job_id=production.job_id,
                output_collection=output,
                production_sha256=production.production_sha256,
                checkpoint=TargetSettlementSealCheckpoint(
                    binding_hash_state=CheckpointSHA256().export_state()
                ),
            )
        )
        if seal.state == "failed":
            raise RuntimeError(seal.failure or "target settlement binding failed")
        if seal.state == "sealed":
            return seal.settlement
        checkpoint = seal.checkpoint
        assert checkpoint is not None
        page = self.api.get_portable_collection_inventory(
            output.collection_id,
            cursor=checkpoint.inventory_cursor,
            limit=self.authority_batch_size,
            inventory_identity=checkpoint.inventory_identity,
        )
        authority = page.authority
        inventory_identity = checkpoint.inventory_identity or authority.inventory_identity
        if (
            authority.inventory_identity != inventory_identity
            or authority.header.collection != output.collection_id
            or authority.header.content_identity != output.content_identity
        ):
            raise RuntimeError("Riverhog output inventory changed during settlement")
        files = tuple(file for file in page.files if not file.path.startswith("riverhog/"))
        declarations = (
            self.state.target_output_path_page(
                record.work_id,
                production.job_id,
                after_path=checkpoint.output_path_cursor,
                limit=len(files),
            )
            if files
            else ()
        )
        if len(declarations) != len(files):
            raise RuntimeError("Riverhog output collection differs from target production")
        digest = CheckpointSHA256.from_state(checkpoint.binding_hash_state)
        artifact_count = checkpoint.artifact_count
        total_bytes = checkpoint.total_bytes
        output_path_cursor = checkpoint.output_path_cursor
        for declared, file in zip(declarations, files, strict=True):
            if (
                declared.path != file.path
                or declared.bytes != file.bytes
                or declared.sha256 != file.sha256
            ):
                raise RuntimeError("Riverhog artifact differs from its target declaration")
            binding = TargetOutputBinding.model_validate(
                dict(
                    output_id=declared.id,
                    role=declared.role,
                    collection=output,
                    path=declared.path,
                    bytes=str(declared.bytes),
                    sha256=declared.sha256,
                    media_type=declared.media_type,
                )
            )
            update_target_output_binding_commitment(
                digest,
                ordinal=artifact_count,
                binding=binding,
            )
            artifact_count += 1
            total_bytes += declared.bytes
            output_path_cursor = declared.path
        if not page.complete and page.next_cursor is None:
            raise RuntimeError("Riverhog output inventory ended without completion")
        next_checkpoint = TargetSettlementSealCheckpoint(
            inventory_identity=inventory_identity,
            inventory_cursor=page.next_cursor,
            output_path_cursor=output_path_cursor,
            binding_hash_state=digest.export_state(),
            artifact_count=artifact_count,
            total_bytes=total_bytes,
        )
        settlement: TargetSettlementAuthority | None = None
        if page.complete:
            if self.state.target_output_path_page(
                record.work_id,
                production.job_id,
                after_path=output_path_cursor,
                limit=1,
            ) or (
                artifact_count != production.outputs.artifact_count
                or total_bytes != production.outputs.total_bytes
            ):
                raise RuntimeError("post-root output bindings differ from target production")
            settlement = TargetSettlementAuthority.seal(
                TargetSettlementAuthorityPayload(
                    job_id=production.job_id,
                    production_sha256=production.production_sha256,
                    output_collection=output,
                    output_bindings=TargetOutputBindingSetIdentity.model_validate(
                        dict(
                            artifact_count=artifact_count,
                            total_bytes=str(total_bytes),
                            sha256=digest.hexdigest(),
                        )
                    ),
                )
            )
        replacement = TargetSettlementSealRecord.model_validate(
            seal.model_copy(
                update={
                    "revision": seal.revision + 1,
                    "state": "sealed" if settlement is not None else "binding",
                    "checkpoint": None if settlement is not None else next_checkpoint,
                    "settlement": settlement,
                }
            ).model_dump(mode="python")
        )
        try:
            sealed = self.state.compare_and_swap_target_settlement_seal(
                record.work_id,
                production.job_id,
                expected_revision=seal.revision,
                replacement=replacement,
            )
        except ConcurrentWorkUpdate:
            concurrent = self.state.load_target_settlement_seal(record.work_id, production.job_id)
            if concurrent is None:
                raise RuntimeError("target settlement binding disappeared") from None
            if (
                concurrent.output_collection != output
                or concurrent.production_sha256 != production.production_sha256
            ):
                raise RuntimeError("target settlement binding changed") from None
            sealed = concurrent
        return sealed.settlement

    def verify_and_settle_effect(
        self,
        record: WorkRecord,
        operation: OperationContract,
        parent_outcome: ParentOutcomeBinding | None = None,
    ) -> str | None:
        if (
            record.phase != "verifying"
            or record.claim is None
            or record.workflow_plan is None
            or record.controller_evidence is None
            or record.target_request is None
            or record.target_status is None
            or record.target_status.state != "succeeded"
            or record.target_status.effect_receipt is None
            or record.workflow_plan.result_kind != "external-effect"
        ):
            raise ValueError("effect settlement requires exact successful target evidence")
        validate_status_against_request(record.target_status, record.target_request, operation)
        claim = self.api.get_processing_claim(record.claim.claim_id)
        plan = claim.plan
        evidence = record.controller_evidence.model_dump(
            mode="json", by_alias=True, exclude_none=True
        )
        if (
            plan is None
            or _claim_binding(claim) != record.claim
            or plan.result_kind != "external-effect"
            or plan.execution_id
            != record.controller_evidence.execution_envelope.execution_envelope_sha256
            or plan.operation.id != operation.id
            or plan.operation.sha256 != operation.contract_sha256
            or plan.controller_evidence_sha256 != riverhog_canonical_json_sha256(evidence)
        ):
            raise RuntimeError("Riverhog effect execution differs from controller authority")
        receipt = record.target_status.effect_receipt
        ordinal = 0
        while True:
            page = self.api.list_processing_claim_artifacts(
                record.claim.claim_id,
                identity_sha256=plan.artifacts.sha256,
                start_ordinal=ordinal,
            )
            if (
                page.start_ordinal != ordinal
                or page.identity.sha256 != plan.artifacts.sha256
                or not page.artifacts
            ):
                raise RuntimeError("Riverhog effect input scope changed during projection")
            self.api.record_processing_claim_dispositions(
                record.claim.claim_id,
                fence=record.claim.fence,
                dispositions=[
                    ArtifactDisposition(
                        input_collection_id=item.collection.collection_id,
                        input_archive_root_sha256=item.collection.archive_root_sha256,
                        input_path=item.path,
                        status="effect-applied",
                        effect_receipt_sha256=receipt.receipt_sha256,
                    ).as_dict()
                    for item in page.artifacts
                ],
            )
            if page.next_ordinal is None:
                break
            if page.next_ordinal != ordinal + len(page.artifacts):
                raise RuntimeError("Riverhog effect input scope has a gap")
            ordinal = page.next_ordinal
        disposition_set = self.api.seal_processing_claim_dispositions(
            record.claim.claim_id, fence=record.claim.fence
        )
        if disposition_set.state == "sealing":
            return None
        if disposition_set.state != "sealed" or disposition_set.identity is None:
            raise RuntimeError(disposition_set.failure or "Riverhog effect disposition seal failed")
        identity = ArtifactDispositionSetIdentity.from_mapping(
            disposition_set.identity.model_dump(mode="json")
        )
        document = ExternalEffectSettlement(
            claim_id=record.claim.claim_id,
            fence=record.claim.fence,
            execution_id=plan.execution_id,
            execution_sha256=receipt.execution_sha256,
            operation=OperationIdentity(operation.id, operation.contract_sha256),
            input_set_sha256=plan.inputs.sha256,
            artifact_set_sha256=plan.artifacts.sha256,
            controller_evidence_sha256=plan.controller_evidence_sha256,
            receipt=receipt.model_dump(
                mode="json", by_alias=True, exclude_none=True, exclude={"receipt_sha256"}
            ),
            receipt_sha256=receipt.receipt_sha256,
            disposition_set=identity,
        )
        settled = self.api.settle_processing_claim_effect(
            record.claim.claim_id,
            fence=record.claim.fence,
            settlement=document.as_dict(),
            outcome_claim_id=parent_outcome.claim.claim_id if parent_outcome else None,
            outcome_fence=parent_outcome.claim.fence if parent_outcome else None,
            outcome_id=parent_outcome.outcome_id if parent_outcome else None,
        )
        if (
            _claim_binding(settled) != record.claim
            or settled.state not in {"settled", "retiring", "released"}
            or settled.effect_settlement_sha256 != document.sha256
        ):
            raise RuntimeError("Riverhog did not durably settle the exact external effect")
        return document.sha256

    def verify_and_settle_no_output(
        self,
        record: WorkRecord,
        no_action: RecipeNoAction,
        source_collection_retirement_policy: SourceCollectionRetirementPolicy,
        source_collection_retirement_grace_seconds: int,
        parent_outcome: ParentOutcomeBinding | None = None,
    ) -> str | None:
        if record.phase != "no_output_pending" or record.claim is None:
            raise ValueError("no-output execution requires a planned Riverhog claim")
        preview = record.no_action_preview
        if preview is None or preview.outcome is None or preview.state != "no_action":
            raise ValueError("no-output execution requires the accepted decision")
        if (
            preview.outcome.code != no_action.code
            or preview.outcome.message != no_action.message
            or record.no_output_retirement_policy != source_collection_retirement_policy
        ):
            raise ValueError("no-output execution differs from the selected recipe decision")
        claim = self.api.get_processing_claim(record.claim.claim_id)
        if _claim_binding(claim) != record.claim:
            raise RuntimeError("Riverhog no-output claim generation changed")
        operation = _no_output_operation(record.work, preview, no_action)
        operation_sha256 = riverhog_canonical_json_sha256(operation)
        decision = _no_output_decision(preview)
        decision_sha256 = riverhog_canonical_json_sha256(decision)
        evidence = {
            "format": "stove0-no-output-controller-evidence/v1",
            "work_id": record.work_id,
            "preview_sha256": preview.preview_sha256,
            "decision_sha256": decision_sha256,
        }
        evidence_sha256 = riverhog_canonical_json_sha256(evidence)
        execution_id = _no_output_execution_id(
            record, operation_sha256=operation_sha256, decision_sha256=decision_sha256
        )
        if claim.plan is None:
            claim = self.api.seal_processing_claim_plan(
                record.claim.claim_id,
                fence=record.claim.fence,
                execution_id=execution_id,
                controller_evidence=evidence,
                controller_evidence_sha256=evidence_sha256,
                operation_id=str(operation["id"]),
                operation_sha256=operation_sha256,
                operation_contract=operation,
                input_artifacts=self._no_output_artifacts(record.work),
                result_kind="no-output",
                source_collection_retirement_policy=source_collection_retirement_policy,
                source_collection_retirement_grace_seconds=(
                    source_collection_retirement_grace_seconds
                ),
            )
        plan = claim.plan
        if (
            plan is None
            or plan.result_kind != "no-output"
            or plan.execution_id != execution_id
            or plan.operation.id != operation["id"]
            or plan.operation.sha256 != operation_sha256
            or plan.controller_evidence_sha256 != evidence_sha256
            or plan.source_collection_retirement_policy != source_collection_retirement_policy
        ):
            raise RuntimeError("Riverhog no-output plan differs from the accepted decision")
        if claim.state == "active":
            consideration_index = (
                _consideration_index(no_action.source_loss, preview)
                if no_action.source_loss is not None
                else None
            )
            if no_action.source_loss is not None:
                selected = {
                    (slot.observation_contract_id, slot.facts_profile_sha256)
                    for slot in no_action.source_loss.evidence_slots
                }
                for observation in preview.observations:
                    profile = observation.result.facts_schema
                    if (
                        profile is None
                        or (
                            observation.request.observer_contract_id,
                            profile.profile_sha256,
                        )
                        not in selected
                    ):
                        continue
                    evidence_document = {
                        "request": observation.request.model_dump(
                            mode="json", by_alias=True, exclude_none=True
                        ),
                        "result": observation.result.model_dump(
                            mode="json", by_alias=True, exclude_none=True
                        ),
                    }
                    observation_sha256 = riverhog_canonical_json_sha256(evidence_document)
                    retained = self.api.record_processing_claim_consideration_evidence(
                        record.claim.claim_id,
                        fence=record.claim.fence,
                        document=evidence_document,
                        sha256=observation_sha256,
                    )
                    if retained.sha256 != observation_sha256:
                        raise RuntimeError("Riverhog retained another consideration document")
            ordinal = 0
            while True:
                page = self.api.list_processing_claim_artifacts(
                    record.claim.claim_id,
                    identity_sha256=plan.artifacts.sha256,
                    start_ordinal=ordinal,
                )
                if (
                    page.start_ordinal != ordinal
                    or page.identity.sha256 != plan.artifacts.sha256
                    or not page.artifacts
                ):
                    raise RuntimeError("Riverhog no-output input scope changed")
                dispositions: list[dict[str, object]] = []
                for item in page.artifacts:
                    subject = CollectionArtifactIdentity(
                        collection=CollectionRootIdentity(
                            item.collection.collection_id,
                            item.collection.archive_root_sha256,
                            item.collection.content_identity,
                        ),
                        path=item.path,
                        bytes=item.bytes,
                        sha256=item.sha256,
                    )
                    approval = (
                        _no_output_discard_approval(
                            subject,
                            no_action.source_loss,
                            preview,
                            controller_id=claim.consumer.app,
                            reason=no_action.message,
                            index=consideration_index,
                        )
                        if no_action.source_loss is not None
                        else None
                    )
                    dispositions.append(
                        ArtifactDisposition(
                            input_collection_id=item.collection.collection_id,
                            input_archive_root_sha256=item.collection.archive_root_sha256,
                            input_path=item.path,
                            status="not-carried-forward",
                            code=preview.outcome.code,
                            message=preview.outcome.message,
                            discard_approval=approval,
                        ).as_dict()
                    )
                self.api.record_processing_claim_dispositions(
                    record.claim.claim_id,
                    fence=record.claim.fence,
                    dispositions=dispositions,
                )
                if page.next_ordinal is None:
                    break
                if page.next_ordinal != ordinal + len(page.artifacts):
                    raise RuntimeError("Riverhog no-output input scope has a gap")
                ordinal = page.next_ordinal
            status = self.api.seal_processing_claim_dispositions(
                record.claim.claim_id, fence=record.claim.fence
            )
        else:
            status = self.api.get_processing_claim_dispositions(record.claim.claim_id)
        if status.state == "sealing":
            return None
        if status.state != "sealed" or status.identity is None:
            raise RuntimeError(status.failure or "Riverhog no-output disposition seal failed")
        identity = ArtifactDispositionSetIdentity.from_mapping(
            status.identity.model_dump(mode="json")
        )
        document = NoOutputSettlement(
            claim_id=record.claim.claim_id,
            fence=record.claim.fence,
            execution_id=execution_id,
            operation=OperationIdentity(str(operation["id"]), operation_sha256),
            input_set_sha256=plan.inputs.sha256,
            artifact_set_sha256=plan.artifacts.sha256,
            controller_evidence_sha256=evidence_sha256,
            decision=decision,
            decision_sha256=decision_sha256,
            disposition_set=identity,
        )
        settled = self.api.settle_processing_claim_no_output(
            record.claim.claim_id,
            fence=record.claim.fence,
            settlement=document.as_dict(),
            outcome_claim_id=parent_outcome.claim.claim_id if parent_outcome else None,
            outcome_fence=parent_outcome.claim.fence if parent_outcome else None,
            outcome_id=parent_outcome.outcome_id if parent_outcome else None,
        )
        if (
            _claim_binding(settled) != record.claim
            or settled.state not in {"settled", "retiring", "released"}
            or settled.no_output_settlement_sha256 != document.sha256
        ):
            raise RuntimeError("Riverhog did not settle the exact no-output decision")
        return document.sha256

    def _no_output_artifacts(self, work: WorkIdentity) -> Iterable[Mapping[str, object]]:
        for root in work.inputs:
            current = self.api.get_collection(root.collection_id)
            if (
                current.get("archive_root_sha256") != root.archive_root_sha256
                or current.get("content_identity") != root.content_identity
            ):
                raise RuntimeError("no-output source collection root changed")
            identity: str | None = None
            cursor: str | None = None
            while True:
                page = self.api.get_portable_collection_inventory(
                    root.collection_id,
                    cursor=cursor,
                    limit=1000,
                    inventory_identity=identity,
                )
                if identity is None:
                    identity = page.authority.inventory_identity
                elif page.authority.inventory_identity != identity:
                    raise RuntimeError("no-output source inventory changed")
                for item in page.files:
                    yield CollectionArtifactIdentity(
                        collection=CollectionRootIdentity(
                            root.collection_id,
                            root.archive_root_sha256,
                            root.content_identity,
                        ),
                        path=item.path,
                        bytes=item.bytes,
                        sha256=item.sha256,
                    ).as_dict()
                if page.complete:
                    break
                if page.next_cursor is None:
                    raise RuntimeError("no-output source inventory continuation is missing")
                cursor = page.next_cursor

    def settle_outcomes(
        self,
        record: WorkRecord,
        evaluation: BranchSetEvaluation,
    ) -> bool:
        if (
            record.claim is None
            or record.branch_set_plan is None
            or not evaluation.branch_set_succeeded
            or evaluation.branch_set_sha256 != record.branch_set_plan.branch_set_sha256
            or self.state is None
        ):
            raise ValueError(
                "exact outcome settlement requires successful coordination and durable child state"
            )
        expected: list[CollectionProcessingOutcomeIdentity] = []
        for item in evaluation.succeeded_branches:
            child = self._settled_child(item.work_id)
            if (
                child.output is None
                or child.target_settlement is None
                or child.target_settlement.settlement_sha256 != item.producer_settlement_sha256
                or child.output.derivation_sha256 != item.derivation_sha256
                or child.output.collection_id != item.output_collection.collection_id
                or child.output.archive_root_sha256 != item.output_collection.archive_root_sha256
                or child.output.content_identity != item.output_collection.content_identity
            ):
                raise RuntimeError(
                    "collection branch result differs from its durable settled child"
                )
            expected.append(self._child_outcome(f"branch/{item.branch_id}", child))
        for effect_result in evaluation.succeeded_effects:
            child = self._settled_child(effect_result.work_id)
            if (
                child.effect_settlement_sha256 != effect_result.effect_settlement_sha256
                or child.target_status is None
                or child.target_status.effect_receipt is None
                or child.target_status.effect_receipt.receipt_sha256
                != effect_result.effect_receipt_sha256
            ):
                raise RuntimeError("effect branch result differs from its Riverhog settlement")
            expected.append(self._child_outcome(f"branch/{effect_result.branch_id}", child))
        for no_output_result in evaluation.succeeded_no_outputs:
            child = self._settled_child(no_output_result.work_id)
            if child.no_output_settlement_sha256 != no_output_result.no_output_settlement_sha256:
                raise RuntimeError("no-output branch differs from its Riverhog settlement")
            expected.append(self._child_outcome(f"branch/{no_output_result.branch_id}", child))
        for nested_result in evaluation.succeeded_coordinations:
            nested_child = self.state.load(nested_result.work.work_id)
            if (
                nested_child is None
                or nested_child.phase != "complete"
                or nested_child.coordination_settlement != nested_result
                or nested_child.claim is None
            ):
                raise RuntimeError(
                    "nested coordination differs from its complete durable nested_child"
                )
            remote = self.api.get_processing_claim(nested_child.claim.claim_id)
            if (
                _claim_binding(remote) != nested_child.claim
                or remote.outcomes.identity is None
                or remote.state not in {"settled", "retiring", "released"}
            ):
                raise RuntimeError("nested coordination has no exact Riverhog outcome settlement")
            for outcome in self._outcomes(
                nested_child.claim.claim_id, remote.outcomes.identity.sha256
            ):
                label = "nested/" + riverhog_canonical_json_sha256(
                    {
                        "work_id": nested_result.work.work_id,
                        "outcome_id": outcome.outcome_id,
                    }
                )
                expected.append(
                    CollectionProcessingOutcomeIdentity.from_mapping(
                        {**outcome.as_dict(), "outcome_id": label}
                    )
                )
        if evaluation.join_settlement is not None:
            join = evaluation.join_settlement
            child = self._settled_child(join.work_id)
            if (
                child.target_settlement is None
                or child.target_settlement.settlement_sha256 != join.producer_settlement_sha256
            ):
                raise RuntimeError("join result differs from its durable settlement")
            expected.append(self._child_outcome("join", child))
        # Never replace an exact closed set with a locally inferred successful subset.
        required = processing_outcome_set_identity(expected)
        ordered = sorted(expected, key=lambda item: item.outcome_id)
        for start in range(0, len(ordered), DISPOSITION_BATCH_MAX):
            self.api.append_processing_claim_outcomes(
                record.claim.claim_id,
                fence=record.claim.fence,
                outcomes=[
                    item.as_dict() for item in ordered[start : start + DISPOSITION_BATCH_MAX]
                ],
            )
        payload = self.api.settle_processing_claim_outcomes(
            record.claim.claim_id,
            fence=record.claim.fence,
            outcomes_count=len(ordered),
            outcomes_sha256=str(required["sha256"]),
            source_collection_retirement_policy=record.branch_set_plan.source_collection_retirement_policy,
            source_collection_retirement_grace_seconds=record.branch_set_plan.source_collection_retirement_grace_seconds,
        )
        if _claim_binding(payload) != record.claim:
            raise RuntimeError("Riverhog settled another claim generation")
        if payload.state == "active":
            return False
        if (
            payload.state not in {"settled", "retiring", "released"}
            or payload.outcomes.identity is None
            or payload.outcomes.identity.model_dump(mode="json") != required
        ):
            raise RuntimeError("Riverhog did not seal the exact required processing outcomes")
        if self._outcomes(record.claim.claim_id, str(required["sha256"])) != ordered:
            raise RuntimeError("Riverhog processing outcomes differ from Stove0 truth")
        return True

    def _settled_child(self, work_id: str) -> WorkRecord:
        if self.state is None:
            raise RuntimeError("outcome verification requires durable child state")
        child = self.state.load(work_id)
        if (
            child is None
            or child.phase not in {"complete", "no_action"}
            or child.claim is None
            or (
                child.phase == "complete"
                and (child.controller_evidence is None or child.workflow_plan is None)
            )
            or (
                child.phase == "no_action"
                and (child.no_action_preview is None or child.no_output_settlement_sha256 is None)
            )
        ):
            raise RuntimeError("outcome child has not completed exact durable settlement")
        return child

    def _child_outcome(self, label: str, child: WorkRecord) -> CollectionProcessingOutcomeIdentity:
        assert child.claim is not None
        if child.phase == "no_action":
            assert child.no_action_preview is not None
            assert child.no_output_settlement_sha256 is not None
            remote = self.api.get_processing_claim(child.claim.claim_id)
            if (
                _claim_binding(remote) != child.claim
                or remote.plan is None
                or remote.plan.result_kind != "no-output"
                or remote.no_output_settlement_sha256 != child.no_output_settlement_sha256
            ):
                raise RuntimeError("no-output child has no exact Riverhog settlement")
            return CollectionProcessingOutcomeIdentity(
                outcome_id=label,
                source_claim_id=child.claim.claim_id,
                source_fence=child.claim.fence,
                execution_id=remote.plan.execution_id,
                result_kind="no-output",
                no_output_settlement_sha256=child.no_output_settlement_sha256,
            )
        assert child.controller_evidence is not None and child.workflow_plan is not None
        execution_id = child.controller_evidence.execution_envelope.execution_envelope_sha256
        if child.workflow_plan.result_kind == "external-effect":
            assert (
                child.target_status is not None and child.target_status.effect_receipt is not None
            )
            return CollectionProcessingOutcomeIdentity(
                outcome_id=label,
                source_claim_id=child.claim.claim_id,
                source_fence=child.claim.fence,
                execution_id=execution_id,
                result_kind="external-effect",
                effect_receipt_sha256=child.target_status.effect_receipt.receipt_sha256,
                effect_settlement_sha256=child.effect_settlement_sha256,
            )
        assert child.output is not None
        return CollectionProcessingOutcomeIdentity(
            outcome_id=label,
            source_claim_id=child.claim.claim_id,
            source_fence=child.claim.fence,
            execution_id=execution_id,
            result_kind="collection",
            output_collection=CollectionRootIdentity(
                child.output.collection_id,
                child.output.archive_root_sha256,
                child.output.content_identity,
            ),
            derivation_sha256=child.output.derivation_sha256,
        )

    def _outcomes(self, claim_id: str, identity: str) -> list[CollectionProcessingOutcomeIdentity]:
        outcomes: list[CollectionProcessingOutcomeIdentity] = []
        ordinal = 0
        while True:
            page = self.api.list_processing_claim_outcomes(
                claim_id, identity_sha256=identity, start_ordinal=ordinal
            )
            if page.start_ordinal != ordinal or page.identity.sha256 != identity:
                raise RuntimeError("Riverhog outcome page changed identity or position")
            outcomes.extend(
                CollectionProcessingOutcomeIdentity.from_mapping(
                    item.model_dump(mode="json", exclude_none=True)
                )
                for item in page.outcomes
            )
            if page.next_ordinal is None:
                if len(outcomes) != page.identity.count:
                    raise RuntimeError("Riverhog outcome page omitted required results")
                return outcomes
            if page.next_ordinal <= ordinal:
                raise RuntimeError("Riverhog outcome paging did not advance")
            ordinal = page.next_ordinal

    def abandon_preview_claim(
        self,
        request: WorkflowPreviewRequest,
        claim: ClaimBinding,
    ) -> None:
        payload = self.api.abandon_processing_claim(
            claim.claim_id,
            fence=claim.fence,
            reason=f"preview-complete:{request.preview_id}",
        )
        if payload.get("state") != "abandoned" or _claim_binding(payload) != claim:
            raise RuntimeError("Riverhog did not abandon the expected preview claim")

    def begin_source_collection_retirement(self, record: WorkRecord) -> bool:
        claim = _record_claim(record)
        payload = self.api.begin_source_collection_retirement(
            claim.claim_id,
            fence=claim.fence,
        )
        if _claim_binding(payload) != claim:
            raise RuntimeError("Riverhog returned another source collection retirement claim")
        state = payload.get("state")
        if state == "settled":
            return False
        if state != "retiring":
            raise RuntimeError(
                "Riverhog did not enter the expected source collection retirement claim"
            )
        return True

    def abandon_claim(self, record: WorkRecord) -> None:
        claim = _record_claim(record)
        payload = self.api.abandon_processing_claim(
            claim.claim_id,
            fence=claim.fence,
            reason=_abandonment_reason(record),
        )
        if payload.get("state") != "abandoned" or _claim_binding(payload) != claim:
            raise RuntimeError("Riverhog did not abandon the expected processing claim")

    def retire_source_collection(self, record: WorkRecord, collection_id: int) -> bool:
        claim = _record_claim(record)
        if int(collection_id) not in {item.collection_id for item in record.work.inputs}:
            raise ValueError("source collection for retirement is outside the stove0 work")
        try:
            plan = self.api.plan_collection_deletion(
                int(collection_id),
                source_collection_retirement_claim_id=claim.claim_id,
            )
        except NotFound:
            # A prior attempt may have deleted the exact immutable input before
            # stove0 durably recorded the phase transition. Absence is the
            # idempotent success condition; Riverhog still gates final release.
            return True
        blockers = plan.get("blockers")
        challenge = plan.get("challenge")
        if blockers:
            return False
        if plan.get("status") != "ready" or not isinstance(challenge, str) or not challenge:
            raise RuntimeError("Riverhog did not return a ready source collection deletion plan")
        result = self.api.delete_collection(
            int(collection_id),
            challenge=challenge,
            source_collection_retirement_claim_id=claim.claim_id,
            event_context={
                "initiator": {
                    "app": "stove0",
                    "claim_id": claim.claim_id,
                    "fence": claim.fence,
                    "work_id": record.work_id,
                }
            },
        )
        if result.get("status") not in {"deleted", "already_absent"}:
            raise RuntimeError("Riverhog did not confirm source collection deletion")
        return True

    def release_claim(self, record: WorkRecord) -> None:
        claim = _record_claim(record)
        payload = self.api.release_processing_claim(
            claim.claim_id,
            fence=claim.fence,
        )
        if payload.get("state") != "released" or _claim_binding(payload) != claim:
            raise RuntimeError("Riverhog did not release the expected claim")

    def _acquire_document_claim(
        self,
        *,
        identity: str,
        document: Mapping[str, object],
        inputs: Iterable[Mapping[str, object]],
        purpose: str,
    ) -> ClaimBinding:
        payload = self.api.create_or_resume_processing_claim(
            work_id=identity,
            work_document=dict(document),
            work_document_sha256=riverhog_canonical_json_sha256(document),
            inputs=(dict(item) for item in inputs),
            lease_seconds=self.claim_lease_seconds,
            purpose=purpose,
        )
        if payload.get("work_id") != identity:
            raise RuntimeError("Riverhog claim differs from the requested identity")
        state = str(payload.get("state") or "")
        if state != "active":
            raise RuntimeError(f"Riverhog processing claim is terminal: {state or 'unknown'}")
        return _claim_binding(payload)

    def _capability(
        self,
        claim: ClaimBinding,
        *,
        audience: str,
        actions: Sequence[CapabilityAction],
        artifacts: Iterable[CollectionArtifactIdentity],
    ) -> ProcessingCapabilityDocument:
        payload = self.api.create_processing_capability(
            claim.claim_id,
            fence=claim.fence,
            audience=audience,
            actions=tuple(actions),
            artifacts=(item.as_dict() for item in artifacts),
            ttl_seconds=self.capability_ttl_seconds,
        )
        if (
            payload.get("claim_id") != claim.claim_id
            or _positive_int(payload.get("fence"), "capability fence") != claim.fence
            or payload.get("audience") != audience
            or tuple(payload.get("actions", ())) != tuple(sorted(set(actions)))
        ):
            raise RuntimeError("Riverhog returned an inconsistent scoped capability")
        return payload


def _artifact_identity(
    value: WorkArtifactSubject | InputArtifact,
) -> CollectionArtifactIdentity:
    return CollectionArtifactIdentity(
        collection=CollectionRootIdentity(
            collection_id=value.collection.collection_id,
            archive_root_sha256=value.collection.archive_root_sha256,
            content_identity=value.collection.content_identity,
        ),
        path=value.path,
        bytes=value.bytes,
        sha256=value.sha256,
    )


def _no_output_operation(
    work: WorkIdentity, preview: WorkflowPreview, no_action: RecipeNoAction
) -> dict[str, object]:
    outcome = preview.outcome
    assert outcome is not None
    operation: dict[str, object] = {
        "id": "stove0.no-action/v1",
        "result_kind": "no-output",
        "source_collection_retirement_permitted": no_action.source_loss is not None,
        "recipe": work.recipe.model_dump(mode="json"),
        "decision_code": outcome.code,
    }
    if no_action.source_loss is not None:
        operation["source_loss"] = {
            "rule_sha256": no_action.source_loss.sha256,
            "rule": no_action.source_loss.model_dump(mode="json", exclude_none=True),
            "evidence_slots": [
                {
                    "id": slot.observation_contract_id,
                    "contract_sha256": slot.observation_contract_sha256,
                    "profile_sha256": slot.facts_profile_sha256,
                }
                for slot in no_action.source_loss.evidence_slots
            ],
        }
    return operation


def _no_output_decision(preview: WorkflowPreview) -> dict[str, object]:
    outcome = preview.outcome
    assert outcome is not None
    return {
        "format": "stove0-no-output-decision/v1",
        "preview_sha256": preview.preview_sha256,
        "outcome": {"code": outcome.code, "message": outcome.message},
    }


def _no_output_discard_approval(
    subject: CollectionArtifactIdentity,
    rule: RecipeSourceLossRule,
    preview: WorkflowPreview,
    *,
    controller_id: str,
    reason: str,
    index: dict[
        tuple[str, str, CollectionArtifactIdentity],
        list[tuple[str, list[dict[str, object]]]],
    ]
    | None = None,
) -> ArtifactDiscardApproval | None:
    """Endorse only a universal, exact subject-keyed decision under the selected rule."""

    matches_by_subject = index if index is not None else _consideration_index(rule, preview)
    slots: list[dict[str, object]] = []
    for required in rule.evidence_slots:
        matches = matches_by_subject.get(
            (required.observation_contract_id, required.facts_profile_sha256, subject), []
        )
        if len(matches) != 1:
            return None
        document_sha256, matching_records = matches[0]
        if len(matching_records) != 1:
            return None
        present, verdict = _consideration_pointer(matching_records[0], required.verdict_pointer)
        if not present or verdict != required.verdict_value:
            return None
        slots.append(
            {
                "id": required.observation_contract_id,
                "contract_sha256": required.observation_contract_sha256,
                "profile_sha256": required.facts_profile_sha256,
                "document_sha256": document_sha256,
            }
        )
    evidence = {
        "format": "riverhog-artifact-consideration/v1",
        "subject": subject.as_dict(),
        "slots": slots,
        "reason": reason,
    }
    return ArtifactDiscardApproval(
        controller_id=controller_id,
        rule_sha256=rule.sha256,
        evidence_json=riverhog_canonical_json_bytes(evidence).decode("utf-8"),
        evidence_sha256=riverhog_canonical_json_sha256(evidence),
    )


def _consideration_index(
    rule: RecipeSourceLossRule, preview: WorkflowPreview
) -> dict[
    tuple[str, str, CollectionArtifactIdentity],
    list[tuple[str, list[dict[str, object]]]],
]:
    index: dict[
        tuple[str, str, CollectionArtifactIdentity],
        list[tuple[str, list[dict[str, object]]]],
    ] = {}
    document_hashes: dict[str, str] = {}
    for required in rule.evidence_slots:
        for observation in preview.observations:
            profile = observation.result.facts_schema
            if (
                observation.request.observer_contract_id != required.observation_contract_id
                or observation.request.observer_contract_sha256
                != required.observation_contract_sha256
                or profile is None
                or profile.profile_sha256 != required.facts_profile_sha256
            ):
                continue
            facts = observation.result.facts
            if facts is None:
                continue
            present, records = _consideration_pointer(
                facts, required.artifact_facts.records_pointer
            )
            if not present or not isinstance(records, list):
                continue
            document_sha256 = document_hashes.get(observation.request.request_id)
            if document_sha256 is None:
                document_sha256 = riverhog_canonical_json_sha256(
                    {
                        "request": observation.request.model_dump(
                            mode="json", by_alias=True, exclude_none=True
                        ),
                        "result": observation.result.model_dump(
                            mode="json", by_alias=True, exclude_none=True
                        ),
                    }
                )
                document_hashes[observation.request.request_id] = document_sha256
            records_by_id: dict[str, list[dict[str, object]]] = {}
            for record in records:
                if not isinstance(record, dict):
                    continue
                present, artifact_id = _consideration_pointer(
                    record, required.artifact_facts.artifact_id_pointer
                )
                if present and isinstance(artifact_id, str):
                    records_by_id.setdefault(artifact_id, []).append(record)
            for candidate in observation.request.subjects:
                key = (
                    required.observation_contract_id,
                    required.facts_profile_sha256,
                    _artifact_identity(candidate),
                )
                index.setdefault(key, []).append(
                    (document_sha256, records_by_id.get(candidate.id, []))
                )
    return index


def _consideration_pointer(document: object, pointer: str) -> tuple[bool, object]:
    current = document
    if pointer == "":
        return True, current
    for raw in pointer.split("/")[1:]:
        token = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict) and token in current:
            current = current[token]
        elif isinstance(current, list) and token.isdigit() and int(token) < len(current):
            current = current[int(token)]
        else:
            return False, None
    return True, current


def _no_output_execution_id(
    record: WorkRecord, *, operation_sha256: str, decision_sha256: str
) -> str:
    if record.claim is None or record.no_action_preview is None:
        raise ValueError("no-output execution requires the accepted claim and decision")
    return riverhog_canonical_json_sha256(
        {
            "format": "stove0-no-output-execution/v1",
            "claim_id": record.claim.claim_id,
            "fence": str(record.claim.fence),
            "decision_sha256": decision_sha256,
            "operation_sha256": operation_sha256,
        }
    )


def _target_binding(plan: TargetPlan) -> TargetPlanBinding:
    return TargetPlanBinding(
        protocol=plan.protocol,
        target_implementation_id=plan.target_implementation_id,
        target_descriptor_sha256=plan.target_descriptor_sha256,
        operation_contract_sha256=plan.operation_contract_sha256,
        plan=plan.binding_document(),
        plan_sha256=plan.plan_sha256,
    )


def _record_claim(record: WorkRecord) -> ClaimBinding:
    if record.claim is None:
        raise ValueError("stove0 work has no Riverhog claim")
    return record.claim


def _abandonment_reason(record: WorkRecord) -> str:
    outcome = record.abandon_outcome
    if outcome == "inapplicable" and record.inapplicable is not None:
        return f"inapplicable:{record.inapplicable.code}: {record.inapplicable.message}"
    if outcome == "failed" and record.failure is not None:
        return f"failed:{record.failure.code}: {record.failure.message}"
    if outcome == "canceled":
        return "canceled: stove0 work was canceled before Riverhog settlement"
    raise ValueError("stove0 work has no terminal claim-abandonment outcome")


def _claim_binding(value: ProcessingClaimDocument | Mapping[str, Any]) -> ClaimBinding:
    return ClaimBinding(
        claim_id=_text(value.get("id"), "claim id"),
        fence=_positive_int(value.get("fence"), "claim fence"),
    )


def _token(value: ProcessingCapabilityDocument | Mapping[str, Any]) -> str:
    return _text(value.get("token"), "capability token")


def _text(value: object, label: str) -> str:
    text = str(value or "")
    if not text or text != text.strip():
        raise RuntimeError(f"Riverhog returned an invalid {label}")
    return text


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"Riverhog returned an invalid {label}")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Riverhog returned an invalid {label}") from exc
    if parsed < 1:
        raise RuntimeError(f"Riverhog returned an invalid {label}")
    return parsed


__all__ = ["RiverhogApi", "Stove0RiverhogClient"]
