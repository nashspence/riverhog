"""Deterministic stove0 coordinator over narrow injected authorities.

This module is deliberately transport- and persistence-neutral. It owns no
payload bytes, format semantics, observer implementation, target implementation,
or Riverhog database access. Production controller and worker roles may split the
ports across processes while sharing the same durable :class:`WorkRecord`.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal, Protocol

from http_api_contracts.control import ControlBudgetExhausted
from riverhog_protocol.collection_workflows import SourceCollectionRetirementPolicy
from riverhog_protocol.workspace_protection import DeclaredWorkspaceProtection
from stove0_observer_client import ContentObserverClient
from stove0_observer_protocol import (
    AcceptedObservationJob,
    ContentObservationEvidence,
    ContentObservationInvocation,
    ContentObservationRequest,
    ContentObservationResult,
    ObservationJobStatus,
    ObserverDescriptor,
    ObserverRuntimeAuthority,
    SemanticValidatorProvider,
)
from stove0_protocol import (
    ArtifactSelection,
    BranchSetEvaluation,
    BranchWorkBinding,
    ControllerEvidence,
    JoinWorkBinding,
    OperationIdentityRef,
    PreviewOutcome,
    WorkArtifactSubject,
    WorkflowPlan,
    WorkflowPreview,
    WorkflowPreviewPayload,
    WorkflowPreviewRequest,
    WorkflowPreviewRequestPayload,
    WorkIdentity,
    branch_work,
)
from stove0_protocol.no_output_decisions import CompiledNoOutputDecision
from stove0_protocol.recipe_outcomes import NoOutputDefinition
from stove0_target_client import TargetClient
from stove0_target_protocol import (
    AcceptedTargetJob,
    OperationContract,
    OutputCollectionRef,
    TargetCallbackAccess,
    TargetDescriptor,
    TargetJobDeclaration,
    TargetJobRequest,
    TargetJobStatus,
    TargetPlan,
    TargetPreflightRequest,
    TargetPreflightResponse,
    TargetRuntimeAuthority,
    TargetSettlementAuthority,
    validate_preflight_response_against_request,
)

from stove0_core.control_contacts import ControlContacts
from stove0_core.coordination import project_coordination
from stove0_core.metadata_steps import advance_planning, planning_control_budget
from stove0_core.observation_state import ObservationDeliveryPort, ObservationOwnerKind
from stove0_core.planning_progress import PlanningProgress
from stove0_core.work_state import (
    ClaimBinding,
    Stove0WorkService,
    WorkFailure,
    WorkInapplicable,
    WorkNoAction,
    WorkRecord,
)


@dataclass(frozen=True, slots=True)
class TargetInvocationAuthority:
    runtime: TargetRuntimeAuthority
    declared_workspace_protection: DeclaredWorkspaceProtection


@dataclass(frozen=True, slots=True)
class ParentOutcomeBinding:
    """Exact parent claim outcome receiving one verified ordinary work output."""

    claim: ClaimBinding
    outcome_id: str


def _raise_observation_outcome(result: ContentObservationResult) -> None:
    if result.state == "inapplicable":
        assert result.inapplicable is not None
        raise PlanningObservationTerminal(
            state="inapplicable", code=result.inapplicable.code, message=result.inapplicable.message
        )
    if result.state == "failed":
        assert result.failure is not None
        raise PlanningObservationTerminal(
            state="failed",
            code=result.failure.code,
            message=result.failure.message,
            retryable=result.failure.retryable,
        )
    raise PlanningObservationTerminal(
        state="canceled",
        code="observer-canceled",
        message="The observer canceled the planning question.",
    )


class ObservationAuthorityPort(Protocol):
    def observation_authority(
        self,
        claim: ClaimBinding,
        request: ContentObservationRequest,
        *,
        owner_kind: str,
        owner_id: str,
    ) -> ObserverRuntimeAuthority | None: ...


class RiverhogControlPort(ObservationAuthorityPort, Protocol):
    """Riverhog claim/capability/verification authority used by stove0."""

    def acquire_claim(self, work: WorkIdentity) -> ClaimBinding | None: ...

    def renew_claim(self, work: WorkIdentity, claim: ClaimBinding) -> ClaimBinding: ...

    def restart_claim(self, work: WorkIdentity, claim: ClaimBinding) -> ClaimBinding: ...

    def seal_execution(
        self,
        claim: ClaimBinding,
        evidence: ControllerEvidence,
        plan: WorkflowPlan,
        target_plan: TargetPlan,
        inputs: Iterable[WorkArtifactSubject],
        operation: OperationContract,
    ) -> bool: ...

    def target_authority(
        self,
        claim: ClaimBinding,
        evidence: ControllerEvidence,
        target_plan: TargetPlan,
        inputs: Iterable[WorkArtifactSubject],
    ) -> TargetInvocationAuthority | None: ...

    def verify_and_settle(
        self,
        record: WorkRecord,
        parent_outcome: ParentOutcomeBinding | None = None,
    ) -> tuple[OutputCollectionRef, TargetSettlementAuthority | None]: ...

    def verify_and_settle_effect(
        self,
        record: WorkRecord,
        operation: OperationContract,
        parent_outcome: ParentOutcomeBinding | None = None,
    ) -> str | None: ...

    def verify_and_settle_no_output(
        self,
        record: WorkRecord,
        no_action: NoOutputDefinition,
        source_collection_retirement_policy: SourceCollectionRetirementPolicy,
        source_collection_retirement_grace_seconds: int,
        parent_outcome: ParentOutcomeBinding | None = None,
    ) -> str | None: ...

    def settle_outcomes(
        self,
        record: WorkRecord,
        evaluation: BranchSetEvaluation,
    ) -> bool: ...

    def abandon_claim(self, record: WorkRecord) -> None: ...

    def begin_source_collection_retirement(self, record: WorkRecord) -> bool: ...

    def retire_source_collection(self, record: WorkRecord, collection_id: int) -> bool: ...

    def release_claim(self, record: WorkRecord) -> None: ...


class PlanningPort(Protocol):
    """Recipe/policy authority; implementations may not inspect content bytes."""

    def for_invocation(self, owner_kind: str, owner_id: str) -> PlanningPort: ...

    def step(self, work: WorkIdentity) -> PlanningProgress: ...

    def deliver_observation(
        self,
        progress: PlanningProgress,
        *,
        owner_kind: ObservationOwnerKind,
        owner_id: str,
        claim: ClaimBinding,
        riverhog: ObservationAuthorityPort,
        deliveries: ObservationDeliveryPort,
    ) -> ContentObservationResult | None: ...

    def accepted_evidence(self, work: WorkIdentity) -> tuple[ContentObservationEvidence, ...]: ...

    def no_output_policy(
        self, work: WorkIdentity, *, decision: CompiledNoOutputDecision
    ) -> tuple[NoOutputDefinition, SourceCollectionRetirementPolicy, int]: ...

    def target_preflight_request(
        self,
        plan: WorkflowPlan,
        selections: dict[str, ArtifactSelection],
        *,
        descriptor: TargetDescriptor,
    ) -> TargetPreflightRequest: ...

    def target_input_selection(
        self,
        plan: WorkflowPlan,
        selections: dict[str, ArtifactSelection],
    ) -> ArtifactSelection: ...

    def operation_contract(self, operation: OperationIdentityRef) -> OperationContract: ...


class ObserverPort(Protocol):
    def registration_ids(self) -> tuple[str, ...]: ...

    def semantic_validators(self, registration_id: str) -> SemanticValidatorProvider | None: ...

    def descriptor(self, registration_id: str) -> ObserverDescriptor: ...

    def put_job(
        self,
        registration_id: str,
        invocation: ContentObservationInvocation,
        *,
        descriptor: ObserverDescriptor,
    ) -> ObservationJobStatus: ...

    def get_job(
        self,
        registration_id: str,
        accepted: AcceptedObservationJob,
        *,
        descriptor: ObserverDescriptor,
    ) -> ObservationJobStatus: ...

    def cancel_job(
        self,
        registration_id: str,
        accepted: AcceptedObservationJob,
        *,
        descriptor: ObserverDescriptor,
    ) -> ObservationJobStatus: ...


class TargetPort(Protocol):
    def registration_ids(self) -> tuple[str, ...]: ...

    def descriptor(self, registration_id: str) -> TargetDescriptor: ...

    def preflight(
        self,
        registration_id: str,
        request: TargetPreflightRequest,
    ) -> TargetPreflightResponse: ...

    def put_job(
        self,
        registration_id: str,
        request: TargetJobRequest,
        *,
        operation: OperationContract,
    ) -> TargetJobStatus: ...

    def get_job(
        self,
        registration_id: str,
        request: TargetJobRequest | AcceptedTargetJob,
        *,
        operation: OperationContract,
    ) -> TargetJobStatus: ...

    def cancel_job(
        self,
        registration_id: str,
        request: TargetJobRequest | AcceptedTargetJob,
        *,
        operation: OperationContract,
    ) -> TargetJobStatus: ...


class TargetCallbackPort(Protocol):
    """Stove0-owned capability issuer for one independently deployed target."""

    def issue_access(
        self,
        record: WorkRecord,
        target_registration_id: str,
    ) -> TargetCallbackAccess: ...


class PlanningObservationPending(RuntimeError):
    """A durable nested observation remains pending without a payload wait."""


class PlanningObservationTerminal(RuntimeError):
    """Truthful terminal result from an observation required during tree planning."""

    def __init__(
        self,
        *,
        state: Literal["inapplicable", "failed", "canceled"],
        code: str,
        message: str,
        retryable: bool | None = None,
    ) -> None:
        super().__init__(message)
        self.state = state
        self.code = code
        self.message = message
        self.retryable = retryable


class HttpObserverPort:
    """Explicit configuration-backed observer registry using the v1 HTTP client."""

    def __init__(self, registrations: dict[str, ContentObserverClient]) -> None:
        self._registrations = dict(registrations)
        self._contacts = ControlContacts(registrations)

    def registration_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._registrations))

    def close(self) -> None:
        for client in self._registrations.values():
            client.close()

    def semantic_validators(self, registration_id: str) -> SemanticValidatorProvider | None:
        return self._client(registration_id).semantic_validators

    def descriptor(self, registration_id: str) -> ObserverDescriptor:
        return self._contacts.call(
            registration_id, lambda: self._client(registration_id).descriptor()
        )

    def put_job(
        self,
        registration_id: str,
        invocation: ContentObservationInvocation,
        *,
        descriptor: ObserverDescriptor,
    ) -> ObservationJobStatus:
        return self._contacts.call(
            registration_id,
            lambda: self._client(registration_id).put_job(invocation, descriptor=descriptor),
        )

    def get_job(
        self,
        registration_id: str,
        accepted: AcceptedObservationJob,
        *,
        descriptor: ObserverDescriptor,
    ) -> ObservationJobStatus:
        return self._contacts.call(
            registration_id,
            lambda: self._client(registration_id).status(accepted, descriptor=descriptor),
        )

    def cancel_job(
        self,
        registration_id: str,
        accepted: AcceptedObservationJob,
        *,
        descriptor: ObserverDescriptor,
    ) -> ObservationJobStatus:
        return self._contacts.call(
            registration_id,
            lambda: self._client(registration_id).cancel(accepted, descriptor=descriptor),
        )

    def _client(self, registration_id: str) -> ContentObserverClient:
        try:
            return self._registrations[registration_id]
        except KeyError as exc:
            raise KeyError(f"unknown content-observer registration: {registration_id}") from exc


class HttpTargetPort:
    """Explicit configuration-backed target registry using the v1 HTTP client."""

    def __init__(self, registrations: dict[str, TargetClient]) -> None:
        self._registrations = dict(registrations)
        self._contacts = ControlContacts(registrations)

    def close(self) -> None:
        for client in self._registrations.values():
            client.close()

    def registration_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._registrations))

    def descriptor(self, registration_id: str) -> TargetDescriptor:
        return self._contacts.call(
            registration_id, lambda: self._client(registration_id).descriptor()
        )

    def preflight(
        self,
        registration_id: str,
        request: TargetPreflightRequest,
    ) -> TargetPreflightResponse:
        return self._contacts.call(
            registration_id, lambda: self._client(registration_id).preflight(request)
        )

    def put_job(
        self,
        registration_id: str,
        request: TargetJobRequest,
        *,
        operation: OperationContract,
    ) -> TargetJobStatus:
        return self._contacts.call(
            registration_id,
            lambda: self._client(registration_id).put_job(request, operation=operation),
        )

    def get_job(
        self,
        registration_id: str,
        request: TargetJobRequest | AcceptedTargetJob,
        *,
        operation: OperationContract,
    ) -> TargetJobStatus:
        return self._contacts.call(
            registration_id,
            lambda: self._client(registration_id).status(request, operation=operation),
        )

    def cancel_job(
        self,
        registration_id: str,
        request: TargetJobRequest | AcceptedTargetJob,
        *,
        operation: OperationContract,
    ) -> TargetJobStatus:
        return self._contacts.call(
            registration_id,
            lambda: self._client(registration_id).cancel(request, operation=operation),
        )

    def _client(self, registration_id: str) -> TargetClient:
        try:
            return self._registrations[registration_id]
        except KeyError as exc:
            raise KeyError(f"unknown target registration: {registration_id}") from exc


class Stove0Coordinator:
    """Advance one work record by one externally visible state transition."""

    def __init__(
        self,
        work: Stove0WorkService,
        *,
        riverhog: RiverhogControlPort,
        planning: PlanningPort,
        observers: ObserverPort,
        targets: TargetPort,
        target_callbacks: TargetCallbackPort,
    ) -> None:
        self.work = work
        self.riverhog = riverhog
        self.planning = planning
        self.observers = observers
        self.targets = targets
        self.target_callbacks = target_callbacks

    def create_or_resume(
        self,
        identity: WorkIdentity,
        *,
        preview: WorkflowPreview | None = None,
    ) -> WorkRecord:
        return self.work.create_or_resume(identity, preview=preview)

    def inspect_coordination(self, work_id: str) -> BranchSetEvaluation:
        record = self.work.store.load(work_id)
        if record is None:
            raise KeyError(work_id)
        return project_coordination(record, self.work.store).evaluation

    def maintain(self, work_id: str) -> WorkRecord:
        """Renew live custody independently of extension contact and its backoff."""
        record = self.work.store.load(work_id)
        if record is None:
            raise KeyError(work_id)
        if record.claim is None or record.phase in {
            "complete",
            "no_action",
            "inapplicable",
            "failed",
            "canceled",
            "abandon_pending",
            "settled",
            "source_collection_retirement_pending",
        }:
            return record
        renewed = self.riverhog.renew_claim(record.work, record.claim)
        if renewed != record.claim:
            return self.work.rebind_claim(
                work_id,
                claim_id=renewed.claim_id,
                fence=renewed.fence,
                expected_revision=record.revision,
            )
        return record

    def step(self, work_id: str) -> WorkRecord:
        with planning_control_budget():
            try:
                return self._step(work_id)
            except ControlBudgetExhausted:
                current = self.work.store.load(work_id)
                if current is None:
                    raise KeyError(work_id) from None
                return current

    def _step(self, work_id: str) -> WorkRecord:
        record = self.work.store.load(work_id)
        if record is None:
            raise KeyError(work_id)
        phase = record.phase
        if (
            record.coordination_cancel_requested
            and record.branch_set_plan is not None
            and phase in {"eligible", "claimed", "coordinating"}
        ):
            return self._advance_coordination_cancel(record)
        if phase == "abandon_pending":
            if self._cancel_observation_delivery(record, stale_only=False):
                return record
            self.riverhog.abandon_claim(record)
            return self.work.complete_abandon(
                work_id,
                expected_revision=record.revision,
            )
        if record.claim is not None and self._cancel_observation_delivery(record, stale_only=True):
            return record
        if phase == "eligible":
            claim = self.riverhog.acquire_claim(record.work)
            if claim is None:
                return record
            return self.work.bind_claim(
                work_id,
                claim_id=claim.claim_id,
                fence=claim.fence,
                expected_revision=record.revision,
            )
        if phase == "claimed":
            if record.workflow_plan is not None:
                return self.work.activate_preplanned(
                    work_id,
                    expected_revision=record.revision,
                )
            if record.branch_set_plan is not None:
                return self.work.activate_preplanned_coordination(
                    work_id,
                    expected_revision=record.revision,
                )
            return self.work.begin_planning(work_id, expected_revision=record.revision)
        if phase == "planning":
            assert record.claim is not None
            try:
                planning = self.planning.for_invocation("work", record.work_id)
                claim = record.claim
                assert claim is not None

                def deliver(progress: PlanningProgress) -> None:
                    result = planning.deliver_observation(
                        progress,
                        owner_kind="work",
                        owner_id=record.work_id,
                        claim=claim,
                        riverhog=self.riverhog,
                        deliveries=self.work.store,
                    )
                    if result is not None:
                        _raise_observation_outcome(result)

                progress = advance_planning(
                    planning,
                    record.work,
                    owner_kind="work",
                    owner_id=record.work_id,
                    deliver=deliver,
                )
                if progress.state == "question":
                    deliver(progress)
                    return record
                if progress.state == "pending":
                    return record
                decision = progress.decision if progress.state == "ready" else progress.outcome
                evidence = (
                    planning.accepted_evidence(record.work)
                    if isinstance(decision, WorkNoAction)
                    else ()
                )
            except PlanningObservationPending:
                return record
            except PlanningObservationTerminal as outcome:
                if outcome.state == "inapplicable":
                    return self.work.mark_inapplicable(
                        work_id,
                        WorkInapplicable(code=outcome.code, message=outcome.message),
                        expected_revision=record.revision,
                    )
                if outcome.state == "failed":
                    assert outcome.retryable is not None
                    return self.work.fail(
                        work_id,
                        WorkFailure(
                            code=outcome.code,
                            message=outcome.message,
                            retryable=outcome.retryable,
                        ),
                        expected_revision=record.revision,
                    )
                return self.work.cancel(work_id, expected_revision=record.revision)
            if isinstance(decision, WorkInapplicable):
                return self.work.mark_inapplicable(
                    work_id,
                    decision,
                    expected_revision=record.revision,
                )
            if isinstance(decision, WorkNoAction):
                if record.preview_acceptance is not None:
                    return self.work.fail(
                        work_id,
                        WorkFailure(
                            code="accepted-preview-changed",
                            message=(
                                "Current observations no longer produce the accepted branch plan."
                            ),
                            retryable=True,
                        ),
                        expected_revision=record.revision,
                    )
                request = WorkflowPreviewRequest.seal(
                    WorkflowPreviewRequestPayload(work=record.work)
                )
                preview = WorkflowPreview.seal(
                    WorkflowPreviewPayload(
                        preview_id=request.preview_id,
                        state="no_action",
                        work=record.work,
                        observations=evidence,
                        outcome=PreviewOutcome(code=decision.code, message=decision.message),
                        no_output_decision=decision.decision,
                    )
                )
                if record.no_action_preview is not None and record.no_action_preview != preview:
                    return self.work.fail(
                        work_id,
                        WorkFailure(
                            code="accepted-preview-changed",
                            message="Current observations changed the accepted no-action decision.",
                            retryable=True,
                        ),
                        expected_revision=record.revision,
                    )
                return self.work.mark_no_action(
                    work_id,
                    preview,
                    source_collection_retirement_policy=self.planning.no_output_policy(
                        record.work, decision=decision.decision
                    )[1],
                    expected_revision=record.revision,
                )
            acceptance = record.preview_acceptance
            if record.no_action_preview is not None:
                return self.work.fail(
                    work_id,
                    WorkFailure(
                        code="accepted-preview-changed",
                        message=(
                            "Current observations no longer produce the accepted no-action "
                            "decision."
                        ),
                        retryable=True,
                    ),
                    expected_revision=record.revision,
                )
            if decision is None:
                raise ValueError("ready compiled planning lost its exact branch decision")
            if (
                acceptance is not None
                and decision.plan.branch_set_sha256 != acceptance.branch_set_sha256
            ):
                return self.work.fail(
                    work_id,
                    WorkFailure(
                        code="accepted-preview-changed",
                        message=(
                            "Current observation and routing authorities no longer produce "
                            "the accepted workflow preview."
                        ),
                        retryable=True,
                    ),
                    expected_revision=record.revision,
                )
            return self.work.admit_branch_set(
                work_id,
                decision,
                expected_revision=record.revision,
            )
        if phase == "coordinating":
            if record.coordination_cancel_requested:
                return self._advance_coordination_cancel(record)
            projection = project_coordination(record, self.work.store)
            if projection.pending_join is not None:
                return self.work.admit_join(
                    work_id,
                    projection.pending_join,
                    projection.pending_join_selections,
                    expected_revision=record.revision,
                )
            if projection.evaluation.branch_set_succeeded:
                settlement = projection.evaluation.coordination_settlement
                if settlement is None:
                    raise RuntimeError("successful coordination has no exact settlement")
                if record.coordination_settlement is None:
                    return self.work.record_coordination_settlement(
                        work_id,
                        settlement,
                        expected_revision=record.revision,
                    )
                if record.coordination_settlement != settlement:
                    raise RuntimeError("durable coordination settlement changed")
                if not self._successful_children_complete(record):
                    return record
                if not self.riverhog.settle_outcomes(record, projection.evaluation):
                    return record
                return self._begin_or_complete_retirement(record)
            return self._converge_coordination_outcome(record, projection.evaluation)
        if phase == "target_preflight":
            return self._preflight(record)
        if phase == "queued":
            return self._queue_or_poll(record)
        if phase in {"executing", "output_finalizing"}:
            return self._poll_target(record)
        if phase == "verifying":
            if (
                record.workflow_plan is not None
                and record.workflow_plan.result_kind == "external-effect"
            ):
                effect_settlement = self.riverhog.verify_and_settle_effect(
                    record,
                    self.planning.operation_contract(record.workflow_plan.operation),
                    self._parent_outcome(record),
                )
                if effect_settlement is None:
                    return record
                return self.work.verify_effect(
                    work_id, effect_settlement, expected_revision=record.revision
                )
            output, target_settlement = self.riverhog.verify_and_settle(
                record,
                self._parent_outcome(record),
            )
            if target_settlement is None:
                return record
            return self.work.verify_output(
                work_id,
                output,
                target_settlement,
                expected_revision=record.revision,
            )
        if phase == "settled":
            return self._begin_or_complete_retirement(record)
        if phase == "no_output_pending":
            no_action, retirement_policy, grace_seconds = self._no_output_policy(record)
            no_output_sha256 = self.riverhog.verify_and_settle_no_output(
                record,
                no_action,
                retirement_policy,
                grace_seconds,
                self._parent_outcome(record),
            )
            if no_output_sha256 is None:
                return record
            if retirement_policy == "retain":
                self.riverhog.release_claim(record)
            return self.work.verify_no_output(
                work_id, no_output_sha256, expected_revision=record.revision
            )
        if phase == "source_collection_retirement_pending":
            return self._retire_one(record)
        return record

    def retry(self, work_id: str) -> WorkRecord:
        """Restart one retryable failure under a fresh Riverhog fence."""

        record = self.work.store.load(work_id)
        if record is None:
            raise KeyError(work_id)
        if record.phase != "failed" or record.failure is None:
            raise RuntimeError("only failed stove0 work can be retried")
        if not record.failure.retryable or record.claim is None:
            raise RuntimeError("stove0 work failure is terminal")
        if record.branch_set_plan is not None:
            return self._retry_coordination(record)
        restarted = self.riverhog.restart_claim(record.work, record.claim)
        return self.work.retry_failed(
            work_id,
            claim_id=restarted.claim_id,
            fence=restarted.fence,
            expected_revision=record.revision,
        )

    def cancel(self, work_id: str) -> WorkRecord:
        """Request cancellation without creating another workflow authority.

        Work that has not reached a target enters a durable claim-abandonment
        phase. Active target work uses the target's explicit cancellation
        contract first; repeated calls remain idempotent through the accepted job
        identity. Riverhog revokes scoped capabilities before stove0 records the
        final canceled state.
        """

        record = self.work.store.load(work_id)
        if record is None:
            raise KeyError(work_id)
        if record.phase in {"complete", "no_action", "inapplicable", "canceled"}:
            return record
        if record.phase == "failed":
            if record.failure is None or not record.failure.retryable:
                return record
            return self.work.cancel(work_id, expected_revision=record.revision)
        if record.phase in {"settled", "source_collection_retirement_pending"}:
            raise RuntimeError("settled work cannot be canceled")
        if record.coordination_settlement is not None:
            raise RuntimeError("successfully settled coordination cannot be canceled")
        if record.branch_set_plan is not None and record.phase in {
            "eligible",
            "claimed",
            "coordinating",
        }:
            return self.work.request_coordination_cancel(
                work_id,
                expected_revision=record.revision,
            )
        if record.target_request is None or record.workflow_plan is None:
            return self.work.cancel(work_id, expected_revision=record.revision)
        operation = self.planning.operation_contract(record.workflow_plan.operation)
        status = self.targets.cancel_job(
            record.workflow_plan.target_registration_id,
            record.target_request,
            operation=operation,
        )
        return self.work.record_target_status(
            work_id,
            status,
            operation=operation,
            expected_revision=record.revision,
        )

    def _cancel_observation_delivery(self, owner: WorkRecord, *, stale_only: bool) -> bool:
        exclude = (
            (owner.claim.claim_id, owner.claim.fence)
            if stale_only and owner.claim is not None
            else None
        )
        pending = self.work.store.scan_observation_deliveries(
            "work",
            owner.work_id,
            incomplete_only=True,
            exclude_claim=exclude,
            limit=1,
        )
        if not pending:
            return False
        accepted = pending[0].accepted
        descriptor = self.observers.descriptor(accepted.request.observer_registration_id)
        status = self.observers.cancel_job(
            accepted.request.observer_registration_id,
            accepted,
            descriptor=descriptor,
        )
        self.work.store.update_observation_delivery("work", owner.work_id, accepted.job_id, status)
        return True

    def _preflight(self, record: WorkRecord) -> WorkRecord:
        plan = record.workflow_plan
        if plan is None:
            raise RuntimeError("target preflight work has no workflow plan")
        target = self.targets.descriptor(plan.target_registration_id)
        if target.descriptor_sha256 != plan.target_descriptor_sha256:
            raise RuntimeError("configured target descriptor changed after workflow planning")
        documents = self._selection_documents(record)
        input_selection = self.planning.target_input_selection(plan, documents)
        self.work.store.retain_selection(input_selection)
        request = self.planning.target_preflight_request(plan, documents, descriptor=target)
        response = self.targets.preflight(plan.target_registration_id, request)
        if (
            record.expected_target_plan_sha256 is not None
            and response.plan.plan_sha256 != record.expected_target_plan_sha256
        ):
            return self.work.fail(
                record.work_id,
                WorkFailure(
                    code="accepted-preview-target-changed",
                    message=(
                        "Current target preflight no longer produces the plan accepted "
                        "by the workflow preview."
                    ),
                    retryable=True,
                ),
                expected_revision=record.revision,
            )
        validate_preflight_response_against_request(response, request)
        return self.work.seal_target_plan(
            record.work_id,
            target=target,
            plan=response.plan,
            expected_revision=record.revision,
        )

    def _queue_or_poll(self, record: WorkRecord) -> WorkRecord:
        if record.target_request is not None:
            return self._poll_target(record)
        if (
            record.claim is None
            or record.workflow_plan is None
            or record.target_plan is None
            or record.controller_evidence is None
        ):
            raise RuntimeError("queued work is missing its sealed authorities")
        sealed = self.riverhog.seal_execution(
            record.claim,
            record.controller_evidence,
            record.workflow_plan,
            record.target_plan,
            self.work.store.iter_selection_artifacts(
                record.target_plan.inputs.selection.selection_sha256
            ),
            self.planning.operation_contract(record.workflow_plan.operation),
        )
        if not sealed:
            return record
        authority = self.riverhog.target_authority(
            record.claim,
            record.controller_evidence,
            record.target_plan,
            self.work.store.iter_selection_artifacts(
                record.target_plan.inputs.selection.selection_sha256
            ),
        )
        if authority is None:
            return record
        declaration = TargetJobDeclaration(
            job_id=(record.controller_evidence.execution_envelope.execution_envelope_sha256),
            claim_id=record.claim.claim_id,
            fence=record.claim.fence,
            controller_evidence=record.controller_evidence,
            plan=record.target_plan,
            declared_workspace_protection=authority.declared_workspace_protection,
        )
        callback_access = self.target_callbacks.issue_access(
            record,
            record.workflow_plan.target_registration_id,
        )
        invocation = TargetJobRequest.seal(declaration, authority.runtime, callback_access)
        accepted = self.work.bind_target_request(
            record.work_id,
            invocation,
            expected_revision=record.revision,
        )
        operation = self.planning.operation_contract(record.workflow_plan.operation)
        status = self.targets.put_job(
            record.workflow_plan.target_registration_id,
            invocation,
            operation=operation,
        )
        return self.work.record_target_status(
            record.work_id,
            status,
            operation=operation,
            expected_revision=accepted.revision,
        )

    def _poll_target(self, record: WorkRecord) -> WorkRecord:
        if (
            record.claim is None
            or record.workflow_plan is None
            or record.target_request is None
            or record.controller_evidence is None
            or record.target_plan is None
        ):
            raise RuntimeError("active target work has no accepted target authorities")
        authority = self.riverhog.target_authority(
            record.claim,
            record.controller_evidence,
            record.target_plan,
            self.work.store.iter_selection_artifacts(
                record.target_plan.inputs.selection.selection_sha256
            ),
        )
        if authority is None:
            return record
        refreshed = TargetJobRequest(
            declaration=record.target_request.declaration,
            runtime=authority.runtime,
            callback_access=self.target_callbacks.issue_access(
                record,
                record.workflow_plan.target_registration_id,
            ),
            request_sha256=record.target_request.request_sha256,
        )
        operation = self.planning.operation_contract(record.workflow_plan.operation)
        status = self.targets.put_job(
            record.workflow_plan.target_registration_id,
            refreshed,
            operation=operation,
        )
        return self.work.record_target_status(
            record.work_id,
            status,
            operation=operation,
            expected_revision=record.revision,
        )

    def _no_output_policy(
        self, record: WorkRecord
    ) -> tuple[NoOutputDefinition, SourceCollectionRetirementPolicy, int]:
        preview = record.no_action_preview
        if preview is None or preview.no_output_decision is None:
            raise ValueError("successful no-output settlement lacks its exact indexed decision")
        return self.planning.no_output_policy(record.work, decision=preview.no_output_decision)

    def _begin_or_complete_retirement(self, record: WorkRecord) -> WorkRecord:
        policy: SourceCollectionRetirementPolicy | None
        if record.branch_set_plan is not None:
            policy = record.branch_set_plan.source_collection_retirement_policy
        elif record.workflow_plan is not None:
            policy = record.workflow_plan.source_collection_retirement_policy
        elif record.no_action_preview is not None:
            policy = record.no_output_retirement_policy
        else:
            raise RuntimeError("settled work has no selected retirement policy")
        if policy == "retain":
            self.riverhog.release_claim(record)
            return self.work.begin_source_collection_retirement(
                record.work_id,
                (),
                expected_revision=record.revision,
            )
        if record.branch_set_plan is not None:
            leaves = self._coordination_descendant_leaves(record)
            if not all(
                self.planning.operation_contract(
                    child.workflow_plan.operation
                ).source_collection_retirement_permitted
                if child.workflow_plan is not None
                else self._no_output_policy(child)[0].source_loss is not None
                for child in leaves
            ):
                raise RuntimeError(
                    "every branch operation contract must authorize source collection retirement"
                )
        elif record.workflow_plan is not None:
            assert record.workflow_plan is not None
            operation = self.planning.operation_contract(record.workflow_plan.operation)
            if not operation.source_collection_retirement_permitted:
                raise RuntimeError(
                    "operation contract does not authorize source collection retirement"
                )
        elif self._no_output_policy(record)[0].source_loss is None:
            raise RuntimeError("no-output decision has no source-loss retirement permission")
        if not self.riverhog.begin_source_collection_retirement(record):
            return record
        return self.work.begin_source_collection_retirement(
            record.work_id,
            tuple(item.collection_id for item in record.work.inputs),
            expected_revision=record.revision,
        )

    def _retire_one(self, record: WorkRecord) -> WorkRecord:
        if not record.source_collection_retirement_remaining:
            raise RuntimeError("source collection retirement phase has no remaining collection")
        collection_id = record.source_collection_retirement_remaining[0]
        if not self.riverhog.retire_source_collection(record, collection_id):
            return record
        if len(record.source_collection_retirement_remaining) == 1:
            self.riverhog.release_claim(record)
        return self.work.record_source_collection_deleted(
            record.work_id,
            collection_id,
            expected_revision=record.revision,
        )

    def _selection_documents(
        self,
        record: WorkRecord,
    ) -> dict[str, ArtifactSelection]:
        binding = record.work.fork_join
        digests: tuple[str, ...]
        if isinstance(binding, BranchWorkBinding):
            digests = (binding.artifact_selection_sha256,)
        elif isinstance(binding, JoinWorkBinding):
            digests = tuple(item.artifact_selection_sha256 for item in binding.members)
        else:
            raise RuntimeError("target work has no exact branch or join artifact selection")
        documents: dict[str, ArtifactSelection] = {}
        for digest in digests:
            selection = self.work.store.load_selection(digest)
            if selection is None:
                raise RuntimeError(f"durable target selection is unavailable: {digest}")
            documents[digest] = selection
        return documents

    def _parent_outcome(
        self,
        record: WorkRecord,
    ) -> ParentOutcomeBinding | None:
        binding = record.work.fork_join
        parent_work_id: str
        outcome_id: str
        if isinstance(binding, BranchWorkBinding):
            parent_work_id = binding.parent_work_id
            outcome_id = f"branch/{binding.branch_id}"
        elif isinstance(binding, JoinWorkBinding):
            parent_work_id = binding.parent_work_id
            outcome_id = "join"
        else:
            return None
        parent = self.work.store.load(parent_work_id)
        if (
            parent is None
            or parent.phase != "coordinating"
            or parent.claim is None
            or parent.branch_set_plan is None
        ):
            raise RuntimeError("coordination parent claim is unavailable")
        return ParentOutcomeBinding(
            claim=parent.claim,
            outcome_id=outcome_id,
        )

    def _advance_coordination_cancel(self, record: WorkRecord) -> WorkRecord:
        if record.branch_set_plan is None:
            raise RuntimeError("coordination cancellation has no branch-set plan")
        terminal = {"complete", "no_action", "inapplicable", "failed", "canceled"}
        for child_id in self._coordination_child_ids(record):
            child = self.work.store.load(child_id)
            if child is None:
                raise RuntimeError("coordination cancellation child is unavailable")
            if child.phase not in terminal:
                self.cancel(child_id)
                return record
        return self.work.cancel(record.work_id, expected_revision=record.revision)

    def _successful_children_complete(self, record: WorkRecord) -> bool:
        if record.branch_set_plan is None:
            raise RuntimeError("successful coordination has no branch-set plan")
        for child_id in self._coordination_child_ids(record):
            child = self.work.store.load(child_id)
            if child is None:
                raise RuntimeError("successful coordination child is unavailable")
            if child.phase not in {"complete", "no_action"}:
                return False
        return True

    def _converge_coordination_outcome(
        self,
        record: WorkRecord,
        evaluation: BranchSetEvaluation,
    ) -> WorkRecord:
        children = self._coordination_children(record)
        terminal = {"complete", "no_action", "inapplicable", "failed", "canceled"}
        if any(child.phase not in terminal for child in children):
            return record
        failed = tuple(child for child in children if child.phase == "failed")
        if failed:
            failures = tuple(child.failure for child in failed)
            if any(failure is None for failure in failures):
                raise RuntimeError("failed coordination child has no failure details")
            retryable = all(failure is not None and failure.retryable for failure in failures)
            return self.work.fail(
                record.work_id,
                WorkFailure(
                    code="branch-set-failed",
                    message=(
                        "Required branch/join work failed: "
                        + ", ".join(child.work_id for child in failed)
                    )[:1000],
                    retryable=retryable,
                ),
                expected_revision=record.revision,
            )
        if evaluation.inapplicable_branch_ids or evaluation.join_state == "inapplicable":
            return self.work.mark_inapplicable(
                record.work_id,
                WorkInapplicable(
                    code="branch-set-inapplicable",
                    message="Required branch/join work is inapplicable.",
                ),
                expected_revision=record.revision,
            )
        if evaluation.canceled_branch_ids or evaluation.join_state == "canceled":
            return self.work.cancel(record.work_id, expected_revision=record.revision)
        return record

    def _retry_coordination(self, record: WorkRecord) -> WorkRecord:
        assert record.branch_set_plan is not None
        assert record.claim is not None
        for child_id in self._coordination_child_ids(record):
            child = self.work.store.load(child_id)
            if child is None:
                raise RuntimeError("coordination retry child is unavailable")
            if child.phase == "failed":
                if child.failure is None or not child.failure.retryable:
                    raise RuntimeError("coordination contains a terminal child failure")
                self.retry(child_id)
            elif child.phase in {"inapplicable", "canceled"}:
                raise RuntimeError("coordination contains a non-retryable child outcome")
        restarted = self.riverhog.restart_claim(record.work, record.claim)
        return self.work.retry_coordination(
            record.work_id,
            claim_id=restarted.claim_id,
            fence=restarted.fence,
            expected_revision=record.revision,
        )

    def _coordination_child_ids(self, record: WorkRecord) -> tuple[str, ...]:
        if record.branch_set_plan is None:
            raise RuntimeError("coordination has no branch-set plan")
        child_ids = [branch_work(branch).work_id for branch in record.branch_set_plan.branches]
        if record.join_plan is not None:
            child_ids.append(record.join_plan.work.work_id)
        return tuple(child_ids)

    def _coordination_descendant_leaves(self, record: WorkRecord) -> tuple[WorkRecord, ...]:
        leaves: list[WorkRecord] = []
        if record.branch_set_plan is None:
            raise RuntimeError("coordination has no branch-set plan")
        pending = [branch_work(branch).work_id for branch in record.branch_set_plan.branches]
        while pending:
            child_id = pending.pop()
            child = self.work.store.load(child_id)
            if child is None:
                raise RuntimeError("coordination descendant is unavailable")
            if child.branch_set_plan is not None:
                pending.extend(
                    branch_work(branch).work_id for branch in child.branch_set_plan.branches
                )
            elif child.workflow_plan is not None or child.no_action_preview is not None:
                leaves.append(child)
        return tuple(sorted(leaves, key=lambda item: item.work_id))

    def _coordination_children(self, record: WorkRecord) -> tuple[WorkRecord, ...]:
        children: list[WorkRecord] = []
        for child_id in self._coordination_child_ids(record):
            child = self.work.store.load(child_id)
            if child is None:
                raise RuntimeError("coordination child is unavailable")
            children.append(child)
        return tuple(children)


__all__ = [
    "HttpObserverPort",
    "HttpTargetPort",
    "ParentOutcomeBinding",
    "ObserverPort",
    "PlanningObservationTerminal",
    "PlanningPort",
    "RiverhogControlPort",
    "Stove0Coordinator",
    "TargetInvocationAuthority",
    "TargetPort",
]
