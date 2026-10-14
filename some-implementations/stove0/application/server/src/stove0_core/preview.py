"""Resumable read-only planning over the production planning authorities."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any, Literal, Protocol

from http_api_contracts.control import ControlBudgetExhausted, check_control_budget, control_budget
from http_api_contracts.metadata_exchange import MetadataPreparationPending
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ContentObservationRequest,
    ObserverRuntimeAuthority,
)
from stove0_protocol import (
    BranchSetDecision,
    BranchTargetPreview,
    PlanningJobPayload,
    PlanningJobRequest,
    PlanningJobStatus,
    PreviewOutcome,
    TargetPlanBinding,
    WorkflowPreview,
    WorkflowPreviewPayload,
    WorkflowPreviewRequest,
    WorkflowPreviewRequestPayload,
    WorkIdentity,
)
from stove0_protocol.no_output_decisions import CompiledNoOutputDecision
from stove0_target_protocol import validate_preflight_response_against_request
from time_formats import format_utc_timestamp, utc_now, utc_timestamp_now

from stove0_core.control_contacts import ContactDeferred
from stove0_core.coordinator import (
    ObserverPort,
    PlanningObservationPending,
    PlanningObservationTerminal,
    PlanningPort,
    TargetPort,
    _raise_observation_outcome,
)
from stove0_core.metadata_steps import MetadataSteps, advance_planning, planning_control_budget
from stove0_core.planning_progress import PlanningProgress
from stove0_core.preview_state import InMemoryPreviewStore, PreviewRecord, PreviewStore
from stove0_core.work_state import (
    ClaimBinding,
    ConcurrentWorkUpdate,
    InMemoryWorkStore,
    WorkInapplicable,
    WorkNoAction,
    WorkStore,
)


class PreviewRiverhogPort(Protocol):
    def acquire_preview_claim(
        self, request: WorkflowPreviewRequest, *, invocation_id: str
    ) -> ClaimBinding | None: ...
    def renew_preview_claim(
        self, request: WorkflowPreviewRequest, claim: ClaimBinding, *, invocation_id: str
    ) -> ClaimBinding: ...
    def observation_authority(
        self,
        claim: ClaimBinding,
        request: ContentObservationRequest,
        *,
        owner_kind: str,
        owner_id: str,
    ) -> ObserverRuntimeAuthority | None: ...
    def abandon_preview_claim(
        self, request: WorkflowPreviewRequest, claim: ClaimBinding
    ) -> None: ...


class WorkflowPreviewService:
    """Durable delivery and planning with separately isolated metadata preparation.

    Each submitted invocation retains its claim identity before the first remote
    call. Completed semantic preview evidence is exposed only after subordinate
    reads stop and the read-only claim is abandoned. A fresh acceptance check is
    another invocation, even when the work and preview digests are unchanged.
    """

    def __init__(
        self,
        *,
        riverhog: PreviewRiverhogPort,
        planning: PlanningPort,
        observers: ObserverPort,
        targets: TargetPort,
        store: PreviewStore | None = None,
        deliveries: WorkStore | None = None,
        claim_renew_seconds: float = 300.0,
        control_seconds: float = 5.0,
        accept_preview: Callable[[WorkIdentity, WorkflowPreview], object] | None = None,
        metadata_steps: MetadataSteps | None = None,
    ) -> None:
        if claim_renew_seconds <= 0:
            raise ValueError("preview claim renewal interval must be positive")
        self.riverhog, self.planning, self.observers, self.targets = (
            riverhog,
            planning,
            observers,
            targets,
        )
        self.store = store if store is not None else InMemoryPreviewStore()
        self.deliveries = deliveries if deliveries is not None else InMemoryWorkStore()
        self.claim_renew_seconds, self.control_seconds = claim_renew_seconds, control_seconds
        self.accept_preview = accept_preview
        self.metadata_steps = metadata_steps
        self._scan_cursor = ""
        self._maintenance_cursor = ""

    def submit(
        self, work: WorkIdentity, *, invocation_id: str, accepted_preview_sha256: str | None = None
    ) -> PlanningJobStatus:
        job = PlanningJobRequest.seal(
            PlanningJobPayload(
                work=work,
                invocation_id=invocation_id,
                accepted_preview_sha256=accepted_preview_sha256,
            )
        )
        return self.store.create_preview(PreviewRecord(job=job)).status()

    def get(self, job_id: str) -> PlanningJobStatus:
        return self._load(job_id).status()

    def request(self, job_id: str) -> PlanningJobRequest:
        return self._load(job_id).job

    def cancel(self, job_id: str) -> PlanningJobStatus:
        record = self._load(job_id)
        if record.phase == "completed" or record.canceled:
            return record.status()
        return self._save(
            record, canceled=True, phase="canceling", contact_at=utc_timestamp_now()
        ).status()

    def maintain(self, job_id: str) -> PlanningJobStatus:
        record = self._load(job_id)
        if record.claim is None or record.phase in {"completed", "abandoning", "admitting"}:
            return record.status()
        with planning_control_budget(self.control_seconds):
            request = self._semantic_request(record)
            claim = self.riverhog.renew_preview_claim(
                request, record.claim, invocation_id=record.job.job_id
            )
            return self._save(record, claim=claim, claim_renew_at=self._renew_at()).status()

    def advance(self, *, limit: int = 25) -> dict[str, object]:
        progressed: list[str] = []
        failures: list[dict[str, str]] = []
        with control_budget(self.control_seconds):
            for maintenance in (True, False):
                cursor = self._maintenance_cursor if maintenance else self._scan_cursor
                records = self.store.scan_previews(
                    after_id=cursor, limit=limit, maintenance=maintenance
                )
                if not records and cursor:
                    records = self.store.scan_previews(limit=limit, maintenance=maintenance)
                for record in records:
                    try:
                        check_control_budget()
                        job_id = record.job_id
                        if self.metadata_steps is not None:
                            self.metadata_steps.preview(self, job_id, maintenance=maintenance)
                        else:
                            self.maintain(job_id) if maintenance else self.step(job_id)
                        progressed.append(job_id)
                    except MetadataPreparationPending:
                        pass
                    except ControlBudgetExhausted:
                        return {"progressed": progressed, "failures": failures}
                    except ConcurrentWorkUpdate:
                        pass
                    except Exception as exc:
                        failures.append({"job_id": record.job_id, "error": type(exc).__name__})
                    # Persisted work remains exact; this cursor only provides
                    # fair scheduling among independent due continuations.
                    if maintenance:
                        self._maintenance_cursor = record.job_id
                    else:
                        self._scan_cursor = record.job_id
        return {"progressed": progressed, "failures": failures}

    def step(self, job_id: str) -> PlanningJobStatus:
        record = self._load(job_id)
        if record.phase == "completed":
            return record.status()
        with planning_control_budget(self.control_seconds):
            try:
                return self._step(record).status()
            except (ControlBudgetExhausted, PlanningObservationPending, ContactDeferred):
                return self._load(job_id).status()
            except PlanningObservationTerminal as outcome:
                return self._finish(
                    record,
                    state=outcome.state,
                    outcome=PreviewOutcome(
                        code=outcome.code,
                        message=outcome.message,
                        retryable=outcome.retryable,
                    ),
                ).status()
            except ConcurrentWorkUpdate:
                raise
            except Exception as exc:
                if record.phase in {"canceling", "abandoning"}:
                    raise
                return self._finish(
                    record,
                    state="failed",
                    outcome=PreviewOutcome(
                        code="workflow-preview-failed",
                        message=f"{type(exc).__name__}: {exc}"[:1000],
                        retryable=True,
                    ),
                ).status()

    def _step(self, record: PreviewRecord) -> PreviewRecord:
        if record.phase == "abandoning":
            if record.claim is not None:
                self.riverhog.abandon_preview_claim(self._semantic_request(record), record.claim)
            return self._save(
                record,
                phase="admitting"
                if record.job.accepted_preview_sha256 is not None
                else "completed",
                claim_renew_at=None,
            )
        if record.phase == "admitting":
            assert record.result is not None
            if (
                record.result.state in {"ready", "no_action"}
                and record.result.preview_sha256 == record.job.accepted_preview_sha256
            ):
                if self.accept_preview is None:
                    raise RuntimeError("work initiation has no owning acceptance authority")
                self.accept_preview(record.job.work, record.result)
            return self._save(record, phase="completed")
        if record.canceled or record.phase == "canceling":
            pending = self.deliveries.scan_observation_deliveries(
                "preview", record.job.job_id, incomplete_only=True, limit=1
            )
            if pending:
                delivery = pending[0]
                descriptor = self.observers.descriptor(
                    delivery.accepted.request.observer_registration_id
                )
                status = self.observers.cancel_job(
                    delivery.accepted.request.observer_registration_id,
                    delivery.accepted,
                    descriptor=descriptor,
                )
                self.deliveries.update_observation_delivery(
                    "preview", record.job.job_id, delivery.accepted.job_id, status
                )
                return record
            if record.result is None:
                return self._finish(
                    record,
                    state="canceled",
                    outcome=PreviewOutcome(
                        code="preview-canceled", message="The planning invocation was canceled."
                    ),
                )
            return self._save(record, phase="abandoning")
        if record.phase == "queued":
            claim = self.riverhog.acquire_preview_claim(
                self._semantic_request(record), invocation_id=record.job.job_id
            )
            if claim is None:
                return record
            return self._save(
                record, phase="observing", claim=claim, claim_renew_at=self._renew_at()
            )
        assert record.claim is not None
        stale = self.deliveries.scan_observation_deliveries(
            "preview",
            record.job.job_id,
            incomplete_only=True,
            exclude_claim=(record.claim.claim_id, record.claim.fence),
            limit=1,
        )
        if stale:
            delivery = stale[0]
            descriptor = self.observers.descriptor(
                delivery.accepted.request.observer_registration_id
            )
            status = self.observers.cancel_job(
                delivery.accepted.request.observer_registration_id,
                delivery.accepted,
                descriptor=descriptor,
            )
            self.deliveries.update_observation_delivery(
                "preview", record.job.job_id, delivery.accepted.job_id, status
            )
            return record
        if record.phase in {"observing", "planning"}:
            planning = self.planning.for_invocation("preview", record.job.job_id)
            claim = record.claim
            assert claim is not None

            def deliver(progress: PlanningProgress) -> None:
                result = planning.deliver_observation(
                    progress,
                    owner_kind="preview",
                    owner_id=record.job.job_id,
                    claim=claim,
                    riverhog=self.riverhog,
                    deliveries=self.deliveries,
                )
                if result is not None:
                    _raise_observation_outcome(result)

            progress = advance_planning(
                planning,
                record.job.work,
                owner_kind="preview",
                owner_id=record.job.job_id,
                deliver=deliver,
            )
            if progress.state == "question":
                deliver(progress)
                return record
            if progress.state == "pending":
                return record
            decision = progress.decision if progress.state == "ready" else progress.outcome
            # Nested observation delivery may have updated this continuation.
            record = self._load(record.job.job_id)
            if isinstance(decision, (WorkInapplicable, WorkNoAction)):
                return self._finish(
                    record,
                    state="no_action" if isinstance(decision, WorkNoAction) else "inapplicable",
                    outcome=PreviewOutcome(code=decision.code, message=decision.message),
                    no_output_decision=decision.decision
                    if isinstance(decision, WorkNoAction)
                    else None,
                )
            if not isinstance(decision, BranchSetDecision):
                raise RuntimeError("planning returned an unsupported workflow decision")
            return self._save(record, decision=decision, phase="preflight")
        if record.phase == "preflight":
            assert record.decision is not None
            done = {item.work_id for item in record.target_plans}
            branch = next(
                (
                    item
                    for item in record.decision.leaf_branches()
                    if item.workflow_plan.work.work_id not in done
                ),
                None,
            )
            if branch is None:
                return self._finish(record, state="ready")
            workflow = branch.workflow_plan
            target = self.targets.descriptor(workflow.target_registration_id)
            if target.descriptor_sha256 != workflow.target_descriptor_sha256:
                raise RuntimeError("configured target descriptor changed after workflow planning")
            request = self.planning.target_preflight_request(
                workflow, record.decision.selection_documents, descriptor=target
            )
            response = self.targets.preflight(workflow.target_registration_id, request)
            validate_preflight_response_against_request(response, request)
            plan = response.plan
            item = BranchTargetPreview(
                branch_id=branch.branch_id,
                work_id=workflow.work.work_id,
                workflow_plan_sha256=workflow.workflow_plan_sha256,
                target_plan=TargetPlanBinding(
                    protocol=target.protocol,
                    target_implementation_id=target.implementation_id,
                    target_descriptor_sha256=target.descriptor_sha256,
                    operation_contract_sha256=plan.operation_contract_sha256,
                    plan=plan.binding_document(),
                    plan_sha256=plan.plan_sha256,
                ),
            )
            return self._save(
                record,
                target_plans=tuple(
                    sorted((*record.target_plans, item), key=lambda item: item.work_id)
                ),
            )
        raise RuntimeError("unsupported planning continuation phase")

    def _finish(
        self,
        record: PreviewRecord,
        *,
        state: Literal["ready", "no_action", "inapplicable", "failed", "canceled"],
        outcome: PreviewOutcome | None = None,
        no_output_decision: CompiledNoOutputDecision | None = None,
    ) -> PreviewRecord:
        decision = record.decision if state == "ready" else None
        result = WorkflowPreview.seal(
            WorkflowPreviewPayload(
                preview_id=self._semantic_request(record).preview_id,
                work=record.job.work,
                state=state,
                observations=self._evidence_for_completion(record),
                outcome=outcome,
                no_output_decision=no_output_decision,
                branch_set_plan=decision.plan if decision is not None else None,
                branch_sets=decision.branch_sets if decision is not None else (),
                selections=decision.selections if decision is not None else (),
                target_plans=record.target_plans if decision is not None else (),
            )
        )
        pending = self.deliveries.scan_observation_deliveries(
            "preview", record.job.job_id, incomplete_only=True, limit=1
        )
        current = self._load(record.job.job_id)
        return self._save(
            current,
            result=result,
            phase="canceling" if pending else "abandoning",
            claim_renew_at=current.claim_renew_at if pending else None,
        )

    def _evidence_for_completion(
        self, record: PreviewRecord
    ) -> tuple[ContentObservationEvidence, ...]:
        # Raw job responses are operational custody, never accepted evidence.
        planning = self.planning.for_invocation("preview", record.job.job_id)
        return planning.accepted_evidence(record.job.work)

    def _load(self, job_id: str) -> PreviewRecord:
        record = self.store.load_preview(job_id)
        if record is None:
            raise KeyError(job_id)
        return record

    def _save(self, record: PreviewRecord, **changes: Any) -> PreviewRecord:
        replacement = PreviewRecord.model_validate(
            {**record.model_dump(mode="python"), **changes, "revision": record.revision + 1}
        )
        return self.store.compare_and_swap_preview(
            expected_revision=record.revision, replacement=replacement
        )

    def _renew_at(self) -> str:
        return format_utc_timestamp(utc_now() + timedelta(seconds=self.claim_renew_seconds))

    @staticmethod
    def _semantic_request(record: PreviewRecord) -> WorkflowPreviewRequest:
        return WorkflowPreviewRequest.seal(WorkflowPreviewRequestPayload(work=record.job.work))


__all__ = ["PreviewRiverhogPort", "WorkflowPreviewService"]
