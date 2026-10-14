"""Deliver one compiled named question through the ordinary observer job rail."""

from __future__ import annotations

from typing import Protocol

from sqlalchemy import select
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ContentObservationInvocation,
    ContentObservationRequest,
    ContentObservationResult,
    ObservationEvidenceSlot,
    ObserverDescriptor,
)
from stove0_observer_protocol.interfaces import interface_subject_ports
from stove0_protocol import ArtifactSelection, WorkArtifactSubject, WorkIdentity
from stove0_protocol.accepted_inputs import AcceptedEvidenceInput
from stove0_protocol.observation_evidence import ObservationQuestion
from stove0_protocol.observation_interfaces import EvidencePort
from stove0_recipe_config.compiled import CompiledRecipe
from stove0_recipe_config.dependencies import RecipeDependencyClosure

from stove0_core.compiled_state_ports import CompiledStatePort
from stove0_core.coordinator import ObservationAuthorityPort, ObserverPort
from stove0_core.metadata_steps import planning_cost
from stove0_core.observation_questions import physical_question
from stove0_core.observation_state import (
    ObservationDeliveryPort,
    ObservationDeliveryRecord,
    ObservationOwnerKind,
)
from stove0_core.planning_progress import PlanningProgress
from stove0_core.work_state import ClaimBinding


class ObservationPlanningPort(Protocol):
    @property
    def state(self) -> CompiledStatePort: ...

    @property
    def observers(self) -> ObserverPort: ...

    @property
    def observation_execution_timeout_seconds(self) -> int: ...

    def _definition(self, work: WorkIdentity) -> tuple[CompiledRecipe, RecipeDependencyClosure]: ...

    def observer_binding(
        self, work: WorkIdentity, question: ObservationQuestion
    ) -> tuple[str, ObserverDescriptor] | None: ...


class CompiledObservationDelivery:
    """Acceptance remains controller-owned; executors receive only scoped views."""

    def __init__(self, planner: ObservationPlanningPort) -> None:
        self.planner, self.state = planner, planner.state

    def request(
        self, work: WorkIdentity, question: ObservationQuestion
    ) -> tuple[ContentObservationRequest, ObserverDescriptor] | None:
        _, closure = self.planner._definition(work)
        resource = closure.interface(id=question.interface.id, sha256=question.interface.sha256)
        binding = self.planner.observer_binding(work, question)
        if binding is None:
            return None
        provider, descriptor = binding
        store = self.state.accepted_observations
        physical = store.tables["physical"]
        with self.state.engine.connect() as connection:
            retained = connection.scalar(
                select(physical.c.request_json)
                .where(
                    physical.c.question_sha256 == self.state.planning_key(question.question_sha256),
                    physical.c.evidence_json.is_(None),
                )
                .order_by(physical.c.request_id)
                .limit(1)
            )
        if retained is not None:
            return ContentObservationRequest.model_validate_json(retained), descriptor
        members, coverage = self.state.compiled_planning.members, store.tables["coverage"]
        query = (
            select(members.c.document_json)
            .where(
                members.c.selection_sha256 == question.scope.selection_sha256,
                ~select(coverage.c.subject_id)
                .where(
                    coverage.c.question_sha256 == self.state.planning_key(question.question_sha256),
                    coverage.c.subject_id == members.c.artifact_id,
                )
                .exists(),
            )
            .order_by(members.c.artifact_id)
        )
        if resource.interface.partitioning == "independent-subjects":
            query = query.limit(
                min(
                    100,
                    descriptor.support_for(
                        question.observer_contract.id
                    ).preferred_subject_batch_size,
                )
            )
        with self.state.engine.connect() as connection:
            subjects = tuple(
                WorkArtifactSubject.model_validate_json(document)
                for document in connection.scalars(query)
            )
        if not subjects:
            return None
        subject_ports = {}
        ids = [subject.id for subject in subjects]
        for name, scope in question.subject_ports.items():
            with self.state.engine.connect() as connection:
                subject_ports[name] = tuple(
                    connection.scalars(
                        select(members.c.artifact_id)
                        .where(
                            members.c.selection_sha256 == scope.selection_sha256,
                            members.c.artifact_id.in_(ids),
                        )
                        .order_by(members.c.artifact_id)
                    )
                )
        evidence_ports = {}
        for name, port in sorted(resource.interface.inputs.items()):
            if not isinstance(port, EvidencePort):
                continue
            selected_ids = {id for covered in port.covers for id in subject_ports[covered]}
            selection = ArtifactSelection.seal(
                tuple(subject for subject in subjects if subject.id in selected_ids)
            )
            self.state.retain_selection(selection)
            predecessor = question.evidence_ports[name]
            accepted = store.accepted(work.work_id, predecessor.task_id)
            if accepted is None:
                raise ValueError("observation delivery lost its accepted predecessor")
            owner = closure.interface(
                id=predecessor.interface.id, sha256=predecessor.interface.sha256
            )
            authority = store.input_step(
                accepted, interface=owner.interface, selected_scope=selection.ref()
            )
            if authority is None:
                return None
            evidence_ports[name] = (
                ObservationEvidenceSlot(
                    slot="stove0.accepted-input/" + authority.input_sha256,
                    accepted_input_sha256=authority.input_sha256,
                    observer_contract_id=accepted.question.observer_contract.id,
                ),
            )
        request = physical_question(
            question=question,
            interface=resource.interface,
            registration_id=provider,
            descriptor=descriptor,
            subjects=subjects,
            subject_ports=subject_ports,
            evidence_ports=evidence_ports,
            timeout_seconds=self.planner.observation_execution_timeout_seconds,
        )
        store.register_request(question, request, descriptor, resource.interface)
        return request, descriptor

    def inputs(self, request: ContentObservationRequest) -> tuple[AcceptedEvidenceInput, ...]:
        return tuple(
            self.state.accepted_observations.input_delivery(
                slot.accepted_input_sha256,
                authorize=lambda scope: None,
            )
            for slot in request.evidence_slots or ()
        )

    def accept(self, work: WorkIdentity, evidence: ContentObservationEvidence) -> None:
        with planning_cost(
            "evidence-validation",
            work_id=work.work_id,
            task_id=evidence.request.task_id,
            subjects=len(evidence.request.subjects),
        ):
            _, closure = self.planner._definition(work)
            request = evidence.request
            resource = closure.interface(id=request.interface.id, sha256=request.interface.sha256)
            self.state.accepted_observations.accept(
                evidence,
                contract=resource.contract,
                interface=resource.interface,
                subject_ports=interface_subject_ports(
                    resource.interface, request.subjects, request.options
                ),
                predecessor_interfaces={
                    owner.interface.ref: owner.interface for owner in closure.observers
                },
                semantic_validators=self.planner.observers.semantic_validators(
                    request.observer_registration_id
                ),
            )

    def step(
        self,
        progress: PlanningProgress,
        *,
        owner_kind: ObservationOwnerKind,
        owner_id: str,
        claim: ClaimBinding,
        riverhog: ObservationAuthorityPort,
        deliveries: ObservationDeliveryPort,
    ) -> ContentObservationResult | None:
        if progress.question is None:
            raise ValueError("observation delivery has no logical question")
        prepared = self.request(progress.work, progress.question)
        if prepared is None:
            return None
        request, descriptor = prepared
        authority = riverhog.observation_authority(
            claim, request, owner_kind=owner_kind, owner_id=owner_id
        )
        if authority is None:
            return None
        invocation = ContentObservationInvocation(
            request=request,
            claim_id=claim.claim_id,
            fence=claim.fence,
            runtime=authority,
            evidence=self.inputs(request),
        )
        delivery = deliveries.ensure_observation_delivery(
            ObservationDeliveryRecord(
                owner_kind=owner_kind,
                owner_id=owner_id,
                accepted=invocation.accepted(),
            )
        )
        status = delivery.status
        if status is None or status.state != "completed":
            with planning_cost(
                "observer-contact",
                work_id=progress.work.work_id,
                task_id=request.task_id,
                subjects=len(request.subjects),
            ):
                status = self.planner.observers.put_job(
                    request.observer_registration_id,
                    invocation,
                    descriptor=descriptor,
                )
            deliveries.update_observation_delivery(owner_kind, owner_id, invocation.job_id, status)
        if status.result is None:
            return None
        if status.result.state == "observed":
            self.accept(
                progress.work, ContentObservationEvidence(request=request, result=status.result)
            )
            return None
        return status.result
