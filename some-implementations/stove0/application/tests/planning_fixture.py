"""Current planning port doubles for lifecycle tests; recipe semantics are tested separately."""

from copy import copy

from stove0_core.observation_state import ObservationDeliveryRecord
from stove0_core.planning_progress import PlanningProgress
from stove0_core.work_state import WorkInapplicable, WorkNoAction
from stove0_observer_protocol import ContentObservationEvidence, ContentObservationInvocation
from stove0_protocol import (
    ArtifactSelection,
    PreviewOutcome,
    WorkflowPreview,
    WorkflowPreviewPayload,
    WorkflowPreviewRequest,
    WorkflowPreviewRequestPayload,
    canonical_json_sha256,
)
from stove0_protocol.no_output_decisions import (
    CompiledConditionProof,
    CompiledConditionProofPayload,
    CompiledNoOutputDecision,
    CompiledNoOutputPayload,
)
from stove0_protocol.observation_evidence import ObservationQuestion, ObservationQuestionPayload
from stove0_protocol.observation_interfaces import (
    OBSERVATION_INTERFACE_SEMANTICS,
    ExactDocumentRef,
    GlobalFactsView,
    ObservationInterface,
    ObservationInterfacePayload,
    SubjectPort,
)
from stove0_protocol.predicates import Truth
from stove0_protocol.recipe_outcomes import NoOutputDefinition


def fixture_interface(contract):
    interface = ObservationInterface.seal(
        ObservationInterfacePayload(
            id="fixture.whole-question/v1",
            observer_contract=ExactDocumentRef(id=contract.id, sha256=contract.contract_sha256),
            facts_profile=ExactDocumentRef(
                id=contract.facts_schema.id, sha256=contract.facts_schema.profile_sha256
            ),
            semantic_profile=ExactDocumentRef(
                id=contract.facts_semantics.id, sha256=contract.facts_semantics.profile_sha256
            ),
            inputs={"subjects": SubjectPort()},
            views={"facts": GlobalFactsView(record_at="", record_schema_at="")},
            partitioning="whole-scope",
            empty_scope="inapplicable",
            interface_semantics=ExactDocumentRef(
                id=OBSERVATION_INTERFACE_SEMANTICS.id,
                sha256=OBSERVATION_INTERFACE_SEMANTICS.profile_sha256,
            ),
            conformance_vectors_sha256=canonical_json_sha256({"fixture": "lifecycle-port"}),
        )
    )
    interface.validate_contract(contract)
    return interface


def observation_headers(*, work_id, contract, subjects, task_id="facts"):
    scope = ArtifactSelection.seal(subjects).ref()
    question = ObservationQuestion.seal(
        ObservationQuestionPayload(
            work_id=work_id,
            task_id=task_id,
            observer_contract=ExactDocumentRef(id=contract.id, sha256=contract.contract_sha256),
            interface=fixture_interface(contract).ref,
            scope=scope,
            subject_ports={"subjects": scope},
            read_actions=contract.read_actions,
        )
    )
    return {
        "task_id": task_id,
        "question_sha256": question.question_sha256,
        "interface": question.interface,
    }


def no_output_decision(
    work, inventory, *, code="fixture.no-output/v1", message="No output is required."
):
    condition = CompiledConditionProof.seal(
        CompiledConditionProofPayload(
            work_id=work.work_id,
            recipe=work.recipe,
            inventory=inventory,
            decision_index="0",
            condition=True,
            evaluations=(),
            truth=Truth.TRUE,
        )
    )
    return CompiledNoOutputDecision.seal(
        CompiledNoOutputPayload(
            work_id=work.work_id,
            recipe=work.recipe,
            inventory=inventory,
            decision_index="0",
            definition=NoOutputDefinition(code=code, message=message),
            conditions=(condition,),
        )
    )


def no_output_preview(
    work, inventory, *, code="fixture.no-output/v1", message="No output is required."
):
    request = WorkflowPreviewRequest.seal(WorkflowPreviewRequestPayload(work=work))
    decision = no_output_decision(work, inventory, code=code, message=message)
    return WorkflowPreview.seal(
        WorkflowPreviewPayload(
            preview_id=request.preview_id,
            work=work,
            state="no_action",
            outcome=PreviewOutcome(code=code, message=message),
            no_output_decision=decision,
        )
    )


class _QuestionPending(Exception):
    def __init__(self, work, question):
        self.work, self.question = work, question


class FixturePlanningRuntime:
    """A controllable planning port with original job testimony and one contact per step."""

    def for_invocation(self, owner_kind, owner_id):
        if not hasattr(self, "_fixture_contexts"):
            self._fixture_contexts = {}
        context = self._fixture_contexts.setdefault(
            (owner_kind, owner_id),
            {
                "evidence": {},
                "requests": {},
                "cursors": {},
            },
        )
        bound = copy(self)
        bound._fixture_context = context
        return bound

    def accepted_for(self, work):
        context = self._fixture_context
        accepted = context["evidence"].setdefault(work.work_id, {})
        evidence = tuple(accepted[key] for key in sorted(accepted))
        requests = self.fixture_requests(work, evidence)
        pending = sorted(
            (request for request in requests if request.request_id not in accepted),
            key=lambda request: request.request_id,
        )
        if not pending:
            return evidence
        position = context["cursors"].get(work.work_id, 0)
        request = pending[position % len(pending)]
        context["cursors"][work.work_id] = position + 1
        scope = ArtifactSelection.seal(request.subjects).ref()
        question = ObservationQuestion.seal(
            ObservationQuestionPayload(
                work_id=work.work_id,
                task_id=request.task_id,
                observer_contract=ExactDocumentRef(
                    id=request.observer_contract_id, sha256=request.observer_contract_sha256
                ),
                interface=request.interface,
                scope=scope,
                subject_ports={"subjects": scope},
                options=request.options,
                retrieve=request.retrieval_policy,
                read_actions=request.read_actions,
            )
        )
        assert request.question_sha256 == question.question_sha256
        context["requests"][question.question_sha256] = request
        raise _QuestionPending(work, question)

    def step(self, work):
        try:
            evidence = self.accepted_for(work)
            result = self.fixture_decision(work, evidence, accepted_for=self.accepted_for)
        except _QuestionPending as pending:
            return PlanningProgress("question", pending.work, question=pending.question)
        if isinstance(result, WorkNoAction):
            return PlanningProgress("no-output", work, outcome=result)
        if isinstance(result, WorkInapplicable):
            return PlanningProgress("inapplicable", work, outcome=result)
        return PlanningProgress("ready", work, decision=result)

    def deliver_observation(self, progress, *, owner_kind, owner_id, claim, riverhog, deliveries):
        request = self._fixture_context["requests"][progress.question.question_sha256]
        descriptor = self.observers.descriptor(request.observer_registration_id)
        authority = riverhog.observation_authority(
            claim, request, owner_kind=owner_kind, owner_id=owner_id
        )
        if authority is None:
            return None
        invocation = ContentObservationInvocation(
            request=request, claim_id=claim.claim_id, fence=claim.fence, runtime=authority
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
            status = self.observers.put_job(
                request.observer_registration_id, invocation, descriptor=descriptor
            )
            deliveries.update_observation_delivery(owner_kind, owner_id, invocation.job_id, status)
        if status.result is None:
            return None
        if status.result.state != "observed":
            return status.result
        self._fixture_context["evidence"].setdefault(progress.work.work_id, {})[
            request.request_id
        ] = ContentObservationEvidence(request=request, result=status.result)
        return None

    def accepted_evidence(self, work):
        values = {
            key: value
            for evidence in self._fixture_context["evidence"].values()
            for key, value in evidence.items()
        }
        return tuple(values[key] for key in sorted(values))
