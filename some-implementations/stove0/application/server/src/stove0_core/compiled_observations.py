"""Resume the compiled observation/classification graph one metadata step at a time."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stove0_core.persistence import SqlAlchemyStateStore
from dataclasses import dataclass
from typing import Literal

from stove0_protocol import ArtifactSelectionRef, WorkIdentity, canonical_json_sha256
from stove0_protocol.observation_evidence import ObservationQuestion
from stove0_protocol.observation_interfaces import SubjectPort
from stove0_protocol.observation_views import ProjectedView
from stove0_protocol.predicates import facts_predicates
from stove0_recipe_config.compiler import observation_dependencies
from stove0_recipe_config.dependencies import RecipeDependencyClosure
from stove0_recipe_config.source import EvidenceInput

from stove0_core.accepted_view_queries import accepted_view_queries
from stove0_core.compiled_fact_evaluation import CompiledFactEvaluation
from stove0_core.compiled_input_scopes import CompiledInputScopes
from stove0_core.observation_questions import logical_question
from stove0_core.planning_program import TaskView
from stove0_core.work_metadata import RiverhogInventoryPort, WorkInventory
from stove0_core.work_state import WorkInapplicable


@dataclass(frozen=True, slots=True)
class CompiledObservationProgress:
    state: Literal["pending", "question", "complete", "inapplicable"]
    inventory: ArtifactSelectionRef | None
    question: ObservationQuestion | None = None
    outcome: WorkInapplicable | None = None


class CompiledObservationPlanning:
    """Runtime reads only the queued definition and its exact offline closure.

    The caller delivers the returned logical question through the extension job
    boundary. A task advances only after exact acceptance and view sealing. Empty
    scopes complete in controller state without manufacturing observer evidence.
    """

    def __init__(self, state: SqlAlchemyStateStore, riverhog: RiverhogInventoryPort) -> None:
        self.state = state
        self.inventory = WorkInventory(state, riverhog)
        self.scopes = CompiledInputScopes(state)
        self.facts = CompiledFactEvaluation(state)

    def step(self, work: WorkIdentity) -> CompiledObservationProgress:
        retained = self.state.recipe_definitions.load(work.recipe)
        if retained is None:
            raise ValueError("queued work has no retained exact compiled recipe closure")
        recipe, closure = retained
        planning = self.state.compiled_planning
        row = planning.ensure(work.work_id, recipe)
        scope = planning.inventory_ref(work.work_id)
        if row["phase"] == "inventory":
            scope = self.inventory.step(work)
            if scope is not None:
                planning.bind_inventory(
                    work.work_id, expected_revision=row["revision"], scope=scope
                )
            return CompiledObservationProgress("pending", scope)
        if scope is None:
            raise ValueError("compiled graph lost its original invocation inventory")
        graph = observation_dependencies(recipe, closure)
        if not planning.initialize_tasks(work.work_id, graph, expected_revision=row["revision"]):
            return CompiledObservationProgress("pending", scope)
        task_row = planning.due_task(work.work_id)
        row = planning.ensure(work.work_id, recipe)
        if task_row is None:
            if not planning.tasks_complete(work.work_id):
                return CompiledObservationProgress("pending", scope)
            if row["phase"] not in {"decisions", "complete"}:
                planning.advance(
                    work.work_id,
                    expected_revision=row["revision"],
                    phase="decisions",
                    input_ordinal=0,
                )
            return CompiledObservationProgress("complete", scope)
        task_id = task_row["task_id"]
        if task_id == "$classify":
            required = {}
            for case in recipe.classification.cases:
                for predicate in facts_predicates(case.when):
                    if predicate.scope == "input":
                        key = canonical_json_sha256(
                            predicate.model_dump(mode="json", by_alias=True)
                        )
                        required[key] = predicate
            ordered = sorted(required)
            if row["input_ordinal"] < len(ordered):
                predicate = required[ordered[row["input_ordinal"]]]
                if self.facts.step(work, predicate) is not None:
                    planning.advance(
                        work.work_id,
                        expected_revision=row["revision"],
                        input_ordinal=row["input_ordinal"] + 1,
                    )
                return CompiledObservationProgress("pending", scope)
            if row["input_ordinal"] != len(ordered):
                raise ValueError(
                    "classification input continuation exceeds its compiled predicates"
                )
            answers = {}
            for key, predicate in required.items():
                result = self.facts.step(work, predicate)
                if result is None:
                    raise ValueError("classification lost a completed whole-invocation input fact")
                answers[key] = result
            if planning.classify_step(
                work.work_id, recipe=recipe, views=self.views(work, closure), input_answers=answers
            ):
                planning.advance_task(task_row, state="complete")
            return CompiledObservationProgress("pending", scope)

        task = recipe.observations[task_id]
        resource = closure.interface(id=task.interface.id, sha256=task.interface.sha256)
        store = self.state.accepted_observations
        question = store.question(work.work_id, task_id)
        if question is None:
            ports = sorted(
                name
                for name, port in resource.interface.inputs.items()
                if isinstance(port, SubjectPort)
            )
            if task_row["port_ordinal"] < len(ports):
                name = ports[task_row["port_ordinal"]]
                port_binding = task.inputs[name]
                if isinstance(port_binding, EvidenceInput):
                    raise ValueError("subject port selected an evidence binding")
                reference = self.scopes.subject_port(
                    work=work,
                    task_id=task_id,
                    port_id=name,
                    binding=port_binding,
                    inventory=scope,
                )
                if reference is not None:
                    planning.retain_port(
                        work.work_id,
                        task_id,
                        name,
                        reference,
                        expected_revision=task_row["revision"],
                    )
                return CompiledObservationProgress("pending", scope)
            if task_row["port_ordinal"] != len(ports):
                raise ValueError("compiled graph port continuation exceeds its exact interface")
            selected_ports = planning.task_ports(work.work_id, task_id)
            if set(selected_ports) != set(ports):
                raise ValueError("compiled graph omits a completed subject port")
            selected = self.scopes.subject_union(work=work, task_id=task_id, ports=selected_ports)
            if selected is None:
                return CompiledObservationProgress("pending", scope)
            predecessors = {}
            for binding in task.inputs.values():
                if isinstance(binding, EvidenceInput):
                    accepted = store.accepted(work.work_id, binding.evidence)
                    if accepted is None:
                        raise ValueError("compiled graph advanced past an incomplete predecessor")
                    predecessors[binding.evidence] = accepted
            question = store.ensure_question(
                logical_question(
                    work=work,
                    task_id=task_id,
                    task=task,
                    resource=resource,
                    scope=selected,
                    subject_ports=selected_ports,
                    predecessors=predecessors,
                )
            )
            return CompiledObservationProgress("pending", scope)

        accepted = store.accepted(work.work_id, task_id)
        if accepted is None:
            if (
                question.scope.artifact_count == 0
                and resource.interface.empty_scope == "inapplicable"
            ):
                return CompiledObservationProgress(
                    "inapplicable",
                    scope,
                    outcome=WorkInapplicable(
                        code="empty-observation-scope",
                        message=f"Required task {task_id} does not apply to an empty input scope.",
                    ),
                )
            accepted = store.seal_step(question, interface=resource.interface)
            if accepted is None:
                return CompiledObservationProgress("question", scope, question)
            return CompiledObservationProgress("pending", scope)
        views = sorted(resource.interface.views)
        if task_row["view_ordinal"] < len(views):
            name = views[task_row["view_ordinal"]]
            key = store.ensure_view(
                accepted, interface=resource.interface, view_id=name, selected_scope=question.scope
            )
            if store.view_step(key, interface=resource.interface) is not None:
                planning.advance_task(
                    task_row,
                    view_ordinal=task_row["view_ordinal"] + 1,
                )
            return CompiledObservationProgress("pending", scope)
        if task_row["view_ordinal"] != len(views):
            raise ValueError("compiled graph view continuation exceeds its exact interface")
        planning.advance_task(task_row, state="complete")
        return CompiledObservationProgress("pending", scope)

    def views(self, work: WorkIdentity, closure: RecipeDependencyClosure) -> TaskView:
        def view(task_id: str, view_id: str) -> ProjectedView | None:
            store = self.state.accepted_observations
            accepted = store.accepted(work.work_id, task_id)
            if accepted is None:
                return None
            interface_ref = accepted.question.interface
            resource = closure.interface(id=interface_ref.id, sha256=interface_ref.sha256)
            authority = store.retained_view(
                accepted, view_id=view_id, selected_scope=accepted.question.scope
            )
            if authority is None:
                return None
            return accepted_view_queries(store, authority, interface=resource.interface)

        return view
