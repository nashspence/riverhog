"""Interpret only retained compiled recipes and their exact dependency closure."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, Protocol, Self

from pydantic import BaseModel, JsonValue
from riverhog_protocol.collection_workflows import SourceCollectionRetirementPolicy
from riverhog_protocol.output_collection_policy import OutputCollectionPolicy
from stove0_observer_protocol import (
    CollectionRootIdentityRef,
    ContentObservationEvidence,
    ContentObservationResult,
)
from stove0_protocol import ArtifactSelectionRef, WorkflowPlan
from stove0_protocol.no_output_decisions import CompiledNoOutputDecision
from stove0_protocol.observation_evidence import ObservationQuestion
from stove0_protocol.recipe_outcomes import NoOutputDefinition
from stove0_recipe_config.compiled import CompiledRecipe
from stove0_recipe_config.dependencies import RecipeDependencyClosure
from stove0_recipe_config.source import BranchExport
from stove0_target_protocol import OperationContract

from stove0_core.observation_state import ObservationDeliveryPort, ObservationOwnerKind
from stove0_core.work_metadata import RiverhogInventoryPort
from stove0_core.work_state import ClaimBinding

if TYPE_CHECKING:
    from stove0_core.coordinator import ObservationAuthorityPort, ObserverPort, TargetPort
    from stove0_core.persistence import SqlAlchemyStateStore

import json

from jsonschema import Draft202012Validator
from pydantic import TypeAdapter
from sqlalchemy import select
from stove0_observer_protocol import ObserverDescriptor
from stove0_protocol import (
    ArtifactSelection,
    BranchDeclaration,
    BranchPlan,
    BranchSetDecision,
    BranchSetPlan,
    BranchWorkBinding,
    CoordinationBranchPlan,
    JoinDeclaration,
    JoinMemberDeclaration,
    JoinWorkBinding,
    NoOutputBranchPlan,
    OperationIdentityRef,
    WorkArtifactSubject,
    WorkflowPlanIntent,
    WorkIdentity,
    WorkInputGroup,
    WorkPayload,
    canonical_json_bytes,
    canonical_json_sha256,
)
from stove0_protocol.fork_join import BranchCollectionExport
from stove0_protocol.observation_evidence import EvidenceResultRef
from stove0_recipe_config.bindings import apply_bindings
from stove0_recipe_config.catalog import CompiledRecipeCatalog
from stove0_recipe_config.compiled import CompiledOperationCall, CompiledRecipeCall
from stove0_target_protocol import TargetDescriptor, TargetInputAuthority, TargetPreflightRequest

from stove0_core.compiled_branches import CompiledBranchScope
from stove0_core.compiled_decisions import CompiledDecisionPlanning
from stove0_core.compiled_observation_delivery import CompiledObservationDelivery
from stove0_core.compiled_observations import CompiledObservationPlanning
from stove0_core.planning_progress import PlanningProgress
from stove0_core.work_state import WorkInapplicable, WorkNoAction


def _json(model: BaseModel) -> str:
    return canonical_json_bytes(model.model_dump(mode="json", by_alias=True)).decode("utf-8")


class _DescriptorRegistry[Descriptor: BaseModel](Protocol):
    def registration_ids(self) -> tuple[str, ...]: ...

    def descriptor(self, registration_id: str) -> Descriptor: ...


class RecipePlanner:
    """A selected group set makes one call; every selected branch is required.

    Task delivery is a separate component boundary. This interpreter returns a
    logical question and advances only after the acceptance owner seals its
    complete exact testimony. Nested frames are durable and advance independently
    of process-local recursion or original authoring files.
    """

    def __init__(
        self,
        *,
        catalog: CompiledRecipeCatalog,
        state: SqlAlchemyStateStore,
        riverhog: RiverhogInventoryPort,
        observers: ObserverPort,
        targets: TargetPort,
    ) -> None:
        self.catalog, self.state, self.riverhog = catalog, state, riverhog
        self.observers, self.targets = observers, targets
        self.observation_planning = CompiledObservationPlanning(state, riverhog)
        self.decision_planning = CompiledDecisionPlanning(state)
        self.observation_delivery = CompiledObservationDelivery(self)
        for recipe in (*catalog.recipes, *catalog.closure.recipes):
            state.recipe_definitions.retain(recipe, catalog.closure)

    def for_invocation(self, owner_kind: str, owner_id: str) -> Self:
        from copy import copy

        scoped = copy(self)
        scoped.state = self.state.planning_context(owner_kind, owner_id)
        scoped.observation_planning = CompiledObservationPlanning(scoped.state, self.riverhog)
        scoped.decision_planning = CompiledDecisionPlanning(scoped.state)
        scoped.observation_delivery = CompiledObservationDelivery(scoped)
        return scoped

    def create_work(
        self,
        recipe_id: str,
        roots: Sequence[CollectionRootIdentityRef],
        *,
        revision: int | None = None,
        effective_intent: dict[str, JsonValue] | None = None,
    ) -> WorkIdentity:
        recipe = self.catalog.recipe(recipe_id, revision)
        self.state.recipe_definitions.retain_tree(recipe, self.catalog.closure)
        parameters = dict(effective_intent or {})
        Draft202012Validator(recipe.parameters_schema.document).validate(parameters)
        return WorkIdentity.seal(
            WorkPayload(
                recipe=recipe.ref,
                inputs=tuple(sorted(roots, key=lambda r: r.collection_id)),
                effective_intent=parameters,
            )
        )

    def _definition(self, work: WorkIdentity) -> tuple[CompiledRecipe, RecipeDependencyClosure]:
        retained = self.state.recipe_definitions.load(work.recipe)
        if retained is None:
            raise ValueError("work lacks its retained compiled recipe and exact offline closure")
        recipe, closure = retained
        Draft202012Validator(recipe.parameters_schema.document).validate(work.effective_intent)
        return recipe, closure

    def step(self, work: WorkIdentity) -> PlanningProgress:
        frames = self.state.compiled_runtime
        frames.ensure(work, owner_work_id=work.work_id, depth=0)
        row = frames.due(work.work_id)
        if row is None:
            row = frames.load(work.work_id)
            return self._result(row)
        current = WorkIdentity.model_validate_json(row["work_json"])
        recipe, closure = self._definition(current)
        if row["phase"] == "observations":
            progress = self.observation_planning.step(current)
            if progress.state == "question":
                return PlanningProgress("question", current, question=progress.question)
            if progress.state == "inapplicable":
                return self._inapplicable(row, progress.outcome)
            if progress.state == "complete":
                frames.change(row, phase="decisions")
        elif row["phase"] == "decisions":
            decision_progress = self.decision_planning.step(current)
            if decision_progress.state == "no-output":
                if decision_progress.decision is None:
                    raise ValueError("no-output planning has no exact indexed decision")
                frames.change(
                    row,
                    phase="no-output",
                    outcome_json=_json(WorkNoAction(decision=decision_progress.decision)),
                )
            elif decision_progress.state == "inapplicable":
                return self._inapplicable(row, decision_progress.outcome)
            elif decision_progress.state == "branches":
                frames.change(row, phase="selections", branch_ordinal=0)
        elif row["phase"] == "selections":
            names = sorted(recipe.branches)
            if row["branch_ordinal"] == len(names):
                frames.change(row, phase="coverage", branch_ordinal=0)
            else:
                name = names[row["branch_ordinal"]]
                selected = self.state.compiled_branches.step(current, name)
                if isinstance(selected, WorkInapplicable):
                    return self._inapplicable(row, selected)
                if selected is not None:
                    frames.change(row, scope=selected, branch_ordinal=row["branch_ordinal"] + 1)
        elif row["phase"] == "coverage":
            scopes = [
                CompiledBranchScope.model_validate_json(b["scope_json"])
                for b in frames.branches(current.work_id)
            ]
            if not any(scope.selection.artifact_count for scope in scopes):
                return self._inapplicable(
                    row,
                    WorkInapplicable(
                        code="no-selected-branch",
                        message="No recipe branch applies to the exact input.",
                    ),
                )
            inventory = self.state.compiled_planning.inventory_ref(current.work_id)
            if inventory is None:
                raise ValueError("compiled coverage lost its original inventory")
            if recipe.source.unmatched == "reject-work" and self._unmatched(current, inventory):
                return self._inapplicable(
                    row,
                    WorkInapplicable(
                        code="unmatched-recipe-input",
                        message="The recipe rejects uncovered input members.",
                    ),
                )
            if (
                current.fork_join is None
                and recipe.source.retirement.mode == "after-settlement"
                and self._unmatched(current, inventory)
            ):
                return self._inapplicable(
                    row,
                    WorkInapplicable(
                        code="unsafe-retirement-coverage",
                        message="Root retirement requires dispositions for every original member.",
                    ),
                )
            accepted = [
                self.state.accepted_observations.accepted(current.work_id, name)
                for name in sorted(recipe.observations)
            ]
            if any(item is None for item in accepted):
                raise ValueError("compiled coverage lost required accepted task evidence")
            decision_sha256 = canonical_json_sha256(
                {
                    "format": "stove0-compiled-fork-decision/v1",
                    "work_id": current.work_id,
                    "recipe": current.recipe.model_dump(mode="json"),
                    "inventory": inventory.model_dump(mode="json"),
                    "scopes": [scope.branch_selection_sha256 for scope in scopes],
                    "evidence_sets": [
                        item.evidence_set_sha256 for item in accepted if item is not None
                    ],
                }
            )
            frames.change(row, phase="calls", branch_ordinal=0, decision_sha256=decision_sha256)
        elif row["phase"] == "calls":
            names = sorted(recipe.branches)
            if row["branch_ordinal"] == len(names):
                if frames.calls_complete(current.work_id):
                    frames.change(row, phase="seal")
                else:
                    frames.change(row, branch_ordinal=0)
            else:
                name = names[row["branch_ordinal"]]
                branch = frames.branch(current.work_id, name)
                if branch is None:
                    raise ValueError("compiled call lost its selected branch")
                scope = CompiledBranchScope.model_validate_json(branch["scope_json"])
                if scope.selection.artifact_count == 0 or branch["declaration_json"] is not None:
                    frames.change(row, branch_ordinal=row["branch_ordinal"] + 1)
                else:
                    return self._call(row, current, recipe, closure, name, scope)
        elif row["phase"] == "seal":
            return self._seal(row, current, recipe, closure)
        else:
            raise ValueError("compiled recipe frame has an unsupported continuation phase")
        return PlanningProgress("pending", work)

    def _result(self, row: Mapping[str, Any] | None) -> PlanningProgress:
        if row is None:
            raise ValueError("compiled planning lost its retained recipe frame")
        work = WorkIdentity.model_validate_json(row["work_json"])
        if row["phase"] == "ready":
            return PlanningProgress(
                "ready", work, decision=BranchSetDecision.model_validate_json(row["decision_json"])
            )
        if row["phase"] == "no-output":
            outcome = WorkNoAction.model_validate_json(row["outcome_json"])
            outcome.decision.verify_recipe(self._definition(work)[0])
            return PlanningProgress("no-output", work, outcome=outcome)
        if row["phase"] == "inapplicable":
            return PlanningProgress(
                "inapplicable",
                work,
                outcome=WorkInapplicable.model_validate_json(row["outcome_json"]),
            )
        return PlanningProgress("pending", work)

    def _inapplicable(
        self, row: Mapping[str, Any], outcome: WorkInapplicable | None
    ) -> PlanningProgress:
        if outcome is None:
            raise ValueError("inapplicable planning has no exact outcome")
        self.state.compiled_runtime.change(row, phase="inapplicable", outcome_json=_json(outcome))
        return PlanningProgress("pending", WorkIdentity.model_validate_json(row["work_json"]))

    def _unmatched(self, work: WorkIdentity, inventory: ArtifactSelectionRef) -> bool:
        # The selected set is a union by immutable member instance, not by bytes.
        members = self.state.compiled_planning.members
        selected = members.alias("compiled_selected_member")
        branches = self.state.compiled_runtime.tables["branches"]
        covered = (
            select(selected.c.artifact_id)
            .join(
                branches,
                branches.c.selection_sha256 == selected.c.selection_sha256,
            )
            .where(
                branches.c.work_id == self.state.planning_key(work.work_id),
                selected.c.member_identity_sha256 == members.c.member_identity_sha256,
            )
            .exists()
        )
        with self.state.engine.connect() as connection:
            return (
                connection.scalar(
                    select(members.c.artifact_id)
                    .where(
                        members.c.selection_sha256 == inventory.selection_sha256,
                        ~covered,
                    )
                    .limit(1)
                )
                is not None
            )

    def _call(
        self,
        row: Mapping[str, Any],
        work: WorkIdentity,
        recipe: CompiledRecipe,
        closure: RecipeDependencyClosure,
        name: str,
        scope: CompiledBranchScope,
    ) -> PlanningProgress:
        declaration: BranchDeclaration
        definition = recipe.branches[name]
        selection = self.state.load_selection(scope.selection.selection_sha256)
        if selection is None or selection.ref() != scope.selection:
            raise ValueError("compiled call lost its exact selected members")
        call = definition.call
        intent, options = apply_bindings(
            intent=call.intent,
            options=call.options if isinstance(call, CompiledOperationCall) else {},
            bindings=call.bind,
            parameters=work.effective_intent,
            evaluation=work.evaluation.model_dump(mode="json")
            if work.evaluation is not None
            else None,
        )
        if isinstance(call, CompiledRecipeCall):
            child_recipe = closure.recipe(id=call.recipe.id, sha256=call.recipe.sha256)
            Draft202012Validator(child_recipe.parameters_schema.document).validate(intent)
            child_work = CoordinationBranchPlan.build_work(
                parent_work=work,
                branch_id=name,
                decision_sha256=row["decision_sha256"],
                selection=selection,
                recipe=child_recipe.ref,
                effective_intent=intent,
            )
            child_row = self.state.compiled_runtime.ensure(
                child_work,
                owner_work_id=row["owner_work_id"],
                depth=row["depth"] + 1,
            )
            if child_row["phase"] not in {"ready", "no-output", "inapplicable"}:
                self.state.compiled_runtime.change(
                    row,
                    branch_id=name,
                    child_work_id=child_work.work_id,
                    branch_ordinal=row["branch_ordinal"] + 1,
                )
                return PlanningProgress("pending", work)
            child = self._result(child_row)
            if child.state == "inapplicable":
                if not isinstance(child.outcome, WorkInapplicable):
                    raise ValueError("child inapplicability has no exact outcome")
                return self._inapplicable(
                    row,
                    WorkInapplicable(
                        code=child.outcome.code,
                        message=f"Subrecipe {name}: {child.outcome.message}",
                    ),
                )
            if child.state == "no-output":
                if not isinstance(child.outcome, WorkNoAction):
                    raise ValueError("child no-output planning has no exact decision")
                declaration = NoOutputBranchPlan.seal(
                    branch_id=name,
                    selection=selection,
                    work=child_work,
                    observations=self._evidence(child_work, tuple(child_recipe.observations)),
                    decision=child.outcome.decision,
                )
            else:
                if child.decision is None:
                    raise ValueError("ready child planning has no exact branch decision")
                declaration = CoordinationBranchPlan(
                    branch_id=name,
                    artifact_selection=selection.ref(),
                    work=child_work,
                    branch_set_sha256=child.decision.plan.branch_set_sha256,
                )
        else:
            operation = closure.operation(id=call.operation.id, sha256=call.operation.sha256)
            problem = _input_problem(operation, selection)
            if problem is not None:
                return self._inapplicable(
                    row,
                    WorkInapplicable(
                        code="operation-inputs-inapplicable",
                        message=problem,
                    ),
                )
            binding = self._provider(work, name, call, operation)
            if binding is None:
                self.state.compiled_runtime.change(row, branch_ordinal=row["branch_ordinal"] + 1)
                return PlanningProgress("pending", work)
            provider, target = binding
            support = target.support_for(operation.id)
            Draft202012Validator(operation.intent_schema.document).validate(intent)
            Draft202012Validator(support.options_schema.document).validate(options)
            declaration = BranchPlan.build(
                parent_work=work,
                branch_id=name,
                decision_sha256=row["decision_sha256"],
                selection=selection,
                recipe=recipe.ref,
                effective_intent=intent,
                workflow_intent=WorkflowPlanIntent(
                    operation=OperationIdentityRef(
                        id=operation.id, sha256=operation.contract_sha256
                    ),
                    result_kind=operation.result_kind,
                    target_registration_id=provider,
                    target_descriptor_sha256=target.descriptor_sha256,
                    requested_target_options=options,
                    input_groups=self._groups(scope),
                    input_retrieval_policy=call.retrieve,
                    output_policy=call.output
                    if call.output is not None
                    else OutputCollectionPolicy(),
                ),
                observations=self._evidence(work, call.evidence),
            )
        self.state.compiled_runtime.change(
            row,
            declaration=declaration,
            child_work_id=child_work.work_id if isinstance(call, CompiledRecipeCall) else None,
            branch_ordinal=row["branch_ordinal"] + 1,
        )
        return PlanningProgress("pending", work)

    def _provider(
        self,
        work: WorkIdentity,
        call_id: str,
        call: CompiledOperationCall,
        operation: OperationContract,
    ) -> tuple[str, TargetDescriptor] | None:
        return self._bind_provider(
            work,
            "call:" + call_id,
            registry=self.targets,
            descriptor_type=TargetDescriptor,
            expected={
                "kind": "operation",
                "executor": call.executor,
                "contract": {"id": operation.id, "sha256": operation.contract_sha256},
                "result_kind": operation.result_kind,
            },
            accepts=lambda descriptor: any(
                (support.operation_id, support.operation_contract_sha256, support.result_kind)
                == (operation.id, operation.contract_sha256, operation.result_kind)
                for support in descriptor.operations
            ),
        )

    def observer_binding(
        self, work: WorkIdentity, question: ObservationQuestion
    ) -> tuple[str, ObserverDescriptor] | None:
        recipe, _ = self._definition(work)
        task = recipe.observations[question.task_id]
        return self._bind_provider(
            work,
            "task:" + question.task_id,
            registry=self.observers,
            descriptor_type=ObserverDescriptor,
            expected={
                "kind": "observer",
                "executor": task.executor,
                "contract": task.observer.model_dump(mode="json"),
                "interface": task.interface.model_dump(mode="json"),
            },
            accepts=lambda descriptor: any(
                support.contract_id == task.observer.id
                and support.contract_sha256 == task.observer.sha256
                and task.interface in support.interfaces
                for support in descriptor.contracts
            ),
        )

    def _bind_provider[Descriptor: BaseModel](
        self,
        work: WorkIdentity,
        binding_id: str,
        *,
        registry: _DescriptorRegistry[Descriptor],
        descriptor_type: type[Descriptor],
        expected: dict[str, JsonValue],
        accepts: Callable[[Descriptor], bool],
    ) -> tuple[str, Descriptor] | None:
        executor = expected["executor"]
        if executor is not None and not isinstance(executor, str):
            raise ValueError("deployment executor must be a registration ID")
        names = (executor,) if executor is not None else registry.registration_ids()
        store = self.state.compiled_runtime
        row = store.ensure_provider(
            work.work_id, binding_id, {**expected, "providers": list(names)}
        )
        if row["phase"] == "chosen":
            descriptor = descriptor_type.model_validate_json(row["descriptor_json"])
            if not accepts(descriptor):
                raise ValueError("retained provider binding changed its exact required contracts")
            provider_id = row["provider_id"]
            if not isinstance(provider_id, str):
                raise ValueError("chosen provider has no registration ID")
            return provider_id, descriptor
        if row["phase"] in {"ambiguous", "unavailable"}:
            raise ValueError("call requires one unambiguous exact deployed provider binding")
        providers = json.loads(row["binding_json"])["providers"]
        if row["position"] == len(providers):
            store.change_provider(
                row, phase="chosen" if row["provider_id"] is not None else "unavailable"
            )
            return None
        name = providers[row["position"]]
        descriptor = registry.descriptor(name)
        changes: dict[str, Any] = {"position": row["position"] + 1}
        if accepts(descriptor):
            if row["provider_id"] is not None:
                store.change_provider(row, phase="ambiguous")
                raise ValueError("call requires one unambiguous exact deployed provider binding")
            changes.update(provider_id=name, descriptor_json=_json(descriptor))
        store.change_provider(row, **changes)
        return None

    def _groups(self, scope: CompiledBranchScope) -> tuple[WorkInputGroup, ...]:
        if scope.group_source is None:
            return ()
        result, ordinal = [], 0
        while True:
            page = self.state.compiled_branches.choice_page(scope, start_ordinal=ordinal)
            for primary_id, candidate_ref, condition in page:
                if condition.truth.value != "true":
                    continue
                candidate = self.state.load_selection(candidate_ref.selection_sha256)
                if candidate is None or candidate.ref() != candidate_ref:
                    raise ValueError("compiled group lost its exact selected candidate")
                primary = next(item for item in candidate.artifacts if item.id == primary_id)
                result.append(
                    WorkInputGroup(
                        primary_id=primary.id,
                        associated_ids=tuple(
                            sorted(item.id for item in candidate.artifacts if item.id != primary.id)
                        ),
                    )
                )
            ordinal += len(page)
            if ordinal == scope.choice_count:
                return tuple(sorted(result, key=lambda group: group.primary_id))
            if not page:
                raise ValueError("selected group choices made no continuation progress")

    def _evidence(
        self, work: WorkIdentity, task_ids: Sequence[str]
    ) -> tuple[ContentObservationEvidence, ...]:
        store, result = self.state.accepted_observations, {}
        for task_id in sorted(task_ids):
            accepted = store.accepted(work.work_id, task_id)
            if accepted is None:
                raise ValueError("compiled call lacks complete accepted named task evidence")
            ordinal = 0
            while True:
                page = store.evidence_page(
                    accepted, start_ordinal=ordinal, authorize=lambda scope: None
                )
                for reference in page.results:
                    original = store.original_evidence(
                        reference.request_id, authorize=lambda scope: None
                    )
                    if original.request.task_id != task_id:
                        raise ValueError("forwarded evidence changed its exact logical task")
                    result[reference.request_id] = original
                if page.complete:
                    break
                ordinal += len(page.results)
        return tuple(result[key] for key in sorted(result))

    def _seal(
        self,
        row: Mapping[str, Any],
        work: WorkIdentity,
        recipe: CompiledRecipe,
        closure: RecipeDependencyClosure,
    ) -> PlanningProgress:
        frames = self.state.compiled_runtime
        branches: list[BranchDeclaration] = []
        selections: dict[str, ArtifactSelection] = {}
        children: dict[str, BranchSetPlan] = {}
        declaration_parser: TypeAdapter[BranchDeclaration] = TypeAdapter(BranchDeclaration)
        for retained in frames.branches(work.work_id):
            scope = CompiledBranchScope.model_validate_json(retained["scope_json"])
            if scope.selection.artifact_count == 0:
                continue
            if retained["declaration_json"] is None:
                raise ValueError("compiled recipe lost a required selected call")
            branch = declaration_parser.validate_json(retained["declaration_json"])
            branches.append(branch)
            selection = self.state.load_selection(branch.artifact_selection.selection_sha256)
            if selection is None or selection.ref() != branch.artifact_selection:
                raise ValueError("compiled branch lost its exact selected members")
            selections[selection.selection_sha256] = selection
            if isinstance(branch, CoordinationBranchPlan):
                child = self._result(frames.load(branch.work.work_id)).decision
                if child is None:
                    raise ValueError("compiled child lost its exact ready decision")
                for digest, selection in child.selection_documents.items():
                    _retain(selections, digest, selection)
                for digest, plan in child.branch_set_documents.items():
                    _retain(children, digest, plan)
        join = None
        if recipe.join is not None:
            declared = {item.branch_id: item for item in branches}
            if not set(recipe.join.members) <= set(declared) or any(
                isinstance(declared[name], NoOutputBranchPlan) for name in recipe.join.members
            ):
                return self._inapplicable(
                    row,
                    WorkInapplicable(
                        code="join-member-inapplicable",
                        message="Every join member must produce its declared exact collection.",
                    ),
                )
            call = recipe.join.call
            operation = closure.operation(id=call.operation.id, sha256=call.operation.sha256)
            binding = self._provider(work, "$join", call, operation)
            if binding is None:
                return PlanningProgress("pending", work)
            provider, target = binding
            intent, options = apply_bindings(
                intent=call.intent,
                options=call.options,
                bindings=call.bind,
                parameters=work.effective_intent,
                evaluation=work.evaluation.model_dump(mode="json")
                if work.evaluation is not None
                else None,
            )
            join = JoinDeclaration.seal(
                recipe=recipe.ref,
                effective_intent=intent,
                members=tuple(
                    JoinMemberDeclaration(branch_id=name, output_roles=roles)
                    for name, roles in sorted(recipe.join.members.items())
                ),
                workflow_intent=WorkflowPlanIntent(
                    operation=OperationIdentityRef(
                        id=operation.id, sha256=operation.contract_sha256
                    ),
                    result_kind="collection",
                    target_registration_id=provider,
                    target_descriptor_sha256=target.descriptor_sha256,
                    requested_target_options=options,
                    input_retrieval_policy=call.retrieve,
                    output_policy=call.output
                    if call.output is not None
                    else OutputCollectionPolicy(),
                ),
            )
        export: Literal["join"] | BranchCollectionExport | None
        if isinstance(recipe.export, BranchExport):
            export = BranchCollectionExport(branch=recipe.export.branch)
        else:
            export = recipe.export
        try:
            policy, grace = _retirement(recipe, work)
            plan = BranchSetPlan.seal(
                parent_work=work,
                decision_sha256=row["decision_sha256"],
                evidence_sha256s=tuple(
                    sorted(
                        item.result.result_sha256
                        for item in self._evidence(work, tuple(recipe.observations))
                    )
                ),
                branches=branches,
                join=join,
                export=export,
                source_collection_retirement_policy=policy,
                source_collection_retirement_grace_seconds=grace,
                selections=selections,
                branch_sets=children,
            )
        except ValueError as exc:
            return self._inapplicable(
                row,
                WorkInapplicable(
                    code="recipe-outcome-inapplicable",
                    message=str(exc),
                ),
            )
        decision = BranchSetDecision(
            plan=plan,
            selections=tuple(selections[key] for key in sorted(selections)),
            branch_sets=tuple(children[key] for key in sorted(children)),
        )
        frames.change(row, phase="ready", decision_json=_json(decision))
        return PlanningProgress("pending", work)

    def deliver_observation(
        self,
        progress: PlanningProgress,
        *,
        owner_kind: ObservationOwnerKind,
        owner_id: str,
        claim: ClaimBinding,
        riverhog: ObservationAuthorityPort,
        deliveries: ObservationDeliveryPort,
    ) -> ContentObservationResult | None:
        return self.observation_delivery.step(
            progress,
            owner_kind=owner_kind,
            owner_id=owner_id,
            claim=claim,
            riverhog=riverhog,
            deliveries=deliveries,
        )

    def accepted_evidence(self, work: WorkIdentity) -> tuple[ContentObservationEvidence, ...]:
        # Physical testimony becomes accepted only after controller validation.
        # A failed preview may retain earlier valid results from incomplete tasks.
        store = self.state.accepted_observations
        q, p = store.tables["question"], store.tables["physical"]
        frames = self.state.compiled_runtime.tables["frames"]
        with self.state.engine.connect() as connection:
            references = tuple(
                connection.scalars(
                    select(p.c.ref_json)
                    .select_from(
                        p.join(q, p.c.question_sha256 == q.c.question_sha256).join(
                            frames, frames.c.work_id == q.c.work_id
                        )
                    )
                    .where(
                        frames.c.owner_work_id == self.state.planning_key(work.work_id),
                        p.c.evidence_json.is_not(None),
                    )
                    .order_by(p.c.request_id)
                )
            )
        return tuple(
            store.original_evidence(
                EvidenceResultRef.model_validate_json(reference).request_id,
                authorize=lambda scope: None,
            )
            for reference in references
        )

    def no_output_policy(
        self, work: WorkIdentity, *, decision: CompiledNoOutputDecision
    ) -> tuple[NoOutputDefinition, SourceCollectionRetirementPolicy, int]:
        recipe, _ = self._definition(work)
        decision.verify_recipe(recipe)
        if decision.work_id != work.work_id:
            raise ValueError("no-output policy requires the exact invocation's indexed decision")
        policy, grace = _retirement(recipe, work)
        return decision.definition, policy, grace

    def operation_contract(self, operation: OperationIdentityRef) -> OperationContract:
        return self.state.recipe_definitions.operation(operation)

    def target_input_selection(
        self, plan: WorkflowPlan, selections: dict[str, ArtifactSelection]
    ) -> ArtifactSelection:
        binding = plan.work.fork_join
        if isinstance(binding, BranchWorkBinding):
            selected = selections.get(binding.artifact_selection_sha256)
            if selected is None:
                raise ValueError("target lost its exact branch selection")
            return selected
        if not isinstance(binding, JoinWorkBinding):
            raise ValueError("target work requires a branch or join input authority")
        subjects: dict[str, WorkArtifactSubject] = {}
        for member in binding.members:
            selected = selections.get(member.artifact_selection_sha256)
            if selected is None:
                raise ValueError("join target lost an exact producer output selection")
            for artifact in selected.artifacts:
                _retain(subjects, artifact.id, artifact)
        return ArtifactSelection.seal(tuple(subjects.values()))

    def target_preflight_request(
        self,
        plan: WorkflowPlan,
        selections: dict[str, ArtifactSelection],
        *,
        descriptor: TargetDescriptor,
    ) -> TargetPreflightRequest:
        selected = self.target_input_selection(plan, selections)
        if descriptor.descriptor_sha256 != plan.target_descriptor_sha256:
            raise ValueError("preflight changed its exact selected target descriptor")
        return TargetPreflightRequest(
            invocation_sha256=plan.workflow_plan_sha256,
            protocol=descriptor.protocol,
            operation_id=plan.operation.id,
            operation_contract_sha256=plan.operation.sha256,
            inputs=TargetInputAuthority.from_selection(selected),
            intent=plan.work.effective_intent,
            target_options=plan.requested_target_options,
            input_groups=plan.input_groups,
            observations=plan.observations,
        )


def _retirement(
    recipe: CompiledRecipe, work: WorkIdentity
) -> tuple[SourceCollectionRetirementPolicy, int]:
    retirement = recipe.source.retirement
    if work.fork_join is not None or retirement.mode == "retain":
        return "retain", 0
    return "retire-after-settlement", retirement.grace_seconds


def _retain[Document: BaseModel](
    documents: dict[str, Document], key: str, document: Document
) -> None:
    existing = documents.setdefault(key, document)
    if existing != document:
        raise ValueError("exact compiled planning identity was rebound")


def _input_problem(operation: OperationContract, selection: ArtifactSelection) -> str | None:
    counts: dict[str, int] = {}
    for artifact in selection.artifacts:
        counts[artifact.role] = counts.get(artifact.role, 0) + 1
    contracts = {item.role: item for item in operation.inputs}
    if "*" not in contracts and set(counts) - set(contracts):
        return "The operation does not accept every selected role."
    for role, contract in contracts.items():
        count = selection.artifact_count if role == "*" else counts.get(role, 0)
        if count < contract.minimum or (contract.maximum is not None and count > contract.maximum):
            return f"The operation input cardinality is inapplicable for role {role}."
    return None
