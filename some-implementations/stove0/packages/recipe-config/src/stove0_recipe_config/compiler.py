"""Compile once to exact closed definitions, and rederive their cached boundary."""

from __future__ import annotations

from collections.abc import Set
from copy import deepcopy
from typing import Literal, overload

from jsonschema import Draft202012Validator
from pydantic import BaseModel, JsonValue
from stove0_protocol import (
    RecipeIdentityRef,
    SemanticValidationProfile,
    SemanticValidationProfilePayload,
    canonical_json_sha256,
)
from stove0_protocol.interface_schemas import schema_slice, validate_row_schema
from stove0_protocol.models import JsonSchemaValidationProfile
from stove0_protocol.observation_interfaces import (
    EvidencePort,
    ExactDocumentRef,
    GlobalFactsView,
    ObservationView,
    RelationView,
    SubjectFactsView,
    SubjectPort,
)
from stove0_protocol.observation_views import coverage_record_schema
from stove0_protocol.predicates import (
    Predicate,
    PredicateAll,
    PredicateAny,
    PredicateFacts,
    PredicateNot,
    facts_predicates,
    pointer_parts,
)
from stove0_protocol.recipe_outcomes import SourceLossRule

from stove0_recipe_config.compiled import (
    CollectionRecipeOutcome,
    CompiledBranch,
    CompiledCall,
    CompiledJoin,
    CompiledObservationTask,
    CompiledOperationCall,
    CompiledRecipe,
    CompiledRecipeBody,
    CompiledRecipeCall,
    CompiledRecipePayload,
    CompiledRoleSelection,
    CompletionRecipeOutcome,
    EvaluationUsage,
    RecipeContract,
    RecipeContractPayload,
    RecipeDependencyRef,
    RecipeExposure,
    RecipeInvocationContract,
    RecipeNormalOutcome,
    RecipeOutcomes,
    RecipeOutputRole,
    RecipeSourceContract,
)
from stove0_recipe_config.dependencies import (
    ObserverResource,
    OperationResource,
    RecipeDependencyCatalog,
    RecipeDependencyClosure,
    RecipeResource,
)
from stove0_recipe_config.diagnostics import (
    RecipeCompileError,
    compile_at,
    compile_value,
    source_pointer,
)
from stove0_recipe_config.source import (
    CallSource,
    ClassificationCase,
    ClassificationSource,
    DecisionSource,
    EvidenceInput,
    GroupSelection,
    InputGroupSource,
    OperationCallSource,
    RecipeCallSource,
    RecipeSource,
    RoleSelection,
    ValueBinding,
    validate_bindings,
)

LANGUAGE_VECTORS = {
    "format": "stove0-recipe-language-vectors/v1",
    "json_equality": [
        {"left": True, "right": 1, "equal": False},
        {"left": None, "right": None, "equal": True},
    ],
    "complete_empty": {"any": False, "every": False, "none": True},
    "missing_comparison": "indeterminate",
    "fact_inspection": {
        "records": {"unsupported": "indeterminate"},
        "status": {"unsupported": "inspectable", "missing": "indeterminate"},
    },
    "binding": {"insert": "absent", "replace": "present", "merge-object": "explicit-shallow"},
    "child_scope": "exact-parent-selection",
}
LANGUAGE_SEMANTICS = SemanticValidationProfile.seal(
    SemanticValidationProfilePayload(
        id="stove0.recipe-language/v1",
        rules=(
            "stove0.recipe.bindings/v1",
            "stove0.recipe.compile/v1",
            "stove0.recipe.conditions/v1",
            "stove0.recipe.exports/v1",
            "stove0.recipe.observation-tasks/v1",
            "stove0.recipe.retirement/v1",
        ),
        conformance_vectors_sha256=canonical_json_sha256(LANGUAGE_VECTORS),
    )
)
LANGUAGE_PROFILE = ExactDocumentRef(
    id=LANGUAGE_SEMANTICS.id, sha256=LANGUAGE_SEMANTICS.profile_sha256
)


def _roles(condition: Predicate, roles: dict[str, str]) -> Predicate:
    if isinstance(condition, bool):
        return condition
    if isinstance(condition, PredicateAll):
        return PredicateAll(all=tuple(_roles(item, roles) for item in condition.all))
    if isinstance(condition, PredicateAny):
        return PredicateAny(any=tuple(_roles(item, roles) for item in condition.any))
    if isinstance(condition, PredicateNot):
        return PredicateNot(**{"not": _roles(condition.negated, roles)})
    filters = condition.facts.roles
    if filters is None:
        return condition
    try:
        semantic = tuple(sorted(roles[role] for role in filters))
    except KeyError as exc:
        raise ValueError(f"unknown recipe role: {exc.args[0]}") from exc
    return PredicateFacts(facts=condition.facts.model_copy(update={"roles": semantic}))


def _bindings(bindings: tuple[ValueBinding, ...]) -> tuple[ValueBinding, ...]:
    validate_bindings(bindings)
    return tuple(sorted(bindings, key=lambda item: (item.to, pointer_parts(item.at))))


def _topological(graph: dict[str, set[str]]) -> tuple[str, ...]:
    for name, dependencies in graph.items():
        missing = dependencies - graph.keys()
        if missing:
            raise ValueError(f"{name}: unresolved predecessors: {', '.join(sorted(missing))}")
    order: list[str] = []
    remaining, complete = set(graph), set[str]()
    while remaining:
        ready = sorted(name for name in remaining if graph[name] <= complete)
        if not ready:
            # Follow actual dependency edges iteratively to give a useful cycle.
            path: list[str] = []
            positions, node = dict[str, int](), min(remaining)
            while node not in positions:
                positions[node] = len(path)
                path.append(node)
                node = min(graph[node] & remaining)
            raise ValueError(
                "recipe dependency cycle: " + " -> ".join(path[positions[node] :] + [node])
            )
        order.extend(ready)
        complete.update(ready)
        remaining.difference_update(ready)
    return tuple(order)


def _resource[T: BaseModel](catalog: RecipeDependencyCatalog, name: str, expected: type[T]) -> T:
    resource = catalog.resources.get(name)
    if not isinstance(resource, expected):
        raise ValueError(f"resource {name}: expected {expected.__name__}")
    return resource


@overload
def _call(call: OperationCallSource, catalog: RecipeDependencyCatalog) -> CompiledOperationCall: ...


@overload
def _call(call: RecipeCallSource, catalog: RecipeDependencyCatalog) -> CompiledRecipeCall: ...


def _call(call: CallSource, catalog: RecipeDependencyCatalog) -> CompiledCall:
    if isinstance(call, RecipeCallSource):
        child = compile_value("/recipe", _resource, catalog, call.recipe, RecipeResource)
        return CompiledRecipeCall(
            recipe=child.recipe.ref, intent=call.intent, bind=_bindings(call.bind)
        )
    operation = compile_value(
        "/operation", _resource, catalog, call.operation, OperationResource
    ).contract
    if operation.result_kind == "external-effect" and call.output is not None:
        raise ValueError("external effect call cannot declare output placement")
    return CompiledOperationCall(
        operation=ExactDocumentRef(id=operation.id, sha256=operation.contract_sha256),
        executor=call.executor,
        intent=call.intent,
        options=call.options,
        bind=_bindings(call.bind),
        evidence=tuple(sorted(set(call.evidence))),
        retrieve=call.retrieve,
        output=call.output.to_policy() if call.output is not None else None,
    )


def _direct_dependencies(body: CompiledRecipeBody) -> tuple[RecipeDependencyRef, ...]:
    refs = {}

    def add(
        kind: Literal[
            "observer-contract", "observation-interface", "operation-contract", "compiled-recipe"
        ],
        ref: ExactDocumentRef | RecipeIdentityRef,
    ) -> None:
        key = (kind, ref.id, ref.sha256)
        refs[key] = RecipeDependencyRef(kind=kind, id=ref.id, sha256=ref.sha256)

    for task in body.observations.values():
        add("observer-contract", task.observer)
        add("observation-interface", task.interface)
    for call in (
        *tuple(branch.call for branch in body.branches.values()),
        *((body.join.call,) if body.join is not None else ()),
    ):
        add(
            "operation-contract" if isinstance(call, CompiledOperationCall) else "compiled-recipe",
            call.operation if isinstance(call, CompiledOperationCall) else call.recipe,
        )
    return tuple(refs[key] for key in sorted(refs))


def collect_closure(
    body: CompiledRecipeBody, catalog: RecipeDependencyCatalog
) -> RecipeDependencyClosure:
    observers, operations, recipes, seen = {}, {}, {}, set()
    pending = list(_direct_dependencies(body))
    resources = tuple(catalog.resources.values())
    while pending:
        ref = pending.pop()
        key = (ref.kind, ref.id, ref.sha256)
        if key in seen:
            continue
        seen.add(key)
        if ref.kind in {"observer-contract", "observation-interface"}:
            matches = [
                resource
                for resource in resources
                if isinstance(resource, ObserverResource)
                and (
                    resource.contract.id == ref.id
                    and resource.contract.contract_sha256 == ref.sha256
                    if ref.kind == "observer-contract"
                    else resource.interface.id == ref.id
                    and resource.interface.interface_sha256 == ref.sha256
                )
            ]
            if not matches:
                raise ValueError("exact observer/interface dependency unavailable offline")
            if ref.kind == "observer-contract":
                # The exact task interface retains the complete owner document.
                # Another unused interface over that same contract is unrelated.
                continue
            resource = matches[0]
            observers[resource.interface.interface_sha256] = resource
        elif ref.kind == "operation-contract":
            operation_matches = [
                resource
                for resource in resources
                if isinstance(resource, OperationResource)
                and resource.contract.id == ref.id
                and resource.contract.contract_sha256 == ref.sha256
            ]
            if not operation_matches:
                raise ValueError("exact operation dependency unavailable offline")
            operations[ref.sha256] = operation_matches[0]
        else:
            child_matches = [
                resource.recipe
                for resource in resources
                if isinstance(resource, RecipeResource)
                and resource.recipe.id == ref.id
                and resource.recipe.sha256 == ref.sha256
            ]
            if not child_matches:
                raise ValueError("exact child recipe dependency unavailable offline")
            child = child_matches[0]
            recipes[ref.sha256] = child
            pending.extend(child.dependencies)
    return RecipeDependencyClosure(
        observers=tuple(observers[key] for key in sorted(observers)),
        operations=tuple(operations[key] for key in sorted(operations)),
        recipes=tuple(recipes[key] for key in sorted(recipes)),
    )


def full_dependencies(
    body: CompiledRecipeBody, closure: RecipeDependencyClosure
) -> tuple[RecipeDependencyRef, ...]:
    refs = {(ref.kind, ref.id, ref.sha256): ref for ref in _direct_dependencies(body)}
    for ref in tuple(refs.values()):
        if ref.kind == "compiled-recipe":
            child = closure.recipe(id=ref.id, sha256=ref.sha256)
            for descendant in child.dependencies:
                refs[(descendant.kind, descendant.id, descendant.sha256)] = descendant
    return tuple(refs[key] for key in sorted(refs))


def _view(body: CompiledRecipeBody, closure: RecipeDependencyClosure, name: str) -> ObservationView:
    task_name, view_name = name.split(".")
    task = body.observations.get(task_name)
    if task is None:
        raise ValueError("condition names an unknown observation task")
    resource = closure.interface(id=task.interface.id, sha256=task.interface.sha256)
    view = resource.interface.views.get(view_name)
    if view is None:
        raise ValueError("condition names an unknown interface view")
    return view


def observation_dependencies(
    body: CompiledRecipeBody, closure: RecipeDependencyClosure
) -> dict[str, set[str]]:
    graph = {name: set(task.after) for name, task in body.observations.items()}
    graph["$classify"] = set()
    for name, task in body.observations.items():
        resource = closure.interface(id=task.interface.id, sha256=task.interface.sha256)
        if task.observer != resource.interface.observer_contract:
            raise ValueError("compiled task's observer differs from its exact interface")
        if set(task.inputs) != set(resource.interface.inputs):
            raise ValueError("task inputs differ from the declared interface ports")
        if task.after != tuple(sorted(set(task.after))):
            raise ValueError("compiled task predecessors are not canonical")
        generated = []
        for port_name, binding in task.inputs.items():
            port = resource.interface.inputs[port_name]
            at = port.option_slots_at if isinstance(port, EvidencePort) else port.option_ids_at
            if at is not None:
                path = pointer_parts(at)
                container = task.options
                for part in path[:-1]:
                    if part not in container:
                        container = {}
                        continue
                    candidate = container[part]
                    if not isinstance(candidate, dict):
                        raise ValueError("generated question option has a non-object ancestor")
                    container = candidate
                if path[-1] in container:
                    raise ValueError("generated question option collides with literal options")
                generated.append(path)
            if isinstance(port, EvidencePort):
                if not isinstance(binding, EvidenceInput):
                    raise ValueError("evidence input requires a named predecessor task")
                graph[name].add(binding.evidence)
                predecessor = body.observations.get(binding.evidence)
                if (
                    predecessor is None
                    or predecessor.observer not in port.contracts
                    or predecessor.interface not in port.interfaces
                ):
                    raise ValueError(
                        "forwarded task differs from the evidence port's exact contracts/interfaces"
                    )
            else:
                if isinstance(binding, EvidenceInput):
                    raise ValueError("subject input cannot bind an evidence task")
                if isinstance(binding, CompiledRoleSelection):
                    if binding.roles != tuple(sorted(set(binding.roles))) or not set(
                        binding.roles
                    ) <= set(body.roles):
                        raise ValueError(
                            "compiled subject role selection is not canonical or declared"
                        )
                    graph[name].add("$classify")
        _partial_literal(task.options, resource.contract.options_schema.document, tuple(generated))
    for case in body.classification.cases:
        for predicate in facts_predicates(case.when):
            graph["$classify"].add(predicate.view.split(".")[0])
            if predicate.roles:
                graph["$classify"].add("$classify")
    _topological(graph)
    return graph


def observation_order(
    body: CompiledRecipeBody, closure: RecipeDependencyClosure
) -> tuple[str, ...]:
    return _topological(observation_dependencies(body, closure))


def _condition(
    body: CompiledRecipeBody,
    closure: RecipeDependencyClosure,
    condition: Predicate,
    scopes: Set[str],
    candidate_roles: Set[str] | None = None,
) -> None:
    for predicate in facts_predicates(condition):
        view = _view(body, closure, predicate.view)
        if isinstance(view, RelationView):
            raise ValueError("facts condition requires a facts view")
        if predicate.scope not in scopes:
            raise ValueError("condition uses an unavailable subject scope")
        if isinstance(view, GlobalFactsView) and (
            predicate.scope != "input" or predicate.roles or predicate.inspect != "records"
        ):
            raise ValueError("global views cannot become subject-keyed evidence")
        if predicate.roles and not set(predicate.roles) <= set(body.roles):
            raise ValueError("compiled condition has undeclared roles")
        if predicate.scope == "self" and predicate.roles:
            raise ValueError("classification cannot filter its unassigned role")
        if (
            predicate.scope == "candidate"
            and predicate.roles
            and (candidate_roles is None or not set(predicate.roles) <= candidate_roles)
        ):
            raise ValueError("candidate condition selects a role outside its input group")
        task_name, _ = predicate.view.split(".")
        task = body.observations[task_name]
        resource = closure.interface(id=task.interface.id, sha256=task.interface.sha256)
        validate_row_schema(
            predicate.where,
            coverage_record_schema()
            if predicate.inspect == "status"
            else schema_slice(resource.contract.facts_schema, view.record_schema_at),
        )


def _call_result(
    call: CompiledCall, closure: RecipeDependencyClosure
) -> RecipeNormalOutcome | None:
    if isinstance(call, CompiledRecipeCall):
        return closure.recipe(id=call.recipe.id, sha256=call.recipe.sha256).contract.outcomes.normal
    operation = closure.operation(id=call.operation.id, sha256=call.operation.sha256)
    if operation.result_kind != "collection":
        return None
    return CollectionRecipeOutcome(
        artifacts=tuple(
            RecipeOutputRole.model_validate(
                {
                    "role": output.role,
                    "minimum": str(output.minimum),
                    "maximum": None if output.maximum is None else str(output.maximum),
                }
            )
            for output in sorted(operation.outputs, key=lambda item: item.role)
        )
    )


def derive_recipe_contract(
    body: CompiledRecipeBody, closure: RecipeDependencyClosure
) -> RecipeContract:
    materializes = effects = False
    retrieval = any(task.retrieve == "allow" for task in body.observations.values())
    reads: set[str] = set()
    results = {}
    calls = [(name, branch.call) for name, branch in body.branches.items()]
    if body.join is not None:
        calls.append(("$join", body.join.call))
    for name, call in calls:
        reads.update(binding.path for binding in call.bind if binding.source == "evaluation")
        results[name] = _call_result(call, closure)
        if isinstance(call, CompiledRecipeCall):
            child = closure.recipe(id=call.recipe.id, sha256=call.recipe.sha256).contract
            reads.update(child.invocation.evaluation.possible_reads)
            materializes |= child.exposure.may_materialize_collections
            effects |= child.exposure.may_perform_external_effects
            retrieval |= child.exposure.may_request_retrieval
        else:
            operation = closure.operation(id=call.operation.id, sha256=call.operation.sha256)
            materializes |= operation.result_kind == "collection"
            effects |= operation.result_kind == "external-effect"
            retrieval |= call.retrieve == "allow"
    normal: RecipeNormalOutcome | None = CompletionRecipeOutcome() if body.branches else None
    if body.export is not None:
        normal = results.get("$join" if body.export == "join" else body.export.branch)
        if not isinstance(normal, CollectionRecipeOutcome):
            raise ValueError("recipe export must designate one collection-capable producer")
    return RecipeContract.seal(
        RecipeContractPayload(
            invocation=RecipeInvocationContract(
                parameters_schema=body.parameters_schema.document,
                evaluation=EvaluationUsage(
                    usage="path-dependent" if reads else "unused",
                    possible_reads=tuple(sorted(reads)),
                ),
            ),
            outcomes=RecipeOutcomes(
                normal=normal,
                no_output_codes=tuple(
                    sorted({decision.no_output.code for decision in body.decisions})
                ),
            ),
            exposure=RecipeExposure(
                may_materialize_collections=materializes,
                may_perform_external_effects=effects,
                may_request_retrieval=retrieval,
            ),
            source=RecipeSourceContract(
                unmatched=body.source.unmatched, root_retirement=body.source.retirement
            ),
        )
    )


def _partial_literal(
    document: JsonValue, schema: dict[str, JsonValue], deferred: tuple[tuple[str, ...], ...]
) -> None:
    for error in Draft202012Validator(schema).iter_errors(document):
        position = tuple(str(part) for part in error.absolute_path)
        # A binding can change conditional applicability, even when a failing
        # branch points at an otherwise untouched literal. Such constraints
        # belong to validation of the final bound invocation.
        if deferred and any(
            keyword in error.absolute_schema_path
            for keyword in ("if", "then", "else", "dependentSchemas")
        ):
            continue
        if error.validator == "required":
            missing = [
                position + (key,) for key in error.validator_value if key not in error.instance
            ]
            if missing and all(
                any(path[: len(item)] == item or item[: len(path)] == path for path in deferred)
                for item in missing
            ):
                continue
            raise RecipeCompileError(
                source_pointer(*position),
                f"literal call arguments contradict the exact schema: {error.message}",
            )
        if any(
            position[: len(path)] == path or path[: len(position)] == position for path in deferred
        ):
            # Aggregate constraints can be unresolved at any ancestor of a
            # destination (anyOf/oneOf/not and object-wide cardinality rules).
            continue
        raise RecipeCompileError(
            source_pointer(*position),
            f"literal call arguments contradict the exact schema: {error.message}",
        )


def _literal_bindings(call: CompiledCall) -> None:
    for binding in call.bind:
        if binding.to == "intent":
            document = call.intent
        elif isinstance(call, CompiledOperationCall):
            document = call.options
        else:
            raise ValueError("child recipe binding cannot target operation options")
        parts = pointer_parts(binding.at)
        if not parts:
            continue
        container = document
        for part in parts[:-1]:
            if part not in container and binding.mode == "insert":
                container = {}
                continue
            child = container.get(part)
            if not isinstance(child, dict):
                raise ValueError("binding destination ancestor is not an object")
            container = child
        present = parts[-1] in container
        if binding.mode == "insert" and present:
            raise ValueError("insert binding collides with a literal call field")
        if binding.mode != "insert" and not present:
            raise ValueError("replace or merge binding requires a literal destination")
        if binding.mode == "merge-object" and not isinstance(container[parts[-1]], dict):
            raise ValueError("merge-object destination must be an object")


def _retirement_capable(body: CompiledRecipeBody, closure: RecipeDependencyClosure) -> bool:
    pending, seen = [body], set()
    while pending:
        current = pending.pop()
        key = canonical_json_sha256(current.model_dump(mode="json", by_alias=True))
        if key in seen:
            continue
        seen.add(key)
        if any(decision.no_output.source_loss is None for decision in current.decisions):
            return False
        # A materializing join consumes derived branch results, not the root's
        # original scope. It cannot grant original-source retirement permission.
        for branch in current.branches.values():
            call = branch.call
            if isinstance(call, CompiledOperationCall):
                if not closure.operation(
                    id=call.operation.id, sha256=call.operation.sha256
                ).source_collection_retirement_permitted:
                    return False
            else:
                pending.append(closure.recipe(id=call.recipe.id, sha256=call.recipe.sha256))
    return True


def validate_compiled_body(body: CompiledRecipeBody, closure: RecipeDependencyClosure) -> None:
    if body.language_profile != LANGUAGE_PROFILE:
        raise ValueError("unsupported exact recipe language profile")
    if not body.branches and not body.decisions:
        raise ValueError("compiled recipe has no calls or decisions")
    if body.dependencies != full_dependencies(body, closure):
        raise ValueError("compiled dependency commitment differs from its exact transitive closure")
    with compile_at("/observe"):
        observation_order(body, closure)
    roles = set(body.roles)
    if body.classification.otherwise is not None and body.classification.otherwise not in roles:
        raise ValueError("classification otherwise has an undeclared semantic role")
    for index, case in enumerate(body.classification.cases):
        if case.role not in roles:
            raise ValueError("classification assigns an undeclared semantic role")
        with compile_at(source_pointer("classify", "cases", str(index), "when")):
            _condition(body, closure, case.when, {"input", "self"})
    for group in body.groups.values():
        if group.attach != tuple(sorted(set(group.attach))):
            raise ValueError("compiled input group roles are not canonical")
        if group.primary not in roles or not set(group.attach) <= roles:
            raise ValueError("input group has undeclared semantic roles")
        for name in group.prefer:
            if not isinstance(_view(body, closure, name), RelationView):
                raise ValueError("input association requires a relation view")
    for index, decision in enumerate(body.decisions):
        with compile_at(source_pointer("decisions", str(index), "when")):
            _condition(body, closure, decision.when, {"input"})
        loss = decision.no_output.source_loss
        if loss is not None:
            keys = [
                (slot.view, canonical_json_sha256(slot.verdict.model_dump(mode="json")))
                for slot in loss.evidence
            ]
            if keys != sorted(set(keys)):
                raise ValueError("compiled source-loss evidence is not canonical")
            for slot in loss.evidence:
                if not isinstance(_view(body, closure, slot.view), SubjectFactsView):
                    raise ValueError("source-loss approval must use exact subject-keyed evidence")
    calls = []
    for name, branch in body.branches.items():
        candidate = None
        if isinstance(branch.select, GroupSelection):
            selected_group = body.groups.get(branch.select.groups)
            if selected_group is None:
                raise ValueError("branch selects an unknown input group")
            candidate = {selected_group.primary, *selected_group.attach}
        _condition(
            body,
            closure,
            branch.when,
            {"input", "candidate"} if candidate is not None else {"input"},
            candidate,
        )
        calls.append((source_pointer("fork", name, "call"), branch.call))
    if body.join is not None:
        if body.join.call.evidence:
            raise ValueError(
                "join cannot forward original-input observation evidence to derived-output inputs"
            )
        calls.append(("/join/call", body.join.call))
        if not isinstance(_call_result(body.join.call, closure), CollectionRecipeOutcome):
            raise ValueError("join must call a collection-producing operation")
        for name, selected_roles in body.join.members.items():
            member_branch = body.branches.get(name)
            result = (
                _call_result(member_branch.call, closure) if member_branch is not None else None
            )
            if not isinstance(result, CollectionRecipeOutcome):
                raise ValueError("join member must have an explicit collection-capable result")
            if tuple(sorted(set(selected_roles))) != selected_roles or not selected_roles:
                raise ValueError("join output roles must be a nonempty canonical set")
            if not set(selected_roles) <= {item.role for item in result.artifacts}:
                raise ValueError("join requests an undeclared output role")
    for pointer, call in calls:
        with compile_at(pointer):
            validate_bindings(call.bind)
            if call.bind != _bindings(call.bind):
                raise ValueError("compiled disjoint bindings are not canonical")
            if isinstance(call, CompiledRecipeCall) and any(
                binding.to != "intent" for binding in call.bind
            ):
                raise ValueError("child recipe bindings cannot select target options")
            _literal_bindings(call)
            deferred = tuple(
                pointer_parts(binding.at) for binding in call.bind if binding.to == "intent"
            )
            if isinstance(call, CompiledRecipeCall):
                child = closure.recipe(id=call.recipe.id, sha256=call.recipe.sha256)
                if child.ref != call.recipe:
                    raise ValueError("child recipe revision differs from its exact identity")
                if any(binding.to != "intent" for binding in call.bind):
                    raise ValueError("child recipe bindings cannot select target options")
                with compile_at("/intent"):
                    _partial_literal(call.intent, child.parameters_schema.document, deferred)
            else:
                operation = closure.operation(id=call.operation.id, sha256=call.operation.sha256)
                with compile_at("/intent"):
                    _partial_literal(call.intent, operation.intent_schema.document, deferred)
                if call.evidence != tuple(sorted(set(call.evidence))) or not set(
                    call.evidence
                ) <= set(body.observations):
                    raise ValueError("call evidence must name canonical declared observation tasks")
                if operation.result_kind == "external-effect" and call.output is not None:
                    raise ValueError("effect call cannot declare collection placement")
    if body.source.retirement.mode == "after-settlement" and not _retirement_capable(body, closure):
        raise ValueError(
            "source retirement is not supported by the exact operation/no-output closure"
        )
    derive_recipe_contract(body, closure)  # Validate explicit export; never infer one.


def verify_compiled_recipe(recipe: CompiledRecipe, closure: RecipeDependencyClosure) -> None:
    """Check every reachable child bottom-up, including cached contract recomputation."""
    # Frozen model attributes do not make nested JSON containers immutable.
    # Reparse the exact retained documents before trusting any digest or pin.
    recipe = CompiledRecipe.model_validate(recipe.model_dump(mode="json", by_alias=True))
    closure = RecipeDependencyClosure.model_validate(closure.model_dump(mode="json", by_alias=True))
    programs: dict[str, CompiledRecipe] = {}
    graph: dict[str, set[str]] = {}
    pending = [recipe]
    while pending:
        current = pending.pop()
        if current.sha256 in programs:
            continue
        programs[current.sha256] = current
        graph[current.sha256] = set()
        for branch in current.branches.values():
            if isinstance(branch.call, CompiledRecipeCall):
                ref = branch.call.recipe
                child = closure.recipe(id=ref.id, sha256=ref.sha256)
                if child.ref != ref:
                    raise ValueError("compiled child reference differs from its exact document")
                graph[current.sha256].add(child.sha256)
                pending.append(child)
    for sha256 in _topological(graph):
        current = programs[sha256]
        validate_compiled_body(current, closure)
        actual = derive_recipe_contract(current, closure)
        if actual != current.contract:
            raise ValueError("supplied RecipeContract differs from its verified compiled body")


def _parameter_schema(schema: dict[str, JsonValue]) -> JsonSchemaValidationProfile:
    # Strip only designated annotations at schema positions. Literal objects in
    # const/default/examples are data, never traversed as language/schema code.
    normalized = deepcopy(schema)
    pending: list[JsonValue] = [normalized]
    while pending:
        node = pending.pop()
        if not isinstance(node, dict):
            continue
        for key in ("$comment", "title", "description", "examples", "default"):
            node.pop(key, None)
        for key in ("$defs", "properties", "patternProperties", "dependentSchemas"):
            children = node.get(key, {})
            if not isinstance(children, dict):
                raise ValueError("parameter schema map must be an object")
            pending.extend(children.values())
        for key in (
            "additionalProperties",
            "unevaluatedProperties",
            "propertyNames",
            "items",
            "contains",
            "unevaluatedItems",
            "if",
            "then",
            "else",
            "not",
        ):
            if key in node:
                pending.append(node[key])
        for key in ("allOf", "anyOf", "oneOf", "prefixItems"):
            branches = node.get(key, [])
            if not isinstance(branches, list):
                raise ValueError("parameter schema alternatives must be an array")
            pending.extend(branches)
    return JsonSchemaValidationProfile.from_schema("stove0.recipe-parameters/v1", normalized)


def compile_recipe(
    source: RecipeSource, catalog: RecipeDependencyCatalog
) -> tuple[CompiledRecipe, RecipeDependencyClosure]:
    observations = {}
    for task_id, task in source.observe.items():
        with compile_at(source_pointer("observe", task_id)):
            with compile_at("/use"):
                resource = _resource(catalog, task.use, ObserverResource)
            inputs = task.inputs
            if inputs is None:
                if set(resource.interface.inputs) != {"subjects"} or not isinstance(
                    resource.interface.inputs["subjects"], SubjectPort
                ):
                    raise ValueError("omitted task inputs require exactly one subjects port")
                inputs = {"subjects": "all"}
            normalized_inputs: dict[
                str, Literal["all"] | CompiledRoleSelection | EvidenceInput
            ] = {}
            for name, binding in inputs.items():
                if isinstance(binding, RoleSelection):
                    try:
                        normalized_inputs[name] = CompiledRoleSelection(
                            roles=tuple(sorted(source.roles[role] for role in binding.roles))
                        )
                    except KeyError as exc:
                        raise ValueError("subject input selects an unknown role alias") from exc
                else:
                    normalized_inputs[name] = binding
            observations[task_id] = CompiledObservationTask(
                observer=resource.interface.observer_contract,
                interface=resource.interface.ref,
                executor=task.executor,
                inputs=normalized_inputs,
                after=tuple(sorted(task.after)),
                options=task.options,
                retrieve=task.retrieve,
            )
    classification = compile_value(
        "/classify",
        lambda: ClassificationSource(
            cases=tuple(
                ClassificationCase(
                    role=source.roles[case.role], when=_roles(case.when, source.roles)
                )
                for case in source.classify.cases
            ),
            otherwise=source.roles[source.classify.otherwise]
            if source.classify.otherwise is not None
            else None,
        ),
    )
    groups = {}
    for name, group in source.groups.items():
        with compile_at(source_pointer("groups", name)):
            groups[name] = InputGroupSource(
                primary=source.roles[group.primary],
                attach=tuple(sorted(source.roles[role] for role in group.attach)),
                prefer=group.prefer,
            )
    decisions = []
    for index, decision in enumerate(source.decisions):
        output = decision.no_output
        if output.source_loss is not None:
            loss = output.source_loss
            slots = tuple(
                sorted(
                    loss.evidence,
                    key=lambda slot: (
                        slot.view,
                        canonical_json_sha256(slot.verdict.model_dump(mode="json")),
                    ),
                )
            )
            if len(slots) != len(
                {canonical_json_sha256(slot.model_dump(mode="json")) for slot in slots}
            ):
                raise ValueError("source-loss evidence slots must be distinct")
            output = output.model_copy(
                update={"source_loss": SourceLossRule(id=loss.id, evidence=slots)}
            )
        decisions.append(
            DecisionSource(
                when=compile_value(
                    source_pointer("decisions", str(index), "when"),
                    _roles,
                    decision.when,
                    source.roles,
                ),
                no_output=output,
            )
        )
    branches: dict[str, CompiledBranch] = {}
    for name, branch in source.fork.items():
        with compile_at(source_pointer("fork", name, "when")):
            when = _roles(branch.when, source.roles)
        with compile_at(source_pointer("fork", name, "call")):
            call = _call(branch.call, catalog)
        branches[name] = CompiledBranch(select=branch.select, when=when, call=call)
    join: CompiledJoin | None = None
    if source.join is not None:
        with compile_at("/join/call"):
            join_call = _call(source.join.call, catalog)
        join = CompiledJoin(
            members={name: tuple(sorted(roles)) for name, roles in source.join.members.items()},
            call=join_call,
        )
    body = CompiledRecipeBody.model_validate(
        {
            "id": source.id,
            "revision": str(source.revision),
            "language_profile": LANGUAGE_PROFILE,
            "parameters_schema": _parameter_schema(source.parameters),
            "roles": tuple(sorted(source.roles.values())),
            "observations": observations,
            "classification": classification,
            "groups": groups,
            "decisions": tuple(decisions),
            "branches": branches,
            "join": join,
            "export": source.export,
            "source": source.source,
            "dependencies": (),
        }
    )
    closure = collect_closure(body, catalog)
    # Verify externally supplied children before consuming their cached public
    # boundary. Aliases, endpoints and original source are not needed for replay.
    for child in closure.recipes:
        verify_compiled_recipe(child, closure)
    body = body.model_copy(update={"dependencies": full_dependencies(body, closure)})
    validate_compiled_body(body, closure)
    contract = derive_recipe_contract(body, closure)
    payload = CompiledRecipePayload(
        **body.model_dump(mode="python", by_alias=True), contract=contract
    )
    recipe = CompiledRecipe.seal(payload)
    verify_compiled_recipe(recipe, closure)
    return recipe, closure


def compile_recipe_catalog(
    sources: dict[str, RecipeSource], dependencies: RecipeDependencyCatalog
) -> tuple[dict[str, CompiledRecipe], dict[str, RecipeDependencyClosure]]:
    if set(sources) & set(dependencies.resources):
        raise ValueError("local recipe names collide with dependency aliases")
    graph = {}
    for name, source in sources.items():
        graph[name] = {
            branch.call.recipe
            for branch in source.fork.values()
            if isinstance(branch.call, RecipeCallSource) and branch.call.recipe in sources
        }
    resources = dict(dependencies.resources)
    compiled, closures = {}, {}
    for name in _topological(graph):
        with compile_at(source_pointer("recipes", name)):
            recipe, closure = compile_recipe(
                sources[name], RecipeDependencyCatalog(resources=resources)
            )
        resources[name] = RecipeResource(recipe=recipe)
        compiled[name], closures[name] = recipe, closure
    return compiled, closures
