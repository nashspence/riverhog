"""Portable, deployment-owned Stove0 recipe catalog contracts."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal, Self

from config_validation import load_yaml_config
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator
from riverhog_protocol.collection_workflows import RecipeIdentity
from riverhog_protocol.exact_scalar import NonnegativeDecimal
from riverhog_protocol.output_collection_policy import OutputCollectionPolicy
from stove0_protocol import RecipeIdentityRef, SemanticId, Sha256, canonical_json_sha256
from stove0_target_protocol import OperationContract

_JSON_POINTER_PATTERN = r"^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$"


class RecipeModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ArtifactRule(RecipeModel):
    """Assign a role from accepted facts; the first matching rule wins."""

    role: SemanticId = "stove0.source/v1"
    when: tuple[FactPredicate, ...] = ()


class ArtifactAssociation(RecipeModel):
    """Declared role association resolved from accepted relation evidence."""

    primary_role: SemanticId
    associated_roles: tuple[SemanticId, ...] = Field(min_length=1)
    sources: tuple[AssociationEvidenceSource, ...] = Field(min_length=1)

    @field_validator("associated_roles")
    @classmethod
    def canonical_roles(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("associated artifact roles must be unique and canonical")
        return value


class ObservationPartition(RecipeModel):
    """Copy the exact subject IDs assigned a role into an observer question."""

    pointer: str = Field(min_length=1, pattern=_JSON_POINTER_PATTERN)
    roles: tuple[SemanticId, ...] = Field(min_length=1)

    @field_validator("roles")
    @classmethod
    def canonical_roles(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("observation partition roles must be unique and ordered")
        return value


class ObserverUse(RecipeModel):
    registration_id: str
    contract_id: SemanticId
    contract_sha256: Sha256
    options: dict[str, JsonValue] = Field(default_factory=dict)
    after: tuple[str, ...] = ()
    evidence_from: tuple[str, ...] = ()
    evidence_slots_pointer: str | None = Field(
        default=None, min_length=1, pattern=_JSON_POINTER_PATTERN
    )
    subject_roles: tuple[SemanticId, ...] = ()
    partitions: tuple[ObservationPartition, ...] = ()
    subject_batch_size: int | None = Field(default=None, ge=1)
    timeout_seconds: int = Field(default=300, ge=1, le=86400)
    maximum_result_bytes: int = Field(default=1024 * 1024, ge=1, le=64 * 1024 * 1024)
    retrieval_policy: Literal["available-only", "allow"] = "available-only"

    @model_validator(mode="after")
    def staged_question(self) -> Self:
        for label, values in (
            ("observation predecessors", self.after),
            ("forwarded evidence", self.evidence_from),
            ("selected subject roles", self.subject_roles),
        ):
            if values != tuple(sorted(set(values))):
                raise ValueError(f"{label} must be unique and ordered")
        if not set(self.evidence_from) <= set(self.after):
            raise ValueError("forwarded evidence must be a declared predecessor")
        if bool(self.evidence_from) != (self.evidence_slots_pointer is not None):
            raise ValueError("forwarded evidence needs one declared slot option")
        pointers = [item.pointer for item in self.partitions]
        if self.evidence_slots_pointer is not None:
            pointers.append(self.evidence_slots_pointer)
        if len(pointers) != len(set(pointers)):
            raise ValueError("generated observation option pointers must be unique")
        if self.partitions and not self.subject_roles:
            raise ValueError("role partitions require selected subject roles")
        return self


class ArtifactFactBinding(RecipeModel):
    """Locate subject-keyed records inside one observer's declared facts schema."""

    records_pointer: str = Field(pattern=_JSON_POINTER_PATTERN)
    artifact_id_pointer: str = Field(default="/artifact_id", pattern=_JSON_POINTER_PATTERN)


class FactCondition(RecipeModel):
    """One typed JSON Pointer comparison within a selected fact record."""

    pointer: str = Field(pattern=_JSON_POINTER_PATTERN)
    operator: Literal["equals", "exists", "one-of"] = "equals"
    value: JsonValue = None

    @model_validator(mode="after")
    def valid_value(self) -> Self:
        if self.operator == "exists" and not isinstance(self.value, bool):
            raise ValueError("exists conditions require a boolean value")
        if self.operator == "one-of" and (not isinstance(self.value, list) or not self.value):
            raise ValueError("one-of conditions require a nonempty JSON value list")
        return self


class FactPredicate(RecipeModel):
    observation_contract_id: SemanticId
    artifact_roles: tuple[SemanticId, ...] = ()
    artifact_facts: ArtifactFactBinding | None = None
    array_pointer: str | None = Field(default=None, pattern=_JSON_POINTER_PATTERN)
    same_item: tuple[FactCondition, ...] = ()
    pointer: str = Field(pattern=_JSON_POINTER_PATTERN)
    operator: Literal["equals", "not-equals", "contains", "exists", "one-of"] = "equals"
    value: JsonValue = None

    @model_validator(mode="after")
    def valid_scope(self) -> Self:
        if self.artifact_roles != tuple(sorted(set(self.artifact_roles))):
            raise ValueError("predicate artifact roles must be unique and canonical")
        if bool(self.artifact_roles) != (self.artifact_facts is not None):
            raise ValueError(
                "artifact-scoped predicates require artifact roles and an artifact-facts binding"
            )
        if self.operator == "exists" and not isinstance(self.value, bool):
            raise ValueError("exists predicates require a boolean value")
        if self.operator == "one-of" and (not isinstance(self.value, list) or not self.value):
            raise ValueError("one-of predicates require a nonempty JSON value list")
        if self.same_item and self.array_pointer is None:
            raise ValueError("same-item conditions require a declared fact array")
        return self


class AssociationEvidenceSource(RecipeModel):
    """One ordered factual relation source, with no path interpretation."""

    observation_contract_id: SemanticId
    observation_contract_sha256: Sha256
    records_pointer: str = Field(pattern=_JSON_POINTER_PATTERN)
    record_array_pointer: str | None = Field(default=None, pattern=_JSON_POINTER_PATTERN)
    where: tuple[FactPredicate, ...] = ()
    associated_pointer: str = Field(pattern=_JSON_POINTER_PATTERN)
    primary_pointer: str = Field(pattern=_JSON_POINTER_PATTERN)
    endpoint_mode: Literal["subject-id", "exact-endpoint"]
    endpoint_observation_contract_id: SemanticId | None = None
    endpoint_records_pointer: str = Field(default="/artifacts", pattern=_JSON_POINTER_PATTERN)
    endpoint_subject_pointer: str = Field(default="/subject_id", pattern=_JSON_POINTER_PATTERN)
    endpoint_pointers: tuple[str, ...] = ("/state", "/occurrence")
    primary_partition_pointer: str | None = Field(default=None, pattern=_JSON_POINTER_PATTERN)
    associated_partition_pointer: str | None = Field(default=None, pattern=_JSON_POINTER_PATTERN)
    evidence_slots_pointer: str | None = Field(default=None, pattern=_JSON_POINTER_PATTERN)
    status_records_pointer: str | None = Field(default=None, pattern=_JSON_POINTER_PATTERN)
    status_subject_pointer: str = Field(default="/subject_id", pattern=_JSON_POINTER_PATTERN)
    status_pointer: str | None = Field(default=None, pattern=_JSON_POINTER_PATTERN)
    required: tuple[FactPredicate, ...] = ()
    expected_options: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def valid_endpoint_binding(self) -> Self:
        if (self.endpoint_mode == "exact-endpoint") != (
            self.endpoint_observation_contract_id is not None
        ):
            raise ValueError("exact relation endpoints require their selected observer contract")
        if (self.status_records_pointer is None) != (self.status_pointer is None):
            raise ValueError("relation status records and status value pointer must be paired")
        if any(
            rule.observation_contract_id != self.observation_contract_id
            or rule.artifact_roles
            or rule.artifact_facts is not None
            for rule in (*self.where, *self.required)
        ):
            raise ValueError("relation row predicates must use the selected observer's row facts")
        return self


class OperationProjection(RecipeModel):
    """One declarative JSON-pointer copy into an operation request."""

    source: Literal["work-effective-intent", "work-evaluation"]
    source_pointer: str = Field(pattern=_JSON_POINTER_PATTERN)
    destination: Literal["intent", "target-options"]
    destination_pointer: str = Field(pattern=_JSON_POINTER_PATTERN)


class _RecipeRouteBase(RecipeModel):
    id: SemanticId
    when: tuple[FactPredicate, ...] = ()
    primary_role: SemanticId | None = None
    associated_roles: tuple[SemanticId, ...] = ()
    intent: dict[str, JsonValue] = Field(default_factory=dict)
    projections: tuple[OperationProjection, ...] = ()

    @model_validator(mode="after")
    def canonical_members(self) -> Self:
        _validate_projections(self.projections)
        if self.associated_roles != tuple(sorted(set(self.associated_roles))):
            raise ValueError("route associated roles must be unique and canonical")
        if self.associated_roles and self.primary_role is None:
            raise ValueError("associated roles require per-artifact routing with a primary role")
        candidate_roles = set(self.associated_roles)
        if self.primary_role is not None:
            candidate_roles.add(self.primary_role)
        scoped_roles = {role for predicate in self.when for role in predicate.artifact_roles}
        unknown = sorted(scoped_roles - candidate_roles)
        if unknown:
            raise ValueError(
                "artifact-scoped predicates reference roles outside the route candidate: "
                + ", ".join(unknown)
            )
        if scoped_roles and self.primary_role is None:
            raise ValueError("artifact-scoped predicates require a route primary role")
        return self


class RecipeRoute(_RecipeRouteBase):
    """One ordinary target/effect leaf selected by a recipe."""

    kind: Literal["operation"] = "operation"
    operation_id: SemanticId
    target_registration_id: str
    target_options: dict[str, JsonValue] = Field(default_factory=dict)
    forward_observation_contract_ids: tuple[SemanticId, ...] = ()
    input_retrieval_policy: Literal["available-only", "allow"] = "available-only"
    output_policy: OutputCollectionPolicy = Field(default_factory=OutputCollectionPolicy)

    @field_validator("forward_observation_contract_ids")
    @classmethod
    def canonical_forwarded_contracts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("forwarded observation contracts must be unique and ordered")
        return value


class RecipeCoordinationRoute(_RecipeRouteBase):
    """One exact subrecipe selected as a branch-bound coordinator."""

    kind: Literal["coordination"] = "coordination"
    recipe: RecipeIdentityRef

    @model_validator(mode="after")
    def coordination_projections_target_intent_only(self) -> Self:
        if any(item.destination == "target-options" for item in self.projections):
            raise ValueError("coordination routes may project only child effective intent")
        return self


RecipeBranch = Annotated[
    RecipeRoute | RecipeCoordinationRoute,
    Field(discriminator="kind"),
]


class RecipeJoinMember(RecipeModel):
    branch_id: SemanticId
    output_roles: tuple[SemanticId, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def canonical_roles(self) -> Self:
        if self.output_roles != tuple(sorted(set(self.output_roles))):
            raise ValueError("join output roles must be unique and canonical")
        return self


class RecipeJoin(RecipeModel):
    id: SemanticId
    members: tuple[RecipeJoinMember, ...] = Field(min_length=2)
    operation_id: SemanticId
    target_registration_id: str
    intent: dict[str, JsonValue] = Field(default_factory=dict)
    target_options: dict[str, JsonValue] = Field(default_factory=dict)
    projections: tuple[OperationProjection, ...] = ()
    input_retrieval_policy: Literal["available-only", "allow"] = "available-only"
    output_policy: OutputCollectionPolicy = Field(default_factory=OutputCollectionPolicy)

    @model_validator(mode="after")
    def canonical_projections(self) -> Self:
        _validate_projections(self.projections)
        return self


class RecipeNoAction(RecipeModel):
    """Explicit observation decision that succeeds without executing a target."""

    code: SemanticId
    message: str = Field(min_length=1, max_length=1000)
    when: tuple[FactPredicate, ...] = Field(min_length=1)
    source_loss: RecipeSourceLossRule | None = None

    @model_validator(mode="after")
    def global_observation_decision(self) -> Self:
        if any(predicate.artifact_roles for predicate in self.when):
            raise ValueError("no-action predicates must evaluate whole observation facts")
        return self


class RecipeSourceLossEvidenceSlot(RecipeModel):
    """One selected observer fact required for each exact discarded artifact."""

    observation_contract_id: SemanticId
    observation_contract_sha256: Sha256
    facts_profile_sha256: Sha256
    artifact_facts: ArtifactFactBinding
    verdict_pointer: str = Field(pattern=_JSON_POINTER_PATTERN)
    verdict_value: JsonValue


class RecipeSourceLossRule(RecipeModel):
    """Controller policy for an affirmative per-artifact no-successor decision."""

    id: SemanticId
    evidence_slots: tuple[RecipeSourceLossEvidenceSlot, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def canonical_slots(self) -> Self:
        ids = [item.observation_contract_id for item in self.evidence_slots]
        if ids != sorted(set(ids)):
            raise ValueError("source-loss evidence slots must be unique and ordered")
        return self

    @property
    def sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json", exclude_none=True))


class RecipeDefinition(RecipeModel):
    id: SemanticId
    revision: NonnegativeDecimal = Field(ge=1)
    event_input_closure: Literal["single-finalized-collection"] = "single-finalized-collection"
    artifact_rules: tuple[ArtifactRule, ...] = (ArtifactRule(),)
    artifact_associations: tuple[ArtifactAssociation, ...] = ()
    observers: tuple[ObserverUse, ...] = ()
    routes: tuple[RecipeBranch, ...] = Field(min_length=1)
    no_action: RecipeNoAction | None = None
    unmatched_artifact_disposition: Literal["retain-in-source", "reject-work"]
    source_collection_retirement_policy: Literal["retain", "retire-after-settlement"] = Field(
        default="retain",
        description=(
            "Retain source collections, or permit their permanent deletion after exact "
            "settlement, the grace period, and collection deletion checks."
        ),
    )
    source_collection_retirement_grace_seconds: int = Field(default=0, ge=0)
    join: RecipeJoin | None = None

    @model_validator(mode="after")
    def canonical_members(self) -> Self:
        stages = {item.registration_id: item for item in self.observers}
        if len(stages) != len(self.observers):
            raise ValueError("recipe observation registrations must be unique")
        contracts = {item.contract_id: item.registration_id for item in self.observers}
        if len(contracts) != len(self.observers):
            raise ValueError("recipe observation contracts must be selected exactly once")
        completed: set[str] = set()
        while len(completed) < len(stages):
            ready = {
                key
                for key, item in stages.items()
                if key not in completed and set(item.after) <= completed
            }
            if not ready:
                raise ValueError("recipe observation stages contain a cycle or unknown predecessor")
            completed.update(ready)
        for use in self.observers:
            if use.registration_id in use.after:
                raise ValueError("observation stage cannot depend on itself")
            if use.partitions:
                roles = [role for item in use.partitions for role in item.roles]
                if len(roles) != len(set(roles)) or set(roles) != set(use.subject_roles):
                    raise ValueError("observation partitions must cover selected roles exactly")
            if use.subject_roles:
                ancestors = set(use.after)
                pending = list(use.after)
                while pending:
                    predecessor = pending.pop()
                    for earlier in stages[predecessor].after:
                        if earlier not in ancestors:
                            ancestors.add(earlier)
                            pending.append(earlier)
                required = {
                    contracts.get(predicate.observation_contract_id)
                    for rule in self.artifact_rules
                    for predicate in rule.when
                }
                if None in required or not required <= ancestors:
                    raise ValueError("role-selected observation lacks its classification stages")
        association_roles = [item.primary_role for item in self.artifact_associations]
        if association_roles != sorted(association_roles) or len(association_roles) != len(
            set(association_roles)
        ):
            raise ValueError("artifact associations must be unique and ordered by primary role")
        associations = {item.primary_role: item for item in self.artifact_associations}
        observer_contracts = set(contracts)
        for rule in self.artifact_rules:
            if any(item.observation_contract_id not in observer_contracts for item in rule.when):
                raise ValueError("artifact role rule references an undeclared observation")
        for route in self.routes:
            if isinstance(route, RecipeRoute):
                undeclared = set(route.forward_observation_contract_ids) - observer_contracts
                if undeclared:
                    raise ValueError("route forwards an undeclared observation contract")
            if not route.associated_roles:
                continue
            assert route.primary_role is not None
            association = associations.get(route.primary_role)
            if association is None:
                raise ValueError(f"route {route.id} has no artifact association")
            unknown_roles = sorted(set(route.associated_roles) - set(association.associated_roles))
            if unknown_roles:
                raise ValueError(
                    f"route {route.id} references undeclared associated roles: "
                    + ", ".join(unknown_roles)
                )
        route_ids = [route.id for route in self.routes]
        if len(route_ids) != len(set(route_ids)):
            raise ValueError("recipe route IDs must be unique")
        if self.join is not None:
            member_ids = [member.branch_id for member in self.join.members]
            if member_ids != sorted(member_ids) or len(member_ids) != len(set(member_ids)):
                raise ValueError("join members must be unique and ordered by branch ID")
            unknown = sorted(set(member_ids) - set(route_ids))
            if unknown:
                raise ValueError("join members reference unknown route IDs: " + ", ".join(unknown))
        if (
            self.source_collection_retirement_policy == "retain"
            and self.source_collection_retirement_grace_seconds
        ):
            raise ValueError("retain recipes cannot declare a retirement grace period")
        if (
            self.no_action is not None
            and self.source_collection_retirement_policy == "retire-after-settlement"
            and self.no_action.source_loss is None
        ):
            raise ValueError("no-action retirement requires an exact source-loss rule")
        return self

    @property
    def sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json", by_alias=True, exclude_none=True))

    @property
    def ref(self) -> RecipeIdentityRef:
        return RecipeIdentityRef.from_identity(
            RecipeIdentity(id=self.id, revision=self.revision, sha256=self.sha256)
        )

    def identity_document(self) -> dict[str, JsonValue]:
        return self.ref.model_dump(mode="json")


class RecipeCatalog(RecipeModel):
    format: Literal["stove0-recipes/v1"] = "stove0-recipes/v1"
    operations: tuple[OperationContract, ...]
    recipes: tuple[RecipeDefinition, ...]

    @model_validator(mode="after")
    def valid_catalog(self) -> Self:
        operation_ids = [operation.id for operation in self.operations]
        if operation_ids != sorted(operation_ids) or len(operation_ids) != len(set(operation_ids)):
            raise ValueError("operation contracts must be unique and ordered by ID")
        identities = [(recipe.id, recipe.revision) for recipe in self.recipes]
        if identities != sorted(identities) or len(identities) != len(set(identities)):
            raise ValueError("recipes must be unique and ordered by ID and revision")
        operations = {operation.id: operation for operation in self.operations}
        recipes = {(recipe.id, recipe.revision): recipe for recipe in self.recipes}
        for recipe in self.recipes:
            if recipe.no_action is not None and recipe.no_action.source_loss is not None:
                observer_contracts = {
                    item.contract_id: item.contract_sha256 for item in recipe.observers
                }
                for slot in recipe.no_action.source_loss.evidence_slots:
                    if (
                        observer_contracts.get(slot.observation_contract_id)
                        != slot.observation_contract_sha256
                    ):
                        raise ValueError(
                            f"recipe {recipe.id} source-loss slot lacks its selected observer"
                        )
            for route in recipe.routes:
                if not isinstance(route, RecipeCoordinationRoute):
                    continue
                child = recipes.get((route.recipe.id, route.recipe.revision))
                if child is None or child.sha256 != route.recipe.sha256:
                    raise ValueError(
                        f"recipe {recipe.id} references unavailable exact subrecipe "
                        f"{route.recipe.id}@{route.recipe.revision}"
                    )
        _validate_recipe_cycles(self.recipes, recipes)
        for recipe in self.recipes:
            referenced = [
                route.operation_id for route in recipe.routes if isinstance(route, RecipeRoute)
            ]
            if recipe.join is not None:
                referenced.append(recipe.join.operation_id)
            unknown = sorted(set(referenced) - set(operations))
            if unknown:
                raise ValueError(
                    f"recipe {recipe.id} references unknown operation(s): " + ", ".join(unknown)
                )
            for route in recipe.routes:
                if isinstance(route, RecipeCoordinationRoute):
                    continue
                if (
                    operations[route.operation_id].result_kind == "external-effect"
                    and route.output_policy != OutputCollectionPolicy()
                ):
                    raise ValueError(
                        f"effect route {route.id} cannot declare output collection policy"
                    )
            if recipe.join is not None:
                join_operation = operations[recipe.join.operation_id]
                if join_operation.result_kind != "collection":
                    raise ValueError(f"recipe {recipe.id} join must produce a collection")
                route_result_kinds: dict[str, str] = {}
                for route in recipe.routes:
                    if isinstance(route, RecipeRoute):
                        route_result_kinds[route.id] = operations[route.operation_id].result_kind
                        continue
                    child = recipes[(route.recipe.id, route.recipe.revision)]
                    route_result_kinds[route.id] = (
                        "collection" if child.join is not None else "coordination"
                    )
                effect_members = [
                    member.branch_id
                    for member in recipe.join.members
                    if route_result_kinds[member.branch_id] != "collection"
                ]
                if effect_members:
                    raise ValueError(
                        f"recipe {recipe.id} join cannot consume non-collection branch(es): "
                        + ", ".join(effect_members)
                    )
            if recipe.source_collection_retirement_policy == "retire-after-settlement":
                unsafe: list[str] = []
                for route in recipe.routes:
                    if isinstance(route, RecipeRoute):
                        if not operations[
                            route.operation_id
                        ].source_collection_retirement_permitted:
                            unsafe.append(route.id)
                        continue
                    child = recipes[(route.recipe.id, route.recipe.revision)]
                    if any(
                        not operations[operation_id].source_collection_retirement_permitted
                        for operation_id in _descendant_operation_ids(child, recipes)
                    ):
                        unsafe.append(route.id)
                unsafe.sort()
                if unsafe:
                    raise ValueError(
                        f"recipe {recipe.id} retires its source but branch operation(s) do not "
                        "authorize retirement: " + ", ".join(unsafe)
                    )
        return self

    @property
    def sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json", by_alias=True, exclude_none=True))

    def operation(self, operation_id: str) -> OperationContract:
        for operation in self.operations:
            if operation.id == operation_id:
                return operation
        raise KeyError(operation_id)

    @classmethod
    def load(cls, path: Path) -> RecipeCatalog:
        return cls.model_validate(load_yaml_config(Path(path)))

    def recipe(self, recipe_id: str, revision: int | None = None) -> RecipeDefinition:
        matches = [
            recipe
            for recipe in self.recipes
            if recipe.id == recipe_id and (revision is None or recipe.revision == revision)
        ]
        if not matches:
            raise KeyError(recipe_id)
        return max(matches, key=lambda recipe: recipe.revision)

    def validation_document(self) -> dict[str, JsonValue]:
        return {
            "format": "stove0-recipe-catalog-validation/v1",
            "catalog_sha256": self.sha256,
            "operation_count": len(self.operations),
            "recipe_count": len(self.recipes),
            "recipes": [recipe.identity_document() for recipe in self.recipes],
        }


def _validate_projections(projections: tuple[OperationProjection, ...]) -> None:
    keys = [(item.destination, item.destination_pointer) for item in projections]
    if keys != sorted(keys) or len(keys) != len(set(keys)):
        raise ValueError("operation projections must be unique and canonically ordered")
    for index, (destination, pointer) in enumerate(keys):
        for other_destination, other_pointer in keys[index + 1 :]:
            if other_destination != destination:
                continue
            if other_pointer.startswith(f"{pointer}/"):
                raise ValueError("operation projection destinations must not overlap")


def _validate_recipe_cycles(
    recipes: tuple[RecipeDefinition, ...],
    by_identity: dict[tuple[str, int], RecipeDefinition],
) -> None:
    """Reject exact subrecipe cycles without imposing a depth ceiling."""

    complete: set[tuple[str, int]] = set()
    for root in ((item.id, item.revision) for item in recipes):
        if root in complete:
            continue
        visiting: set[tuple[str, int]] = set()
        stack: list[tuple[tuple[str, int], bool]] = [(root, False)]
        while stack:
            identity, leaving = stack.pop()
            if leaving:
                visiting.remove(identity)
                complete.add(identity)
                continue
            if identity in complete:
                continue
            if identity in visiting:
                raise ValueError("recipe catalog contains a subrecipe cycle")
            visiting.add(identity)
            stack.append((identity, True))
            recipe = by_identity[identity]
            children = [
                (route.recipe.id, route.recipe.revision)
                for route in recipe.routes
                if isinstance(route, RecipeCoordinationRoute)
            ]
            for child in reversed(children):
                if child in visiting:
                    raise ValueError("recipe catalog contains a subrecipe cycle")
                if child not in complete:
                    stack.append((child, False))


def _descendant_operation_ids(
    recipe: RecipeDefinition,
    by_identity: dict[tuple[str, int], RecipeDefinition],
) -> tuple[str, ...]:
    operations: set[str] = set()
    stack = [recipe]
    while stack:
        current = stack.pop()
        for route in current.routes:
            if isinstance(route, RecipeRoute):
                operations.add(route.operation_id)
            else:
                stack.append(by_identity[(route.recipe.id, route.recipe.revision)])
        if current.join is not None:
            operations.add(current.join.operation_id)
    return tuple(sorted(operations))


__all__ = [
    "ArtifactAssociation",
    "ArtifactFactBinding",
    "ArtifactRule",
    "AssociationEvidenceSource",
    "FactCondition",
    "FactPredicate",
    "ObservationPartition",
    "ObserverUse",
    "OperationProjection",
    "RecipeBranch",
    "RecipeCatalog",
    "RecipeCoordinationRoute",
    "RecipeDefinition",
    "RecipeJoin",
    "RecipeJoinMember",
    "RecipeNoAction",
    "RecipeRoute",
    "RecipeSourceLossEvidenceSlot",
    "RecipeSourceLossRule",
]
