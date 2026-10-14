"""Authoring grammar for the single compiled Stove0 recipe language."""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import (
    ConfigDict,
    Field,
    JsonValue,
    StrictBool,
    StrictInt,
    field_validator,
    model_validator,
)
from riverhog_protocol.collection_tags import CollectionTag
from riverhog_protocol.exact_scalar import NonnegativeDecimal
from riverhog_protocol.output_collection_policy import OutputCollectionPolicy
from riverhog_protocol.storage_names import ArchiveStoreName
from stove0_protocol.models import SemanticId, Stove0ProtocolModel
from stove0_protocol.predicates import LocalName, Pointer, Predicate, ViewName, pointer_parts
from stove0_protocol.recipe_outcomes import NoOutputDefinition


class SourceModel(Stove0ProtocolModel):
    model_config = ConfigDict(populate_by_name=False, validate_by_name=False)


def _empty_parameters() -> dict[str, JsonValue]:
    return {"type": "object", "additionalProperties": False}


class RoleSelection(SourceModel):
    roles: tuple[LocalName, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_roles(self) -> Self:
        if len(self.roles) != len(set(self.roles)):
            raise ValueError("selected roles must be unique")
        return self


class EvidenceInput(SourceModel):
    evidence: LocalName


class ObservationTaskSource(SourceModel):
    use: LocalName
    executor: LocalName | None = None
    inputs: dict[LocalName, Literal["all"] | RoleSelection | EvidenceInput] | None = None
    after: tuple[LocalName, ...] = ()
    options: dict[str, JsonValue] = Field(default_factory=dict)
    retrieve: Literal["available-only", "allow"] = "available-only"

    @model_validator(mode="after")
    def unique_predecessors(self) -> Self:
        if len(self.after) != len(set(self.after)):
            raise ValueError("task predecessors must be unique")
        return self


class ClassificationCase(SourceModel):
    role: SemanticId
    when: Predicate


class ClassificationSource(SourceModel):
    cases: tuple[ClassificationCase, ...] = ()
    otherwise: SemanticId | None


class InputGroupSource(SourceModel):
    primary: SemanticId
    attach: tuple[SemanticId, ...] = ()
    prefer: tuple[ViewName, ...] = ()

    @model_validator(mode="after")
    def unique_associations(self) -> Self:
        if len(self.attach) != len(set(self.attach)) or self.primary in self.attach:
            raise ValueError("input group roles must be distinct")
        if len(self.prefer) != len(set(self.prefer)):
            raise ValueError("association preference tiers must be distinct")
        if bool(self.attach) != bool(self.prefer):
            raise ValueError("attached roles and relation views must be declared together")
        return self


class ValueBinding(SourceModel):
    source: Literal["parameters", "evaluation"] = Field(alias="from")
    path: Pointer
    to: Literal["intent", "options"]
    at: Pointer
    mode: Literal["insert", "replace", "merge-object"]


class OutputPolicySource(SourceModel):
    archive_store: ArchiveStoreName | None = None
    use_cache: StrictBool | None = None
    copy_to: tuple[ArchiveStoreName, ...] = ()
    tags: tuple[CollectionTag, ...] = ()

    @field_validator("copy_to", "tags")
    @classmethod
    def canonical_set(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("output policy sets must contain distinct values")
        return tuple(sorted(value, key=lambda item: item.encode("utf-8")))

    @model_validator(mode="after")
    def independent_destinations(self) -> Self:
        self.to_policy()
        return self

    def to_policy(self) -> OutputCollectionPolicy:
        return OutputCollectionPolicy(**self.model_dump(mode="python"))


class OperationCallSource(SourceModel):
    operation: LocalName
    executor: LocalName | None = None
    intent: dict[str, JsonValue] = Field(default_factory=dict)
    options: dict[str, JsonValue] = Field(default_factory=dict)
    bind: tuple[ValueBinding, ...] = ()
    evidence: tuple[LocalName, ...] = ()
    retrieve: Literal["available-only", "allow"] = "available-only"
    output: OutputPolicySource | None = None

    @model_validator(mode="after")
    def distinct_evidence_tasks(self) -> Self:
        if len(self.evidence) != len(set(self.evidence)):
            raise ValueError("forwarded evidence task names must be distinct")
        return self


class RecipeCallSource(SourceModel):
    recipe: LocalName
    intent: dict[str, JsonValue] = Field(default_factory=dict)
    bind: tuple[ValueBinding, ...] = ()

    @model_validator(mode="after")
    def portable_bindings(self) -> Self:
        if any(item.to != "intent" for item in self.bind):
            raise ValueError("child recipe bindings can only set portable parameters")
        return self


type CallSource = OperationCallSource | RecipeCallSource


class GroupSelection(SourceModel):
    groups: LocalName


class BranchSource(SourceModel):
    select: Literal["all"] | GroupSelection = "all"
    when: Predicate = True
    call: CallSource


class JoinOperationCallSource(OperationCallSource):
    # Observation tasks cover recipe inputs; join inputs are produced later.
    evidence: tuple[LocalName, ...] = Field(default=(), max_length=0)


class JoinSource(SourceModel):
    members: dict[LocalName, tuple[SemanticId, ...]] = Field(min_length=2)
    call: JoinOperationCallSource

    @model_validator(mode="after")
    def required_output_roles(self) -> Self:
        if any(not roles or len(roles) != len(set(roles)) for roles in self.members.values()):
            raise ValueError("join members require distinct nonempty output roles")
        return self


class DecisionSource(SourceModel):
    when: Predicate
    no_output: NoOutputDefinition


class RetainSource(SourceModel):
    mode: Literal["retain"] = "retain"


class RetireSource(SourceModel):
    mode: Literal["after-settlement"]
    grace_seconds: NonnegativeDecimal = 0

    @field_validator(
        "grace_seconds",
        mode="before",
        json_schema_input_type=Annotated[StrictInt, Field(ge=0)] | NonnegativeDecimal,
    )
    @classmethod
    def exact_source_integer(cls, value: object) -> object:
        return str(value) if type(value) is int else value


type RetirementSource = Annotated[RetainSource | RetireSource, Field(discriminator="mode")]


class SourceDisposition(SourceModel):
    unmatched: Literal["retain-in-source", "reject-work"] = "retain-in-source"
    retirement: RetirementSource = Field(default_factory=RetainSource)


class BranchExport(SourceModel):
    branch: LocalName


class RecipeSource(SourceModel):
    format: Literal["stove0-recipe/v1"]
    id: SemanticId
    revision: NonnegativeDecimal = Field(ge=1)
    description: str | None = None
    parameters: dict[str, JsonValue] = Field(default_factory=_empty_parameters)
    roles: dict[LocalName, SemanticId] = Field(
        default_factory=lambda: {
            "source": "stove0.source/v1",
        },
        min_length=1,
    )
    observe: dict[LocalName, ObservationTaskSource] = Field(default_factory=dict)
    classify: ClassificationSource = Field(
        default_factory=lambda: ClassificationSource(
            otherwise="source",
        )
    )
    groups: dict[LocalName, InputGroupSource] = Field(default_factory=dict)
    decisions: tuple[DecisionSource, ...] = ()
    fork: dict[LocalName, BranchSource] = Field(default_factory=dict)
    join: JoinSource | None = None
    export: Literal["join"] | BranchExport | None = None
    source: SourceDisposition = Field(default_factory=SourceDisposition)

    @field_validator(
        "revision",
        mode="before",
        json_schema_input_type=Annotated[StrictInt, Field(ge=1)] | NonnegativeDecimal,
    )
    @classmethod
    def exact_source_integer(cls, value: object) -> object:
        return str(value) if type(value) is int else value

    @model_validator(mode="after")
    def definition(self) -> Self:
        if not self.fork and not self.decisions:
            raise ValueError("recipe requires a nonempty fork or decision list")
        if len(self.roles) != len(set(self.roles.values())):
            raise ValueError("role aliases must identify distinct semantic roles")
        for branch in self.fork.values():
            validate_bindings(branch.call.bind)
        if self.join is not None:
            validate_bindings(self.join.call.bind)
        return self


def validate_bindings(bindings: tuple[ValueBinding, ...]) -> None:
    destinations: list[tuple[str, tuple[str, ...]]] = []
    for binding in bindings:
        path = pointer_parts(binding.at)
        if binding.mode == "insert" and not path:
            raise ValueError("the existing document root cannot be inserted")
        for target, prior in destinations:
            if target == binding.to and (path[: len(prior)] == prior or prior[: len(path)] == path):
                raise ValueError("call bindings have overlapping decoded destinations")
        destinations.append((binding.to, path))
