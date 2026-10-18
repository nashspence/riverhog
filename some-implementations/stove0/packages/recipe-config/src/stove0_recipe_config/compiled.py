"""Closed semantic recipe definitions and separately derived public boundaries."""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, StrictBool, model_validator
from riverhog_protocol.exact_scalar import NonnegativeDecimal
from riverhog_protocol.output_collection_policy import OutputCollectionPolicy
from stove0_protocol import RecipeIdentityRef, canonical_json_sha256
from stove0_protocol.models import (
    JsonSchemaValidationProfile,
    SemanticId,
    Sha256,
    Stove0ProtocolModel,
)
from stove0_protocol.observation_interfaces import ExactDocumentRef
from stove0_protocol.predicates import LocalName, Pointer, Predicate

from stove0_recipe_config.source import (
    BranchExport,
    ClassificationSource,
    DecisionSource,
    EvidenceInput,
    GroupSelection,
    InputGroupSource,
    RetirementSource,
    SourceDisposition,
    ValueBinding,
)


class CompiledRoleSelection(Stove0ProtocolModel):
    roles: tuple[SemanticId, ...] = Field(min_length=1)


class CompiledObservationTask(Stove0ProtocolModel):
    observer: ExactDocumentRef
    interface: ExactDocumentRef
    executor: LocalName | None = None
    inputs: dict[LocalName, Literal["all"] | CompiledRoleSelection | EvidenceInput]
    after: tuple[LocalName, ...] = ()
    options: dict[str, JsonValue] = Field(default_factory=dict)
    retrieve: Literal["available-only", "allow"] = "available-only"


class CompiledOperationCall(Stove0ProtocolModel):
    kind: Literal["operation"] = "operation"
    operation: ExactDocumentRef
    executor: LocalName | None = None
    intent: dict[str, JsonValue] = Field(default_factory=dict)
    options: dict[str, JsonValue] = Field(default_factory=dict)
    bind: tuple[ValueBinding, ...] = ()
    evidence: tuple[LocalName, ...] = ()
    retrieve: Literal["available-only", "allow"] = "available-only"
    output: OutputCollectionPolicy | None = None


class CompiledRecipeCall(Stove0ProtocolModel):
    kind: Literal["recipe"] = "recipe"
    recipe: RecipeIdentityRef
    intent: dict[str, JsonValue] = Field(default_factory=dict)
    bind: tuple[ValueBinding, ...] = ()


type CompiledCall = Annotated[
    CompiledOperationCall | CompiledRecipeCall, Field(discriminator="kind")
]


class CompiledBranch(Stove0ProtocolModel):
    select: Literal["all"] | GroupSelection = "all"
    when: Predicate = True
    call: CompiledCall


class CompiledJoin(Stove0ProtocolModel):
    members: dict[LocalName, tuple[SemanticId, ...]] = Field(min_length=2)
    call: CompiledOperationCall


class RecipeDependencyRef(Stove0ProtocolModel):
    kind: Literal[
        "observer-contract", "observation-interface", "operation-contract", "compiled-recipe"
    ]
    id: SemanticId
    sha256: Sha256


class CompiledRecipeBody(Stove0ProtocolModel):
    format: Literal["stove0-compiled-recipe/v1"] = "stove0-compiled-recipe/v1"
    id: SemanticId
    revision: NonnegativeDecimal = Field(ge=1)
    language_profile: ExactDocumentRef
    parameters_schema: JsonSchemaValidationProfile
    roles: tuple[SemanticId, ...] = Field(min_length=1)
    observations: dict[LocalName, CompiledObservationTask]
    classification: ClassificationSource
    groups: dict[LocalName, InputGroupSource]
    decisions: tuple[DecisionSource, ...]
    branches: dict[LocalName, CompiledBranch]
    join: CompiledJoin | None = None
    export: Literal["join"] | BranchExport | None = None
    source: SourceDisposition
    dependencies: tuple[RecipeDependencyRef, ...]

    @model_validator(mode="after")
    def canonical_sets(self) -> Self:
        if self.roles != tuple(sorted(set(self.roles))):
            raise ValueError("compiled semantic roles must be a canonical set")
        keys = [(ref.kind, ref.id, ref.sha256) for ref in self.dependencies]
        if keys != sorted(set(keys)):
            raise ValueError("compiled dependencies must be a canonical set of exact references")
        return self


class CompiledRecipePayload(CompiledRecipeBody):
    contract: RecipeContract


class CompiledRecipe(CompiledRecipePayload):
    sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        document = self.model_dump(mode="json", by_alias=True, exclude={"sha256"})
        if canonical_json_sha256(document) != self.sha256:
            raise ValueError("compiled recipe digest differs from its normalized body")
        return self

    @classmethod
    def seal(cls, payload: CompiledRecipePayload) -> CompiledRecipe:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate({**document, "sha256": canonical_json_sha256(document)})

    @property
    def ref(self) -> RecipeIdentityRef:
        return RecipeIdentityRef.model_validate(
            {"id": self.id, "revision": str(self.revision), "sha256": self.sha256}
        )


class RecipeCollectionDomain(Stove0ProtocolModel):
    minimum: Literal["1"] = "1"
    maximum: None = None
    state: Literal["finalized"] = "finalized"
    root_scope: Literal["complete-root-inventories"] = "complete-root-inventories"
    child_scope: Literal["parent-bound-exact-selection"] = "parent-bound-exact-selection"


class EvaluationUsage(Stove0ProtocolModel):
    usage: Literal["unused", "path-dependent"]
    possible_reads: tuple[Pointer, ...]


class RecipeInvocationContract(Stove0ProtocolModel):
    collections: RecipeCollectionDomain = Field(default_factory=RecipeCollectionDomain)
    parameters_schema: dict[str, JsonValue]
    evaluation: EvaluationUsage


class RecipeOutputRole(Stove0ProtocolModel):
    role: SemanticId
    minimum: NonnegativeDecimal
    maximum: NonnegativeDecimal | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def valid_cardinality(self) -> Self:
        if self.maximum is not None and self.maximum < self.minimum:
            raise ValueError("recipe output cardinality is invalid")
        return self


class CollectionRecipeOutcome(Stove0ProtocolModel):
    kind: Literal["collection"] = "collection"
    artifacts: tuple[RecipeOutputRole, ...] = Field(min_length=1)


class CompletionRecipeOutcome(Stove0ProtocolModel):
    kind: Literal["completion"] = "completion"


type RecipeNormalOutcome = Annotated[
    CollectionRecipeOutcome | CompletionRecipeOutcome, Field(discriminator="kind")
]


class RecipeOutcomes(Stove0ProtocolModel):
    normal: RecipeNormalOutcome | None
    no_output_codes: tuple[SemanticId, ...]


class RecipeExposure(Stove0ProtocolModel):
    may_materialize_collections: StrictBool
    may_perform_external_effects: StrictBool
    may_request_retrieval: StrictBool


class RecipeSourceContract(Stove0ProtocolModel):
    unmatched: Literal["retain-in-source", "reject-work"]
    root_retirement: RetirementSource
    parent_bound_retirement: Literal["retain"] = "retain"


class RecipeContractPayload(Stove0ProtocolModel):
    format: Literal["stove0-recipe-contract/v1"] = "stove0-recipe-contract/v1"
    projection_profile: Literal["stove0-recipe-contract-projection/v1"] = (
        "stove0-recipe-contract-projection/v1"
    )
    invocation: RecipeInvocationContract
    outcomes: RecipeOutcomes
    exposure: RecipeExposure
    source: RecipeSourceContract


class RecipeContract(RecipeContractPayload):
    contract_sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        document = self.model_dump(mode="json", by_alias=True, exclude={"contract_sha256"})
        if canonical_json_sha256(document) != self.contract_sha256:
            raise ValueError("recipe contract digest differs from its public boundary")
        return self

    @classmethod
    def seal(cls, payload: RecipeContractPayload) -> RecipeContract:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate({**document, "contract_sha256": canonical_json_sha256(document)})


CompiledRecipePayload.model_rebuild()
CompiledRecipe.model_rebuild()
