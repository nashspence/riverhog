"""Offline exact documents for recipe compilation and semantic replay."""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator
from stove0_observer_protocol import ObserverContract, SemanticFactsConformanceVectors
from stove0_observer_protocol.interfaces import verify_interface_vectors
from stove0_protocol.models import JSON_SCHEMA_ONLY_SEMANTIC_PROFILE, Stove0ProtocolModel
from stove0_protocol.observation_interfaces import (
    ObservationInterface,
    ObservationInterfaceConformanceVectors,
)
from stove0_protocol.predicates import LocalName
from stove0_target_protocol import OperationContract, SemanticIntentConformanceVectors

from stove0_recipe_config.compiled import CompiledRecipe


class ObserverResource(Stove0ProtocolModel):
    kind: Literal["observer"] = "observer"
    contract: ObserverContract
    interface: ObservationInterface
    interface_vectors: ObservationInterfaceConformanceVectors
    facts_vectors: SemanticFactsConformanceVectors | None = None

    @model_validator(mode="after")
    def exact_documents(self) -> Self:
        self.interface.validate_contract(self.contract)
        if self.interface_vectors.interface_id != self.interface.id or (
            self.interface_vectors.sha256 != self.interface.conformance_vectors_sha256
        ):
            raise ValueError("observation interface conformance identity differs from its document")
        verify_interface_vectors(self.interface, self.contract, self.interface_vectors)
        semantics = self.contract.facts_semantics
        if semantics != JSON_SCHEMA_ONLY_SEMANTIC_PROFILE:
            if (
                self.facts_vectors is None
                or self.facts_vectors.profile_id != semantics.id
                or (self.facts_vectors.sha256 != semantics.conformance_vectors_sha256)
            ):
                raise ValueError(
                    "observer semantic vectors are missing or differ from the exact profile"
                )
        return self


class OperationResource(Stove0ProtocolModel):
    kind: Literal["operation"] = "operation"
    contract: OperationContract
    intent_vectors: SemanticIntentConformanceVectors | None = None

    @model_validator(mode="after")
    def exact_vectors(self) -> Self:
        semantics = self.contract.intent_semantics
        if semantics != JSON_SCHEMA_ONLY_SEMANTIC_PROFILE:
            if (
                self.intent_vectors is None
                or self.intent_vectors.profile_id != semantics.id
                or (self.intent_vectors.sha256 != semantics.conformance_vectors_sha256)
            ):
                raise ValueError(
                    "operation intent vectors are missing or differ from the exact profile"
                )
        return self


class RecipeResource(Stove0ProtocolModel):
    kind: Literal["recipe"] = "recipe"
    recipe: CompiledRecipe


type RecipeResourceDocument = Annotated[
    ObserverResource | OperationResource | RecipeResource, Field(discriminator="kind")
]


class RecipeDependencyCatalog(Stove0ProtocolModel):
    resources: dict[LocalName, RecipeResourceDocument] = Field(default_factory=dict)


class RecipeDependencyClosure(Stove0ProtocolModel):
    format: Literal["stove0-recipe-dependency-closure/v1"] = "stove0-recipe-dependency-closure/v1"
    observers: tuple[ObserverResource, ...] = ()
    operations: tuple[OperationResource, ...] = ()
    recipes: tuple[CompiledRecipe, ...] = ()

    @model_validator(mode="after")
    def canonical_documents(self) -> Self:
        for digests in (
            [document.interface.interface_sha256 for document in self.observers],
            [document.contract.contract_sha256 for document in self.operations],
            [document.sha256 for document in self.recipes],
        ):
            if digests != sorted(set(digests)):
                raise ValueError(
                    "offline dependency documents must be distinct canonical exact sets"
                )
        return self

    def observer(self, *, id: str, sha256: str) -> ObserverResource:
        choices = tuple(
            resource
            for resource in self.observers
            if resource.contract.id == id and resource.contract.contract_sha256 == sha256
        )
        if not choices:
            raise ValueError("exact observer dependency is unavailable offline")
        return choices[0]

    def interface(self, *, id: str, sha256: str) -> ObserverResource:
        choices = tuple(
            resource
            for resource in self.observers
            if resource.interface.id == id and resource.interface.interface_sha256 == sha256
        )
        if len(choices) != 1:
            raise ValueError("exact observation interface is missing or ambiguous offline")
        return choices[0]

    def operation(self, *, id: str, sha256: str) -> OperationContract:
        choices = tuple(
            resource.contract
            for resource in self.operations
            if resource.contract.id == id and resource.contract.contract_sha256 == sha256
        )
        if len(choices) != 1:
            raise ValueError("exact operation dependency is missing or ambiguous offline")
        return choices[0]

    def recipe(self, *, id: str, sha256: str) -> CompiledRecipe:
        choices = tuple(
            recipe for recipe in self.recipes if recipe.id == id and recipe.sha256 == sha256
        )
        if len(choices) != 1:
            raise ValueError("exact compiled recipe dependency is missing or ambiguous offline")
        return choices[0]
