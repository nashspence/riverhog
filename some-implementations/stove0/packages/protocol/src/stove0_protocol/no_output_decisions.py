"""Exact indexed first-true decision preimages, without compiler/runtime imports."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, Protocol, Self

from pydantic import Field, JsonValue, model_validator
from riverhog_protocol.exact_scalar import NonnegativeDecimal

from stove0_protocol.compiled_evidence import CompiledFactProof
from stove0_protocol.jcs import canonical_json_bytes, canonical_json_sha256
from stove0_protocol.models import RecipeIdentityRef, Sha256, Stove0ProtocolModel
from stove0_protocol.predicates import (
    FactsQuantification,
    Predicate,
    Truth,
    evaluate_predicate,
    facts_predicates,
)
from stove0_protocol.recipe_outcomes import NoOutputDefinition
from stove0_protocol.selection_refs import ArtifactSelectionRef


def _fact_key(predicate: FactsQuantification) -> str:
    return canonical_json_sha256(predicate.model_dump(mode="json", by_alias=True))


def _condition_document(condition: Predicate) -> JsonValue:
    return (
        condition
        if isinstance(condition, bool)
        else condition.model_dump(mode="json", by_alias=True)
    )


class NoOutputDecisionDefinition(Protocol):
    @property
    def no_output(self) -> NoOutputDefinition: ...

    @property
    def when(self) -> Predicate: ...


class DecisionRecipe(Protocol):
    @property
    def ref(self) -> RecipeIdentityRef: ...

    @property
    def decisions(self) -> Sequence[NoOutputDecisionDefinition]: ...


class CompiledConditionProofPayload(Stove0ProtocolModel):
    format: Literal["stove0-compiled-decision-condition/v1"] = (
        "stove0-compiled-decision-condition/v1"
    )
    work_id: Sha256
    recipe: RecipeIdentityRef
    inventory: ArtifactSelectionRef
    decision_index: NonnegativeDecimal
    condition: Predicate
    evaluations: tuple[CompiledFactProof, ...]
    truth: Truth

    @model_validator(mode="after")
    def exact_evaluation(self) -> Self:
        keys = [_fact_key(proof.binding.predicate) for proof in self.evaluations]
        required = {_fact_key(predicate) for predicate in facts_predicates(self.condition)}
        if keys != sorted(required):
            raise ValueError(
                "condition proof omits, duplicates or changes required exact predicates"
            )
        answers = {}
        for key, proof in zip(keys, self.evaluations, strict=True):
            binding = proof.binding
            if (
                binding.work_id,
                binding.recipe,
                binding.inventory,
                binding.scope,
                binding.predicate.scope,
            ) != (self.work_id, self.recipe, self.inventory, self.inventory, "input"):
                raise ValueError("no-output condition proof changed its exact invocation domain")
            answers[key] = proof.truth
        actual = evaluate_predicate(self.condition, lambda predicate: answers[_fact_key(predicate)])
        if self.truth != actual:
            raise ValueError("condition proof truth contradicts the typed Boolean program")
        return self


class CompiledConditionProof(CompiledConditionProofPayload):
    proof_sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        if self.proof_sha256 != canonical_json_sha256(
            self.model_dump(mode="json", by_alias=True, exclude={"proof_sha256"})
        ):
            raise ValueError("condition proof digest differs from its exact preimage")
        return self

    @classmethod
    def seal(cls, payload: CompiledConditionProofPayload) -> Self:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate({**document, "proof_sha256": canonical_json_sha256(document)})


class CompiledNoOutputPayload(Stove0ProtocolModel):
    format: Literal["stove0-compiled-no-output-decision/v1"] = (
        "stove0-compiled-no-output-decision/v1"
    )
    work_id: Sha256
    recipe: RecipeIdentityRef
    inventory: ArtifactSelectionRef
    decision_index: NonnegativeDecimal
    definition: NoOutputDefinition
    conditions: tuple[CompiledConditionProof, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def first_true(self) -> Self:
        if len(self.conditions) != self.decision_index + 1:
            raise ValueError("no-output decision lacks its complete first-true proof")
        for index, condition in enumerate(self.conditions):
            if (
                condition.work_id,
                condition.recipe,
                condition.inventory,
                condition.decision_index,
            ) != (self.work_id, self.recipe, self.inventory, index) or condition.truth != (
                Truth.TRUE if index == self.decision_index else Truth.FALSE
            ):
                raise ValueError("no-output decision skipped or changed an earlier condition")
        return self


class CompiledNoOutputDecision(CompiledNoOutputPayload):
    decision_sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        if self.decision_sha256 != canonical_json_sha256(
            self.model_dump(mode="json", by_alias=True, exclude={"decision_sha256"})
        ):
            raise ValueError(
                "no-output decision differs from its exact selected definition and proof"
            )
        return self

    @classmethod
    def seal(cls, payload: CompiledNoOutputPayload) -> Self:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate({**document, "decision_sha256": canonical_json_sha256(document)})

    def verify_recipe(self, recipe: DecisionRecipe) -> None:
        if self.recipe != recipe.ref or self.decision_index >= len(recipe.decisions):
            raise ValueError("no-output decision does not belong to its retained exact recipe")
        if canonical_json_bytes(
            self.definition.model_dump(mode="json", by_alias=True)
        ) != canonical_json_bytes(
            recipe.decisions[self.decision_index].no_output.model_dump(mode="json", by_alias=True)
        ) or any(
            canonical_json_bytes(
                condition.condition
                if isinstance(condition.condition, bool)
                else condition.condition.model_dump(mode="json", by_alias=True)
            )
            != canonical_json_bytes(_condition_document(recipe.decisions[index].when))
            for index, condition in enumerate(self.conditions)
        ):
            raise ValueError("no-output decision changed the indexed compiled definition")
