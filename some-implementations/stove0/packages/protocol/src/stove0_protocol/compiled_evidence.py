"""Exact completed structural condition evidence retained by the controller."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import StrictBool, model_validator

from stove0_protocol.jcs import canonical_json_sha256
from stove0_protocol.models import RecipeIdentityRef, Sha256, Stove0ProtocolModel
from stove0_protocol.observation_evidence import AcceptedView
from stove0_protocol.predicates import FactsQuantification, Truth, quantify
from stove0_protocol.selection_refs import ArtifactSelectionRef


class CompiledFactBinding(Stove0ProtocolModel):
    format: Literal["stove0-compiled-fact-evaluation/v1"] = "stove0-compiled-fact-evaluation/v1"
    work_id: Sha256
    recipe: RecipeIdentityRef
    predicate: FactsQuantification
    inventory: ArtifactSelectionRef
    scope: ArtifactSelectionRef
    view: AcceptedView

    @model_validator(mode="after")
    def exact_scope(self) -> Self:
        task_id, view_id = self.predicate.view.split(".")
        if self.view.work_id != self.work_id or (self.view.task_id, self.view.view_id) != (
            task_id,
            view_id,
        ):
            raise ValueError("fact proof uses another exact task view")
        if self.predicate.scope == "input" and self.scope != self.inventory:
            raise ValueError("input fact proof changed the original invocation scope")
        if self.predicate.scope == "self":
            raise ValueError("whole-scope proof cannot stand in for a member-local predicate")
        return self

    @property
    def evaluation_key(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json", by_alias=True))


class CompiledFactProofPayload(Stove0ProtocolModel):
    format: Literal["stove0-compiled-fact-result/v1"] = "stove0-compiled-fact-result/v1"
    binding: CompiledFactBinding
    positive: StrictBool
    negative: StrictBool
    indeterminate: StrictBool
    truth: Truth

    @model_validator(mode="after")
    def exact_truth(self) -> Self:
        expected = quantify(
            self.binding.predicate.quantifier,
            (
                value
                for present, value in (
                    (self.positive, Truth.TRUE),
                    (self.negative, Truth.FALSE),
                    (self.indeterminate, Truth.INDETERMINATE),
                )
                if present
            ),
        )
        if self.truth != expected:
            raise ValueError("fact proof truth contradicts its complete quantification summary")
        return self


class CompiledFactProof(CompiledFactProofPayload):
    result_sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        expected = canonical_json_sha256(
            self.model_dump(mode="json", by_alias=True, exclude={"result_sha256"})
        )
        if expected != self.result_sha256:
            raise ValueError("fact proof differs from its exact binding and completed result")
        return self

    @classmethod
    def seal(cls, payload: CompiledFactProofPayload) -> Self:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate({**document, "result_sha256": canonical_json_sha256(document)})
