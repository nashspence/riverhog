"""Media target output advice from accepted canonical Occurrence hint evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Literal, Self

from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_OBSERVER_CONTRACT,
    validate_materialization_hint_facts,
)
from a_stove0_media_metadata_contract_lib import MEDIA_METADATA_OBSERVATION_ID
from pydantic import BaseModel, ConfigDict, Field, model_validator
from riverhog_protocol.provenance_transport import MaterializationHintDocument
from stove0_observer_protocol import canonical_json_bytes, canonical_json_sha256
from stove0_protocol import Sha256
from stove0_target_protocol import TargetPreflightRequest


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class MaterializationDecisionRequired(ValueError):
    """An output lacks canonical advice and explicit permission to omit it."""


class MediaOutputPublicationDecision(_Model):
    output_id: str = Field(min_length=1)
    materialization_hint: MaterializationHintDocument | None = None
    allow_missing_materialization_hint: bool = False

    @model_validator(mode="after")
    def exact_choice(self) -> Self:
        if "materialization_hint" in self.model_fields_set and self.materialization_hint is None:
            raise ValueError("a null output hint is not an omission decision")
        if (self.materialization_hint is None) != self.allow_missing_materialization_hint:
            raise ValueError("output publication requires advice or explicit omission")
        return self

    @property
    def components(self) -> tuple[str, ...] | None:
        return (
            None
            if self.materialization_hint is None
            else tuple(self.materialization_hint.components)
        )


class MediaPublicationPlanPayload(_Model):
    format: Literal["stove0-media-publication-decisions/v1"] = (
        "stove0-media-publication-decisions/v1"
    )
    hint_result_sha256s: tuple[Sha256, ...] = ()
    decisions: tuple[MediaOutputPublicationDecision, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def complete_canonical_set(self) -> Self:
        if self.hint_result_sha256s != tuple(sorted(set(self.hint_result_sha256s))):
            raise ValueError("hint evidence identities must be unique and ordered")
        ids = tuple(item.output_id for item in self.decisions)
        if ids != tuple(sorted(set(ids))):
            raise ValueError("output publication decisions must be unique and ordered")
        return self


class MediaPublicationPlan(MediaPublicationPlanPayload):
    decision_set_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def verify_identity(self) -> Self:
        document = self.model_dump(
            mode="json", exclude_none=True, exclude={"decision_set_sha256"}
        )
        if canonical_json_sha256(document) != self.decision_set_sha256:
            raise ValueError("media publication decision set changed identity")
        return self

    @classmethod
    def seal(cls, payload: MediaPublicationPlanPayload) -> MediaPublicationPlan:
        document = payload.model_dump(mode="json", exclude_none=True)
        return cls(
            **payload.model_dump(exclude_none=True),
            decision_set_sha256=canonical_json_sha256(document),
        )

    @classmethod
    def from_json_value(cls, document: object) -> MediaPublicationPlan:
        return cls.model_validate_json(canonical_json_bytes(document))

    def decision_for(self, output_id: str) -> MediaOutputPublicationDecision:
        for item in self.decisions:
            if item.output_id == output_id:
                return item
        raise ValueError("actual output has no accepted publication decision")

    def require_exact_outputs(self, output_ids: Sequence[str]) -> None:
        if tuple(sorted(output_ids)) != tuple(item.output_id for item in self.decisions):
            raise ValueError("actual output set differs from accepted publication decisions")


def accepted_source_hints(
    request: TargetPreflightRequest,
) -> tuple[dict[str, tuple[str, ...] | None], tuple[str, ...]]:
    """Resolve only controller-forwarded hint facts for exact selected inputs."""

    selected = {
        subject.id: subject
        for item in request.observations
        if item.request.observer_contract_id == MEDIA_METADATA_OBSERVATION_ID
        for subject in item.request.subjects
    }
    expected_ids = {
        subject_id
        for group in request.input_groups
        for subject_id in (group.primary_id, *group.associated_ids)
    }
    if not selected or set(selected) != expected_ids:
        raise ValueError("media selection lacks exact accepted subject evidence")
    hints: dict[str, tuple[str, ...] | None] = {}
    identities: list[str] = []
    for item in request.observations:
        if item.request.observer_contract_id != MATERIALIZATION_HINT_OBSERVER_CONTRACT.id:
            continue
        if (
            item.request.observer_contract_sha256
            != MATERIALIZATION_HINT_OBSERVER_CONTRACT.contract_sha256
            or item.request.read_actions != ("read-provenance",)
            or item.request.options
            or item.result.state != "observed"
            or item.result.facts_schema != MATERIALIZATION_HINT_OBSERVER_CONTRACT.facts_schema
            or item.result.facts is None
        ):
            raise ValueError("media hint evidence differs from its accepted contract")
        facts = validate_materialization_hint_facts(item.result.facts, item.request.subjects)
        identities.append(item.result.result_sha256)
        for subject, fact in zip(item.request.subjects, facts.artifacts, strict=True):
            expected = selected.get(subject.id)
            if (
                expected is None
                or subject.id in hints
                or (subject.collection, subject.artifact_id, subject.bytes, subject.sha256)
                != (
                    expected.collection,
                    expected.artifact_id,
                    expected.bytes,
                    expected.sha256,
                )
            ):
                raise ValueError("hint evidence differs from an exact selected input")
            hints[subject.id] = (
                None
                if fact.materialization_hint is None
                else tuple(
                    MaterializationHintDocument.model_validate(
                        fact.materialization_hint
                    ).components
                )
            )
    if set(hints) != expected_ids:
        raise ValueError("accepted hint evidence does not cover every selected input")
    return hints, tuple(sorted(set(identities)))


def replace_final_suffix(
    source: tuple[str, ...] | None, suffix: str
) -> tuple[str, ...] | None:
    """Adapt only an accepted leaf suffix; leave unknown source names unnamed."""

    if source is None or not suffix.startswith(".") or len(suffix) < 2:
        return None
    stem, dot, old_suffix = source[-1].rpartition(".")
    if not dot or not stem or not old_suffix:
        return None
    return (*source[:-1], stem + suffix)


def append_leaf_suffix(source: tuple[str, ...] | None, suffix: str) -> tuple[str, ...] | None:
    if source is None:
        return None
    return (*source[:-1], source[-1] + suffix)


def sibling_hint(
    source: tuple[str, ...] | None, leaf: str
) -> tuple[str, ...] | None:
    if source is None:
        return None
    return (*source[:-1], leaf)


def seal_publication_plan(
    proposals: Mapping[str, tuple[str, ...] | None],
    *,
    hint_result_sha256s: tuple[str, ...],
    allow_missing: bool,
) -> MediaPublicationPlan:
    """Expand only a caller's explicit omission policy into exact output choices."""

    decisions: list[MediaOutputPublicationDecision] = []
    for output_id, components in sorted(proposals.items()):
        if components is None:
            if not allow_missing:
                raise MaterializationDecisionRequired(
                    "No accepted source hint supports "
                    f"output {output_id}; explicitly allow omission for this invocation"
                )
            decisions.append(
                MediaOutputPublicationDecision(
                    output_id=output_id, allow_missing_materialization_hint=True
                )
            )
        else:
            decisions.append(
                MediaOutputPublicationDecision(
                    output_id=output_id,
                    materialization_hint=MaterializationHintDocument(components=list(components)),
                )
            )
    return MediaPublicationPlan.seal(
        MediaPublicationPlanPayload(
            hint_result_sha256s=hint_result_sha256s,
            decisions=tuple(decisions),
        )
    )


__all__ = [
    "MediaOutputPublicationDecision",
    "MediaPublicationPlan",
    "MaterializationDecisionRequired",
    "accepted_source_hints",
    "append_leaf_suffix",
    "replace_final_suffix",
    "seal_publication_plan",
    "sibling_hint",
]
