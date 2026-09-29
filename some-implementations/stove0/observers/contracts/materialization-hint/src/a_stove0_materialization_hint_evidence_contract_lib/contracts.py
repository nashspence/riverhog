"""Stove0 evidence that forwards existing canonical Occurrence advice."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator
from riverhog_protocol import CollectionArtifactProvenanceBindingDocument
from riverhog_provenance_contracts import PROFILE, ContractCatalog, ExternalReference
from stove0_observer_protocol import (
    ContentObservationRequest,
    JsonSchemaValidationProfile,
    ObserverContract,
    ObserverContractPayload,
    SemanticFactsConformanceVectors,
    SemanticValidationProfile,
    SemanticValidationProfilePayload,
    SemanticValidatorBinding,
    WorkArtifactSubject,
    canonical_json_bytes,
)

MATERIALIZATION_HINT_OBSERVATION_ID = "stove0.riverhog-materialization-hint/v1"
_HINT_SCHEMA = PROFILE + "/materialization-hint.schema.json"


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class MaterializationHintFact(_Model):
    """One selected subject and its exact canonical Occurrence assertion."""

    subject_id: str = Field(min_length=1)
    primary_binding: CollectionArtifactProvenanceBindingDocument
    occurrence: ExternalReference
    materialization_hint: dict[str, JsonValue] | None

    @model_validator(mode="after")
    def exact_occurrence(self) -> Self:
        if self.occurrence.object_type != "occurrence":
            raise ValueError("materialization hint support must name an Occurrence")
        if self.primary_binding.journal.journal_id != self.occurrence.journal_id or int(
            self.occurrence.entry.sequence
        ) > int(self.primary_binding.journal.through.sequence):
            raise ValueError("materialization hint Occurrence is outside its primary binding")
        return self

    @field_validator("materialization_hint")
    @classmethod
    def canonical_hint(cls, value: dict[str, JsonValue] | None) -> dict[str, JsonValue] | None:
        if value is not None:
            ContractCatalog().validate(_HINT_SCHEMA, value)
        return value


class MaterializationHintFacts(_Model):
    artifacts: tuple[MaterializationHintFact, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def canonical_artifacts(self) -> Self:
        ids = [item.subject_id for item in self.artifacts]
        if ids != sorted(set(ids)):
            raise ValueError("materialization hint facts must be unique and ordered by subject")
        return self


def validate_materialization_hint_facts(
    facts: Mapping[str, object], subjects: Sequence[WorkArtifactSubject]
) -> MaterializationHintFacts:
    document = MaterializationHintFacts.model_validate_json(canonical_json_bytes(dict(facts)))
    if tuple(item.subject_id for item in document.artifacts) != tuple(item.id for item in subjects):
        raise ValueError("materialization hint facts differ from the exact request subjects")
    if any(
        fact.primary_binding.artifact_id != subject.artifact_id
        for fact, subject in zip(document.artifacts, subjects, strict=True)
    ):
        raise ValueError("materialization hint binding differs from the exact member")
    return document


def _validate_observation(request: ContentObservationRequest, facts: Mapping[str, object]) -> None:
    validate_materialization_hint_facts(facts, request.subjects)


MATERIALIZATION_HINT_OPTIONS_SCHEMA = JsonSchemaValidationProfile.from_schema(
    "stove0.riverhog-materialization-hint-options/v1",
    {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
    },
)
MATERIALIZATION_HINT_FACTS_SCHEMA = JsonSchemaValidationProfile.from_schema(
    "stove0.riverhog-materialization-hint-facts/v1",
    MaterializationHintFacts.model_json_schema(),
)
_SAMPLE_SUBJECT = {
    "id": "sample",
    "role": "stove0.source/v1",
    "collection": {
        "collection_id": "1",
        "archive_root_sha256": "a" * 64,
        "artifact_set_identity": "b" * 64,
    },
    "artifact_id": "c" * 64,
    "bytes": "1",
    "sha256": "d" * 64,
}
_SAMPLE_OCCURRENCE: dict[str, object] = {
    "scope": "external",
    "journal_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
    "entry": {
        "entry_id": "urn:uuid:22222222-2222-4222-8222-222222222222",
        "sequence": "0",
        "json_sha256": "e" * 64,
    },
    "assertion_id": "urn:uuid:33333333-3333-4333-8333-333333333333",
    "object_id": "urn:uuid:44444444-4444-4444-8444-444444444444",
    "object_type": "occurrence",
}
_SAMPLE_BINDING: dict[str, object] = {
    "artifact_id": "c" * 64,
    "journal": {
        "journal_id": _SAMPLE_OCCURRENCE["journal_id"],
        "through": _SAMPLE_OCCURRENCE["entry"],
        "prefix_sha256": "f" * 64,
        "prefix_bytes": "100",
    },
    "delivery_association_id": "urn:uuid:55555555-5555-4555-8555-555555555555",
}
_SAMPLE_FACT: dict[str, object] = {
    "subject_id": "sample",
    "primary_binding": _SAMPLE_BINDING,
    "occurrence": _SAMPLE_OCCURRENCE,
    "materialization_hint": {"components": ["archive", "sample.bin"]},
}
MATERIALIZATION_HINT_CONFORMANCE_VECTORS = SemanticFactsConformanceVectors.model_validate(
    {
        "profile_id": "stove0.riverhog-materialization-hint-facts-semantics/v1",
        "vectors": [
            {
                "id": "accepted-exact-subject-and-hint",
                "accepted": True,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {"artifacts": [_SAMPLE_FACT]},
            },
            {
                "id": "rejected-invalid-hint",
                "accepted": False,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {
                    "artifacts": [{**_SAMPLE_FACT, "materialization_hint": {"components": [".."]}}]
                },
            },
            {
                "id": "rejected-other-member-binding",
                "accepted": False,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {
                    "artifacts": [
                        {
                            **_SAMPLE_FACT,
                            "primary_binding": {
                                **_SAMPLE_BINDING,
                                "artifact_id": "0" * 64,
                            },
                        }
                    ]
                },
            },
            {
                "id": "rejected-other-subject",
                "accepted": False,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {"artifacts": [{**_SAMPLE_FACT, "subject_id": "other"}]},
            },
            {
                "id": "rejected-state-reference",
                "accepted": False,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {
                    "artifacts": [
                        {
                            **_SAMPLE_FACT,
                            "occurrence": {**_SAMPLE_OCCURRENCE, "object_type": "state"},
                        }
                    ]
                },
            },
        ],
    }
)
MATERIALIZATION_HINT_FACTS_SEMANTICS = SemanticValidationProfile.seal(
    SemanticValidationProfilePayload(
        id="stove0.riverhog-materialization-hint-facts-semantics/v1",
        rules=(
            "stove0.riverhog-materialization-hint.canonical-advice/v1",
            "stove0.riverhog-materialization-hint.exact-subject-and-occurrence/v1",
            "stove0.riverhog-materialization-hint.root-selected-primary-binding/v1",
        ),
        conformance_vectors_sha256=MATERIALIZATION_HINT_CONFORMANCE_VECTORS.sha256,
    )
)
MATERIALIZATION_HINT_SEMANTIC_VALIDATOR = SemanticValidatorBinding.from_profile(
    MATERIALIZATION_HINT_FACTS_SEMANTICS, _validate_observation
)
MATERIALIZATION_HINT_OBSERVER_CONTRACT = ObserverContract.seal(
    ObserverContractPayload(
        id=MATERIALIZATION_HINT_OBSERVATION_ID,
        read_actions=("read-provenance",),
        options_schema=MATERIALIZATION_HINT_OPTIONS_SCHEMA,
        facts_schema=MATERIALIZATION_HINT_FACTS_SCHEMA,
        facts_semantics=MATERIALIZATION_HINT_FACTS_SEMANTICS,
        maximum_result_bytes=1024 * 1024,
    )
)


__all__ = [
    "MATERIALIZATION_HINT_OBSERVATION_ID",
    "MATERIALIZATION_HINT_OBSERVER_CONTRACT",
    "MATERIALIZATION_HINT_SEMANTIC_VALIDATOR",
    "MaterializationHintFact",
    "MaterializationHintFacts",
    "validate_materialization_hint_facts",
]
