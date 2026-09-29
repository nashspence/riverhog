"""Subject-bound, bounded libmagic facts without filename or provenance claims."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
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

MAGIC_OBSERVATION_ID = "stove0.magic/v1"
MAX_PREFIX_BYTES = 1024 * 1024


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class MagicOptions(_Model):
    maximum_prefix_bytes: int = Field(default=262144, ge=1, le=MAX_PREFIX_BYTES)
    timeout_seconds: int = Field(default=10, ge=1, le=60)


class MagicArtifactFacts(_Model):
    subject_id: str = Field(min_length=1, max_length=160)
    mime_type: str = Field(min_length=1, max_length=255)
    description: str = Field(min_length=1, max_length=4096)
    sampled_bytes: int = Field(ge=0, le=MAX_PREFIX_BYTES)
    sample_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    complete_payload: bool
    file_version: str = Field(min_length=1, max_length=200)
    executable_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    database_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class MagicFacts(_Model):
    artifacts: tuple[MagicArtifactFacts, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def ordered_unique_subjects(self) -> Self:
        ids = [row.subject_id for row in self.artifacts]
        if ids != sorted(set(ids)):
            raise ValueError("magic facts must be unique and ordered by subject")
        return self


def validate_magic_facts(
    facts: Mapping[str, object],
    subjects: Sequence[WorkArtifactSubject],
    options: Mapping[str, object],
) -> MagicFacts:
    requested = MagicOptions.model_validate_json(canonical_json_bytes(dict(options)))
    document = MagicFacts.model_validate_json(canonical_json_bytes(dict(facts)))
    if tuple(row.subject_id for row in document.artifacts) != tuple(
        subject.id for subject in subjects
    ):
        raise ValueError("magic facts differ from the exact request subjects")
    for row, subject in zip(document.artifacts, subjects, strict=True):
        expected_bytes = min(int(subject.bytes), requested.maximum_prefix_bytes)
        if row.sampled_bytes != expected_bytes or row.complete_payload != (
            expected_bytes == int(subject.bytes)
        ):
            raise ValueError("magic sample coverage differs from the requested byte extent")
    return document


def _validate_observation(request: ContentObservationRequest, facts: Mapping[str, object]) -> None:
    validate_magic_facts(facts, request.subjects, request.options)


MAGIC_OPTIONS_SCHEMA = JsonSchemaValidationProfile.from_schema(
    "stove0.magic-options/v1", MagicOptions.model_json_schema()
)
MAGIC_FACTS_SCHEMA = JsonSchemaValidationProfile.from_schema(
    "stove0.magic-facts/v1", MagicFacts.model_json_schema()
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
_SAMPLE_FACT = {
    "subject_id": "sample",
    "mime_type": "application/octet-stream",
    "description": "data",
    "sampled_bytes": 1,
    "sample_sha256": "e" * 64,
    "complete_payload": True,
    "file_version": "file-5.45",
    "executable_sha256": "f" * 64,
    "database_sha256": "0" * 64,
}
MAGIC_CONFORMANCE_VECTORS = SemanticFactsConformanceVectors.model_validate(
    {
        "profile_id": "stove0.magic-facts-semantics/v1",
        "vectors": [
            {
                "id": "accepted-complete-sample",
                "accepted": True,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {"artifacts": [_SAMPLE_FACT]},
            },
            {
                "id": "rejected-false-coverage",
                "accepted": False,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {"artifacts": [{**_SAMPLE_FACT, "complete_payload": False}]},
            },
            {
                "id": "rejected-other-subject",
                "accepted": False,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {"artifacts": [{**_SAMPLE_FACT, "subject_id": "other"}]},
            },
        ],
    }
)
MAGIC_FACTS_SEMANTICS = SemanticValidationProfile.seal(
    SemanticValidationProfilePayload(
        id="stove0.magic-facts-semantics/v1",
        rules=(
            "stove0.magic.complete-bounded-sample/v1",
            "stove0.magic.exact-request-subjects/v1",
        ),
        conformance_vectors_sha256=MAGIC_CONFORMANCE_VECTORS.sha256,
    )
)
MAGIC_SEMANTIC_VALIDATOR = SemanticValidatorBinding.from_profile(
    MAGIC_FACTS_SEMANTICS, _validate_observation
)
MAGIC_OBSERVER_CONTRACT = ObserverContract.seal(
    ObserverContractPayload(
        id=MAGIC_OBSERVATION_ID,
        read_actions=("read-inputs",),
        options_schema=MAGIC_OPTIONS_SCHEMA,
        facts_schema=MAGIC_FACTS_SCHEMA,
        facts_semantics=MAGIC_FACTS_SEMANTICS,
        maximum_result_bytes=1024 * 1024,
    )
)


__all__ = [
    "MAGIC_CONFORMANCE_VECTORS",
    "MAGIC_FACTS_SEMANTICS",
    "MAGIC_OBSERVATION_ID",
    "MAGIC_OBSERVER_CONTRACT",
    "MAGIC_SEMANTIC_VALIDATOR",
    "MagicFacts",
    "MagicOptions",
    "validate_magic_facts",
]
