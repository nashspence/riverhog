"""Evidence contract for lexical candidates over accepted canonical locator facts."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Literal, Self

from a_stove0_riverhog_provenance_evidence_contract_lib import (
    CORE_PROVENANCE_OBSERVER_CONTRACT,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator
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

FILENAME_PREFIX_SIDECARS_OBSERVATION_ID = "stove0.filename-prefix-sidecars/v1"
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_SUFFIX = re.compile(r"\.[A-Za-z0-9_-]{1,32}\Z")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class FilenameQuestion(_Model):
    provenance_slot: str = Field(min_length=1, max_length=160)
    primary_ids: tuple[str, ...] = Field(min_length=1)
    sidecar_ids: tuple[str, ...] = Field(min_length=1)
    sidecar_suffix: str

    @model_validator(mode="after")
    def canonical_question(self) -> Self:
        if (
            self.primary_ids != tuple(sorted(set(self.primary_ids)))
            or self.sidecar_ids != tuple(sorted(set(self.sidecar_ids)))
            or set(self.primary_ids) & set(self.sidecar_ids)
            or _SUFFIX.fullmatch(self.sidecar_suffix) is None
        ):
            raise ValueError("filename partitions or sidecar suffix are invalid")
        return self


class FilenameSourceStatus(_Model):
    subject_id: str = Field(min_length=1)
    status: Literal["usable", "no-locator", "insufficient", "unsupported", "ambiguous"]
    support: tuple[dict[str, JsonValue], ...]


class FilenameCandidate(_Model):
    primary_id: str = Field(min_length=1)
    sidecar_id: str = Field(min_length=1)
    rule: Literal["full-leaf", "stem"]
    support: tuple[dict[str, JsonValue], ...] = Field(min_length=1)


class FilenameFacts(_Model):
    provenance_request_id: str
    provenance_result_sha256: str
    statuses: tuple[FilenameSourceStatus, ...]
    candidates: tuple[FilenameCandidate, ...]

    @model_validator(mode="after")
    def canonical_rows(self) -> Self:
        if (
            _DIGEST.fullmatch(self.provenance_request_id) is None
            or _DIGEST.fullmatch(self.provenance_result_sha256) is None
        ):
            raise ValueError("filename evidence requires exact predecessor identities")
        status_ids = tuple(item.subject_id for item in self.statuses)
        if status_ids != tuple(sorted(set(status_ids))):
            raise ValueError("filename status rows must be unique and ordered")
        keys = tuple((row.sidecar_id, row.primary_id, row.rule) for row in self.candidates)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("filename candidates must be unique and ordered")
        return self


def validate_filename_facts(
    facts: Mapping[str, object],
    subjects: Sequence[WorkArtifactSubject],
    options: Mapping[str, object],
    *,
    request: ContentObservationRequest | None = None,
) -> FilenameFacts:
    question = FilenameQuestion.model_validate_json(canonical_json_bytes(dict(options)))
    ids = tuple(subject.id for subject in subjects)
    if tuple(sorted((*question.primary_ids, *question.sidecar_ids))) != ids:
        raise ValueError("filename question does not partition the exact selected subjects")
    document = FilenameFacts.model_validate_json(canonical_json_bytes(dict(facts)))
    if tuple(row.subject_id for row in document.statuses) != ids:
        raise ValueError("filename status rows do not cover the exact selected subjects")
    statuses = {row.subject_id: row.status for row in document.statuses}
    for row in document.candidates:
        if (
            row.primary_id not in question.primary_ids
            or row.sidecar_id not in question.sidecar_ids
            or statuses[row.primary_id] not in {"usable", "insufficient"}
            or statuses[row.sidecar_id] not in {"usable", "insufficient"}
        ):
            raise ValueError("filename candidate is outside the exact usable partitions")
    if request is not None:
        slots = tuple(request.evidence_slots or ())
        if (
            len(slots) != 1
            or slots[0].slot != question.provenance_slot
            or slots[0].observer_contract_id != CORE_PROVENANCE_OBSERVER_CONTRACT.id
            or slots[0].request_id != document.provenance_request_id
            or slots[0].result_sha256 != document.provenance_result_sha256
        ):
            raise ValueError("filename facts differ from the accepted provenance slot")
    return document


def _validate_observation(request: ContentObservationRequest, facts: Mapping[str, object]) -> None:
    validate_filename_facts(facts, request.subjects, request.options, request=request)


FILENAME_OPTIONS_SCHEMA = JsonSchemaValidationProfile.from_schema(
    "stove0.filename-prefix-sidecars-options/v1", FilenameQuestion.model_json_schema()
)
FILENAME_FACTS_SCHEMA = JsonSchemaValidationProfile.from_schema(
    "stove0.filename-prefix-sidecars-facts/v1", FilenameFacts.model_json_schema()
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
_SAMPLE_OPTIONS = {
    "provenance_slot": "core",
    "primary_ids": ["sample"],
    "sidecar_ids": ["sidecar"],
    "sidecar_suffix": ".xmp",
}
_SAMPLE_SIDECAR = {**_SAMPLE_SUBJECT, "id": "sidecar", "artifact_id": "e" * 64}
FILENAME_CONFORMANCE_VECTORS = SemanticFactsConformanceVectors.model_validate(
    {
        "profile_id": "stove0.filename-prefix-sidecars-facts-semantics/v1",
        "vectors": [
            {
                "id": "accepted-complete-no-locator",
                "accepted": True,
                "subjects": [_SAMPLE_SUBJECT, _SAMPLE_SIDECAR],
                "options": _SAMPLE_OPTIONS,
                "facts": {
                    "provenance_request_id": "f" * 64,
                    "provenance_result_sha256": "0" * 64,
                    "statuses": [
                        {"subject_id": "sample", "status": "no-locator", "support": []},
                        {"subject_id": "sidecar", "status": "no-locator", "support": []},
                    ],
                    "candidates": [],
                },
            },
            {
                "id": "rejected-incomplete-status",
                "accepted": False,
                "subjects": [_SAMPLE_SUBJECT, _SAMPLE_SIDECAR],
                "options": _SAMPLE_OPTIONS,
                "facts": {
                    "provenance_request_id": "f" * 64,
                    "provenance_result_sha256": "0" * 64,
                    "statuses": [{"subject_id": "sample", "status": "no-locator", "support": []}],
                    "candidates": [],
                },
            },
        ],
    }
)
FILENAME_FACTS_SEMANTICS = SemanticValidationProfile.seal(
    SemanticValidationProfilePayload(
        id="stove0.filename-prefix-sidecars-facts-semantics/v1",
        rules=(
            "stove0.filename-prefix-sidecars.accepted-core-locator-dependency/v1",
            "stove0.filename-prefix-sidecars.complete-status-and-candidate-set/v1",
            "stove0.filename-prefix-sidecars.exact-selected-partitions/v1",
        ),
        conformance_vectors_sha256=FILENAME_CONFORMANCE_VECTORS.sha256,
    )
)
FILENAME_SEMANTIC_VALIDATOR = SemanticValidatorBinding.from_profile(
    FILENAME_FACTS_SEMANTICS, _validate_observation
)
FILENAME_OBSERVER_CONTRACT = ObserverContract.seal(
    ObserverContractPayload(
        id=FILENAME_PREFIX_SIDECARS_OBSERVATION_ID,
        read_actions=("read-evidence",),
        options_schema=FILENAME_OPTIONS_SCHEMA,
        facts_schema=FILENAME_FACTS_SCHEMA,
        facts_semantics=FILENAME_FACTS_SEMANTICS,
        maximum_result_bytes=64 * 1024 * 1024,
    )
)


__all__ = [
    "FILENAME_CONFORMANCE_VECTORS",
    "FILENAME_FACTS_SCHEMA",
    "FILENAME_FACTS_SEMANTICS",
    "FILENAME_OBSERVER_CONTRACT",
    "FILENAME_OPTIONS_SCHEMA",
    "FILENAME_PREFIX_SIDECARS_OBSERVATION_ID",
    "FILENAME_SEMANTIC_VALIDATOR",
    "FilenameCandidate",
    "FilenameFacts",
    "FilenameQuestion",
    "FilenameSourceStatus",
    "validate_filename_facts",
]
