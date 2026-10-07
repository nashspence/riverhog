"""Evidence contract for lexical candidates over accepted canonical locator facts."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Literal, Self

from a_stove0_riverhog_provenance_evidence_contract_lib import (
    CORE_PROVENANCE_OBSERVER_CONTRACT,
    AssertionSupport,
)
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

FILENAME_PREFIX_SIDECARS_OBSERVATION_ID = "stove0.filename-prefix-sidecars/v1"
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_SUFFIX = re.compile(r"\.[A-Za-z0-9_-]{1,32}\Z")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _support_keys(rows: Sequence[AssertionSupport]) -> tuple[bytes, ...]:
    return tuple(canonical_json_bytes(row.model_dump(mode="json")) for row in rows)


class FilenameQuestion(_Model):
    provenance_slots: tuple[str, ...] = Field(min_length=1)
    primary_ids: tuple[str, ...]
    sidecar_ids: tuple[str, ...]
    sidecar_suffixes: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def canonical_question(self) -> Self:
        if (
            self.primary_ids != tuple(sorted(set(self.primary_ids)))
            or self.sidecar_ids != tuple(sorted(set(self.sidecar_ids)))
            or set(self.primary_ids) & set(self.sidecar_ids)
            or self.provenance_slots != tuple(sorted(set(self.provenance_slots)))
            or any(not slot or len(slot) > 160 for slot in self.provenance_slots)
            or self.sidecar_suffixes != tuple(sorted(set(self.sidecar_suffixes)))
            or any(_SUFFIX.fullmatch(suffix) is None for suffix in self.sidecar_suffixes)
            or any(
                other.endswith(suffix)
                for suffix in self.sidecar_suffixes
                for other in self.sidecar_suffixes
                if suffix != other
            )
        ):
            raise ValueError("filename partitions or sidecar suffix are invalid")
        return self


class FilenameSourceStatus(_Model):
    subject_id: str = Field(min_length=1)
    status: Literal["usable", "no-locator", "insufficient", "unsupported", "ambiguous"]
    support: tuple[AssertionSupport, ...]

    @model_validator(mode="after")
    def supported_status(self) -> Self:
        if (self.status == "no-locator") != (not self.support):
            raise ValueError("filename status and canonical locator support differ")
        keys = _support_keys(self.support)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("filename status support must be unique and ordered")
        return self


class FilenameCandidate(_Model):
    primary_id: str = Field(min_length=1)
    sidecar_id: str = Field(min_length=1)
    rule: Literal["full-leaf", "stem"]
    support: tuple[AssertionSupport, ...] = Field(min_length=1)


class FilenameProvenanceInput(_Model):
    accepted_input_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class FilenameFacts(_Model):
    provenance_inputs: tuple[FilenameProvenanceInput, ...] = Field(min_length=1)
    statuses: tuple[FilenameSourceStatus, ...]
    candidates: tuple[FilenameCandidate, ...]

    @model_validator(mode="after")
    def canonical_rows(self) -> Self:
        request_ids = tuple(item.accepted_input_sha256 for item in self.provenance_inputs)
        if request_ids != tuple(sorted(set(request_ids))):
            raise ValueError("filename predecessor identities must be unique and ordered")
        status_ids = tuple(item.subject_id for item in self.statuses)
        if status_ids != tuple(sorted(set(status_ids))):
            raise ValueError("filename status rows must be unique and ordered")
        keys = tuple((row.sidecar_id, row.primary_id, row.rule) for row in self.candidates)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("filename candidates must be unique and ordered")
        statuses = {row.subject_id: row for row in self.statuses}
        for candidate in self.candidates:
            primary = statuses.get(candidate.primary_id)
            sidecar = statuses.get(candidate.sidecar_id)
            if primary is None or sidecar is None:
                raise ValueError("filename candidate lacks subject status")
            expected = tuple(sorted(set(_support_keys((*primary.support, *sidecar.support)))))
            if _support_keys(candidate.support) != expected:
                raise ValueError("filename candidate support differs from both exact subjects")
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
            tuple(item.slot for item in slots) != question.provenance_slots
            or any(
                item.observer_contract_id != CORE_PROVENANCE_OBSERVER_CONTRACT.id for item in slots
            )
            or tuple(item.accepted_input_sha256 for item in slots)
            != tuple(item.accepted_input_sha256 for item in document.provenance_inputs)
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
    "provenance_slots": ["core"],
    "primary_ids": ["sample"],
    "sidecar_ids": ["sidecar"],
    "sidecar_suffixes": [".XMP", ".xmp"],
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
                    "provenance_inputs": [{"accepted_input_sha256": "f" * 64}],
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
                    "provenance_inputs": [{"accepted_input_sha256": "f" * 64}],
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
    "FilenameProvenanceInput",
    "FilenameQuestion",
    "FilenameSourceStatus",
    "validate_filename_facts",
]
