"""Logical questions and paged controller acceptance retain original testimony."""

from __future__ import annotations

import hashlib
from typing import Any, Literal, Self

from pydantic import Field, JsonValue, StrictBool, model_validator
from riverhog_protocol.exact_scalar import NonnegativeDecimal

from stove0_protocol.jcs import CommitmentDigest, canonical_json_bytes, canonical_json_sha256
from stove0_protocol.models import Sha256, Stove0ProtocolModel
from stove0_protocol.observation_interfaces import ExactDocumentRef
from stove0_protocol.predicates import LocalName
from stove0_protocol.selection_refs import ArtifactSelectionRef

OBSERVATION_EVIDENCE_PAGE_MAX = 100
_PAGE_EXTENT: dict[str, Any] = {
    "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "reason": "bounded-observation-evidence-page",
        "progression": "authority-bound-ordinal",
    }
}


class AcceptedTaskInput(Stove0ProtocolModel):
    task_id: LocalName
    question_sha256: Sha256
    evidence_set_sha256: Sha256
    interface: ExactDocumentRef
    scope: ArtifactSelectionRef


class ObservationQuestionPayload(Stove0ProtocolModel):
    format: Literal["stove0-observation-question/v1"] = "stove0-observation-question/v1"
    work_id: Sha256
    task_id: LocalName
    observer_contract: ExactDocumentRef
    interface: ExactDocumentRef
    scope: ArtifactSelectionRef
    subject_ports: dict[LocalName, ArtifactSelectionRef] = Field(min_length=1)
    evidence_ports: dict[LocalName, AcceptedTaskInput] = Field(default_factory=dict)
    options: dict[str, JsonValue] = Field(default_factory=dict)
    retrieve: Literal["available-only", "allow"] = "available-only"
    read_actions: tuple[Literal["read-inputs", "read-provenance", "read-evidence"], ...] = Field(
        min_length=1,
        max_length=1,
    )


class ObservationQuestion(ObservationQuestionPayload):
    question_sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        document = self.model_dump(mode="json", by_alias=True, exclude={"question_sha256"})
        if canonical_json_sha256(document) != self.question_sha256:
            raise ValueError("logical question differs from its exact task, scope and contracts")
        return self

    @classmethod
    def seal(cls, payload: ObservationQuestionPayload) -> Self:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate({**document, "question_sha256": canonical_json_sha256(document)})


class EvidenceResultRef(Stove0ProtocolModel):
    request_id: Sha256
    result_sha256: Sha256
    observer_descriptor_sha256: Sha256
    scope: ArtifactSelectionRef


class EvidenceResultSetRef(Stove0ProtocolModel):
    result_count: NonnegativeDecimal
    results_sha256: Sha256


def update_evidence_result_commitment(
    digest: CommitmentDigest, *, ordinal: int, result: EvidenceResultRef
) -> None:
    _update_record(
        digest, domain=b"stove0-observation-results/v1\x00", ordinal=ordinal, document=result
    )


def _update_record(
    digest: CommitmentDigest, *, domain: bytes, ordinal: int, document: Stove0ProtocolModel
) -> None:
    if type(ordinal) is not int or ordinal < 0:
        raise ValueError("evidence ordinal must be nonnegative")
    encoded = canonical_json_bytes(document.model_dump(mode="json", by_alias=True))
    digest.update(domain)
    digest.update(str(ordinal).encode("ascii"))
    digest.update(b"\x00")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)


class AcceptedEvidenceSetPayload(Stove0ProtocolModel):
    format: Literal["stove0-accepted-evidence-set/v1"] = "stove0-accepted-evidence-set/v1"
    question: ObservationQuestion
    state: Literal["complete", "complete-empty"]
    results: EvidenceResultSetRef

    @model_validator(mode="after")
    def complete_empty(self) -> Self:
        empty = self.question.scope.artifact_count == 0
        if empty != (self.state == "complete-empty") or empty != (self.results.result_count == 0):
            raise ValueError(
                "accepted task completion differs from its exact empty/nonempty domain"
            )
        if empty and self.results.results_sha256 != hashlib.sha256().hexdigest():
            raise ValueError("empty accepted task has a nonempty result-set commitment")
        return self


class AcceptedEvidenceSet(AcceptedEvidenceSetPayload):
    evidence_set_sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        document = self.model_dump(mode="json", by_alias=True, exclude={"evidence_set_sha256"})
        if canonical_json_sha256(document) != self.evidence_set_sha256:
            raise ValueError(
                "accepted evidence set differs from its exact complete question/results"
            )
        return self

    @classmethod
    def seal(cls, payload: AcceptedEvidenceSetPayload) -> Self:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate(
            {**document, "evidence_set_sha256": canonical_json_sha256(document)}
        )


class AcceptedEvidencePage(Stove0ProtocolModel):
    authority: AcceptedEvidenceSet
    start_ordinal: NonnegativeDecimal
    results: tuple[EvidenceResultRef, ...] = Field(
        max_length=OBSERVATION_EVIDENCE_PAGE_MAX, json_schema_extra=_PAGE_EXTENT
    )
    complete: StrictBool

    @model_validator(mode="after")
    def continuation(self) -> Self:
        end = self.start_ordinal + len(self.results)
        if end > self.authority.results.result_count or self.complete != (
            end == self.authority.results.result_count
        ):
            raise ValueError("accepted evidence page differs from its exact result authority")
        if not self.complete and not self.results:
            raise ValueError("incomplete accepted evidence page makes no progress")
        return self


class AcceptedViewRecord(Stove0ProtocolModel):
    support: tuple[EvidenceResultRef, ...] = Field(min_length=1)
    kind: Literal["subject", "global", "relation", "coverage"]
    subject_id: str | None = None
    value: JsonValue

    @model_validator(mode="after")
    def subject_kind(self) -> Self:
        keys = [reference.request_id for reference in self.support]
        if keys != sorted(set(keys)):
            raise ValueError("accepted view support must be a canonical exact result set")
        if (self.subject_id is not None) != (self.kind in {"subject", "coverage"}):
            raise ValueError(
                "accepted view record subject identity differs from its structural kind"
            )
        if self.kind == "coverage" and (
            not isinstance(self.value, str)
            or self.value
            not in {
                "complete",
                "unsupported",
                "ambiguous",
                "insufficient",
            }
        ):
            raise ValueError("accepted view coverage status is undeclared")
        return self


def update_accepted_view_commitment(
    digest: CommitmentDigest, *, ordinal: int, record: AcceptedViewRecord
) -> None:
    _update_record(
        digest, domain=b"stove0-accepted-view-records/v1\x00", ordinal=ordinal, document=record
    )


class AcceptedViewPayload(Stove0ProtocolModel):
    format: Literal["stove0-accepted-view/v1"] = "stove0-accepted-view/v1"
    work_id: Sha256
    task_id: LocalName
    question_sha256: Sha256
    evidence_set_sha256: Sha256
    interface: ExactDocumentRef
    view_id: LocalName
    selected_scope: ArtifactSelectionRef
    view_semantics: ExactDocumentRef
    record_count: NonnegativeDecimal
    records_sha256: Sha256


class AcceptedView(AcceptedViewPayload):
    view_sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        document = self.model_dump(mode="json", by_alias=True, exclude={"view_sha256"})
        if canonical_json_sha256(document) != self.view_sha256:
            raise ValueError(
                "accepted view differs from its exact source evidence and selected scope"
            )
        return self

    @classmethod
    def seal(cls, payload: AcceptedViewPayload) -> Self:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate({**document, "view_sha256": canonical_json_sha256(document)})


class AcceptedViewPage(Stove0ProtocolModel):
    authority: AcceptedView
    start_ordinal: NonnegativeDecimal
    records: tuple[AcceptedViewRecord, ...] = Field(
        max_length=OBSERVATION_EVIDENCE_PAGE_MAX, json_schema_extra=_PAGE_EXTENT
    )
    complete: StrictBool

    @model_validator(mode="after")
    def continuation(self) -> Self:
        end = self.start_ordinal + len(self.records)
        if end > self.authority.record_count or self.complete != (
            end == self.authority.record_count
        ):
            raise ValueError("accepted view page differs from its exact selected-view authority")
        if not self.complete and not self.records:
            raise ValueError("incomplete accepted view page makes no progress")
        return self
