"""Accepted pre-root completion requirements for collection-producing executions."""

from __future__ import annotations

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance_contracts import ProvenanceJournalId
from time_formats import CanonicalUtcTimestamp

from .collection_workflows import canonical_json_sha256
from .provenance_transport import JournalAnchorDocument

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]

COMPLETION_REQUIRED_RECORD_KINDS = (
    "accepted-construction",
    "controller",
    "disposition-identity",
    "disposition-pages",
    "implementation",
    "input-history-bindings",
    "invocation",
    "output-bindings",
    "target-execution",
    "target-output-declarations",
    "target-result",
)


class CollectionCompletionRequirementDocument(BaseModel):
    """Declare future record kinds without inventing their not-yet-known hashes."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    execution_id: Sha256
    execution_envelope_sha256: Sha256
    controller_evidence_sha256: Sha256
    record_kinds: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_declaration(self) -> Self:
        if tuple(sorted(set(self.record_kinds))) != self.record_kinds:
            raise ValueError("completion record kinds must be unique and sorted")
        if any(not name or name != name.strip() for name in self.record_kinds):
            raise ValueError("completion record kind is not canonical")
        if not set(COMPLETION_REQUIRED_RECORD_KINDS).issubset(self.record_kinds):
            raise ValueError("execution completion omits a required record kind")
        if len(canonical_json_bytes(self.model_dump(mode="json"))) > 64 * 1024:
            raise ValueError("completion declaration exceeds its bounded control document")
        return self

    @property
    def identity(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))


class CollectionCompletionRecordingRequestDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    requirement_sha256: Sha256
    records_sha256: Sha256


class CollectionCompletionRecordingDocument(CollectionCompletionRecordingRequestDocument):
    journal_id: ProvenanceJournalId
    recorded_at: CanonicalUtcTimestamp


class CollectionCompletionRecordIdentityDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    sha256: Sha256
    bytes: Annotated[str, Field(pattern=r"^(0|[1-9][0-9]*)$")]


class CollectionCompletionPublicationReceiptDocument(BaseModel):
    """Root-bearing protocol responses retain verified completion retry identities.

    These hashes are a catalog projection of the accepted canonical completion,
    without granting journal-read authority or becoming archived semantic claims.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
    requirement_sha256: Sha256
    records_sha256: Sha256
    journal: JournalAnchorDocument
    records: dict[str, CollectionCompletionRecordIdentityDocument]

    @model_validator(mode="after")
    def validate_identities(self) -> Self:
        from .collection_record_preimages import completion_record_inventory_sha256

        if not set(COMPLETION_REQUIRED_RECORD_KINDS).issubset(self.records):
            raise ValueError("completion receipt omits required record identities")
        if any(not kind or kind != kind.strip() for kind in self.records):
            raise ValueError("completion receipt record kind is not canonical")
        if len(canonical_json_bytes(self.model_dump(mode="json"))) > 64 * 1024:
            raise ValueError("completion receipt exceeds its bounded control document")
        if (
            completion_record_inventory_sha256(
                (kind, record.sha256, int(record.bytes))
                for kind, record in sorted(self.records.items())
            )
            != self.records_sha256
        ):
            raise ValueError("completion receipt differs from its accepted inventory")
        return self


__all__ = ["COMPLETION_REQUIRED_RECORD_KINDS", "CollectionCompletionRequirementDocument"]
