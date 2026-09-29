"""Strict reference models. None of these identifiers denotes a 'current file'."""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator

from .binding import CANONICAL_UUID_URN_PATTERN, SHA256_PATTERN, require_canonical_uuid_urn


def _identity(value: str) -> str:
    return require_canonical_uuid_urn(value)


type ProvenanceId = Annotated[
    str, Field(pattern=re.compile(CANONICAL_UUID_URN_PATTERN)), AfterValidator(_identity)
]
type ProvenanceJournalId = ProvenanceId
type ProvenanceStateId = ProvenanceId
type ProvenanceEntryId = ProvenanceId
type ProvenanceArtifactId = ProvenanceId
type ProvenanceOccurrenceId = ProvenanceId
type ProvenanceAssertionId = ProvenanceId
type Sha256 = Annotated[str, Field(pattern=re.compile(SHA256_PATTERN))]
type ObjectType = Literal[
    "artifact",
    "occurrence",
    "state",
    "agent",
    "context",
    "activity",
    "observation",
    "reported_description",
    "usage",
    "generation",
    "invalidation",
    "derivation",
    "specialization",
    "continuity",
    "content_comparison",
    "locator_binding",
    "locator_binding_end",
    "delivery_association",
    "custody_assertion",
    "availability_assertion",
    "journal_subject",
    "extension",
]


class StrictReference(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, regex_engine="python-re")


class EntryReference(StrictReference):
    entry_id: ProvenanceEntryId
    sequence: Annotated[str, Field(pattern=r"^(?:0|[1-9][0-9]*)(?![\s\S])")]
    json_sha256: Sha256

    @staticmethod
    def _bounded_sequence(value: str) -> str:
        if len(value) > 19 or int(value) > (1 << 63) - 1:
            raise ValueError("sequence exceeds the sequence63 domain")
        return value

    _check_sequence = field_validator("sequence")(_bounded_sequence)


class LocalReference(StrictReference):
    scope: Literal["local"] = "local"
    object_id: ProvenanceId
    object_type: ObjectType


class ExternalReference(StrictReference):
    scope: Literal["external"] = "external"
    journal_id: ProvenanceJournalId
    entry: EntryReference
    assertion_id: ProvenanceAssertionId
    object_id: ProvenanceId
    object_type: ObjectType


type EntityReference = Annotated[LocalReference | ExternalReference, Field(discriminator="scope")]


class ProvenanceStateReference(StrictReference):
    """An exact statement about one state in one immutable journal entry."""

    journal_id: ProvenanceJournalId
    entry: EntryReference
    assertion_id: ProvenanceAssertionId
    state_id: ProvenanceStateId

    def external(self) -> ExternalReference:
        return ExternalReference(
            journal_id=self.journal_id,
            entry=self.entry,
            assertion_id=self.assertion_id,
            object_id=self.state_id,
            object_type="state",
        )
