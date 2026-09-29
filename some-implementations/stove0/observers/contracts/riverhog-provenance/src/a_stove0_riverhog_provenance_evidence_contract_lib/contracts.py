"""Exact subject-bound canonical facts, with no platform or predicate interpretation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator
from riverhog_protocol import (
    CollectionArtifactProvenanceBindingDocument,
    JournalAnchorDocument,
)
from riverhog_provenance_contracts import (
    PROFILE,
    ContractCatalog,
    EntryReference,
    ExternalReference,
    require_canonical_uuid_urn,
)
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

CORE_PROVENANCE_OBSERVATION_ID = "stove0.riverhog-provenance/v1"
_HINT_SCHEMA = PROFILE + "/materialization-hint.schema.json"


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class CoreProvenanceOptions(_Model):
    predicates: tuple[str, ...] = Field(default=(), max_length=64)

    @field_validator("predicates")
    @classmethod
    def canonical_predicates(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))) or any(
            not item or len(item) > 2048 for item in value
        ):
            raise ValueError("requested predicate list must be unique, ordered and bounded")
        return value


class CanonicalEndpoint(_Model):
    journal_id: str
    entry: EntryReference
    assertion_id: str
    object_id: str
    object_type: str

    @model_validator(mode="after")
    def exact_reference_shape(self) -> Self:
        ExternalReference.model_validate({"scope": "external", **self.model_dump(mode="json")})
        return self


class AssertionSupport(_Model):
    journal: JournalAnchorDocument
    entry: EntryReference
    assertion_id: str
    referent_id: str
    pointer: str

    @model_validator(mode="after")
    def within_anchor(self) -> Self:
        require_canonical_uuid_urn(self.assertion_id, "assertion")
        require_canonical_uuid_urn(self.referent_id, "referent")
        if int(self.entry.sequence) > int(self.journal.through.sequence):
            raise ValueError("assertion support is outside its exact journal anchor")
        return self


class CoreLocatorFact(_Model):
    subject_id: str
    locator: dict[str, JsonValue]
    context_endpoint: CanonicalEndpoint
    context_identifiers: tuple[dict[str, JsonValue], ...]
    context_support: AssertionSupport
    locator_support: AssertionSupport
    temporal_scope: dict[str, JsonValue]
    observation_endpoint: CanonicalEndpoint

    @model_validator(mode="after")
    def typed_support(self) -> Self:
        if (
            self.context_endpoint.object_type != "context"
            or self.observation_endpoint.object_type != "observation"
            or self.context_support.referent_id != self.context_endpoint.object_id
            or self.locator_support.journal.journal_id != self.context_endpoint.journal_id
        ):
            raise ValueError("locator has inconsistent canonical context support")
        return self


class CoreClaimFact(_Model):
    predicate: str = Field(min_length=1, max_length=2048)
    subject: CanonicalEndpoint
    value_type: str = Field(min_length=1)
    object: CanonicalEndpoint | None
    value: dict[str, JsonValue] | None
    evidence: tuple[dict[str, JsonValue], ...]
    support: AssertionSupport

    @model_validator(mode="after")
    def preserve_shape(self) -> Self:
        if self.value_type == "reference":
            if self.object is None or self.value is not None:
                raise ValueError("reference claim must retain its exact object")
        elif self.object is not None or self.value is None:
            raise ValueError("non-reference claim must retain its original typed value")
        return self


class CoreProvenanceFact(_Model):
    subject_id: str = Field(min_length=1)
    primary_binding: CollectionArtifactProvenanceBindingDocument
    state: CanonicalEndpoint
    occurrence: CanonicalEndpoint
    locators: tuple[CoreLocatorFact, ...]
    claims: tuple[CoreClaimFact, ...]
    materialization_hint: dict[str, JsonValue] | None

    @model_validator(mode="after")
    def primary_snapshot(self) -> Self:
        if self.state.object_type != "state" or self.occurrence.object_type != "occurrence":
            raise ValueError("primary facts require a State and an Occurrence")
        anchor = self.primary_binding.journal
        for endpoint in (self.state, self.occurrence):
            if endpoint.journal_id != anchor.journal_id or int(endpoint.entry.sequence) > int(
                anchor.through.sequence
            ):
                raise ValueError("selected canonical fact is outside its primary anchor")
        if any(item.subject_id != self.subject_id for item in self.locators):
            raise ValueError("locator fact differs from the exact subject")
        if any(item.subject not in (self.state, self.occurrence) for item in self.claims):
            raise ValueError("claim differs from the delivered State or Occurrence")
        return self

    @field_validator("materialization_hint")
    @classmethod
    def canonical_hint(cls, value: dict[str, JsonValue] | None) -> dict[str, JsonValue] | None:
        if value is not None:
            ContractCatalog().validate(_HINT_SCHEMA, value)
        return value


class CoreProvenanceFacts(_Model):
    artifacts: tuple[CoreProvenanceFact, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def ordered_unique(self) -> Self:
        identities = tuple(item.subject_id for item in self.artifacts)
        if identities != tuple(sorted(set(identities))):
            raise ValueError("core provenance facts must be unique and ordered by subject")
        return self


def validate_core_provenance_facts(
    facts: Mapping[str, object],
    subjects: Sequence[WorkArtifactSubject],
    options: Mapping[str, object],
) -> CoreProvenanceFacts:
    requested = CoreProvenanceOptions.model_validate_json(canonical_json_bytes(dict(options)))
    document = CoreProvenanceFacts.model_validate_json(canonical_json_bytes(dict(facts)))
    if tuple(item.subject_id for item in document.artifacts) != tuple(item.id for item in subjects):
        raise ValueError("core provenance facts differ from the exact request subjects")
    for fact, subject in zip(document.artifacts, subjects, strict=True):
        if fact.primary_binding.artifact_id != subject.artifact_id:
            raise ValueError("core provenance binding differs from the selected member")
        if any(claim.predicate not in requested.predicates for claim in fact.claims):
            raise ValueError("core provenance returned an unrequested predicate")
    return document


def _validate_observation(request: ContentObservationRequest, facts: Mapping[str, object]) -> None:
    validate_core_provenance_facts(facts, request.subjects, request.options)


CORE_PROVENANCE_OPTIONS_SCHEMA = JsonSchemaValidationProfile.from_schema(
    "stove0.riverhog-provenance-options/v1", CoreProvenanceOptions.model_json_schema()
)
CORE_PROVENANCE_FACTS_SCHEMA = JsonSchemaValidationProfile.from_schema(
    "stove0.riverhog-provenance-facts/v1", CoreProvenanceFacts.model_json_schema()
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
_ENTRY = {
    "entry_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
    "sequence": "0",
    "json_sha256": "e" * 64,
}
_JOURNAL_ID = "urn:uuid:22222222-2222-4222-8222-222222222222"
_STATE = {
    "journal_id": _JOURNAL_ID,
    "entry": _ENTRY,
    "assertion_id": "urn:uuid:33333333-3333-4333-8333-333333333333",
    "object_id": "urn:uuid:44444444-4444-4444-8444-444444444444",
    "object_type": "state",
}
_OCCURRENCE = {
    **_STATE,
    "assertion_id": "urn:uuid:55555555-5555-4555-8555-555555555555",
    "object_id": "urn:uuid:66666666-6666-4666-8666-666666666666",
    "object_type": "occurrence",
}
_BINDING = {
    "artifact_id": "c" * 64,
    "journal": {
        "journal_id": _JOURNAL_ID,
        "through": _ENTRY,
        "prefix_sha256": "f" * 64,
        "prefix_bytes": "100",
    },
    "delivery_association_id": "urn:uuid:77777777-7777-4777-8777-777777777777",
}
_SUPPORT = {
    "journal": _BINDING["journal"],
    "entry": _ENTRY,
    "assertion_id": "urn:uuid:88888888-8888-4888-8888-888888888888",
    "referent_id": "urn:uuid:99999999-9999-4999-8999-999999999999",
    "pointer": "",
}
_SAMPLE_FACT: dict[str, object] = {
    "subject_id": "sample",
    "primary_binding": _BINDING,
    "state": _STATE,
    "occurrence": _OCCURRENCE,
    "locators": [],
    "claims": [],
    "materialization_hint": None,
}
CORE_PROVENANCE_CONFORMANCE_VECTORS = SemanticFactsConformanceVectors.model_validate(
    {
        "profile_id": "stove0.riverhog-provenance-facts-semantics/v1",
        "vectors": [
            {
                "id": "accepted-exact-subject",
                "accepted": True,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {"artifacts": [_SAMPLE_FACT]},
            },
            {
                "id": "rejected-other-member-binding",
                "accepted": False,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {
                    "artifacts": [
                        {
                            **_SAMPLE_FACT,
                            "primary_binding": {**_BINDING, "artifact_id": "0" * 64},
                        }
                    ]
                },
            },
            {
                "id": "rejected-unrequested-predicate",
                "accepted": False,
                "subjects": [_SAMPLE_SUBJECT],
                "facts": {
                    "artifacts": [
                        {
                            **_SAMPLE_FACT,
                            "claims": [
                                {
                                    "predicate": "https://example.org/unknown",
                                    "subject": _STATE,
                                    "value_type": "json",
                                    "object": None,
                                    "value": {"type": "json", "value": {"label": "x"}},
                                    "evidence": [],
                                    "support": _SUPPORT,
                                }
                            ],
                        }
                    ]
                },
            },
        ],
    }
)
CORE_PROVENANCE_FACTS_SEMANTICS = SemanticValidationProfile.seal(
    SemanticValidationProfilePayload(
        id="stove0.riverhog-provenance-facts-semantics/v1",
        rules=(
            "stove0.riverhog-provenance.exact-primary-binding/v1",
            "stove0.riverhog-provenance.requested-predicate-scope/v1",
        ),
        conformance_vectors_sha256=CORE_PROVENANCE_CONFORMANCE_VECTORS.sha256,
    )
)
CORE_PROVENANCE_SEMANTIC_VALIDATOR = SemanticValidatorBinding.from_profile(
    CORE_PROVENANCE_FACTS_SEMANTICS, _validate_observation
)
CORE_PROVENANCE_OBSERVER_CONTRACT = ObserverContract.seal(
    ObserverContractPayload(
        id=CORE_PROVENANCE_OBSERVATION_ID,
        read_actions=("read-provenance",),
        options_schema=CORE_PROVENANCE_OPTIONS_SCHEMA,
        facts_schema=CORE_PROVENANCE_FACTS_SCHEMA,
        facts_semantics=CORE_PROVENANCE_FACTS_SEMANTICS,
        maximum_result_bytes=1024 * 1024,
    )
)


__all__ = [
    "CORE_PROVENANCE_CONFORMANCE_VECTORS",
    "CORE_PROVENANCE_OBSERVER_CONTRACT",
    "CORE_PROVENANCE_SEMANTIC_VALIDATOR",
    "CORE_PROVENANCE_OBSERVATION_ID",
    "CoreProvenanceOptions",
    "validate_core_provenance_facts",
]
