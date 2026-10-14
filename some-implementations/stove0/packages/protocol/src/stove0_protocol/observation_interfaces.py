"""Exact structural interfaces over observer-owned questions and accepted facts."""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, StrictBool, field_validator, model_validator

from stove0_protocol.interface_schemas import schema_slice
from stove0_protocol.jcs import canonical_json_sha256
from stove0_protocol.models import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    ObserverContract,
    SemanticId,
    SemanticValidationProfile,
    SemanticValidationProfilePayload,
    Sha256,
    Stove0ProtocolModel,
    WorkArtifactSubject,
)
from stove0_protocol.models import (
    ExactDocumentRef as ExactDocumentRef,
)
from stove0_protocol.predicates import LocalName, Pointer, RowPredicate, pointer_parts


class SubjectPort(Stove0ProtocolModel):
    kind: Literal["subjects"] = "subjects"
    option_ids_at: Pointer | None = None


class EvidencePort(Stove0ProtocolModel):
    kind: Literal["evidence"] = "evidence"
    contracts: tuple[ExactDocumentRef, ...] = Field(min_length=1)
    interfaces: tuple[ExactDocumentRef, ...] = Field(min_length=1)
    covers: tuple[LocalName, ...] = Field(min_length=1)
    option_slots_at: Pointer | None = None

    @model_validator(mode="after")
    def canonical_sets(self) -> Self:
        for values in (self.contracts, self.interfaces):
            keys = [(item.id, item.sha256) for item in values]
            if keys != sorted(set(keys)):
                raise ValueError("evidence port exact references must be canonical sets")
        if self.covers != tuple(sorted(set(self.covers))):
            raise ValueError("evidence coverage ports must be canonical and distinct")
        return self


type ObservationInputPort = Annotated[SubjectPort | EvidencePort, Field(discriminator="kind")]


class RecordSource(Stove0ProtocolModel):
    records_at: Pointer
    nested_records_at: Pointer | None = None


class RecordStatus(Stove0ProtocolModel):
    kind: Literal["records"] = "records"
    records_at: Pointer
    subject_at: Pointer
    value_at: Pointer
    values: dict[str, Literal["complete", "unsupported", "ambiguous", "insufficient"]] = Field(
        min_length=1
    )


class SemanticStatus(Stove0ProtocolModel):
    kind: Literal["semantic-profile"] = "semantic-profile"
    profile: ExactDocumentRef


type StatusSource = Annotated[RecordStatus | SemanticStatus, Field(discriminator="kind")]


class SubjectFactsView(Stove0ProtocolModel):
    kind: Literal["subject-facts"] = "subject-facts"
    records: RecordSource
    subject_at: Pointer
    record_schema_at: Pointer
    coverage: Literal["every-question-subject"] = "every-question-subject"
    cardinality: Literal["one-per-subject", "many-per-subject"] = "one-per-subject"
    status: StatusSource | None = None

    @model_validator(mode="after")
    def complete_many(self) -> Self:
        if self.cardinality == "many-per-subject" and self.status is None:
            raise ValueError("many-per-subject views need independent subject coverage proof")
        return self


class GlobalFactsView(Stove0ProtocolModel):
    kind: Literal["global-facts"] = "global-facts"
    record_at: Pointer
    record_schema_at: Pointer
    coverage: Literal["whole-question"] = "whole-question"


class SubjectEndpoint(Stove0ProtocolModel):
    kind: Literal["subject-id"] = "subject-id"
    at: Pointer


class EndpointInput(Stove0ProtocolModel):
    input: LocalName


class EndpointLookup(Stove0ProtocolModel):
    source: Literal["self"] | EndpointInput
    view: LocalName
    keys: tuple[Pointer, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def canonical_keys(self) -> Self:
        if self.keys != tuple(sorted(set(self.keys))):
            raise ValueError("endpoint lookup keys must be canonical and distinct")
        return self


class ExactEndpoint(Stove0ProtocolModel):
    kind: Literal["exact-endpoint"] = "exact-endpoint"
    at: Pointer
    lookup: EndpointLookup


type RelationEndpoint = Annotated[SubjectEndpoint | ExactEndpoint, Field(discriminator="kind")]


class RelationView(Stove0ProtocolModel):
    kind: Literal["relation"] = "relation"
    records: RecordSource
    where: RowPredicate
    require: RowPredicate
    primary: RelationEndpoint
    associated: RelationEndpoint
    coverage: tuple[LocalName, ...] = Field(min_length=1)
    status: StatusSource
    cardinality: Literal["at-most-one-primary-per-associated"] = (
        "at-most-one-primary-per-associated"
    )

    @model_validator(mode="after")
    def canonical_coverage(self) -> Self:
        if self.coverage != tuple(sorted(set(self.coverage))):
            raise ValueError("relation coverage must be canonical and distinct")
        return self


type ObservationView = Annotated[
    SubjectFactsView | GlobalFactsView | RelationView, Field(discriminator="kind")
]


# The profile commits the structural algebra's executable conformance inputs.
# Domain meaning and its vectors remain separately pinned by each observer.
INTERFACE_STRUCTURE_VECTORS = {
    "format": "stove0-observation-interface-structure-vectors/v1",
    "coverage": ["duplicate-subject-rejected", "missing-subject-rejected", "complete-empty"],
    "pointers": [{"pointer": "/a~1b/~0", "tokens": ["a/b", "~"]}],
    "scope": ["original-result-retained", "subset-view-distinct", "full-result-read-scoped"],
}
OBSERVATION_INTERFACE_SEMANTICS = SemanticValidationProfile.seal(
    SemanticValidationProfilePayload(
        id="stove0.observation-interface-semantics/v1",
        rules=(
            "stove0.observation-interface.coverage/v1",
            "stove0.observation-interface.ports/v1",
            "stove0.observation-interface.views/v1",
        ),
        conformance_vectors_sha256=canonical_json_sha256(INTERFACE_STRUCTURE_VECTORS),
    )
)


class ObservationInterfacePayload(Stove0ProtocolModel):
    format: Literal["stove0-observation-interface/v1"] = "stove0-observation-interface/v1"
    id: SemanticId
    observer_contract: ExactDocumentRef
    facts_profile: ExactDocumentRef
    semantic_profile: ExactDocumentRef
    inputs: dict[LocalName, ObservationInputPort] = Field(min_length=1)
    views: dict[LocalName, ObservationView] = Field(min_length=1)
    partitioning: Literal["independent-subjects", "whole-scope"]
    empty_scope: Literal["complete-empty", "inapplicable"]
    interface_semantics: ExactDocumentRef
    conformance_vectors_sha256: Sha256

    @model_validator(mode="after")
    def structure(self) -> Self:
        expected = ExactDocumentRef(
            id=OBSERVATION_INTERFACE_SEMANTICS.id,
            sha256=OBSERVATION_INTERFACE_SEMANTICS.profile_sha256,
        )
        if self.interface_semantics != expected:
            raise ValueError("unsupported observation interface semantics")
        subjects = {name for name, port in self.inputs.items() if isinstance(port, SubjectPort)}
        if not subjects:
            raise ValueError("observation interface requires a declared subject port")
        generated: list[tuple[str, ...]] = []
        for port in self.inputs.values():
            if isinstance(port, EvidencePort):
                if not set(port.covers) <= subjects:
                    raise ValueError("evidence port coverage refers to unknown subject inputs")
                pointer = port.option_slots_at
            else:
                pointer = port.option_ids_at
            if pointer is not None:
                parts = pointer_parts(pointer)
                if not parts:
                    raise ValueError("generated port options cannot replace the question root")
                if any(
                    parts[: len(prior)] == prior or prior[: len(parts)] == parts
                    for prior in generated
                ):
                    raise ValueError("interface-generated options overlap")
                generated.append(parts)
        for view in self.views.values():
            if isinstance(view, GlobalFactsView):
                if self.empty_scope != "inapplicable" or self.partitioning != "whole-scope":
                    raise ValueError("global views require a nonempty whole-scope question")
            if isinstance(view, RelationView):
                if self.partitioning != "whole-scope":
                    raise ValueError("relation completeness requires a whole-scope question")
                if not set(view.coverage) <= subjects:
                    raise ValueError("relation view coverage names unknown subject ports")
                for endpoint in (view.primary, view.associated):
                    if isinstance(endpoint, ExactEndpoint):
                        lookup = endpoint.lookup
                        if lookup.source == "self":
                            if not isinstance(self.views.get(lookup.view), SubjectFactsView):
                                raise ValueError(
                                    "exact endpoint lookup requires a subject facts view"
                                )
                        elif not isinstance(self.inputs.get(lookup.source.input), EvidencePort):
                            raise ValueError(
                                "exact endpoint lookup requires a declared evidence input"
                            )
            status = getattr(view, "status", None)
            if isinstance(status, SemanticStatus) and status.profile != self.semantic_profile:
                raise ValueError("view status profile differs from the exact observer semantics")
        return self


class ObservationInterface(ObservationInterfacePayload):
    interface_sha256: Sha256

    @model_validator(mode="after")
    def exact_digest(self) -> Self:
        document = self.model_dump(mode="json", by_alias=True, exclude={"interface_sha256"})
        if canonical_json_sha256(document) != self.interface_sha256:
            raise ValueError("observation interface digest differs from its exact payload")
        return self

    @classmethod
    def seal(cls, payload: ObservationInterfacePayload) -> ObservationInterface:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate({**document, "interface_sha256": canonical_json_sha256(document)})

    @property
    def ref(self) -> ExactDocumentRef:
        return ExactDocumentRef(id=self.id, sha256=self.interface_sha256)

    def validate_contract(self, contract: ObserverContract) -> None:
        if self.observer_contract != ExactDocumentRef(
            id=contract.id, sha256=contract.contract_sha256
        ):
            raise ValueError("observation interface differs from its selected observer contract")
        if self.facts_profile != ExactDocumentRef(
            id=contract.facts_schema.id, sha256=contract.facts_schema.profile_sha256
        ):
            raise ValueError("interface facts profile differs from the observer authority")
        if self.semantic_profile != ExactDocumentRef(
            id=contract.facts_semantics.id, sha256=contract.facts_semantics.profile_sha256
        ):
            raise ValueError("interface fact semantics differ from the observer authority")
        for view in self.views.values():
            if isinstance(view, (SubjectFactsView, GlobalFactsView)):
                schema_slice(contract.facts_schema, view.record_schema_at)
            if isinstance(getattr(view, "status", None), SemanticStatus) and (
                contract.facts_semantics == JSON_SCHEMA_ONLY_SEMANTIC_PROFILE
            ):
                raise ValueError(
                    "schema-only semantics cannot establish subject completeness status"
                )


class ObservationInterfaceEvidenceContext(Stove0ProtocolModel):
    """Exact local predecessor fixture for interface conformance, never runtime evidence."""

    contract: ObserverContract
    interface: ObservationInterface
    subjects: tuple[WorkArtifactSubject, ...]
    options: dict[str, JsonValue]
    facts: dict[str, JsonValue] | None
    evidence: dict[LocalName, ObservationInterfaceEvidenceContext] = Field(default_factory=dict)
    semantic_statuses: (
        dict[str, Literal["complete", "unsupported", "ambiguous", "insufficient"]] | None
    ) = None


class ObservationInterfaceVector(Stove0ProtocolModel):
    id: SemanticId
    accepted: StrictBool
    subjects: tuple[WorkArtifactSubject, ...]
    options: dict[str, JsonValue]
    facts: dict[str, JsonValue] | None
    evidence: dict[LocalName, ObservationInterfaceEvidenceContext] = Field(default_factory=dict)
    semantic_statuses: (
        dict[str, Literal["complete", "unsupported", "ambiguous", "insufficient"]] | None
    ) = None
    expected_views: dict[LocalName, tuple[JsonValue, ...]] | None = None

    @field_validator("subjects")
    @classmethod
    def exact_subjects(
        cls, subjects: tuple[WorkArtifactSubject, ...]
    ) -> tuple[WorkArtifactSubject, ...]:
        ids = [subject.id for subject in subjects]
        if ids != sorted(set(ids)):
            raise ValueError("interface vectors require exact canonical subject instances")
        return subjects


class ObservationInterfaceConformanceVectors(Stove0ProtocolModel):
    format: Literal["stove0-observation-interface-conformance/v1"] = (
        "stove0-observation-interface-conformance/v1"
    )
    interface_id: SemanticId
    vectors: tuple[ObservationInterfaceVector, ...] = Field(min_length=2)

    @model_validator(mode="after")
    def qualified_cases(self) -> Self:
        ids = [vector.id for vector in self.vectors]
        if ids != sorted(set(ids)) or {vector.accepted for vector in self.vectors} != {True, False}:
            raise ValueError("interface conformance needs canonical accepted and rejected cases")
        return self

    @property
    def sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json", by_alias=True))
