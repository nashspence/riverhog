"""Canonical producer journal for an exactly measured Riverhog member."""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol import (
    ArtifactMaterializationDecisionBatchDocument,
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingBatchDocument,
)
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_MEMBER_HISTORY_ROLE,
    COLLECTION_MEMBER_ROLE,
    COLLECTION_PRODUCTION_CONTRACT_ID,
    COLLECTION_RECORD_FRAGMENT_BYTES_MAX,
    PRODUCER_SCHEMA_ID,
    RECORD_FRAGMENT_SCHEMA_ID,
    RECORD_MANIFEST_SCHEMA_ID,
    collection_production_contract,
    collection_production_profile,
)
from riverhog_protocol.provenance_transport import (
    CollectionArtifactProvenanceBindingDocument,
)
from riverhog_provenance import (
    ObservationResult,
    append_assertion_batches,
    assertion,
    create_journal,
    evidence,
    new_id,
    reference,
    software_agent_id,
    validate_journal,
)
from riverhog_provenance_contracts import require_canonical_uuid_urn

_RECORD_FRAGMENTS_PER_ENTRY = 24


@dataclass(frozen=True, slots=True)
class ProducerAttribution:
    producer_app: str
    adapter_id: str
    adapter_version: str
    source_event_id: str
    ingest_source: str
    source_context: Mapping[str, Any]
    construction_identity: str


@dataclass(frozen=True, slots=True)
class ProducedMemberJournal:
    journal_id: str
    content: bytes
    binding: CollectionArtifactProvenanceBindingDocument


class CanonicalProductionApi(Protocol):
    def upload_collection_upload_session_provenance_journal(
        self,
        collection_id: int,
        journal_id: str,
        *,
        content: Iterable[bytes],
        byte_count: int,
        sha256: str,
    ) -> object: ...

    def bind_collection_upload_session_artifact_provenance(
        self, collection_id: int, batch: CollectionArtifactProvenanceBindingBatchDocument
    ) -> object: ...

    def set_collection_upload_session_materialization_decisions(
        self, collection_id: int, batch: ArtifactMaterializationDecisionBatchDocument
    ) -> object: ...


def build_member_journal(
    *,
    member: ArtifactMemberIdentityDocument,
    observation: ObservationResult,
    delivery_context_id: str,
    attribution: ProducerAttribution,
    materialization_hint: tuple[str, ...] | None,
) -> ProducedMemberJournal:
    """Bind source measurement and producer attribution to one opaque member.

    The observation may carry genuine native source assertions. No locator or
    collection member name is inferred from the upload workspace.
    """

    require_canonical_uuid_urn(delivery_context_id, "delivery context")
    graph = observation.graph_fragment()
    objects = {row["id"]: row for rows in graph.values() for row in rows}
    directly_measured = objects[observation.observation_id]
    size = int(directly_measured["content"]["size_bytes"])
    digests = [
        item["value"]
        for item in directly_measured["content"]["digests"]
        if item["algorithm"] == "sha-256"
    ]
    if size != member.bytes or digests != [member.sha256]:
        raise ValueError("producer observation differs from registered member bytes")
    producer_agent_id = software_agent_id(attribution.producer_app, attribution.adapter_version)
    if producer_agent_id not in objects:
        graph.setdefault("agents", []).append(
            assertion(
                "agent",
                producer_agent_id,
                object_id=producer_agent_id,
                kind="software",
                name=attribution.producer_app,
                version=attribution.adapter_version,
            )
        )
    if materialization_hint is not None:
        if not materialization_hint:
            raise ValueError("materialization hint must have at least one component")
        occurrence = objects[observation.occurrence_id]
        occurrence["materialization_hint"] = {"components": list(materialization_hint)}
        occurrence["evidence"] = [evidence(producer_agent_id, "process_record")]
        occurrence["assertion_id"] = new_id()
    graph.setdefault("contexts", []).append(
        assertion(
            "context",
            producer_agent_id,
            object_id=delivery_context_id,
            kind="delivery",
        )
    )
    source_context = canonical_json_bytes(dict(attribution.source_context))
    source_context_sha256 = hashlib.sha256(source_context).hexdigest()
    producer_profile = collection_production_profile(
        PRODUCER_SCHEMA_ID,
        {
            "construction_identity": attribution.construction_identity,
            "producer_app": attribution.producer_app,
            "adapter_id": attribution.adapter_id,
            "adapter_version": attribution.adapter_version,
            "source_event_id": attribution.source_event_id,
            "ingest_source": attribution.ingest_source,
            "source_context_sha256": source_context_sha256,
            "source_context_bytes": str(len(source_context)),
        },
    )
    graph.setdefault("activities", []).append(
        assertion(
            "activity",
            producer_agent_id,
            kind="recording",
            outcome="success",
            associations=[
                {
                    "agent_id": producer_agent_id,
                    "role": COLLECTION_PRODUCTION_CONTRACT_ID + "/recorder",
                }
            ],
            contexts=[{"context_id": delivery_context_id, "role": "recording"}],
            evidence_items=[evidence(producer_agent_id, "process_record")],
        )
    )
    graph.setdefault("extensions", []).extend(
        (
            assertion(
                "extension",
                producer_agent_id,
                subject=reference(delivery_context_id, "context"),
                property=COLLECTION_PRODUCTION_CONTRACT_ID + "/producer",
                value={"type": "json", "value": producer_profile},
                evidence_items=[evidence(producer_agent_id, "process_record")],
            ),
            assertion(
                "extension",
                producer_agent_id,
                subject=reference(delivery_context_id, "context"),
                property=COLLECTION_PRODUCTION_CONTRACT_ID + "/record-manifest",
                value={
                    "type": "json",
                    "value": collection_production_profile(
                        RECORD_MANIFEST_SCHEMA_ID,
                        {
                            "record_kind": "source-context",
                            "record_sha256": source_context_sha256,
                            "total_bytes": str(len(source_context)),
                            "part_count": str(
                                (len(source_context) + COLLECTION_RECORD_FRAGMENT_BYTES_MAX - 1)
                                // COLLECTION_RECORD_FRAGMENT_BYTES_MAX
                            ),
                        },
                    ),
                },
                evidence_items=[evidence(producer_agent_id, "process_record")],
            ),
        )
    )
    journal_id = new_id()
    association = assertion(
        "delivery_association",
        producer_agent_id,
        delivery_context_id=delivery_context_id,
        slot={"kind": "text", "text": str(member.artifact_id)},
        role=COLLECTION_MEMBER_ROLE,
        state=reference(observation.state_id, "state"),
        verification_observation_id=observation.observation_id,
        evidence_items=[evidence(producer_agent_id, "process_record")],
    )
    graph.setdefault("delivery_associations", []).append(association)
    graph.setdefault("journal_subjects", []).append(
        assertion(
            "journal_subject",
            producer_agent_id,
            journal_id=journal_id,
            artifact=reference(observation.artifact_id, "artifact"),
            role=COLLECTION_MEMBER_HISTORY_ROLE,
            evidence_items=[evidence(producer_agent_id, "process_record")],
        )
    )
    catalog = observation.catalog.with_contracts((collection_production_contract(),))
    raw = create_journal(
        graph,
        recorded_by_agent_id=producer_agent_id,
        journal_id=journal_id,
        catalog=catalog,
    )

    def fragment_batches() -> Iterable[dict[str, Any]]:
        fragment_rows: list[dict[str, Any]] = []
        for offset in range(0, len(source_context), COLLECTION_RECORD_FRAGMENT_BYTES_MAX):
            fragment = source_context[offset : offset + COLLECTION_RECORD_FRAGMENT_BYTES_MAX]
            fragment_rows.append(
                assertion(
                    "extension",
                    producer_agent_id,
                    subject=reference(delivery_context_id, "context"),
                    property=COLLECTION_PRODUCTION_CONTRACT_ID + "/record-fragment",
                    value={
                        "type": "json",
                        "value": collection_production_profile(
                            RECORD_FRAGMENT_SCHEMA_ID,
                            {
                                "record_kind": "source-context",
                                "record_sha256": source_context_sha256,
                                "total_bytes": str(len(source_context)),
                                "offset": str(offset),
                                "data_base64": base64.b64encode(fragment).decode("ascii"),
                            },
                        ),
                    },
                    evidence_items=[evidence(producer_agent_id, "process_record")],
                )
            )
            if len(fragment_rows) == _RECORD_FRAGMENTS_PER_ENTRY:
                yield {"extensions": fragment_rows}
                fragment_rows = []
        if fragment_rows:
            yield {"extensions": fragment_rows}

    raw = append_assertion_batches(
        raw, fragment_batches(), recorded_by_agent_id=producer_agent_id, catalog=catalog
    )
    summary = validate_journal(raw, catalog=catalog)
    binding = CollectionArtifactProvenanceBindingDocument.model_validate(
        {
            "artifact_id": member.artifact_id,
            "journal": summary.anchor,
            "delivery_association_id": association["id"],
        }
    )
    return ProducedMemberJournal(journal_id, raw, binding)


def bind_produced_member(
    api: CanonicalProductionApi,
    *,
    collection_id: int,
    member: ArtifactMemberIdentityDocument,
    observation: ObservationResult,
    delivery_context_id: str,
    attribution: ProducerAttribution,
    materialization_hint: tuple[str, ...] | None,
    allow_missing_materialization_hint: bool,
) -> ProducedMemberJournal:
    """Stage and bind one already registered member before finalization."""

    decision = member_materialization_decision(
        artifact_id=member.artifact_id,
        materialization_hint=materialization_hint,
        allow_missing_materialization_hint=allow_missing_materialization_hint,
    )
    produced = build_member_journal(
        member=member,
        observation=observation,
        delivery_context_id=delivery_context_id,
        attribution=attribution,
        materialization_hint=materialization_hint,
    )
    api.upload_collection_upload_session_provenance_journal(
        collection_id,
        produced.journal_id,
        content=(produced.content,),
        byte_count=len(produced.content),
        sha256=hashlib.sha256(produced.content).hexdigest(),
    )
    api.bind_collection_upload_session_artifact_provenance(
        collection_id,
        CollectionArtifactProvenanceBindingBatchDocument(bindings=[produced.binding]),
    )
    api.set_collection_upload_session_materialization_decisions(collection_id, decision)
    return produced


def member_materialization_decision(
    *,
    artifact_id: str,
    materialization_hint: tuple[str, ...] | None,
    allow_missing_materialization_hint: bool,
) -> ArtifactMaterializationDecisionBatchDocument:
    """Validate the same immutable choice for initial publication and retries."""

    return ArtifactMaterializationDecisionBatchDocument.model_validate(
        {
            "decisions": [
                {
                    "artifact_id": artifact_id,
                    "materialization_hint": (
                        {"components": list(materialization_hint)}
                        if materialization_hint is not None
                        else None
                    ),
                    "allow_missing_materialization_hint": allow_missing_materialization_hint,
                }
            ]
        }
    )


__all__ = [
    "CanonicalProductionApi",
    "ProducedMemberJournal",
    "ProducerAttribution",
    "bind_produced_member",
    "build_member_journal",
    "member_materialization_decision",
]
