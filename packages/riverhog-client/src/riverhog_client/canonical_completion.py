"""Canonical terminal recording without rewriting early causal journals or sealed records."""

from __future__ import annotations

import base64
import builtins
import hashlib
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from io import BytesIO
from typing import Any, BinaryIO

from riverhog_protocol.collection_completion import CollectionCompletionRequirementDocument
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_PRODUCTION_CONTRACT_ID,
    COLLECTION_RECORD_FRAGMENT_BYTES_MAX,
    EXECUTION_COMPLETION_SCHEMA_ID,
    RECORD_FRAGMENT_SCHEMA_ID,
    RECORD_MANIFEST_SCHEMA_ID,
    collection_production_contract,
    collection_production_profile,
)
from riverhog_provenance import (
    JournalSummary,
    assertion,
    create_journal,
    evidence,
    new_id,
    reference,
    software_agent_id,
    write_assertion_batches,
)
from riverhog_provenance_contracts import ContractCatalog


@dataclass(frozen=True, slots=True)
class CompletionRecord:
    kind: str
    bytes: int
    sha256: str
    read: Callable[[], Iterable[builtins.bytes]]

    @classmethod
    def from_bytes(cls, kind: str, content: builtins.bytes) -> CompletionRecord:
        raw = builtins.bytes(content)
        return cls(kind, len(raw), hashlib.sha256(raw).hexdigest(), lambda: (raw,))


def write_completion_journal(
    destination: BinaryIO,
    *,
    requirement: CollectionCompletionRequirementDocument,
    records: Iterable[CompletionRecord],
    execution_sha256: str,
    output_bindings_sha256: str,
    input_history_bindings_sha256: str,
    disposition_set_sha256: str,
    producer_app: str,
    producer_version: str,
    late_assertions: Iterable[Mapping[str, Any]] = (),
    journal_id: str | None = None,
    recorded_at: str | None = None,
) -> JournalSummary:
    """Record late evidence through a distinct recording activity, never a new generation.

    Record preimages are supplied exactly, before the final H/corpus/archive root.
    The caller owns validation against the accepted execution/operation contracts.
    """

    journal_id = journal_id or new_id()
    namespace = uuid.UUID(journal_id.removeprefix("urn:uuid:"))
    ordinal = 0

    def next_id() -> str:
        nonlocal ordinal
        ordinal += 1
        return "urn:uuid:" + str(uuid.uuid5(namespace, "record:" + str(ordinal)))

    def named_assertion(record_type: str, agent: str, **fields: Any) -> dict[str, Any]:
        fields.setdefault("object_id", next_id())
        fields["assertion_id"] = next_id()
        return assertion(record_type, agent, **fields)

    def entry_ids() -> Iterator[str]:
        ordinal = 1
        while True:
            yield "urn:uuid:" + str(uuid.uuid5(namespace, "entry:" + str(ordinal)))
            ordinal += 1

    who = software_agent_id(producer_app, producer_version)
    activity = next_id()
    catalog = ContractCatalog((collection_production_contract(),))
    graph = {
        "agents": [
            named_assertion(
                "agent",
                who,
                object_id=who,
                kind="software",
                name=producer_app,
                version=producer_version,
            )
        ],
        "activities": [
            named_assertion(
                "activity",
                who,
                object_id=activity,
                kind="recording",
                outcome="success",
                associations=[
                    {"agent_id": who, "role": COLLECTION_PRODUCTION_CONTRACT_ID + "/recorder"}
                ],
                evidence_items=[evidence(who, "process_record")],
            )
        ],
        "extensions": [
            named_assertion(
                "extension",
                who,
                subject=reference(activity, "activity"),
                property=COLLECTION_PRODUCTION_CONTRACT_ID + "/execution-completion",
                value={
                    "type": "json",
                    "value": collection_production_profile(
                        EXECUTION_COMPLETION_SCHEMA_ID,
                        {
                            "requirement_sha256": requirement.identity,
                            "execution_id": requirement.execution_id,
                            "execution_sha256": execution_sha256,
                            "output_bindings_sha256": output_bindings_sha256,
                            "input_history_bindings_sha256": input_history_bindings_sha256,
                            "disposition_set_sha256": disposition_set_sha256,
                        },
                    ),
                },
                evidence_items=[evidence(who, "process_record")],
            )
        ],
    }
    raw = create_journal(
        graph,
        recorded_by_agent_id=who,
        catalog=catalog,
        journal_id=journal_id,
        recorded_at=recorded_at,
        entry_id="urn:uuid:" + str(uuid.uuid5(namespace, "entry:0")),
    )

    def batches() -> Iterator[Mapping[str, Any]]:
        seen: set[str] = set()
        for record in records:
            if (
                record.kind in seen
                or record.kind not in requirement.record_kinds
                or record.bytes < 1
            ):
                raise ValueError(
                    "completion record inventory differs from its accepted declaration"
                )
            seen.add(record.kind)
            parts = (
                record.bytes + COLLECTION_RECORD_FRAGMENT_BYTES_MAX - 1
            ) // COLLECTION_RECORD_FRAGMENT_BYTES_MAX
            yield {
                "extensions": [
                    named_assertion(
                        "extension",
                        who,
                        subject=reference(activity, "activity"),
                        property=COLLECTION_PRODUCTION_CONTRACT_ID + "/record-manifest",
                        value={
                            "type": "json",
                            "value": collection_production_profile(
                                RECORD_MANIFEST_SCHEMA_ID,
                                {
                                    "record_kind": record.kind,
                                    "record_sha256": record.sha256,
                                    "total_bytes": str(record.bytes),
                                    "part_count": str(parts),
                                },
                            ),
                        },
                        evidence_items=[evidence(who, "process_record")],
                    )
                ]
            }
            digest = hashlib.sha256()
            offset = 0
            pending = bytearray()

            def part(
                content: bytes, current_record: CompletionRecord, current_offset: int
            ) -> Mapping[str, Any]:
                return {
                    "extensions": [
                        named_assertion(
                            "extension",
                            who,
                            subject=reference(activity, "activity"),
                            property=COLLECTION_PRODUCTION_CONTRACT_ID + "/record-fragment",
                            value={
                                "type": "json",
                                "value": collection_production_profile(
                                    RECORD_FRAGMENT_SCHEMA_ID,
                                    {
                                        "record_kind": current_record.kind,
                                        "record_sha256": current_record.sha256,
                                        "total_bytes": str(current_record.bytes),
                                        "offset": str(current_offset),
                                        "data_base64": base64.b64encode(content).decode("ascii"),
                                    },
                                ),
                            },
                            evidence_items=[evidence(who, "process_record")],
                        )
                    ]
                }

            for chunk in record.read():
                digest.update(chunk)
                for start in range(0, len(chunk), COLLECTION_RECORD_FRAGMENT_BYTES_MAX):
                    pending.extend(chunk[start : start + COLLECTION_RECORD_FRAGMENT_BYTES_MAX])
                    while len(pending) >= COLLECTION_RECORD_FRAGMENT_BYTES_MAX:
                        content = bytes(pending[:COLLECTION_RECORD_FRAGMENT_BYTES_MAX])
                        del pending[:COLLECTION_RECORD_FRAGMENT_BYTES_MAX]
                        yield part(content, record, offset)
                        offset += len(content)
            if pending:
                yield part(bytes(pending), record, offset)
                offset += len(pending)
            if offset != record.bytes or digest.hexdigest() != record.sha256:
                raise ValueError("completion record preimage differs from its sealed identity")
        if seen != set(requirement.record_kinds):
            raise ValueError("accepted completion record preimage is missing")
        yield from late_assertions

    return write_assertion_batches(
        destination,
        raw,
        batches(),
        recorded_by_agent_id=who,
        catalog=catalog,
        recorded_at=recorded_at,
        entry_ids=entry_ids(),
    )


def build_completion_journal(
    *,
    requirement: CollectionCompletionRequirementDocument,
    records: Iterable[CompletionRecord],
    execution_sha256: str,
    output_bindings_sha256: str,
    input_history_bindings_sha256: str,
    disposition_set_sha256: str,
    producer_app: str,
    producer_version: str,
    late_assertions: Iterable[Mapping[str, Any]] = (),
    journal_id: str | None = None,
    recorded_at: str | None = None,
) -> bytes:
    """Materialize a completion journal for callers that explicitly need bytes."""
    with BytesIO() as destination:
        write_completion_journal(
            destination,
            requirement=requirement,
            records=records,
            execution_sha256=execution_sha256,
            output_bindings_sha256=output_bindings_sha256,
            input_history_bindings_sha256=input_history_bindings_sha256,
            disposition_set_sha256=disposition_set_sha256,
            producer_app=producer_app,
            producer_version=producer_version,
            late_assertions=late_assertions,
            journal_id=journal_id,
            recorded_at=recorded_at,
        )
        return destination.getvalue()


__all__ = ["CompletionRecord", "build_completion_journal", "write_completion_journal"]
