"""Transactional staging of a non-authoritative canonical discovery generation.

The caller supplies already validated exact journal snapshots. Small committed
batches may be retried; the active generation is changed only by a separate
complete-generation publication step.
"""

from __future__ import annotations

import base64
import hashlib
import uuid
from collections.abc import Sequence
from typing import Any, cast

from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance import JournalSummary
from riverhog_provenance_contracts import core_contract
from sqlalchemy import Table, func, select
from sqlalchemy.orm import Session
from time_formats import utc_timestamp_now

from riverhog_core.canonical_discovery_extraction import ascii_fold, literal_chunks
from riverhog_core.canonical_discovery_rows import IndexedAssertion
from riverhog_core.catalog_models import CollectionRecord
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexAssertionRecord,
    CollectionProvenanceIndexEdgeRecord,
    CollectionProvenanceIndexEntryRecord,
    CollectionProvenanceIndexGenerationRecord,
    CollectionProvenanceIndexMemberRecord,
    CollectionProvenanceIndexMembershipRecord,
    CollectionProvenanceIndexProfileRecord,
    CollectionProvenanceIndexSnapshotRecord,
    CollectionProvenanceIndexStateRecord,
    CollectionProvenanceIndexTextChunkRecord,
    CollectionProvenanceIndexValueRecord,
)

_EXTRACTION_POLICY = {
    "format": "riverhog-canonical-discovery-extraction/v1",
    "text": "literal/exact-or-ascii-fold",
    "name_bytes": "escape-and-explicit-codec",
    "scope": "member/input-history/collection/recorded-history",
    "profile_data": "lexical-only",
    "record_state": "effective-or-retracted",
}


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


EXTRACTION_CONTRACT_SHA256 = _sha256(canonical_json_bytes(_EXTRACTION_POLICY))
_MAX_ROWS_PER_BATCH = 32
_MAX_POSTINGS_PER_ASSERTION = 20_000
_MAX_POSTINGS_PER_PAGE = 50_000
_MAX_TEXT_CHUNKS_PER_POSTING = 10_000


class StaleIndexBuild(RuntimeError):
    pass


class IndexResourceLimit(RuntimeError):
    pass


def begin_index_build(session: Session, *, collection_id: int) -> str:
    """Fence one rebuild against the current archive root and prior worker epoch."""

    collection = session.get(CollectionRecord, collection_id, with_for_update=True)
    if (
        collection is None
        or collection.archive_root_sha256 is None
        or collection.provenance_identity is None
        or collection.artifact_set_identity is None
    ):
        raise StaleIndexBuild("collection has no sealed canonical archive root")
    state = session.get(CollectionProvenanceIndexStateRecord, collection_id, with_for_update=True)
    if state is None:
        state = CollectionProvenanceIndexStateRecord(collection_id=collection_id, epoch=1)
        session.add(state)
    else:
        state.epoch += 1
    build_id = str(uuid.uuid4())
    state.pending_build_id = build_id
    state.phase = "indexing"
    state.failure = None
    session.add(
        CollectionProvenanceIndexGenerationRecord(
            build_id=build_id,
            collection_id=collection_id,
            archive_generation=collection.archive_generation,
            archive_root_sha256=collection.archive_root_sha256,
            provenance_identity=collection.provenance_identity,
            core_contract_sha256=core_contract().contract_sha256,
            extraction_contract_sha256=EXTRACTION_CONTRACT_SHA256,
            complete=False,
            expected_epoch=state.epoch,
            created_at=utc_timestamp_now(),
        )
    )
    return build_id


def _require_pending(session: Session, build_id: str) -> CollectionProvenanceIndexGenerationRecord:
    build = session.get(CollectionProvenanceIndexGenerationRecord, build_id)
    if build is None or build.complete:
        raise StaleIndexBuild("discovery build is absent or already complete")
    state = session.get(
        CollectionProvenanceIndexStateRecord, build.collection_id, with_for_update=True
    )
    collection = session.get(CollectionRecord, build.collection_id)
    if (
        state is None
        or state.pending_build_id != build_id
        or state.epoch != build.expected_epoch
        or collection is None
        or collection.archive_generation != build.archive_generation
        or collection.archive_root_sha256 != build.archive_root_sha256
        or collection.provenance_identity != build.provenance_identity
    ):
        raise StaleIndexBuild("discovery worker lost its archive-root fence")
    return build


def _same_record(existing: Any, proposed: Any, fields: tuple[str, ...]) -> bool:
    return all(getattr(existing, field) == getattr(proposed, field) for field in fields)


def stage_snapshot_header(session: Session, *, build_id: str, summary: JournalSummary) -> None:
    """Commit one exact journal anchor before bounded entry and assertion pages."""

    _require_pending(session, build_id)
    anchor = summary.anchor
    proposed = CollectionProvenanceIndexSnapshotRecord(
        build_id=build_id,
        journal_id=summary.journal_id,
        prefix_sha256=summary.journal_sha256,
        prefix_bytes=summary.journal_bytes,
        through_entry_id=anchor["through"]["entry_id"],
        through_sequence=int(anchor["through"]["sequence"]),
        through_json_sha256=anchor["through"]["json_sha256"],
        assertion_count=sum(
            len(rows)
            for frame in summary.frames
            for rows in frame.document["body"].get("assertions", {}).values()
        ),
    )
    key = (build_id, summary.journal_id, summary.journal_sha256)
    existing = session.get(CollectionProvenanceIndexSnapshotRecord, key)
    if existing is None:
        session.add(proposed)
    elif not _same_record(
        existing,
        proposed,
        (
            "prefix_bytes",
            "through_entry_id",
            "through_sequence",
            "through_json_sha256",
            "assertion_count",
        ),
    ):
        raise StaleIndexBuild("journal snapshot differs from staged header")


def stage_entry_page(
    session: Session, *, build_id: str, summary: JournalSummary, start: int, limit: int = 128
) -> int:
    """Stage a bounded exact entry range; return the next sequence."""

    _require_pending(session, build_id)
    if type(start) is not int or type(limit) is not int or start < 0 or not 1 <= limit <= 128:
        raise ValueError("discovery entry page bounds are invalid")
    key = (build_id, summary.journal_id, summary.journal_sha256)
    if session.get(CollectionProvenanceIndexSnapshotRecord, key) is None:
        raise StaleIndexBuild("journal snapshot header is absent")
    end = min(start + limit, len(summary.frames))
    for sequence in range(start, end):
        frame = summary.frames[sequence]
        document = frame.document
        proposed_entry = CollectionProvenanceIndexEntryRecord(
            build_id=build_id,
            journal_id=summary.journal_id,
            prefix_sha256=summary.journal_sha256,
            sequence=sequence,
            entry_id=document["id"],
            json_sha256=frame.sha256,
            entry_kind=document["entry_kind"],
        )
        entry_key = (*key, sequence)
        existing_entry = session.get(CollectionProvenanceIndexEntryRecord, entry_key)
        if existing_entry is None:
            session.add(proposed_entry)
        elif not _same_record(
            existing_entry, proposed_entry, ("entry_id", "json_sha256", "entry_kind")
        ):
            raise StaleIndexBuild("journal entry differs from staged header")
    return end


def _scalar_type(value: object) -> str:
    if value is None:
        return "null"
    if type(value) is bool:
        return "boolean"
    if type(value) is int:
        return "integer"
    if type(value) is str:
        return "string"
    raise TypeError("nonportable index scalar")


def stage_assertion_page(
    session: Session, *, build_id: str, rows: Sequence[IndexedAssertion]
) -> None:
    """Stage <=32 complete rows; a retry cannot alter existing exact support."""

    _require_pending(session, build_id)
    if not 1 <= len(rows) <= _MAX_ROWS_PER_BATCH:
        raise ValueError("discovery assertion page must contain 1 to 32 rows")
    if len({row.row_key for row in rows}) != len(rows):
        raise ValueError("discovery assertion page repeats a row key")
    if sum(len(row.postings) for row in rows) > _MAX_POSTINGS_PER_PAGE:
        raise IndexResourceLimit("discovery page exceeds scalar posting budget")
    new_rows: list[IndexedAssertion] = []
    for row in rows:
        if len(row.postings) > _MAX_POSTINGS_PER_ASSERTION:
            raise IndexResourceLimit("assertion exceeds scalar posting budget")
        entry = session.get(
            CollectionProvenanceIndexEntryRecord,
            (build_id, row.journal_id, row.prefix_sha256, row.entry_sequence),
        )
        if entry is None or (entry.entry_id, entry.json_sha256) != (
            row.entry_id,
            row.entry_sha256,
        ):
            raise StaleIndexBuild("assertion has no exact staged journal entry")
        existing = session.get(CollectionProvenanceIndexAssertionRecord, (build_id, row.row_key))
        if existing is not None:
            if not _same_record(
                existing,
                row,
                ("assertion_id", "referent_id", "kind", "assertion_state", "canonical_json"),
            ) or (existing.journal_id, existing.prefix_sha256, existing.sequence) != (
                row.journal_id,
                row.prefix_sha256,
                row.entry_sequence,
            ):
                raise StaleIndexBuild("assertion row changed during retry")
            continue
        new_rows.append(row)
        session.add(
            CollectionProvenanceIndexAssertionRecord(
                build_id=build_id,
                row_key=row.row_key,
                journal_id=row.journal_id,
                prefix_sha256=row.prefix_sha256,
                sequence=row.entry_sequence,
                entry_id=row.entry_id,
                assertion_id=row.assertion_id,
                referent_id=row.referent_id,
                kind=row.kind,
                assertion_state=row.assertion_state,
                assertion_sha256=_sha256(row.canonical_json),
                canonical_json=row.canonical_json,
            )
        )
    session.flush()
    chunks: list[CollectionProvenanceIndexTextChunkRecord] = []
    for row in new_rows:
        for pointer in row.profiles:
            session.add(
                CollectionProvenanceIndexProfileRecord(
                    build_id=build_id,
                    row_key=row.row_key,
                    pointer=pointer.pointer,
                    contract_id=pointer.profile.contract_id,
                    contract_sha256=pointer.profile.contract_sha256,
                    schema_id=pointer.profile.schema_id,
                )
            )
        for ordinal, edge in enumerate(row.edges):
            session.add(
                CollectionProvenanceIndexEdgeRecord(
                    build_id=build_id,
                    row_key=row.row_key,
                    ordinal=ordinal,
                    role=edge.role,
                    target_id=edge.target_id,
                    target_type=edge.target_type,
                    target_scope=edge.target_scope,
                    exact_foreign_reference_json=edge.exact_foreign_reference,
                )
            )
        for ordinal, posting in enumerate(row.postings):
            value = posting.value
            text = value if type(value) is str else None
            session.add(
                CollectionProvenanceIndexValueRecord(
                    build_id=build_id,
                    row_key=row.row_key,
                    ordinal=ordinal,
                    pointer=posting.pointer,
                    representation=posting.representation,
                    scalar_type=_scalar_type(value),
                    source_value_sha256=posting.source_value_sha256,
                    exact_json=canonical_json_bytes(value),
                    text_value=text,
                    folded_text=ascii_fold(text) if text is not None else None,
                )
            )
            if text is None or posting.representation == "exact-bytes":
                continue
            for chunk_ordinal, (offset, content) in enumerate(literal_chunks(text)):
                if chunk_ordinal >= _MAX_TEXT_CHUNKS_PER_POSTING:
                    raise IndexResourceLimit("scalar exceeds literal text chunk budget")
                chunks.append(
                    CollectionProvenanceIndexTextChunkRecord(
                        build_id=build_id,
                        row_key=row.row_key,
                        value_ordinal=ordinal,
                        chunk_ordinal=chunk_ordinal,
                        codepoint_offset=offset,
                        text_chunk=content,
                        folded_chunk=ascii_fold(content),
                    )
                )
    session.flush()
    session.add_all(chunks)


def stage_member(
    session: Session,
    *,
    build_id: str,
    artifact_id: str,
    bytes: int,
    sha256: str,
    journal_id: str,
    prefix_sha256: str,
    delivery_association_id: str,
) -> None:
    """Bind a member to its exact already staged primary snapshot."""

    _require_pending(session, build_id)
    if (
        session.get(CollectionProvenanceIndexSnapshotRecord, (build_id, journal_id, prefix_sha256))
        is None
    ):
        raise StaleIndexBuild("member primary snapshot is not staged")
    proposed = CollectionProvenanceIndexMemberRecord(
        build_id=build_id,
        artifact_id=artifact_id,
        bytes=bytes,
        sha256=sha256,
        journal_id=journal_id,
        prefix_sha256=prefix_sha256,
        delivery_association_id=delivery_association_id,
    )
    existing = session.get(CollectionProvenanceIndexMemberRecord, (build_id, artifact_id))
    if existing is None:
        session.add(proposed)
    elif not _same_record(
        existing,
        proposed,
        ("bytes", "sha256", "journal_id", "prefix_sha256", "delivery_association_id"),
    ):
        raise StaleIndexBuild("member differs from staged exact binding")


def stage_membership_page(
    session: Session,
    *,
    build_id: str,
    rows: Sequence[tuple[str, str, str]],
) -> None:
    """Stage bounded (artifact, exact assertion row, relevance scope) links."""

    _require_pending(session, build_id)
    if not 1 <= len(rows) <= 1024 or len(set(rows)) != len(rows):
        raise ValueError("discovery membership page is empty, repeated or too large")
    for artifact_id, row_key, scope in rows:
        if scope not in {"member", "input-history", "collection", "recorded-history"}:
            raise ValueError("discovery relevance scope is invalid")
        if session.get(CollectionProvenanceIndexMemberRecord, (build_id, artifact_id)) is None:
            raise StaleIndexBuild("membership has no exact staged member")
        if session.get(CollectionProvenanceIndexAssertionRecord, (build_id, row_key)) is None:
            raise StaleIndexBuild("membership has no exact staged assertion")
        if (
            session.get(
                CollectionProvenanceIndexMembershipRecord,
                (build_id, artifact_id, row_key, scope),
            )
            is None
        ):
            session.add(
                CollectionProvenanceIndexMembershipRecord(
                    build_id=build_id, artifact_id=artifact_id, row_key=row_key, scope=scope
                )
            )


_DATASET_TABLES = (
    CollectionProvenanceIndexSnapshotRecord,
    CollectionProvenanceIndexEntryRecord,
    CollectionProvenanceIndexAssertionRecord,
    CollectionProvenanceIndexProfileRecord,
    CollectionProvenanceIndexValueRecord,
    CollectionProvenanceIndexTextChunkRecord,
    CollectionProvenanceIndexEdgeRecord,
    CollectionProvenanceIndexMemberRecord,
    CollectionProvenanceIndexMembershipRecord,
)


def _dataset_sha256(session: Session, build_id: str) -> str:
    """Commit logical rows in deterministic order without materializing the dataset."""

    digest = hashlib.sha256(b"riverhog-canonical-discovery-dataset/v1\n")
    for model in _DATASET_TABLES:
        table = cast(Table, model.__table__)
        columns = [column for column in table.columns if column.name != "build_id"]
        keys = [column for column in table.primary_key.columns if column.name != "build_id"]
        statement = (
            select(*columns)
            .where(table.c.build_id == build_id)
            .order_by(*keys)
            .execution_options(yield_per=128)
        )
        for row in session.execute(statement):
            values = {
                str(column.name): (
                    {"base64": base64.b64encode(value).decode("ascii")}
                    if isinstance(value, bytes)
                    else str(value)
                    if type(value) is int
                    else value
                )
                for column, value in zip(columns, row, strict=True)
            }
            encoded = canonical_json_bytes({"table": str(table.name), "values": values})
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
    return digest.hexdigest()


def complete_index_build(
    session: Session,
    *,
    build_id: str,
    expected_snapshots: int,
    expected_members: int,
    expected_memberships: int,
) -> str:
    """Seal only a fully staged dataset; return its content generation identity."""

    build = _require_pending(session, build_id)
    counts = []
    for model in (
        CollectionProvenanceIndexSnapshotRecord,
        CollectionProvenanceIndexMemberRecord,
        CollectionProvenanceIndexMembershipRecord,
    ):
        counts.append(
            session.scalar(
                select(func.count()).select_from(model).where(model.build_id == build_id)
            )
        )
    if counts != [expected_snapshots, expected_members, expected_memberships]:
        raise StaleIndexBuild("discovery generation is incomplete")
    snapshots = session.execute(
        select(
            CollectionProvenanceIndexSnapshotRecord.journal_id,
            CollectionProvenanceIndexSnapshotRecord.prefix_sha256,
            CollectionProvenanceIndexSnapshotRecord.through_sequence,
            CollectionProvenanceIndexSnapshotRecord.assertion_count,
        )
        .where(CollectionProvenanceIndexSnapshotRecord.build_id == build_id)
        .execution_options(yield_per=128)
    )
    for journal_id, prefix_sha256, through_sequence, assertion_count in snapshots:
        entry_count = session.scalar(
            select(func.count())
            .select_from(CollectionProvenanceIndexEntryRecord)
            .where(
                CollectionProvenanceIndexEntryRecord.build_id == build_id,
                CollectionProvenanceIndexEntryRecord.journal_id == journal_id,
                CollectionProvenanceIndexEntryRecord.prefix_sha256 == prefix_sha256,
            )
        )
        assertion_count_actual = session.scalar(
            select(func.count())
            .select_from(CollectionProvenanceIndexAssertionRecord)
            .where(
                CollectionProvenanceIndexAssertionRecord.build_id == build_id,
                CollectionProvenanceIndexAssertionRecord.journal_id == journal_id,
                CollectionProvenanceIndexAssertionRecord.prefix_sha256 == prefix_sha256,
            )
        )
        if (entry_count, assertion_count_actual) != (through_sequence + 1, assertion_count):
            raise StaleIndexBuild("discovery snapshot entries or assertions are incomplete")
    dataset_sha256 = _dataset_sha256(session, build_id)
    generation_id = _sha256(
        canonical_json_bytes(
            {
                "format": "riverhog-canonical-discovery-generation/v1",
                "archive_root_sha256": build.archive_root_sha256,
                "provenance_identity": build.provenance_identity,
                "core_contract_sha256": build.core_contract_sha256,
                "extraction_contract_sha256": build.extraction_contract_sha256,
                "dataset_sha256": dataset_sha256,
            }
        )
    )
    build.dataset_sha256 = dataset_sha256
    build.generation_id = generation_id
    build.complete = True
    build.completed_at = utc_timestamp_now()
    return generation_id


def publish_index_build(session: Session, *, build_id: str) -> str:
    """Atomically switch one root-fenced completed generation pointer."""

    build = session.get(CollectionProvenanceIndexGenerationRecord, build_id)
    if build is None or not build.complete or build.generation_id is None:
        raise StaleIndexBuild("discovery generation is not complete")
    state = session.get(
        CollectionProvenanceIndexStateRecord, build.collection_id, with_for_update=True
    )
    collection = session.get(CollectionRecord, build.collection_id, with_for_update=True)
    if (
        state is None
        or state.pending_build_id != build_id
        or state.epoch != build.expected_epoch
        or collection is None
        or collection.archive_generation != build.archive_generation
        or collection.archive_root_sha256 != build.archive_root_sha256
        or collection.provenance_identity != build.provenance_identity
    ):
        raise StaleIndexBuild("completed discovery worker lost its publication fence")
    state.active_build_id = build_id
    state.pending_build_id = None
    state.phase = "ready"
    state.failure = None
    return build.generation_id


__all__ = [
    "EXTRACTION_CONTRACT_SHA256",
    "IndexResourceLimit",
    "StaleIndexBuild",
    "begin_index_build",
    "complete_index_build",
    "publish_index_build",
    "stage_assertion_page",
    "stage_entry_page",
    "stage_member",
    "stage_membership_page",
    "stage_snapshot_header",
]
