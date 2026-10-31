"""Attempt-fenced bounded capture of private journal topology occurrences."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from riverhog_provenance.framing import JournalFramePosition
from riverhog_provenance.topology_checkpoint import TopologyCaptureCheckpoint
from riverhog_provenance.topology_facts import (
    TOPOLOGY_CAPTURE_ALGORITHM,
    advance_topology_frame,
    plan_topology_frame,
)
from sqlalchemy import select, update
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochScanRecord as Scan
from riverhog_core.catalog_models import CollectionUploadProvenanceFrameRecord as Frame
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.catalog_models import CollectionUploadProvenanceTopologyFactRecord as Fact

from . import collection_uploads as source_owner
from . import upload_journal_epochs as epochs


@dataclass(frozen=True, slots=True)
class _Work:
    source: source_owner._UploadJournalValidationWork
    version: int
    next_frame: int
    state: str
    cursor_json: str
    header: TopologyCaptureCheckpoint


def _fence(work: _Work) -> tuple[Any, ...]:
    source = work.source
    return (
        Scan.collection_id == source.collection_id,
        Scan.journal_id == source.journal_id,
        Scan.attempt_id == source.attempt_id,
        Scan.directory_id == source.directory_id,
        Scan.topology_algorithm == TOPOLOGY_CAPTURE_ALGORITHM,
        Scan.version == work.version,
        Scan.topology_next_frame == work.next_frame,
        Scan.topology_state == work.state,
        Scan.topology_cursor_json == work.cursor_json,
        Scan.state == "complete",
    )


def _begin(
    factory: SessionFactory, source: source_owner._UploadJournalValidationWork
) -> _Work | None:
    with factory() as session, session.begin():
        if (
            session.scalar(
                select(Journal.collection_id)
                .where(*source_owner._upload_journal_validation_fence(source))
                .with_for_update(skip_locked=True)
            )
            is None
        ):
            return None
        row = session.get(Scan, (source.collection_id, source.journal_id), with_for_update=True)
        if row is None or row.attempt_id != source.attempt_id:
            return None
        if (
            row.directory_id != source.directory_id
            or row.topology_algorithm != TOPOLOGY_CAPTURE_ALGORITHM
        ):
            raise RuntimeError("topology checkpoint binding differs from its attempt")
        header = TopologyCaptureCheckpoint.from_json(row.topology_cursor_json)
        work = _Work(
            source,
            row.version,
            row.topology_next_frame,
            row.topology_state,
            row.topology_cursor_json,
            header,
        )
        if not 0 <= work.next_frame <= source.cursor.frame_count:
            raise RuntimeError("topology checkpoint frame differs from its source")
        if work.state == "complete" and (
            work.next_frame != source.cursor.frame_count or header.active
        ):
            raise RuntimeError("completed topology checkpoint differs from its source")
        if header.last is not None:
            frame, array, item, digest = header.last
            last = session.get(
                Fact,
                (source.collection_id, source.journal_id, source.attempt_id, frame, array, item),
            )
            if (
                frame > work.next_frame
                or last is None
                or (last.directory_id != source.directory_id or last.json_sha256 != digest)
            ):
                raise RuntimeError("topology checkpoint lost its last documentary fact")
        if row.state == "deferred":
            return _Work(
                source, work.version, work.next_frame, "deferred", work.cursor_json, header
            )
        if row.state != "complete":
            return None
        return work


@dataclass(frozen=True, slots=True)
class _Frame:
    position: JournalFramePosition
    epoch: int
    json_sha256: str


def _read_frame(factory: SessionFactory, work: _Work) -> _Frame | None:
    source = work.source
    with read_snapshot(factory) as session:
        if (
            session.scalar(
                select(Scan.collection_id)
                .join(
                    Journal,
                    (Scan.collection_id == Journal.collection_id)
                    & (Scan.journal_id == Journal.journal_id),
                )
                .where(*_fence(work), *source_owner._upload_journal_validation_fence(source))
            )
            is None
        ):
            return None
        row = session.get(
            Frame, (source.collection_id, source.journal_id, source.directory_id, work.next_frame)
        )
        if (
            row is None
            or row.epoch_attempt_id != source.attempt_id
            or row.epoch_ordinal is None
            or row.epoch_json_sha256 is None
        ):
            raise RuntimeError("topology capture lost its exact epoch frame")
        frame = _Frame(
            JournalFramePosition(row.ordinal, row.byte_offset, row.json_bytes),
            row.epoch_ordinal,
            row.epoch_json_sha256,
        )
    if frame.position.byte_offset + frame.position.json_bytes + 2 > source.source_bytes:
        raise RuntimeError("topology frame exceeds its source")
    return frame


class _SourceChanged(Exception):
    pass


@dataclass(frozen=True, slots=True)
class _Advance:
    next_frame: int
    state: str
    header: TopologyCaptureCheckpoint
    facts: tuple[dict[str, Any], ...]


def _compute(factory: SessionFactory, work: _Work) -> _Advance | None:
    frame = _read_frame(factory, work)
    if frame is None:
        return None
    cache: dict[int, bytes] = {}
    active = work.header.active
    raw = None
    if active is None:
        raw = epochs._read_frame(factory, work.source, frame.position, page_cache=cache)
        if raw is None:
            return None
        try:
            active = plan_topology_frame(raw)
        except (ValueError, TypeError):
            return _Advance(work.next_frame, "deferred", work.header, ())
    if active.json_sha256 != frame.json_sha256:
        raise RuntimeError("topology frame checkpoint differs from its captured digest")
    for array in (active.retracts, active.relations):
        if (
            not 0 <= array.offset <= array.end <= frame.position.json_bytes
            or array.index > array.count
        ):
            raise RuntimeError("topology array checkpoint exceeds its frame")
        if array.count and array.index < array.count and array.offset >= array.end:
            raise RuntimeError("topology array checkpoint lost its remaining items")

    def read(offset: int, length: int) -> bytes:
        if raw is not None:
            return raw[offset : offset + length]
        absolute = frame.position.byte_offset + 1 + offset
        extent = source_owner._JOURNAL_CAPTURE_SEGMENT_OCTETS
        ordinal, within = divmod(absolute, extent)
        page = cache.get(ordinal)
        if page is None:
            page = source_owner._read_upload_journal_capture_segment(
                factory, work.source, ordinal=ordinal
            )
            if page is None:
                raise _SourceChanged
            cache.clear()
            cache[ordinal] = page
        return page[within : min(extent, within + length)]

    try:
        result = advance_topology_frame(active, read)
    except _SourceChanged:
        return None
    except (ValueError, TypeError):
        return _Advance(work.next_frame, "deferred", work.header, ())
    facts: tuple[dict[str, Any], ...] = tuple(
        {
            "collection_id": work.source.collection_id,
            "journal_id": work.source.journal_id,
            "attempt_id": work.source.attempt_id,
            "frame_ordinal": work.next_frame,
            "array_kind": fact.array,
            "item_ordinal": fact.index,
            "directory_id": work.source.directory_id,
            "epoch": frame.epoch,
            "byte_offset": frame.position.byte_offset + 1 + fact.byte_offset,
            "json_bytes": fact.json_bytes,
            "json_sha256": fact.json_sha256,
            "assertion_id": fact.assertion_id,
            "kind": fact.kind,
            "source_id": fact.source_id,
            "target_id": fact.target_id,
        }
        for fact in result.facts
    )
    last = work.header.last
    if facts:
        fact = facts[-1]
        last = (work.next_frame, fact["array_kind"], fact["item_ordinal"], fact["json_sha256"])
    next_frame = work.next_frame + int(result.complete)
    state = "complete" if next_frame == work.source.cursor.frame_count else "scanning"
    header = TopologyCaptureCheckpoint(
        None if result.complete else result.cursor, work.header.facts + len(facts), last
    )
    return _Advance(next_frame, state, header, facts)


def _commit(factory: SessionFactory, work: _Work, advance: _Advance) -> bool:
    source = work.source
    with factory() as session, session.begin():
        if (
            session.scalar(
                select(Journal.collection_id)
                .where(*source_owner._upload_journal_validation_fence(source))
                .with_for_update(skip_locked=True)
            )
            is None
        ):
            return False
        changed = session.execute(
            update(Scan)
            .where(*_fence(work))
            .values(
                version=work.version + 1,
                topology_next_frame=advance.next_frame,
                topology_state=advance.state,
                topology_cursor_json=advance.header.to_json(),
            )
            .returning(Scan.collection_id)
        ).scalar_one_or_none()
        if changed is None:
            return False
        session.add_all(Fact(**fact) for fact in advance.facts)
        session.flush()
    return True


def advance_upload_journal_topology_capture(
    factory: SessionFactory, source: source_owner._UploadJournalValidationWork
) -> bool:
    """Commit one private topology unit and report readiness for native replay."""
    work = _begin(factory, source)
    if work is None:
        return False
    if work.state in {"complete", "deferred"}:
        return True
    advance = _compute(factory, work)
    if advance is None or not _commit(factory, work, advance):
        return False
    return advance.state in {"complete", "deferred"}
