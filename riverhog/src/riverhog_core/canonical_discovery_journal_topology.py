"""Private bounded topology facts beneath the registered original journal owner."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass

from riverhog_canonical_json import format_scalar, parse_scalar
from riverhog_provenance.epoch_boundaries import JournalEpochBoundary
from riverhog_provenance.framing import JournalFramePosition
from riverhog_provenance.topology_checkpoint import (
    TopologyCaptureCheckpoint,
    validate_checkpoint_digest,
)
from riverhog_provenance.topology_facts import (
    TOPOLOGY_CAPTURE_ALGORITHM,
    JournalTopologyFact,
    advance_topology_frame,
    plan_topology_frame,
)

from riverhog_core import canonical_discovery_journal_epochs as epochs
from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_history_inputs import _one
from riverhog_core.canonical_discovery_journal_topology_growth import (
    declare_growth_for_journal_topology,
)


@dataclass(frozen=True, slots=True)
class DiscoveryJournalTopologyProgress:
    version: int
    next_frame: int
    phase: str
    cursor_json: str
    checkpoint: TopologyCaptureCheckpoint


@dataclass(frozen=True, slots=True)
class DiscoveryJournalTopologyFact:
    frame: int
    epoch: int
    item: JournalTopologyFact


@dataclass(frozen=True, slots=True)
class PreparedDiscoveryJournalTopology:
    key: ArchiveTaskKey
    expected: DiscoveryJournalTopologyProgress
    progress: DiscoveryJournalTopologyProgress
    frame: JournalFramePosition | None
    boundary: JournalEpochBoundary | None
    facts: tuple[JournalTopologyFact, ...]
    item_budget: int
    octet_budget: int


class DiscoveryJournalTopologyWork:
    """One original epoch owner and at most one bounded, detached prepared atom."""

    def __init__(self, original: epochs.DiscoveryJournalEpochWork) -> None:
        self.epochs = original
        self.binding_json = original.binding_json
        self._prepared: PreparedDiscoveryJournalTopology | None = None


def _natural(value: object) -> int:
    try:
        return parse_scalar("nonnegative", value)
    except (ValueError, TypeError):
        raise StaleArchiveTask("journal topology counter is not canonical") from None


def _key(key: ArchiveTaskKey, work: DiscoveryJournalTopologyWork) -> tuple[str, str, str]:
    return key.build_id, key.admission_sha256, work.epochs.source.binding.journal_id


def _epochs(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalTopologyWork
) -> epochs.DiscoveryJournalEpochProgress:
    progress = epochs.discovery_journal_epoch_progress(db, key, work.epochs)
    if progress.phase not in {"complete", "deferred"}:
        raise StaleArchiveTask("journal topology requires terminal original epoch preparation")
    return progress


def _fact(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalTopologyWork, ordinal: int
) -> DiscoveryJournalTopologyFact:
    row = _one(
        db,
        "SELECT frame_ordinal,epoch,array_kind,item_ordinal,byte_offset,json_bytes,"
        "json_sha256,assertion_id,kind,source_id,target_id "
        "FROM discovery_journal_topology_facts "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND ordinal=?",
        (*_key(key, work), format_scalar("nonnegative", ordinal)),
    )
    if row is None:
        raise StaleArchiveTask("journal topology lost its published original fact")
    try:
        frame, epoch = _natural(row[0]), _natural(row[1])
        item = JournalTopologyFact(
            row[2],
            _natural(row[3]),
            _natural(row[4]),
            _natural(row[5]),
            validate_checkpoint_digest(row[6]),
            row[7],
            row[8],
            row[9],
            row[10],
        )
        if (
            item.array not in {"retracts", "relations"}
            or item.kind
            not in (
                {"retraction"} if item.array == "retracts" else {"derivation", "specialization"}
            )
            or not item.json_bytes
            or type(item.assertion_id) is not str
            or item.kind == "retraction"
            and (item.source_id is not None or item.target_id is not None)
            or item.kind != "retraction"
            and (type(item.source_id) is not str or type(item.target_id) is not str)
        ):
            raise ValueError("invalid topology fact controls")
    except (ValueError, TypeError):
        raise StaleArchiveTask("journal topology fact controls changed") from None
    position, boundary = epochs.discovery_journal_epoch_frame(db, key, work.epochs, frame)
    if boundary.epoch != epoch or item.byte_offset + item.json_bytes > position.json_bytes:
        raise StaleArchiveTask("journal topology fact exceeds its original epoch/frame")
    return DiscoveryJournalTopologyFact(frame, epoch, item)


def discovery_journal_topology_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalTopologyWork
) -> DiscoveryJournalTopologyProgress:
    original = _epochs(db, key, work)
    row = _one(
        db,
        "SELECT worker_id,directory_id,algorithm,binding_json,epoch_version,"
        "version,next_frame,phase,cursor_json FROM discovery_journal_topology "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
        _key(key, work),
    )
    if row is None or row[:5] != (
        key.worker_id,
        work.epochs.source.binding.directory_id,
        TOPOLOGY_CAPTURE_ALGORITHM,
        work.binding_json,
        format_scalar("nonnegative", original.version),
    ):
        raise StaleArchiveTask("journal topology checkpoint has another original binding")
    try:
        checkpoint = TopologyCaptureCheckpoint.from_json(row[8])
    except RuntimeError:
        raise StaleArchiveTask("journal topology checkpoint controls changed") from None
    progress = DiscoveryJournalTopologyProgress(
        _natural(row[5]), _natural(row[6]), row[7], row[8], checkpoint
    )
    active = checkpoint.active
    if (
        progress.phase not in {"scanning", "complete", "deferred"}
        or not 0 <= progress.next_frame <= original.next_frame
        or progress.version < progress.next_frame
        or progress.phase == "complete"
        and (original.phase != "complete" or progress.next_frame != original.next_frame or active)
        or progress.phase == "scanning"
        and progress.next_frame == original.next_frame
        and original.next_frame > 0
    ):
        raise StaleArchiveTask("journal topology checkpoint extent changed")
    if active is not None:
        if progress.next_frame >= original.next_frame:
            raise StaleArchiveTask("journal topology active frame exceeds the original directory")
        position, boundary = epochs.discovery_journal_epoch_frame(
            db, key, work.epochs, progress.next_frame
        )
        if (
            active.json_sha256 != boundary.json_sha256
            or all(array.index == array.count for array in (active.retracts, active.relations))
            or any(array.end > position.json_bytes for array in (active.retracts, active.relations))
        ):
            raise StaleArchiveTask("journal topology active cursor differs from its original frame")
    if checkpoint.facts:
        last = _fact(db, key, work, checkpoint.facts - 1)
        item = last.item
        if (
            checkpoint.last != (last.frame, item.array, item.index, item.json_sha256)
            or last.frame > progress.next_frame
            or last.frame == progress.next_frame
            and active is None
            or last.frame == progress.next_frame
            and active is not None
            and item.index >= getattr(active, item.array).index
        ):
            raise StaleArchiveTask("journal topology checkpoint lost its last published fact")
    return progress


def begin_discovery_journal_topology(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalTopologyWork
) -> DiscoveryJournalTopologyProgress:
    if not db.in_transaction:
        raise ValueError("journal topology initialization requires an owner transaction")
    original = _epochs(db, key, work)
    db.execute(
        "INSERT OR IGNORE INTO discovery_journal_topology VALUES("
        "?,?,?,?,?,?,?,?, '0','0','scanning',?)",
        (
            *_key(key, work),
            key.worker_id,
            work.epochs.source.binding.directory_id,
            TOPOLOGY_CAPTURE_ALGORITHM,
            work.binding_json,
            format_scalar("nonnegative", original.version),
            TopologyCaptureCheckpoint(None, 0, None).to_json(),
        ),
    ).close()
    return discovery_journal_topology_progress(db, key, work)


class _SourceChanged(Exception):
    pass


def prepare_discovery_journal_topology(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalTopologyWork,
    *,
    item_budget: int = 64,
    octet_budget: int = 65536,
) -> PreparedDiscoveryJournalTopology | None:
    if db.in_transaction:
        raise ValueError("journal topology preparation requires no write transaction")
    if (
        type(item_budget) is not int
        or not 1 <= item_budget <= 64
        or type(octet_budget) is not int
        or not 1 <= octet_budget <= 65536
    ):
        raise ValueError("invalid journal topology work budget")
    progress = discovery_journal_topology_progress(db, key, work)
    if progress.phase != "scanning":
        return None
    if (
        work._prepared is not None
        and work._prepared.key == key
        and work._prepared.expected == progress
        and work._prepared.item_budget == item_budget
        and work._prepared.octet_budget == octet_budget
    ):
        return work._prepared
    original = _epochs(db, key, work)
    frame = boundary = None
    facts: tuple[JournalTopologyFact, ...] = ()
    next_frame, checkpoint, phase = progress.next_frame, progress.checkpoint, "deferred"
    if next_frame < original.next_frame:
        frame, boundary = epochs.discovery_journal_epoch_frame(db, key, work.epochs, next_frame)
        cursor = checkpoint.active
        if cursor is None:
            raw = work.epochs.source.read_exact(frame.byte_offset + 1, frame.json_bytes)
            try:
                cursor = plan_topology_frame(raw)
            except (ValueError, TypeError):
                pass
            else:
                if cursor.json_sha256 != boundary.json_sha256:
                    raise StaleArchiveTask("topology plan differs from its original frame")

        def read(offset: int, maximum: int) -> bytes:
            if offset < 0 or maximum < 0 or offset + maximum > frame.json_bytes:
                raise StaleArchiveTask("journal topology item exceeds its original frame")
            try:
                return work.epochs.source.read_window(frame.byte_offset + 1 + offset, maximum)
            except Exception as error:
                raise _SourceChanged from error

        if cursor is not None:
            try:
                result = advance_topology_frame(
                    cursor, read, item_budget=item_budget, octet_budget=octet_budget
                )
            except _SourceChanged as error:
                assert error.__cause__ is not None
                raise error.__cause__ from None
            except (ValueError, TypeError):
                pass
            else:
                facts = result.facts
                last = checkpoint.last
                if facts:
                    item = facts[-1]
                    last = (next_frame, item.array, item.index, item.json_sha256)
                next_frame += int(result.complete)
                checkpoint = TopologyCaptureCheckpoint(
                    None if result.complete else result.cursor,
                    checkpoint.facts + len(facts),
                    last,
                )
                phase = "scanning" if next_frame < original.next_frame else original.phase
    updated = DiscoveryJournalTopologyProgress(
        progress.version + 1, next_frame, phase, checkpoint.to_json(), checkpoint
    )
    prepared = PreparedDiscoveryJournalTopology(
        key, progress, updated, frame, boundary, facts, item_budget, octet_budget
    )
    work._prepared = prepared
    return prepared


def apply_discovery_journal_topology(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalTopologyWork,
    prepared: PreparedDiscoveryJournalTopology,
) -> bool:
    if not db.in_transaction:
        raise ValueError("journal topology publication requires an owner transaction")
    if prepared is not work._prepared or prepared.key != key:
        raise StaleArchiveTask("journal topology publication has no owned prepared atom")
    if discovery_journal_topology_progress(db, key, work) != prepared.expected:
        return False
    frame, boundary = prepared.frame, prepared.boundary
    for index, item in enumerate(prepared.facts):
        assert frame is not None and boundary is not None
        db.execute(
            "INSERT INTO discovery_journal_topology_facts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                *_key(key, work),
                format_scalar("nonnegative", prepared.expected.checkpoint.facts + index),
                format_scalar("nonnegative", frame.ordinal),
                format_scalar("nonnegative", boundary.epoch),
                item.array,
                format_scalar("nonnegative", item.index),
                format_scalar("nonnegative", item.byte_offset),
                format_scalar("nonnegative", item.json_bytes),
                item.json_sha256,
                item.assertion_id,
                item.kind,
                item.source_id,
                item.target_id,
            ),
        ).close()
    updated, expected = prepared.progress, prepared.expected
    with closing(
        db.execute(
            "UPDATE discovery_journal_topology SET version=?,next_frame=?,phase=?,cursor_json=? "
            "WHERE build_id=? AND admission_sha256=? AND journal_id=? "
            "AND version=? AND next_frame=? AND phase=? AND cursor_json=?",
            (
                format_scalar("nonnegative", updated.version),
                format_scalar("nonnegative", updated.next_frame),
                updated.phase,
                updated.cursor_json,
                *_key(key, work),
                format_scalar("nonnegative", expected.version),
                format_scalar("nonnegative", expected.next_frame),
                expected.phase,
                expected.cursor_json,
            ),
        )
    ) as result:
        if result.rowcount != 1:
            raise StaleArchiveTask("journal topology publication lost its exact checkpoint")
    work._prepared = None
    return True


declare_growth_for_journal_topology(globals())
