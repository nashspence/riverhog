"""Private bounded correction facts over one registered original archive journal."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass

from riverhog_canonical_json import format_scalar, parse_scalar
from riverhog_provenance.epoch_boundaries import (
    JOURNAL_EPOCH_BOUNDARY_ALGORITHM,
    JournalEpochBoundary,
    classify_epoch_boundary,
)
from riverhog_provenance.framing import JournalFramePosition

from riverhog_core import canonical_discovery_archive_framing as framing
from riverhog_core import canonical_discovery_journal_registration as registration
from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_history_inputs import _one
from riverhog_core.canonical_discovery_journal_epochs_growth import (
    declare_growth_for_journal_epochs,
)
from riverhog_core.canonical_discovery_journal_source import DiscoveryJournalSource


@dataclass(frozen=True, slots=True)
class DiscoveryJournalEpochProgress:
    version: int
    next_frame: int
    byte_offset: int
    epoch: int
    phase: str


@dataclass(frozen=True, slots=True)
class PreparedDiscoveryJournalEpoch:
    key: ArchiveTaskKey
    expected: DiscoveryJournalEpochProgress
    position: JournalFramePosition | None
    boundary: JournalEpochBoundary | None
    progress: DiscoveryJournalEpochProgress


class DiscoveryJournalEpochWork:
    """One original binding and at most one entry-bounded detached classification."""

    def __init__(self, original: framing.ArchiveJournalFramingWork, source: DiscoveryJournalSource):
        if source._work is not original:
            raise StaleArchiveTask("journal epoch work has another original framing owner")
        self.framing, self.source = original, source
        self.binding_json = source.binding.to_json()
        self._prepared: PreparedDiscoveryJournalEpoch | None = None


def _natural(value: object) -> int:
    if type(value) is not str:
        raise StaleArchiveTask("journal epoch counter representation changed")
    try:
        return parse_scalar("nonnegative", value)
    except (ValueError, TypeError):
        raise StaleArchiveTask("journal epoch counter is not canonical") from None


def _key(key: ArchiveTaskKey, work: DiscoveryJournalEpochWork) -> tuple[str, str, str]:
    return key.build_id, key.admission_sha256, work.source.binding.journal_id


def _fence(db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalEpochWork) -> None:
    selected = registration.find_discovery_journal(db, key, work.framing)
    if selected is None or selected.binding != work.source.binding:
        raise StaleArchiveTask("journal epoch work lost its registered original source")


def _fact(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalEpochWork, ordinal: int
) -> tuple[JournalFramePosition, JournalEpochBoundary]:
    row = _one(
        db,
        "SELECT byte_offset,json_bytes,epoch,entry_kind,json_sha256 "
        "FROM discovery_journal_epoch_frames "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND ordinal=?",
        (*_key(key, work), format_scalar("nonnegative", ordinal)),
    )
    if row is None:
        raise StaleArchiveTask("journal epoch checkpoint lost its original frame fact")
    position = JournalFramePosition(ordinal, _natural(row[0]), _natural(row[1]))
    boundary = JournalEpochBoundary(ordinal, _natural(row[2]), row[3], row[4])
    if (
        boundary.epoch > ordinal
        or boundary.entry_kind not in {"journal_init", "assertion", "correction", "checkpoint"}
        or type(boundary.json_sha256) is not str
        or len(boundary.json_sha256) != 64
        or any(value not in "0123456789abcdef" for value in boundary.json_sha256)
        or framing.archive_journal_frame(db, key, work.framing, ordinal) != position
    ):
        raise StaleArchiveTask("journal epoch fact differs from its original frame")
    return position, boundary


def discovery_journal_epoch_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalEpochWork
) -> DiscoveryJournalEpochProgress:
    _fence(db, key, work)
    row = _one(
        db,
        "SELECT worker_id,directory_id,algorithm,binding_json,version,next_frame,"
        "byte_offset,epoch,phase FROM discovery_journal_epochs "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
        _key(key, work),
    )
    binding = work.source.binding
    if row is None or row[:4] != (
        key.worker_id,
        binding.directory_id,
        JOURNAL_EPOCH_BOUNDARY_ALGORITHM,
        work.binding_json,
    ):
        raise StaleArchiveTask("journal epoch checkpoint has another original binding")
    progress = DiscoveryJournalEpochProgress(
        _natural(row[4]), _natural(row[5]), _natural(row[6]), _natural(row[7]), row[8]
    )
    if (
        not 0 <= progress.next_frame <= _natural(binding.frame_count)
        or not 0 <= progress.byte_offset <= _natural(binding.source_bytes)
        or not 0 <= progress.epoch <= progress.next_frame
        or progress.phase not in {"scanning", "complete", "deferred"}
        or progress.phase != "deferred"
        and progress.version != progress.next_frame
        or progress.phase == "deferred"
        and progress.version not in {progress.next_frame, progress.next_frame + 1}
        or progress.phase == "scanning"
        and progress.next_frame == _natural(binding.frame_count)
        and progress.next_frame > 0
        or not progress.next_frame
        and (progress.byte_offset or progress.epoch)
    ):
        raise StaleArchiveTask("journal epoch checkpoint extent changed")
    if progress.next_frame:
        position, boundary = _fact(db, key, work, progress.next_frame - 1)
        if (
            position.byte_offset + position.json_bytes + 2 != progress.byte_offset
            or boundary.epoch != progress.epoch
        ):
            raise StaleArchiveTask("journal epoch checkpoint lost its last published frame")
    if progress.phase == "complete":
        original = framing.archive_framing_progress(db, key, work.framing)
        if (
            original.phase != "complete"
            or progress.next_frame != _natural(binding.frame_count)
            or progress.byte_offset != _natural(binding.source_bytes)
        ):
            raise StaleArchiveTask("completed journal epochs differ from their full source")
    return progress


def begin_discovery_journal_epochs(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalEpochWork
) -> DiscoveryJournalEpochProgress:
    if not db.in_transaction:
        raise ValueError("journal epoch initialization requires an owner transaction")
    _fence(db, key, work)
    db.execute(
        "INSERT OR IGNORE INTO discovery_journal_epochs VALUES("
        "?,?,?,?,?,?,?,'0','0','0','0','scanning')",
        (
            *_key(key, work),
            key.worker_id,
            work.source.binding.directory_id,
            JOURNAL_EPOCH_BOUNDARY_ALGORITHM,
            work.binding_json,
        ),
    ).close()
    return discovery_journal_epoch_progress(db, key, work)


def prepare_discovery_journal_epoch(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalEpochWork
) -> PreparedDiscoveryJournalEpoch | None:
    if db.in_transaction:
        raise ValueError("journal epoch preparation requires no write transaction")
    progress = discovery_journal_epoch_progress(db, key, work)
    if progress.phase != "scanning":
        return None
    prepared = work._prepared
    if prepared is not None and prepared.key == key and prepared.expected == progress:
        return prepared
    position = framing.archive_journal_frame(db, key, work.framing, progress.next_frame)
    boundary = None
    phase = "deferred"
    next_frame, offset, epoch = progress.next_frame, progress.byte_offset, progress.epoch
    if position is not None:
        if position.byte_offset != offset:
            raise StaleArchiveTask("journal epoch source position is discontinuous")
        # Source framing owns the separators. Separate exact reads preserve the
        # existing JSON entry limit even when JSON plus separators crosses it.
        if (
            work.source.read_exact(offset, 1) != b"\x1e"
            or work.source.read_exact(offset + position.json_bytes + 1, 1) != b"\n"
        ):
            raise StaleArchiveTask("journal epoch source lost its original frame separators")
        raw = work.source.read_exact(offset + 1, position.json_bytes)
        try:
            boundary = classify_epoch_boundary(raw, ordinal=position.ordinal, previous_epoch=epoch)
        except (ValueError, TypeError):
            # Later speculative findings cannot outrank earlier Native errors.
            pass
        else:
            next_frame += 1
            offset += position.json_bytes + 2
            epoch = boundary.epoch
            phase = "scanning"
            if next_frame == _natural(work.source.binding.frame_count):
                original = framing.archive_framing_progress(db, key, work.framing)
                phase = "complete" if original.phase == "complete" else "deferred"
    updated = DiscoveryJournalEpochProgress(progress.version + 1, next_frame, offset, epoch, phase)
    prepared = PreparedDiscoveryJournalEpoch(key, progress, position, boundary, updated)
    work._prepared = prepared
    return prepared


def apply_discovery_journal_epoch(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalEpochWork,
    prepared: PreparedDiscoveryJournalEpoch,
) -> bool:
    if not db.in_transaction:
        raise ValueError("journal epoch publication requires an owner transaction")
    if prepared is not work._prepared or prepared.key != key:
        raise StaleArchiveTask("journal epoch publication has no owned prepared atom")
    current = discovery_journal_epoch_progress(db, key, work)
    if current != prepared.expected:
        return False
    expected, updated = prepared.expected, prepared.progress
    if prepared.boundary is not None:
        position, boundary = prepared.position, prepared.boundary
        assert position is not None
        db.execute(
            "INSERT INTO discovery_journal_epoch_frames VALUES(?,?,?,?,?,?,?,?,?)",
            (
                *_key(key, work),
                format_scalar("nonnegative", position.ordinal),
                format_scalar("nonnegative", position.byte_offset),
                format_scalar("nonnegative", position.json_bytes),
                format_scalar("nonnegative", boundary.epoch),
                boundary.entry_kind,
                boundary.json_sha256,
            ),
        ).close()
    with closing(
        db.execute(
            "UPDATE discovery_journal_epochs SET version=?,next_frame=?,byte_offset=?,"
            "epoch=?,phase=? "
            "WHERE build_id=? AND admission_sha256=? AND journal_id=? "
            "AND version=? AND next_frame=? AND byte_offset=? AND epoch=? AND phase=?",
            (
                *(
                    format_scalar("nonnegative", value)
                    for value in (
                        updated.version,
                        updated.next_frame,
                        updated.byte_offset,
                        updated.epoch,
                    )
                ),
                updated.phase,
                *_key(key, work),
                *(
                    format_scalar("nonnegative", value)
                    for value in (
                        expected.version,
                        expected.next_frame,
                        expected.byte_offset,
                        expected.epoch,
                    )
                ),
                expected.phase,
            ),
        )
    ) as result:
        if result.rowcount != 1:
            raise StaleArchiveTask("journal epoch publication lost its exact checkpoint")
    work._prepared = None
    return True


declare_growth_for_journal_epochs(globals())
