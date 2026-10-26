"""Persist exact child directory admission before retained Native opening.

A registration records original source identity and confirmed storage opening.
Native retains all journal checkpoints and semantic acceptance authority.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass

from riverhog_canonical_json import format_scalar
from riverhog_provenance import JournalReplayBinding

from riverhog_core import canonical_discovery_archive_framing as framing
from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_history_inputs import _one
from riverhog_core.canonical_discovery_journal_registration_growth import (
    declare_growth_for_journal_registration,
)


@dataclass(frozen=True, slots=True)
class DiscoveryJournalRegistration:
    binding: JournalReplayBinding
    confirmed: bool


def initialize_journal_registration(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("journal registration initialization requires no active transaction")
    db.execute("""
        CREATE TABLE IF NOT EXISTS discovery_journal_children (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, directory_id TEXT NOT NULL, binding_json TEXT NOT NULL,
            confirmed INTEGER NOT NULL CHECK(confirmed IN (0,1)),
            PRIMARY KEY(build_id,admission_sha256,journal_id),
            UNIQUE(build_id,admission_sha256,directory_id)
        ) WITHOUT ROWID
    """).close()


def _key(key: ArchiveTaskKey, work: framing.ArchiveJournalFramingWork) -> tuple[str, str, str]:
    return key.build_id, key.admission_sha256, work.source.identity.journal_id


def _fence(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: framing.ArchiveJournalFramingWork,
) -> framing.ArchiveFramingProgress:
    progress = framing.archive_framing_progress(db, key, work)
    if progress.phase not in {"complete", "deferred"}:
        raise StaleArchiveTask("child registration has no frozen original framing")
    return progress


def _binding_current(
    key: ArchiveTaskKey,
    work: framing.ArchiveJournalFramingWork,
    binding: JournalReplayBinding,
    progress: framing.ArchiveFramingProgress,
) -> None:
    identity, job = work.source.identity, work.source.metadata.job
    if re.fullmatch(r"[0-9a-f]{32}", binding.directory_id) is None:
        raise StaleArchiveTask("child directory identity is outside its exact kernel domain")
    if (
        binding.attempt_id,
        binding.journal_id,
        binding.source_bytes,
        binding.source_sha256,
        binding.source_version,
        binding.frame_count,
        binding.catalog_sha256,
        binding.require_profiles,
    ) != (
        key.worker_id,
        identity.journal_id,
        format_scalar("nonnegative", identity.source_bytes),
        identity.source_sha256,
        format_scalar("nonnegative", progress.version),
        format_scalar("nonnegative", progress.cursor.frame_count),
        job.catalog_sha256,
        False,
    ):
        raise StaleArchiveTask("child registration has another exact original source binding")


def find_discovery_journal(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: framing.ArchiveJournalFramingWork
) -> DiscoveryJournalRegistration | None:
    progress = _fence(db, key, work)
    row = _one(
        db,
        "SELECT worker_id,directory_id,binding_json,confirmed FROM discovery_journal_children "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
        _key(key, work),
    )
    if row is None:
        return None
    try:
        binding = JournalReplayBinding.from_json(row[2])
    except (TypeError, RuntimeError) as error:
        raise StaleArchiveTask("child registration binding is invalid") from error
    _binding_current(key, work, binding, progress)
    if row[0] != key.worker_id or row[1] != binding.directory_id or row[3] not in (0, 1):
        raise StaleArchiveTask("child registration lost its owned directory or disposition")
    return DiscoveryJournalRegistration(binding, bool(row[3]))


def register_discovery_journal(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: framing.ArchiveJournalFramingWork,
    binding: JournalReplayBinding,
) -> DiscoveryJournalRegistration:
    if not db.in_transaction:
        raise ValueError("journal registration requires an owner-managed transaction")
    current = find_discovery_journal(db, key, work)
    _binding_current(key, work, binding, _fence(db, key, work))
    if current is not None:
        if current.binding != binding:
            raise StaleArchiveTask("registered child directory cannot be replaced")
        return current
    try:
        db.execute(
            "INSERT INTO discovery_journal_children VALUES(?,?,?,?,?,?,0)",
            (*_key(key, work), key.worker_id, binding.directory_id, binding.to_json()),
        ).close()
    except sqlite3.IntegrityError as error:
        raise StaleArchiveTask(
            "child directory already belongs to another original journal"
        ) from error
    return DiscoveryJournalRegistration(binding, False)


def confirm_discovery_journal(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: framing.ArchiveJournalFramingWork,
    expected: DiscoveryJournalRegistration,
) -> bool:
    """Record the caller's confirmed Native open without copying Native progress."""
    if not db.in_transaction:
        raise ValueError("journal confirmation requires an owner-managed transaction")
    current = find_discovery_journal(db, key, work)
    if current is None or current.binding != expected.binding:
        return False
    if current.confirmed:
        return True
    with closing(
        db.execute(
            "UPDATE discovery_journal_children SET confirmed=1 "
            "WHERE build_id=? AND admission_sha256=? AND journal_id=? "
            "AND worker_id=? AND directory_id=? AND binding_json=? AND confirmed=0",
            (
                *_key(key, work),
                key.worker_id,
                expected.binding.directory_id,
                expected.binding.to_json(),
            ),
        )
    ) as rows:
        return rows.rowcount == 1


declare_growth_for_journal_registration(globals())
