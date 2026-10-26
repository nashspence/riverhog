"""Coordinate one owned graph-preparation atom or one atomic stage transition."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from types import ModuleType
from typing import Any

from riverhog_canonical_json import format_scalar, parse_scalar

from riverhog_core import canonical_discovery_journal_epochs as epochs
from riverhog_core import canonical_discovery_journal_rank as rank
from riverhog_core import canonical_discovery_journal_topology as topology
from riverhog_core import canonical_discovery_journal_union as union
from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_history_inputs import _one
from riverhog_core.canonical_discovery_journal_preparation_growth import (
    declare_growth_for_journal_preparation,
)

_ALGORITHM = "riverhog-discovery-journal-preparation/v1"
_PHASES = ("epochs", "topology", "union", "rank", "complete")


@dataclass(frozen=True, slots=True)
class DiscoveryJournalPreparationProgress:
    version: int
    phase: str
    child_version: int
    child_complete: bool

    @property
    def complete(self) -> bool:
        return self.phase == "complete"


@dataclass(frozen=True, slots=True)
class PreparedDiscoveryJournalPreparation:
    key: ArchiveTaskKey
    expected: DiscoveryJournalPreparationProgress
    child: Any = None
    work_items: int = 0


@dataclass(frozen=True, slots=True)
class _Stage:
    name: str
    table: str
    stem: str
    module: ModuleType
    work: Any

    def call(self, operation: str, db: sqlite3.Connection, key: ArchiveTaskKey, *args: Any) -> Any:
        name = (
            self.stem + "_progress"
            if operation == "progress"
            else operation
            + "_"
            + self.stem
            + ("s" if operation == "begin" and self.name == "epochs" else "")
        )
        return getattr(self.module, name)(db, key, self.work, *args)

    def terminal(self, progress: Any) -> bool:
        value = progress.phase if self.name in {"epochs", "topology"} else progress.state
        return value in {"complete", "deferred"}


class DiscoveryJournalPreparationWork:
    """One original source with fixed child owners and one unpublished bounded atom."""

    def __init__(self, original: epochs.DiscoveryJournalEpochWork) -> None:
        self.epochs = original
        self.topology = topology.DiscoveryJournalTopologyWork(original)
        self.union = union.DiscoveryJournalUnionWork(self.topology)
        self.rank = rank.DiscoveryJournalRankWork(self.union)
        self.binding_json = original.binding_json
        self._prepared: PreparedDiscoveryJournalPreparation | None = None

    def stage(self, phase: str) -> _Stage:
        values: dict[str, tuple[ModuleType, Any, str, str]] = {
            "epochs": (epochs, self.epochs, "discovery_journal_epoch", "discovery_journal_epochs"),
            "topology": (
                topology,
                self.topology,
                "discovery_journal_topology",
                "discovery_journal_topology",
            ),
            "union": (union, self.union, "discovery_journal_union", "discovery_journal_union"),
            "rank": (rank, self.rank, "discovery_journal_rank", "discovery_journal_rank"),
        }
        selected = "rank" if phase == "complete" else phase
        module, work, stem, table = values[selected]
        return _Stage(selected, table, stem, module, work)


def _natural(value: object) -> int:
    try:
        return parse_scalar("nonnegative", value)
    except (TypeError, ValueError):
        raise StaleArchiveTask("journal preparation control is not canonical") from None


def _key(key: ArchiveTaskKey, work: DiscoveryJournalPreparationWork) -> tuple[str, str, str]:
    return key.build_id, key.admission_sha256, work.epochs.source.binding.journal_id


def _row(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalPreparationWork
) -> tuple[Any, ...] | None:
    row = _one(
        db,
        "SELECT worker_id,directory_id,binding_json,algorithm,version,phase,child_version "
        "FROM discovery_journal_preparation WHERE build_id=? AND admission_sha256=? "
        "AND journal_id=?",
        _key(key, work),
    )
    return None if row is None else tuple(row)


def _exists(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalPreparationWork,
    stage: _Stage,
) -> bool:
    return (
        _one(
            db,
            "SELECT 1 FROM "
            + stage.table
            + " WHERE build_id=? AND admission_sha256=? AND journal_id=?",
            _key(key, work),
        )
        is not None
    )


def discovery_journal_preparation_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalPreparationWork
) -> DiscoveryJournalPreparationProgress:
    row = _row(db, key, work)
    if row is None or row[:4] != (
        key.worker_id,
        work.epochs.source.binding.directory_id,
        work.binding_json,
        _ALGORITHM,
    ):
        raise StaleArchiveTask("journal preparation has another original source or attempt")
    if row[5] not in _PHASES:
        raise StaleArchiveTask("journal preparation stage changed")
    version, phase, child_version = _natural(row[4]), row[5], _natural(row[6])
    stage = work.stage(phase)
    child = stage.call("progress", db, key)
    complete = stage.terminal(child)
    if (
        version < _PHASES.index(phase)
        or child.version != child_version
        or (phase == "complete" and not complete)
    ):
        raise StaleArchiveTask("journal preparation lost its exact initialized child cursor")
    return DiscoveryJournalPreparationProgress(version, phase, child_version, complete)


def begin_discovery_journal_preparation(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalPreparationWork
) -> DiscoveryJournalPreparationProgress:
    if not db.in_transaction:
        raise ValueError("journal preparation initialization requires an owner transaction")
    if _row(db, key, work) is None:
        stage = work.stage("epochs")
        if _exists(db, key, work, stage):
            raise StaleArchiveTask("journal preparation initial child already exists")
        child = stage.call("begin", db, key)
        db.execute(
            "INSERT INTO discovery_journal_preparation VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                *_key(key, work),
                key.worker_id,
                work.epochs.source.binding.directory_id,
                work.binding_json,
                _ALGORITHM,
                "0",
                "epochs",
                format_scalar("nonnegative", child.version),
            ),
        ).close()
    return discovery_journal_preparation_progress(db, key, work)


def prepare_discovery_journal_preparation(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: DiscoveryJournalPreparationWork
) -> PreparedDiscoveryJournalPreparation | None:
    if db.in_transaction:
        raise ValueError("journal preparation requires no write transaction")
    progress = discovery_journal_preparation_progress(db, key, work)
    if progress.complete:
        return None
    cached = work._prepared
    if cached is not None and cached.key == key and cached.expected == progress:
        return cached
    child, items = None, 0
    if not progress.child_complete:
        child = work.stage(progress.phase).call("prepare", db, key)
        if child is None:
            raise StaleArchiveTask("journal preparation lost an unfinished child atom")
        items = (
            1
            if progress.phase == "epochs"
            else len(child.facts)
            if progress.phase == "topology"
            else child.work_items
        )
    prepared = PreparedDiscoveryJournalPreparation(key, progress, child, items)
    work._prepared = prepared
    return prepared


def apply_discovery_journal_preparation(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: DiscoveryJournalPreparationWork,
    prepared: PreparedDiscoveryJournalPreparation,
) -> bool:
    if not db.in_transaction:
        raise ValueError("journal preparation publication requires an owner transaction")
    if prepared is not work._prepared or prepared.key != key:
        raise StaleArchiveTask("journal preparation has no owned publication atom")
    before = discovery_journal_preparation_progress(db, key, work)
    if before != prepared.expected:
        return False
    phase, child_version = before.phase, before.child_version
    try:
        if prepared.child is not None:
            if not work.stage(phase).call("apply", db, key, prepared.child):
                raise StaleArchiveTask("journal preparation child publication became stale")
            child_version = prepared.child.progress.version
        else:
            if not before.child_complete or before.complete:
                raise StaleArchiveTask("journal preparation transition has no terminal child")
            phase = _PHASES[_PHASES.index(phase) + 1]
            if phase != "complete":
                stage = work.stage(phase)
                if _exists(db, key, work, stage):
                    raise StaleArchiveTask("journal preparation next child already exists")
                child_version = stage.call("begin", db, key).version
        with closing(
            db.execute(
                "UPDATE discovery_journal_preparation SET version=?,phase=?,child_version=? "
                "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND version=? "
                "AND phase=? AND child_version=?",
                (
                    format_scalar("nonnegative", before.version + 1),
                    phase,
                    format_scalar("nonnegative", child_version),
                    *_key(key, work),
                    format_scalar("nonnegative", before.version),
                    before.phase,
                    format_scalar("nonnegative", before.child_version),
                ),
            )
        ) as changed:
            if changed.rowcount != 1:
                raise StaleArchiveTask("journal preparation lost its exact publication cursor")
    except BaseException:
        # A child may already have consumed its in-memory token before the
        # enclosing transaction rolls back. Reprepare from retained controls.
        work._prepared = None
        raise
    work._prepared = None
    return True


declare_growth_for_journal_preparation(globals())
