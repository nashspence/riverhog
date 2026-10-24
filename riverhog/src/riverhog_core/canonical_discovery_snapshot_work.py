"""Bind shared history selections to exact admitted Native prefix headers.

Headers and root-set incidence preserve original selection without a member by
snapshot assertion expansion. Native corpus/history semantics, current access and
generation publication retain their independent validation terminals.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from riverhog_archive_contracts import (
    MEMBER_HISTORY_ROOTS_SCHEMA,
    HistoryJournalAnchor,
    MemberHistoryRoot,
)
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from riverhog_core.canonical_discovery_archive_work import (
    ArchiveTaskKey,
    StaleArchiveTask,
    archive_metadata_progress,
)
from riverhog_core.canonical_discovery_history_work import (
    HistorySetProgress,
    HistorySetWork,
    history_set_progress,
)
from riverhog_core.canonical_discovery_journal_work import QualifiedJournalSource


@dataclass(frozen=True, slots=True)
class PreparedHistorySnapshot:
    key: ArchiveTaskKey
    expected_set: HistorySetProgress
    record_key: str
    record_json: str
    source_key: str
    source_binding_json: str
    prefix_sha256: str
    anchor_json: str
    inclusion: str


class HistorySnapshotWork:
    def __init__(self, history: HistorySetWork, source: QualifiedJournalSource) -> None:
        self.history = history
        self.source = source
        self.source_key = source.source_key
        self.source_binding_json = source.binding_json
        binding = require_canonical_json(self.source_binding_json.encode())
        if (
            not isinstance(binding, dict)
            or set(binding) != {"admission_sha256", "anchor", "domain"}
            or binding["domain"] != "riverhog-discovery-journal-source/v1"
            or binding["admission_sha256"] != history.admission.identity
            or hashlib.sha256(self.source_binding_json.encode()).hexdigest() != self.source_key
        ):
            raise StaleArchiveTask("snapshot Native source has another exact archive binding")
        self.source_anchor = HistoryJournalAnchor.from_mapping(binding["anchor"])
        self._prepared: PreparedHistorySnapshot | None = None

    def check_source(self) -> None:
        if (
            self.source.source_key != self.source_key
            or self.source.binding_json != self.source_binding_json
        ):
            raise StaleArchiveTask("snapshot Native source binding changed")


def initialize_snapshot_work(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("snapshot initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_snapshot_sources (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, source_key TEXT NOT NULL,
            binding_json TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,source_key)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_snapshot_headers (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, source_key TEXT NOT NULL,
            prefix_sha256 TEXT NOT NULL, anchor_json TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,source_key,prefix_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_history_snapshot_links (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, root_set_sha256 TEXT NOT NULL,
            record_key TEXT NOT NULL, source_key TEXT NOT NULL, prefix_sha256 TEXT NOT NULL,
            inclusion TEXT NOT NULL CHECK(inclusion IN ('bound','retained')),
            PRIMARY KEY(build_id,admission_sha256,root_set_sha256,record_key)
        ) WITHOUT ROWID;
    """)


def _one(db: sqlite3.Connection, sql: str, parameters: tuple[object, ...]) -> Any:
    cursor = db.execute(sql, parameters)
    try:
        return cursor.fetchone()
    finally:
        cursor.close()


@contextmanager
def _atomic(db: sqlite3.Connection) -> Iterator[None]:
    if not db.in_transaction:
        raise ValueError("snapshot work requires an owner-managed transaction")
    db.execute("SAVEPOINT discovery_snapshot")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_snapshot")
        db.execute("RELEASE discovery_snapshot")
        raise
    else:
        db.execute("RELEASE discovery_snapshot")


def _selection(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySnapshotWork
) -> HistorySetProgress:
    if (
        key.admission_sha256 != owner.history.admission.identity
        or owner.history.authority.schema_id != MEMBER_HISTORY_ROOTS_SCHEMA
    ):
        raise StaleArchiveTask("snapshot selection has another archive or set schema")
    progress = history_set_progress(db, key, owner.history)
    if not progress.complete:
        raise StaleArchiveTask("snapshot root set is not completely verified")
    return progress


def _record(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySnapshotWork, record_key: str
) -> str:
    row = _one(
        db,
        "SELECT value_json FROM discovery_history_set_records "
        "WHERE build_id=? AND admission_sha256=? AND records_sha256=? AND record_key=?",
        (key.build_id, key.admission_sha256, owner.history.authority.records_sha256, record_key),
    )
    if row is None:
        raise StaleArchiveTask("snapshot root record is not registered")
    value: object = row[0]
    if not isinstance(value, str):
        raise StaleArchiveTask("snapshot root record encoding changed")
    return value


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-history-snapshot-prepare.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_snapshot_work.prepare_history_snapshot",),
        scope="one exact history root to Native source prefix binding preparation",
        starts_at="receive one verified root-set key and complete archive-admitted Native source",
        ends_at="detach the exact matching Native anchor and original root-set incidence",
        dimensions=(Dimension("registered_snapshots", COUNT, "All retained snapshot headers."),),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=4, unit=COUNT),
                "Archive/set fences, one root record and the exact complete journal header.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=4, unit=COUNT),
                "Each exact Core workspace query returns at most one row.",
            ),
            Bound(
                "native_anchor_calls",
                COUNT,
                "equal",
                Expression("constant", value=2, unit=COUNT),
                "The complete source lease fence and selected prefix use Native point headers.",
            ),
        ),
        conditions=(
            "The caller retains current archive/build authority and the exact workspace lease.",
            "Preparation runs outside the owner write transaction.",
            "The source was bound to the archive's exact complete journal terminal.",
        ),
        invariants=(
            "Journal, entry, sequence, hash, prefix SHA and bytes match the exact root.",
            "No entry body, source prefix or effective snapshot graph is replayed or materialized.",
            "Different source admissions never deduplicate merely by journal UUID or prefix hash.",
        ),
        limitations=(
            "SQL counts cover the Core workspace only; Native owns indexed anchor/padding work.",
            "Native corpus semantics, import closure and current authorization remain separate.",
        ),
    )
)
def prepare_history_snapshot(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySnapshotWork, record_key: str
) -> PreparedHistorySnapshot:
    if db.in_transaction:
        raise ValueError("snapshot preparation requires no active transaction")
    if not archive_metadata_progress(db, key).complete:
        raise StaleArchiveTask("snapshot archive metadata is incomplete")
    progress = _selection(db, key, owner)
    raw = _record(db, key, owner, record_key)
    selected = MemberHistoryRoot.from_mapping(require_canonical_json(raw.encode()))
    if selected.key != record_key:
        raise StaleArchiveTask("snapshot root record key changed")
    owner.check_source()
    complete_source = _one(
        db,
        "SELECT source_bytes,source_sha256 FROM discovery_archive_journals "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
        (key.build_id, key.admission_sha256, owner.source_anchor.journal_id),
    )
    if complete_source != (
        str(owner.source_anchor.prefix_bytes),
        owner.source_anchor.prefix_sha256,
    ):
        raise StaleArchiveTask("snapshot Native prefix is not the complete archived source")
    anchor = owner.source.anchor_at(selected.journal.through_sequence)
    if anchor != selected.journal.to_mapping():
        raise StaleArchiveTask("history snapshot differs from the exact admitted Native anchor")
    page = PreparedHistorySnapshot(
        key,
        progress,
        record_key,
        raw,
        owner.source_key,
        owner.source_binding_json,
        selected.journal.prefix_sha256,
        canonical_json_bytes(anchor).decode(),
        selected.inclusion,
    )
    owner._prepared = page
    return page


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-history-snapshot-apply.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_snapshot_work.apply_history_snapshot",),
        scope="one exact history root to Native source prefix binding commit",
        starts_at="receive the exact detached anchor and current owner transaction",
        ends_at="commit a shared source/header with its original root-set incidence",
        dimensions=(Dimension("registered_snapshots", COUNT, "All retained snapshot headers."),),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("constant", value=10, unit=COUNT),
                "Two selection reads and three exact keyed registrations in a savepoint.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=5, unit=COUNT),
                "Exact selection/record and existing source/header/incidence lookups.",
            ),
            Bound(
                "native_anchor_calls",
                COUNT,
                "equal",
                Expression("constant", value=1, unit=COUNT),
                "The frozen Native source lease retains its exact terminal fence.",
            ),
            Bound(
                "source_hash_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No journal payload, assertion or source-prefix octets are hashed in apply.",
            ),
        ),
        conditions=(
            "The caller retains current archive/build authority and the exact workspace lease.",
            "Apply receives the exact preparation; the schema has no added triggers.",
        ),
        invariants=(
            "Source/header/incidence registration is atomic and exact retries do not add copies.",
            "Existing keys compare full original coordinates; a digest alone is not equality.",
            "A closed/replaced source or changed selection cannot register a snapshot header.",
        ),
        limitations=(
            "SQL counts cover the Core workspace only; Native owns indexed anchor/padding work.",
            "Storage-engine/index costs, corpus semantics and publication retain separate owners.",
        ),
    )
)
def apply_history_snapshot(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: HistorySnapshotWork,
    page: PreparedHistorySnapshot,
) -> None:
    if owner._prepared is not page or page.key != key:
        raise StaleArchiveTask("snapshot is not the current exact detached preparation")
    owner.check_source()
    owner.source.check_open()
    with _atomic(db):
        if _selection(db, key, owner) != page.expected_set:
            raise StaleArchiveTask("snapshot root-set version or continuation changed")
        if _record(db, key, owner, page.record_key) != page.record_json:
            raise StaleArchiveTask("snapshot root record changed")
        source = _one(
            db,
            "SELECT binding_json FROM discovery_snapshot_sources "
            "WHERE build_id=? AND admission_sha256=? AND source_key=?",
            (key.build_id, key.admission_sha256, page.source_key),
        )
        if source is None:
            db.execute(
                "INSERT INTO discovery_snapshot_sources VALUES (?,?,?,?)",
                (key.build_id, key.admission_sha256, page.source_key, page.source_binding_json),
            )
        elif source != (page.source_binding_json,):
            raise StaleArchiveTask("snapshot source identity has conflicting coordinates")
        header = _one(
            db,
            "SELECT anchor_json FROM discovery_snapshot_headers "
            "WHERE build_id=? AND admission_sha256=? AND source_key=? AND prefix_sha256=?",
            (key.build_id, key.admission_sha256, page.source_key, page.prefix_sha256),
        )
        if header is None:
            db.execute(
                "INSERT INTO discovery_snapshot_headers VALUES (?,?,?,?,?)",
                (
                    key.build_id,
                    key.admission_sha256,
                    page.source_key,
                    page.prefix_sha256,
                    page.anchor_json,
                ),
            )
        elif header != (page.anchor_json,):
            raise StaleArchiveTask("snapshot prefix identity has conflicting coordinates")
        expected = page.source_key, page.prefix_sha256, page.inclusion
        incidence = _one(
            db,
            "SELECT source_key,prefix_sha256,inclusion FROM discovery_history_snapshot_links "
            "WHERE build_id=? AND admission_sha256=? AND root_set_sha256=? AND record_key=?",
            (
                key.build_id,
                key.admission_sha256,
                owner.history.authority.records_sha256,
                page.record_key,
            ),
        )
        if incidence is None:
            db.execute(
                "INSERT INTO discovery_history_snapshot_links VALUES (?,?,?,?,?,?,?)",
                (
                    key.build_id,
                    key.admission_sha256,
                    owner.history.authority.records_sha256,
                    page.record_key,
                    *expected,
                ),
            )
        elif incidence != expected:
            raise StaleArchiveTask("snapshot root incidence has conflicting coordinates")
