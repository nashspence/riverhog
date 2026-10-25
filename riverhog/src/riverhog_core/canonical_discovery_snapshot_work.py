"""Bind shared history selections to exact admitted Native prefix headers.

Headers and root-set incidence preserve original selection without a member by
snapshot assertion expansion. Native corpus/history semantics, current access and
generation publication retain their independent validation terminals.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from typing import Any

from riverhog_archive_contracts import (
    MEMBER_HISTORY_ROOTS_SCHEMA,
    HistoryJournalAnchor,
    MemberHistoryRoot,
)
from riverhog_canonical_json import (
    canonical_json_bytes,
    format_scalar,
    parse_scalar,
    require_canonical_json,
)
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
class HistorySnapshotProgress:
    expected_roots: int
    registered_roots: int

    @property
    def complete(self) -> bool:
        return self.registered_roots == self.expected_roots


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
        CREATE TABLE IF NOT EXISTS discovery_history_snapshot_coverage (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, root_set_sha256 TEXT NOT NULL,
            worker_id TEXT NOT NULL, authority_json TEXT NOT NULL, registered_roots TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,root_set_sha256)
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


def _root_set(
    db: sqlite3.Connection, key: ArchiveTaskKey, history: HistorySetWork
) -> HistorySetProgress:
    if (
        key.admission_sha256 != history.admission.identity
        or history.authority.schema_id != MEMBER_HISTORY_ROOTS_SCHEMA
        or canonical_json_bytes(history.authority.to_mapping()).decode() != history.authority_json
    ):
        raise StaleArchiveTask("snapshot selection has another archive or set authority")
    progress = history_set_progress(db, key, history)
    if not progress.complete or progress.records != history.authority.record_count:
        raise StaleArchiveTask("snapshot root set is not completely verified")
    return progress


def _selection(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySnapshotWork
) -> HistorySetProgress:
    return _root_set(db, key, owner.history)


def _coverage(
    db: sqlite3.Connection, key: ArchiveTaskKey, history: HistorySetWork
) -> tuple[HistorySnapshotProgress, bool]:
    row = _one(
        db,
        "SELECT worker_id,authority_json,registered_roots "
        "FROM discovery_history_snapshot_coverage "
        "WHERE build_id=? AND admission_sha256=? AND root_set_sha256=?",
        (key.build_id, key.admission_sha256, history.authority.records_sha256),
    )
    if row is None:
        return HistorySnapshotProgress(history.authority.record_count, 0), False
    if row[:2] != (key.worker_id, history.authority_json) or type(row[2]) is not str:
        raise StaleArchiveTask("snapshot coverage has another worker or root authority")
    try:
        registered = parse_scalar("nonnegative", row[2])
    except (ValueError, TypeError):
        raise StaleArchiveTask("snapshot coverage counter is not canonical") from None
    if registered > history.authority.record_count:
        raise StaleArchiveTask("snapshot coverage exceeds its authenticated root count")
    return HistorySnapshotProgress(history.authority.record_count, registered), True


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-history-snapshot-coverage.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_snapshot_work.history_snapshot_progress",),
        scope="one authenticated root-set snapshot incidence completion check",
        starts_at="receive the exact current archive, worker and authenticated root-set authority",
        ends_at="close all Core cursors and detach required versus uniquely registered root counts",
        dimensions=(
            Dimension(
                "registered_records", COUNT, "All retained archive, set and coverage headers."
            ),
            Dimension(
                "metadata_octets", OCTETS, "Largest complete selected or incoming metadata row."
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=3, unit=COUNT),
                "One archive fence, one authenticated set header and one exact coverage lookup.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=3, unit=COUNT),
                "Each indexed query returns at most one original metadata row.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=16, unit=()),
                        Expression("dimension", name="metadata_octets"),
                    ),
                ),
                "Complete bound keys are charged; no retained incidence or payload scan.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=8, unit=()),
                        Expression("dimension", name="metadata_octets"),
                    ),
                ),
                "All returned header fields are charged.",
            ),
            Bound(
                "encoded_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=2, unit=()),
                        Expression("dimension", name="metadata_octets"),
                    ),
                ),
                "The complete incoming authority is compared to its original canonical bytes.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=4096, unit=COUNT),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=512, unit=COUNT),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("dimension", name="registered_records"),
                                                Expression("constant", value=2, unit=COUNT),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Qualified SQLite B-tree point work; unrelated rows are never visited.",
            ),
        ),
        conditions=(
            "The owner retains current catalog/build authority and exclusive workspace custody.",
            "The maintained workspace schema has no added triggers or indexes.",
            "Snapshot registration increments coverage only with a new authenticated incidence.",
        ),
        invariants=(
            "The expected count comes from the fully verified original structural root set.",
            "A shared snapshot header cannot substitute for a missing root-set incidence.",
            "An exact retry increments no count; incidence and counter changes are atomic.",
            "This operation performs no COUNT, source-byte read, Native call or write.",
        ),
        limitations=(
            "This is one root set's coverage, not complete member/import/dependency closure.",
            "Native validity, source custody and publication retain their separate terminals.",
            "VM constants depend on SQLite; lock waits and elapsed time are observations.",
        ),
    )
)
def history_snapshot_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, history: HistorySetWork
) -> HistorySnapshotProgress:
    if not archive_metadata_progress(db, key).complete:
        raise StaleArchiveTask("snapshot archive source metadata is incomplete")
    _root_set(db, key, history)
    return _coverage(db, key, history)[0]


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
                Expression("constant", value=13, unit=COUNT),
                "Exact source/root lookups, registrations and atomic coverage accounting.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=6, unit=COUNT),
                "Exact selection/record, source/header/incidence and coverage lookups.",
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
            "Source/header/incidence and coverage commit atomically; retries add no counts.",
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
        coverage, coverage_exists = _coverage(db, key, owner.history)
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
            if coverage.complete:
                raise StaleArchiveTask("snapshot coverage already reaches its authenticated count")
            if not coverage_exists:
                db.execute(
                    "INSERT INTO discovery_history_snapshot_coverage VALUES (?,?,?,?,?,'0')",
                    (
                        key.build_id,
                        key.admission_sha256,
                        owner.history.authority.records_sha256,
                        key.worker_id,
                        owner.history.authority_json,
                    ),
                )
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
            with closing(
                db.execute(
                    "UPDATE discovery_history_snapshot_coverage SET registered_roots=? "
                    "WHERE build_id=? AND admission_sha256=? AND root_set_sha256=? "
                    "AND worker_id=? AND authority_json=? AND registered_roots=?",
                    (
                        format_scalar("nonnegative", coverage.registered_roots + 1),
                        key.build_id,
                        key.admission_sha256,
                        owner.history.authority.records_sha256,
                        key.worker_id,
                        owner.history.authority_json,
                        format_scalar("nonnegative", coverage.registered_roots),
                    ),
                )
            ) as changed:
                if changed.rowcount != 1:
                    raise StaleArchiveTask("snapshot coverage changed during registration")
        elif incidence != expected:
            raise StaleArchiveTask("snapshot root incidence has conflicting coordinates")
        elif not coverage_exists or coverage.registered_roots == 0:
            raise StaleArchiveTask("snapshot incidence has no matching coverage")
