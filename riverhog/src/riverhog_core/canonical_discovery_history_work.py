"""Root-selected history descriptors and shared resumable structural set checks.

These terminals establish structural coverage. Native source-union semantics,
import membership proofs, current permissions and publication have separate owners.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from riverhog_archive_contracts import (
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    MemberHistoryBinding,
    MemberHistoryDocument,
    MemberHistoryImport,
    MemberHistoryRoot,
    RecordPage,
    RecordSetCheckpoint,
    RecordSetRef,
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
    ArchiveDiscoveryAdmission,
    ArchiveTaskKey,
    StaleArchiveTask,
    archive_metadata_progress,
)
from riverhog_core.canonical_discovery_member_work import archive_member_progress
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader


@dataclass(frozen=True, slots=True)
class HistorySetProgress:
    version: int
    page_ordinal: int
    item_index: int
    records: int
    complete: bool


@dataclass(frozen=True, slots=True)
class PreparedHistoryDescriptor:
    key: ArchiveTaskKey
    artifact_id: str
    binding_json: str
    descriptor: MemberHistoryDocument
    descriptor_json: str
    sets: tuple[tuple[str, str, str], ...]


@dataclass(frozen=True, slots=True)
class PreparedHistoryRecord:
    key: ArchiveTaskKey
    expected_row: tuple[Any, ...]
    record_key: str | None
    value_json: str | None
    bound_journal: str | None
    next_page: int
    next_item: int
    checkpoint_json: str
    count: int
    terminal: bool


class ArchiveHistoryWork:
    def __init__(
        self, admission: ArchiveDiscoveryAdmission, reader: CanonicalProvenanceArchiveReader
    ) -> None:
        self.admission = admission
        self.reader = reader
        self._prepared: PreparedHistoryDescriptor | None = None


class HistorySetWork:
    """Own one selected set and at most its current bounded source page."""

    def __init__(
        self,
        admission: ArchiveDiscoveryAdmission,
        reader: CanonicalProvenanceArchiveReader,
        authority: RecordSetRef,
    ) -> None:
        self.admission = admission
        self.reader = reader
        self.authority = authority
        self.authority_json = canonical_json_bytes(authority.to_mapping()).decode()
        self._cache: RecordPage | None = None
        self._prepared: PreparedHistoryRecord | None = None


def initialize_history_work(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("history work initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_archive_histories (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, artifact_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, binding_json TEXT NOT NULL, descriptor_json TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,artifact_id)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_history_sets (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, records_sha256 TEXT NOT NULL,
            worker_id TEXT NOT NULL, authority_json TEXT NOT NULL, version TEXT NOT NULL,
            page_ordinal TEXT NOT NULL, item_index INTEGER NOT NULL, checkpoint_json TEXT NOT NULL,
            complete INTEGER NOT NULL CHECK(complete IN (0,1)),
            PRIMARY KEY(build_id,admission_sha256,records_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_history_set_records (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, records_sha256 TEXT NOT NULL,
            record_key TEXT NOT NULL, value_json TEXT NOT NULL,
            source_page TEXT NOT NULL, source_item INTEGER NOT NULL, bound_journal TEXT,
            PRIMARY KEY(build_id,admission_sha256,records_sha256,record_key)
        ) WITHOUT ROWID;
        CREATE UNIQUE INDEX IF NOT EXISTS discovery_history_bound_heads
        ON discovery_history_set_records(build_id,admission_sha256,records_sha256,bound_journal)
        WHERE bound_journal IS NOT NULL;
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
        raise ValueError("history work requires an owner-managed transaction")
    db.execute("SAVEPOINT discovery_history_work")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_history_work")
        db.execute("RELEASE discovery_history_work")
        raise
    else:
        db.execute("RELEASE discovery_history_work")


def _require_archive(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> None:
    if (
        key.admission_sha256 != admission.identity
        or not archive_metadata_progress(db, key).complete
    ):
        raise StaleArchiveTask("history source metadata is incomplete or changed")


def prepare_history_descriptor(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: ArchiveHistoryWork, artifact_id: str
) -> PreparedHistoryDescriptor:
    if db.in_transaction:
        raise ValueError("history source preparation requires no active transaction")
    _require_archive(db, key, owner.admission)
    if not archive_member_progress(db, key).complete:
        raise StaleArchiveTask("history member registration is incomplete")
    row = _one(
        db,
        "SELECT binding_json FROM discovery_archive_members "
        "WHERE build_id=? AND admission_sha256=? AND artifact_id=?",
        (key.build_id, key.admission_sha256, artifact_id),
    )
    if row is None:
        raise StaleArchiveTask("history has no exact registered member binding")
    binding = MemberHistoryBinding.from_mapping(require_canonical_json(row[0].encode()))
    descriptor = owner.reader.history_descriptor(binding)
    sets = tuple(
        (
            authority.records_sha256,
            canonical_json_bytes(authority.to_mapping()).decode(),
            RecordSetCheckpoint.begin(authority.schema_id).to_json_bytes().decode(),
        )
        for authority in (descriptor.roots, descriptor.imports)
    )
    page = PreparedHistoryDescriptor(
        key, artifact_id, row[0], descriptor, descriptor.to_json_bytes().decode(), sets
    )
    owner._prepared = page
    return page


def apply_history_descriptor(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: ArchiveHistoryWork,
    page: PreparedHistoryDescriptor,
) -> None:
    if owner._prepared is not page or page.key != key:
        raise StaleArchiveTask("history descriptor is not the exact detached preparation")
    with _atomic(db):
        binding = _one(
            db,
            "SELECT binding_json FROM discovery_archive_members "
            "WHERE build_id=? AND admission_sha256=? AND artifact_id=?",
            (key.build_id, key.admission_sha256, page.artifact_id),
        )
        if binding != (page.binding_json,):
            raise StaleArchiveTask("history member binding changed")
        expected = key.worker_id, page.binding_json, page.descriptor_json
        old = _one(
            db,
            "SELECT worker_id,binding_json,descriptor_json FROM discovery_archive_histories "
            "WHERE build_id=? AND admission_sha256=? AND artifact_id=?",
            (key.build_id, key.admission_sha256, page.artifact_id),
        )
        if old is None:
            db.execute(
                "INSERT INTO discovery_archive_histories VALUES (?,?,?,?,?,?)",
                (key.build_id, key.admission_sha256, page.artifact_id, *expected),
            )
        elif old != expected:
            raise StaleArchiveTask("history descriptor worker or source changed")
        for identity, authority_json, checkpoint_json in page.sets:
            previous = _one(
                db,
                "SELECT worker_id,authority_json FROM discovery_history_sets "
                "WHERE build_id=? AND admission_sha256=? AND records_sha256=?",
                (key.build_id, key.admission_sha256, identity),
            )
            if previous is None:
                db.execute(
                    "INSERT INTO discovery_history_sets VALUES (?,?,?,?,?,'0','0',0,?,0)",
                    (
                        key.build_id,
                        key.admission_sha256,
                        identity,
                        key.worker_id,
                        authority_json,
                        checkpoint_json,
                    ),
                )
            elif previous != (key.worker_id, authority_json):
                raise StaleArchiveTask("shared history set worker or authority changed")


def _header(db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork) -> tuple[Any, ...]:
    row = _one(
        db,
        "SELECT worker_id,authority_json,version,page_ordinal,item_index,checkpoint_json,complete "
        "FROM discovery_history_sets WHERE build_id=? AND admission_sha256=? AND records_sha256=?",
        (key.build_id, key.admission_sha256, owner.authority.records_sha256),
    )
    if (
        key.admission_sha256 != owner.admission.identity
        or row is None
        or row[:2] != (key.worker_id, owner.authority_json)
    ):
        raise StaleArchiveTask("history set has no exact source/worker/authority binding")
    return tuple(row)


def history_set_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork
) -> HistorySetProgress:
    row = _header(db, key, owner)
    checkpoint = RecordSetCheckpoint.from_json_bytes(row[5].encode())
    return HistorySetProgress(int(row[2]), int(row[3]), row[4], checkpoint.count, bool(row[6]))


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-history-set-prepare.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_history_work.prepare_history_record",),
        scope="one shared root/import record preparation",
        starts_at="receive an admitted history set and saved page/item continuation",
        ends_at="detach one validated record/checkpoint or verify the independent set terminal",
        dimensions=(
            Dimension("registered_records", COUNT, "Previously registered exact records."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=2, unit=COUNT),
                "One exact archive fence and one set header.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=2, unit=COUNT),
                "Each exact lookup returns at most one row.",
            ),
            Bound(
                "object_reads",
                COUNT,
                "upper",
                Expression("constant", value=2, unit=COUNT),
                "On a cache miss, one selected root and only the current bounded page.",
            ),
        ),
        conditions=(
            "The caller retains current archive/build authority and the exact workspace lease.",
            "Preparation runs outside the owner write transaction.",
            "The source page obeys the existing structural record/page byte and item bounds.",
        ),
        invariants=(
            "Only the current bounded page is cached; completed pages are never replayed.",
            "A mandatory terminal compares the independently selected count and full digest.",
            "Root/import records preserve original source coordinates and exact semantic keys.",
        ),
        limitations=(
            "SQL counts exclude storage-engine work; codec/hash work follows the bounded record.",
            "Native source-union, imported source proofs and publication are separate obligations.",
        ),
    )
)
def prepare_history_record(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork
) -> PreparedHistoryRecord:
    if db.in_transaction:
        raise ValueError("history source preparation requires no active transaction")
    _require_archive(db, key, owner.admission)
    row = _header(db, key, owner)
    if row[6]:
        raise StaleArchiveTask("history set is already complete")
    ordinal, item = int(row[3]), row[4]
    checkpoint = RecordSetCheckpoint.from_json_bytes(row[5].encode())
    if checkpoint.schema_id != owner.authority.schema_id:
        raise StaleArchiveTask("history checkpoint schema changed")
    if owner._cache is None or owner._cache.ordinal != ordinal:
        owner._cache = owner.reader.history_page(owner.authority, ordinal)
    source = owner._cache
    if source.terminal:
        if item != 0 or checkpoint.ref() != owner.authority:
            raise StaleArchiveTask("history set terminal does not cover its exact authority")
        page = PreparedHistoryRecord(
            key, row, None, None, None, ordinal, 0, row[5], checkpoint.count, True
        )
    else:
        if not 0 <= item < len(source.records) or checkpoint.count >= owner.authority.record_count:
            raise StaleArchiveTask("history record position exceeds its exact authority")
        record = source.records[item]
        if set(record) != {"key", "value"} or not isinstance(record["value"], dict):
            raise ValueError("history record fields are invalid")
        bound_journal = None
        if owner.authority.schema_id == MEMBER_HISTORY_ROOTS_SCHEMA:
            selected = MemberHistoryRoot.from_mapping(record["value"])
            if record["key"] != selected.key:
                raise ValueError("history root record key differs")
            if selected.inclusion == "bound":
                bound_journal = selected.journal.journal_id
        elif owner.authority.schema_id == MEMBER_HISTORY_IMPORTS_SCHEMA:
            imported = MemberHistoryImport.from_mapping(record["value"])
            if record["key"] != imported.key:
                raise ValueError("history import record key differs")
        else:
            raise ValueError("history record set schema is invalid")
        following = checkpoint.append(record["key"], record["value"])
        last = item + 1 == len(source.records)
        page = PreparedHistoryRecord(
            key,
            row,
            record["key"],
            canonical_json_bytes(record["value"]).decode(),
            bound_journal,
            ordinal + int(last),
            0 if last else item + 1,
            following.to_json_bytes().decode(),
            following.count,
            False,
        )
    owner._prepared = page
    return page


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-history-set-apply.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_history_work.apply_history_record",),
        scope="one shared root/import record commit",
        starts_at="receive the exact detached history record and current owner transaction",
        ends_at="commit the child and continuation together or record the verified terminal",
        dimensions=(
            Dimension("registered_records", COUNT, "Previously registered exact records."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("constant", value=5, unit=COUNT),
                "Savepoint, one set header, optional record insert, update and release.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=1, unit=COUNT),
                "Only the exact current set header is read.",
            ),
            Bound(
                "source_hash_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Source hashing and checkpoint encoding precede the write transaction.",
            ),
        ),
        conditions=(
            "The caller retains current archive/build authority and the exact workspace lease.",
            "Apply receives the exact preparation; the schema has no added triggers.",
        ),
        invariants=(
            "A record's original coordinates and its commitment continuation are atomic.",
            "The unique bound-journal index rejects conflicting bound heads across pages.",
            "Stale source/worker/version or forged preparation cannot advance work.",
        ),
        limitations=(
            "SQL counts exclude storage-engine/index costs and owner transaction entry/commit.",
            "Structural completion grants no Native semantic or publication authority.",
        ),
    )
)
def apply_history_record(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork, page: PreparedHistoryRecord
) -> HistorySetProgress:
    if owner._prepared is not page or page.key != key:
        raise StaleArchiveTask("history record is not the current exact detached preparation")
    with _atomic(db):
        if _header(db, key, owner) != page.expected_row:
            raise StaleArchiveTask("history set version or continuation changed")
        if page.record_key is not None:
            db.execute(
                "INSERT INTO discovery_history_set_records VALUES (?,?,?,?,?,?,?,?)",
                (
                    key.build_id,
                    key.admission_sha256,
                    owner.authority.records_sha256,
                    page.record_key,
                    page.value_json,
                    page.expected_row[3],
                    page.expected_row[4],
                    page.bound_journal,
                ),
            )
        version = int(page.expected_row[2]) + 1
        db.execute(
            "UPDATE discovery_history_sets SET version=?,page_ordinal=?,item_index=?,"
            "checkpoint_json=?,complete=? WHERE build_id=? AND admission_sha256=? "
            "AND records_sha256=?",
            (
                str(version),
                str(page.next_page),
                page.next_item,
                page.checkpoint_json,
                int(page.terminal),
                key.build_id,
                key.admission_sha256,
                owner.authority.records_sha256,
            ),
        )
        return HistorySetProgress(
            version, page.next_page, page.next_item, page.count, page.terminal
        )


def archive_history_ready(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: ArchiveHistoryWork, artifact_id: str
) -> bool:
    """Check exact set terminals and primary membership, without granting claim authority."""
    _require_archive(db, key, owner.admission)
    row = _one(
        db,
        "SELECT worker_id,descriptor_json FROM discovery_archive_histories "
        "WHERE build_id=? AND admission_sha256=? AND artifact_id=?",
        (key.build_id, key.admission_sha256, artifact_id),
    )
    if row is None or row[0] != key.worker_id:
        raise StaleArchiveTask("history descriptor source or worker changed")
    descriptor = MemberHistoryDocument.from_json_bytes(row[1].encode())
    for authority in (descriptor.roots, descriptor.imports):
        header = _header(db, key, HistorySetWork(owner.admission, owner.reader, authority))
        if not header[6]:
            return False
    primary = MemberHistoryRoot(descriptor.primary.journal, "bound")
    selected = _one(
        db,
        "SELECT value_json FROM discovery_history_set_records "
        "WHERE build_id=? AND admission_sha256=? AND records_sha256=? AND record_key=?",
        (key.build_id, key.admission_sha256, descriptor.roots.records_sha256, primary.key),
    )
    if selected != (canonical_json_bytes(primary.to_mapping()).decode(),):
        raise StaleArchiveTask("history omits its exact bound primary root")
    return True
