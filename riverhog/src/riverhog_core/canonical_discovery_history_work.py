"""Root-selected history descriptors and shared resumable structural set checks.

These terminals establish structural coverage. Native source-union semantics,
import membership proofs, current permissions and publication have separate owners.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
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
    ArchiveDiscoveryAdmission,
    ArchiveTaskKey,
    StaleArchiveTask,
    archive_metadata_progress,
)
from riverhog_core.canonical_discovery_member_work import archive_member_progress
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader


@dataclass(frozen=True, slots=True)
class HistoryImportProgress:
    expected_imports: int
    registered_imports: int

    @property
    def complete(self) -> bool:
        return self.registered_imports == self.expected_imports


@dataclass(frozen=True, slots=True)
class HistorySetProgress:
    version: int
    page_ordinal: int
    item_index: int
    records: int
    bound_roots: int
    complete: bool


@dataclass(frozen=True, slots=True)
class PreparedHistoryDescriptor:
    key: ArchiveTaskKey
    artifact_id: str
    binding_json: str
    history_sha256: str
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
    bound_roots: int
    terminal: bool


@dataclass(frozen=True, slots=True)
class PreparedHistoryImport:
    key: ArchiveTaskKey
    expected_row: tuple[Any, ...]
    record_key: str
    record_json: str
    proof_sha256: str
    proof_json: str
    history_sha256: str
    descriptor_json: str
    sets: tuple[tuple[str, str, str], ...]


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
        self._prepared_import: PreparedHistoryImport | None = None


def initialize_history_work(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("history work initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_archive_histories (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, artifact_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, binding_json TEXT NOT NULL, history_sha256 TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,artifact_id)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_history_descriptors (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, history_sha256 TEXT NOT NULL,
            descriptor_json TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,history_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_history_import_proofs (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, proof_sha256 TEXT NOT NULL,
            worker_id TEXT NOT NULL, proof_json TEXT NOT NULL, history_sha256 TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,proof_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_history_import_coverage (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, import_set_sha256 TEXT NOT NULL,
            worker_id TEXT NOT NULL, authority_json TEXT NOT NULL, registered_imports TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,import_set_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_history_import_links (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, import_set_sha256 TEXT NOT NULL,
            record_key TEXT NOT NULL, proof_sha256 TEXT NOT NULL, history_sha256 TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,import_set_sha256,record_key)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_history_sets (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, records_sha256 TEXT NOT NULL,
            worker_id TEXT NOT NULL, authority_json TEXT NOT NULL, version TEXT NOT NULL,
            page_ordinal TEXT NOT NULL, item_index INTEGER NOT NULL, checkpoint_json TEXT NOT NULL,
            complete INTEGER NOT NULL CHECK(complete IN (0,1)), bound_roots TEXT NOT NULL,
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
    page = PreparedHistoryDescriptor(
        key,
        artifact_id,
        row[0],
        binding.history_sha256,
        descriptor,
        descriptor.to_json_bytes().decode(),
        _initial_sets(descriptor),
    )
    owner._prepared = page
    return page


def _initial_sets(descriptor: MemberHistoryDocument) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (
            authority.records_sha256,
            canonical_json_bytes(authority.to_mapping()).decode(),
            RecordSetCheckpoint.begin(authority.schema_id).to_json_bytes().decode(),
        )
        for authority in (descriptor.roots, descriptor.imports)
    )


def _register_descriptor(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    history_sha256: str,
    descriptor_json: str,
    sets: tuple[tuple[str, str, str], ...],
) -> None:
    old = _one(
        db,
        "SELECT descriptor_json FROM discovery_history_descriptors "
        "WHERE build_id=? AND admission_sha256=? AND history_sha256=?",
        (key.build_id, key.admission_sha256, history_sha256),
    )
    if old is None:
        db.execute(
            "INSERT INTO discovery_history_descriptors VALUES (?,?,?,?)",
            (key.build_id, key.admission_sha256, history_sha256, descriptor_json),
        )
    elif old != (descriptor_json,):
        raise StaleArchiveTask("shared history descriptor changed")
    for identity, authority_json, checkpoint_json in sets:
        previous = _one(
            db,
            "SELECT worker_id,authority_json FROM discovery_history_sets "
            "WHERE build_id=? AND admission_sha256=? AND records_sha256=?",
            (key.build_id, key.admission_sha256, identity),
        )
        if previous is None:
            db.execute(
                "INSERT INTO discovery_history_sets VALUES (?,?,?,?,?,'0','0',0,?,0,'0')",
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
        expected = key.worker_id, page.binding_json, page.history_sha256
        old = _one(
            db,
            "SELECT worker_id,binding_json,history_sha256 FROM discovery_archive_histories "
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
        _register_descriptor(db, key, page.history_sha256, page.descriptor_json, page.sets)


def _header(db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork) -> tuple[Any, ...]:
    row = _one(
        db,
        "SELECT worker_id,authority_json,version,page_ordinal,item_index,checkpoint_json,complete,"
        "bound_roots "
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


def _bound_roots(row: tuple[Any, ...], checkpoint: RecordSetCheckpoint, schema_id: str) -> int:
    try:
        if type(row[7]) is not str:
            raise ValueError
        bound = parse_scalar("nonnegative", row[7])
    except (TypeError, ValueError):
        raise StaleArchiveTask("history bound-root count is not canonical") from None
    if (
        checkpoint.schema_id != schema_id
        or bound > checkpoint.count
        or (schema_id != MEMBER_HISTORY_ROOTS_SCHEMA and bound != 0)
    ):
        raise StaleArchiveTask("history bound-root count differs from its structural coverage")
    return bound


def history_set_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork
) -> HistorySetProgress:
    row = _header(db, key, owner)
    checkpoint = RecordSetCheckpoint.from_json_bytes(row[5].encode())
    bound = _bound_roots(row, checkpoint, owner.authority.schema_id)
    return HistorySetProgress(
        int(row[2]), int(row[3]), row[4], checkpoint.count, bound, bool(row[6])
    )


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
    bound_roots = _bound_roots(row, checkpoint, owner.authority.schema_id)
    if owner._cache is None or owner._cache.ordinal != ordinal:
        owner._cache = owner.reader.history_page(owner.authority, ordinal)
    source = owner._cache
    if source.terminal:
        if item != 0 or checkpoint.ref() != owner.authority:
            raise StaleArchiveTask("history set terminal does not cover its exact authority")
        page = PreparedHistoryRecord(
            key, row, None, None, None, ordinal, 0, row[5], checkpoint.count, bound_roots, True
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
            bound_roots + int(bound_journal is not None),
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
            "Original coordinates, commitment continuation and bound-root input count are atomic.",
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
            "checkpoint_json=?,complete=?,bound_roots=? WHERE build_id=? AND admission_sha256=? "
            "AND records_sha256=?",
            (
                str(version),
                str(page.next_page),
                page.next_item,
                page.checkpoint_json,
                int(page.terminal),
                format_scalar("nonnegative", page.bound_roots),
                key.build_id,
                key.admission_sha256,
                owner.authority.records_sha256,
            ),
        )
        return HistorySetProgress(
            version, page.next_page, page.next_item, page.count, page.bound_roots, page.terminal
        )


def history_descriptor_ready(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: ArchiveHistoryWork, history_sha256: str
) -> bool:
    """Verify one descriptor's set terminals and primary, without claiming Native validation."""
    _require_archive(db, key, owner.admission)
    row = _one(
        db,
        "SELECT descriptor_json FROM discovery_history_descriptors "
        "WHERE build_id=? AND admission_sha256=? AND history_sha256=?",
        (key.build_id, key.admission_sha256, history_sha256),
    )
    if row is None:
        raise StaleArchiveTask("history descriptor is not registered")
    descriptor = MemberHistoryDocument.from_json_bytes(row[0].encode())
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


def archive_history_ready(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: ArchiveHistoryWork, artifact_id: str
) -> bool:
    """Check exact set terminals and primary membership for a root-registered member."""
    row = _one(
        db,
        "SELECT worker_id,history_sha256 FROM discovery_archive_histories "
        "WHERE build_id=? AND admission_sha256=? AND artifact_id=?",
        (key.build_id, key.admission_sha256, artifact_id),
    )
    if row is None or row[0] != key.worker_id:
        raise StaleArchiveTask("history descriptor source or worker changed")
    return history_descriptor_ready(db, key, owner, row[1])


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-history-import-prepare.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_history_work.prepare_history_import",),
        scope="one exact inherited history membership proof preparation",
        starts_at="receive one registered record from a completely verified import set",
        ends_at="detach the exact source proof, selected history descriptor and set obligations",
        dimensions=(
            Dimension("registered_imports", COUNT, "Previously registered source proofs."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=3, unit=COUNT),
                "One archive fence, one set header and one exact import record.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=3, unit=COUNT),
                "Each selected lookup returns at most one row.",
            ),
            Bound(
                "object_reads",
                COUNT,
                "upper",
                Expression("constant", value=4, unit=COUNT),
                "One source proof and descriptor, each with the current archive root fence.",
            ),
        ),
        conditions=(
            "The caller retains current archive/build authority and the exact workspace lease.",
            "Preparation runs outside the owner write transaction.",
            "The source proof and history descriptor obey their existing representation bounds.",
        ),
        invariants=(
            "Import membership retains the original source identity, roots and proof coordinates.",
            "The exact selected descriptor registers shared root/import set obligations.",
            "No newer source head, provider lookup or member-union authorization is substituted.",
        ),
        limitations=(
            "SQL counts exclude storage-engine work; bounded proof verification is separate.",
            "Native State causality, graph closure and publication have independent obligations.",
        ),
    )
)
def prepare_history_import(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork, record_key: str
) -> PreparedHistoryImport:
    if db.in_transaction:
        raise ValueError("history import preparation requires no active transaction")
    _require_archive(db, key, owner.admission)
    header = _verified_import_set(db, key, owner)
    row = _one(
        db,
        "SELECT value_json FROM discovery_history_set_records "
        "WHERE build_id=? AND admission_sha256=? AND records_sha256=? AND record_key=?",
        (key.build_id, key.admission_sha256, owner.authority.records_sha256, record_key),
    )
    if row is None:
        raise StaleArchiveTask("history import record is not registered")
    imported = MemberHistoryImport.from_mapping(require_canonical_json(row[0].encode()))
    if imported.key != record_key:
        raise StaleArchiveTask("history import record key changed")
    proof = owner.reader.source_binding_proof(imported)
    descriptor = owner.reader.history_descriptor(proof.binding)
    page = PreparedHistoryImport(
        key,
        header,
        record_key,
        row[0],
        proof.identity,
        proof.to_json_bytes().decode(),
        proof.binding.history_sha256,
        descriptor.to_json_bytes().decode(),
        _initial_sets(descriptor),
    )
    owner._prepared_import = page
    return page


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-history-import-apply.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_history_work.apply_history_import",),
        scope="one exact inherited history membership proof commit",
        starts_at="receive the exact detached proof and current owner transaction",
        ends_at="retain the original proof with its shared descriptor and child set obligations",
        dimensions=(
            Dimension("registered_imports", COUNT, "Previously registered source proofs."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("constant", value=17, unit=COUNT),
                "Exact source reads, shared children and atomic incidence coverage.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=8, unit=COUNT),
                "Exact source, shared child/proof, incidence and coverage lookups.",
            ),
            Bound(
                "source_hash_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Source verification and checkpoint encoding precede the write transaction.",
            ),
        ),
        conditions=(
            "The caller retains current archive/build authority and the exact workspace lease.",
            "Apply receives the exact preparation; the schema has no added triggers.",
        ),
        invariants=(
            "Proof, descriptor, children, incidence and coverage commit atomically.",
            "Existing shared descriptors and completed set continuations are never reset.",
            "The original archive roots and member inclusion coordinates remain exact.",
        ),
        limitations=(
            "SQL counts exclude storage-engine/index costs and owner transaction entry/commit.",
            "Structural membership grants no Native semantic or publication authority.",
        ),
    )
)
def apply_history_import(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork, page: PreparedHistoryImport
) -> None:
    if owner._prepared_import is not page or page.key != key:
        raise StaleArchiveTask("history import is not the current exact detached preparation")
    with _atomic(db):
        if _verified_import_set(db, key, owner) != page.expected_row:
            raise StaleArchiveTask("history import source or continuation changed")
        coverage, coverage_exists = _import_coverage(db, key, owner)
        current = _one(
            db,
            "SELECT value_json FROM discovery_history_set_records "
            "WHERE build_id=? AND admission_sha256=? AND records_sha256=? AND record_key=?",
            (key.build_id, key.admission_sha256, owner.authority.records_sha256, page.record_key),
        )
        if current != (page.record_json,):
            raise StaleArchiveTask("history import record changed")
        _register_descriptor(db, key, page.history_sha256, page.descriptor_json, page.sets)
        expected = key.worker_id, page.proof_json, page.history_sha256
        previous = _one(
            db,
            "SELECT worker_id,proof_json,history_sha256 FROM discovery_history_import_proofs "
            "WHERE build_id=? AND admission_sha256=? AND proof_sha256=?",
            (key.build_id, key.admission_sha256, page.proof_sha256),
        )
        if previous is None:
            db.execute(
                "INSERT INTO discovery_history_import_proofs VALUES (?,?,?,?,?,?)",
                (key.build_id, key.admission_sha256, page.proof_sha256, *expected),
            )
        elif previous != expected:
            raise StaleArchiveTask("history import proof source or worker changed")
        incidence = _one(
            db,
            "SELECT proof_sha256,history_sha256 FROM discovery_history_import_links "
            "WHERE build_id=? AND admission_sha256=? AND import_set_sha256=? AND record_key=?",
            (key.build_id, key.admission_sha256, owner.authority.records_sha256, page.record_key),
        )
        expected_link = page.proof_sha256, page.history_sha256
        if incidence is None:
            if coverage.complete:
                raise StaleArchiveTask("import coverage already reaches its authenticated count")
            if not coverage_exists:
                db.execute(
                    "INSERT INTO discovery_history_import_coverage VALUES (?,?,?,?,?,'0')",
                    (
                        key.build_id,
                        key.admission_sha256,
                        owner.authority.records_sha256,
                        key.worker_id,
                        owner.authority_json,
                    ),
                ).close()
            db.execute(
                "INSERT INTO discovery_history_import_links VALUES (?,?,?,?,?,?)",
                (
                    key.build_id,
                    key.admission_sha256,
                    owner.authority.records_sha256,
                    page.record_key,
                    *expected_link,
                ),
            ).close()
            with closing(
                db.execute(
                    "UPDATE discovery_history_import_coverage SET registered_imports=? "
                    "WHERE build_id=? AND admission_sha256=? AND import_set_sha256=? "
                    "AND worker_id=? AND authority_json=? AND registered_imports=?",
                    (
                        format_scalar("nonnegative", coverage.registered_imports + 1),
                        key.build_id,
                        key.admission_sha256,
                        owner.authority.records_sha256,
                        key.worker_id,
                        owner.authority_json,
                        format_scalar("nonnegative", coverage.registered_imports),
                    ),
                )
            ) as changed:
                if changed.rowcount != 1:
                    raise StaleArchiveTask("history import coverage changed during registration")
        elif incidence != expected_link:
            raise StaleArchiveTask("history import incidence has conflicting original coordinates")
        elif not coverage_exists or coverage.registered_imports == 0:
            raise StaleArchiveTask("history import incidence has no matching coverage")


def _verified_import_set(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork
) -> tuple[Any, ...]:
    if (
        key.admission_sha256 != owner.admission.identity
        or owner.authority.schema_id != MEMBER_HISTORY_IMPORTS_SCHEMA
        or canonical_json_bytes(owner.authority.to_mapping()).decode() != owner.authority_json
    ):
        raise StaleArchiveTask("history import set has another source or original authority")
    row = _header(db, key, owner)
    checkpoint = RecordSetCheckpoint.from_json_bytes(row[5].encode())
    if (
        not row[6]
        or checkpoint.schema_id != owner.authority.schema_id
        or checkpoint.count != owner.authority.record_count
    ):
        raise StaleArchiveTask("history import set is not completely verified")
    return row


def _import_coverage(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork
) -> tuple[HistoryImportProgress, bool]:
    row = _one(
        db,
        "SELECT worker_id,authority_json,registered_imports "
        "FROM discovery_history_import_coverage "
        "WHERE build_id=? AND admission_sha256=? AND import_set_sha256=?",
        (key.build_id, key.admission_sha256, owner.authority.records_sha256),
    )
    if row is None:
        return HistoryImportProgress(owner.authority.record_count, 0), False
    if row[:2] != (key.worker_id, owner.authority_json) or type(row[2]) is not str:
        raise StaleArchiveTask("history import coverage has another worker or authority")
    try:
        registered = parse_scalar("nonnegative", row[2])
    except (ValueError, TypeError):
        raise StaleArchiveTask("history import coverage counter is not canonical") from None
    if registered > owner.authority.record_count:
        raise StaleArchiveTask("history import coverage exceeds its authenticated count")
    return HistoryImportProgress(owner.authority.record_count, registered), True


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-history-import-coverage.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_history_work.history_import_progress",),
        scope="one authenticated import-set original incidence completion check",
        starts_at="receive the current archive, worker and exact authenticated import set",
        ends_at="close Core cursors and detach required versus registered import counts",
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
            "Import registration increments coverage only with a new authenticated incidence.",
        ),
        invariants=(
            "The expected count comes from the fully verified original structural import set.",
            "A shared source proof cannot substitute for a missing import-set incidence.",
            "An exact retry increments no count; incidence and counter changes are atomic.",
            "This operation performs no COUNT, source-byte read, Native call or write.",
        ),
        limitations=(
            "This is one import set's coverage, not complete member/import/dependency closure.",
            "Native validity, source custody and publication retain their separate terminals.",
            "VM constants depend on SQLite; lock waits and elapsed time are observations.",
        ),
    )
)
def history_import_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork
) -> HistoryImportProgress:
    if not archive_metadata_progress(db, key).complete:
        raise StaleArchiveTask("history import archive metadata is incomplete")
    _verified_import_set(db, key, owner)
    return _import_coverage(db, key, owner)[0]
