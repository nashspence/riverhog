"""Shared original history inputs, preserving recorded root extents and imports."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    RETAINED_HISTORY_EXTENT,
    MemberHistoryDocument,
    MemberHistoryImport,
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
)
from riverhog_core.canonical_discovery_history_work import (
    HistorySetWork,
    PreparedHistoryImport,
    _require_archive,
    apply_history_import,
    history_import_progress,
    prepare_history_import,
)


@dataclass(frozen=True, slots=True)
class CorpusHistoryProgress:
    version: int
    root_sets: int
    import_sets: int
    pending_roots: int
    pending_imports: int


@dataclass(frozen=True, slots=True)
class CorpusHistoryInput:
    kind: str
    authority: RecordSetRef
    required_extent: str
    version: int
    pending: bool
    after_key: str | None
    records: int


@dataclass(frozen=True, slots=True)
class PreparedCorpusImport:
    key: ArchiveTaskKey
    expected: CorpusHistoryInput
    child: PreparedHistoryImport | None
    descriptor: MemberHistoryDocument | None
    extent: str | None


class CorpusImportWork:
    """Own one shared import set and its next exact original incidence."""

    def __init__(self, history: HistorySetWork) -> None:
        self.history = history
        self._prepared: PreparedCorpusImport | None = None


def initialize_corpus_history_inputs(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("history input initialization requires no active transaction")
    db.executescript(f"""
        CREATE TABLE IF NOT EXISTS discovery_corpus_history_headers (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, worker_id TEXT NOT NULL,
            version TEXT NOT NULL, root_sets TEXT NOT NULL, import_sets TEXT NOT NULL,
            pending_roots TEXT NOT NULL, pending_imports TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_corpus_history_inputs (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, kind TEXT NOT NULL,
            records_sha256 TEXT NOT NULL, worker_id TEXT NOT NULL, authority_json TEXT NOT NULL,
            required_extent TEXT NOT NULL, version TEXT NOT NULL,
            pending INTEGER NOT NULL CHECK(pending IN (0,1)),
            after_key TEXT, records TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,kind,records_sha256),
            CHECK(kind IN ('roots','imports')),
            CHECK((kind='roots' AND required_extent IN (
                '{BOUND_HISTORY_EXTENT}','{RETAINED_HISTORY_EXTENT}'))
                OR (kind='imports' AND required_extent='all'))
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS discovery_corpus_history_pending
        ON discovery_corpus_history_inputs(build_id,admission_sha256,kind,records_sha256)
        WHERE pending=1;
        CREATE INDEX IF NOT EXISTS discovery_history_structural_pending
        ON discovery_history_sets(build_id,admission_sha256,records_sha256)
        WHERE complete=0;
    """)


def _one(db: sqlite3.Connection, sql: str, parameters: tuple[object, ...]) -> Any:
    cursor = db.execute(sql, parameters)
    try:
        return cursor.fetchone()
    finally:
        cursor.close()


def _count(value: object) -> int:
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
        raise StaleArchiveTask("history input count is invalid")
    result = int(value)
    if str(result) != value:
        raise StaleArchiveTask("history input count is not canonical")
    return result


@contextmanager
def _atomic(db: sqlite3.Connection) -> Iterator[None]:
    if not db.in_transaction:
        raise ValueError("history inputs require an owner-managed transaction")
    db.execute("SAVEPOINT discovery_corpus_history_inputs")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_corpus_history_inputs")
        db.execute("RELEASE discovery_corpus_history_inputs")
        raise
    else:
        db.execute("RELEASE discovery_corpus_history_inputs")


def _declaration(mode: str) -> Declaration:
    subject = {
        "begin": "begin_corpus_history_inputs",
        "progress": "corpus_history_progress",
        "register": "_register_corpus_history",
        "next": "next_corpus_history_input",
        "structure": "next_corpus_history_structure",
        "prepare": "prepare_corpus_import",
        "apply": "apply_corpus_import",
    }[mode]

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression(
            "product",
            terms=(
                Expression("constant", value=value, unit=()),
                term,
            ),
        )

    return Declaration(
        id="riverhog-server.discovery-history-input-" + mode + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_history_inputs." + subject,),
        scope="one shared original history requirement or import continuation",
        starts_at="receive exact admitted metadata, worker and original set continuation",
        ends_at="detach one requirement/record or commit its original child and continuation",
        dimensions=(
            Dimension("registered_inputs", COUNT, "All retained metadata and history index rows."),
            Dimension("metadata_octets", OCTETS, "Largest complete metadata row or preparation."),
            Dimension("source_octets", OCTETS, "Largest bounded root, source proof or descriptor."),
        ),
        bounds=(
            Bound("sql_statements", COUNT, "upper", count(128), "Fixed indexed parent/child work."),
            Bound(
                "returned_rows", COUNT, "upper", count(64), "Exact metadata and incidence reads."
            ),
            Bound(
                "source_reads",
                COUNT,
                "upper",
                count(4 if mode == "prepare" else 0),
                "One exact source binding proof and descriptor, with their root fences.",
            ),
            Bound(
                "source_read_octets",
                OCTETS,
                "upper",
                scale(4 if mode == "prepare" else 0, dim("source_octets")),
                "Complete bounded original source objects; no preceding imports are replayed.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    scale(256, dim("metadata_octets")),
                    "Complete metadata keys, rows and writes.",
                )
                for metric in ("parameter_octets", "returned_octets", "written_octets")
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    scale(
                        256,
                        Expression(
                            "sum",
                            terms=(
                                dim("metadata_octets"),
                                dim("source_octets"),
                            ),
                        ),
                    ),
                    "Complete metadata/source codec and hashing work.",
                )
                for metric in ("encoded_octets", "hashed_octets")
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(512 if mode == "structure" else 32768),
                        Expression(
                            "product",
                            terms=(
                                count(64 if mode == "structure" else 4096),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum", terms=(dim("registered_inputs"), count(2))
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Fixed SQLite point/range work over growing shared metadata indexes.",
            ),
        ),
        conditions=(
            "The caller retains exact archive/build, reader custody and exclusive writes.",
            "Preparation is outside writes; application is in the current owner's transaction.",
            "The maintained schema has no added triggers; injected failures have separate checks.",
            "Source proof and descriptor bytes retain their archive-owned bounds.",
        ),
        invariants=(
            "Shared structural sets are processed once while original set incidences remain exact.",
            "Members select retained history; imports preserve their recorded target extent.",
            "A higher required extent cannot erase completed or pending original work.",
            "Import proof, child requirements, incidence and saved cursor commit together.",
            "Queue emptiness alone grants no global seed, Native semantic or publication terminal.",
        ),
        limitations=(
            "Root selection and Native State causality/source-union/cycles remain separate.",
            "Metadata proof verification grants no current access or publication authority.",
        ),
    )


@declare_growth(_declaration("progress"))
def corpus_history_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> CorpusHistoryProgress:
    _require_archive(db, key, admission)
    row = _one(
        db,
        "SELECT worker_id,version,root_sets,import_sets,pending_roots,pending_imports "
        "FROM discovery_corpus_history_headers WHERE build_id=? AND admission_sha256=?",
        (key.build_id, key.admission_sha256),
    )
    if row is None or row[0] != key.worker_id:
        raise StaleArchiveTask("history inputs have another source/build/worker")
    result = CorpusHistoryProgress(*(_count(value) for value in row[1:]))
    if (
        result.pending_roots > result.root_sets
        or result.pending_imports > result.import_sets
        or result.version < result.root_sets + result.import_sets
    ):
        raise StaleArchiveTask("history pending counts exceed registered original sets")
    return result


@declare_growth(_declaration("begin"))
def begin_corpus_history_inputs(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> CorpusHistoryProgress:
    _require_archive(db, key, admission)
    with _atomic(db):
        existing = _one(
            db,
            "SELECT worker_id FROM discovery_corpus_history_headers "
            "WHERE build_id=? AND admission_sha256=?",
            (key.build_id, key.admission_sha256),
        )
        if existing is None:
            member = _one(
                db,
                "SELECT worker_id,version FROM discovery_archive_member_tasks "
                "WHERE build_id=? AND admission_sha256=?",
                (key.build_id, key.admission_sha256),
            )
            if member != (key.worker_id, "0"):
                raise StaleArchiveTask("advanced member source lacks original history inputs")
        db.execute(
            "INSERT OR IGNORE INTO discovery_corpus_history_headers "
            "VALUES (?,?,?,'0','0','0','0','0')",
            (key.build_id, key.admission_sha256, key.worker_id),
        ).close()
        return corpus_history_progress(db, key, admission)


def _change_counts(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    *,
    roots: int = 0,
    imports: int = 0,
    pending_roots: int = 0,
    pending_imports: int = 0,
) -> None:
    old = corpus_history_progress(db, key, admission)
    values = (
        old.root_sets + roots,
        old.import_sets + imports,
        old.pending_roots + pending_roots,
        old.pending_imports + pending_imports,
    )
    if min(values) < 0 or values[2] > values[0] or values[3] > values[1]:
        raise StaleArchiveTask("history input count transition is inconsistent")
    cursor = db.execute(
        "UPDATE discovery_corpus_history_headers SET version=?,root_sets=?,import_sets=?,"
        "pending_roots=?,pending_imports=? "
        "WHERE build_id=? AND admission_sha256=? AND worker_id=? AND version=?",
        (
            str(old.version + 1),
            *(str(value) for value in values),
            key.build_id,
            key.admission_sha256,
            key.worker_id,
            str(old.version),
        ),
    )
    try:
        if cursor.rowcount != 1:
            raise StaleArchiveTask("history input counts changed during registration")
    finally:
        cursor.close()


def _input(
    db: sqlite3.Connection, key: ArchiveTaskKey, kind: str, identity: str
) -> CorpusHistoryInput:
    row = _one(
        db,
        "SELECT worker_id,authority_json,required_extent,version,pending,after_key,records "
        "FROM discovery_corpus_history_inputs "
        "WHERE build_id=? AND admission_sha256=? AND kind=? AND records_sha256=?",
        (key.build_id, key.admission_sha256, kind, identity),
    )
    if row is None or row[0] != key.worker_id:
        raise StaleArchiveTask("history input has another exact source/build/worker")
    authority = RecordSetRef.from_mapping(require_canonical_json(row[1].encode()))
    schema = MEMBER_HISTORY_ROOTS_SCHEMA if kind == "roots" else MEMBER_HISTORY_IMPORTS_SCHEMA
    if authority.records_sha256 != identity or authority.schema_id != schema:
        raise StaleArchiveTask("history input authority or kind changed")
    version, records = _count(row[3]), _count(row[6])
    if records > authority.record_count or version < records:
        raise StaleArchiveTask("history input cursor exceeds independent source count")
    if row[5] is not None and (
        not isinstance(row[5], str)
        or len(row[5]) != 64
        or any(c not in "0123456789abcdef" for c in row[5])
    ):
        raise StaleArchiveTask("history input original cursor is invalid")
    if kind == "imports":
        if row[2] != "all" or (records == 0) != (row[5] is None):
            raise StaleArchiveTask("history import cursor and count disagree")
        if not row[4] and records != authority.record_count:
            raise StaleArchiveTask("history import terminal omits original records")
        if version != records + int(not row[4]):
            raise StaleArchiveTask("history import version and original cursor disagree")
        if records:
            last = _one(
                db,
                "SELECT record_key FROM discovery_history_import_links "
                "WHERE build_id=? AND admission_sha256=? AND import_set_sha256=? "
                "ORDER BY record_key DESC LIMIT 1",
                (key.build_id, key.admission_sha256, identity),
            )
            if last != (row[5],):
                raise StaleArchiveTask(
                    "history import continuation is not its last original incidence"
                )
    elif row[2] not in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT):
        raise StaleArchiveTask("history root extent is unsupported")
    return CorpusHistoryInput(kind, authority, row[2], version, bool(row[4]), row[5], records)


@declare_growth(_declaration("register"))
def _register_corpus_history(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    descriptor: MemberHistoryDocument,
    *,
    extent: str,
) -> CorpusHistoryProgress:
    if extent not in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT):
        raise ValueError("unsupported selected history extent")
    _require_archive(db, key, admission)
    with _atomic(db):
        original = _one(
            db,
            "SELECT descriptor_json FROM discovery_history_descriptors "
            "WHERE build_id=? AND admission_sha256=? AND history_sha256=?",
            (key.build_id, key.admission_sha256, descriptor.identity),
        )
        if original != (descriptor.to_json_bytes().decode(),):
            raise StaleArchiveTask("history seed has no exact registered descriptor")
        for kind, authority, required in (
            ("roots", descriptor.roots, extent),
            ("imports", descriptor.imports, "all"),
        ):
            current = _one(
                db,
                "SELECT 1 FROM discovery_corpus_history_inputs "
                "WHERE build_id=? AND admission_sha256=? AND kind=? AND records_sha256=?",
                (key.build_id, key.admission_sha256, kind, authority.records_sha256),
            )
            authority_json = canonical_json_bytes(authority.to_mapping()).decode()
            structural = _one(
                db,
                "SELECT worker_id,authority_json FROM discovery_history_sets "
                "WHERE build_id=? AND admission_sha256=? AND records_sha256=?",
                (key.build_id, key.admission_sha256, authority.records_sha256),
            )
            if structural != (key.worker_id, authority_json):
                raise StaleArchiveTask("history input lacks exact original structural obligation")
            if current is None:
                db.execute(
                    "INSERT INTO discovery_corpus_history_inputs "
                    "VALUES (?,?,?,?,?,?,?,'0',1,NULL,'0')",
                    (
                        key.build_id,
                        key.admission_sha256,
                        kind,
                        authority.records_sha256,
                        key.worker_id,
                        authority_json,
                        required,
                    ),
                ).close()
                _change_counts(
                    db,
                    key,
                    admission,
                    **(
                        {"roots": 1, "pending_roots": 1}
                        if kind == "roots"
                        else {"imports": 1, "pending_imports": 1}
                    ),
                )
            else:
                old = _input(db, key, kind, authority.records_sha256)
                if old.authority != authority:
                    raise StaleArchiveTask("shared history input commitment changed")
                if (
                    kind == "roots"
                    and old.required_extent == BOUND_HISTORY_EXTENT
                    and required == RETAINED_HISTORY_EXTENT
                ):
                    db.execute(
                        "UPDATE discovery_corpus_history_inputs SET required_extent=?,version=?,"
                        "pending=1 "
                        "WHERE build_id=? AND admission_sha256=? AND kind=? AND records_sha256=?",
                        (
                            required,
                            str(old.version + 1),
                            key.build_id,
                            key.admission_sha256,
                            kind,
                            authority.records_sha256,
                        ),
                    ).close()
                    _change_counts(db, key, admission, pending_roots=int(not old.pending))
        return corpus_history_progress(db, key, admission)


@declare_growth(_declaration("next"))
def next_corpus_history_input(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission, *, kind: str
) -> CorpusHistoryInput | None:
    if kind not in ("roots", "imports"):
        raise ValueError("unsupported history input kind")
    progress = corpus_history_progress(db, key, admission)
    row = _one(
        db,
        "SELECT records_sha256 FROM discovery_corpus_history_inputs "
        "INDEXED BY discovery_corpus_history_pending "
        "WHERE build_id=? AND admission_sha256=? AND kind=? AND pending=1 "
        "ORDER BY records_sha256 LIMIT 1",
        (key.build_id, key.admission_sha256, kind),
    )
    pending = progress.pending_roots if kind == "roots" else progress.pending_imports
    if (row is None) != (pending == 0):
        raise StaleArchiveTask("history pending queue and independent count disagree")
    return None if row is None else _input(db, key, kind, row[0])


@declare_growth(_declaration("structure"))
def next_corpus_history_structure(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> RecordSetRef | None:
    corpus_history_progress(db, key, admission)
    row = _one(
        db,
        "SELECT records_sha256,worker_id,authority_json FROM discovery_history_sets "
        "INDEXED BY discovery_history_structural_pending "
        "WHERE build_id=? AND admission_sha256=? AND complete=0 ORDER BY records_sha256 LIMIT 1",
        (key.build_id, key.admission_sha256),
    )
    if row is None:
        return None
    authority = RecordSetRef.from_mapping(require_canonical_json(row[2].encode()))
    if row[1] != key.worker_id or row[0] != authority.records_sha256:
        raise StaleArchiveTask("structural history queue has another exact source or worker")
    return authority


@declare_growth(_declaration("prepare"))
def prepare_corpus_import(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusImportWork
) -> PreparedCorpusImport:
    if db.in_transaction:
        raise ValueError("history import preparation requires no active transaction")
    history = owner.history
    corpus_history_progress(db, key, history.admission)
    task = _input(db, key, "imports", history.authority.records_sha256)
    if task.authority != history.authority or not task.pending:
        raise StaleArchiveTask("history import task is complete or changed")
    coverage = history_import_progress(db, key, history)
    if task.records != coverage.registered_imports:
        raise StaleArchiveTask("history original cursor and import incidence coverage disagree")
    if task.after_key is None:
        record = _one(
            db,
            "SELECT record_key FROM discovery_history_set_records "
            "WHERE build_id=? AND admission_sha256=? AND records_sha256=? "
            "ORDER BY record_key LIMIT 1",
            (key.build_id, key.admission_sha256, history.authority.records_sha256),
        )
    else:
        record = _one(
            db,
            "SELECT record_key FROM discovery_history_set_records "
            "WHERE build_id=? AND admission_sha256=? AND records_sha256=? AND record_key>? "
            "ORDER BY record_key LIMIT 1",
            (key.build_id, key.admission_sha256, history.authority.records_sha256, task.after_key),
        )
    if record is None:
        if not coverage.complete:
            raise StaleArchiveTask("history import terminal lacks original incidence coverage")
        page = PreparedCorpusImport(key, task, None, None, None)
    else:
        child = prepare_history_import(db, key, history, record[0])
        imported = MemberHistoryImport.from_mapping(
            require_canonical_json(child.record_json.encode())
        )
        descriptor = MemberHistoryDocument.from_json_bytes(child.descriptor_json.encode())
        page = PreparedCorpusImport(key, task, child, descriptor, imported.extent)
    owner._prepared = page
    return page


@declare_growth(_declaration("apply"))
def apply_corpus_import(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusImportWork, page: PreparedCorpusImport
) -> CorpusHistoryInput:
    if owner._prepared is not page or page.key != key:
        raise StaleArchiveTask("corpus import is not the exact detached preparation")
    history = owner.history
    with _atomic(db):
        corpus_history_progress(db, key, history.admission)
        current = _input(db, key, "imports", history.authority.records_sha256)
        if current != page.expected:
            raise StaleArchiveTask("history original import continuation changed")
        after: str | None
        if page.child is not None:
            if page.descriptor is None or page.extent is None:
                raise StaleArchiveTask("history import lacks exact detached child requirements")
            apply_history_import(db, key, history, page.child)
            _register_corpus_history(
                db, key, history.admission, page.descriptor, extent=page.extent
            )
            after, records, pending = page.child.record_key, current.records + 1, 1
        else:
            coverage = history_import_progress(db, key, history)
            if not coverage.complete or coverage.registered_imports != current.records:
                raise StaleArchiveTask("history import completion coverage changed")
            after, records, pending = current.after_key, current.records, 0
            _change_counts(db, key, history.admission, pending_imports=-1)
        db.execute(
            "UPDATE discovery_corpus_history_inputs SET version=?,after_key=?,records=?,pending=? "
            "WHERE build_id=? AND admission_sha256=? AND kind='imports' AND records_sha256=?",
            (
                str(current.version + 1),
                after,
                str(records),
                pending,
                key.build_id,
                key.admission_sha256,
                history.authority.records_sha256,
            ),
        ).close()
        return _input(db, key, "imports", history.authority.records_sha256)
