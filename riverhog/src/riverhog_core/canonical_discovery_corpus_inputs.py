"""Fixed archive-selected complete-journal inputs for retained corpus validation."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from riverhog_archive_contracts import HistoryJournalAnchor
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
    Unit,
    declare_growth,
)

from riverhog_core.canonical_discovery_archive_work import (
    ArchiveDiscoveryAdmission,
    ArchiveTaskKey,
    StaleArchiveTask,
)
from riverhog_core.canonical_discovery_journal_work import (
    QualifiedJournalSource,
    journal_source_binding,
)

_FORMAT = "riverhog-discovery-corpus-source-inputs/v1"


@dataclass(frozen=True, slots=True)
class CorpusSourceInputProgress:
    version: int
    registered: int
    complete: bool


@dataclass(frozen=True, slots=True)
class FrozenCorpusSourceInputs:
    admission_sha256: str
    manifest_sha256: str
    source_count: int

    def __post_init__(self) -> None:
        for digest in (self.admission_sha256, self.manifest_sha256):
            if (
                type(digest) is not str
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError("corpus input manifest identities require lowercase SHA-256")
        if type(self.source_count) is not int or self.source_count < 0:
            raise ValueError("corpus input manifest count must be a nonnegative integer")


@dataclass(frozen=True, slots=True)
class PreparedCorpusSourceInput:
    key: ArchiveTaskKey
    expected_version: int
    source_key: str
    source_binding_json: str
    anchor: HistoryJournalAnchor
    anchor_json: str


class CorpusSourceInputWork:
    """Keep the exact qualified source and one detached preparation."""

    def __init__(
        self, admission: ArchiveDiscoveryAdmission, source: QualifiedJournalSource
    ) -> None:
        self.admission, self.source = admission, source
        self.source_key, self.source_binding_json = source.source_key, source.binding_json
        value = require_canonical_json(self.source_binding_json.encode())
        if (
            not isinstance(value, dict)
            or set(value) != {"admission_sha256", "anchor", "domain"}
            or value["admission_sha256"] != admission.identity
            or value["domain"] != "riverhog-discovery-journal-source/v1"
            or sha256(self.source_binding_json.encode()).hexdigest() != self.source_key
        ):
            raise StaleArchiveTask("corpus source has another exact archive admission")
        self.anchor = HistoryJournalAnchor.from_mapping(value["anchor"])
        self._prepared: PreparedCorpusSourceInput | None = None

    def check_source(self) -> None:
        if (
            self.source.source_key != self.source_key
            or self.source.binding_json != self.source_binding_json
        ):
            raise StaleArchiveTask("corpus source binding changed")


def initialize_corpus_source_inputs(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("corpus input initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_corpus_source_manifests (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, worker_id TEXT NOT NULL,
            manifest_json TEXT NOT NULL, expected_sources TEXT NOT NULL,
            registered_sources TEXT NOT NULL, version TEXT NOT NULL,
            complete INTEGER NOT NULL CHECK(complete IN (0,1)),
            PRIMARY KEY(build_id,admission_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_corpus_source_inputs (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            source_key TEXT NOT NULL, source_binding_json TEXT NOT NULL, anchor_json TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id),
            UNIQUE(build_id,admission_sha256,source_key)
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
        raise ValueError("corpus input changes require an owner-managed transaction")
    db.execute("SAVEPOINT discovery_corpus_source_inputs")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_corpus_source_inputs")
        db.execute("RELEASE discovery_corpus_source_inputs")
        raise
    else:
        db.execute("RELEASE discovery_corpus_source_inputs")


def _manifest(admission: ArchiveDiscoveryAdmission) -> str:
    # The authenticated root commits the complete journal byte identities.
    # Full validated bytes determine the exact terminal anchors independently of extraction.
    return canonical_json_bytes(
        {
            "format": _FORMAT,
            "archive_admission_sha256": admission.identity,
            "source_count": format_scalar("nonnegative", admission.root.journal_count),
        }
    ).decode()


def _parent(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> None:
    if key.admission_sha256 != admission.identity:
        raise StaleArchiveTask("corpus inputs have another archive admission")
    if canonical_json_bytes(admission.root.to_mapping()).decode() != admission.root_json:
        raise StaleArchiveTask("corpus input root preimage changed")
    current = _one(
        db,
        "SELECT 1 FROM discovery_archive_tasks WHERE build_id=? AND admission_sha256=? "
        "AND worker_id=? AND binding_json=? AND root_json=? AND complete=1",
        (
            key.build_id,
            key.admission_sha256,
            key.worker_id,
            admission.binding_json,
            admission.root_json,
        ),
    )
    if current != (1,):
        raise StaleArchiveTask("corpus source archive coverage or worker is not current")


def _natural(value: object) -> int:
    if type(value) is not str:
        raise StaleArchiveTask("corpus input counter representation changed")
    try:
        return parse_scalar("nonnegative", value)
    except (TypeError, ValueError):
        raise StaleArchiveTask("corpus input counter is not canonical") from None


def _header(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> CorpusSourceInputProgress:
    _parent(db, key, admission)
    row = _one(
        db,
        "SELECT worker_id,manifest_json,expected_sources,registered_sources,version,complete "
        "FROM discovery_corpus_source_manifests WHERE build_id=? AND admission_sha256=?",
        (key.build_id, key.admission_sha256),
    )
    if row is None or row[:3] != (
        key.worker_id,
        _manifest(admission),
        format_scalar("nonnegative", admission.root.journal_count),
    ):
        raise StaleArchiveTask("corpus source manifest binding changed")
    registered, version = _natural(row[3]), _natural(row[4])
    if registered > admission.root.journal_count or row[5] not in (0, 1):
        raise StaleArchiveTask("corpus source manifest progress is invalid")
    if row[5] and registered != admission.root.journal_count:
        raise StaleArchiveTask("completed corpus source manifest is incomplete")
    return CorpusSourceInputProgress(version, registered, bool(row[5]))


def _declaration(mode: str) -> Declaration:
    def constant(value: int, unit: Unit = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(constant(value, ()), term))

    def work_scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(constant(value), term))

    statements = {"begin": 9, "prepare": 3, "apply": 13, "complete": 6, "membership": 3}[mode]
    native = {"prepare": 2, "apply": 1}.get(mode, 0)
    subject = {
        "begin": "begin_corpus_source_inputs",
        "prepare": "prepare_corpus_source_input",
        "apply": "apply_corpus_source_input",
        "complete": "complete_corpus_source_inputs",
        "membership": "corpus_source_admitted",
    }[mode]
    return Declaration(
        id="riverhog-server.discovery-corpus-source-input-" + mode + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_corpus_inputs." + subject,),
        scope="one Core fixed-source-manifest metadata operation and Native point-call boundary",
        starts_at="receive the exact archive/work binding and complete candidate metadata",
        ends_at="close Core cursors and detach progress, a complete manifest, or exact membership",
        dimensions=(
            Dimension(
                "registered_inputs", COUNT, "All retained archive/input control and input rows."
            ),
            Dimension(
                "metadata_octets",
                OCTETS,
                "Largest complete incoming or selected metadata row width.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                constant(statements),
                "Exact root/worker/manifest keys and bounded savepoint mutations.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                constant(8),
                "Every Core read is an exact point lookup.",
            ),
            Bound(
                "native_anchor_calls",
                COUNT,
                "upper",
                constant(native),
                "Actual Neutral point-header calls; no journal body or prefix replay.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    scale(multiplier, dim("metadata_octets")),
                    "Complete logical metadata bytes, including original coordinates and counters.",
                )
                for metric, multiplier in (
                    ("parameter_octets", 32),
                    ("returned_octets", 8),
                    ("written_octets", 8),
                    ("encoded_octets", 8),
                    ("hashed_octets", 8),
                )
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        constant(8192),
                        work_scale(
                            2048,
                            Expression(
                                "log2_ceil",
                                terms=(
                                    Expression(
                                        "sum", terms=(dim("registered_inputs"), constant(2))
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Indexed Core primary/unique-key work on the qualified SQLite engine.",
            ),
        ),
        conditions=(
            "The owner retains current archive/build authority and the exact kernel lease.",
            "The maintained schema has its primary/unique keys and no added triggers.",
            "Preparation is outside a write transaction; changes use the owner transaction.",
        ),
        invariants=(
            "Expected cardinality comes from the authenticated archive root, before corpus work.",
            "Each qualified complete journal contributes once; exact retries add no copies.",
            "Membership compares the complete original anchor, and returns only a boolean.",
            "A manifest identity references root authority and is never an access grant.",
        ),
        limitations=(
            "Native primitive point-header costs remain the Neutral owner's composed child costs.",
            "Source-owner construction, global catalog currentness, input selection closure and "
            "kernel custody have separate declarations; this is the complete-source set only.",
            "SQLite VM counts exclude internal byte comparisons and physical I/O; metadata byte "
            "observations remain separate.",
        ),
    )


@declare_growth(_declaration("begin"))
def begin_corpus_source_inputs(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> CorpusSourceInputProgress:
    _parent(db, key, admission)
    expected = (
        key.worker_id,
        _manifest(admission),
        format_scalar("nonnegative", admission.root.journal_count),
    )
    with _atomic(db):
        old = _one(
            db,
            "SELECT worker_id,manifest_json,expected_sources "
            "FROM discovery_corpus_source_manifests WHERE build_id=? AND admission_sha256=?",
            (key.build_id, key.admission_sha256),
        )
        if old is None:
            db.execute(
                "INSERT INTO discovery_corpus_source_manifests VALUES (?,?,?,?,?,'0','0',0)",
                (key.build_id, key.admission_sha256, *expected),
            )
        elif old != expected:
            raise StaleArchiveTask("corpus input registration differs from its archive")
        return _header(db, key, admission)


@declare_growth(_declaration("prepare"))
def prepare_corpus_source_input(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusSourceInputWork
) -> PreparedCorpusSourceInput:
    if db.in_transaction:
        raise ValueError("corpus source preparation requires no active transaction")
    progress = _header(db, key, owner.admission)
    owner.check_source()
    anchor = owner.source.anchor_at(owner.source.entry_count - 1)
    if anchor != owner.anchor.to_mapping():
        raise StaleArchiveTask("corpus source terminal coordinates changed")
    complete = _one(
        db,
        "SELECT source_bytes,source_sha256 FROM discovery_archive_journals "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
        (key.build_id, key.admission_sha256, owner.anchor.journal_id),
    )
    if complete != (
        format_scalar("nonnegative", owner.anchor.prefix_bytes),
        owner.anchor.prefix_sha256,
    ):
        raise StaleArchiveTask("corpus input is not the complete root-selected journal")
    page = PreparedCorpusSourceInput(
        key,
        progress.version,
        owner.source_key,
        owner.source_binding_json,
        owner.anchor,
        canonical_json_bytes(anchor).decode(),
    )
    owner._prepared = page
    return page


@declare_growth(_declaration("apply"))
def apply_corpus_source_input(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusSourceInputWork,
    page: PreparedCorpusSourceInput,
) -> CorpusSourceInputProgress:
    if owner._prepared is not page or page.key != key:
        raise StaleArchiveTask("corpus input requires its exact preparation")
    owner.check_source()
    owner.source.check_open()
    with _atomic(db):
        progress = _header(db, key, owner.admission)
        expected = page.source_key, page.source_binding_json, page.anchor_json
        old = _one(
            db,
            "SELECT source_key=? AND source_binding_json=? AND anchor_json=? "
            "FROM discovery_corpus_source_inputs "
            "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
            (*expected, key.build_id, key.admission_sha256, page.anchor.journal_id),
        )
        if old is not None:
            if old != (1,):
                raise StaleArchiveTask("corpus input conflicts with original source coordinates")
            return progress
        collision = _one(
            db,
            "SELECT 1 "
            "FROM discovery_corpus_source_inputs "
            "WHERE build_id=? AND admission_sha256=? AND source_key=?",
            (key.build_id, key.admission_sha256, page.source_key),
        )
        if collision is not None:
            raise StaleArchiveTask("corpus source digest names different original coordinates")
        if progress.complete or progress.version != page.expected_version:
            raise StaleArchiveTask("corpus input preparation is stale or frozen")
        registered = progress.registered + 1
        if registered > owner.admission.root.journal_count:
            raise StaleArchiveTask("corpus inputs exceed the independent archive count")
        db.execute(
            "INSERT INTO discovery_corpus_source_inputs VALUES (?,?,?,?,?,?)",
            (key.build_id, key.admission_sha256, page.anchor.journal_id, *expected),
        )
        changed = db.execute(
            "UPDATE discovery_corpus_source_manifests SET registered_sources=?,version=? "
            "WHERE build_id=? AND admission_sha256=? AND worker_id=? AND version=? AND complete=0",
            (
                format_scalar("nonnegative", registered),
                format_scalar("nonnegative", progress.version + 1),
                key.build_id,
                key.admission_sha256,
                key.worker_id,
                format_scalar("nonnegative", progress.version),
            ),
        ).rowcount
        if changed != 1:
            raise StaleArchiveTask("corpus input worker changed")
        return CorpusSourceInputProgress(progress.version + 1, registered, False)


@declare_growth(_declaration("complete"))
def complete_corpus_source_inputs(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> FrozenCorpusSourceInputs:
    with _atomic(db):
        progress = _header(db, key, admission)
        if progress.registered != admission.root.journal_count:
            raise StaleArchiveTask("corpus input manifest is incomplete")
        if not progress.complete:
            changed = db.execute(
                "UPDATE discovery_corpus_source_manifests SET complete=1,version=? "
                "WHERE build_id=? AND admission_sha256=? AND worker_id=? AND version=?",
                (
                    format_scalar("nonnegative", progress.version + 1),
                    key.build_id,
                    key.admission_sha256,
                    key.worker_id,
                    format_scalar("nonnegative", progress.version),
                ),
            ).rowcount
            if changed != 1:
                raise StaleArchiveTask("corpus input completion worker changed")
        return FrozenCorpusSourceInputs(
            admission.identity,
            sha256(_manifest(admission).encode()).hexdigest(),
            admission.root.journal_count,
        )


@declare_growth(_declaration("membership"))
def corpus_source_admitted(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    manifest: FrozenCorpusSourceInputs,
    anchor: HistoryJournalAnchor,
) -> bool:
    progress = _header(db, key, admission)
    expected = FrozenCorpusSourceInputs(
        admission.identity,
        sha256(_manifest(admission).encode()).hexdigest(),
        admission.root.journal_count,
    )
    if not progress.complete or manifest != expected:
        raise StaleArchiveTask("corpus input manifest is not the exact completed source set")
    source_binding = journal_source_binding(admission.identity, anchor.to_mapping())
    row = _one(
        db,
        "SELECT anchor_json=? AND source_binding_json=? AND source_key=? "
        "FROM discovery_corpus_source_inputs "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
        (
            canonical_json_bytes(anchor.to_mapping()).decode(),
            source_binding,
            sha256(source_binding.encode()).hexdigest(),
            key.build_id,
            key.admission_sha256,
            anchor.journal_id,
        ),
    )
    return bool(row == (1,))
