"""Exact admitted prefix requests, shared by complete original journal source."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes, format_scalar, require_canonical_json
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
from riverhog_core.canonical_discovery_corpus_inputs import (
    CorpusSourceInputWork,
    FrozenCorpusSourceInputs,
    _natural,
    _one,
    _require_frozen_sources,
    corpus_source_admitted,
    initialize_corpus_source_inputs,
)
from riverhog_core.canonical_discovery_journal_work import QualifiedJournalSource


@dataclass(frozen=True, slots=True)
class CorpusPrefixProgress:
    version: int
    prefixes: int
    journals: int
    pending: int
    closed: bool


@dataclass(frozen=True, slots=True)
class PreparedCorpusPrefix:
    key: ArchiveTaskKey
    expected_version: int
    source_key: str
    source_binding_json: str
    anchor: HistoryJournalAnchor
    anchor_json: str


@dataclass(frozen=True, slots=True)
class CorpusPrefixMaximum:
    source_key: str
    source_binding_json: str
    anchor: HistoryJournalAnchor
    version: int
    pending: bool


class CorpusPrefixWork:
    """Retain one exact admitted complete source and its detached preparation."""

    def __init__(
        self,
        admission: ArchiveDiscoveryAdmission,
        manifest: FrozenCorpusSourceInputs,
        source: QualifiedJournalSource,
    ) -> None:
        self.original = CorpusSourceInputWork(admission, source)
        self.manifest = manifest
        self._prepared: PreparedCorpusPrefix | None = None


def initialize_corpus_prefix_inputs(db: sqlite3.Connection) -> None:
    initialize_corpus_source_inputs(db)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_corpus_prefix_headers (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, worker_id TEXT NOT NULL,
            source_manifest_sha256 TEXT NOT NULL, prefixes TEXT NOT NULL, journals TEXT NOT NULL,
            pending_sources TEXT NOT NULL, version TEXT NOT NULL,
            closed INTEGER NOT NULL CHECK(closed IN (0,1)),
            PRIMARY KEY(build_id,admission_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_corpus_prefix_inputs (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, source_key TEXT NOT NULL,
            prefix_sha256 TEXT NOT NULL, anchor_json TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,source_key,prefix_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_corpus_prefix_maxima (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, source_key TEXT NOT NULL,
            binding_json TEXT NOT NULL, anchor_json TEXT NOT NULL, version TEXT NOT NULL,
            pending INTEGER NOT NULL CHECK(pending IN (0,1)),
            PRIMARY KEY(build_id,admission_sha256,source_key)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS discovery_corpus_dependency_pending
        ON discovery_corpus_prefix_maxima(build_id,admission_sha256,source_key)
        WHERE pending=1;
        CREATE TABLE IF NOT EXISTS discovery_corpus_dependency_cursors (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, source_key TEXT NOT NULL,
            version TEXT NOT NULL, fork_complete INTEGER NOT NULL CHECK(fork_complete IN (0,1)),
            after_json TEXT, active_json TEXT, after_node TEXT, scanned_through TEXT,
            receipt_json TEXT, PRIMARY KEY(build_id,admission_sha256,source_key)
        ) WITHOUT ROWID;
    """)


@contextmanager
def _atomic(db: sqlite3.Connection) -> Iterator[None]:
    if not db.in_transaction:
        raise ValueError("prefix registration requires an owner-managed transaction")
    db.execute("SAVEPOINT discovery_corpus_prefix_inputs")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_corpus_prefix_inputs")
        db.execute("RELEASE discovery_corpus_prefix_inputs")
        raise
    else:
        db.execute("RELEASE discovery_corpus_prefix_inputs")


def _header(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    manifest: FrozenCorpusSourceInputs,
) -> CorpusPrefixProgress:
    _require_frozen_sources(db, key, admission, manifest)
    row = _one(
        db,
        "SELECT worker_id,source_manifest_sha256,prefixes,journals,pending_sources,version,closed "
        "FROM discovery_corpus_prefix_headers WHERE build_id=? AND admission_sha256=?",
        (key.build_id, key.admission_sha256),
    )
    if row is None or row[:2] != (key.worker_id, manifest.manifest_sha256):
        raise StaleArchiveTask("prefix registry has another current source manifest or worker")
    if row[6] not in (0, 1):
        raise StaleArchiveTask("prefix registry has an invalid selection seal")
    prefixes, journals, pending, version = map(_natural, row[2:6])
    if (
        version != prefixes
        or journals > manifest.source_count
        or journals > prefixes
        or pending > journals
        or (prefixes == 0) != (journals == 0)
    ):
        raise StaleArchiveTask("prefix registry counters differ from admitted sources")
    return CorpusPrefixProgress(version, prefixes, journals, pending, bool(row[6]))


def _declaration(mode: str) -> Declaration:
    subject = {
        "begin": "begin_corpus_prefix_inputs",
        "progress": "corpus_prefix_progress",
        "prepare": "prepare_corpus_prefix",
        "apply": "apply_corpus_prefix",
        "maximum": "corpus_prefix_maximum",
    }[mode]

    def constant(value: int, unit: Unit = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def width(multiplier: int) -> Expression:
        return Expression(
            "product",
            terms=(
                constant(multiplier, ()),
                Expression("dimension", name="metadata_octets"),
            ),
        )

    return Declaration(
        id="riverhog-server.discovery-corpus-prefix-" + mode + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_prefix_inputs." + subject,),
        scope="one admitted prefix-registry metadata operation",
        starts_at="receive the exact fixed source manifest and archive/build/worker",
        ends_at="detach one request/progress/maximum or commit its exact original prefix",
        dimensions=(
            Dimension(
                "registered_inputs", COUNT, "All retained source/prefix control and data rows."
            ),
            Dimension(
                "metadata_octets", OCTETS, "Largest complete input or selected metadata row."
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                constant(20),
                "Fixed point checks and bounded savepoint mutations.",
            ),
            Bound(
                "returned_rows", COUNT, "upper", constant(12), "Every read selects an exact key."
            ),
            Bound(
                "native_anchor_calls",
                COUNT,
                "upper",
                constant({"prepare": 2, "apply": 1}.get(mode, 0)),
                "Native point headers retain exact source/anchor coordinates.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    width(multiplier),
                    "Complete logical metadata preimages and parameter/row bytes.",
                )
                for metric, multiplier in (
                    ("parameter_octets", 64),
                    ("returned_octets", 12),
                    ("written_octets", 32),
                    ("encoded_octets", 64),
                    ("hashed_octets", 64),
                )
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        constant(16384),
                        Expression(
                            "product",
                            terms=(
                                constant(2048),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("dimension", name="registered_inputs"),
                                                constant(2),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Indexed metadata work on the maintained SQLite engine.",
            ),
        ),
        conditions=(
            "The owner retains source custody, kernel lease and current catalog authority.",
            "The original complete-source manifest is independently fixed and fully registered.",
            "Prepare outside writes; apply inside the owner transaction on the maintained schema.",
        ),
        invariants=(
            "Original exact prefixes remain distinct while one maximum is retained per source.",
            "A retry never increments counters or moves a source maximum backwards.",
            "Counts arise from exact admitted inputs, never Native or extracted output counts.",
            "The registry records inputs; it does not prove closed dependencies or grant access.",
        ),
        limitations=(
            "Native owns its composed point-header cost; only call boundaries are measured here.",
            "Source extraction, dependency scanning and fixed selection freeze are separate work.",
            "Metadata byte bounds exclude Native's header codecs and mandatory SHA padding.",
            "SQLite VM excludes internal comparisons, I/O and lock wait; elapsed time is observed.",
        ),
    )


@declare_growth(_declaration("begin"))
def begin_corpus_prefix_inputs(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    manifest: FrozenCorpusSourceInputs,
) -> CorpusPrefixProgress:
    _require_frozen_sources(db, key, admission, manifest)
    with _atomic(db):
        old = _one(
            db,
            "SELECT worker_id,source_manifest_sha256 "
            "FROM discovery_corpus_prefix_headers WHERE build_id=? AND admission_sha256=?",
            (key.build_id, key.admission_sha256),
        )
        expected = key.worker_id, manifest.manifest_sha256
        if old is None:
            db.execute(
                "INSERT INTO discovery_corpus_prefix_headers VALUES (?,?,?,?,'0','0','0','0',0)",
                (key.build_id, key.admission_sha256, *expected),
            )
        elif old != expected:
            raise StaleArchiveTask("prefix registry source manifest or worker changed")
        return _header(db, key, admission, manifest)


@declare_growth(_declaration("progress"))
def corpus_prefix_progress(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    manifest: FrozenCorpusSourceInputs,
) -> CorpusPrefixProgress:
    return _header(db, key, admission, manifest)


@declare_growth(_declaration("prepare"))
def prepare_corpus_prefix(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusPrefixWork,
    anchor: HistoryJournalAnchor,
) -> PreparedCorpusPrefix:
    if db.in_transaction:
        raise ValueError("prefix preparation requires no active transaction")
    original = owner.original
    progress = _header(db, key, original.admission, owner.manifest)
    original.check_source()
    if not corpus_source_admitted(db, key, original.admission, owner.manifest, original.anchor):
        raise StaleArchiveTask("prefix request has no exact admitted complete source")
    selected = original.source.anchor_at(anchor.through_sequence)
    if selected != anchor.to_mapping():
        raise StaleArchiveTask("prefix request differs from the exact original Native anchor")
    page = PreparedCorpusPrefix(
        key,
        progress.version,
        original.source_key,
        original.source_binding_json,
        anchor,
        canonical_json_bytes(selected).decode(),
    )
    owner._prepared = page
    return page


def _maximum(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    source_key: str,
) -> CorpusPrefixMaximum | None:
    row = _one(
        db,
        "SELECT binding_json,anchor_json,version,pending FROM discovery_corpus_prefix_maxima "
        "WHERE build_id=? AND admission_sha256=? AND source_key=?",
        (key.build_id, key.admission_sha256, source_key),
    )
    if row is None:
        existing = _one(
            db,
            "SELECT 1 FROM discovery_corpus_prefix_inputs "
            "WHERE build_id=? AND admission_sha256=? AND source_key=? LIMIT 1",
            (key.build_id, key.admission_sha256, source_key),
        )
        if existing is not None:
            raise StaleArchiveTask("original prefix inputs have no matching shared maximum")
        return None
    anchor = HistoryJournalAnchor.from_mapping(require_canonical_json(row[1].encode()))
    original = _one(
        db,
        "SELECT source_binding_json FROM discovery_corpus_source_inputs "
        "WHERE build_id=? AND admission_sha256=? AND source_key=?",
        (key.build_id, key.admission_sha256, source_key),
    )
    selected = _one(
        db,
        "SELECT anchor_json FROM discovery_corpus_prefix_inputs "
        "WHERE build_id=? AND admission_sha256=? AND source_key=? AND prefix_sha256=?",
        (key.build_id, key.admission_sha256, source_key, anchor.prefix_sha256),
    )
    if original != (row[0],) or selected != (row[1],):
        raise StaleArchiveTask("shared maximum differs from original source or prefix coordinates")
    return CorpusPrefixMaximum(source_key, row[0], anchor, _natural(row[2]), bool(row[3]))


@declare_growth(_declaration("apply"))
def apply_corpus_prefix(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusPrefixWork,
    page: PreparedCorpusPrefix,
) -> CorpusPrefixProgress:
    original = owner.original
    if (
        owner._prepared is not page
        or page.key != key
        or page.source_key != original.source_key
        or page.source_binding_json != original.source_binding_json
    ):
        raise StaleArchiveTask("prefix request requires its exact preparation")
    original.check_source()
    original.source.check_open()
    with _atomic(db):
        progress = _header(db, key, original.admission, owner.manifest)
        old = _one(
            db,
            "SELECT anchor_json=? FROM discovery_corpus_prefix_inputs "
            "WHERE build_id=? AND admission_sha256=? AND source_key=? AND prefix_sha256=?",
            (
                page.anchor_json,
                key.build_id,
                key.admission_sha256,
                page.source_key,
                page.anchor.prefix_sha256,
            ),
        )
        current = _maximum(db, key, page.source_key)
        if old is not None:
            if old != (1,) or current is None:
                raise StaleArchiveTask("prefix digest names different original coordinates")
            return progress
        if progress.closed:
            raise StaleArchiveTask("original prefix selection is sealed")
        if progress.version != page.expected_version:
            raise StaleArchiveTask("prefix preparation has a stale registry version")
        if current is not None and current.source_binding_json != page.source_binding_json:
            raise StaleArchiveTask("prefix maximum source binding changed")
        journals = progress.journals + int(current is None)
        extends = current is None or page.anchor.through_sequence > current.anchor.through_sequence
        pending = progress.pending + int(extends and (current is None or not current.pending))
        if journals > owner.manifest.source_count:
            raise StaleArchiveTask("prefix journal count exceeds the complete source manifest")
        db.execute(
            "INSERT INTO discovery_corpus_prefix_inputs VALUES (?,?,?,?,?)",
            (
                key.build_id,
                key.admission_sha256,
                page.source_key,
                page.anchor.prefix_sha256,
                page.anchor_json,
            ),
        )
        if current is None:
            db.execute(
                "INSERT INTO discovery_corpus_prefix_maxima VALUES (?,?,?,?,?,'0',1)",
                (
                    key.build_id,
                    key.admission_sha256,
                    page.source_key,
                    page.source_binding_json,
                    page.anchor_json,
                ),
            )
            db.execute(
                "INSERT INTO discovery_corpus_dependency_cursors "
                "VALUES (?,?,?,'0',0,NULL,NULL,NULL,NULL,NULL)",
                (key.build_id, key.admission_sha256, page.source_key),
            )
        elif page.anchor.through_sequence > current.anchor.through_sequence:
            db.execute(
                "UPDATE discovery_corpus_prefix_maxima SET anchor_json=?,version=?,pending=1 "
                "WHERE build_id=? AND admission_sha256=? AND source_key=?",
                (
                    page.anchor_json,
                    format_scalar("nonnegative", current.version + 1),
                    key.build_id,
                    key.admission_sha256,
                    page.source_key,
                ),
            )
        changed = db.execute(
            "UPDATE discovery_corpus_prefix_headers SET prefixes=?,journals=?,"
            "pending_sources=?,version=? "
            "WHERE build_id=? AND admission_sha256=? AND worker_id=? AND version=?",
            (
                format_scalar("nonnegative", progress.prefixes + 1),
                format_scalar("nonnegative", journals),
                format_scalar("nonnegative", pending),
                format_scalar("nonnegative", progress.version + 1),
                key.build_id,
                key.admission_sha256,
                key.worker_id,
                format_scalar("nonnegative", progress.version),
            ),
        ).rowcount
        if changed != 1:
            raise StaleArchiveTask("prefix registry worker changed")
        return CorpusPrefixProgress(
            progress.version + 1, progress.prefixes + 1, journals, pending, False
        )


@declare_growth(_declaration("maximum"))
def corpus_prefix_maximum(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    manifest: FrozenCorpusSourceInputs,
    source_key: str,
) -> CorpusPrefixMaximum | None:
    _header(db, key, admission, manifest)
    return _maximum(db, key, source_key)
