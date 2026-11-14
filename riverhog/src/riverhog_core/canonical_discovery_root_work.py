"""Resume original root incidences into exact Native snapshots and shared prefixes."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, replace

from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
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

from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_corpus_inputs import (
    FrozenCorpusSourceInputs,
    _require_frozen_sources,
)
from riverhog_core.canonical_discovery_history_inputs import (
    CorpusHistoryInput,
    _atomic,
    _change_counts,
    _count,
    _input,
    _one,
    corpus_history_progress,
)
from riverhog_core.canonical_discovery_history_work import HistorySetWork
from riverhog_core.canonical_discovery_prefix_inputs import (
    CorpusPrefixWork,
    PreparedCorpusPrefix,
    apply_corpus_prefix,
    corpus_prefix_maximum,
    initialize_corpus_prefix_inputs,
    prepare_corpus_prefix,
)
from riverhog_core.canonical_discovery_snapshot_work import (
    HistorySnapshotWork,
    PreparedHistorySnapshot,
    apply_history_snapshot,
    history_snapshot_progress,
    initialize_snapshot_work,
    prepare_history_snapshot,
)


@dataclass(frozen=True, slots=True)
class CorpusRootProgress:
    requirement: CorpusHistoryInput
    version: int
    bound_after: str | None
    retained_after: str | None
    bound_records: int
    retained_records: int
    bound_complete: bool
    retained_complete: bool
    receipt_json: str | None


@dataclass(frozen=True, slots=True)
class PreparedCorpusRoot:
    key: ArchiveTaskKey
    expected: CorpusRootProgress
    action: str
    snapshot: PreparedHistorySnapshot | None
    prefix: PreparedCorpusPrefix | None


class CorpusRootWork:
    """Retain one root set and one borrowed complete Native source through apply."""

    def __init__(self, history: HistorySetWork, manifest: FrozenCorpusSourceInputs) -> None:
        self.history = history
        self.manifest = manifest
        self._prepared: PreparedCorpusRoot | None = None
        self._snapshot: HistorySnapshotWork | None = None
        self._source: CorpusPrefixWork | None = None


def initialize_corpus_root_work(db: sqlite3.Connection) -> None:
    initialize_corpus_prefix_inputs(db)
    initialize_snapshot_work(db)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_corpus_root_cursors (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, records_sha256 TEXT NOT NULL,
            source_manifest_sha256 TEXT NOT NULL, version TEXT NOT NULL,
            bound_after TEXT, retained_after TEXT,
            bound_records TEXT NOT NULL, retained_records TEXT NOT NULL,
            bound_complete INTEGER NOT NULL CHECK(bound_complete IN (0,1)),
            retained_complete INTEGER NOT NULL CHECK(retained_complete IN (0,1)),
            receipt_json TEXT,
            PRIMARY KEY(build_id,admission_sha256,records_sha256)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS discovery_history_retained_roots
        ON discovery_history_set_records(build_id,admission_sha256,records_sha256,record_key)
        WHERE bound_journal IS NULL;
    """)


def _last_receipt(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusRootWork, progress: CorpusRootProgress
) -> None:
    if progress.receipt_json is None:
        if progress.bound_records or progress.retained_records:
            raise StaleArchiveTask("root continuation lacks its original child receipt")
        return
    value = require_canonical_json(progress.receipt_json.encode())
    if not isinstance(value, dict) or set(value) != {
        "action",
        "record_key",
        "record_json",
        "source_key",
        "binding_json",
        "anchor_json",
    }:
        raise StaleArchiveTask("root child receipt is invalid")
    receipt: dict[str, str] = {}
    for name, item in value.items():
        if not isinstance(item, str):
            raise StaleArchiveTask("root child receipt fields must be original strings")
        receipt[name] = item
    action = receipt["action"]
    if action not in ("bound", "retained"):
        raise StaleArchiveTask("root child receipt has an invalid phase")
    root = MemberHistoryRoot.from_mapping(require_canonical_json(receipt["record_json"].encode()))
    position = root.journal.journal_id if action == "bound" else root.key
    expected_position = progress.bound_after if action == "bound" else progress.retained_after
    if (
        root.key != receipt["record_key"]
        or root.inclusion != action
        or position != expected_position
        or (action == "bound" and progress.retained_records != 0)
        or root.key != progress.requirement.after_key
        or canonical_json_bytes(root.journal.to_mapping()).decode() != receipt["anchor_json"]
    ):
        raise StaleArchiveTask("root child receipt differs from its original continuation")
    original = _one(
        db,
        "SELECT value_json,bound_journal FROM discovery_history_set_records "
        "WHERE build_id=? AND admission_sha256=? AND records_sha256=? AND record_key=?",
        (key.build_id, key.admission_sha256, owner.history.authority.records_sha256, root.key),
    )
    if original != (receipt["record_json"], root.journal.journal_id if action == "bound" else None):
        raise StaleArchiveTask("root child receipt differs from the exact original record")
    child = _one(
        db,
        "SELECT l.source_key,l.prefix_sha256,l.inclusion,s.binding_json,h.anchor_json,"
        "p.anchor_json,i.source_binding_json FROM discovery_history_snapshot_links AS l "
        "JOIN discovery_snapshot_sources AS s "
        "ON s.build_id=l.build_id AND s.admission_sha256=l.admission_sha256 "
        "AND s.source_key=l.source_key "
        "JOIN discovery_snapshot_headers AS h "
        "ON h.build_id=l.build_id AND h.admission_sha256=l.admission_sha256 "
        "AND h.source_key=l.source_key AND h.prefix_sha256=l.prefix_sha256 "
        "JOIN discovery_corpus_prefix_inputs AS p "
        "ON p.build_id=l.build_id AND p.admission_sha256=l.admission_sha256 "
        "AND p.source_key=l.source_key AND p.prefix_sha256=l.prefix_sha256 "
        "JOIN discovery_corpus_source_inputs AS i "
        "ON i.build_id=l.build_id AND i.admission_sha256=l.admission_sha256 "
        "AND i.source_key=l.source_key "
        "WHERE l.build_id=? AND l.admission_sha256=? AND l.root_set_sha256=? AND l.record_key=?",
        (key.build_id, key.admission_sha256, owner.history.authority.records_sha256, root.key),
    )
    if child != (
        receipt["source_key"],
        root.journal.prefix_sha256,
        action,
        receipt["binding_json"],
        receipt["anchor_json"],
        receipt["anchor_json"],
        receipt["binding_json"],
    ):
        raise StaleArchiveTask("root child receipt lacks exact snapshot/prefix/source children")
    maximum = corpus_prefix_maximum(
        db, key, owner.history.admission, owner.manifest, receipt["source_key"]
    )
    if (
        maximum is None
        or maximum.source_binding_json != receipt["binding_json"]
        or maximum.anchor.journal_id != root.journal.journal_id
        or maximum.anchor.through_sequence < root.journal.through_sequence
    ):
        raise StaleArchiveTask("root child receipt lacks its current shared prefix maximum")


def _declaration(mode: str) -> Declaration:
    subject = {
        "progress": "corpus_root_progress",
        "next": "_next_record",
        "prepare": "prepare_corpus_root",
        "apply": "apply_corpus_root",
    }[mode]

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def width(value: int) -> Expression:
        return Expression(
            "product",
            terms=(
                Expression("constant", value=value, unit=()),
                Expression("dimension", name="metadata_octets"),
            ),
        )

    return Declaration(
        id="riverhog-server.discovery-corpus-root-" + mode + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_root_work." + subject,),
        scope="one original root continuation, indexed selection or composed child advance",
        starts_at="receive exact admitted root-set authority, fixed sources and original cursor",
        ends_at="detach one original root or commit snapshot, prefix, receipt and cursor",
        dimensions=(
            Dimension("registered_inputs", COUNT, "All retained metadata and original index rows."),
            Dimension(
                "metadata_octets", OCTETS, "Largest complete input, row or detached receipt."
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(1 if mode == "next" else 128),
                "Fixed indexed parent/child checks and owner savepoints.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(1 if mode == "next" else 80),
                "Point metadata checks and at most one original indexed root.",
            ),
            Bound(
                "source_reads",
                COUNT,
                "equal",
                count(0),
                "Original root objects are already independently authenticated.",
            ),
            Bound(
                "source_read_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No source page or complete journal is replayed.",
            ),
            Bound(
                "native_anchor_calls",
                COUNT,
                "upper",
                count({"prepare": 4, "apply": 2}.get(mode, 0)),
                "Exact complete-source lease fences and selected Native point headers.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    width(256),
                    "Complete parent and composed child metadata preimages and row bytes.",
                )
                for metric in (
                    "parameter_octets",
                    "returned_octets",
                    "written_octets",
                    "encoded_octets",
                    "hashed_octets",
                )
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(64 if mode == "next" else 8192),
                        Expression(
                            "product",
                            terms=(
                                count(16 if mode == "next" else 1024),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("dimension", name="registered_inputs"),
                                                count(2),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Maintained point/partial-index range seeks; opposite extents are not scanned.",
            ),
        ),
        conditions=(
            "The caller retains current archive/catalog/build authority and exclusive writes.",
            "Source admission and structural root-set verification have independent terminals.",
            "The selector receives the original cursor validated by its composing parent.",
            "The resolver lends one already admitted source through application with bounded cost.",
            "Preparation is outside writes; application uses the current owner's transaction.",
            "The maintained schema has no extra triggers; rollback injection has separate checks.",
        ),
        invariants=(
            "Separate original bound and retained-only positions survive required extent upgrades.",
            "Independent original counts, not Native outputs, determine extent completion.",
            "Snapshot incidence, exact prefix, original cursor and queue counters commit together.",
            "A shared root set is consumed once without member-by-root expansion.",
            "Queue completion grants no Native corpus, authorization or publication terminal.",
        ),
        limitations=(
            "Native owns the composed point-header SQL, codecs, hashing and padding costs.",
            "Complete-source admission, source extraction and lease acquisition are separate work.",
            "VM observations exclude internal comparisons, I/O and lock wait; elapsed is observed.",
        ),
    )


@declare_growth(_declaration("progress"))
def corpus_root_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusRootWork
) -> CorpusRootProgress:
    history = owner.history
    _require_frozen_sources(db, key, history.admission, owner.manifest)
    corpus_history_progress(db, key, history.admission)
    task = _input(db, key, "roots", history.authority.records_sha256)
    if task.authority != history.authority:
        raise StaleArchiveTask("root requirement has another exact original authority")
    bound = history_snapshot_progress(db, key, history, extent=BOUND_HISTORY_EXTENT)
    retained = history_snapshot_progress(db, key, history, extent=RETAINED_HISTORY_EXTENT)
    row = _one(
        db,
        "SELECT source_manifest_sha256,version,bound_after,retained_after,bound_records,"
        "retained_records,bound_complete,retained_complete,receipt_json "
        "FROM discovery_corpus_root_cursors "
        "WHERE build_id=? AND admission_sha256=? AND records_sha256=?",
        (key.build_id, key.admission_sha256, history.authority.records_sha256),
    )
    if row is None:
        progress = CorpusRootProgress(task, 0, None, None, 0, 0, False, False, None)
        if task.records != 0 or task.after_key is not None or not task.pending:
            raise StaleArchiveTask("advanced root requirement has no original continuation")
    else:
        if row[0] != owner.manifest.manifest_sha256:
            raise StaleArchiveTask("root continuation has another fixed source manifest")
        progress = CorpusRootProgress(
            task,
            _count(row[1]),
            row[2],
            row[3],
            _count(row[4]),
            _count(row[5]),
            bool(row[6]),
            bool(row[7]),
            row[8],
        )
    expected_retained = retained.expected_roots - bound.expected_roots
    if (
        progress.bound_records != bound.registered_roots
        or progress.retained_records != retained.registered_roots - bound.registered_roots
        or (task.after_key is None) != (task.records == 0)
        or task.records != progress.bound_records + progress.retained_records
        or progress.version
        != task.records + int(progress.bound_complete) + int(progress.retained_complete)
        or (progress.bound_after is None) != (progress.bound_records == 0)
        or (progress.retained_after is None) != (progress.retained_records == 0)
        or (progress.bound_complete and not bound.complete)
        or (progress.retained_complete and progress.retained_records != expected_retained)
        or (
            (progress.retained_records or progress.retained_complete)
            and not progress.bound_complete
        )
        or (
            not task.pending
            and (
                not progress.bound_complete
                or (
                    task.required_extent == RETAINED_HISTORY_EXTENT
                    and not progress.retained_complete
                )
            )
        )
        or (
            task.required_extent == BOUND_HISTORY_EXTENT
            and (progress.retained_records or progress.retained_complete)
        )
    ):
        raise StaleArchiveTask("root original continuation and independent coverage disagree")
    complete = progress.bound_complete and (
        task.required_extent == BOUND_HISTORY_EXTENT or progress.retained_complete
    )
    if task.pending == complete:
        raise StaleArchiveTask("root pending requirement and extent terminals disagree")
    _last_receipt(db, key, owner, progress)
    return progress


@declare_growth(_declaration("next"))
def _next_record(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusRootWork,
    progress: CorpusRootProgress,
    action: str,
) -> tuple[str, str] | None:
    position = progress.bound_after if action == "bound" else progress.retained_after
    column = "bound_journal" if action == "bound" else "record_key"
    condition = "bound_journal IS NOT NULL" if action == "bound" else "bound_journal IS NULL"
    index = (
        "discovery_history_bound_heads" if action == "bound" else "discovery_history_retained_roots"
    )
    row = _one(
        db,
        "SELECT record_key,value_json FROM discovery_history_set_records "
        f"INDEXED BY {index} "
        f"WHERE build_id=? AND admission_sha256=? AND records_sha256=? AND {condition} "
        + (f"AND {column}>? " if position is not None else "")
        + f"ORDER BY {column} LIMIT 1",
        (key.build_id, key.admission_sha256, owner.history.authority.records_sha256)
        + (() if position is None else (position,)),
    )

    if row is None:
        return None
    record_key, value_json = row
    if not isinstance(record_key, str) or not isinstance(value_json, str):
        raise StaleArchiveTask("root original indexed row is not exact encoded metadata")
    return record_key, value_json


@declare_growth(_declaration("prepare"))
def prepare_corpus_root(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusRootWork,
    *,
    resolve: Callable[[str], CorpusPrefixWork],
) -> PreparedCorpusRoot:
    if db.in_transaction:
        raise ValueError("root preparation requires no active transaction")
    progress = corpus_root_progress(db, key, owner)
    if not progress.requirement.pending:
        raise StaleArchiveTask("root original selection is already complete")
    action = "bound" if not progress.bound_complete else "retained"
    row = _next_record(db, key, owner, progress, action)
    owner._source = None
    owner._snapshot = None
    if row is None:
        page = PreparedCorpusRoot(key, progress, action + "-done", None, None)
    else:
        root = MemberHistoryRoot.from_mapping(require_canonical_json(row[1].encode()))
        if root.key != row[0] or root.inclusion != action:
            raise StaleArchiveTask("root indexed selection differs from its original coordinates")
        source = resolve(root.journal.journal_id)
        if (
            source.manifest != owner.manifest
            or source.original.admission != owner.history.admission
        ):
            raise StaleArchiveTask("root Native source has another fixed manifest or archive")
        snapshot = HistorySnapshotWork(owner.history, source.original.source)
        child = prepare_history_snapshot(db, key, snapshot, row[0])
        prefix = prepare_corpus_prefix(db, key, source, root.journal)
        owner._source, owner._snapshot = source, snapshot
        page = PreparedCorpusRoot(key, progress, action, child, prefix)
    owner._prepared = page
    return page


@declare_growth(_declaration("apply"))
def apply_corpus_root(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusRootWork, page: PreparedCorpusRoot
) -> CorpusRootProgress:
    if owner._prepared is not page or page.key != key:
        raise StaleArchiveTask("root seed requires its exact detached preparation")
    with _atomic(db):
        current = corpus_root_progress(db, key, owner)
        if current != page.expected:
            raise StaleArchiveTask("root original continuation or required extent changed")
        next_progress = replace(current, version=current.version + 1)
        after = current.requirement.after_key
        pending = True
        if page.action in ("bound", "retained"):
            if (
                page.snapshot is None
                or page.prefix is None
                or owner._snapshot is None
                or owner._source is None
            ):
                raise StaleArchiveTask("root seed lacks its exact borrowed children")
            apply_history_snapshot(db, key, owner._snapshot, page.snapshot)
            apply_corpus_prefix(db, key, owner._source, page.prefix)
            root = MemberHistoryRoot.from_mapping(
                require_canonical_json(page.snapshot.record_json.encode())
            )
            receipt = canonical_json_bytes(
                {
                    "action": page.action,
                    "record_key": root.key,
                    "record_json": page.snapshot.record_json,
                    "source_key": page.snapshot.source_key,
                    "binding_json": page.snapshot.source_binding_json,
                    "anchor_json": page.snapshot.anchor_json,
                }
            ).decode()
            after = root.key
            if page.action == "bound":
                next_progress = replace(
                    next_progress,
                    bound_after=root.journal.journal_id,
                    bound_records=current.bound_records + 1,
                    receipt_json=receipt,
                )
            else:
                next_progress = replace(
                    next_progress,
                    retained_after=root.key,
                    retained_records=current.retained_records + 1,
                    receipt_json=receipt,
                )
        elif page.action == "bound-done":
            coverage = history_snapshot_progress(
                db, key, owner.history, extent=BOUND_HISTORY_EXTENT
            )
            if not coverage.complete or _next_record(db, key, owner, current, "bound") is not None:
                raise StaleArchiveTask("root bound terminal omits original incidences")
            next_progress = replace(next_progress, bound_complete=True)
            pending = current.requirement.required_extent == RETAINED_HISTORY_EXTENT
        elif page.action == "retained-done":
            coverage = history_snapshot_progress(
                db, key, owner.history, extent=RETAINED_HISTORY_EXTENT
            )
            if (
                not coverage.complete
                or _next_record(db, key, owner, current, "retained") is not None
            ):
                raise StaleArchiveTask("root retained terminal omits original incidences")
            next_progress = replace(next_progress, retained_complete=True)
            pending = False
        else:
            raise StaleArchiveTask("root seed has an invalid action")
        values = (
            owner.manifest.manifest_sha256,
            str(next_progress.version),
            next_progress.bound_after,
            next_progress.retained_after,
            str(next_progress.bound_records),
            str(next_progress.retained_records),
            int(next_progress.bound_complete),
            int(next_progress.retained_complete),
            next_progress.receipt_json,
        )
        db.execute(
            "INSERT INTO discovery_corpus_root_cursors VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(build_id,admission_sha256,records_sha256) DO UPDATE SET "
            "source_manifest_sha256=excluded.source_manifest_sha256,version=excluded.version,"
            "bound_after=excluded.bound_after,retained_after=excluded.retained_after,"
            "bound_records=excluded.bound_records,retained_records=excluded.retained_records,"
            "bound_complete=excluded.bound_complete,retained_complete=excluded.retained_complete,"
            "receipt_json=excluded.receipt_json",
            (key.build_id, key.admission_sha256, owner.history.authority.records_sha256, *values),
        ).close()
        db.execute(
            "UPDATE discovery_corpus_history_inputs SET version=?,records=?,after_key=?,pending=? "
            "WHERE build_id=? AND admission_sha256=? AND kind='roots' AND records_sha256=?",
            (
                str(current.requirement.version + 1),
                str(next_progress.bound_records + next_progress.retained_records),
                after,
                int(pending),
                key.build_id,
                key.admission_sha256,
                owner.history.authority.records_sha256,
            ),
        ).close()
        if not pending:
            _change_counts(db, key, owner.history.admission, pending_roots=-1)
        return corpus_root_progress(db, key, owner)
