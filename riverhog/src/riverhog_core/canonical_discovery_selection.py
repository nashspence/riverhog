"""Freeze original corpus inputs before retained Native semantic validation."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from typing import Any

from riverhog_archive_contracts import (
    RETAINED_HISTORY_EXTENT,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryDocument,
    MemberHistoryImport,
    SourceMemberHistoryBindingProof,
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
from sha256_continuation import CheckpointSHA256

from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_corpus_inputs import (
    FrozenCorpusSourceInputs,
    corpus_source_admitted,
)
from riverhog_core.canonical_discovery_corpus_members import (
    _coverage as member_history_coverage,
)
from riverhog_core.canonical_discovery_corpus_members import (
    initialize_corpus_members,
)
from riverhog_core.canonical_discovery_dependency_work import (
    corpus_dependency_progress,
    next_corpus_dependency,
)
from riverhog_core.canonical_discovery_history_inputs import (
    _atomic,
    _count,
    _input,
    _one,
    corpus_history_progress,
    next_corpus_history_input,
    next_corpus_history_structure,
)
from riverhog_core.canonical_discovery_history_work import (
    ArchiveHistoryWork,
    HistorySetWork,
    history_descriptor_ready,
    history_import_progress,
)
from riverhog_core.canonical_discovery_member_work import archive_member_progress
from riverhog_core.canonical_discovery_prefix_inputs import (
    corpus_prefix_maximum,
    corpus_prefix_progress,
)
from riverhog_core.canonical_discovery_root_work import (
    CorpusRootWork,
    corpus_root_progress,
    initialize_corpus_root_work,
)

_FORMAT = "riverhog-discovery-corpus-selection/v1"
_PHASES = ("requirements", "members", "imports", "dependencies", "prefixes", "complete")
_COUNTS = ("roots", "imports", "members", "import_links", "dependencies", "prefixes")


@dataclass(frozen=True, slots=True)
class CorpusSelectionProgress:
    version: int
    phase: str
    after: tuple[str, str] | None
    counts_json: str
    required_import_links: int
    hash_state: str
    hashed_octets: int
    receipt_json: str | None
    selection_sha256: str | None


@dataclass(frozen=True, slots=True)
class FrozenCorpusSelection:
    sources: FrozenCorpusSourceInputs
    selection_sha256: str
    selection_count: int


@dataclass(frozen=True, slots=True)
class PreparedCorpusSelection:
    key: ArchiveTaskKey
    expected: CorpusSelectionProgress
    position: tuple[str, str] | None
    evidence_json: str | None
    required_import_links: int
    hash_state: str
    hashed_octets: int
    selection_sha256: str | None


class CorpusSelectionWork:
    """Retain exact archive authority and one detached bounded freeze action."""

    def __init__(self, history: ArchiveHistoryWork, sources: FrozenCorpusSourceInputs) -> None:
        self.history = history
        self.sources = sources
        self._prepared: PreparedCorpusSelection | None = None


def initialize_corpus_selection(db: sqlite3.Connection) -> None:
    initialize_corpus_members(db)
    initialize_corpus_root_work(db)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_corpus_selection (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, worker_id TEXT NOT NULL,
            source_manifest_sha256 TEXT NOT NULL, fence_json TEXT NOT NULL,
            version TEXT NOT NULL, phase TEXT NOT NULL, after_a TEXT, after_b TEXT,
            counts_json TEXT NOT NULL, required_import_links TEXT NOT NULL,
            hash_state TEXT NOT NULL, hashed_octets TEXT NOT NULL,
            receipt_json TEXT, selection_sha256 TEXT,
            PRIMARY KEY(build_id,admission_sha256),
            CHECK((after_a IS NULL) = (after_b IS NULL))
        ) WITHOUT ROWID;
    """)


def _counts(raw: str) -> dict[str, int]:
    value = require_canonical_json(raw.encode())
    if not isinstance(value, dict) or set(value) != set(_COUNTS):
        raise StaleArchiveTask("selection freeze has different original counters")
    return {name: _count(value[name]) for name in _COUNTS}


def _counts_json(value: dict[str, int]) -> str:
    return canonical_json_bytes({name: str(value[name]) for name in _COUNTS}).decode()


def _fence(db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusSelectionWork) -> str:
    admission = owner.history.admission
    prefixes = corpus_prefix_progress(db, key, admission, owner.sources)
    members = archive_member_progress(db, key)
    histories = corpus_history_progress(db, key, admission)
    if (
        not members.complete
        or members.members != admission.root.binding_count
        or member_history_coverage(db, key) != members.members
        or histories.pending_roots
        or histories.pending_imports
        or prefixes.pending
    ):
        raise StaleArchiveTask("selection freeze requires complete original seed work")
    if (
        next_corpus_history_structure(db, key, admission) is not None
        or next_corpus_history_input(db, key, admission, kind="roots") is not None
        or next_corpus_history_input(db, key, admission, kind="imports") is not None
        or next_corpus_dependency(db, key, admission, owner.sources) is not None
    ):
        raise StaleArchiveTask("selection freeze has pending original input obligations")
    return canonical_json_bytes(
        {
            "member_version": str(members.version),
            "members": str(members.members),
            "history_version": str(histories.version),
            "roots": str(histories.root_sets),
            "imports": str(histories.import_sets),
            "prefix_version": str(prefixes.version),
            "prefixes": str(prefixes.prefixes),
            "dependencies": str(prefixes.journals),
        }
    ).decode()


def _initial_hash(owner: CorpusSelectionWork, count: int) -> CheckpointSHA256:
    return CheckpointSHA256(
        canonical_json_bytes(
            {
                "format": _FORMAT,
                "admission_sha256": owner.history.admission.identity,
                "source_manifest_sha256": owner.sources.manifest_sha256,
                "prefix_count": str(count),
            }
        )
        + b"\n"
    )


def _descriptor(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusSelectionWork,
    identity: str,
    extent: str,
) -> str:
    row = _one(
        db,
        "SELECT descriptor_json FROM discovery_history_descriptors "
        "WHERE build_id=? AND admission_sha256=? AND history_sha256=?",
        (key.build_id, key.admission_sha256, identity),
    )
    if row is None or not isinstance(row[0], str):
        raise StaleArchiveTask("original incidence lost its exact history descriptor")
    raw = row[0]
    descriptor = MemberHistoryDocument.from_json_bytes(raw.encode())
    if descriptor.identity != identity or not history_descriptor_ready(
        db, key, owner.history, identity
    ):
        raise StaleArchiveTask("original history descriptor lacks its exact structural terminal")
    roots = _input(db, key, "roots", descriptor.roots.records_sha256)
    imports = _input(db, key, "imports", descriptor.imports.records_sha256)
    if (
        roots.authority != descriptor.roots
        or imports.authority != descriptor.imports
        or roots.pending
        or imports.pending
        or (extent == RETAINED_HISTORY_EXTENT and roots.required_extent != extent)
    ):
        raise StaleArchiveTask("original descriptor lacks its recorded completed history extent")
    return raw


def _selection_record(source_binding_json: str, anchor: HistoryJournalAnchor) -> str:
    return canonical_json_bytes(
        {
            "source_binding": require_canonical_json(source_binding_json.encode()),
            "anchor": anchor.to_mapping(),
        }
    ).decode()


def _prefix(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusSelectionWork,
    position: tuple[str, str],
) -> str:
    row = _one(
        db,
        "SELECT p.anchor_json,i.source_binding_json,i.anchor_json "
        "FROM discovery_corpus_prefix_inputs AS p JOIN discovery_corpus_source_inputs AS i "
        "ON i.build_id=p.build_id AND i.admission_sha256=p.admission_sha256 "
        "AND i.source_key=p.source_key "
        "WHERE p.build_id=? AND p.admission_sha256=? AND p.source_key=? AND p.prefix_sha256=?",
        (key.build_id, key.admission_sha256, *position),
    )
    if row is None:
        raise StaleArchiveTask("selected original prefix lost its complete source")
    anchor = HistoryJournalAnchor.from_mapping(require_canonical_json(row[0].encode()))
    complete = HistoryJournalAnchor.from_mapping(require_canonical_json(row[2].encode()))
    if (
        anchor.prefix_sha256 != position[1]
        or hashlib.sha256(row[1].encode()).hexdigest() != position[0]
        or anchor.journal_id != complete.journal_id
        or not corpus_source_admitted(db, key, owner.history.admission, owner.sources, complete)
    ):
        raise StaleArchiveTask("selected prefix differs from its exact original source coordinates")
    maximum = corpus_prefix_maximum(db, key, owner.history.admission, owner.sources, position[0])
    if (
        maximum is None
        or maximum.pending
        or maximum.source_binding_json != row[1]
        or maximum.anchor.through_sequence < anchor.through_sequence
    ):
        raise StaleArchiveTask("selected prefix lacks its closed original maximum")
    return _selection_record(row[1], anchor)


def _evidence(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusSelectionWork,
    phase: str,
    position: tuple[str, str],
) -> tuple[str, int]:
    admission, reader = owner.history.admission, owner.history.reader
    a, b = position
    if phase == "requirements":
        task = _input(db, key, a, b)
        if task.pending:
            raise StaleArchiveTask("selected original history input became pending")
        history = HistorySetWork(admission, reader, task.authority)
        proof: dict[str, Any] = {
            "kind": a,
            "authority": task.authority.to_mapping(),
            "extent": task.required_extent,
            "version": str(task.version),
            "after_key": task.after_key,
            "records": str(task.records),
        }
        if a == "roots":
            current = corpus_root_progress(db, key, CorpusRootWork(history, owner.sources))
            proof.update(
                bound_after=current.bound_after,
                retained_after=current.retained_after,
                bound_records=str(current.bound_records),
                retained_records=str(current.retained_records),
                bound_complete=current.bound_complete,
                retained_complete=current.retained_complete,
                receipt_json=current.receipt_json,
            )
            required = 0
        else:
            current_imports = history_import_progress(db, key, history)
            if not current_imports.complete or current_imports.registered_imports != task.records:
                raise StaleArchiveTask("original import set has incomplete incidence coverage")
            required = task.authority.record_count
        return canonical_json_bytes(proof).decode(), required
    if phase == "members":
        row = _one(
            db,
            "SELECT m.binding_json,h.worker_id,h.binding_json,h.history_sha256 "
            "FROM discovery_archive_members AS m JOIN discovery_archive_histories AS h "
            "ON h.build_id=m.build_id AND h.admission_sha256=m.admission_sha256 "
            "AND h.artifact_id=m.artifact_id "
            "WHERE m.build_id=? AND m.admission_sha256=? AND m.artifact_id=?",
            (key.build_id, key.admission_sha256, a),
        )
        if row is None or row[1] != key.worker_id or row[0] != row[2]:
            raise StaleArchiveTask("original member lost its exact history child")
        binding = MemberHistoryBinding.from_mapping(require_canonical_json(row[0].encode()))
        if binding.artifact_id != a or binding.history_sha256 != row[3]:
            raise StaleArchiveTask("original member history binding changed")
        descriptor = _descriptor(db, key, owner, row[3], RETAINED_HISTORY_EXTENT)
        return canonical_json_bytes(
            {"binding_json": row[0], "descriptor_json": descriptor}
        ).decode(), 0
    if phase == "imports":
        row = _one(
            db,
            "SELECT r.value_json,l.proof_sha256,l.history_sha256,p.worker_id,p.proof_json,"
            "p.history_sha256 FROM discovery_history_import_links AS l "
            "JOIN discovery_history_set_records AS r "
            "ON r.build_id=l.build_id AND r.admission_sha256=l.admission_sha256 "
            "AND r.records_sha256=l.import_set_sha256 AND r.record_key=l.record_key "
            "JOIN discovery_history_import_proofs AS p "
            "ON p.build_id=l.build_id AND p.admission_sha256=l.admission_sha256 "
            "AND p.proof_sha256=l.proof_sha256 "
            "WHERE l.build_id=? AND l.admission_sha256=? "
            "AND l.import_set_sha256=? AND l.record_key=?",
            (key.build_id, key.admission_sha256, a, b),
        )
        if row is None or row[3] != key.worker_id:
            raise StaleArchiveTask("original import lost its exact registered source proof")
        imported = MemberHistoryImport.from_mapping(require_canonical_json(row[0].encode()))
        if (
            imported.key != b
            or imported.source_binding_proof_sha256 != row[1]
            or imported.source_history_sha256 != row[2]
            or row[2] != row[5]
        ):
            raise StaleArchiveTask("original import incidence changed its source identity")
        task = _input(db, key, "imports", a)
        if task.pending:
            raise StaleArchiveTask("original import source set became pending")
        descriptor = _descriptor(db, key, owner, row[2], imported.extent)
        return canonical_json_bytes(
            {
                "record_json": row[0],
                "proof_json": row[4],
                "descriptor_json": descriptor,
            }
        ).decode(), 0
    if phase == "dependencies":
        current_dependency = corpus_dependency_progress(db, key, admission, owner.sources, a)
        if current_dependency.maximum.pending:
            raise StaleArchiveTask("selected original dependency source became pending")
        return canonical_json_bytes(
            {
                "source": a,
                "binding_json": current_dependency.maximum.source_binding_json,
                "anchor": current_dependency.maximum.anchor.to_mapping(),
                "version": str(current_dependency.version),
                "receipt_json": current_dependency.receipt_json,
            }
        ).decode(), 0
    if phase == "prefixes":
        return _prefix(db, key, owner, position), 0
    raise StaleArchiveTask("selection freeze has an unsupported original phase")


def _declaration(mode: str) -> Declaration:
    subject = {
        "begin": "begin_corpus_selection",
        "progress": "corpus_selection_progress",
        "next": "_next",
        "prepare": "prepare_corpus_selection",
        "apply": "apply_corpus_selection",
        "frozen": "frozen_corpus_selection",
        "membership": "corpus_selection_admitted",
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
        id="riverhog-server.discovery-corpus-selection-" + mode + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_selection." + subject,),
        scope="one sealed original-input check, indexed selection or bounded freeze advance",
        starts_at="receive exact original archive, immutable sources and current worker/cursor",
        ends_at="detach one input/progress or commit its original evidence/hash continuation",
        dimensions=(
            Dimension("registered_inputs", COUNT, "All retained metadata and original index rows."),
            Dimension(
                "metadata_octets", OCTETS, "Largest complete input, metadata row or preparation."
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(2 if mode == "next" else 256),
                "Fixed indexed owner checks, original child evidence and atomic checkpoint.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(1 if mode == "next" else 192),
                "Point metadata reads and at most one next original input.",
            ),
            Bound(
                "source_reads",
                COUNT,
                "equal",
                count(0),
                "Original authority and incidences were authenticated before sealing.",
            ),
            Bound(
                "source_read_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No archived page, journal or complete manifest is replayed.",
            ),
            Bound(
                "native_anchor_calls",
                COUNT,
                "equal",
                count(0),
                "Native point validation preceded registration of original prefix inputs.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    width(512),
                    "Complete logical owner/child metadata, codecs and cryptographic preimages.",
                )
                for metric in (
                    "parameter_octets",
                    "returned_octets",
                    "written_octets",
                    "encoded_octets",
                    "decoded_octets",
                    "hashed_octets",
                    "sha256_block_octets",
                )
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(256 if mode == "next" else 16384),
                        Expression(
                            "product",
                            terms=(
                                count(32 if mode == "next" else 2048),
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
                "Indexed point/range work, including fixed first-read schema work after reopening.",
            ),
        ),
        conditions=(
            "The current owner retains catalog/build authority and exclusive workspace custody.",
            "Each bounded preparation runs outside writes; application uses the owner transaction.",
            "The selector receives a validated source phase and original continuation.",
            "The maintained schema has no added triggers; failure injection is checked separately.",
            "Archive roots, descriptors and source proofs retain their owning metadata bounds.",
        ),
        invariants=(
            "Original counts, not Native or extracted outputs, establish each phase terminal.",
            "Original member/import primary and extent checks precede the final prefix digest.",
            "A sealed registry rejects new input while interrupted freeze remains resumable.",
            "Exact evidence, original cursor, count and hash continuation commit atomically.",
            "The private manifest grants no Native semantic, access or publication terminal.",
        ),
        limitations=(
            "Admission, extraction, Native corpus semantics and generation identity are separate.",
            "The private replay-input codec does not replace archived or generation commitments.",
            "SHA block bytes include actual neutral continuation compression and padding work.",
            "VM excludes internal comparisons, I/O and lock wait; elapsed time is observed.",
        ),
    )


@declare_growth(_declaration("next"))
def _next(
    db: sqlite3.Connection, key: ArchiveTaskKey, progress: CorpusSelectionProgress
) -> tuple[str, str] | None:
    table, first, second = {
        "requirements": ("discovery_corpus_history_inputs", "kind", "records_sha256"),
        "members": ("discovery_archive_members", "artifact_id", None),
        "imports": ("discovery_history_import_links", "import_set_sha256", "record_key"),
        "dependencies": ("discovery_corpus_prefix_maxima", "source_key", None),
        "prefixes": ("discovery_corpus_prefix_inputs", "source_key", "prefix_sha256"),
    }[progress.phase]
    base = f"SELECT {first}," + (second or "''") + f" FROM {table} "
    where = "WHERE build_id=? AND admission_sha256=? "
    parameters: tuple[object, ...] = (key.build_id, key.admission_sha256)
    if progress.after is None:
        row = _one(
            db,
            base + where + f"ORDER BY {first}" + (f",{second}" if second else "") + " LIMIT 1",
            parameters,
        )
    else:
        a, b = progress.after
        row = None
        if second is not None:
            row = _one(
                db,
                base + where + f"AND {first}=? AND {second}>? ORDER BY {second} LIMIT 1",
                (*parameters, a, b),
            )
        if row is None:
            row = _one(
                db,
                base
                + where
                + f"AND {first}>? ORDER BY {first}"
                + (f",{second}" if second else "")
                + " LIMIT 1",
                (*parameters, a),
            )
    if row is None:
        return None
    a, b = row
    if not isinstance(a, str) or not isinstance(b, str):
        raise StaleArchiveTask("selection source index has invalid original coordinates")
    return a, b


@declare_growth(_declaration("progress"))
def corpus_selection_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusSelectionWork
) -> CorpusSelectionProgress:
    fence = _fence(db, key, owner)
    if not corpus_prefix_progress(db, key, owner.history.admission, owner.sources).closed:
        raise StaleArchiveTask("selection prefix registry is not sealed")
    row = _one(
        db,
        "SELECT worker_id,source_manifest_sha256,fence_json,version,phase,after_a,after_b,"
        "counts_json,required_import_links,hash_state,hashed_octets,receipt_json,selection_sha256 "
        "FROM discovery_corpus_selection WHERE build_id=? AND admission_sha256=?",
        (key.build_id, key.admission_sha256),
    )
    if row is None or row[:3] != (key.worker_id, owner.sources.manifest_sha256, fence):
        raise StaleArchiveTask("selection freeze has another original source fence or worker")
    if row[4] not in _PHASES or (row[5] is None) != (row[6] is None):
        raise StaleArchiveTask("selection freeze has invalid phase or original cursor")
    result = CorpusSelectionProgress(
        _count(row[3]),
        row[4],
        None if row[5] is None else (row[5], row[6]),
        row[7],
        _count(row[8]),
        row[9],
        _count(row[10]),
        row[11],
        row[12],
    )
    counts = _counts(result.counts_json)
    source = require_canonical_json(fence.encode())
    assert isinstance(source, dict)
    expected = {
        name: result.required_import_links if name == "import_links" else _count(source[name])
        for name in _COUNTS
    }
    phase_index = _PHASES.index(result.phase)
    phase_counts = (
        counts["roots"] + counts["imports"],
        counts["members"],
        counts["import_links"],
        counts["dependencies"],
        counts["prefixes"],
    )
    if (
        result.version != sum(counts.values()) + phase_index
        or any(counts[name] > expected[name] for name in _COUNTS)
        or any(
            counts[name] != expected[name]
            for index, names in enumerate(
                (
                    ("roots", "imports"),
                    ("members",),
                    ("import_links",),
                    ("dependencies",),
                    ("prefixes",),
                )
            )
            if index < phase_index
            for name in names
        )
        or any(value != 0 for value in phase_counts[phase_index + 1 :])
        or ((result.phase == "complete") != (result.selection_sha256 is not None))
    ):
        raise StaleArchiveTask("selection freeze counters differ from independent original inputs")
    state = CheckpointSHA256.from_state(result.hash_state)
    if state.total_bytes != result.hashed_octets:
        raise StaleArchiveTask("selection hash continuation has another original byte position")
    if result.phase not in ("prefixes", "complete") or counts["prefixes"] == 0:
        initial = _initial_hash(owner, expected["prefixes"])
        if state.export_state() != initial.export_state():
            raise StaleArchiveTask("selection hash advanced before original prefix inputs")
    if result.receipt_json is None:
        current_count = (
            counts["prefixes"] if result.phase == "complete" else phase_counts[phase_index]
        )
        if result.after is not None or current_count:
            raise StaleArchiveTask("selection continuation lost its last original receipt")
    else:
        current_count = (
            counts["prefixes"] if result.phase == "complete" else phase_counts[phase_index]
        )
        if current_count == 0:
            raise StaleArchiveTask("selection receipt has no original counted input")
        receipt = require_canonical_json(result.receipt_json.encode())
        if not isinstance(receipt, dict) or set(receipt) != {
            "phase",
            "a",
            "b",
            "evidence_json",
            "hash_state",
        }:
            raise StaleArchiveTask("selection original receipt is invalid")
        phase = "prefixes" if result.phase == "complete" else result.phase
        if (
            result.after is None
            or receipt["phase"] != phase
            or (receipt["a"], receipt["b"]) != result.after
            or receipt["hash_state"] != result.hash_state
            or _evidence(db, key, owner, phase, result.after)[0] != receipt["evidence_json"]
        ):
            raise StaleArchiveTask("selection receipt differs from its exact original input")
    if result.phase == "complete" and state.hexdigest() != result.selection_sha256:
        raise StaleArchiveTask("selection terminal differs from its exact hash continuation")
    return result


@declare_growth(_declaration("begin"))
def begin_corpus_selection(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusSelectionWork
) -> CorpusSelectionProgress:
    with _atomic(db):
        fence = _fence(db, key, owner)
        prefix = corpus_prefix_progress(db, key, owner.history.admission, owner.sources)
        existing = _one(
            db,
            "SELECT 1 FROM discovery_corpus_selection WHERE build_id=? AND admission_sha256=?",
            (key.build_id, key.admission_sha256),
        )
        if existing is None:
            if prefix.closed:
                raise StaleArchiveTask("sealed prefix registry lost its original freeze")
            state = _initial_hash(owner, prefix.prefixes)
            db.execute(
                "INSERT INTO discovery_corpus_selection VALUES (?,?,?,?,?,'0','requirements',"
                "NULL,NULL,?,'0',?,?,NULL,NULL)",
                (
                    key.build_id,
                    key.admission_sha256,
                    key.worker_id,
                    owner.sources.manifest_sha256,
                    fence,
                    _counts_json(dict.fromkeys(_COUNTS, 0)),
                    state.export_state(),
                    str(state.total_bytes),
                ),
            ).close()
            changed = db.execute(
                "UPDATE discovery_corpus_prefix_headers SET closed=1 "
                "WHERE build_id=? AND admission_sha256=? AND worker_id=? "
                "AND version=? AND closed=0",
                (key.build_id, key.admission_sha256, key.worker_id, str(prefix.version)),
            ).rowcount
            if changed != 1:
                raise StaleArchiveTask("prefix registry changed during original selection seal")
        return corpus_selection_progress(db, key, owner)


@declare_growth(_declaration("prepare"))
def prepare_corpus_selection(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusSelectionWork
) -> PreparedCorpusSelection:
    if db.in_transaction:
        raise ValueError("selection preparation requires no active transaction")
    current = corpus_selection_progress(db, key, owner)
    if current.phase == "complete":
        raise StaleArchiveTask("original selection is already frozen")
    position = _next(db, key, current)
    state = CheckpointSHA256.from_state(current.hash_state)
    raw, required = _evidence(db, key, owner, current.phase, position) if position else (None, 0)
    if current.phase == "imports" and raw is not None:
        evidence = require_canonical_json(raw.encode())
        if not isinstance(evidence, dict):
            raise StaleArchiveTask("original import evidence is invalid")
        proof_raw, record_raw = evidence["proof_json"], evidence["record_json"]
        if not isinstance(proof_raw, str) or not isinstance(record_raw, str):
            raise StaleArchiveTask("original import proof has invalid exact preimages")
        proof = SourceMemberHistoryBindingProof.from_json_bytes(proof_raw.encode())
        proof.verify_import(
            MemberHistoryImport.from_mapping(require_canonical_json(record_raw.encode()))
        )
    if current.phase == "prefixes" and raw is not None:
        state.update(raw.encode() + b"\n")
    page = PreparedCorpusSelection(
        key,
        current,
        position,
        raw,
        required,
        state.export_state(),
        state.total_bytes,
        state.hexdigest() if current.phase == "prefixes" and position is None else None,
    )
    owner._prepared = page
    return page


@declare_growth(_declaration("apply"))
def apply_corpus_selection(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusSelectionWork,
    page: PreparedCorpusSelection,
) -> CorpusSelectionProgress:
    if owner._prepared is not page or page.key != key:
        raise StaleArchiveTask("selection advance requires its exact detached preparation")
    with _atomic(db):
        current = corpus_selection_progress(db, key, owner)
        if current != page.expected:
            raise StaleArchiveTask("selection original continuation changed")
        if _next(db, key, current) != page.position:
            raise StaleArchiveTask("selection next original input changed")
        counts = _counts(current.counts_json)
        phase = current.phase
        after = page.position
        required = current.required_import_links
        receipt = None
        if page.position is not None:
            if _evidence(db, key, owner, phase, page.position) != (
                page.evidence_json,
                page.required_import_links,
            ):
                raise StaleArchiveTask("selection original evidence changed")
            name = (
                page.position[0]
                if phase == "requirements"
                else "import_links"
                if phase == "imports"
                else phase
            )
            counts[name] += 1
            required += page.required_import_links
            receipt = canonical_json_bytes(
                {
                    "phase": phase,
                    "a": page.position[0],
                    "b": page.position[1],
                    "evidence_json": page.evidence_json,
                    "hash_state": page.hash_state,
                }
            ).decode()
        else:
            phase = _PHASES[_PHASES.index(phase) + 1]
            if phase == "complete":
                after, receipt = current.after, current.receipt_json
        changed = db.execute(
            "UPDATE discovery_corpus_selection SET version=?,phase=?,after_a=?,after_b=?,"
            "counts_json=?,required_import_links=?,hash_state=?,hashed_octets=?,"
            "receipt_json=?,selection_sha256=? "
            "WHERE build_id=? AND admission_sha256=? AND worker_id=? AND version=?",
            (
                str(current.version + 1),
                phase,
                None if after is None else after[0],
                None if after is None else after[1],
                _counts_json(counts),
                str(required),
                page.hash_state,
                str(page.hashed_octets),
                receipt,
                page.selection_sha256,
                key.build_id,
                key.admission_sha256,
                key.worker_id,
                str(current.version),
            ),
        ).rowcount
        if changed != 1:
            raise StaleArchiveTask("selection freeze worker or original version changed")
        return corpus_selection_progress(db, key, owner)


@declare_growth(_declaration("frozen"))
def frozen_corpus_selection(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusSelectionWork
) -> FrozenCorpusSelection:
    current = corpus_selection_progress(db, key, owner)
    if current.phase != "complete" or current.selection_sha256 is None:
        raise StaleArchiveTask("original selection has no independent freeze terminal")
    return FrozenCorpusSelection(
        owner.sources, current.selection_sha256, _counts(current.counts_json)["prefixes"]
    )


@declare_growth(_declaration("membership"))
def corpus_selection_admitted(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusSelectionWork,
    manifest: FrozenCorpusSelection,
    anchor: HistoryJournalAnchor,
) -> bool:
    if manifest != frozen_corpus_selection(db, key, owner):
        raise StaleArchiveTask("selection membership has another exact frozen manifest")
    row = _one(
        db,
        "SELECT p.anchor_json FROM discovery_corpus_source_inputs AS i "
        "JOIN discovery_corpus_prefix_inputs AS p "
        "ON p.build_id=i.build_id AND p.admission_sha256=i.admission_sha256 "
        "AND p.source_key=i.source_key "
        "WHERE i.build_id=? AND i.admission_sha256=? AND i.journal_id=? AND p.prefix_sha256=?",
        (key.build_id, key.admission_sha256, anchor.journal_id, anchor.prefix_sha256),
    )
    return bool(row == (canonical_json_bytes(anchor.to_mapping()).decode(),))
