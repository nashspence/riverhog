"""Bind a shared history primary to Native-verified original State support.

Only exact documentary references are retained per history. Assertion payloads,
historical semantics, source-union validation and publication keep their owners.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from riverhog_archive_contracts import (
    MemberHistoryDocument,
    MemberHistoryImport,
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
from riverhog_provenance import (
    CanonicalCorpusValidator,
    ProvenanceValidationError,
    RetainedCorpusValidator,
)

from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_history_work import (
    ArchiveHistoryWork,
    HistorySetWork,
    history_descriptor_ready,
    history_import_progress,
)
from riverhog_core.canonical_discovery_journal_work import QualifiedJournalSource
from riverhog_core.canonical_discovery_snapshot_work import HistorySnapshotWork


@dataclass(frozen=True, slots=True)
class PreparedHistoryPrimary:
    key: ArchiveTaskKey
    history_sha256: str
    descriptor_json: str
    source_key: str
    binding_json: str
    anchor_json: str
    state_reference_json: str
    occurrence_reference_json: str
    member_role: str


class HistoryPrimaryWork:
    def __init__(
        self,
        history: ArchiveHistoryWork,
        source: QualifiedJournalSource,
        corpus: CanonicalCorpusValidator | RetainedCorpusValidator,
        *,
        member_role: str,
    ) -> None:
        self.history = history
        self.source = source
        self.corpus = corpus
        self.member_role = member_role
        self.source_key = source.source_key
        self.binding_json = source.binding_json
        self._prepared: PreparedHistoryPrimary | None = None

    def check_source(self) -> None:
        if (
            self.source.source_key != self.source_key
            or self.source.binding_json != self.binding_json
        ):
            raise StaleArchiveTask("primary Native source binding changed")
        self.source.check_open()


def initialize_primary_work(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("primary initialization requires no active transaction")
    db.execute("""
        CREATE TABLE IF NOT EXISTS discovery_history_primary_bindings (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL,
            history_sha256 TEXT NOT NULL, worker_id TEXT NOT NULL,
            source_key TEXT NOT NULL, anchor_json TEXT NOT NULL,
            state_reference_json TEXT NOT NULL, occurrence_reference_json TEXT NOT NULL,
            member_role TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,history_sha256)
        ) WITHOUT ROWID
    """).close()


def _one(db: sqlite3.Connection, sql: str, parameters: tuple[object, ...]) -> Any:
    cursor = db.execute(sql, parameters)
    try:
        return cursor.fetchone()
    finally:
        cursor.close()


@contextmanager
def _atomic(db: sqlite3.Connection) -> Iterator[None]:
    if not db.in_transaction:
        raise ValueError("primary work requires an owner-managed transaction")
    db.execute("SAVEPOINT discovery_history_primary").close()
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_history_primary").close()
        db.execute("RELEASE discovery_history_primary").close()
        raise
    else:
        db.execute("RELEASE discovery_history_primary").close()


def _selection(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: HistoryPrimaryWork,
    history_sha256: str,
    expected: PreparedHistoryPrimary | None = None,
) -> tuple[str, MemberHistoryDocument, str]:
    row = _one(
        db,
        "SELECT descriptor_json FROM discovery_history_descriptors "
        "WHERE build_id=? AND admission_sha256=? AND history_sha256=?",
        (key.build_id, key.admission_sha256, history_sha256),
    )
    if row is None or (expected is not None and row != (expected.descriptor_json,)):
        raise StaleArchiveTask("primary history descriptor is absent or changed")
    raw: str = row[0]
    if expected is None and hashlib.sha256(raw.encode()).hexdigest() != history_sha256:
        raise StaleArchiveTask("primary history descriptor differs from its exact identity")
    descriptor = MemberHistoryDocument.from_json_bytes(raw.encode())
    if not history_descriptor_ready(db, key, owner.history, history_sha256):
        raise StaleArchiveTask("primary history record sets are incomplete")
    primary = MemberHistoryRoot(descriptor.primary.journal, "bound")
    anchor_json = canonical_json_bytes(primary.journal.to_mapping()).decode()
    found = _one(
        db,
        "SELECT h.anchor_json,l.inclusion,s.binding_json "
        "FROM discovery_history_snapshot_links l "
        "JOIN discovery_snapshot_headers h ON "
        "h.build_id=l.build_id AND h.admission_sha256=l.admission_sha256 "
        "AND h.source_key=l.source_key AND h.prefix_sha256=l.prefix_sha256 "
        "JOIN discovery_snapshot_sources s ON "
        "s.build_id=h.build_id AND s.admission_sha256=h.admission_sha256 "
        "AND s.source_key=h.source_key "
        "WHERE l.build_id=? AND l.admission_sha256=? AND l.root_set_sha256=? "
        "AND l.record_key=? AND l.source_key=?",
        (
            key.build_id,
            key.admission_sha256,
            descriptor.roots.records_sha256,
            primary.key,
            owner.source_key,
        ),
    )
    if found != (anchor_json, "bound", owner.binding_json):
        raise StaleArchiveTask("primary lacks its exact admitted snapshot binding")
    return raw, descriptor, anchor_json


def _declaration(phase: str) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    bounds = [
        Bound(
            "sql_statements",
            COUNT,
            "upper",
            count(7 if phase == "prepare" else 11),
            "Exact Core descriptor, set terminals, primary snapshot and binding point work.",
        ),
        Bound(
            "returned_rows",
            COUNT,
            "upper",
            count(7 if phase == "prepare" else 8),
            "Every Core query returns at most one exact keyed row.",
        ),
        Bound(
            "native_delivery_calls",
            COUNT,
            "equal",
            count(1 if phase == "prepare" else 0),
            "Native owns the selected association, State, Occurrence and fixity predicates.",
        ),
        Bound(
            "native_assertion_calls",
            COUNT,
            "equal",
            count(0),
            "Primary binding never requests full Native assertion payloads.",
        ),
        Bound(
            "native_documentary_calls",
            COUNT,
            "equal",
            count(2 if phase == "prepare" else 0),
            "Original State/Occurrence references are checked by the admitted Native source.",
        ),
        Bound(
            "source_frame_reads",
            COUNT,
            "equal",
            count(0),
            "Neither preparation nor commit loads a source frame or historical graph.",
        ),
    ]
    if phase == "apply":
        bounds.append(
            Bound(
                "source_hash_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Only detached metadata is written; no original source payload is hashed.",
            )
        )
    return Declaration(
        id=f"riverhog-server.discovery-history-primary-{phase}.growth/v1",
        authority="riverhog-server",
        subjects=(f"riverhog_core.canonical_discovery_primary_work.{phase}_history_primary",),
        scope=f"one exact history-primary Native support {phase}",
        starts_at="receive one exact history identity, admitted Native source and owner fence",
        ends_at="detach or commit only the primary's original State and Occurrence references",
        dimensions=(Dimension("registered_primaries", COUNT, "Existing shared primary bindings."),),
        bounds=tuple(bounds),
        conditions=(
            "The caller retains current archive/build authority and the exact workspace lease.",
            "The history's root/import terminals and bound primary snapshot are verified.",
            "Preparation explicitly admits each narrow Native semantic metadata atom.",
        ),
        invariants=(
            "The exact historical delivery uses Native's maintained policy and original support.",
            "Later corrections never substitute current-tail State or association assertions.",
            "Original support is metadata per shared history, never copied assertion payloads.",
            "Changed source, descriptor, worker or exact preparation fails before registration.",
        ),
        limitations=(
            "Core SQL counts exclude B-tree work and transaction entry/commit.",
            "Native owns indexed semantic projection reads and documentary/header work.",
            "Native global corpus, effective-history and causal-State terminals are separate.",
            "Private workspace registration grants no current access or generation publication.",
        ),
    )


@declare_growth(_declaration("prepare"))
def prepare_history_primary(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: HistoryPrimaryWork,
    history_sha256: str,
    *,
    metadata_byte_budget: int,
) -> PreparedHistoryPrimary:
    if db.in_transaction:
        raise ValueError("primary preparation requires no active transaction")
    owner.check_source()
    raw, descriptor, anchor_json = _selection(db, key, owner, history_sha256)
    HistorySnapshotWork(
        HistorySetWork(owner.history.admission, owner.history.reader, descriptor.roots),
        owner.source,
    )
    support = owner.corpus.selected_delivery_support(
        binding={"artifact_id": descriptor.artifact_id, **descriptor.primary.to_mapping()},
        artifact_id=descriptor.artifact_id,
        byte_count=descriptor.bytes,
        sha256=descriptor.sha256,
        member_role=owner.member_role,
        metadata_byte_budget=metadata_byte_budget,
    )
    supports = (support.state_reference, support.occurrence_reference)
    for reference in supports:
        owner.source.reference_anchor(reference)
    if any(
        int(value["entry"]["sequence"]) > descriptor.primary.journal.through_sequence
        for value in supports
    ):
        raise StaleArchiveTask("primary support exceeds its exact historical prefix")
    page = PreparedHistoryPrimary(
        key,
        history_sha256,
        raw,
        owner.source_key,
        owner.binding_json,
        anchor_json,
        canonical_json_bytes(supports[0]).decode(),
        canonical_json_bytes(supports[1]).decode(),
        owner.member_role,
    )
    owner._prepared = page
    return page


@declare_growth(_declaration("apply"))
def apply_history_primary(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: HistoryPrimaryWork,
    page: PreparedHistoryPrimary,
) -> None:
    if (
        owner._prepared is not page
        or page.key != key
        or page.source_key != owner.source_key
        or page.binding_json != owner.binding_json
        or page.member_role != owner.member_role
    ):
        raise StaleArchiveTask("primary is not the current exact detached preparation")
    owner.check_source()
    with _atomic(db):
        _selection(db, key, owner, page.history_sha256, page)
        expected = (
            key.worker_id,
            page.source_key,
            page.anchor_json,
            page.state_reference_json,
            page.occurrence_reference_json,
            page.member_role,
        )
        previous = _one(
            db,
            "SELECT worker_id,source_key,anchor_json,state_reference_json,"
            "occurrence_reference_json,member_role FROM discovery_history_primary_bindings "
            "WHERE build_id=? AND admission_sha256=? AND history_sha256=?",
            (key.build_id, key.admission_sha256, page.history_sha256),
        )
        if previous is None:
            db.execute(
                "INSERT INTO discovery_history_primary_bindings VALUES (?,?,?,?,?,?,?,?,?)",
                (key.build_id, key.admission_sha256, page.history_sha256, *expected),
            ).close()
        elif previous != expected:
            raise StaleArchiveTask("primary source or original support changed")


def _import_primary_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def width(value: int) -> Expression:
        return Expression(
            "product",
            terms=(
                Expression("constant", value=value),
                Expression("dimension", name="metadata_octets"),
            ),
        )

    return Declaration(
        id="riverhog-server.discovery-history-import-primary.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_primary_work.verify_history_import_primary",),
        scope="one original import incidence and its shared Native-verified source primary",
        starts_at="receive current archive custody and one exact authenticated import-set record",
        ends_at="close all Core cursors and accept the exact original State or reject the import",
        dimensions=(
            Dimension("registered_records", COUNT, "All independently retained Core records."),
            Dimension(
                "metadata_octets",
                OCTETS,
                "Largest complete input, parameter tuple or selected metadata row.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(4),
                "Three original coverage/header reads and one indexed source-primary join.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(4),
                "Each exact lookup returns at most one complete row.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                width(4),
                "All fields from the four complete rows are charged.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                width(24),
                "Complete repeated archive, set, worker and original incidence keys.",
            ),
            Bound(
                "encoded_octets",
                OCTETS,
                "upper",
                width(8),
                "Authority/checkpoint round trips, import key, descriptor and exact references.",
            ),
            Bound(
                "decoded_octets",
                OCTETS,
                "upper",
                width(4),
                "The original checkpoint, import and source descriptor are bounded metadata.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                width(2),
                "One exact original import key and source descriptor identity.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(4096),
                        Expression(
                            "product",
                            terms=(
                                count(512),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("dimension", name="registered_records"),
                                                count(2),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Fixed indexed point work on the maintained SQLite schema.",
            ),
            *(
                Bound(
                    metric,
                    COUNT,
                    "equal",
                    count(0),
                    "No mutation, Native body call, source replay or structure-object traversal.",
                )
                for metric in ("changed_rows", "native_calls", "source_frame_reads", "object_reads")
            ),
        ),
        conditions=(
            "The application retains current metadata, original archive and worker/kernel fences.",
            "Structural import proofs and source primary bindings have their owning verification.",
            "The shared Native primary row is immutable while this attempt remains current.",
            "All Core queries use the maintained exact-key indexes without table traversal.",
        ),
        invariants=(
            "Every required original incidence is checked against its own authenticated source.",
            "A known history or valid journal union does not establish imported State equality.",
            "The exact Native-proven historical State is used, never a corrected current tail.",
            "Missing source-primary proof is incomplete; a different imported State is invalid.",
            "Both declared history extents retain the same exact source-primary equality check.",
        ),
        limitations=(
            "This point check creates no complete semantic, causal-State or publication terminal.",
            "Whole-corpus validity, import cycles and same-member causal closure are separate.",
            "VM and logical bytes exclude physical I/O, lock waits and internal byte comparisons.",
        ),
    )


@declare_growth(_import_primary_declaration())
def verify_history_import_primary(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: HistorySetWork, record_key: str
) -> None:
    """Check one original imported State against its Native-verified source primary.

    This reads the original incidence and shared primary binding. It neither
    traverses the selected history nor grants a complete semantic or publication
    terminal. The application retains current metadata custody around this read.
    """
    if db.in_transaction:
        raise ValueError("import primary verification requires no active transaction")
    if not history_import_progress(db, key, owner).complete:
        raise StaleArchiveTask("import primary requires complete original incidence coverage")
    row = _one(
        db,
        "SELECT r.value_json,l.proof_sha256,l.history_sha256,p.worker_id,p.history_sha256,"
        "b.worker_id,b.anchor_json,b.state_reference_json,d.descriptor_json "
        "FROM discovery_history_set_records r "
        "JOIN discovery_history_import_links l ON "
        "l.build_id=r.build_id AND l.admission_sha256=r.admission_sha256 "
        "AND l.import_set_sha256=r.records_sha256 AND l.record_key=r.record_key "
        "JOIN discovery_history_import_proofs p ON "
        "p.build_id=l.build_id AND p.admission_sha256=l.admission_sha256 "
        "AND p.proof_sha256=l.proof_sha256 "
        "JOIN discovery_history_primary_bindings b ON "
        "b.build_id=l.build_id AND b.admission_sha256=l.admission_sha256 "
        "AND b.history_sha256=l.history_sha256 "
        "JOIN discovery_history_descriptors d ON "
        "d.build_id=b.build_id AND d.admission_sha256=b.admission_sha256 "
        "AND d.history_sha256=b.history_sha256 "
        "WHERE r.build_id=? AND r.admission_sha256=? AND r.records_sha256=? AND r.record_key=?",
        (key.build_id, key.admission_sha256, owner.authority.records_sha256, record_key),
    )
    if row is None:
        raise StaleArchiveTask("import has no exact Native-verified source primary")
    raw, proof_id, history_id, proof_worker, proof_history, primary_worker, anchor, state, body = (
        row
    )
    imported = MemberHistoryImport.from_mapping(require_canonical_json(raw.encode()))
    descriptor = MemberHistoryDocument.from_json_bytes(body.encode())
    if (
        imported.key != record_key
        or imported.source_binding_proof_sha256 != proof_id
        or imported.source_history_sha256 != history_id
        or proof_history != history_id
        or proof_worker != key.worker_id
        or primary_worker != key.worker_id
        or descriptor.identity != history_id
        or descriptor.artifact_id != imported.source_artifact_id
        or canonical_json_bytes(descriptor.primary.journal.to_mapping()).decode() != anchor
    ):
        raise StaleArchiveTask("import source proof or Native primary binding changed")
    if canonical_json_bytes(imported.input_state).decode() != state:
        raise ProvenanceValidationError("imported State differs from the source primary")
