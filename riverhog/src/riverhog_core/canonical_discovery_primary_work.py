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

from riverhog_archive_contracts import MemberHistoryDocument, MemberHistoryRoot
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance import CanonicalCorpusValidator

from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_history_work import (
    ArchiveHistoryWork,
    HistorySetWork,
    history_descriptor_ready,
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
        corpus: CanonicalCorpusValidator,
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
