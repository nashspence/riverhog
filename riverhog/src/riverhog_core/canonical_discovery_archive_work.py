"""Root-bound resumable archive metadata and exact journal admissions.

Complete metadata authenticates its registered binding/journal obligations.
Payload/member-history trees, Native semantics and source-union references have
their own terminals; metadata completion alone never publishes a generation.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any, cast

from riverhog_archive_contracts import (
    ARCHIVE_ROOT_DOCUMENT_BYTES_MAX,
    PROVENANCE_METADATA_BYTES_MAX,
    PROVENANCE_SEQUENCE_DOMAIN,
    CollectionArchiveManifest,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    format_archive_sequence,
    update_provenance_commitment,
)
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
from riverhog_provenance import JournalSummary
from sha256_continuation import CheckpointSHA256

from riverhog_core.canonical_discovery_journal_work import QualifiedJournalSource
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader


class StaleArchiveTask(RuntimeError):
    """Archive, worker, source position or prepared metadata no longer agrees."""


def _digest(value: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValueError("archive identities must be lowercase SHA-256")


def _identity(value: str) -> None:
    if type(value) is not str or not value or len(value) > 128 or not value.isascii():
        raise ValueError("work identities must be bounded nonempty ASCII")


@dataclass(frozen=True, slots=True)
class ArchiveTaskKey:
    build_id: str
    admission_sha256: str
    worker_id: str


@dataclass(frozen=True, slots=True)
class ArchiveMetadataProgress:
    version: int
    next_sequence: int
    bindings: int
    journals: int
    complete: bool


@dataclass(frozen=True, slots=True)
class _State:
    hash_state: str
    bindings: int = 0
    journals: int = 0
    last_artifact: str | None = None
    journal_id: str | None = None
    journal_offset: int = 0
    journal_bytes: int = 0
    journal_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class PreparedArchiveDescriptor:
    key: ArchiveTaskKey
    expected: ArchiveMetadataProgress
    expected_state: _State
    document: ProvenanceVolumeDocument | ProvenanceTerminalDocument
    descriptor_json: str
    next_state: _State
    terminal: bool


class ArchiveDiscoveryAdmission:
    """Verify the exact published root pair supplied by the archive owner.

    The expected archive identity comes from the current owner fence. This
    object preserves immutable input authority, not a cached permission grant.
    """

    def __init__(
        self, archive_root: bytes, provenance_root: bytes, *, expected_archive_sha256: str
    ) -> None:
        _digest(expected_archive_sha256)
        if (
            type(archive_root) is not bytes
            or len(archive_root) > ARCHIVE_ROOT_DOCUMENT_BYTES_MAX
            or type(provenance_root) is not bytes
            or len(provenance_root) > PROVENANCE_METADATA_BYTES_MAX
        ):
            raise ValueError("root preimages must obey their existing archive byte contracts")
        if hashlib.sha256(archive_root).hexdigest() != expected_archive_sha256:
            raise StaleArchiveTask("archive manifest differs from the selected exact root")
        archive = CollectionArchiveManifest.from_json_bytes(archive_root)
        root = ProvenanceRootDocument.from_json_bytes(provenance_root)
        if (
            archive.provenance.identity != hashlib.sha256(provenance_root).hexdigest()
            or archive.provenance.root.plaintext_bytes != len(provenance_root)
            or archive.archive_generation != root.archive_generation
            or archive.artifact_set_sha256 != root.artifact_set_sha256
            or archive.artifacts != root.binding_count
        ):
            raise StaleArchiveTask("archive and provenance roots disagree")
        self.root = root
        self.archive_sha256 = expected_archive_sha256
        self.root_json = root.to_json_bytes().decode()
        self.provenance_sha256 = root.identity
        self.binding_json = canonical_json_bytes(
            {
                "domain": "riverhog-discovery-archive-admission/v1",
                "archive_root_sha256": expected_archive_sha256,
                "provenance_root_sha256": self.provenance_sha256,
            }
        ).decode()
        self.identity = hashlib.sha256(self.binding_json.encode()).hexdigest()
        self._prepared: PreparedArchiveDescriptor | None = None


def initialize_archive_work(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("archive work initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_archive_tasks (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, worker_id TEXT NOT NULL,
            binding_json TEXT NOT NULL, root_json TEXT NOT NULL,
            version TEXT NOT NULL, next_sequence TEXT NOT NULL, hash_state TEXT NOT NULL,
            bindings TEXT NOT NULL, journals TEXT NOT NULL, last_artifact TEXT,
            journal_id TEXT, journal_offset TEXT NOT NULL, journal_bytes TEXT NOT NULL,
            journal_sha256 TEXT, complete INTEGER NOT NULL CHECK(complete IN (0,1)),
            PRIMARY KEY(build_id,admission_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_archive_binding_pages (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL,
            sequence TEXT NOT NULL, descriptor_json TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,sequence)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_archive_journal_segments (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            sequence TEXT NOT NULL, descriptor_json TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id,sequence)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_archive_journals (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL,
            journal_id TEXT NOT NULL, source_bytes TEXT NOT NULL, source_sha256 TEXT NOT NULL,
            first_sequence TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id)
        ) WITHOUT ROWID;
    """)


@contextmanager
def _atomic(db: sqlite3.Connection) -> Iterator[None]:
    if not db.in_transaction:
        raise ValueError("archive work requires an owner-managed transaction")
    db.execute("SAVEPOINT discovery_archive_step")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_archive_step")
        db.execute("RELEASE discovery_archive_step")
        raise
    else:
        db.execute("RELEASE discovery_archive_step")


def _one(db: sqlite3.Connection, sql: str, parameters: tuple[object, ...]) -> Any:
    cursor = db.execute(sql, parameters)
    try:
        return cursor.fetchone()
    finally:
        cursor.close()


def _task(db: sqlite3.Connection, key: ArchiveTaskKey) -> tuple[Any, ...]:
    _identity(key.build_id)
    _identity(key.worker_id)
    _digest(key.admission_sha256)
    row = _one(
        db,
        "SELECT worker_id,binding_json,root_json,version,next_sequence,hash_state,"
        "bindings,journals,last_artifact,journal_id,journal_offset,journal_bytes,"
        "journal_sha256,complete FROM discovery_archive_tasks "
        "WHERE build_id=? AND admission_sha256=?",
        (key.build_id, key.admission_sha256),
    )
    if row is None or row[0] != key.worker_id:
        raise StaleArchiveTask("archive has no matching source/build/worker binding")
    return cast(tuple[Any, ...], row)


def _progress(row: tuple[Any, ...]) -> ArchiveMetadataProgress:
    return ArchiveMetadataProgress(
        int(row[3]), int(row[4]), int(row[6]), int(row[7]), bool(row[13])
    )


def _state(row: tuple[Any, ...]) -> _State:
    return _State(
        row[5], int(row[6]), int(row[7]), row[8], row[9], int(row[10]), int(row[11]), row[12]
    )


def archive_metadata_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey
) -> ArchiveMetadataProgress:
    return _progress(_task(db, key))


def begin_archive_metadata(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> ArchiveMetadataProgress:
    if key.admission_sha256 != admission.identity:
        raise StaleArchiveTask("archive admission binding changed")
    _identity(key.build_id)
    _identity(key.worker_id)
    # This is fixed domain initialization; source metadata hashing belongs to preparation.
    initial = CheckpointSHA256(PROVENANCE_SEQUENCE_DOMAIN).export_state()
    expected = key.worker_id, admission.binding_json, admission.root_json
    with _atomic(db):
        old = _one(
            db,
            "SELECT worker_id,binding_json,root_json FROM discovery_archive_tasks "
            "WHERE build_id=? AND admission_sha256=?",
            (key.build_id, key.admission_sha256),
        )
        if old is None:
            db.execute(
                "INSERT INTO discovery_archive_tasks VALUES "
                "(?,?,?,?,?,'0','0',?,'0','0',NULL,NULL,'0','0',NULL,0)",
                (key.build_id, key.admission_sha256, *expected, initial),
            )
        elif old != expected:
            raise StaleArchiveTask("archive registration differs from its exact admission")
        return archive_metadata_progress(db, key)


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-archive-descriptor-prepare.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_archive_work.prepare_archive_descriptor",),
        scope="one root-bound archive metadata prepare step",
        starts_at="receive exact archive admission and its current owner-leased metadata cursor",
        ends_at="return one detached descriptor step or its committed child/cursor progress",
        dimensions=(
            Dimension("descriptor_octets", OCTETS, "Exact canonical source descriptor bytes."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=1, unit=COUNT),
                "One exact work-parent header read.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=1, unit=COUNT),
                "The exact parent key returns one complete metadata row.",
            ),
            Bound(
                "object_reads",
                COUNT,
                "upper",
                Expression("constant", value=2, unit=COUNT),
                "One bounded root and descriptor; the prepared root may be cached.",
            ),
            Bound(
                "payload_reads",
                COUNT,
                "equal",
                Expression("constant", value=0, unit=COUNT),
                "This phase reads metadata, without journal or binding payloads.",
            ),
            Bound(
                "checkpoint_input_octets",
                OCTETS,
                "equal",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=8, unit=OCTETS),
                        Expression("dimension", name="descriptor_octets"),
                    ),
                ),
                "The existing ordered commitment receives its length header and exact descriptor.",
            ),
        ),
        conditions=(
            "The caller retains current archive/build authority and the workspace lease.",
            "Input objects obey their existing archive root/metadata byte contracts.",
            "Preparation runs outside a write transaction; apply receives its exact detached page.",
            "The private work schema has its exact keys and no added triggers.",
        ),
        invariants=(
            "Metadata input resumes at its exact saved sequence without prior-source replay.",
            "Child registration and the next ordered-commitment checkpoint commit together.",
            "The terminal compares source counts and commitment to independently admitted roots.",
            "Metadata completion grants neither member/history semantic coverage nor publication.",
        ),
        limitations=(
            "Checkpoint input counts do not claim compression, padding or all hashing costs.",
            "Root verification, codecs, storage-engine work and physical I/O are separate costs.",
            "Member binding trees, payload custody and Native/source-union proofs remain separate.",
        ),
    )
)
def prepare_archive_descriptor(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    reader: CanonicalProvenanceArchiveReader,
) -> PreparedArchiveDescriptor:
    """Read and hash one bounded descriptor outside the owner transaction."""
    if db.in_transaction:
        raise ValueError("archive source preparation requires no active transaction")
    row = _task(db, key)
    progress = _progress(row)
    if (
        key.admission_sha256 != admission.identity
        or row[1] != admission.binding_json
        or row[2] != admission.root_json
        or progress.complete
    ):
        raise StaleArchiveTask("archive source or metadata terminal changed")
    state = _state(row)
    document = reader.descriptor_at(progress.next_sequence)
    root = admission.root
    if (document.archive_generation, document.artifact_set_sha256) != (
        root.archive_generation,
        root.artifact_set_sha256,
    ):
        raise StaleArchiveTask("descriptor belongs to another archive")
    digest = CheckpointSHA256.from_state(state.hash_state)
    update_provenance_commitment(digest, document)
    next_state = replace(state, hash_state=digest.export_state())
    terminal = isinstance(document, ProvenanceTerminalDocument)
    if isinstance(document, ProvenanceTerminalDocument):
        if (
            state.journal_id is not None
            and state.journal_offset != state.journal_bytes
            or digest.hexdigest() != root.ordered_volume_sha256
            or state.bindings != root.binding_count
            or state.journals != root.journal_count
        ):
            raise StaleArchiveTask("metadata terminal does not cover the root-selected source")
    elif document.payload.kind == "bindings":
        assert document.first_artifact_id is not None and document.last_artifact_id is not None
        if (
            state.journal_id is not None
            or state.last_artifact is not None
            and document.first_artifact_id <= state.last_artifact
        ):
            raise StaleArchiveTask("binding pages overlap or follow journal sources")
        assert document.binding_count is not None
        assert document.first_artifact_id is not None and document.last_artifact_id is not None
        next_state = replace(
            next_state,
            bindings=state.bindings + document.binding_count,
            last_artifact=document.last_artifact_id,
        )
    else:
        assert document.journal_id is not None and document.journal_offset is not None
        assert document.journal_bytes is not None and document.journal_sha256 is not None
        if document.journal_id != state.journal_id:
            if (
                state.journal_id is not None
                and (
                    state.journal_offset != state.journal_bytes
                    or document.journal_id <= state.journal_id
                )
                or document.journal_offset != 0
            ):
                raise StaleArchiveTask("journal source order or preceding coverage changed")
            next_state = replace(
                next_state,
                journals=state.journals + 1,
                journal_id=document.journal_id,
                journal_offset=0,
                journal_bytes=document.journal_bytes,
                journal_sha256=document.journal_sha256,
            )
        if (
            document.journal_offset != next_state.journal_offset
            or document.journal_bytes != next_state.journal_bytes
            or document.journal_sha256 != next_state.journal_sha256
        ):
            raise StaleArchiveTask("journal descriptors do not cover contiguous exact bytes")
        if document.journal_offset + document.payload.bytes > next_state.journal_bytes:
            raise StaleArchiveTask("journal descriptor exceeds the selected byte extent")
        next_state = replace(
            next_state, journal_offset=document.journal_offset + document.payload.bytes
        )
    page = PreparedArchiveDescriptor(
        key, progress, state, document, document.to_json_bytes().decode(), next_state, terminal
    )
    admission._prepared = page
    return page


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-archive-descriptor-apply.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_archive_work.apply_archive_descriptor",),
        scope="one root-bound archive metadata apply step",
        starts_at="receive exact archive admission and its current owner-leased metadata cursor",
        ends_at="return one detached descriptor step or its committed child/cursor progress",
        dimensions=(
            Dimension("descriptor_octets", OCTETS, "Exact canonical source descriptor bytes."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("constant", value=7, unit=COUNT),
                "Savepoint, parent reads, child/segment inserts, cursor update and release.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=2, unit=COUNT),
                "Only the before/after parent headers are returned.",
            ),
            Bound(
                "checkpoint_input_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Source hashing and descriptor encoding precede the write transaction.",
            ),
        ),
        conditions=(
            "The caller retains current archive/build authority and the workspace lease.",
            "Input objects obey their existing archive root/metadata byte contracts.",
            "Preparation runs outside a write transaction; apply receives its exact detached page.",
            "The private work schema has its exact keys and no added triggers.",
        ),
        invariants=(
            "Metadata input resumes at its exact saved sequence without prior-source replay.",
            "Child registration and the next ordered-commitment checkpoint commit together.",
            "The terminal compares source counts and commitment to independently admitted roots.",
            "Metadata completion grants neither member/history semantic coverage nor publication.",
        ),
        limitations=(
            "Checkpoint input counts do not claim compression, padding or all hashing costs.",
            "Root verification, codecs, storage-engine work and physical I/O are separate costs.",
            "Member binding trees, payload custody and Native/source-union proofs remain separate.",
        ),
    )
)
def apply_archive_descriptor(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    page: PreparedArchiveDescriptor,
) -> ArchiveMetadataProgress:
    """Atomically register exact child obligations and their source continuation."""
    if admission._prepared is not page or page.key != key:
        raise StaleArchiveTask("descriptor is not the current exact detached preparation")
    with _atomic(db):
        row = _task(db, key)
        if (
            _progress(row) != page.expected
            or _state(row) != page.expected_state
            or row[1] != admission.binding_json
            or row[2] != admission.root_json
        ):
            raise StaleArchiveTask("archive metadata version or continuation changed")
        doc = page.document
        if isinstance(doc, ProvenanceVolumeDocument):
            if doc.payload.kind == "bindings":
                db.execute(
                    "INSERT INTO discovery_archive_binding_pages VALUES (?,?,?,?)",
                    (
                        key.build_id,
                        key.admission_sha256,
                        format_archive_sequence(doc.sequence),
                        page.descriptor_json,
                    ),
                )
            elif doc.journal_offset == 0:
                db.execute(
                    "INSERT INTO discovery_archive_journals VALUES (?,?,?,?,?,?)",
                    (
                        key.build_id,
                        key.admission_sha256,
                        doc.journal_id,
                        str(doc.journal_bytes),
                        doc.journal_sha256,
                        format_archive_sequence(doc.sequence),
                    ),
                )
        if isinstance(doc, ProvenanceVolumeDocument) and doc.payload.kind == "journal":
            db.execute(
                "INSERT INTO discovery_archive_journal_segments VALUES (?,?,?,?,?)",
                (
                    key.build_id,
                    key.admission_sha256,
                    doc.journal_id,
                    format_archive_sequence(doc.sequence),
                    page.descriptor_json,
                ),
            )
        state = page.next_state
        db.execute(
            "UPDATE discovery_archive_tasks SET version=?,next_sequence=?,hash_state=?,"
            "bindings=?,journals=?,last_artifact=?,journal_id=?,journal_offset=?,journal_bytes=?,"
            "journal_sha256=?,complete=? WHERE build_id=? AND admission_sha256=?",
            (
                str(page.expected.version + 1),
                str(page.expected.next_sequence + 1),
                state.hash_state,
                str(state.bindings),
                str(state.journals),
                state.last_artifact,
                state.journal_id,
                str(state.journal_offset),
                str(state.journal_bytes),
                state.journal_sha256,
                int(page.terminal),
                key.build_id,
                key.admission_sha256,
            ),
        )
        return archive_metadata_progress(db, key)


def admit_archive_journal(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    summary: JournalSummary,
) -> QualifiedJournalSource:
    """Bind full Native output to a completely authenticated archive source header."""
    if db.in_transaction:
        raise ValueError("Native source admission requires no active write transaction")
    row = _task(db, key)
    if (
        not row[13]
        or row[1] != admission.binding_json
        or key.admission_sha256 != admission.identity
    ):
        raise StaleArchiveTask("archive metadata source coverage is incomplete")
    header = _one(
        db,
        "SELECT source_bytes,source_sha256 FROM discovery_archive_journals "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
        (key.build_id, key.admission_sha256, summary.journal_id),
    )
    if header != (str(summary.journal_bytes), summary.journal_sha256):
        raise StaleArchiveTask("Native journal differs from the root-selected complete source")
    return QualifiedJournalSource(summary, admission.identity)


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-archive-segment-selection.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_archive_work.archive_journal_segment",),
        scope="one authenticated archived journal segment selection",
        starts_at="receive a current source/work key and saved canonical archive sequence",
        ends_at="close the reads and return one detached descriptor or the directory terminal",
        dimensions=(Dimension("registered_segments", COUNT, "All retained segment descriptors."),),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=2, unit=COUNT),
                "One source-parent fence and one indexed segment selection.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=2, unit=COUNT),
                "At most one parent header and one exact next descriptor.",
            ),
        ),
        conditions=(
            "The authenticated metadata terminal precedes use of this directory.",
            "The current primary key includes source, journal and canonical sequence.",
            "The caller retains archive authority and the exact workspace lease.",
        ),
        invariants=(
            "No prior metadata descriptor or payload is replayed to find this segment.",
            "The original metadata sequence and descriptor preimage remain exact.",
            "Every read cursor is closed before decoding or yielding the descriptor.",
        ),
        limitations=(
            "SQL/row counts exclude codec, metadata bytes and storage-engine/index costs.",
            "Payload custody/fixity, Native validation and current authorization are separate.",
        ),
    )
)
def archive_journal_segment(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    journal_id: str,
    *,
    after_sequence: int | None = None,
) -> ProvenanceVolumeDocument | None:
    """Detach the next authenticated segment without replaying the metadata sequence."""
    if db.in_transaction:
        raise ValueError("descriptor decoding requires no active write transaction")
    row = _task(db, key)
    if not row[13]:
        raise StaleArchiveTask("archive metadata source coverage is incomplete")
    after = "" if after_sequence is None else format_archive_sequence(after_sequence)
    selected = _one(
        db,
        "SELECT descriptor_json FROM discovery_archive_journal_segments "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND sequence>? "
        "ORDER BY sequence LIMIT 1",
        (key.build_id, key.admission_sha256, journal_id, after),
    )
    return (
        None if selected is None else ProvenanceVolumeDocument.from_json_bytes(selected[0].encode())
    )
