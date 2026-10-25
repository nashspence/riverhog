"""Root-verified member registration from retained archive binding pages.

Registration preserves exact history obligations. It grants neither history or
Native semantic completion nor current access or generation publication.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from riverhog_archive_contracts import (
    MemberBindingTreeCheckpoint,
    MemberHistoryBinding,
    ProvenanceVolumeDocument,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
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
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader


@dataclass(frozen=True, slots=True)
class ArchiveMemberProgress:
    version: int
    after_page: str
    item_index: int
    members: int
    complete: bool


@dataclass(frozen=True, slots=True)
class PreparedArchiveMember:
    key: ArchiveTaskKey
    expected_row: tuple[Any, ...]
    expected_members: int
    binding: MemberHistoryBinding | None
    binding_json: str | None
    source_page: str | None
    source_item: int | None
    next_after: str
    next_item: int
    checkpoint_json: str
    terminal: bool


class ArchiveMemberRegistration:
    """Retain only the current bounded binding page and exact prepared action."""

    def __init__(
        self, admission: ArchiveDiscoveryAdmission, reader: CanonicalProvenanceArchiveReader
    ) -> None:
        self.admission = admission
        self.reader = reader
        self._cache: tuple[str, str, tuple[MemberHistoryBinding, ...]] | None = None
        self._prepared: PreparedArchiveMember | None = None


def initialize_member_work(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("member work initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_archive_member_tasks (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, worker_id TEXT NOT NULL,
            version TEXT NOT NULL, after_page TEXT NOT NULL, item_index INTEGER NOT NULL,
            checkpoint_json TEXT NOT NULL, complete INTEGER NOT NULL CHECK(complete IN (0,1)),
            PRIMARY KEY(build_id,admission_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_archive_members (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, artifact_id TEXT NOT NULL,
            binding_json TEXT NOT NULL, source_page TEXT NOT NULL, source_item INTEGER NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,artifact_id)
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
        raise ValueError("member registration requires an owner-managed transaction")
    db.execute("SAVEPOINT discovery_archive_member")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_archive_member")
        db.execute("RELEASE discovery_archive_member")
        raise
    else:
        db.execute("RELEASE discovery_archive_member")


def _header(db: sqlite3.Connection, key: ArchiveTaskKey) -> tuple[Any, ...]:
    row = _one(
        db,
        "SELECT worker_id,version,after_page,item_index,checkpoint_json,complete "
        "FROM discovery_archive_member_tasks WHERE build_id=? AND admission_sha256=?",
        (key.build_id, key.admission_sha256),
    )
    if row is None or row[0] != key.worker_id:
        raise StaleArchiveTask("member work has no exact source/build/worker binding")
    return tuple(row)


def _progress(row: tuple[Any, ...]) -> ArchiveMemberProgress:
    checkpoint = MemberBindingTreeCheckpoint.from_json_bytes(row[4].encode())
    return ArchiveMemberProgress(int(row[1]), row[2], row[3], checkpoint.count, bool(row[5]))


def archive_member_progress(db: sqlite3.Connection, key: ArchiveTaskKey) -> ArchiveMemberProgress:
    return _progress(_header(db, key))


def _require_metadata(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> None:
    if (
        key.admission_sha256 != admission.identity
        or not archive_metadata_progress(db, key).complete
    ):
        raise StaleArchiveTask("member source metadata is incomplete or changed")


def begin_archive_members(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> ArchiveMemberProgress:
    _require_metadata(db, key, admission)
    with _atomic(db):
        old = _one(
            db,
            "SELECT worker_id FROM discovery_archive_member_tasks "
            "WHERE build_id=? AND admission_sha256=?",
            (key.build_id, key.admission_sha256),
        )
        if old is None:
            db.execute(
                "INSERT INTO discovery_archive_member_tasks VALUES (?,?,?,'0','',0,?,0)",
                (
                    key.build_id,
                    key.admission_sha256,
                    key.worker_id,
                    MemberBindingTreeCheckpoint().to_json_bytes().decode(),
                ),
            )
        elif old != (key.worker_id,):
            raise StaleArchiveTask("member registration worker changed")
        return archive_member_progress(db, key)


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-archive-member-prepare.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_member_work.prepare_archive_member",),
        scope="one root-bound archive member registration preparation",
        starts_at="receive an exact admitted source, worker and saved member cursor",
        ends_at="detach one member obligation and tree checkpoint or verify their root terminal",
        dimensions=(
            Dimension("registered_members", COUNT, "Previously registered exact bindings."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=3, unit=COUNT),
                "One metadata fence, one member header and one indexed next-page selection.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=3, unit=COUNT),
                "Each exact header/page query returns at most one row.",
            ),
            Bound(
                "binding_leaf_hashes",
                COUNT,
                "upper",
                Expression("constant", value=1, unit=COUNT),
                "Only the selected current member is hashed.",
            ),
            Bound(
                "binding_branch_hashes",
                COUNT,
                "upper",
                Expression("constant", value=256, unit=COUNT),
                "At most one carry/fold per level of the existing 256-bit artifact ID space.",
            ),
        ),
        conditions=(
            "The caller retains current archive/build authority and the exact workspace lease.",
            "Binding pages obey their existing byte/member bounds and exact metadata registration.",
            "The current page is retained only for this source owner; restart rereads that page.",
            "Preparation runs outside the owner write transaction.",
        ),
        invariants=(
            "Completed pages are never replayed; the next member uses its saved page/item cursor.",
            "The terminal compares independent root count and member-binding tree commitment.",
            "Registration preserves exact original history binding and source coordinates.",
        ),
        limitations=(
            "Tree hash counts exclude root/payload fixity hashing, codecs and storage-engine work.",
            "One page has at most 512 members/4 MiB; checkpoint bytes follow the fixed ID space.",
            "History/Native semantic coverage and publication retain separate terminals.",
        ),
    )
)
def prepare_archive_member(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: ArchiveMemberRegistration
) -> PreparedArchiveMember:
    if db.in_transaction:
        raise ValueError("member source preparation requires no active transaction")
    _require_metadata(db, key, owner.admission)
    row = _header(db, key)
    progress = _progress(row)
    if progress.complete:
        raise StaleArchiveTask("member source is already complete")
    selected = _one(
        db,
        "SELECT sequence,descriptor_json FROM discovery_archive_binding_pages "
        "WHERE build_id=? AND admission_sha256=? AND sequence>? ORDER BY sequence LIMIT 1",
        (key.build_id, key.admission_sha256, progress.after_page),
    )
    checkpoint = MemberBindingTreeCheckpoint.from_json_bytes(row[4].encode())
    if selected is None:
        if (
            progress.item_index != 0
            or checkpoint.count != owner.admission.root.binding_count
            or checkpoint.root_sha256 != owner.admission.root.binding_tree_sha256
        ):
            raise StaleArchiveTask("member terminal does not cover the root-selected bindings")
        page = PreparedArchiveMember(
            key, row, progress.members, None, None, None, None, progress.after_page, 0, row[4], True
        )
    else:
        sequence, descriptor_json = selected
        if owner._cache is None or owner._cache[:2] != (sequence, descriptor_json):
            descriptor = ProvenanceVolumeDocument.from_json_bytes(descriptor_json.encode())
            owner._cache = sequence, descriptor_json, owner.reader.binding_page(descriptor)
        bindings = owner._cache[2]
        if not 0 <= progress.item_index < len(bindings):
            raise StaleArchiveTask("member cursor exceeds its exact registered binding page")
        binding = bindings[progress.item_index]
        next_checkpoint = checkpoint.append(binding)
        last = progress.item_index + 1 == len(bindings)
        page = PreparedArchiveMember(
            key,
            row,
            progress.members,
            binding,
            canonical_json_bytes(binding.to_mapping()).decode(),
            sequence,
            progress.item_index,
            sequence if last else progress.after_page,
            0 if last else progress.item_index + 1,
            next_checkpoint.to_json_bytes().decode(),
            False,
        )
    owner._prepared = page
    return page


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-archive-member-apply.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_member_work.apply_archive_member",),
        scope="one root-bound archive member registration commit",
        starts_at="receive the exact detached member preparation and current source/worker lease",
        ends_at="commit one member obligation with its next tree/source cursor or terminal",
        dimensions=(
            Dimension("registered_members", COUNT, "Previously registered exact bindings."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("constant", value=6, unit=COUNT),
                "Savepoint, two exact parent reads, one member insert, update and release.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=2, unit=COUNT),
                "Only before/after member task headers are read.",
            ),
            Bound(
                "binding_leaf_hashes",
                COUNT,
                "equal",
                Expression("constant", value=0, unit=COUNT),
                "Tree hashing precedes apply.",
            ),
            Bound(
                "binding_branch_hashes",
                COUNT,
                "equal",
                Expression("constant", value=0, unit=COUNT),
                "Tree hashing precedes apply.",
            ),
        ),
        conditions=(
            "The caller retains current archive/build authority and the exact workspace lease.",
            "Apply receives the exact object prepared by its source owner before this transaction.",
            "The private schema retains its exact keys without added triggers.",
        ),
        invariants=(
            "A member obligation and its tree/source continuation commit or roll back together.",
            "Stale source/worker/version or replaced preparations cannot advance the cursor.",
            "Storage completion does not authorize generation publication.",
        ),
        limitations=(
            "SQL counts exclude storage-engine/index costs and bounded checkpoint decoding.",
            "Current authorization, member-history and Native source-union proofs remain separate.",
        ),
    )
)
def apply_archive_member(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: ArchiveMemberRegistration,
    page: PreparedArchiveMember,
) -> ArchiveMemberProgress:
    if owner._prepared is not page or page.key != key:
        raise StaleArchiveTask("member page is not the current exact detached preparation")
    with _atomic(db):
        if _header(db, key) != page.expected_row:
            raise StaleArchiveTask("member work version or source continuation changed")
        if page.binding is not None:
            db.execute(
                "INSERT INTO discovery_archive_members VALUES (?,?,?,?,?,?)",
                (
                    key.build_id,
                    key.admission_sha256,
                    page.binding.artifact_id,
                    page.binding_json,
                    page.source_page,
                    page.source_item,
                ),
            )
        db.execute(
            "UPDATE discovery_archive_member_tasks SET version=?,after_page=?,item_index=?,"
            "checkpoint_json=?,complete=? WHERE build_id=? AND admission_sha256=?",
            (
                str(int(page.expected_row[1]) + 1),
                page.next_after,
                page.next_item,
                page.checkpoint_json,
                int(page.terminal),
                key.build_id,
                key.admission_sha256,
            ),
        )
        return archive_member_progress(db, key)
