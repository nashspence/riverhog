"""Original member advancement and its shared history children commit together.

The independent member tree authenticates the selected input. Child coverage
does not replace structural history, Native semantics or current access.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from riverhog_archive_contracts import RETAINED_HISTORY_EXTENT
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
from riverhog_core.canonical_discovery_history_inputs import (
    _register_corpus_history,
    begin_corpus_history_inputs,
    initialize_corpus_history_inputs,
)
from riverhog_core.canonical_discovery_history_work import (
    ArchiveHistoryWork,
    PreparedHistoryDescriptor,
    _prepare_history_binding,
    _require_archive,
    apply_history_descriptor,
    initialize_history_work,
)
from riverhog_core.canonical_discovery_member_work import (
    ArchiveMemberProgress,
    ArchiveMemberRegistration,
    PreparedArchiveMember,
    apply_archive_member,
    begin_archive_members,
    initialize_member_work,
    prepare_archive_member,
)
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader


@dataclass(frozen=True, slots=True)
class PreparedCorpusMember:
    key: ArchiveTaskKey
    member: PreparedArchiveMember
    history: PreparedHistoryDescriptor | None


class CorpusMemberRegistration:
    """Retain one binding page and its current descriptor preparation."""

    def __init__(
        self, admission: ArchiveDiscoveryAdmission, reader: CanonicalProvenanceArchiveReader
    ) -> None:
        self.members = ArchiveMemberRegistration(admission, reader)
        self.histories = ArchiveHistoryWork(admission, reader)
        self._prepared: PreparedCorpusMember | None = None


def initialize_corpus_members(db: sqlite3.Connection) -> None:
    initialize_member_work(db)
    initialize_history_work(db)
    initialize_corpus_history_inputs(db)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_corpus_member_coverage (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, worker_id TEXT NOT NULL,
            members TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256)
        ) WITHOUT ROWID;
    """)


@contextmanager
def _atomic(db: sqlite3.Connection) -> Iterator[None]:
    if not db.in_transaction:
        raise ValueError("corpus member work requires an owner-managed transaction")
    db.execute("SAVEPOINT discovery_corpus_member")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_corpus_member")
        db.execute("RELEASE discovery_corpus_member")
        raise
    else:
        db.execute("RELEASE discovery_corpus_member")


def _coverage(db: sqlite3.Connection, key: ArchiveTaskKey) -> int:
    cursor = db.execute(
        "SELECT worker_id,members FROM discovery_corpus_member_coverage "
        "WHERE build_id=? AND admission_sha256=?",
        (key.build_id, key.admission_sha256),
    )
    try:
        row = cursor.fetchone()
    finally:
        cursor.close()
    if row is None or row[0] != key.worker_id:
        raise StaleArchiveTask("member history coverage has no exact source/build/worker")
    value = row[1]
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
        raise StaleArchiveTask("member history coverage count is invalid")
    count = int(value)
    if str(count) != value:
        raise StaleArchiveTask("member history coverage count is not canonical")
    return count


def begin_corpus_members(
    db: sqlite3.Connection, key: ArchiveTaskKey, admission: ArchiveDiscoveryAdmission
) -> ArchiveMemberProgress:
    with _atomic(db):
        progress = begin_archive_members(db, key, admission)
        begin_corpus_history_inputs(db, key, admission)
        cursor = db.execute(
            "SELECT worker_id FROM discovery_corpus_member_coverage "
            "WHERE build_id=? AND admission_sha256=?",
            (key.build_id, key.admission_sha256),
        )
        try:
            old = cursor.fetchone()
        finally:
            cursor.close()
        if old is None:
            if progress.version != 0 or progress.members != 0 or progress.complete:
                raise StaleArchiveTask("member advancement lacks original history coverage")
            db.execute(
                "INSERT INTO discovery_corpus_member_coverage VALUES (?,?,?,'0')",
                (key.build_id, key.admission_sha256, key.worker_id),
            )
        if _coverage(db, key) != progress.members:
            raise StaleArchiveTask("member cursor and history coverage disagree")
        return progress


def _declaration(mode: str) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    return Declaration(
        id="riverhog-server.discovery-corpus-member-" + mode + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_corpus_members." + mode + "_corpus_member",),
        scope="one original member and its shared history children",
        starts_at="receive exact admitted metadata, worker and original member continuation",
        ends_at="detach one member/history preparation or commit it with child coverage",
        dimensions=(
            Dimension("registered_inputs", COUNT, "All retained Core metadata and history rows."),
            Dimension("metadata_octets", OCTETS, "Largest complete metadata row or checkpoint."),
            Dimension("source_octets", OCTETS, "Largest bounded root, binding page or descriptor."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(64),
                "Fixed parent, descriptor and shared requirement point operations.",
            ),
            Bound("returned_rows", COUNT, "upper", count(32), "Fixed metadata and child lookups."),
            Bound(
                "source_reads",
                COUNT,
                "upper",
                count(4 if mode == "prepare" else 0),
                "One current binding page and descriptor, each with its root fence.",
            ),
            Bound(
                "source_read_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=4 if mode == "prepare" else 0, unit=()),
                        dim("source_octets"),
                    ),
                ),
                "Complete bounded source objects, without previous member or history replay.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    Expression(
                        "product",
                        terms=(
                            Expression("constant", value=64, unit=()),
                            dim("metadata_octets"),
                        ),
                    ),
                    "Complete indexed metadata parameters, rows and writes.",
                )
                for metric in ("parameter_octets", "returned_octets", "written_octets")
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    Expression(
                        "product",
                        terms=(
                            Expression("constant", value=128, unit=()),
                            Expression("sum", terms=(dim("metadata_octets"), dim("source_octets"))),
                        ),
                    ),
                    "Full metadata and bounded source codec/hash execution.",
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
                        count(16384),
                        Expression(
                            "product",
                            terms=(
                                count(2048),
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
                "Fixed point operations over growing metadata indexes.",
            ),
        ),
        conditions=(
            "Archive/build authority and exact reader custody remain current through apply.",
            "Preparation runs outside writes; apply uses the owner's transaction.",
            "Source pages and descriptors retain their existing byte bounds.",
            "The schema has no extra triggers; injected failures are separate correctness checks.",
        ),
        invariants=(
            "One original member cursor is shared with history child registration.",
            "Member, descriptor, shared sets and incidence count commit or roll back together.",
            "The independently authenticated member count/tree is required at termination.",
            "A previously advanced member without original child coverage fails closed.",
        ),
        limitations=(
            "Storage work includes indexed metadata; source codecs and hashes retain child costs.",
            "History, imports, Native semantics and publication retain separate terminals.",
        ),
    )


@declare_growth(_declaration("prepare"))
def prepare_corpus_member(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: CorpusMemberRegistration
) -> PreparedCorpusMember:
    member = prepare_archive_member(db, key, owner.members)
    if _coverage(db, key) != member.expected_members:
        raise StaleArchiveTask("member cursor and history coverage disagree")
    history = (
        None
        if member.binding is None
        else _prepare_history_binding(db, key, owner.histories, member.binding)
    )
    page = PreparedCorpusMember(key, member, history)
    owner._prepared = page
    return page


@declare_growth(_declaration("apply"))
def apply_corpus_member(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusMemberRegistration,
    page: PreparedCorpusMember,
) -> ArchiveMemberProgress:
    if owner._prepared is not page or page.key != key:
        raise StaleArchiveTask("corpus member is not the exact detached preparation")
    with _atomic(db):
        _require_archive(db, key, owner.members.admission)
        if _coverage(db, key) != page.member.expected_members:
            raise StaleArchiveTask("member cursor and history coverage disagree")
        progress = apply_archive_member(db, key, owner.members, page.member)
        if page.history is not None:
            apply_history_descriptor(db, key, owner.histories, page.history)
            _register_corpus_history(
                db,
                key,
                owner.members.admission,
                page.history.descriptor,
                extent=RETAINED_HISTORY_EXTENT,
            )
        db.execute(
            "UPDATE discovery_corpus_member_coverage SET members=? "
            "WHERE build_id=? AND admission_sha256=?",
            (str(progress.members), key.build_id, key.admission_sha256),
        )
        return progress
