"""Bounded SHA-256 leaves and resumable RFC 9162 commitment work.

The build owner supplies the canonical source order, exact next-source cursor,
and independently verified source-manifest and extraction-policy identities.
This private accumulator neither authenticates sources nor authorizes publication.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from riverhog_core.canonical_discovery_fact_work import FACT_FRAGMENT_BYTES

DOMAIN = "riverhog-canonical-discovery-factor-merkle/v1"
_FORMAT = DOMAIN + "/work/v1"
_EMPTY_ROOT = hashlib.sha256(b"").digest()


class StaleCommitmentWork(RuntimeError):
    """The owner, source, policy, phase or exact work version changed."""


@dataclass(frozen=True, slots=True)
class CommitmentKey:
    build_id: str
    task_id: str
    worker_id: str
    source_manifest_sha256: str
    extraction_policy_sha256: str


@dataclass(frozen=True, slots=True)
class CommitmentProgress:
    version: int
    phase: Literal["leaves", "fold", "complete"]
    leaf_count: int
    next_source_key: bytes
    pending_level: int | None
    pending_digest: bytes | None
    fold_level: int
    fold_digest: bytes | None
    fold_leaf_count: int
    root: bytes | None


@dataclass(frozen=True, slots=True)
class PreparedCommitmentStep:
    """One detached carry or fold, prepared without an owner write transaction."""

    expected: CommitmentProgress
    operation: Literal["place", "carry", "fold", "complete"]
    level: int | None
    frontier_digest: bytes | None
    result_digest: bytes


def _natural(value: int) -> bytes:
    if type(value) is not int or value < 0:
        raise ValueError("commitment coordinates must be nonnegative integers")
    return str(value).encode("ascii")


def _digest(value: bytes) -> None:
    if type(value) is not bytes or len(value) != 32:
        raise ValueError("a SHA-256 digest must contain exactly 32 bytes")


def _field(value: bytes) -> bytes:
    return str(len(value)).encode("ascii") + b":" + value


def _leaf_coordinates(table: bytes, record_key: bytes) -> None:
    # These are private schema codes and source-ordinal handles. Variable names,
    # identifiers, values and pointer tokens belong to segmented record content.
    if type(table) is not bytes or not table or len(table) > 128:
        raise ValueError("table code must be a nonempty bounded byte identifier")
    if type(record_key) is not bytes or not record_key or len(record_key) > 256:
        raise ValueError("record key must be a nonempty bounded source-coordinate encoding")


def fact_fragment_leaf(table: bytes, record_key: bytes, ordinal: int, fragment: bytes) -> bytes:
    """Hash one exact representation fragment, independently of transport packing."""
    _leaf_coordinates(table, record_key)
    index = _natural(ordinal)
    if type(fragment) is not bytes or not 1 <= len(fragment) <= FACT_FRAGMENT_BYTES:
        raise ValueError("data leaves require one 1..4096-byte representation fragment")
    payload = b"".join(_field(part) for part in (b"data", table, record_key, index, fragment))
    return hashlib.sha256(b"\x00" + payload).digest()


def fact_terminal_leaf(table: bytes, record_key: bytes, *, octets: int, fragments: int) -> bytes:
    """Commit an explicit empty or complete logical-record terminal."""
    _leaf_coordinates(table, record_key)
    length, count = _natural(octets), _natural(fragments)
    expected = (octets + FACT_FRAGMENT_BYTES - 1) // FACT_FRAGMENT_BYTES
    if fragments != expected:
        raise ValueError("terminal totals disagree with fixed representation fragments")
    payload = b"".join(_field(part) for part in (b"end", table, record_key, length, count))
    return hashlib.sha256(b"\x00" + payload).digest()


def branch_digest(left: bytes, right: bytes) -> bytes:
    _digest(left)
    _digest(right)
    return hashlib.sha256(b"\x01" + left + right).digest()


def _one(db: sqlite3.Connection, sql: str, values: tuple[object, ...]) -> Any:
    cursor = db.execute(sql, values)
    try:
        return cursor.fetchone()
    finally:
        cursor.close()


def _identity(value: str) -> None:
    if type(value) is not str or not value or len(value) > 128 or not value.isascii():
        raise ValueError("work identity must be a nonempty bounded ASCII identifier")


def _source_digest(value: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("source and policy identities must be lowercase SHA-256")


def _cursor(value: bytes) -> None:
    if type(value) is not bytes or len(value) > 512:
        raise ValueError("source cursor must be a bounded exact schema-coordinate encoding")


def initialize_commitment_work(db: sqlite3.Connection) -> None:
    """Install the current private schema, outside any caller transaction."""
    if db.in_transaction:
        raise ValueError("schema initialization requires no active transaction")
    db.execute("PRAGMA foreign_keys = ON")
    db.execute("CREATE TABLE IF NOT EXISTS discovery_commitment_format (format TEXT PRIMARY KEY)")
    formats = db.execute("SELECT format FROM discovery_commitment_format LIMIT 2").fetchall()
    if formats and formats != [(_FORMAT,)]:
        raise StaleCommitmentWork("unsupported discovery commitment work format")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_commitment_tasks (
            build_id TEXT NOT NULL, task_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, source_manifest_sha256 TEXT NOT NULL,
            extraction_policy_sha256 TEXT NOT NULL, initial_source_key BLOB NOT NULL,
            version TEXT NOT NULL,
            phase TEXT NOT NULL CHECK(phase IN ('leaves','fold','complete')),
            leaf_count TEXT NOT NULL,
            next_source_key BLOB NOT NULL CHECK(length(next_source_key) <= 512),
            pending_level TEXT,
            pending_digest BLOB CHECK(length(pending_digest) = 32),
            fold_level TEXT NOT NULL,
            fold_digest BLOB CHECK(length(fold_digest) = 32),
            fold_leaf_count TEXT NOT NULL,
            root BLOB CHECK(length(root) = 32),
            CHECK((pending_level IS NULL) = (pending_digest IS NULL)),
            PRIMARY KEY(build_id, task_id)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_commitment_frontier (
            build_id TEXT NOT NULL, task_id TEXT NOT NULL,
            level TEXT NOT NULL,
            digest BLOB NOT NULL CHECK(length(digest) = 32),
            PRIMARY KEY(build_id, task_id, level),
            FOREIGN KEY(build_id, task_id)
                REFERENCES discovery_commitment_tasks(build_id, task_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS discovery_commitment_frontier_order
            ON discovery_commitment_frontier(build_id,task_id,length(level),level);
    """)
    if not formats:
        db.execute("INSERT INTO discovery_commitment_format VALUES (?)", (_FORMAT,))
        db.commit()


@contextmanager
def _atomic(db: sqlite3.Connection) -> Iterator[None]:
    if not db.in_transaction:
        raise ValueError("commitment mutation requires an owner-managed transaction")
    db.execute("SAVEPOINT discovery_commitment_step")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_commitment_step")
        db.execute("RELEASE discovery_commitment_step")
        raise
    else:
        db.execute("RELEASE discovery_commitment_step")


def _binding(key: CommitmentKey) -> tuple[str, str, str]:
    for value in (key.build_id, key.task_id, key.worker_id):
        _identity(value)
    for value in (key.source_manifest_sha256, key.extraction_policy_sha256):
        _source_digest(value)
    return key.worker_id, key.source_manifest_sha256, key.extraction_policy_sha256


def _stored_natural(value: object) -> int:
    if (
        type(value) is not str
        or not value
        or any(character not in "0123456789" for character in value)
        or len(value) > 1
        and value[0] == "0"
    ):
        raise ValueError("stored commitment coordinate is not exact canonical decimal")
    return int(value)


def _next_frontier(
    db: sqlite3.Connection, key: CommitmentKey, level: int
) -> tuple[int, bytes] | None:
    coordinate = str(level)
    row = _one(
        db,
        "SELECT level, digest FROM discovery_commitment_frontier "
        "WHERE build_id = ? AND task_id = ? AND length(level) >= ? "
        "AND (length(level) > ? OR level >= ?) ORDER BY length(level),level LIMIT 1",
        (key.build_id, key.task_id, len(coordinate), len(coordinate), coordinate),
    )
    return None if row is None else (_stored_natural(row[0]), row[1])


def commitment_progress(db: sqlite3.Connection, key: CommitmentKey) -> CommitmentProgress:
    binding = _binding(key)
    row = _one(
        db,
        "SELECT worker_id, source_manifest_sha256, extraction_policy_sha256, "
        "version, phase, leaf_count, next_source_key, pending_level, pending_digest, "
        "fold_level, fold_digest, fold_leaf_count, root FROM discovery_commitment_tasks "
        "WHERE build_id = ? AND task_id = ?",
        (key.build_id, key.task_id),
    )
    if row is None or row[:3] != binding:
        raise StaleCommitmentWork("no matching commitment build/source/policy/worker binding")
    try:
        progress = CommitmentProgress(
            _stored_natural(row[3]),
            row[4],
            _stored_natural(row[5]),
            row[6],
            None if row[7] is None else _stored_natural(row[7]),
            row[8],
            _stored_natural(row[9]),
            row[10],
            _stored_natural(row[11]),
            row[12],
        )
        for value in (
            progress.version,
            progress.leaf_count,
            progress.fold_level,
            progress.fold_leaf_count,
        ):
            _natural(value)
        _cursor(progress.next_source_key)
        for digest_value in (progress.pending_digest, progress.fold_digest, progress.root):
            if digest_value is not None:
                _digest(digest_value)
        if (
            progress.phase not in {"leaves", "fold", "complete"}
            or progress.fold_leaf_count > progress.leaf_count
            or progress.fold_level > progress.leaf_count.bit_length()
            or (progress.pending_level is None) != (progress.pending_digest is None)
        ):
            raise ValueError("inconsistent commitment checkpoint")
        if progress.pending_level is not None:
            _natural(progress.pending_level)
            if progress.pending_level >= progress.leaf_count.bit_length():
                raise ValueError("pending carry exceeds admitted leaves")
        if progress.phase == "leaves":
            if progress.fold_level or progress.fold_leaf_count or progress.fold_digest is not None:
                raise ValueError("unfrozen commitment already has a fold")
        elif progress.pending_digest is not None:
            raise ValueError("frozen commitment still has a pending leaf")
        if progress.phase == "complete":
            if progress.fold_leaf_count != progress.leaf_count or progress.root != (
                progress.fold_digest or _EMPTY_ROOT
            ):
                raise ValueError("completed commitment has an inconsistent root")
        elif progress.root is not None:
            raise ValueError("unfinished commitment already has a root")
    except ValueError as exc:
        raise StaleCommitmentWork("invalid durable commitment checkpoint") from exc
    return progress


def begin_commitment(
    db: sqlite3.Connection, key: CommitmentKey, *, initial_source_key: bytes
) -> CommitmentProgress:
    """Register one immutable source/order obligation, or verify an exact retry."""
    binding = _binding(key)
    _cursor(initial_source_key)
    with _atomic(db):
        old = _one(
            db,
            "SELECT worker_id, source_manifest_sha256, extraction_policy_sha256, "
            "initial_source_key FROM discovery_commitment_tasks "
            "WHERE build_id = ? AND task_id = ?",
            (key.build_id, key.task_id),
        )
        if old is None:
            db.execute(
                "INSERT INTO discovery_commitment_tasks VALUES "
                "(?,?,?,?,?,?,'0','leaves','0',?,NULL,NULL,'0',NULL,'0',NULL)",
                (key.build_id, key.task_id, *binding, initial_source_key, initial_source_key),
            )
        elif old != (*binding, initial_source_key):
            raise StaleCommitmentWork("commitment registration changed its exact obligation")
        return commitment_progress(db, key)


def _require_version(progress: CommitmentProgress, expected_version: int) -> None:
    _natural(expected_version)
    if progress.version != expected_version:
        raise StaleCommitmentWork("commitment work version changed")


def append_commitment_leaf(
    db: sqlite3.Connection,
    key: CommitmentKey,
    *,
    expected_version: int,
    source_key: bytes,
    next_source_key: bytes,
    digest: bytes,
) -> CommitmentProgress:
    """Save one prepared leaf, its carry and next-source cursor atomically.

    A lost acknowledgement is resolved by reloading the durable cursor. A stale
    append cannot consume the same leaf twice, including after process replacement.
    """
    _digest(digest)
    _cursor(source_key)
    _cursor(next_source_key)
    if source_key == next_source_key:
        raise ValueError("a consumed source leaf must advance its exact coordinate")
    with _atomic(db):
        progress = commitment_progress(db, key)
        _require_version(progress, expected_version)
        if (
            progress.phase != "leaves"
            or progress.pending_digest is not None
            or progress.next_source_key != source_key
        ):
            raise StaleCommitmentWork("leaf source, phase or unfinished carry does not agree")
        db.execute(
            "UPDATE discovery_commitment_tasks SET version = ?, "
            "leaf_count = ?, next_source_key = ?, "
            "pending_level = '0', pending_digest = ? WHERE build_id = ? AND task_id = ?",
            (
                str(progress.version + 1),
                str(progress.leaf_count + 1),
                next_source_key,
                digest,
                key.build_id,
                key.task_id,
            ),
        )
        return commitment_progress(db, key)


def begin_commitment_fold(
    db: sqlite3.Connection, key: CommitmentKey, *, expected_version: int, source_terminal: bytes
) -> CommitmentProgress:
    """Freeze the leaf stream after the owner independently verifies its terminal."""
    _cursor(source_terminal)
    with _atomic(db):
        progress = commitment_progress(db, key)
        _require_version(progress, expected_version)
        if (
            progress.phase != "leaves"
            or progress.pending_digest is not None
            or progress.next_source_key != source_terminal
        ):
            raise StaleCommitmentWork("source terminal or completed carry is absent")
        db.execute(
            "UPDATE discovery_commitment_tasks SET version = ?, phase = 'fold' "
            "WHERE build_id = ? AND task_id = ?",
            (str(progress.version + 1), key.build_id, key.task_id),
        )
        return commitment_progress(db, key)


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-merkle-step-prepare.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_commitment.prepare_commitment_step",),
        scope="detached-single-carry-or-fold-preparation",
        starts_at="receive the current private work database and exact owner/source/policy key",
        ends_at="return one prepared transition with no open cursor or write transaction",
        dimensions=(
            Dimension("frontier_levels", COUNT, "Actual occupied levels before this preparation."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("constant", value=2, unit=COUNT),
                "One bound task lookup and at most one indexed frontier lookup.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=2, unit=COUNT),
                "At most one fixed-shape task and one frontier row.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                Expression("constant", value=65, unit=OCTETS),
                "At most one prefixed SHA-256 pair; placement/empty completion hash nothing.",
            ),
        ),
        conditions=(
            "Current private work schema with bounded internal identities and source cursor.",
            "The source owner supplies its exact manifest/policy/worker binding.",
            "The function runs outside the owner write transaction.",
        ),
        invariants=(
            "A carry never walks all occupied levels in one preparation.",
            "A fold reads the next occupied level in ascending order.",
            "No fact stream or generation-wide aggregate is read.",
        ),
        limitations=(
            "SQLite ordered-index/storage work is a separate backend cost.",
            "Leaf encoding, source ordering/completeness, mutation and publication are excluded.",
            "The apply owner separately rechecks the exact fixed-size prepared pair.",
        ),
    )
)
def prepare_commitment_step(db: sqlite3.Connection, key: CommitmentKey) -> PreparedCommitmentStep:
    """Detach at most one frontier row and hash at most one pair.

    Indexed point/next-level reads replace a frontier array or a hidden carry
    loop. No cursor remains open and no owner write transaction is held while
    hashing. Finalization reads no fact rows or generation-wide aggregate.
    """
    if db.in_transaction:
        raise ValueError("prepare commitment work outside an owner write transaction")
    progress = commitment_progress(db, key)
    if progress.phase == "complete":
        raise StaleCommitmentWork("commitment is already complete")
    if progress.phase == "leaves":
        if progress.pending_digest is None:
            raise StaleCommitmentWork("there is no pending leaf carry")
        assert progress.pending_level is not None
        row = _one(
            db,
            "SELECT digest FROM discovery_commitment_frontier "
            "WHERE build_id = ? AND task_id = ? AND level = ?",
            (key.build_id, key.task_id, str(progress.pending_level)),
        )
        if row is None:
            return PreparedCommitmentStep(
                progress, "place", progress.pending_level, None, progress.pending_digest
            )
        return PreparedCommitmentStep(
            progress,
            "carry",
            progress.pending_level,
            row[0],
            branch_digest(row[0], progress.pending_digest),
        )
    row = _next_frontier(db, key, progress.fold_level)
    if row is None:
        if progress.fold_leaf_count != progress.leaf_count:
            raise StaleCommitmentWork("frontier loss does not cover the admitted leaf count")
        return PreparedCommitmentStep(
            progress, "complete", None, None, progress.fold_digest or _EMPTY_ROOT
        )
    if not 0 <= row[0] < progress.leaf_count.bit_length():
        raise StaleCommitmentWork("frontier level exceeds the admitted leaf count")
    return PreparedCommitmentStep(
        progress,
        "fold",
        row[0],
        row[1],
        row[1] if progress.fold_digest is None else branch_digest(row[1], progress.fold_digest),
    )


def apply_commitment_step(
    db: sqlite3.Connection, key: CommitmentKey, step: PreparedCommitmentStep
) -> CommitmentProgress:
    """Commit one detached carry/fold and its checkpoint under the owner fence.

    Recheck the fixed-size prepared pair before mutation. This private work
    transaction never borrows a catalog connection or opens a source iterator.
    """
    _digest(step.result_digest)
    expected = step.expected
    if step.operation == "place":
        result = expected.pending_digest
    elif step.operation == "carry":
        if step.frontier_digest is None or expected.pending_digest is None:
            raise StaleCommitmentWork("prepared carry has no exact digest pair")
        result = branch_digest(step.frontier_digest, expected.pending_digest)
    elif step.operation == "fold":
        if step.frontier_digest is None:
            raise StaleCommitmentWork("prepared fold has no exact subtree")
        result = (
            step.frontier_digest
            if expected.fold_digest is None
            else branch_digest(step.frontier_digest, expected.fold_digest)
        )
    elif step.operation == "complete":
        result = expected.fold_digest or _EMPTY_ROOT
    else:
        raise ValueError("unknown commitment transition")
    if result != step.result_digest:
        raise StaleCommitmentWork("prepared commitment digest changed")
    with _atomic(db):
        progress = commitment_progress(db, key)
        if progress != step.expected:
            raise StaleCommitmentWork("prepared commitment step has a stale checkpoint")
        if progress.phase == "complete":
            raise StaleCommitmentWork("commitment is already complete")
        if step.operation == "place":
            if (
                progress.phase != "leaves"
                or progress.pending_level != step.level
                or progress.pending_digest != step.result_digest
                or step.frontier_digest is not None
            ):
                raise StaleCommitmentWork("prepared placement does not cover the exact carry")
            db.execute(
                "INSERT INTO discovery_commitment_frontier VALUES (?,?,?,?)",
                (key.build_id, key.task_id, str(step.level), step.result_digest),
            )
            db.execute(
                "UPDATE discovery_commitment_tasks SET version = ?, "
                "pending_level = NULL, pending_digest = NULL "
                "WHERE build_id = ? AND task_id = ?",
                (str(progress.version + 1), key.build_id, key.task_id),
            )
        elif step.operation in {"carry", "fold"}:
            if step.level is None or step.frontier_digest is None:
                raise StaleCommitmentWork("prepared pair has no exact frontier coordinate")
            if step.operation == "carry" and (
                progress.phase != "leaves"
                or progress.pending_level != step.level
                or progress.pending_digest is None
            ):
                raise StaleCommitmentWork("prepared carry does not cover the pending level")
            if step.operation == "fold" and (
                progress.phase != "fold" or step.level < progress.fold_level
            ):
                raise StaleCommitmentWork("prepared fold does not cover the next frontier level")
            if step.operation == "fold":
                first = _next_frontier(db, key, progress.fold_level)
                if first != (step.level, step.frontier_digest):
                    raise StaleCommitmentWork("fold skipped the next occupied frontier level")
            removed = db.execute(
                "DELETE FROM discovery_commitment_frontier "
                "WHERE build_id = ? AND task_id = ? AND level = ? AND digest = ?",
                (key.build_id, key.task_id, str(step.level), step.frontier_digest),
            ).rowcount
            if removed != 1:
                raise StaleCommitmentWork("prepared frontier row changed")
            if step.operation == "carry":
                db.execute(
                    "UPDATE discovery_commitment_tasks SET version = ?, "
                    "pending_level = ?, pending_digest = ? WHERE build_id = ? AND task_id = ?",
                    (
                        str(progress.version + 1),
                        str(step.level + 1),
                        step.result_digest,
                        key.build_id,
                        key.task_id,
                    ),
                )
            else:
                db.execute(
                    "UPDATE discovery_commitment_tasks SET version = ?, "
                    "fold_level = ?, fold_digest = ?, fold_leaf_count = ? "
                    "WHERE build_id = ? AND task_id = ?",
                    (
                        str(progress.version + 1),
                        str(step.level + 1),
                        step.result_digest,
                        str(progress.fold_leaf_count + (1 << step.level)),
                        key.build_id,
                        key.task_id,
                    ),
                )
        elif step.operation == "complete":
            if (
                progress.phase != "fold"
                or step.level is not None
                or progress.fold_leaf_count != progress.leaf_count
                or step.frontier_digest is not None
            ):
                raise StaleCommitmentWork("prepared terminal does not cover the fold")
            leftover = _one(
                db,
                "SELECT level FROM discovery_commitment_frontier "
                "WHERE build_id = ? AND task_id = ? LIMIT 1",
                (key.build_id, key.task_id),
            )
            if leftover is not None:
                raise StaleCommitmentWork("commitment still has unfolded frontier rows")
            db.execute(
                "UPDATE discovery_commitment_tasks SET version = ?, "
                "phase = 'complete', root = ? WHERE build_id = ? AND task_id = ?",
                (str(progress.version + 1), step.result_digest, key.build_id, key.task_id),
            )
        else:
            raise ValueError("unknown commitment transition")
        return commitment_progress(db, key)
