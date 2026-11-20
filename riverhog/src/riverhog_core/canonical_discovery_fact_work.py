"""Versioned segment obligations in an owner-leased discovery work database.

The owning build supplies verified source identities and owns transactions,
leases, extraction coverage and publication. This store accounts for exact
physical streams; completed storage alone does not establish source completeness.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, cast

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

FACT_FRAGMENT_BYTES = 4096
_FORMAT = "riverhog-canonical-discovery-fact-work/v1"


class StaleFactTask(RuntimeError):
    """The exact build, source, worker or work version no longer agrees."""


class FactSegmentConflict(ValueError):
    """A segment or terminal conflicts with already recorded exact work."""


@dataclass(frozen=True, slots=True)
class FactTaskKey:
    build_id: str
    task_id: str
    source_key: str
    worker_id: str


@dataclass(frozen=True, slots=True)
class FactStreamKey:
    """One producer-assigned work identifier, independent of payload equality."""

    kind: str
    record_key: str


@dataclass(frozen=True, slots=True)
class FactTaskProgress:
    version: int
    streams: int
    open_streams: int
    fragments: int
    octets: int
    complete: bool


def _identity(value: str) -> None:
    if type(value) is not str or not value or len(value) > 128 or not value.isascii():
        raise ValueError("work identity must be a nonempty bounded ASCII identifier")


def _digest(value: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("source identities must be lowercase SHA-256")


def _natural(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("work ordinal, count or version must be a nonnegative integer")


def initialize_fact_work(db: sqlite3.Connection) -> None:
    """Install one current private work schema outside a caller transaction."""
    if db.in_transaction:
        raise ValueError("work schema initialization requires no active transaction")
    db.execute("PRAGMA foreign_keys = ON")
    db.execute("CREATE TABLE IF NOT EXISTS discovery_work_format (format TEXT PRIMARY KEY)")
    formats = db.execute("SELECT format FROM discovery_work_format LIMIT 2").fetchall()
    if formats and formats != [(_FORMAT,)]:
        raise StaleFactTask("unsupported discovery work format")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_fact_tasks (
            build_id TEXT NOT NULL, task_id TEXT NOT NULL,
            source_key TEXT NOT NULL, worker_id TEXT NOT NULL,
            assertion_id TEXT NOT NULL, origin_sha256 TEXT NOT NULL,
            canonical_stream_ref TEXT NOT NULL,
            version INTEGER NOT NULL CHECK(version >= 0),
            streams INTEGER NOT NULL CHECK(streams >= 0),
            open_streams INTEGER NOT NULL CHECK(open_streams >= 0),
            fragments INTEGER NOT NULL CHECK(fragments >= 0),
            octets INTEGER NOT NULL CHECK(octets >= 0),
            complete INTEGER NOT NULL CHECK(complete IN (0,1)),
            terminal_version INTEGER,
            PRIMARY KEY(build_id, task_id)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS discovery_fact_cursors (
            build_id TEXT NOT NULL, task_id TEXT NOT NULL,
            category TEXT NOT NULL,
            record_ordinal INTEGER NOT NULL CHECK(record_ordinal >= 0),
            source_node INTEGER NOT NULL CHECK(source_node >= 0),
            source_position INTEGER NOT NULL CHECK(source_position >= 0),
            value_position INTEGER NOT NULL CHECK(value_position >= 0),
            fragment_ordinal INTEGER NOT NULL CHECK(fragment_ordinal >= 0),
            section_ordinal INTEGER NOT NULL CHECK(section_ordinal >= 0),
            stack_depth INTEGER NOT NULL CHECK(stack_depth >= 0),
            hash_state TEXT NOT NULL CHECK(length(hash_state) <= 512),
            PRIMARY KEY(build_id, task_id),
            FOREIGN KEY(build_id, task_id)
                REFERENCES discovery_fact_tasks(build_id, task_id)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_fact_stack (
            build_id TEXT NOT NULL, task_id TEXT NOT NULL,
            depth INTEGER NOT NULL CHECK(depth > 0),
            source_node INTEGER NOT NULL CHECK(source_node >= 0),
            next_child INTEGER NOT NULL CHECK(next_child >= 0),
            path_node INTEGER NOT NULL CHECK(path_node >= 0),
            PRIMARY KEY(build_id, task_id, depth),
            FOREIGN KEY(build_id, task_id)
                REFERENCES discovery_fact_tasks(build_id, task_id)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_fact_streams (
            build_id TEXT NOT NULL, task_id TEXT NOT NULL,
            kind TEXT NOT NULL, record_key TEXT NOT NULL,
            fragments INTEGER NOT NULL CHECK(fragments >= 0),
            octets INTEGER NOT NULL CHECK(octets >= 0),
            last_fragment_bytes INTEGER NOT NULL CHECK(last_fragment_bytes BETWEEN 0 AND 4096),
            complete INTEGER NOT NULL CHECK(complete IN (0,1)),
            terminal_version INTEGER,
            PRIMARY KEY(build_id, task_id, kind, record_key),
            FOREIGN KEY(build_id, task_id)
                REFERENCES discovery_fact_tasks(build_id, task_id)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_fact_segments (
            build_id TEXT NOT NULL, task_id TEXT NOT NULL,
            kind TEXT NOT NULL, record_key TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            exact_bytes BLOB NOT NULL CHECK(length(exact_bytes) BETWEEN 1 AND 4096),
            sha256 TEXT NOT NULL,
            staged_version INTEGER NOT NULL CHECK(staged_version >= 0),
            PRIMARY KEY(build_id, task_id, kind, record_key, ordinal),
            FOREIGN KEY(build_id, task_id, kind, record_key)
                REFERENCES discovery_fact_streams(build_id, task_id, kind, record_key)
        ) WITHOUT ROWID;
    """)
    if not formats:
        db.execute("INSERT INTO discovery_work_format(format) VALUES (?)", (_FORMAT,))
        db.commit()


@contextmanager
def _atomic(db: sqlite3.Connection) -> Iterator[None]:
    if not db.in_transaction:
        raise ValueError("fact work requires an owner-managed transaction")
    db.execute("SAVEPOINT discovery_fact_step")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_fact_step")
        db.execute("RELEASE discovery_fact_step")
        raise
    else:
        db.execute("RELEASE discovery_fact_step")


def _one(db: sqlite3.Connection, sql: str, parameters: tuple[object, ...]) -> Any:
    cursor = db.execute(sql, parameters)
    try:
        return cursor.fetchone()
    finally:
        cursor.close()


def _task(db: sqlite3.Connection, key: FactTaskKey) -> tuple[Any, ...]:
    for value in (key.build_id, key.task_id, key.worker_id):
        _identity(value)
    _digest(key.source_key)
    row = _one(
        db,
        "SELECT source_key, worker_id, version, streams, open_streams, fragments, "
        "octets, complete, terminal_version, canonical_stream_ref "
        "FROM discovery_fact_tasks WHERE build_id = ? AND task_id = ?",
        (key.build_id, key.task_id),
    )
    if row is None or row[:2] != (key.source_key, key.worker_id):
        raise StaleFactTask("fact task has no matching build/source/worker binding")
    return cast(tuple[Any, ...], row)


def _progress(row: tuple[Any, ...]) -> FactTaskProgress:
    return FactTaskProgress(
        version=int(row[2]),
        streams=int(row[3]),
        open_streams=int(row[4]),
        fragments=int(row[5]),
        octets=int(row[6]),
        complete=bool(row[7]),
    )


def fact_task_progress(db: sqlite3.Connection, key: FactTaskKey) -> FactTaskProgress:
    return _progress(_task(db, key))


def begin_assertion(
    db: sqlite3.Connection,
    key: FactTaskKey,
    *,
    assertion_id: str,
    origin_sha256: str,
    canonical_stream_ref: str,
) -> FactTaskProgress:
    """Register an exact source obligation; identical registration changes nothing."""
    for value in (key.build_id, key.task_id, key.worker_id, assertion_id):
        _identity(value)
    for value in (key.source_key, origin_sha256, canonical_stream_ref):
        _digest(value)
    with _atomic(db):
        old = _one(
            db,
            "SELECT source_key, worker_id, assertion_id, origin_sha256, canonical_stream_ref "
            "FROM discovery_fact_tasks WHERE build_id = ? AND task_id = ?",
            (key.build_id, key.task_id),
        )
        expected = (
            key.source_key,
            key.worker_id,
            assertion_id,
            origin_sha256,
            canonical_stream_ref,
        )
        if old is None:
            db.execute(
                "INSERT INTO discovery_fact_tasks VALUES (?,?,?,?,?,?,?,0,0,0,0,0,0,NULL)",
                (key.build_id, key.task_id, *expected),
            )
            db.execute(
                "INSERT INTO discovery_fact_cursors VALUES (?,?,?,0,0,0,0,0,0,0,'')",
                (key.build_id, key.task_id, "canonical"),
            )
        elif old != expected:
            raise StaleFactTask("assertion source obligation changed during registration")
        _cursor(db, key)
        return fact_task_progress(db, key)


def _stream_identity(key: FactTaskKey, stream: FactStreamKey) -> tuple[str, str, str, str]:
    _identity(stream.kind)
    _identity(stream.record_key)
    return key.build_id, key.task_id, stream.kind, stream.record_key


def _require_version(task: tuple[Any, ...], version: int) -> None:
    _natural(version)
    if int(task[2]) != version or task[7]:
        raise StaleFactTask("assertion work version changed or is already complete")


def _segment_declaration() -> Declaration:
    return Declaration(
        id="riverhog-server.discovery-fact-segment.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_fact_work.stage_fact_segment",),
        scope="one exact discovery fact fragment transition or rejected/retried transition",
        starts_at="receive one fragment and the exact owner-leased task/stream/version binding",
        ends_at="return the current task progress or reject without a partial transition",
        dimensions=(
            Dimension("fragment_octets", OCTETS, "Actual immutable input fragment bytes."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("constant", value=12, unit=COUNT),
                "Fixed point reads, bounded writes and savepoint release/rollback.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=4, unit=COUNT),
                "At most two task headers, one exact segment and one stream header.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="fragment_octets"),
                "At most one exact fragment SHA-256; no source or prior-stream rescan.",
            ),
            Bound(
                "bound_fragment_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="fragment_octets"),
                "Only a new segment binds fragment payload bytes.",
            ),
        ),
        conditions=(
            "The owner holds the workspace lease and opens a bounded write transaction.",
            "The current private schema has its exact primary keys and no added triggers.",
            "The immutable fragment contains 1 through 4096 bytes.",
            "Task, source and logical stream identifiers retain their fixed internal shapes.",
        ),
        invariants=(
            "An exact retry changes neither counters nor physical segment rows.",
            "Source and worker bindings are checked before accepting a stored exact retry.",
            "One savepoint commits or rolls back segment, stream and parent checkpoint together.",
            "This transition never enumerates preceding streams or segments.",
        ),
        limitations=(
            "Statement and returned-row counts exclude SQLite B-tree/storage-engine work.",
            "Lease acquisition, transaction commit and source extraction have separate owners.",
            "Source completeness and generation publication require independent owner evidence.",
            "Logical stream totals remain unbounded across bounded fragment transitions.",
        ),
    )


@declare_growth(_segment_declaration())
def stage_fact_segment(
    db: sqlite3.Connection,
    key: FactTaskKey,
    stream: FactStreamKey,
    *,
    ordinal: int,
    exact_bytes: bytes,
    expected_version: int,
) -> FactTaskProgress:
    """Stage one canonical byte fragment and its checkpoint atomically.

    A short fragment ends the data of its logical stream. A separate terminal
    records exact count/length, including for an empty stream. This fixed fragment
    encoding does not limit the total size of an assertion or logical value.
    """
    _natural(ordinal)
    _natural(expected_version)
    if type(exact_bytes) is not bytes or not 1 <= len(exact_bytes) <= FACT_FRAGMENT_BYTES:
        raise ValueError("a data fragment must contain 1 to 4096 exact bytes")
    identity = _stream_identity(key, stream)
    digest = hashlib.sha256(exact_bytes).hexdigest()
    with _atomic(db):
        task = _task(db, key)
        old = _one(
            db,
            "SELECT exact_bytes, sha256, staged_version FROM discovery_fact_segments "
            "WHERE build_id = ? AND task_id = ? AND kind = ? AND record_key = ? AND ordinal = ?",
            (*identity, ordinal),
        )
        if old is not None:
            if old != (exact_bytes, digest, expected_version):
                raise FactSegmentConflict("exact segment or retry version changed")
            return _progress(task)
        _require_version(task, expected_version)
        current = _one(
            db,
            "SELECT fragments, octets, last_fragment_bytes, complete "
            "FROM discovery_fact_streams "
            "WHERE build_id = ? AND task_id = ? AND kind = ? AND record_key = ?",
            identity,
        )
        if current is None:
            if ordinal:
                raise FactSegmentConflict("a stream cannot begin after ordinal zero")
            db.execute("INSERT INTO discovery_fact_streams VALUES (?,?,?,?,0,0,0,0,NULL)", identity)
            db.execute(
                "UPDATE discovery_fact_tasks SET streams = streams + 1, "
                "open_streams = open_streams + 1 WHERE build_id = ? AND task_id = ?",
                (key.build_id, key.task_id),
            )
        elif current[3] or ordinal != current[0] or current[2] != FACT_FRAGMENT_BYTES:
            raise FactSegmentConflict("segment gap, terminal tail or short-fragment tail")
        db.execute(
            "INSERT INTO discovery_fact_segments VALUES (?,?,?,?,?,?,?,?)",
            (*identity, ordinal, exact_bytes, digest, expected_version),
        )
        db.execute(
            "UPDATE discovery_fact_streams SET fragments = fragments + 1, "
            "octets = octets + ?, last_fragment_bytes = ? "
            "WHERE build_id = ? AND task_id = ? AND kind = ? AND record_key = ?",
            (len(exact_bytes), len(exact_bytes), *identity),
        )
        db.execute(
            "UPDATE discovery_fact_tasks SET version = version + 1, "
            "fragments = fragments + 1, octets = octets + ? "
            "WHERE build_id = ? AND task_id = ?",
            (len(exact_bytes), key.build_id, key.task_id),
        )
        return fact_task_progress(db, key)


def finish_fact_stream(
    db: sqlite3.Connection,
    key: FactTaskKey,
    stream: FactStreamKey,
    *,
    fragments: int,
    octets: int,
    expected_version: int,
) -> FactTaskProgress:
    """Record one exact stream terminal; empty streams remain explicit records."""
    for value in (fragments, octets, expected_version):
        _natural(value)
    identity = _stream_identity(key, stream)
    with _atomic(db):
        task = _task(db, key)
        current = _one(
            db,
            "SELECT fragments, octets, complete, terminal_version "
            "FROM discovery_fact_streams "
            "WHERE build_id = ? AND task_id = ? AND kind = ? AND record_key = ?",
            identity,
        )
        if current is not None and current[2]:
            if current != (fragments, octets, 1, expected_version):
                raise FactSegmentConflict("exact stream terminal changed during retry")
            return _progress(task)
        _require_version(task, expected_version)
        if current is None:
            if fragments or octets:
                raise FactSegmentConflict("nonempty stream terminal has no data")
            db.execute(
                "INSERT INTO discovery_fact_streams VALUES (?,?,?,?,0,0,0,1,?)",
                (*identity, expected_version),
            )
            db.execute(
                "UPDATE discovery_fact_tasks SET streams = streams + 1 "
                "WHERE build_id = ? AND task_id = ?",
                (key.build_id, key.task_id),
            )
        else:
            if current[:2] != (fragments, octets):
                raise FactSegmentConflict("stream terminal does not cover its exact data")
            db.execute(
                "UPDATE discovery_fact_streams SET complete = 1, terminal_version = ? "
                "WHERE build_id = ? AND task_id = ? AND kind = ? AND record_key = ?",
                (expected_version, *identity),
            )
            db.execute(
                "UPDATE discovery_fact_tasks SET open_streams = open_streams - 1 "
                "WHERE build_id = ? AND task_id = ?",
                (key.build_id, key.task_id),
            )
        db.execute(
            "UPDATE discovery_fact_tasks SET version = version + 1 "
            "WHERE build_id = ? AND task_id = ?",
            (key.build_id, key.task_id),
        )
        return fact_task_progress(db, key)


def finish_assertion(
    db: sqlite3.Connection,
    key: FactTaskKey,
    *,
    source_terminal: str,
    expected_version: int,
) -> FactTaskProgress:
    """Seal storage against an independently verified exact source reference.

    The extractor must establish complete source traversal before making this
    call. The publication owner separately checks that proof and build coverage.
    """
    _digest(source_terminal)
    _natural(expected_version)
    with _atomic(db):
        task = _task(db, key)
        if task[9] != source_terminal:
            raise StaleFactTask("assertion terminal names another source")
        if task[7]:
            if task[8] != expected_version:
                raise FactSegmentConflict("assertion terminal retry version changed")
            return _progress(task)
        _require_version(task, expected_version)
        if task[4]:
            raise FactSegmentConflict("assertion still has unterminated fact streams")
        cursor = _cursor(db, key)
        if cursor.category != "done" or cursor.stack_depth:
            raise FactSegmentConflict("assertion source extraction checkpoint is unfinished")
        db.execute(
            "UPDATE discovery_fact_tasks SET complete = 1, version = version + 1, "
            "terminal_version = ? WHERE build_id = ? AND task_id = ?",
            (expected_version, key.build_id, key.task_id),
        )
        return fact_task_progress(db, key)


@dataclass(frozen=True, slots=True)
class FactExtractionCursor:
    category: str = "canonical"
    record_ordinal: int = 0
    source_node: int = 0
    source_position: int = 0
    value_position: int = 0
    fragment_ordinal: int = 0
    section_ordinal: int = 0
    stack_depth: int = 0
    hash_state: str = ""


@dataclass(frozen=True, slots=True)
class FactStackFrame:
    source_node: int
    next_child: int
    path_node: int


@dataclass(frozen=True, slots=True)
class FactSegmentWrite:
    stream: FactStreamKey
    ordinal: int
    exact_bytes: bytes


@dataclass(frozen=True, slots=True)
class FactStreamTerminal:
    stream: FactStreamKey
    fragments: int
    octets: int


_CATEGORIES = ("canonical", "scalars", "names", "profiles", "edges", "done")


def _cursor_values(cursor: FactExtractionCursor) -> tuple[Any, ...]:
    if cursor.category not in _CATEGORIES:
        raise ValueError("unknown extraction category")
    values = (
        cursor.category,
        cursor.record_ordinal,
        cursor.source_node,
        cursor.source_position,
        cursor.value_position,
        cursor.fragment_ordinal,
        cursor.section_ordinal,
        cursor.stack_depth,
        cursor.hash_state,
    )
    for value in values[1:-1]:
        _natural(value)
    if (
        type(cursor.hash_state) is not str
        or len(cursor.hash_state) > 512
        or not cursor.hash_state.isascii()
    ):
        raise ValueError("hash continuation exceeds its fixed private checkpoint shape")
    return values


def _cursor(db: sqlite3.Connection, key: FactTaskKey) -> FactExtractionCursor:
    row = _one(
        db,
        "SELECT category, record_ordinal, source_node, source_position, value_position, "
        "fragment_ordinal, section_ordinal, stack_depth, hash_state "
        "FROM discovery_fact_cursors WHERE build_id = ? AND task_id = ?",
        (key.build_id, key.task_id),
    )
    if row is None:
        raise StaleFactTask("assertion extraction checkpoint is absent")
    result = FactExtractionCursor(*row)
    _cursor_values(result)
    return result


def fact_extraction_cursor(
    db: sqlite3.Connection, key: FactTaskKey
) -> tuple[FactTaskProgress, FactExtractionCursor]:
    """Load one bound checkpoint; no completed source postings are enumerated."""
    task = _task(db, key)
    return _progress(task), _cursor(db, key)


def fact_stack_top(db: sqlite3.Connection, key: FactTaskKey) -> FactStackFrame | None:
    """Read one stack frame by its exact depth, without reconstructing the stack."""
    _task(db, key)
    depth = _cursor(db, key).stack_depth
    if not depth:
        return None
    row = _one(
        db,
        "SELECT source_node, next_child, path_node FROM discovery_fact_stack "
        "WHERE build_id = ? AND task_id = ? AND depth = ?",
        (key.build_id, key.task_id, depth),
    )
    if row is None:
        raise StaleFactTask("assertion extraction stack is incomplete")
    return FactStackFrame(*row)


@dataclass(frozen=True, slots=True)
class FactAssertionBinding:
    assertion_id: str
    origin_sha256: str
    canonical_stream_ref: str


def fact_assertion_binding(db: sqlite3.Connection, key: FactTaskKey) -> FactAssertionBinding:
    """Detach one exact assertion obligation without scanning source or work rows."""
    _task(db, key)
    row = _one(
        db,
        "SELECT assertion_id, origin_sha256, canonical_stream_ref "
        "FROM discovery_fact_tasks WHERE build_id = ? AND task_id = ?",
        (key.build_id, key.task_id),
    )
    if row is None:
        raise StaleFactTask("assertion source obligation is absent")
    return FactAssertionBinding(*row)


def fact_stack_frame(db: sqlite3.Connection, key: FactTaskKey, *, depth: int) -> FactStackFrame:
    """Detach one exact saved frame for bounded speculative page preparation."""
    _natural(depth)
    _task(db, key)
    cursor = _cursor(db, key)
    if not 1 <= depth <= cursor.stack_depth:
        raise StaleFactTask("requested frame is outside the current extraction stack")
    row = _one(
        db,
        "SELECT source_node, next_child, path_node FROM discovery_fact_stack "
        "WHERE build_id = ? AND task_id = ? AND depth = ?",
        (key.build_id, key.task_id, depth),
    )
    if row is None:
        raise StaleFactTask("assertion extraction stack is incomplete")
    return FactStackFrame(*row)


def advance_assertion(
    db: sqlite3.Connection,
    key: FactTaskKey,
    *,
    expected_version: int,
    next_cursor: FactExtractionCursor,
    segment: FactSegmentWrite | None = None,
    terminal: FactStreamTerminal | None = None,
    stack_action: str = "none",
    stack_frame: FactStackFrame | None = None,
) -> FactTaskProgress:
    """Commit one prepared physical action, stack edit and extraction checkpoint.

    The source owner prepares this one bounded action before opening its write
    transaction. Reopening loads the saved node/position directly rather than
    reparsing and skipping completed postings. Every stack frame has its own
    indexed row; no checkpoint contains an extent-sized frame array.
    """
    _natural(expected_version)
    values = _cursor_values(next_cursor)
    if segment is not None and terminal is not None:
        raise ValueError("one extraction transition can write only one physical action")
    if stack_action not in {"none", "push", "pop", "replace"}:
        raise ValueError("unknown extraction stack action")
    if (stack_action in {"push", "replace"}) != (stack_frame is not None):
        raise ValueError("stack frame and action do not agree")
    if stack_frame is not None:
        for value in (stack_frame.source_node, stack_frame.next_child, stack_frame.path_node):
            _natural(value)
    with _atomic(db):
        task = _task(db, key)
        _require_version(task, expected_version)
        current = _cursor(db, key)
        old_category = _CATEGORIES.index(current.category)
        new_category = _CATEGORIES.index(next_cursor.category)
        if current.category == "done" or new_category not in {old_category, old_category + 1}:
            raise FactSegmentConflict("extraction category cannot reverse or skip unfinished work")
        if next_cursor.record_ordinal < current.record_ordinal:
            raise FactSegmentConflict("extraction record cursor cannot move backwards")
        depth_change = 1 if stack_action == "push" else -1 if stack_action == "pop" else 0
        if (
            next_cursor.stack_depth != current.stack_depth + depth_change
            or stack_action in {"pop", "replace"}
            and not current.stack_depth
            or new_category != old_category
            and (current.stack_depth or next_cursor.stack_depth)
        ):
            raise FactSegmentConflict("extraction stack checkpoint does not cover its exact edit")
        if segment is not None:
            stage_fact_segment(
                db,
                key,
                segment.stream,
                ordinal=segment.ordinal,
                exact_bytes=segment.exact_bytes,
                expected_version=expected_version,
            )
        elif terminal is not None:
            finish_fact_stream(
                db,
                key,
                terminal.stream,
                fragments=terminal.fragments,
                octets=terminal.octets,
                expected_version=expected_version,
            )
        else:
            db.execute(
                "UPDATE discovery_fact_tasks SET version = version + 1 "
                "WHERE build_id = ? AND task_id = ?",
                (key.build_id, key.task_id),
            )
        if new_category != old_category and fact_task_progress(db, key).open_streams:
            raise FactSegmentConflict("extraction category still has unterminated streams")
        if stack_action == "push":
            assert stack_frame is not None
            db.execute(
                "INSERT INTO discovery_fact_stack VALUES (?,?,?,?,?,?)",
                (
                    key.build_id,
                    key.task_id,
                    next_cursor.stack_depth,
                    stack_frame.source_node,
                    stack_frame.next_child,
                    stack_frame.path_node,
                ),
            )
        elif stack_action == "replace":
            assert stack_frame is not None
            updated = db.execute(
                "UPDATE discovery_fact_stack SET source_node = ?, next_child = ?, path_node = ? "
                "WHERE build_id = ? AND task_id = ? AND depth = ?",
                (
                    stack_frame.source_node,
                    stack_frame.next_child,
                    stack_frame.path_node,
                    key.build_id,
                    key.task_id,
                    current.stack_depth,
                ),
            ).rowcount
            if updated != 1:
                raise StaleFactTask("extraction stack frame disappeared")
        elif stack_action == "pop":
            removed = db.execute(
                "DELETE FROM discovery_fact_stack WHERE build_id = ? AND task_id = ? AND depth = ?",
                (key.build_id, key.task_id, current.stack_depth),
            ).rowcount
            if removed != 1:
                raise StaleFactTask("extraction stack frame disappeared")
        db.execute(
            "UPDATE discovery_fact_cursors SET category = ?, record_ordinal = ?, "
            "source_node = ?, source_position = ?, value_position = ?, fragment_ordinal = ?, "
            "section_ordinal = ?, stack_depth = ?, hash_state = ? "
            "WHERE build_id = ? AND task_id = ?",
            (*values, key.build_id, key.task_id),
        )
        return fact_task_progress(db, key)
