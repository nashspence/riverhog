"""Persist source-normal endpoint candidates beneath exact original fact terminals."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from typing import Any, cast

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes, format_scalar, require_canonical_json

from riverhog_core.canonical_discovery_dependency_source import (
    initialize_dependency_source,
    next_source_assertion,
)
from riverhog_core.canonical_discovery_endpoint_index_growth import (
    declare_growth_for_endpoint_index,
)
from riverhog_core.canonical_discovery_endpoint_source import JournalEndpoint, read_journal_endpoint
from riverhog_core.canonical_discovery_fact_work import FACT_FRAGMENT_BYTES, FactTaskKey, _digest
from riverhog_core.canonical_discovery_fact_work import _task as fact_task
from riverhog_core.canonical_discovery_journal_lookup import _natural, documentary_journal_assertion
from riverhog_core.canonical_discovery_journal_work import (
    JournalAssertion,
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
    _atomic,
    _one,
    _task,
)

_ALGORITHM = "riverhog-discovery-endpoint-index/v1"
_PHASES = ("assertions", "streams", "complete")
_ENDPOINT_COLUMNS = (
    "build_id,source_key,assertion_id,record_key,task_id,canonical_stream_ref,"
    "role,index_role,target_scope,target_type,target_sha256,target_octets,"
    "born_sequence,retired_sequence"
)
_CURSOR_COLUMNS = (
    "after_assertion,after_task,after_sequence,after_group,after_item,after_origin,after_stream"
)
_STATE_COLUMNS = (
    "version,phase,assertions,endpoints,"
    + _CURSOR_COLUMNS
    + ",stream_kind,stream_record,streams,fragments,octets,last_output,"
    "child_receipt,directory_receipt"
)


@dataclass(frozen=True, slots=True)
class EndpointIndexProgress:
    version: int
    phase: str
    assertions: int
    endpoints: int
    after: JournalAssertion | None
    stream_after: tuple[str, str] | None
    streams: int
    fragments: int
    octets: int
    last_output: tuple[Any, ...] | None
    child_receipt: tuple[Any, ...] | None
    directory_receipt: tuple[Any, ...] | None = None

    @property
    def complete(self) -> bool:
        return self.phase == "complete"


@dataclass(frozen=True, slots=True)
class PreparedEndpointIndex:
    key: JournalTaskKey
    expected: EndpointIndexProgress
    result: EndpointIndexProgress
    directory: tuple[Any, ...] | None
    endpoint: JournalEndpoint | None


class EndpointIndexWork:
    """One original source lease and one uncommitted bounded preparation.

    The application supplies existing metadata custody and current-worker checks.
    The stable attempt identity is not a substitute for those worker-epoch fences.
    """

    def __init__(self, source: QualifiedJournalSource, anchor: HistoryJournalAnchor) -> None:
        source.check_open()
        if (
            anchor.through_sequence != source.entry_count - 1
            or source.anchor_at(anchor.through_sequence) != anchor.to_mapping()
        ):
            raise StaleJournalTask("endpoint indexing requires the complete original source")
        self.source, self.anchor = source, anchor
        self.binding = self._binding()
        self._prepared: PreparedEndpointIndex | None = None

    def _binding(self) -> tuple[str, ...]:
        return (
            self.source.binding_json,
            self.source.terminal_json,
            format_scalar("nonnegative", self.source.entry_count),
            canonical_json_bytes(self.anchor.to_mapping()).decode(),
            _ALGORITHM,
        )

    def check(self, key: JournalTaskKey) -> None:
        self.source.check_open()
        if key.source_key != self.source.source_key or self._binding() != self.binding:
            raise StaleJournalTask("endpoint indexing lost its exact original source")


def initialize_endpoint_index(db: sqlite3.Connection) -> None:
    initialize_dependency_source(db)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_endpoint_tasks (
            build_id TEXT NOT NULL, source_key TEXT NOT NULL, worker_id TEXT NOT NULL,
            binding_json TEXT NOT NULL, terminal_json TEXT NOT NULL, entry_count TEXT NOT NULL,
            anchor_json TEXT NOT NULL, algorithm TEXT NOT NULL,
            source_version TEXT NOT NULL, expected_assertions TEXT NOT NULL,
            version TEXT NOT NULL, phase TEXT NOT NULL,
            assertions TEXT NOT NULL, endpoints TEXT NOT NULL,
            after_assertion TEXT, after_task TEXT, after_sequence TEXT,
            after_group TEXT, after_item INTEGER, after_origin TEXT, after_stream TEXT,
            stream_kind TEXT, stream_record TEXT,
            streams TEXT NOT NULL, fragments TEXT NOT NULL, octets TEXT NOT NULL,
            last_output TEXT, child_receipt TEXT, directory_receipt TEXT,
            PRIMARY KEY(build_id,source_key),
            FOREIGN KEY(build_id,source_key)
                REFERENCES discovery_journal_tasks(build_id,source_key)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_endpoints (
            build_id TEXT NOT NULL, source_key TEXT NOT NULL,
            assertion_id TEXT NOT NULL, record_key TEXT NOT NULL,
            task_id TEXT NOT NULL, canonical_stream_ref TEXT NOT NULL,
            role TEXT NOT NULL, index_role TEXT NOT NULL,
            target_scope TEXT NOT NULL, target_type TEXT NOT NULL,
            target_sha256 TEXT NOT NULL, target_octets INTEGER NOT NULL,
            born_sequence TEXT NOT NULL, retired_sequence TEXT,
            PRIMARY KEY(build_id,source_key,assertion_id,record_key),
            FOREIGN KEY(build_id,source_key)
                REFERENCES discovery_endpoint_tasks(build_id,source_key),
            FOREIGN KEY(build_id,source_key,assertion_id)
                REFERENCES discovery_journal_assertions(build_id,source_key,assertion_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS discovery_endpoint_inverse ON discovery_endpoints (
            build_id,source_key,index_role,target_scope,target_type,target_sha256,
            assertion_id,record_key
        );
        CREATE INDEX IF NOT EXISTS discovery_endpoint_births ON discovery_endpoints (
            build_id,source_key,length(born_sequence),born_sequence,assertion_id,record_key
        );
        CREATE INDEX IF NOT EXISTS discovery_endpoint_retirements ON discovery_endpoints (
            build_id,source_key,length(retired_sequence),retired_sequence,assertion_id,record_key
        ) WHERE retired_sequence IS NOT NULL;
    """)


def _parent(
    db: sqlite3.Connection, key: JournalTaskKey, work: EndpointIndexWork
) -> tuple[str, str]:
    work.check(key)
    row = _task(db, key)
    if (
        row[1:4] != work.binding[:3]
        or row[6] != "done"
        or row[5] != row[3]
        or row[13] != "0"
        or row[14] != row[2]
        or row[15] != 1
        or _natural(row[4]) != _natural(row[16]) + 1
    ):
        raise StaleJournalTask("endpoint indexing requires the completed original journal")
    _natural(row[11])
    return row[4], row[11]


def _cursor_values(after: JournalAssertion | None) -> tuple[Any, ...]:
    return (
        (None,) * 7
        if after is None
        else (
            after.assertion_id,
            after.key.task_id,
            format_scalar("nonnegative", after.sequence),
            after.group,
            after.item,
            after.origin_entry_json,
            after.canonical_stream_ref,
        )
    )


def _state_values(progress: EndpointIndexProgress) -> tuple[Any, ...]:
    return (
        format_scalar("nonnegative", progress.version),
        progress.phase,
        format_scalar("nonnegative", progress.assertions),
        format_scalar("nonnegative", progress.endpoints),
        *_cursor_values(progress.after),
        *((None, None) if progress.stream_after is None else progress.stream_after),
        format_scalar("nonnegative", progress.streams),
        format_scalar("nonnegative", progress.fragments),
        format_scalar("nonnegative", progress.octets),
        None
        if progress.last_output is None
        else canonical_json_bytes(list(progress.last_output)).decode(),
        None
        if progress.child_receipt is None
        else canonical_json_bytes(list(progress.child_receipt)).decode(),
        None
        if progress.directory_receipt is None
        else canonical_json_bytes(list(progress.directory_receipt)).decode(),
    )


def _current_child(
    db: sqlite3.Connection, key: JournalTaskKey, after: JournalAssertion
) -> tuple[Any, ...]:
    row = _one(
        db,
        "SELECT assertion_id,task_id,born_sequence,source_group,source_item,"
        "origin_entry_json,canonical_stream_ref,acknowledged "
        "FROM discovery_journal_assertions WHERE build_id=? AND source_key=? AND assertion_id=?",
        (key.build_id, key.source_key, after.assertion_id),
    )
    if row is None or row[:7] != _cursor_values(after) or row[7] != 1:
        raise StaleJournalTask("endpoint cursor lost its original completed assertion")
    child = fact_task(db, after.key)
    if (
        child[7] != 1
        or child[4] != 0
        or child[9] != after.canonical_stream_ref
        or type(child[2]) is not int
        or type(child[8]) is not int
        or child[8] < 0
        or child[2] != child[8] + 1
    ):
        raise StaleJournalTask("endpoint indexing lost its completed fact terminal")
    return child


def endpoint_index_progress(
    db: sqlite3.Connection, key: JournalTaskKey, work: EndpointIndexWork
) -> EndpointIndexProgress:
    parent = _parent(db, key, work)
    row = _one(
        db,
        "SELECT worker_id,binding_json,terminal_json,entry_count,anchor_json,algorithm,"
        "source_version,expected_assertions,"
        + _STATE_COLUMNS
        + " FROM discovery_endpoint_tasks WHERE build_id=? AND source_key=?",
        (key.build_id, key.source_key),
    )
    if row is None or row[:8] != (key.worker_id, *work.binding, *parent):
        raise StaleJournalTask("endpoint index has another source, terminal or attempt")
    state = row[8:]
    version, phase, assertions, endpoints = (
        _natural(state[0]),
        state[1],
        _natural(state[2]),
        _natural(state[3]),
    )
    if phase not in _PHASES:
        raise StaleJournalTask("endpoint index phase changed")
    cursor = state[4:11]
    if any(value is None for value in cursor) and cursor != (None,) * 7:
        raise StaleJournalTask("endpoint index lost its exact original assertion cursor")
    after = (
        None
        if cursor[0] is None
        else JournalAssertion(
            _fact_key(key, cursor[1]),
            cursor[0],
            _natural(cursor[2]),
            cursor[3],
            cursor[4],
            cursor[5],
            cursor[6],
        )
    )
    if (state[11] is None) != (state[12] is None):
        raise StaleJournalTask("endpoint stream cursor is incomplete")
    stream_after = None if state[11] is None else (state[11], state[12])
    counts = (_natural(state[13]), _natural(state[14]), _natural(state[15]))
    last = None
    if state[16] is not None:
        try:
            value = require_canonical_json(state[16].encode())
            if (
                not isinstance(value, list)
                or len(value) != 14
                or value[:2] != [key.build_id, key.source_key]
            ):
                raise ValueError
            last = tuple(value)
        except (TypeError, ValueError):
            raise StaleJournalTask("endpoint index receipt changed") from None
        stored = _one(
            db,
            "SELECT " + _ENDPOINT_COLUMNS + " FROM discovery_endpoints "
            "WHERE build_id=? AND source_key=? AND assertion_id=? AND record_key=?",
            last[:4],
        )
        if stored != last:
            raise StaleJournalTask("endpoint index lost its last exact output")
    try:
        receipt = None
        if state[17] is not None:
            value = require_canonical_json(state[17].encode())
            if not isinstance(value, list) or len(value) != 10:
                raise ValueError
            receipt = tuple(value)
    except (TypeError, ValueError):
        raise StaleJournalTask("endpoint fact terminal receipt changed") from None
    directory_receipt = None
    if state[18] is not None:
        try:
            value = require_canonical_json(state[18].encode())
            if not isinstance(value, list) or len(value) != 7:
                raise ValueError
            directory_receipt = tuple(value)
        except (TypeError, ValueError):
            raise StaleJournalTask("endpoint directory receipt changed") from None
    progress = EndpointIndexProgress(
        version,
        phase,
        assertions,
        endpoints,
        after,
        stream_after,
        *counts,
        last,
        receipt,
        directory_receipt,
    )
    if (
        assertions > _natural(parent[1])
        or (phase == "complete" and assertions != _natural(parent[1]))
        or (after is None and (assertions or phase == "streams"))
        or (phase != "streams" and (stream_after is not None or any(counts)))
        or (stream_after is None and any(counts))
        or (endpoints == 0) != (last is None)
        or (after is None) != (receipt is None)
        or (stream_after is None) != (directory_receipt is None)
        or version < assertions + endpoints + int(phase == "complete")
    ):
        raise StaleJournalTask("endpoint index counters differ from original work")
    if after is not None:
        child = _current_child(db, key, after)
        if child != receipt:
            raise StaleJournalTask("endpoint cursor has another original fact terminal")
        if counts[0] > child[3] or counts[1] > child[5] or counts[2] > child[6]:
            raise StaleJournalTask("endpoint directory exceeds its original fact terminal")
    if directory_receipt is not None:
        assert after is not None
        stored = _one(
            db,
            "SELECT kind,record_key,fragments,octets,last_fragment_bytes,complete,terminal_version "
            "FROM discovery_fact_streams WHERE build_id=? AND task_id=? "
            "AND kind=? AND record_key=?",
            (key.build_id, after.key.task_id, *directory_receipt[:2]),
        )
        if stored != directory_receipt or directory_receipt[:2] != stream_after:
            raise StaleJournalTask("endpoint index lost its exact last directory record")
    return progress


def _fact_key(key: JournalTaskKey, task_id: str) -> FactTaskKey:
    return FactTaskKey(key.build_id, task_id, key.source_key, key.worker_id)


def begin_endpoint_index(
    db: sqlite3.Connection, key: JournalTaskKey, work: EndpointIndexWork
) -> EndpointIndexProgress:
    with _atomic(db):
        parent = _parent(db, key, work)
        present = _one(
            db,
            "SELECT 1 FROM discovery_endpoint_tasks WHERE build_id=? AND source_key=?",
            (key.build_id, key.source_key),
        )
        if present is None:
            if (
                _one(
                    db,
                    "SELECT 1 FROM discovery_endpoints WHERE build_id=? AND source_key=? LIMIT 1",
                    (key.build_id, key.source_key),
                )
                is not None
            ):
                raise StaleJournalTask("endpoint output has no owning index controls")
            initial = EndpointIndexProgress(0, "assertions", 0, 0, None, None, 0, 0, 0, None, None)
            values = (
                key.build_id,
                key.source_key,
                key.worker_id,
                *work.binding,
                *parent,
                *_state_values(initial),
            )
            db.execute(
                "INSERT INTO discovery_endpoint_tasks VALUES("
                + ",".join("?" for _ in values)
                + ")",
                values,
            ).close()
        return endpoint_index_progress(db, key, work)


def _next_stream(
    db: sqlite3.Connection, key: JournalTaskKey, progress: EndpointIndexProgress
) -> tuple[Any, ...] | None:
    assert progress.after is not None
    sql = (
        "SELECT kind,record_key,fragments,octets,last_fragment_bytes,complete,terminal_version "
        "FROM discovery_fact_streams WHERE build_id=? AND task_id=? "
    )
    parameters: tuple[Any, ...] = key.build_id, progress.after.key.task_id
    if progress.stream_after is not None:
        sql += "AND (kind,record_key)>(?,?) "
        parameters += progress.stream_after
    return cast(
        tuple[Any, ...] | None, _one(db, sql + "ORDER BY kind,record_key LIMIT 1", parameters)
    )


def _output(key: JournalTaskKey, endpoint: JournalEndpoint) -> tuple[Any, ...]:
    assertion = endpoint.assertion
    return (
        key.build_id,
        key.source_key,
        assertion.assertion_id,
        endpoint.record_key,
        assertion.key.task_id,
        assertion.canonical_stream_ref,
        endpoint.edge.role,
        endpoint.index_role,
        endpoint.edge.target_scope,
        endpoint.edge.target_type,
        endpoint.target_sha256,
        endpoint.target_octets,
        format_scalar("nonnegative", assertion.born_sequence),
        None
        if assertion.retired_sequence is None
        else format_scalar("nonnegative", assertion.retired_sequence),
    )


def prepare_endpoint_index(
    db: sqlite3.Connection, key: JournalTaskKey, work: EndpointIndexWork, *, octet_budget: int
) -> PreparedEndpointIndex:
    """Inspect one original stream or advance one assertion/terminal boundary."""
    work._prepared = None
    if db.in_transaction:
        raise ValueError("endpoint index preparation requires no write transaction")
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("endpoint execution capacity must be positive")
    before = endpoint_index_progress(db, key, work)
    after = replace(before, version=before.version + 1)
    directory = endpoint = None
    if before.phase == "complete":
        after = before
    elif before.phase == "assertions":
        original = next_source_assertion(
            db, key, through=work.anchor.through_sequence, after=before.after
        )
        if original is None:
            expected = _natural(_parent(db, key, work)[1])
            if before.assertions != expected:
                raise StaleJournalTask("endpoint index ended before original assertion coverage")
            after = replace(after, phase="complete")
        else:
            header = documentary_journal_assertion(
                db, key, work.source, work.anchor, original.assertion_id
            )
            if header is None or header.key != original.key:
                raise StaleJournalTask("endpoint assertion lost its original source header")
            after = replace(
                after,
                phase="streams",
                after=original,
                child_receipt=_current_child(db, key, original),
            )
    else:
        assert before.after is not None
        child = _current_child(db, key, before.after)
        directory = _next_stream(db, key, before)
        if directory is None:
            if (before.streams, before.fragments, before.octets) != (child[3], child[5], child[6]):
                raise StaleJournalTask("endpoint directory lost original fact-stream coverage")
            after = replace(
                after,
                phase="assertions",
                assertions=before.assertions + 1,
                stream_after=None,
                directory_receipt=None,
                streams=0,
                fragments=0,
                octets=0,
            )
        else:
            kind, record, fragments, octets, tail, complete, terminal = directory
            if any(
                type(value) is not int or value < 0 for value in (fragments, octets, tail, terminal)
            ):
                raise StaleJournalTask("endpoint directory has invalid physical controls")
            expected_fragments = (octets + FACT_FRAGMENT_BYTES - 1) // FACT_FRAGMENT_BYTES
            expected_tail = 0 if not octets else (octets - 1) % FACT_FRAGMENT_BYTES + 1
            if (
                complete != 1
                or terminal >= child[8]
                or fragments != expected_fragments
                or tail != expected_tail
            ):
                raise StaleJournalTask("endpoint directory contains an incomplete fact stream")
            if kind == "edge-header":
                endpoint = read_journal_endpoint(
                    db,
                    key,
                    work.source,
                    work.anchor,
                    before.after.assertion_id,
                    record,
                    octet_budget=octet_budget,
                )
            after = replace(
                after,
                stream_after=(kind, record),
                directory_receipt=directory,
                streams=before.streams + 1,
                fragments=before.fragments + fragments,
                octets=before.octets + octets,
                endpoints=before.endpoints + int(endpoint is not None),
                last_output=before.last_output if endpoint is None else _output(key, endpoint),
            )
            if after.streams > child[3] or after.fragments > child[5] or after.octets > child[6]:
                raise StaleJournalTask("endpoint directory exceeds original fact coverage")
    page = PreparedEndpointIndex(key, before, after, directory, endpoint)
    work._prepared = page
    return page


def apply_endpoint_index(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    work: EndpointIndexWork,
    page: PreparedEndpointIndex,
) -> EndpointIndexProgress:
    """Commit one exact prepared output together with its cursor and version."""
    if page.key != key or work._prepared is not page:
        raise StaleJournalTask("endpoint index requires its exact detached preparation")
    with _atomic(db):
        before = endpoint_index_progress(db, key, work)
        if before == page.result and before != page.expected:
            return before
        if before != page.expected:
            raise StaleJournalTask("endpoint index preparation has another current cursor")
        if before.complete:
            return before
        if before.phase == "assertions":
            original = next_source_assertion(
                db, key, through=work.anchor.through_sequence, after=before.after
            )
            if original != (page.result.after if page.result.phase == "streams" else None):
                raise StaleJournalTask("endpoint original next assertion changed before commit")
        elif _next_stream(db, key, before) != page.directory:
            raise StaleJournalTask("endpoint original stream changed before commit")
        if page.endpoint is not None:
            values = _output(key, page.endpoint)
            db.execute(
                "INSERT INTO discovery_endpoints VALUES(" + ",".join("?" for _ in values) + ")",
                values,
            ).close()
        assignments = ",".join(column + "=?" for column in _STATE_COLUMNS.split(","))
        changed = db.execute(
            "UPDATE discovery_endpoint_tasks SET "
            + assignments
            + " WHERE build_id=? AND source_key=? AND worker_id=? AND version=?",
            (
                *_state_values(page.result),
                key.build_id,
                key.source_key,
                key.worker_id,
                format_scalar("nonnegative", before.version),
            ),
        ).rowcount
        if changed != 1:
            raise StaleJournalTask("endpoint index worker changed before commit")
        return page.result


def next_endpoint_candidate(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    work: EndpointIndexWork,
    *,
    role: str,
    target_scope: str,
    target_type: str,
    target_sha256: str,
    after: tuple[str, str] | None = None,
) -> tuple[Any, ...] | None:
    """Return one original coordinate and interval, never digest equality or relevance.

    Consumers must compare original target-stream bytes and apply their exact
    snapshot, same-member, semantic and current-access conditions independently.
    """
    if not endpoint_index_progress(db, key, work).complete:
        raise StaleJournalTask("endpoint candidate index has no completed source terminal")
    _digest(target_sha256)
    sql = (
        "SELECT " + _ENDPOINT_COLUMNS + " FROM discovery_endpoints "
        "INDEXED BY discovery_endpoint_inverse WHERE build_id=? AND source_key=? "
        "AND index_role=? AND target_scope=? AND target_type=? AND target_sha256=? "
    )
    parameters: tuple[Any, ...] = (
        key.build_id,
        key.source_key,
        role,
        target_scope,
        target_type,
        target_sha256,
    )
    if after is not None:
        sql += "AND (assertion_id,record_key)>(?,?) "
        parameters += after
    return cast(
        tuple[Any, ...] | None,
        _one(db, sql + "ORDER BY assertion_id,record_key LIMIT 1", parameters),
    )


declare_growth_for_endpoint_index(globals())
