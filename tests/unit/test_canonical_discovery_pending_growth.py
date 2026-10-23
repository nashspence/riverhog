from __future__ import annotations

import hashlib
import sqlite3
from collections import defaultdict
from contextlib import contextmanager

import pytest
import riverhog_core.canonical_discovery_relevance as owner
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

STATE = owner.MemberRelevance._pending_state
VIEW = owner.MemberRelevance._pending_view


def octets(value):
    if isinstance(value, str):
        return len(value.encode())
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, int):
        return len(str(value))
    if isinstance(value, tuple):
        return sum(octets(item) for item in value)
    assert value is None
    return 0


class Cursor:
    def __init__(self, actual, recorder):
        self.actual, self.recorder = actual, recorder
        recorder.live += 1

    def fetchone(self):
        if self.recorder.failure is not None:
            raise self.recorder.failure
        result = self.actual.fetchone()
        self.recorder.metrics["returned_rows"] += int(result is not None)
        self.recorder.metrics["returned_octets"] += octets(result)
        return result

    def close(self):
        self.actual.close()
        self.recorder.live -= 1
        self.recorder.metrics["closed_cursors"] += 1


class Recorder:
    def __init__(self, actual):
        self.actual, self.metrics, self.live, self.failure = actual, defaultdict(int), 0, None

    def execute(self, query, parameters=()):
        self.metrics["sql_statements"] += 1
        assert not parameters
        return Cursor(self.actual.execute(query, parameters), self)

    @contextmanager
    def observe(self, relevance, monkeypatch):
        self.metrics.clear()

        def stepped():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def decoded(*args, **kwargs):
            self.metrics["json_calls"] += 1
            pytest.fail("pending selection decoded or encoded source facts")

        def hashed(value=b"", *args, **kwargs):
            self.metrics["hashed_octets"] += len(value)
            pytest.fail("pending selection hashed source facts")

        self.actual.set_progress_handler(stepped, 1)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(relevance, "db", self)
                patch.setattr(owner, "canonical_json_bytes", decoded)
                patch.setattr(owner.json, "loads", decoded)
                patch.setattr(hashlib, "sha256", hashed)
                yield
        finally:
            self.actual.set_progress_handler(None, 0)
            assert self.live == 0


def relation(operation):
    return (
        ("states", "journal, prefix, state, scope, value", "pending_states")
        if operation is STATE
        else ("views", "history, extent, binding, own", "pending_views")
    )


def enqueue(db, operation, number, *, width=0):
    if operation is STATE:
        db.execute(
            "INSERT OR IGNORE INTO states VALUES (?, ?, ?, ?, ?, 0)",
            (
                "journal-界",
                "prefix",
                str(number),
                "member" if number % 2 else "input-history",
                b"x" * width,
            ),
        )
    else:
        db.execute(
            "INSERT OR IGNORE INTO views VALUES (?, ?, ?, ?, 0)",
            (str(number), "bound" if number % 2 else "retained", b"x" * width, int(number == 1)),
        )


def samples(monkeypatch, operation):
    records = []
    table, fields, index = relation(operation)
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    for count in (0, 1, 17, 129, 1025):
        with owner.MemberRelevance() as relevance:
            for number in range(1, count + 1):
                enqueue(relevance.db, operation, number, width=number % 137)
            expected = relevance.db.execute(
                f"SELECT rowid, {fields} FROM {table} ORDER BY rowid"
            ).fetchall()
            dimensions = {
                "registered_jobs": count,
                "candidate_octets": max((octets(row[1:]) for row in expected), default=0),
            }
            plan = relevance.db.execute(
                f"EXPLAIN QUERY PLAN SELECT {fields} FROM {table} "
                "WHERE done = 0 ORDER BY rowid LIMIT 1"
            ).fetchall()
            assert any(index in row[-1] for row in plan)
            recorder = Recorder(relevance.db)
            for expected_row in [*expected, None]:
                with recorder.observe(relevance, monkeypatch):
                    result = operation(relevance)
                assert result == (expected_row[1:] if expected_row is not None else None)
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": digest,
                    "dimensions": dimensions,
                    "metrics": {
                        bound.metric: recorder.metrics[bound.metric]
                        for bound in binding.declaration.bounds
                    },
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                records.append(sample)
                if expected_row is not None:
                    relevance.db.execute(
                        f"UPDATE {table} SET done=1 WHERE rowid=?", (expected_row[0],)
                    )
            assert relevance.db.execute(f"SELECT count(*) FROM {table}").fetchone() == (count,)
            # A completed duplicate remains deduplicated; a new dependency is selected.
            if count:
                enqueue(relevance.db, operation, 1)
            enqueue(relevance.db, operation, count + 1, width=131072)
            remaining = relevance.db.execute(
                f"SELECT {fields} FROM {table} WHERE done=0"
            ).fetchall()
            assert len(remaining) == 1 and operation(relevance) == remaining[0]
            assert relevance.db.execute(
                f"SELECT count(*) FROM {table} WHERE done=1"
            ).fetchone() == (count,)
    return records


@pytest.mark.growth_witness(
    "canonical-discovery-state-pending-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_relevance.MemberRelevance._pending_state",
)
def test_actual_state_pending_selection_skips_completed_work(monkeypatch, record_property):
    record_property("growth_samples", samples(monkeypatch, STATE))


@pytest.mark.growth_witness(
    "canonical-discovery-view-pending-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_relevance.MemberRelevance._pending_view",
)
def test_actual_view_pending_selection_skips_completed_work(monkeypatch, record_property):
    record_property("growth_samples", samples(monkeypatch, VIEW))


@pytest.mark.parametrize("operation", [STATE, VIEW])
def test_pending_fetch_failure_closes_cursor_and_retains_exact_retry(operation, monkeypatch):
    with owner.MemberRelevance() as relevance:
        enqueue(relevance.db, operation, 1)
        expected = operation(relevance)
        recorder = Recorder(relevance.db)
        error = sqlite3.OperationalError("interrupted pending fetch")
        recorder.failure = error
        with (
            pytest.raises(sqlite3.OperationalError) as failure,
            recorder.observe(relevance, monkeypatch),
        ):
            operation(relevance)
        assert failure.value is error and recorder.metrics["closed_cursors"] == 1
        assert operation(relevance) == expected


@pytest.mark.parametrize("operation", [STATE, VIEW])
def test_pending_sqlite_interruption_keeps_work_selectable(operation):
    with owner.MemberRelevance() as relevance:
        enqueue(relevance.db, operation, 1)
        expected = operation(relevance)
        relevance.db.set_progress_handler(lambda: 1, 1)
        try:
            with pytest.raises(sqlite3.OperationalError, match="interrupted"):
                operation(relevance)
        finally:
            relevance.db.set_progress_handler(None, 0)
        assert operation(relevance) == expected


@pytest.mark.parametrize("operation", [STATE, VIEW])
def test_pending_dependency_insertions_preserve_order_and_finished_deduplication(operation):
    table, fields, _ = relation(operation)
    with owner.MemberRelevance() as relevance:
        for number in (1, 2):
            enqueue(relevance.db, operation, number)
        expected = relevance.db.execute(
            f"SELECT rowid, {fields} FROM {table} ORDER BY rowid"
        ).fetchall()
        assert operation(relevance) == expected[0][1:]
        relevance.db.execute(f"UPDATE {table} SET done=1 WHERE rowid=?", (expected[0][0],))
        enqueue(relevance.db, operation, 1)
        enqueue(relevance.db, operation, 3)
        assert operation(relevance) == expected[1][1:]
        relevance.db.execute(f"UPDATE {table} SET done=1 WHERE rowid=?", (expected[1][0],))
        assert (
            operation(relevance)
            == relevance.db.execute(f"SELECT {fields} FROM {table} WHERE rowid=3").fetchone()
        )
        assert relevance.db.execute(f"SELECT count(*) FROM {table}").fetchone() == (3,)
