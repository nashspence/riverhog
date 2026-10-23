from __future__ import annotations

import hashlib
import sqlite3
from collections import defaultdict
from contextlib import contextmanager

import pytest
import riverhog_canonical_json as canonical
import riverhog_provenance.history as history
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryStore,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

HISTORY = history.MemberHistoryClosure._pending_history
SNAPSHOT = history.MemberHistoryClosure._pending_snapshot


def octets(value):
    if isinstance(value, str):
        return len(value.encode())
    if isinstance(value, bytes):
        return len(value)
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
    def observe(self, closure, monkeypatch):
        self.metrics.clear()

        def stepped():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def decoded(*args, **kwargs):
            self.metrics["json_calls"] += 1
            pytest.fail("pending selection decoded or encoded a source")

        def hashed(value=b"", *args, **kwargs):
            self.metrics["hashed_octets"] += len(value)
            pytest.fail("pending selection hashed a source")

        self.actual.set_progress_handler(stepped, 1)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(closure, "_db", self)
                patch.setattr(canonical.json, "loads", decoded)
                patch.setattr(canonical, "canonical_json_bytes", decoded)
                patch.setattr(history, "canonical_json_bytes", decoded)
                patch.setattr(history, "require_canonical_json", decoded)
                patch.setattr(history.hashlib, "sha256", hashed)
                yield
        finally:
            self.actual.set_progress_handler(None, 0)
            assert self.live == 0


def binding(number):
    identity = f"{number:064x}"
    return MemberHistoryBinding(identity, number, identity, identity, 100)


def anchor(number):
    return HistoryJournalAnchor(
        "urn:uuid:00000000-0000-4000-8000-000000000001",
        f"urn:uuid:00000000-0000-4000-8000-{number:012x}",
        number,
        f"{number:064x}",
        f"{number:064x}",
        number + 1,
    )


def closure():
    def forbidden(*args):
        pytest.fail("pending selection attempted an archive read")

    return history.MemberHistoryClosure(
        MemberHistoryStore(forbidden), forbidden, member_role="member"
    )


def measure(current, recorder, operation, dimensions, monkeypatch):
    with recorder.observe(current, monkeypatch):
        result = operation(current)
    assert recorder.metrics["closed_cursors"] == 1
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": {
            bound.metric: recorder.metrics[bound.metric] for bound in binding.declaration.bounds
        },
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return result, sample


def samples(monkeypatch, operation):
    records = []
    table, fields, index = (
        ("histories", "identity, extent, binding", "pending_histories")
        if operation is HISTORY
        else ("snapshots", "identity, anchor", "pending_snapshots")
    )
    for count in (0, 1, 17, 129, 1025):
        with closure() as current:
            for number in range(1, count + 1):
                if operation is HISTORY:
                    current._history(
                        binding(number),
                        BOUND_HISTORY_EXTENT if number % 2 else RETAINED_HISTORY_EXTENT,
                    )
                else:
                    current._snapshot(anchor(number))
            expected = current._db.execute(
                f"SELECT rowid, {fields} FROM {table} ORDER BY rowid"
            ).fetchall()
            dimensions = {
                "registered_jobs": count,
                "candidate_octets": max((octets(row[1:]) for row in expected), default=0),
            }
            recorder = Recorder(current._db)
            plan = current._db.execute(
                f"EXPLAIN QUERY PLAN SELECT {fields} FROM {table} "
                "WHERE done = 0 ORDER BY rowid LIMIT 1"
            ).fetchall()
            assert any(index in row[-1] for row in plan)
            for expected_row in expected:
                result, sample = measure(current, recorder, operation, dimensions, monkeypatch)
                assert result == expected_row[1:]
                records.append(sample)
                current._db.execute(f"UPDATE {table} SET done=1 WHERE rowid=?", (expected_row[0],))
            result, sample = measure(current, recorder, operation, dimensions, monkeypatch)
            assert result is None
            records.append(sample)
            assert current._db.execute(f"SELECT count(*) FROM {table}").fetchone() == (count,)
            # Completed rows retain deduplication; a new dependency is still selectable.
            if operation is HISTORY:
                current._history(binding(count + 1), BOUND_HISTORY_EXTENT)
                if count:
                    current._history(binding(1), BOUND_HISTORY_EXTENT)
            else:
                current._snapshot(anchor(count + 1))
                if count:
                    current._snapshot(anchor(1))
            remaining = current._db.execute(f"SELECT {fields} FROM {table} WHERE done=0").fetchall()
            assert len(remaining) == 1
            assert operation(current) == remaining[0]
            assert current._db.execute(f"SELECT count(*) FROM {table} WHERE done=1").fetchone() == (
                count,
            )
    return records


@pytest.mark.growth_witness(
    "native-history-pending-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.history.MemberHistoryClosure._pending_history",
)
def test_actual_history_pending_selection_skips_completed_work(monkeypatch, record_property):
    record_property("growth_samples", samples(monkeypatch, HISTORY))


@pytest.mark.growth_witness(
    "native-snapshot-pending-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.history.MemberHistoryClosure._pending_snapshot",
)
def test_actual_snapshot_pending_selection_skips_completed_work(monkeypatch, record_property):
    record_property("growth_samples", samples(monkeypatch, SNAPSHOT))


@pytest.mark.parametrize("operation", [HISTORY, SNAPSHOT])
def test_pending_selection_closes_cursor_on_failure_and_preserves_retry(operation, monkeypatch):
    with closure() as current:
        if operation is HISTORY:
            current._history(binding(1), BOUND_HISTORY_EXTENT)
        else:
            current._snapshot(anchor(1))
        original = operation(current)
        recorder = Recorder(current._db)
        error = sqlite3.OperationalError("interrupted pending fetch")
        recorder.failure = error
        with (
            pytest.raises(sqlite3.OperationalError) as failed,
            recorder.observe(current, monkeypatch),
        ):
            operation(current)
        assert failed.value is error
        assert recorder.metrics["closed_cursors"] == 1
        assert operation(current) == original


@pytest.mark.parametrize("operation", [HISTORY, SNAPSHOT])
def test_actual_sqlite_interruption_keeps_the_obligation_pending(operation):
    with closure() as current:
        if operation is HISTORY:
            current._history(binding(1), BOUND_HISTORY_EXTENT)
        else:
            current._snapshot(anchor(1))
        original = operation(current)
        current._db.set_progress_handler(lambda: 1, 1)
        try:
            with pytest.raises(sqlite3.OperationalError, match="interrupted"):
                operation(current)
        finally:
            current._db.set_progress_handler(None, 0)
        assert operation(current) == original
