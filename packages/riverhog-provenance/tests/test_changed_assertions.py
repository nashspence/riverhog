from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing, contextmanager

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance._changed_assertions import (
    ChangedAssertionCapacity,
    read_changed_assertion,
)
from test_assertion_staging import connect, create, identity
from test_entry_preparation import logical_octets, owner


class Cursor(sqlite3.Cursor):
    closed = False

    def fetchone(self):
        row = super().fetchone()
        if row is not None and self.connection.recording:
            self.connection.metrics["returned_rows"] += 1
            field = (
                "returned_atom_octets"
                if self.sql.startswith("SELECT value ")
                else "returned_metadata_octets"
            )
            self.connection.metrics[field] += logical_octets(row)
        if self.connection.fault == self.sql.split(" FROM ")[0]:
            raise RuntimeError("injected fetch failure")
        return row

    def close(self):
        self.closed = True
        return super().close()


class Connection(sqlite3.Connection):
    recording = False
    fault = None

    def execute(self, sql, parameters=()):
        cursor = self.cursor(factory=Cursor)
        cursor.sql = sql
        if self.recording:
            self.statements.append((sql, parameters, cursor))
            self.metrics["bound_parameter_octets"] += logical_octets(parameters)
        return cursor.execute(sql, parameters)

    @contextmanager
    def observe(self):
        self.metrics = dict.fromkeys(
            (
                "sql_statements",
                "returned_rows",
                "returned_metadata_octets",
                "returned_atom_octets",
                "bound_parameter_octets",
                "changed_rows",
                "historical_payload_octets",
                "sqlite_vm_instructions",
            ),
            0,
        )
        self.statements = []
        before, transaction = self.total_changes, self.in_transaction
        self.set_trace_callback(
            lambda _sql: self.metrics.__setitem__(
                "sql_statements", self.metrics["sql_statements"] + 1
            )
        )

        def tick():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        self.set_progress_handler(tick, 1)
        self.recording = True
        try:
            yield self.metrics
        finally:
            self.recording = False
            self.set_trace_callback(None)
            self.set_progress_handler(None, 0)
            self.metrics["changed_rows"] = self.total_changes - before
        assert self.in_transaction == transaction
        assert all(cursor.closed for _sql, _args, cursor in self.statements)
        for sql, args, _cursor in self.statements:
            with closing(self.execute("EXPLAIN QUERY PLAN " + sql, args)) as cursor:
                plan = " ".join(row[3] for row in cursor.fetchall())
            assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan, plan


def insert(db, ordinal, *, active=True, raw=None):
    body = (
        raw
        if raw is not None
        else canonical_json_bytes(
            {
                "assertion_id": identity(ordinal),
                "id": identity(1000000 + ordinal),
                "type": "agent",
            }
        )
    )
    db.execute(
        "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
        (
            identity(ordinal),
            identity(1000000 + ordinal),
            "agents",
            "agent",
            None,
            None,
            "0",
            body,
            int(active),
        ),
    ).close()
    return body


def prepare(path, count, mode, *, payload=0):
    create(path)
    with closing(connect(path)) as db:
        for ordinal in range(count):
            key = 10 + ordinal
            db.execute("INSERT INTO changes VALUES (?)", (identity(key),)).close()
            if mode != "missing":
                insert(
                    db,
                    key,
                    active=mode != "retired",
                    raw=b"x" * (payload if ordinal == 0 else 1) if payload else None,
                )
        for ordinal in range(4097 if count in (17, 1025) else count):
            insert(
                db, 10000 + ordinal, active=False, raw=b"unread" * (65536 if ordinal == 0 else 1)
            )
        db.commit()


@pytest.mark.parametrize("mode", ["active", "retired", "missing", "end", "capacity"])
@pytest.mark.parametrize("query_only", [False, True])
def test_one_identity_preserves_view_and_closes_every_cursor(tmp_path, mode, query_only):
    path = tmp_path / "rows.sqlite3"
    prepare(path, 17, mode, payload=1024)
    with closing(connect(path, factory=Connection)) as db:
        if query_only:
            db.execute("PRAGMA query_only=ON").close()
        before = db.total_changes
        with db.observe() as metrics:
            if mode == "capacity":
                with pytest.raises(ChangedAssertionCapacity):
                    read_changed_assertion(db, after=None, byte_budget=1023)
                selected = read_changed_assertion(db, after=None, byte_budget=1024)
                assert selected.raw == b"x" * 1024
            else:
                selected = read_changed_assertion(
                    db, after=identity(9999) if mode == "end" else None, byte_budget=1024
                )
                if mode == "end":
                    assert selected is None
                else:
                    assert selected.assertion_id == identity(10)
                    assert selected.raw == (b"x" * 1024 if mode == "active" else None)
        assert metrics["changed_rows"] == 0 and db.total_changes == before


@pytest.mark.parametrize("fault", ["SELECT assertion_id", "SELECT length(value)", "SELECT value"])
def test_fetch_failure_closes_current_cursor_without_mutation(tmp_path, fault):
    path = tmp_path / "rows.sqlite3"
    prepare(path, 1, "active")
    with closing(connect(path, factory=Connection)) as db:
        db.fault = fault
        with db.observe() as metrics, pytest.raises(RuntimeError, match="injected fetch"):
            read_changed_assertion(db, after=None, byte_budget=1024)
        assert metrics["changed_rows"] == 0


def test_iterator_visits_sparse_keys_once_without_prefetch_or_held_cursor(tmp_path):
    path = tmp_path / "rows.sqlite3"
    create(path)
    with closing(connect(path, factory=Connection)) as db:
        first = insert(db, 1)
        insert(db, 2, active=False, raw=b"never-decode-retired")
        insert(db, 4)
        insert(db, 5, raw=b"invalid-json")
        for key in (1, 2, 3, 4, 5):
            db.execute("INSERT INTO changes VALUES (?)", (identity(key),)).close()
        db.commit()
        rows = owner(db)._changed_assertion_rows()
        with db.observe():
            assert next(rows) == json.loads(first)
        with db.observe() as metrics:
            assert next(rows)["assertion_id"] == identity(4)
        assert (
            metrics["sql_statements"] == 7
        )  # Retired, missing and active keys, without a gap scan.
        with db.observe(), pytest.raises(json.JSONDecodeError):
            next(rows)
        rows.close()


def test_native_row_checks_observe_no_open_assertion_cursor(journal, catalog, monkeypatch):
    from riverhog_provenance import _entry_replay, journal_store, validate_journal
    from riverhog_provenance._generic_semantics import GenericSemantics

    connections = []
    original_connect = sqlite3.connect
    original_admission = GenericSemantics.prepare_admission

    def tracked_connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs, factory=Connection)
        connections.append(connection)
        return connection

    seen = []

    def admission(worker):
        assert worker.db in connections and not worker.db.in_transaction
        assert all(cursor.closed for _sql, _args, cursor in worker.db.statements)
        seen.append(worker.assertion_id)
        return original_admission(worker)

    # Track only the read primitive, so other owning SQL lifetimes remain independent.
    original_read = _entry_replay.read_changed_assertion

    def read(db, **kwargs):
        with db.observe():
            return original_read(db, **kwargs)

    monkeypatch.setattr(journal_store.sqlite3, "connect", tracked_connect)
    monkeypatch.setattr(_entry_replay, "read_changed_assertion", read)
    monkeypatch.setattr(GenericSemantics, "prepare_admission", admission)
    with validate_journal(journal, catalog=catalog) as summary:
        assert seen == sorted(seen) and set(seen) == set(summary.assertion_entries)
        assert summary.journal_bytes == len(journal)


@pytest.mark.growth_witness(
    "riverhog-changed-assertion-admission-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._changed_assertions.read_changed_assertion",
)
def test_actual_cold_changed_assertion_admission(tmp_path, record_property):
    binding = growth_binding(read_changed_assertion)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    modes = ("active", "retired", "missing", "end", "capacity")
    cases = [(n, mode) for n in (1, 15, 16, 17, 129, 1025) for mode in modes] + [(0, "end")]
    for ordinal, (count, mode) in enumerate(cases):
        path = tmp_path / f"admission-{ordinal}.sqlite3"
        payload = 1024 * 1024 if count in (17, 1025) else 1024
        prepare(path, count, mode, payload=payload)
        with closing(connect(path, factory=Connection)) as db:
            with closing(db.execute("SELECT count(*) FROM assertions")) as cursor:
                retained = cursor.fetchone()[0]
            with db.observe() as metrics:
                if mode == "capacity":
                    with pytest.raises(ChangedAssertionCapacity):
                        read_changed_assertion(db, after=None, byte_budget=payload - 1)
                    atom = 0
                else:
                    selected = read_changed_assertion(
                        db, after=identity(9999) if mode == "end" else None, byte_budget=payload
                    )
                    atom = 0 if selected is None or selected.raw is None else len(selected.raw)
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {
                    "indexed_assertions": retained,
                    "changed_identities": count,
                    "atom_octets": atom,
                },
                "metrics": dict(metrics),
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
    record_property("growth_samples", samples)
