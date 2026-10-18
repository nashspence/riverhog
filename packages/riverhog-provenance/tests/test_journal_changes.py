from __future__ import annotations

import copy
import hashlib
import sqlite3
from contextlib import closing

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    append_assertion_batches,
    append_correction,
    assertion,
    assertion_reference,
    journal_store,
    new_id,
    parse_journal,
    reference,
    validate_journal,
)


class ObservedCursor(sqlite3.Cursor):
    def __iter__(self):
        return self

    def __next__(self):
        row = super().__next__()
        if self.recording:
            self.counts["retired_rows"] += 1
            self.counts["retired_identity_octets"] += len(row[0].encode("utf-8"))
        return row

    def close(self):
        self.closed = True
        return super().close()


class ObservedConnection(sqlite3.Connection):
    def execute(self, sql, parameters=()):
        cursor = self.cursor(factory=ObservedCursor)
        cursor.recording = getattr(self, "recording", False)
        cursor.closed = False
        if cursor.recording:
            cursor.counts = self.counts
            self.statements.append((sql, parameters, cursor))
        return cursor.execute(sql, parameters)


@pytest.fixture
def store(monkeypatch):
    connect = journal_store.sqlite3.connect

    def observed_connect(*args, **kwargs):
        return connect(*args, factory=ObservedConnection, **kwargs)

    monkeypatch.setattr(journal_store.sqlite3, "connect", observed_connect)
    value = journal_store.JournalStore()
    try:
        yield value
    finally:
        value.close()


def identities(count):
    return [f"urn:uuid:00000000-0000-4000-8000-{n:012x}" for n in range(count)]


def queue(store, count):
    store.db.executemany(
        "INSERT INTO changes VALUES (?)", ((value,) for value in identities(count))
    )


def forbid_assertion_reads(action, table, column, database, trigger):
    if action == sqlite3.SQLITE_READ and table == "assertions":
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def test_retirement_includes_inactive_identities_without_reading_or_rewriting_payloads(store):
    keys = identities(7)
    payload = b"x" * (1024 * 1024)
    store.db.executemany(
        "INSERT INTO assertions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            (key, key, "extensions", "extension", None, None, "0", payload, n % 2)
            for n, key in enumerate(keys)
        ),
    )
    queue(store, len(keys))
    before = store.db.execute("SELECT * FROM assertions ORDER BY assertion_id").fetchall()
    store.db.set_authorizer(forbid_assertion_reads)
    try:
        counts = [store._reset_changed_assertions(row_budget=3) for _ in range(4)]
    finally:
        store.db.set_authorizer(None)
    assert counts == [3, 3, 1, 0]
    assert store.db.execute("SELECT * FROM changes").fetchall() == []
    assert store.db.execute("SELECT * FROM assertions ORDER BY assertion_id").fetchall() == before


@pytest.mark.parametrize("budget", [0, -1, True, 1.0, None])
def test_invalid_row_admission_performs_no_query(store, budget):
    statements = []
    store.db.set_trace_callback(statements.append)
    try:
        with pytest.raises(ValueError, match="positive"):
            store._reset_changed_assertions(row_budget=budget)
    finally:
        store.db.set_trace_callback(None)
    assert statements == []


def test_native_frame_replay_retires_only_the_prior_changed_set(journal, who, catalog, monkeypatch):
    with validate_journal(journal, catalog=catalog) as initial:
        artifact = initial.graph["artifacts"][0]["id"]
        initial_rows = sum(len(rows) for rows in initial.graph.values())
    batches = (
        {
            "extensions": [
                assertion(
                    "extension",
                    who,
                    subject=reference(artifact, "artifact"),
                    property="urn:test:property",
                    value={"type": "text", "value": str(n)},
                )
            ]
        }
        for n in range(257)
    )
    raw = append_assertion_batches(journal, batches, recorded_by_agent_id=who, catalog=catalog)
    counts = []
    reset = journal_store.JournalStore._reset_changed_assertions

    def observed(self, *, row_budget):
        count = reset(self, row_budget=row_budget)
        counts.append(count)
        return count

    monkeypatch.setattr(journal_store.JournalStore, "_reset_changed_assertions", observed)
    with validate_journal(raw, catalog=catalog) as summary:
        assert summary.journal_sha256 == hashlib.sha256(raw).hexdigest()
        assert len(summary.frames) == 258
        assert len(summary.graph["extensions"]) == 257
    assert sum(counts) == initial_rows + 256
    assert max(counts) <= 256


def test_correction_rebuilds_exact_effective_selection_and_preserves_original_frames(
    journal, who, catalog
):
    with validate_journal(journal, catalog=catalog) as original:
        old = dict(original.graph["agents"][0])
        changed = copy.deepcopy(old)
        changed["assertion_id"] = new_id()
        changed["name"] = "Changed label"
        target = assertion_reference(original, old["assertion_id"])
    raw = append_correction(
        journal,
        [target],
        reason="label",
        recorded_by_agent_id=who,
        assertions={"agents": [changed]},
        catalog=catalog,
    )
    with validate_journal(raw, catalog=catalog) as summary:
        assert summary.retracted_assertion_ids == {old["assertion_id"]}
        assert dict(summary.graph["agents"][0]) == changed
        assert [frame.encoded for frame in summary.frames] == [
            frame.encoded for frame in parse_journal(raw)
        ]
        assert summary.journal_sha256 == hashlib.sha256(raw).hexdigest()


@pytest.mark.growth_witness(
    "riverhog-changed-assertion-reset-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_store.JournalStore._reset_changed_assertions",
)
def test_native_metadata_retirement_records_actual_bounded_work(monkeypatch, record_property):
    binding = growth_binding(journal_store.JournalStore._reset_changed_assertions)
    projection = project_bindings((binding,))
    declaration = projection["declarations"][binding.declaration.id]
    samples = []
    connect = journal_store.sqlite3.connect

    def observed_connect(*args, **kwargs):
        return connect(*args, factory=ObservedConnection, **kwargs)

    monkeypatch.setattr(journal_store.sqlite3, "connect", observed_connect)
    for queued, budget in [
        (0, 1),
        (2, 1),
        (17, 3),
        (129, 3),
        (1025, 3),
        (65, 64),
        (64, 64),
        (17, 64),
    ]:
        current = journal_store.JournalStore()
        try:
            queue(current, queued)
            db = current.db
            db.counts = {"retired_rows": 0, "retired_identity_octets": 0}
            db.statements = []
            instructions = [0]

            def progress(counter=instructions):
                counter[0] += 1
                return 0

            db.set_progress_handler(progress, 1)
            db.set_authorizer(forbid_assertion_reads)
            db.recording = True
            try:
                actual = current._reset_changed_assertions(row_budget=budget)
            finally:
                db.recording = False
                db.set_authorizer(None)
                db.set_progress_handler(None, 0)
            assert actual == db.counts["retired_rows"] == min(queued, budget)
            assert len(db.statements) == 1 and db.statements[0][2].closed
            sql, parameters, _ = db.statements[0]
            with closing(db.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as rows:
                plan = " ".join(row[3] for row in rows)
            assert (
                "SEARCH changes USING" in plan
                and "COVERING INDEX" in plan
                and "USE TEMP" not in plan
            )
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": hashlib.sha256(canonical_json_bytes(declaration)).hexdigest(),
                "dimensions": {"queued_identities": queued, "row_budget": budget},
                "metrics": {
                    "sql_statements": len(db.statements),
                    **db.counts,
                    "sqlite_vm_instructions": instructions[0],
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
        finally:
            current.close()
    record_property("growth_samples", samples)
