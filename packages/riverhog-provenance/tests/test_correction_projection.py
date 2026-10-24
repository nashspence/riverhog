from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    append_correction,
    assertion_reference,
    journal_store,
    new_id,
    parse_journal,
    validate_journal,
)
from riverhog_provenance._correction_projection import (
    CorrectionCapacity,
    CorrectionState,
    CorrectionWork,
    StaleCorrectionWork,
)

TABLES = ("maps", "findings", "fixities", "lifecycle_extrema")


def identifier(ordinal):
    return f"urn:uuid:00000000-0000-4000-8000-{ordinal:012x}"


def connect(path, **options):
    db = sqlite3.connect(path, check_same_thread=False, **options)
    db.execute("PRAGMA synchronous = FULL").close()
    db.execute("PRAGMA cache_size = -64").close()
    db.execute("PRAGMA temp_store = FILE").close()
    return db


def create(path):
    store = journal_store.JournalStore()
    try:
        with closing(connect(path)) as db:
            store.db.backup(db)
    finally:
        store.close()


def work(db, operation="retire", **options):
    return CorrectionWork(
        db,
        **{
            "operation": operation,
            "attempt_id": "owned-correction-attempt",
            "source_binding": "exact-owned-entry",
            **options,
        },
    )


def fill(db, table, count, payload=1):
    if table == "maps":
        db.executemany(
            "INSERT INTO maps VALUES (?, ?, ?)",
            (
                (
                    "captures",
                    canonical_json_bytes(identifier(n)),
                    canonical_json_bytes({"n": n, "payload": "x" * payload}),
                )
                for n in range(count)
            ),
        )
    elif table == "findings":
        db.executemany(
            "INSERT INTO findings VALUES (?, ?)",
            ((identifier(n), "x" * payload) for n in range(count)),
        )
    elif table == "fixities":
        db.executemany(
            "INSERT INTO fixities VALUES (?, ?, ?)",
            (
                (identifier(n), str(n), hashlib.sha256(str(n).encode()).hexdigest())
                for n in range(count)
            ),
        )
    else:
        db.executemany(
            "INSERT INTO lifecycle_extrema VALUES (?, ?, ?)",
            ((identifier(n), str(n), str(n + 1)) for n in range(count)),
        )


def assertions(db, active, inactive):
    accepted = {identifier(2 * n + 1) for n in range(active)}
    retired = {identifier(2 * n) for n in range(inactive)}
    db.executemany(
        "INSERT INTO assertions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            (
                key,
                key,
                "extensions",
                "extension",
                None,
                None,
                "0",
                canonical_json_bytes({"id": key, "payload": "unchanged"}),
                int(key in accepted),
            )
            for key in sorted(accepted | retired)
        ),
    )
    return accepted, retired


def contents(db):
    return {
        name: db.execute("SELECT * FROM " + name + " ORDER BY rowid").fetchall()
        for name in (
            *TABLES,
            "assertions",
            "changes",
            "frames",
            "journal_prefix",
            "journal_correction_work",
        )
    }


def logical_octets(row):
    return sum(
        len(value) if isinstance(value, bytes) else len(str(value).encode())
        for value in row
        if value is not None
    )


def position(current, phase):
    while current.load().phase != phase:
        assert not current.retire().done


def test_retirement_resumes_each_bounded_batch_and_keeps_unrelated_rows(tmp_path):
    path = tmp_path / "retirement.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        for table in TABLES:
            fill(db, table, 17)
        assertions(db, 3, 5)
        db.commit()
        work(db).initialize()
        before = contents(db)
    transitions = 0
    while True:
        with closing(connect(path)) as db:
            current = work(db)
            old = contents(db)
            state = current.retire(row_budget=3)
            after = contents(db)
            changed = sum(len(old[t]) - len(after[t]) for t in TABLES)
            assert 0 <= changed <= 3
            for table in ("assertions", "changes", "frames", "journal_prefix"):
                assert after[table] == before[table]
            assert not db.in_transaction
            transitions += 1
            if state.done:
                assert all(after[table] == [] for table in TABLES)
                assert current.retire() == state
                break
    assert transitions == 28


def test_byte_admission_stops_before_the_next_atom_and_capacity_is_explicit(tmp_path):
    path = tmp_path / "bytes.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        fill(db, "maps", 5, payload=1001)
        db.commit()
        current = work(db)
        current.initialize()
        rows = db.execute("SELECT * FROM maps ORDER BY rowid").fetchall()
        budget = sum(logical_octets(row) for row in rows[:2])
        current.retire(byte_budget=budget)
        assert db.execute("SELECT count(*) FROM maps").fetchone()[0] == 3
        before = contents(db)
        with pytest.raises(CorrectionCapacity):
            current.retire(byte_budget=logical_octets(rows[2]) - 1)
        assert contents(db) == before and not db.in_transaction
        current.retire(byte_budget=logical_octets(rows[2]))
        assert db.execute("SELECT count(*) FROM maps").fetchone()[0] == 2


def test_seeding_resumes_its_exact_active_keyset_and_preserves_payloads(tmp_path):
    path = tmp_path / "seeding.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        active, retired = assertions(db, 17, 4097)
        initial = sorted(active)[:2] + sorted(retired)[:2]
        db.executemany("INSERT INTO changes VALUES (?)", ((key,) for key in initial))
        db.commit()
        work(db, "seed").initialize()
        payloads = contents(db)["assertions"]
    previous = set(initial)
    while True:
        with closing(connect(path)) as db:
            current = work(db, "seed")
            state = current.seed(row_budget=3, byte_budget=90)
            keys = {key for (key,) in db.execute("SELECT assertion_id FROM changes")}
            assert len(keys - previous) <= 2
            assert contents(db)["assertions"] == payloads
            assert not db.in_transaction
            previous = keys
            if state.done:
                assert keys == active | set(initial)
                break


@pytest.mark.parametrize(
    "options",
    [
        {"row_budget": 0},
        {"row_budget": 65},
        {"row_budget": True},
        {"row_budget": 1.0},
        {"byte_budget": 0},
        {"byte_budget": -1},
        {"byte_budget": True},
        {"byte_budget": None},
    ],
)
def test_invalid_admission_performs_no_sql(tmp_path, options):
    path = tmp_path / "invalid.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db)
        current.initialize()
        statements = []
        db.set_trace_callback(statements.append)
        try:
            with pytest.raises(ValueError):
                current.retire(**options)
        finally:
            db.set_trace_callback(None)
        assert statements == []


@pytest.mark.parametrize("operation", ["retire", "seed"])
def test_two_connections_publish_only_one_prepared_batch(tmp_path, operation):
    path = tmp_path / "competing.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        fill(db, "maps", 3)
        assertions(db, 3, 0)
        db.commit()
        work(db, operation).initialize()
    with closing(connect(path)) as first, closing(connect(path)) as second:
        workers = [work(first, operation), work(second, operation)]
        prepared = [w.prepare(row_budget=1) for w in workers]

        def publish(index):
            try:
                return workers[index].commit(prepared[index])
            except StaleCorrectionWork:
                return "stale"

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(publish, range(2)))
        assert outcomes.count("stale") == 1
        assert workers[0].load() == workers[1].load()
        table = "maps" if operation == "retire" else "changes"
        assert first.execute("SELECT count(*) FROM " + table).fetchone()[0] == (
            2 if operation == "retire" else 1
        )
        assert not first.in_transaction and not second.in_transaction


def test_replaced_attempt_fences_a_prepared_old_worker_without_reset(tmp_path):
    path = tmp_path / "replacement.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        fill(db, "maps", 3)
        db.commit()
        old = work(db)
        old.initialize()
        prepared = old.prepare(row_budget=1)
        db.execute("UPDATE journal_correction_work SET attempt_id = 'replacement'")
        db.commit()
        before = contents(db)
        with pytest.raises(StaleCorrectionWork):
            old.commit(prepared)
        assert contents(db) == before
        replacement = work(db, attempt_id="replacement")
        replacement.retire(row_budget=1)
        assert db.execute("SELECT count(*) FROM maps").fetchone()[0] == 2


def test_late_sql_failure_rolls_back_rows_and_exact_checkpoint(tmp_path):
    path = tmp_path / "rollback.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        fill(db, "maps", 3)
        db.commit()
        current = work(db)
        current.initialize()
        proposal = current.prepare(row_budget=2)
        db.execute(
            "CREATE TRIGGER late_failure BEFORE DELETE ON maps "
            "WHEN old.rowid = 2 BEGIN SELECT RAISE(ABORT, 'late failure'); END"
        )
        db.commit()
        before = contents(db)
        with pytest.raises(sqlite3.IntegrityError, match="late failure"):
            current.commit(proposal)
        assert contents(db) == before and not db.in_transaction


@pytest.mark.parametrize(
    "fault",
    [
        "source",
        "attempt",
        "algorithm",
        "missing",
        "shape",
        "phase",
        "version",
        "cursor",
        "noncanonical",
        "oversized",
    ],
)
def test_changed_or_corrupt_checkpoints_fail_without_reset(tmp_path, fault):
    path = tmp_path / "corrupt.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db)
        current.initialize()
        state = current.load()
        if fault in {"source", "attempt", "algorithm"}:
            column = {"source": "binding", "attempt": "attempt_id", "algorithm": "algorithm"}[fault]
            db.execute("UPDATE journal_correction_work SET " + column + " = 'changed'")
        elif fault == "missing":
            db.execute("DELETE FROM journal_correction_work")
        else:
            value = json.loads(state.to_json())
            if fault == "shape":
                value["unknown"] = []
            elif fault == "phase":
                value["phase"] = "other"
            elif fault == "version":
                value["version"] = "00"
            elif fault == "cursor":
                value["after"] = identifier(1)
            raw = canonical_json_bytes(value).decode()
            if fault == "noncanonical":
                raw = " " + raw
            elif fault == "oversized":
                raw = " " * 2049
            db.execute("UPDATE journal_correction_work SET checkpoint = ?", (raw,))
        db.commit()
        before = contents(db)
        with pytest.raises(RuntimeError):
            current.load()
        assert contents(db) == before and not db.in_transaction


def test_owner_transactions_are_not_hidden_in_a_bounded_unit_and_versions_keep_width(tmp_path):
    path = tmp_path / "owner.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db)
        db.execute("INSERT INTO changes VALUES (?)", (identifier(1),))
        with pytest.raises(RuntimeError, match="closed owner transaction"):
            current.initialize()
        db.rollback()
        current.initialize()
        state = CorrectionState("maps", version=2**200)
        db.execute("UPDATE journal_correction_work SET checkpoint = ?", (state.to_json(),))
        db.commit()
        assert current.retire().version == 2**200 + 1
        with pytest.raises(StaleCorrectionWork):
            current.initialize()


_CRASH = r"""
import os, sqlite3, sys
from riverhog_provenance._correction_projection import CorrectionWork
path, operation, point = sys.argv[1:]
class CrashConnection(sqlite3.Connection):
    def execute(self, sql, parameters=()):
        result = super().execute(sql, parameters)
        if point == "header" and sql.startswith("UPDATE journal_correction_work"):
            os._exit(73)
        if point == "mutation" and sql.startswith(
            ("DELETE FROM ", "INSERT OR IGNORE INTO changes")
        ):
            os._exit(73)
        return result
    def commit(self):
        if point == "before-commit":
            os._exit(73)
        super().commit()
        if point == "after-commit":
            os._exit(73)
db = sqlite3.connect(path, factory=CrashConnection)
db.execute("PRAGMA synchronous = FULL").close()
current = CorrectionWork(
    db, operation=operation, attempt_id="owned-correction-attempt",
    source_binding="exact-owned-entry",
)
batch = current.prepare(row_budget=1)
if point == "prepared":
    os._exit(73)
current.commit(batch)
raise AssertionError("crash seam was not reached")
"""


@pytest.mark.parametrize("phase", [*TABLES, "seed"])
@pytest.mark.parametrize(
    "point", ["prepared", "header", "mutation", "before-commit", "after-commit"]
)
def test_real_process_exit_keeps_projection_and_progress_atomic(tmp_path, phase, point):
    path = tmp_path / "process.sqlite3"
    create(path)
    operation = "seed" if phase == "seed" else "retire"
    with closing(connect(path)) as db:
        if operation == "retire":
            fill(db, phase, 3)
        else:
            active, _ = assertions(db, 3, 17)
        db.commit()
        current = work(db, operation)
        current.initialize()
        if operation == "retire":
            position(current, phase)
        before = contents(db)
        prepared = current.prepare(row_budget=1)
    outcome = subprocess.run(
        [sys.executable, "-c", _CRASH, str(path), operation, point],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert outcome.returncode == 73, outcome.stderr
    with closing(connect(path)) as db:
        current = work(db, operation)
        if point == "after-commit":
            assert current.load() == prepared.after
            if operation == "retire":
                assert db.execute("SELECT count(*) FROM " + phase).fetchone()[0] == 2
            else:
                assert db.execute("SELECT count(*) FROM changes").fetchone()[0] == 1
        else:
            assert contents(db) == before
        advance = current.seed if operation == "seed" else current.retire
        while not advance(row_budget=1).done:
            pass
        if operation == "retire":
            assert all(
                db.execute("SELECT count(*) FROM " + name).fetchone()[0] == 0 for name in TABLES
            )
        else:
            assert {key for (key,) in db.execute("SELECT assertion_id FROM changes")} == active
        assert contents(db)["frames"] == before["frames"]
        assert contents(db)["journal_prefix"] == before["journal_prefix"]


def test_native_correction_uses_private_bounded_phases_before_prefix_publication(
    journal, who, catalog, monkeypatch
):
    with validate_journal(journal, catalog=catalog) as summary:
        original = dict(summary.graph["agents"][0])
        target = assertion_reference(summary, original["assertion_id"])
    replacement = copy.deepcopy(original)
    replacement["assertion_id"] = new_id()
    replacement["name"] = "Changed label"
    observed = []
    native = CorrectionWork._apply

    def apply(self, batch):
        assert self.db.in_transaction
        before = self.db.execute("SELECT checkpoint FROM journal_prefix").fetchone()[0]
        changes = self.db.total_changes
        state = native(self, batch)
        assert self.db.total_changes - changes <= 65
        assert self.db.execute("SELECT checkpoint FROM journal_prefix").fetchone()[0] == before
        assert self.db.in_transaction
        observed.append((self.operation, state.phase))
        return state

    monkeypatch.setattr(CorrectionWork, "_apply", apply)
    raw = append_correction(
        journal,
        [target],
        reason="label",
        recorded_by_agent_id=who,
        assertions={"agents": [replacement]},
        catalog=catalog,
    )
    with validate_journal(raw, catalog=catalog) as summary:
        assert dict(summary.graph["agents"][0]) == replacement
        assert summary.retracted_assertion_ids == {original["assertion_id"]}
        assert [frame.encoded for frame in summary.frames] == [
            f.encoded for f in parse_journal(raw)
        ]
        assert summary.journal_sha256 == hashlib.sha256(raw).hexdigest()
    assert ("retire", "done") in observed and ("seed", "done") in observed


class WorkCursor(sqlite3.Cursor):
    closed = False

    def record(self, row):
        if row is None:
            return
        meter = self.connection
        meter.metrics["returned_rows"] += 1
        meter.metrics["metadata_octets"] += logical_octets(row)
        meter.metrics["payload_fetched_octets"] += sum(
            len(value) for value in row if isinstance(value, bytes)
        )
        if self.sql.startswith("SELECT rowid,"):
            key, size = row
            meter.weights[key] = size
            meter.metrics["inspected_octets"] += size
        elif self.sql.startswith("SELECT assertion_id FROM assertions"):
            meter.metrics["inspected_octets"] += len(row[0].encode())

    def fetchone(self):
        row = super().fetchone()
        self.record(row)
        return row

    def fetchall(self):
        rows = super().fetchall()
        for row in rows:
            self.record(row)
        return rows

    def close(self):
        self.closed = True
        return super().close()


class WorkConnection(sqlite3.Connection):
    recording = False

    def execute(self, sql, parameters=()):
        if not self.recording:
            return super().execute(sql, parameters)
        cursor = self.cursor(factory=WorkCursor)
        cursor.sql = sql
        self.statements.append((sql, parameters, cursor))
        result = cursor.execute(sql, parameters)
        if sql.startswith("DELETE FROM "):
            assert cursor.rowcount == 1
            self.metrics["admitted_octets"] += self.weights[parameters[0]]
        elif sql.startswith("INSERT OR IGNORE INTO changes"):
            self.metrics["admitted_octets"] += len(parameters[0].encode())
        return result

    @contextmanager
    def observe(self, *, weights=None):
        was_in_transaction = self.in_transaction
        self.metrics = {
            name: 0
            for name in (
                "sql_statements",
                "returned_rows",
                "metadata_octets",
                "changed_rows",
                "admitted_octets",
                "inspected_octets",
                "payload_fetched_octets",
                "sqlite_vm_instructions",
            )
        }
        self.statements, self.weights = [], dict(weights or {})
        before = self.total_changes

        def trace(_sql):
            self.metrics["sql_statements"] += 1

        def tick():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def authorize(action, table, column, _database, _trigger):
            if action == sqlite3.SQLITE_READ and (
                table in {"frames", "journal_prefix"} or table == "assertions" and column == "value"
            ):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.set_trace_callback(trace)
        self.set_progress_handler(tick, 1)
        self.set_authorizer(authorize)
        self.recording = True
        try:
            yield self.metrics
        finally:
            self.recording = False
            self.set_trace_callback(None)
            self.set_progress_handler(None, 0)
            self.set_authorizer(None)
            self.metrics["changed_rows"] = self.total_changes - before
        assert self.in_transaction == was_in_transaction
        assert all(cursor.closed for _sql, _parameters, cursor in self.statements)
        for sql, parameters, _cursor in self.statements:
            if not sql.startswith(("SELECT", "UPDATE", "DELETE")):
                continue
            with closing(self.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as cursor:
                plan = " ".join(row[3] for row in cursor.fetchall())
            assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan


def execution_sample(binding, projection, dimensions, metrics):
    declaration = projection["declarations"][binding.declaration.id]
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(canonical_json_bytes(declaration)).hexdigest(),
        "dimensions": dimensions,
        "metrics": {bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds},
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


def retirement_unit_samples(tmp_path, operation):
    binding = growth_binding(getattr(CorrectionWork, operation))
    projection = project_bindings((binding,))
    for table in TABLES:
        for count, quantum in ((0, 1), (1, 1), (17, 16), (129, 64), (1025, 64)):
            path = tmp_path / f"retire-{table}-{count}.sqlite3"
            create(path)
            with closing(connect(path)) as db:
                fill(db, table, count, payload=65537 if count == 1025 else 1)
                db.commit()
                current = work(db)
                current.initialize()
                position(current, table)
                with closing(db.execute("SELECT * FROM " + table)) as rows:
                    maximum = max((logical_octets(row) for row in rows), default=0)
                amount = 2 if count == 1025 else 3 if count == 17 else quantum
                admission = max(1, maximum * amount)
            # Closing/reopening discards SQLite's pager cache; no OS-cache claim is made.
            with closing(connect(path, factory=WorkConnection)) as db:
                current = work(db)
                before = db.execute("SELECT count(*) FROM " + table).fetchone()[0]
                unchanged = contents(db) if operation != "retire" else None
                if operation == "_apply":
                    with db.observe():
                        batch = current.prepare(row_budget=quantum, byte_budget=admission)
                    weights = dict(db.weights)
                    db.execute("BEGIN IMMEDIATE").close()
                    with db.observe(weights=weights) as metrics:
                        state = current._apply(batch)
                    assert db.in_transaction and state == batch.after
                elif operation == "prepare":
                    with db.observe() as metrics:
                        batch = current.prepare(row_budget=quantum, byte_budget=admission)
                    state = batch.after
                    assert not db.in_transaction
                else:
                    with db.observe() as metrics:
                        state = current.retire(row_budget=quantum, byte_budget=admission)
                after = db.execute("SELECT count(*) FROM " + table).fetchone()[0]
                if operation == "prepare":
                    assert before == after and metrics["changed_rows"] == 0
                    assert len(batch.keys) <= quantum
                else:
                    assert before - after == metrics["changed_rows"] - 1
                    assert before - after <= quantum
                if operation == "_apply":
                    db.rollback()
                if unchanged is not None:
                    assert contents(db) == unchanged
                if count == 0:
                    assert state.phase != table
                assert metrics["payload_fetched_octets"] == 0
                yield execution_sample(
                    binding,
                    projection,
                    {
                        "retained_rows": count,
                        "row_budget": quantum,
                        "byte_budget": admission,
                        "max_atom_octets": maximum,
                    },
                    metrics,
                )


@pytest.mark.growth_witness(
    "riverhog-correction-retirement-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._correction_projection.CorrectionWork.retire",
)
def test_cold_retirement_records_actual_indexed_row_byte_and_vm_work(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples.extend(retirement_unit_samples(tmp_path, "retire"))
    assert len(samples) == 20


def seeding_unit_samples(tmp_path, operation):
    binding = growth_binding(getattr(CorrectionWork, operation))
    projection = project_bindings((binding,))
    for active_count, retired_count in (
        (0, 0),
        (1, 1),
        (17, 17),
        (129, 129),
        (1025, 1025),
        (1, 1025),
        (0, 4097),
        (17, 4097),
    ):
        path = tmp_path / f"seed-{active_count}-{retired_count}.sqlite3"
        create(path)
        with closing(connect(path)) as db:
            active, _retired = assertions(db, active_count, retired_count)
            initial = set(sorted(active)[:3])
            db.executemany("INSERT INTO changes VALUES (?)", ((key,) for key in initial))
            db.commit()
            current = work(db, "seed")
            current.initialize()
            primed = int(active_count > 0 and retired_count >= 17)
            if primed:
                current.seed(row_budget=1, byte_budget=45)
        quantum = 1 if active_count < 2 else 16 if active_count == 17 else 64
        admission = 90 if active_count == 1025 else max(1, quantum * 45)
        with closing(connect(path, factory=WorkConnection)) as db:
            current = work(db, "seed")
            unchanged = contents(db) if operation != "seed" else None
            if operation == "_apply":
                batch = current.prepare(row_budget=quantum, byte_budget=admission)
                db.execute("BEGIN IMMEDIATE").close()
                with db.observe() as metrics:
                    state = current._apply(batch)
                assert db.in_transaction and state == batch.after
            elif operation == "prepare":
                with db.observe() as metrics:
                    batch = current.prepare(row_budget=quantum, byte_budget=admission)
                state = batch.after
                assert not db.in_transaction
            else:
                with db.observe() as metrics:
                    state = current.seed(row_budget=quantum, byte_budget=admission)
            inserted = {key for (key,) in db.execute("SELECT assertion_id FROM changes")}
            admitted = set(sorted(active)[primed : primed + min(quantum, max(1, admission // 45))])
            if operation == "prepare":
                assert inserted == initial and metrics["changed_rows"] == 0
                assert set(batch.keys) == admitted
            else:
                assert inserted == initial | admitted
                assert metrics["changed_rows"] == 1 + len(admitted - initial)
            if operation == "_apply":
                db.rollback()
            if unchanged is not None:
                assert contents(db) == unchanged
            if not active:
                assert state.done
            assert metrics["payload_fetched_octets"] == 0
            yield execution_sample(
                binding,
                projection,
                {
                    "retained_rows": active_count + retired_count + len(initial),
                    "row_budget": quantum,
                    "byte_budget": admission,
                    "max_atom_octets": 45,
                },
                metrics,
            )


@pytest.mark.growth_witness(
    "riverhog-correction-seeding-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._correction_projection.CorrectionWork.seed",
)
def test_cold_seeding_skips_retired_payloads_and_records_actual_work(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples.extend(seeding_unit_samples(tmp_path, "seed"))
    assert len(samples) == 8


@pytest.mark.parametrize(
    "fault",
    [
        "missing-directory-link",
        "missing-assertion-link",
        "retired-cursor-link",
        "skipped-retirement",
        "premature-tail",
        "premature-empty-tail",
    ],
)
def test_lost_manifest_links_fail_closed_without_rebuilding_progress(tmp_path, fault):
    path = tmp_path / "links.sqlite3"
    create(path)
    operation = "retire" if fault == "skipped-retirement" else "seed"
    with closing(connect(path)) as db:
        fill(db, "maps", 3)
        assertions(db, 3, 0)
        db.commit()
        current = work(db, operation)
        state = current.initialize()
        if operation == "retire":
            broken = CorrectionState("findings", version=state.version + 1)
            db.execute("UPDATE journal_correction_work SET checkpoint = ?", (broken.to_json(),))
        else:
            if fault != "premature-empty-tail":
                state = current.seed(row_budget=1)
            if fault == "missing-directory-link":
                db.execute("DELETE FROM changes WHERE assertion_id = ?", (state.after,))
            elif fault == "missing-assertion-link":
                db.execute("DELETE FROM assertions WHERE assertion_id = ?", (state.after,))
            elif fault == "retired-cursor-link":
                db.execute(
                    "UPDATE assertions SET active = 0 WHERE assertion_id = ?", (state.after,)
                )
            else:
                broken = CorrectionState("done", state.after, state.version + 1)
                db.execute("UPDATE journal_correction_work SET checkpoint = ?", (broken.to_json(),))
        db.commit()
        before = contents(db)
        with pytest.raises(RuntimeError, match="correction"):
            current.load()
        assert contents(db) == before and not db.in_transaction


def test_retirement_uses_sqlites_actual_physical_rowid_domain(tmp_path):
    path = tmp_path / "physical.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        fill(db, "maps", 2)
        db.execute("UPDATE maps SET rowid = ? WHERE rowid = 1", (-(2**63),))
        db.execute("UPDATE maps SET rowid = ? WHERE rowid = 2", (2**63 - 1,))
        db.commit()
        current = work(db)
        current.initialize()
        current.retire(row_budget=2)
        assert db.execute("SELECT count(*) FROM maps").fetchone()[0] == 0
        assert current.retire().phase == "findings"


@pytest.mark.growth_witness(
    "riverhog-correction-projection-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._correction_projection.CorrectionWork.prepare",
)
def test_actual_detached_correction_projection_plan(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    for action, helper in (("retire", retirement_unit_samples), ("seed", seeding_unit_samples)):
        directory = tmp_path / action
        directory.mkdir()
        samples.extend(helper(directory, "prepare"))
    assert len(samples) == 28


@pytest.mark.growth_witness(
    "riverhog-correction-projection-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._correction_projection.CorrectionWork._apply",
)
def test_actual_owner_transaction_correction_projection_application(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    for action, helper in (("retire", retirement_unit_samples), ("seed", seeding_unit_samples)):
        directory = tmp_path / action
        directory.mkdir()
        samples.extend(helper(directory, "_apply"))
    assert len(samples) == 28
