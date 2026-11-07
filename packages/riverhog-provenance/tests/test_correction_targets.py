from __future__ import annotations

import copy
import hashlib
import sqlite3
import subprocess
import sys
from contextlib import closing, contextmanager
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import _correction_targets as implementation
from riverhog_provenance import (
    append_correction,
    assertion_reference,
    journal_store,
    new_id,
    parse_journal,
    validate_journal,
)
from riverhog_provenance._correction_targets import (
    CorrectionTargetCapacity,
    CorrectionTargets,
    StaleCorrectionTargets,
    TargetState,
)
from riverhog_provenance._journal_prefix import PrefixState
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.journal import JournalFrame
from sha256_continuation import CheckpointSHA256


def identity(ordinal):
    return f"urn:uuid:00000000-0000-4000-8000-{ordinal:012x}"


def connect(path, **options):
    db = sqlite3.connect(path, check_same_thread=False, **options)
    db.execute("PRAGMA synchronous = FULL").close()
    db.execute("PRAGMA cache_size = -64").close()
    db.execute("PRAGMA temp_store = FILE").close()
    return db


def create(path, count=3, history=0, payload=1):
    store = journal_store.JournalStore()
    try:
        with closing(connect(path)) as db:
            store.db.backup(db)
    finally:
        store.close()
    prior = JournalFrame(canonical_json_bytes({"id": identity(1), "sequence": "0"}))
    prefix = PrefixState(
        1,
        len(prior.encoded),
        identity(2),
        canonical_json_bytes(prior.reference).decode(),
        CheckpointSHA256(prior.encoded).export_state(),
        False,
    )
    with closing(connect(path)) as db:
        db.execute(
            "INSERT INTO frames VALUES (?, ?, ?, ?)",
            ("0", prior.json_bytes, canonical_json_bytes(prior.reference), prefix.sha256_state),
        )
        db.execute(
            "INSERT INTO journal_prefix VALUES (1, ?, ?, ?)",
            ("owned-prefix-attempt", b"owned-source", prefix.to_json()),
        )
        db.executemany(
            "INSERT INTO assertions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    identity(100 + i),
                    identity(100000 + i),
                    "agents",
                    "agent",
                    None,
                    None,
                    "0",
                    b"x" * payload,
                    int(i < count),
                )
                for i in range(count + history)
            ),
        )
        db.commit()
    return JournalFrame(
        canonical_json_bytes(
            {
                "id": identity(3),
                "journal_id": identity(2),
                "sequence": "1",
                "entry_kind": "correction",
                "body": {
                    "retracts": [
                        {"assertion_id": identity(100 + i), "entry": prior.reference}
                        for i in range(count)
                    ]
                },
            }
        )
    )


def work(db, frame, attempt="owned-target-attempt"):
    return CorrectionTargets(db, attempt_id=attempt, frame=frame)


def contents(db):
    return {
        name: db.execute("SELECT * FROM " + name + " ORDER BY rowid").fetchall()
        for name in (
            "assertions",
            "frames",
            "journal_prefix",
            "journal_correction_targets",
            "correction_targets",
        )
    }


def position(current, phase):
    while current.load().phase != phase:
        assert not current.advance().done


def changed_frame(frame, targets):
    document = frame.document
    document["body"]["retracts"] = targets
    return JournalFrame(canonical_json_bytes(document))


def test_verifies_all_targets_then_resumes_retirement_and_cleanup(tmp_path):
    path = tmp_path / "resume.sqlite3"
    frame = create(path, count=17, history=31)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        original = contents(db)
    phases = set()
    while True:
        with closing(connect(path)) as db:
            current = work(db, frame)
            before = current.load()
            phases.add(before.phase)
            if before.phase == "verify":
                assert (
                    db.execute("SELECT count(*) FROM assertions WHERE active = 1").fetchone()[0]
                    == 17
                )
            state = current.advance(row_budget=3)
            assert not db.in_transaction
            assert (
                db.execute("SELECT * FROM journal_prefix").fetchall() == original["journal_prefix"]
            )
            assert db.execute("SELECT * FROM frames").fetchall() == original["frames"]
            if state.done:
                assert (
                    db.execute("SELECT count(*) FROM assertions WHERE active = 1").fetchone()[0]
                    == 0
                )
                assert db.execute("SELECT count(*) FROM correction_targets").fetchone()[0] == 0
                assert current.advance() == state
                break
    assert phases == {"verify", "apply", "clean"}


@pytest.mark.parametrize("failure", ["missing", "wrong-entry", "retired", "duplicate"])
def test_target_error_keeps_prior_committed_verification_and_never_retires(tmp_path, failure):
    path = tmp_path / "invalid.sqlite3"
    frame = create(path)
    targets = frame.document["body"]["retracts"]
    target = targets[1]
    if failure == "missing":
        target["assertion_id"] = identity(9000)
    elif failure == "wrong-entry":
        target["entry"]["json_sha256"] = "f" * 64
    elif failure == "duplicate":
        targets[1] = copy.deepcopy(targets[0])
        targets[1]["entry"]["json_sha256"] = "f" * 64
    frame = changed_frame(frame, targets)
    with closing(connect(path)) as db:
        if failure == "retired":
            db.execute(
                "UPDATE assertions SET active = 0 WHERE assertion_id = ?", (target["assertion_id"],)
            )
            db.commit()
        current = work(db, frame)
        current.initialize()
        current.advance(row_budget=1)
        before = contents(db)
        message = {
            "missing": "no exact earlier assertion",
            "wrong-entry": "no exact earlier assertion",
            "retired": "already retired assertion",
            "duplicate": "duplicate correction target",
        }[failure]
        with pytest.raises(ProvenanceValidationError, match=message):
            current.advance(row_budget=1)
        assert contents(db) == before


def test_first_native_error_precedes_later_duplicate_and_liveness_errors(tmp_path):
    path = tmp_path / "order.sqlite3"
    frame = create(path)
    targets = frame.document["body"]["retracts"]
    targets[0]["entry"]["json_sha256"] = "f" * 64
    targets[1] = copy.deepcopy(targets[0])
    with closing(connect(path)) as db:
        current = work(db, changed_frame(frame, targets))
        current.initialize()
        before = contents(db)
        with pytest.raises(ProvenanceValidationError, match="no exact earlier assertion"):
            current.advance()
        assert contents(db) == before


@pytest.mark.parametrize("phase", ["verify", "apply", "clean"])
def test_capacity_preserves_progress_and_charges_large_assertion_atoms(tmp_path, phase):
    path = tmp_path / "capacity.sqlite3"
    frame = create(path, payload=65537)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        # Explicit admission is necessary to retire the existing large row atom.
        while current.load().phase != phase:
            current.advance(byte_budget=1000000)
        before = contents(db)
        with pytest.raises(CorrectionTargetCapacity):
            current.advance(byte_budget=1)
        assert contents(db) == before
        if phase == "apply":
            with pytest.raises(CorrectionTargetCapacity):
                current.advance(byte_budget=65536)
            assert contents(db) == before
        assert current.advance(row_budget=1, byte_budget=1000000).position == 1


@pytest.mark.parametrize(
    "option,value",
    [
        ("row_budget", 0),
        ("row_budget", 65),
        ("row_budget", True),
        ("byte_budget", 0),
        ("byte_budget", True),
    ],
)
def test_invalid_admission_performs_no_sql(tmp_path, option, value):
    path = tmp_path / "admission.sqlite3"
    frame = create(path)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        statements = []
        db.set_trace_callback(statements.append)
        with pytest.raises(ValueError):
            current.advance(**{option: value})
        db.set_trace_callback(None)
        assert statements == []


@pytest.mark.parametrize("phase", ["verify", "apply", "clean"])
def test_competing_connections_cannot_publish_a_stale_page(tmp_path, phase):
    path = tmp_path / "competing.sqlite3"
    frame = create(path)
    with closing(connect(path)) as first, closing(connect(path)) as second:
        owner = work(first, frame)
        owner.initialize()
        position(owner, phase)
        stale = work(second, frame)
        proposal = stale.prepare(row_budget=2)
        owner.advance(row_budget=1)
        expected = contents(first)
        with pytest.raises(StaleCorrectionTargets):
            stale.commit(proposal)
        assert contents(first) == expected


def test_source_attempt_and_accepted_prefix_fences_fail_closed(tmp_path):
    path = tmp_path / "fences.sqlite3"
    frame = create(path)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        proposal = current.prepare(row_budget=1)
        other = frame.document
        other["id"] = identity(50)
        with pytest.raises(StaleCorrectionTargets):
            work(db, JournalFrame(canonical_json_bytes(other))).load()
        with pytest.raises(StaleCorrectionTargets):
            work(db, frame, "other-attempt").load()
        db.execute("UPDATE journal_correction_targets SET attempt_id = 'replacement'")
        db.commit()
        expected = contents(db)
        with pytest.raises(StaleCorrectionTargets):
            current.commit(proposal)
        assert contents(db) == expected
        db.execute("UPDATE journal_correction_targets SET attempt_id = ?", (current.attempt_id,))
        prefix = PrefixState.from_json(
            db.execute("SELECT checkpoint FROM journal_prefix").fetchone()[0]
        )
        db.execute(
            "UPDATE journal_prefix SET checkpoint = ?",
            (replace(prefix, matched_anchor=True).to_json(),),
        )
        db.commit()
        expected = contents(db)
        with pytest.raises(StaleCorrectionTargets):
            current.commit(proposal)
        with pytest.raises(StaleCorrectionTargets):
            current.load()
        assert contents(db) == expected


@pytest.mark.parametrize(
    "damage",
    [
        "missing-header",
        "algorithm",
        "checkpoint",
        "count",
        "missing-first",
        "changed-tail",
        "reference",
    ],
)
def test_missing_corrupt_or_unlinked_storage_is_never_reset(tmp_path, damage):
    path = tmp_path / "corrupt.sqlite3"
    frame = create(path)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        current.advance(row_budget=2)
        if damage == "missing-header":
            db.execute("DELETE FROM journal_correction_targets")
        elif damage == "algorithm":
            db.execute("UPDATE journal_correction_targets SET algorithm = 'unknown'")
        elif damage == "checkpoint":
            db.execute("UPDATE journal_correction_targets SET checkpoint = '{}' ")
        elif damage == "count":
            db.execute("UPDATE journal_correction_targets SET target_count = target_count + 1")
        elif damage == "missing-first":
            db.execute("DELETE FROM correction_targets WHERE position = 0")
        elif damage == "changed-tail":
            db.execute("UPDATE correction_targets SET sequence = '7' WHERE position = 1")
        else:
            db.execute("UPDATE frames SET reference = ?", (b"{}",))
        db.commit()
        expected = contents(db)
        with pytest.raises(RuntimeError):
            current.advance(row_budget=1)
        assert contents(db) == expected


@pytest.mark.parametrize("phase", ["verify", "apply", "clean"])
def test_late_statement_failure_rolls_back_rows_and_checkpoint(tmp_path, phase):
    path = tmp_path / "rollback.sqlite3"
    frame = create(path)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        position(current, phase)
        if phase == "verify":
            trigger = "BEFORE INSERT ON correction_targets WHEN new.position = 1"
        elif phase == "apply":
            trigger = f"BEFORE UPDATE ON assertions WHEN new.assertion_id = '{identity(101)}'"
        else:
            trigger = "BEFORE DELETE ON correction_targets WHEN old.position = 1"
        db.execute(
            "CREATE TRIGGER late_failure "
            + trigger
            + " BEGIN SELECT RAISE(ABORT, 'injected late failure'); END"
        )
        db.commit()
        expected = contents(db)
        with pytest.raises(sqlite3.IntegrityError, match="injected late failure"):
            current.advance(row_budget=2)
        assert contents(db) == expected


def test_explicit_initialization_and_large_work_version(tmp_path):
    path = tmp_path / "version.sqlite3"
    frame = create(path)
    with closing(connect(path)) as db:
        current = work(db, frame)
        with pytest.raises(StaleCorrectionTargets):
            current.load()
        db.execute("BEGIN")
        with pytest.raises(RuntimeError, match="closed owner transaction"):
            current.initialize()
        db.rollback()
        current.initialize()
        with pytest.raises(StaleCorrectionTargets):
            current.initialize()
        state = TargetState(version=2**200)
        db.execute("UPDATE journal_correction_targets SET checkpoint = ?", (state.to_json(),))
        db.commit()
        assert current.advance(row_budget=1).version == 2**200 + 1


_CRASH = r"""
import os, sqlite3, sys
from pathlib import Path
from riverhog_provenance._correction_targets import CorrectionTargets
from riverhog_provenance.journal import JournalFrame
path, source, point = sys.argv[1:]
class CrashConnection(sqlite3.Connection):
    def execute(self, sql, parameters=()):
        cursor = super().execute(sql, parameters)
        if point == "header" and sql.startswith("UPDATE journal_correction_targets"):
            os._exit(73)
        if point == "mutation" and sql.startswith((
            "INSERT INTO correction_targets",
            "UPDATE assertions SET active",
            "DELETE FROM correction_targets",
        )):
            os._exit(73)
        return cursor
    def commit(self):
        if point == "before-commit":
            os._exit(73)
        super().commit()
        if point == "after-commit":
            os._exit(73)
db = sqlite3.connect(path, factory=CrashConnection)
db.execute("PRAGMA synchronous = FULL").close()
current = CorrectionTargets(
    db, attempt_id="owned-target-attempt",
    frame=JournalFrame(Path(source).read_bytes()),
)
batch = current.prepare(row_budget=1)
if point == "prepared":
    os._exit(73)
current.commit(batch)
raise AssertionError("crash seam was not reached")
"""


@pytest.mark.parametrize("phase", ["verify", "apply", "clean"])
@pytest.mark.parametrize(
    "point", ["prepared", "header", "mutation", "before-commit", "after-commit"]
)
def test_real_process_exit_keeps_targets_retirement_and_progress_atomic(tmp_path, phase, point):
    path, source = tmp_path / "process.sqlite3", tmp_path / "entry.json"
    frame = create(path)
    source.write_bytes(frame.json_bytes)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        position(current, phase)
        expected = contents(db)
        proposed = current.prepare(row_budget=1)
    outcome = subprocess.run(
        [sys.executable, "-c", _CRASH, str(path), str(source), point],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert outcome.returncode == 73, outcome.stderr
    with closing(connect(path)) as db:
        current = work(db, frame)
        if point == "after-commit":
            assert current.load() == proposed.after
        else:
            assert contents(db) == expected
        while not current.advance(row_budget=1).done:
            pass
        assert db.execute("SELECT count(*) FROM assertions WHERE active = 1").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM correction_targets").fetchone()[0] == 0
        assert db.execute("SELECT * FROM frames").fetchall() == expected["frames"]
        assert db.execute("SELECT * FROM journal_prefix").fetchall() == expected["journal_prefix"]


def test_native_correction_uses_verified_targets_before_prefix_publication(
    journal, who, catalog, monkeypatch
):
    with validate_journal(journal, catalog=catalog) as summary:
        original = dict(summary.graph["agents"][0])
        target = assertion_reference(summary, original["assertion_id"])
    replacement = copy.deepcopy(original)
    replacement["assertion_id"], replacement["name"] = new_id(), "Changed label"
    observed = []
    native = CorrectionTargets._apply

    def apply(self, batch):
        assert self.db.in_transaction
        prefix = self.db.execute("SELECT checkpoint FROM journal_prefix").fetchone()[0]
        before = self.db.total_changes
        state = native(self, batch)
        assert self.db.total_changes - before <= 65
        assert self.db.execute("SELECT checkpoint FROM journal_prefix").fetchone()[0] == prefix
        assert self.db.in_transaction
        observed.append(state.phase)
        return state

    monkeypatch.setattr(CorrectionTargets, "_apply", apply)
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
        assert [f.encoded for f in summary.frames] == [f.encoded for f in parse_journal(raw)]
        assert summary.journal_sha256 == hashlib.sha256(raw).hexdigest()
    assert set(observed) == {"verify", "apply", "clean", "done"}


def logical_octets(row):
    return sum(
        len(value) if isinstance(value, bytes) else len(str(value).encode())
        for value in row
        if value is not None
    )


class WorkCursor(sqlite3.Cursor):
    closed = False

    def record(self, row):
        if row is None:
            return
        meter = self.connection
        meter.metrics["returned_rows"] += 1
        meter.metrics["metadata_octets"] += logical_octets(row)
        if "substr(f.reference" not in self.sql:
            meter.metrics["payload_fetched_octets"] += sum(
                len(v) for v in row if isinstance(v, bytes)
            )
        if self.sql.startswith("SELECT t.position") and "a.active, length(" in self.sql:
            octets = logical_octets(row[:3]) + row[5]
            meter.weights[row[1]] = octets
            meter.metrics["inspected_octets"] += octets
        elif self.sql.startswith("SELECT t.position") and "WHERE t.position >= ?" in self.sql:
            octets = logical_octets(row[:3])
            meter.weights[row[1]] = octets
            meter.metrics["inspected_octets"] += octets

    def fetchone(self):
        row = super().fetchone()
        self.record(row)
        return row

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
        if sql.startswith("INSERT INTO correction_targets"):
            self.metrics["admitted_octets"] += self.weights[parameters[1]]
        elif sql.startswith("UPDATE assertions SET active"):
            self.metrics["admitted_octets"] += self.weights[parameters[0]]
        elif sql.startswith("DELETE FROM correction_targets"):
            self.metrics["admitted_octets"] += self.weights[parameters[1]]
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
        original = implementation.canonical_json_bytes

        def canonical(value):
            encoded = original(value)
            if type(value) is dict and set(value) == {"entry", "assertion_id"}:
                self.weights[value["assertion_id"]] = len(encoded)
                self.metrics["inspected_octets"] += len(encoded)
            return encoded

        def trace(_sql):
            self.metrics["sql_statements"] += 1

        def tick():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def authorize(action, table, column, _database, _trigger):
            if action == sqlite3.SQLITE_READ and table == "frames" and column == "value":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        implementation.canonical_json_bytes = canonical
        self.set_trace_callback(trace)
        self.set_progress_handler(tick, 1)
        self.set_authorizer(authorize)
        self.recording = True
        try:
            yield self.metrics
        finally:
            self.recording = False
            implementation.canonical_json_bytes = original
            self.set_trace_callback(None)
            self.set_progress_handler(None, 0)
            self.set_authorizer(None)
            self.metrics["changed_rows"] = self.total_changes - before
        assert self.in_transaction == was_in_transaction
        assert all(cursor.closed for _sql, _parameters, cursor in self.statements)
        for sql, parameters, _cursor in self.statements:
            if not sql.startswith(("SELECT", "UPDATE", "DELETE")):
                continue
            if "a.value" in sql:
                assert "length(a.value)" in sql and "a.value" not in sql.replace(
                    "length(a.value)", ""
                )
            with closing(self.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as cursor:
                plan = " ".join(row[3] for row in cursor.fetchall())
            assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan


def target_unit_samples(tmp_path, operation):
    binding = growth_binding(getattr(CorrectionTargets, operation))
    projection = project_bindings((binding,))
    regimes = [
        (1, 0, 1, True),
        (1, 1, 1, False),
        (17, 17, 16, False),
        (129, 129, 64, False),
        (1025, 1025, 64, False),
        (1, 4097, 1, False),
        (17, 4097, 16, False),
    ]
    for phase in ("verify", "apply", "clean"):
        for count, history, quantum, terminal in regimes:
            path = tmp_path / f"{phase}-{count}-{history}-{terminal}.sqlite3"
            payload = 65537 if phase == "apply" and count == 1025 else 1
            frame = create(path, count=count, history=history, payload=payload)
            with closing(connect(path)) as db:
                current = work(db, frame)
                current.initialize()
                while current.load().phase != phase:
                    current.advance(byte_budget=10000000)
                if terminal or count == 17 and history == 4097:
                    current.advance(row_budget=1, byte_budget=10000000)
                if phase == "verify":
                    maximum = max(len(canonical_json_bytes(t)) for t in current.targets)
                else:
                    rows = db.execute(
                        "SELECT position, assertion_id, sequence FROM correction_targets"
                    ).fetchall()
                    maximum = max((logical_octets(row) for row in rows), default=0)
                    if phase == "apply":
                        maximum += max(
                            (logical_octets(row) for row in db.execute("SELECT * FROM assertions")),
                            default=0,
                        )
                retained = sum(
                    db.execute("SELECT count(*) FROM " + table).fetchone()[0]
                    for table in ("assertions", "frames", "correction_targets")
                )
                admission = max(1, maximum * (2 if count == 1025 else quantum))
            # Reopening discards SQLite's pager cache; no OS-cache eviction is claimed.
            with closing(connect(path, factory=WorkConnection)) as db:
                current = work(db, frame)
                before = current.load()
                unchanged = contents(db) if operation != "advance" else None
                if operation == "_apply":
                    with db.observe():
                        batch = current.prepare(row_budget=quantum, byte_budget=admission)
                    weights = dict(db.weights)
                    db.execute("BEGIN IMMEDIATE").close()
                    with db.observe(weights=weights) as metrics:
                        after = current._apply(batch)
                    assert after == batch.after and db.in_transaction
                    db.rollback()
                elif operation == "prepare":
                    with db.observe() as metrics:
                        batch = current.prepare(row_budget=quantum, byte_budget=admission)
                    after = batch.after
                    assert not db.in_transaction
                else:
                    with db.observe() as metrics:
                        after = current.advance(row_budget=quantum, byte_budget=admission)
                if unchanged is not None:
                    assert contents(db) == unchanged
                assert metrics["changed_rows"] <= quantum + 1
                assert metrics["payload_fetched_octets"] == 0
                if terminal:
                    assert before.position == count and after.phase != before.phase
                declaration = projection["declarations"][binding.declaration.id]
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": hashlib.sha256(
                        canonical_json_bytes(declaration)
                    ).hexdigest(),
                    "dimensions": {
                        "retained_rows": retained,
                        "row_budget": quantum,
                        "byte_budget": admission,
                        "max_atom_octets": maximum,
                    },
                    "metrics": {
                        bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds
                    },
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                yield sample


@pytest.mark.growth_witness(
    "riverhog-correction-target-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._correction_targets.CorrectionTargets.advance",
)
def test_cold_indexed_targets_record_actual_rows_bytes_and_vm_work(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples.extend(target_unit_samples(tmp_path, "advance"))
    assert len(samples) == 21


@pytest.mark.growth_witness(
    "riverhog-correction-targets-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._correction_targets.CorrectionTargets.prepare",
)
def test_actual_detached_correction_targets_plan(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples.extend(target_unit_samples(tmp_path, "prepare"))
    assert len(samples) == 21


@pytest.mark.growth_witness(
    "riverhog-correction-targets-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._correction_targets.CorrectionTargets._apply",
)
def test_actual_owner_transaction_correction_targets_application(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    samples.extend(target_unit_samples(tmp_path, "_apply"))
    assert len(samples) == 21


@pytest.mark.parametrize("phase", ["apply", "clean", "done"])
def test_restore_rejects_changed_retirement_or_reclamation_links(tmp_path, phase):
    path = tmp_path / "retired-link.sqlite3"
    frame = create(path)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        if phase == "done":
            while not current.advance().done:
                pass
            ordinal = 102
        else:
            position(current, phase)
            if phase == "apply":
                current.advance(row_budget=1)
            ordinal = 100
        db.execute("UPDATE assertions SET active = 1 WHERE assertion_id = ?", (identity(ordinal),))
        db.commit()
        expected = contents(db)
        with pytest.raises(RuntimeError, match="cursor link"):
            current.load()
        assert contents(db) == expected


def test_missing_middle_target_cannot_advance_any_retirement(tmp_path):
    path = tmp_path / "middle-link.sqlite3"
    frame = create(path)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        position(current, "apply")
        db.execute("DELETE FROM correction_targets WHERE position = 1")
        db.commit()
        expected = contents(db)
        with pytest.raises(RuntimeError, match="missing correction target work row"):
            current.advance()
        assert contents(db) == expected


def test_finished_worker_cannot_report_completion_after_replacement(tmp_path):
    path = tmp_path / "finished-fence.sqlite3"
    frame = create(path)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        while not current.advance().done:
            pass
        stale = current.prepare()
        work(db, frame, "replacement").initialize()
        expected = contents(db)
        with pytest.raises(StaleCorrectionTargets):
            current.commit(stale)
        assert contents(db) == expected
