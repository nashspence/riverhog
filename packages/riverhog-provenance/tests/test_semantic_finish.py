from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from contextlib import closing, contextmanager
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import journal_store, validate_journal
from riverhog_provenance._journal_prefix import PrefixState
from riverhog_provenance._semantic_finish import (
    FinishState,
    SemanticFinish,
    SemanticFinishCapacity,
    StaleSemanticFinish,
)
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.journal import JournalFrame
from sha256_continuation import CheckpointSHA256


def identity(ordinal):
    return f"urn:uuid:00000000-0000-4000-8000-{ordinal:012x}"


def connect(path, **options):
    db = sqlite3.connect(path, **options)
    db.execute("PRAGMA synchronous=FULL").close()
    db.execute("PRAGMA cache_size=-64").close()
    db.execute("PRAGMA temp_store=FILE").close()
    return db


class Store:
    _lifecycle_bounds_hold = journal_store.JournalStore._lifecycle_bounds_hold

    def __init__(self, db):
        self.db = db


def frame():
    return JournalFrame(canonical_json_bytes({"sequence": "1", "journal_id": identity(2)}))


def work(db, **options):
    return SemanticFinish(
        Store(db),
        **{
            "attempt_id": "owned-finish-attempt",
            "frame": frame(),
            "catalog_sha256": "a" * 64,
            "require_profiles": False,
            **options,
        },
    )


def create(path):
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
            "INSERT INTO frames VALUES (?,?,?,?)",
            ("0", prior.json_bytes, canonical_json_bytes(prior.reference), prefix.sha256_state),
        )
        db.execute(
            "INSERT INTO journal_prefix VALUES (1,?,?,?)",
            ("prefix-attempt", b"exact-prefix-source-and-modes", prefix.to_json()),
        )
        db.commit()


def assertion(db, ordinal, kind="agent", *, changed=True, active=True, **fields):
    row = {
        "assertion_id": identity(ordinal),
        "id": identity(1000000 + ordinal),
        "type": kind,
        **fields,
    }
    db.execute(
        "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
        (
            row["assertion_id"],
            row["id"],
            "agents" if kind == "agent" else "relations",
            kind,
            row.get("state", {}).get("object_id"),
            row.get("observation_id"),
            "0",
            canonical_json_bytes(row),
            int(active),
        ),
    )
    if changed:
        db.execute("INSERT INTO changes VALUES (?)", (row["assertion_id"],))
    return row


def external(db, row, ordinal, object_type=None, payload=0):
    value = {
        "scope": "external",
        "journal_id": identity(2000000 + ordinal),
        "object_id": row["id"],
        "object_type": object_type or row["type"],
    }
    if payload:
        value["padding"] = "x" * payload
    encoded = canonical_json_bytes(value)
    db.execute("INSERT INTO maps VALUES ('externals',?,?)", (b"b" + encoded, encoded))
    return value


def lifecycle(db, generation="2020-01-01T00:00:00Z", invalidation="2030-01-01T00:00:00Z"):
    state = identity(9000000)
    for kind, at in (("generation", generation), ("invalidation", invalidation)):
        if at is not None:
            db.execute(
                "INSERT INTO maps VALUES (?,?,?)",
                (kind, b"j" + canonical_json_bytes(state), canonical_json_bytes({"at": at})),
            )
    return {"scope": "local", "object_id": state, "object_type": "state"}


def position(current, phase):
    for _ in range(10000):
        state = current.load()
        if state.phase == phase:
            return state
        assert not current.advance(row_budget=1, byte_budget=1000000).done
    pytest.fail("phase did not progress")


def contents(db):
    return {
        name: db.execute("SELECT * FROM " + name + " ORDER BY rowid").fetchall()
        for name in (
            "assertions",
            "changes",
            "maps",
            "findings",
            "fixities",
            "frames",
            "journal_prefix",
            "journal_semantic_finish",
        )
    }


def test_reopen_each_unit_keeps_exact_prefix_and_visits_each_reference_once(tmp_path):
    path = tmp_path / "resume.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        row = assertion(db, 20)
        for i in range(129):
            external(db, row, i)
        assertion(db, 10, active=False)
        db.commit()
        work(db).initialize()
        original = contents(db)
    scans = []
    for _ in range(1000):
        with closing(connect(path)) as db:
            current = work(db)
            before = current.load()
            state = current.advance(row_budget=3)
            assert not db.in_transaction
            after = contents(db)
            for name in ("assertions", "changes", "maps", "frames", "journal_prefix"):
                assert after[name] == original[name]
            if state.phase == "externals" and state.scan != before.scan:
                scans.append(state.scan)
            if state.done:
                assert current.advance() == state
                break
    else:
        pytest.fail("semantic finish did not complete")
    assert scans == list(range(3, 130, 3))


@pytest.mark.parametrize("phase", ["invalidation", "usage", "externals"])
def test_late_scan_failure_retains_the_preceding_exact_checkpoint(tmp_path, phase):
    path = tmp_path / "late.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        if phase == "externals":
            row = assertion(db, 1)
            for i in range(18):
                external(db, row, i, "activity" if i == 17 else None)
            message = "wrong reference type"
        else:
            reference = lifecycle(db)
            row = assertion(db, 1, "generation", state=reference)
            for i in range(18):
                assertion(
                    db,
                    10 + i,
                    phase,
                    changed=False,
                    state=reference,
                    at=(
                        "2010-01-01T00:00:00Z"
                        if phase == "invalidation"
                        else "2040-01-01T00:00:00Z"
                    )
                    if i == 17
                    else "2025-01-01T00:00:00Z",
                )
            db.execute(
                "INSERT INTO lifecycle_extrema VALUES (?,?,?)",
                (reference["object_id"], "0", "9999999999999999999"),
            )
            message = (
                "before known generation" if phase == "invalidation" else "after known invalidation"
            )
        db.commit()
        current = work(db)
        current.initialize()
        position(current, phase)
        current.advance(row_budget=16)
        before = contents(db)
        for _ in range(2):
            with pytest.raises(ProvenanceValidationError, match=message):
                work(db).advance(row_budget=2)
            assert contents(db) == before
            assert not db.in_transaction


def test_lifecycle_diagnostic_uses_native_type_then_physical_row_order(tmp_path):
    path = tmp_path / "order.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        state = lifecycle(db)
        assertion(db, 1, "generation", state=state)
        assertion(db, 2, "usage", changed=False, state=state, at="2040-01-01T00:00:00Z")
        assertion(db, 3, "invalidation", changed=False, state=state, at="2010-01-01T00:00:00Z")
        db.execute(
            "INSERT INTO lifecycle_extrema VALUES (?,?,?)",
            (state["object_id"], "0", "9999999999999999999"),
        )
        db.commit()
        current = work(db)
        current.initialize()
        with pytest.raises(ProvenanceValidationError, match="before known generation"):
            while not current.advance(row_budget=1).done:
                pass


@pytest.mark.parametrize("first", ["address", "fixity", "external"])
def test_content_and_reference_checks_preserve_native_within_row_order(tmp_path, first):
    path = tmp_path / "content.sqlite3"
    create(path)
    content = {"size_bytes": "4", "digests": [{"algorithm": "sha-256", "value": "b" * 64}]}
    with closing(connect(path)) as db:
        state = identity(9000000)
        row = assertion(
            db,
            1,
            "observation",
            state={"object_id": state},
            content=content,
            address_status="known" if first == "address" else "unknown",
        )
        db.execute("INSERT INTO fixities VALUES (?,?,?)", (state, "4", "a" * 64))
        external(db, row, 1, "activity")
        db.commit()
        current = work(db)
        current.initialize()
        position(current, "content")
        if first == "address":
            before = contents(db)
            with pytest.raises(ProvenanceValidationError, match="known address requires"):
                current.advance()
            assert contents(db) == before
        else:
            current.advance()
            assert len(db.execute("SELECT * FROM fixities").fetchall()) == 2
            assert len(db.execute("SELECT * FROM findings").fetchall()) == 1
            with pytest.raises(ProvenanceValidationError, match="wrong reference type"):
                current.advance()


@pytest.mark.parametrize("field", ["attempt_id", "frame", "catalog_sha256", "require_profiles"])
def test_reopen_binds_exact_inputs(tmp_path, field):
    path = tmp_path / "binding.sqlite3"
    create(path)
    changed = {
        "attempt_id": "replacement",
        "frame": JournalFrame(
            canonical_json_bytes({"sequence": "1", "journal_id": identity(2), "changed": True})
        ),
        "catalog_sha256": "b" * 64,
        "require_profiles": True,
    }
    with closing(connect(path)) as db:
        current = work(db)
        current.initialize()
        before = contents(db)
        with pytest.raises(StaleSemanticFinish):
            work(db, **{field: changed[field]}).advance()
        assert contents(db) == before


@pytest.mark.parametrize("field", ["attempt_id", "binding", "checkpoint"])
def test_accepted_prefix_replacement_fences_prepared_units(tmp_path, field):
    path = tmp_path / "prefix.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        assertion(db, 1)
        db.commit()
        current = work(db)
        current.initialize()
        batch = current.prepare()
        value = (
            "replacement"
            if field == "attempt_id"
            else b"replacement"
            if field == "binding"
            else "replaced-checkpoint"
        )
        db.execute("UPDATE journal_prefix SET " + field + "=?", (value,))
        db.commit()
        before = contents(db)
        with pytest.raises(StaleSemanticFinish):
            current.commit(batch)
        assert contents(db) == before


def test_competing_connections_publish_one_unit_and_fence_finished_workers(tmp_path):
    path = tmp_path / "workers.sqlite3"
    create(path)
    with closing(connect(path)) as owner, closing(connect(path)) as stale:
        assertion(owner, 1)
        owner.commit()
        current = work(owner)
        current.initialize()
        batch = work(stale).prepare()
        current.advance()
        before = contents(owner)
        with pytest.raises(StaleSemanticFinish):
            work(stale).commit(batch)
        assert contents(owner) == before
        while not current.advance().done:
            pass
        finished = current.prepare()
        work(stale, attempt_id="replacement").initialize()
        before = contents(owner)
        for action in (current.load, lambda: current.commit(finished)):
            with pytest.raises(StaleSemanticFinish):
                action()
            assert contents(owner) == before


def test_statement_failure_rolls_back_fixity_finding_and_header(tmp_path):
    path = tmp_path / "rollback.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        state = identity(9000000)
        assertion(
            db,
            1,
            "reported_description",
            state={"object_id": state},
            content={"size_bytes": "4", "digests": [{"algorithm": "sha-256", "value": "b" * 64}]},
        )
        db.execute("INSERT INTO fixities VALUES (?,?,?)", (state, "4", "a" * 64))
        db.commit()
        current = work(db)
        current.initialize()
        position(current, "content")
        db.execute(
            "CREATE TRIGGER fail_finding BEFORE INSERT ON findings "
            "BEGIN SELECT RAISE(ABORT,'late failure'); END"
        )
        db.commit()
        before = contents(db)
        with pytest.raises(sqlite3.IntegrityError, match="late failure"):
            current.advance()
        assert contents(db) == before
        assert not db.in_transaction


@pytest.mark.parametrize("phase", ["content", "externals"])
def test_capacity_suspends_before_payload_fetch_or_progress(tmp_path, phase):
    path = tmp_path / "capacity.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        row = assertion(db, 1, "reported_description", padding="x" * 65537)
        external(db, row, 1, payload=65537)
        db.commit()
        current = work(db)
        current.initialize()
        position(current, phase)
        before = contents(db)
        with pytest.raises(SemanticFinishCapacity):
            current.advance(byte_budget=65536)
        assert contents(db) == before
        current.advance(byte_budget=200000)
        assert contents(db) != before


@pytest.mark.parametrize("phase", ["invalidation", "usage", "externals"])
def test_missing_scan_cursor_link_fails_without_reset(tmp_path, phase):
    path = tmp_path / "cursor.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        if phase == "externals":
            row = assertion(db, 1)
            external(db, row, 1)
        else:
            state = lifecycle(db)
            assertion(db, 1, "generation", state=state)
            assertion(db, 2, phase, changed=False, state=state, at="2025-01-01T00:00:00Z")
            db.execute(
                "INSERT INTO lifecycle_extrema VALUES (?,?,?)", (state["object_id"], "0", None)
            )
        db.commit()
        current = work(db)
        current.initialize()
        position(current, phase)
        state = current.advance(row_budget=1)
        table = "maps" if phase == "externals" else "assertions"
        db.execute("DELETE FROM " + table + " WHERE rowid=?", (state.scan,))
        db.commit()
        before = contents(db)
        with pytest.raises(RuntimeError, match="scan cursor link"):
            current.advance()
        assert contents(db) == before


@pytest.mark.parametrize(
    "damage", ["algorithm", "checkpoint", "completed", "current", "scan", "timestamp"]
)
def test_corrupt_header_fails_closed(tmp_path, damage):
    path = tmp_path / "corrupt.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        assertion(db, 1)
        db.commit()
        current = work(db)
        state = current.initialize()
        if damage == "algorithm":
            db.execute("UPDATE journal_semantic_finish SET algorithm='unknown'")
        else:
            value = json.loads(state.to_json())
            if damage == "checkpoint":
                value["extra"] = True
            else:
                key, fault = {
                    "completed": ("completed", identity(300)),
                    "current": ("current", identity(1)),
                    "scan": ("scan", "-9223372036854775809"),
                    "timestamp": ("generated_at", "2020-01-01T00:00:00Z"),
                }[damage]
                value[key] = fault
            db.execute(
                "UPDATE journal_semantic_finish SET checkpoint=?",
                (canonical_json_bytes(value).decode(),),
            )
        db.commit()
        before = contents(db)
        with pytest.raises(RuntimeError):
            current.advance()
        assert contents(db) == before


def test_work_versions_and_physical_rowids_keep_their_own_domains(tmp_path):
    path = tmp_path / "domains.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        row = assertion(db, 1)
        external(db, row, 1)
        external(db, row, 2)
        db.execute("UPDATE maps SET rowid=? WHERE rowid=1", (-(2**63),))
        db.execute("UPDATE maps SET rowid=? WHERE rowid=2", (2**63 - 1,))
        db.commit()
        current = work(db)
        current.initialize()
        state = position(current, "externals")
        db.execute(
            "UPDATE journal_semantic_finish SET checkpoint=?",
            (replace(state, version=2**200).to_json(),),
        )
        db.commit()
        first = current.advance(row_budget=1)
        assert first.version == 2**200 + 1 and first.scan == -(2**63)
        assert current.advance(row_budget=1).scan == 2**63 - 1
        assert current.advance(row_budget=1).phase == "row"


@pytest.mark.parametrize("budget", [0, 65, True])
def test_row_budget_is_an_explicit_work_quantum(tmp_path, budget):
    path = tmp_path / "budget.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db)
        current.initialize()
        before = contents(db)
        with pytest.raises(ValueError):
            current.advance(row_budget=budget)
        assert contents(db) == before


def test_native_wrapper_runs_the_private_phase_without_publishing_early(
    journal, catalog, monkeypatch
):
    observed = []
    original_prepare, original_apply = SemanticFinish.prepare, SemanticFinish._apply

    def prepare(self, **options):
        assert not self.db.in_transaction
        changes = self.db.total_changes
        batch = original_prepare(self, row_budget=1, byte_budget=options["byte_budget"])
        assert self.db.total_changes == changes and not self.db.in_transaction
        return batch

    def apply(self, batch):
        assert self.db.in_transaction
        prefix = self.db.execute("SELECT * FROM journal_prefix").fetchall()
        state = original_apply(self, batch)
        assert self.db.execute("SELECT * FROM journal_prefix").fetchall() == prefix
        assert self.db.in_transaction
        observed.append(FinishState.from_json(batch.before_json).phase)
        return state

    monkeypatch.setattr(SemanticFinish, "prepare", prepare)
    monkeypatch.setattr(SemanticFinish, "_apply", apply)
    with validate_journal(journal, catalog=catalog) as summary:
        assert summary.journal_sha256 == hashlib.sha256(journal).hexdigest()
    assert {"row", "lifecycle", "content", "externals"} <= set(observed)


CRASH_PROGRAM = r"""
import os,sqlite3,sys
from riverhog_provenance._semantic_finish import SemanticFinish
from riverhog_provenance.journal import JournalFrame
from riverhog_provenance.journal_store import JournalStore
class CrashConnection(sqlite3.Connection):
    armed=False
    def execute(self, sql, parameters=()):
        result=super().execute(sql,parameters)
        header=sys.argv[2]=='header' and sql.startswith('UPDATE journal_semantic_finish')
        mutation=sys.argv[2]=='mutation' and sql.startswith('INSERT OR IGNORE INTO fixities')
        if self.armed and (header or mutation):
            os._exit(73)
        return result
    def commit(self):
        if self.armed and sys.argv[2]=='before-commit': os._exit(73)
        super().commit()
        if self.armed and sys.argv[2]=='after-commit': os._exit(73)
class Store:
    _lifecycle_bounds_hold=JournalStore._lifecycle_bounds_hold
    def __init__(self,db): self.db=db
db=sqlite3.connect(sys.argv[1],factory=CrashConnection)
db.execute('PRAGMA synchronous=FULL').close()
current=SemanticFinish(Store(db),attempt_id='owned-finish-attempt',
    frame=JournalFrame(b'{"journal_id":"urn:uuid:00000000-0000-4000-8000-000000000002","sequence":"1"}'),
    catalog_sha256='a'*64,require_profiles=False)
batch=current.prepare(row_budget=1)
if sys.argv[2]=='prepared': os._exit(73)
db.armed=True
current.commit(batch)
raise SystemExit(99)
"""


@pytest.mark.parametrize(
    "phase,seam",
    [
        (phase, seam)
        for phase in ("row", "lifecycle", "invalidation", "usage", "content", "externals")
        for seam in ("prepared", "header", "before-commit", "after-commit")
    ]
    + [("content", "mutation")],
)
def test_real_process_death_keeps_a_complete_sqlite_unit(tmp_path, phase, seam):
    path = tmp_path / "kill.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        if phase in {"invalidation", "usage"}:
            state = lifecycle(db)
            assertion(db, 1, "generation", state=state)
            assertion(db, 2, "usage", changed=False, state=state, at="2025-01-01T00:00:00Z")
            db.execute(
                "INSERT INTO lifecycle_extrema VALUES (?,?,?)", (state["object_id"], "0", None)
            )
        else:
            row = assertion(
                db,
                1,
                "reported_description",
                state={"object_id": identity(9000000)},
                content={
                    "size_bytes": "4",
                    "digests": [{"algorithm": "sha-256", "value": "b" * 64}],
                },
            )
            external(db, row, 1)
        db.commit()
        current = work(db)
        current.initialize()
        position(current, phase)
        batch = current.prepare(row_budget=1)
        before = contents(db)
        current.commit(batch)
        expected = contents(db)
    with closing(connect(path)) as db:
        db.execute("UPDATE journal_semantic_finish SET checkpoint=?", (batch.before_json,))
        if phase == "content":
            db.execute("DELETE FROM fixities")
        db.commit()
    result = subprocess.run(
        [sys.executable, "-c", CRASH_PROGRAM, str(path), seam], capture_output=True, text=True
    )
    assert result.returncode == 73, result.stderr
    with closing(connect(path)) as db:
        assert db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert contents(db) == (expected if seam == "after-commit" else before)
        current = work(db)
        while not current.advance(row_budget=1).done:
            pass


class MeterCursor(sqlite3.Cursor):
    closed = False

    def record(self, row):
        if row is not None:
            self.connection.metrics["returned_rows"] += 1
            if self.sql.startswith("SELECT value "):
                self.connection.metrics["payload_fetched_octets"] += len(row[0])
            else:
                self.connection.metrics["metadata_octets"] += sum(
                    len(value) if isinstance(value, bytes) else len(str(value).encode())
                    for value in row
                    if value is not None
                )
        return row

    def fetchone(self):
        return self.record(super().fetchone())

    def fetchall(self):
        return [self.record(row) for row in super().fetchall()]

    def close(self):
        self.closed = True
        return super().close()


class MeterConnection(sqlite3.Connection):
    recording = False

    def execute(self, sql, parameters=()):
        if not self.recording:
            return super().execute(sql, parameters)
        cursor = self.cursor(factory=MeterCursor)
        cursor.sql = sql
        self.statements.append((sql, parameters, cursor))
        if sql.startswith("UPDATE journal_semantic_finish"):
            self.metrics["written_value_octets"] += len(parameters[0].encode())
        elif sql.startswith("INSERT OR IGNORE"):
            self.metrics["written_value_octets"] += sum(
                len(str(value).encode()) for value in parameters
            )
        return cursor.execute(sql, parameters)

    @contextmanager
    def observe(self):
        was_in_transaction = self.in_transaction
        self.metrics = {
            name: 0
            for name in (
                "sql_statements",
                "returned_rows",
                "changed_rows",
                "payload_fetched_octets",
                "sqlite_vm_instructions",
                "metadata_octets",
                "written_value_octets",
                "json_function_calls",
            )
        }
        self.statements = []
        before = self.total_changes

        def trace(_sql):
            self.metrics["sql_statements"] += 1

        def tick():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def authorize(action, table, column, _database, _trigger):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and table == "frames" and column == "value"
                else sqlite3.SQLITE_OK
            )

        def extract(value, path):
            self.metrics["json_function_calls"] += 1
            return json.loads(value).get(path.removeprefix("$."))

        self.create_function("json_extract", 2, extract, deterministic=True)
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
            if not sql.startswith(("SELECT", "UPDATE")):
                continue
            with closing(self.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as cursor:
                plan = " ".join(row[3] for row in cursor.fetchall())
            assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan, (sql, plan)


def semantic_finish_samples(tmp_path, measured_operation, *, samples):
    binding = growth_binding(getattr(SemanticFinish, measured_operation))
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    regimes = [
        (0, 0, 64, 0, 0),
        (1, 0, 1, 0, 0),
        (15, 0, 16, 0, 0),
        (16, 0, 16, 0, 0),
        (17, 0, 64, 17, 0),
        (17, 0, 16, 0, 0),
        (129, 0, 64, 1, 0),
        (1025, 0, 64, 17, 0),
        (17, 4097, 16, 0, 0),
        (17, 0, 16, 0, 65537),
    ]
    for phase in ("row", "lifecycle", "invalidation", "usage", "content", "externals", "done"):
        for index, (count, history, quantum, resumed, payload) in enumerate(regimes):
            path = tmp_path / f"{phase}-{index}.sqlite3"
            create(path)
            with closing(connect(path)) as db:
                state = lifecycle(db)
                row = assertion(db, 1, "generation", state=state)
                if phase == "content":
                    db.execute("DELETE FROM changes")
                    row = assertion(
                        db,
                        2,
                        "reported_description",
                        state=state,
                        padding="x" * payload,
                        content={
                            "size_bytes": "4",
                            "digests": [{"algorithm": "sha-256", "value": "b" * 64}],
                        },
                    )
                    db.execute(
                        "INSERT INTO fixities VALUES (?,?,?)", (state["object_id"], "4", "a" * 64)
                    )
                for n in range(count):
                    if phase == "externals":
                        external(db, row, n, payload=payload)
                    elif phase in {"invalidation", "usage"}:
                        assertion(
                            db,
                            100 + n,
                            phase,
                            changed=False,
                            state=state,
                            at="2025-01-01T00:00:00Z",
                            padding="x" * payload,
                        )
                if phase in {"invalidation", "usage"}:
                    db.execute(
                        "INSERT INTO lifecycle_extrema VALUES (?,?,?)",
                        (state["object_id"], "0", None),
                    )
                if phase not in {"invalidation", "usage", "externals"}:
                    for n in range(count):
                        assertion(db, 100 + n, changed=False)
                for n in range(history):
                    other = assertion(db, 20000 + n, changed=False)
                    external(db, other, 20000 + n)
                db.commit()
                current = work(db)
                current.initialize()
                if phase == "done":
                    while not current.advance(byte_budget=1000000).done:
                        pass
                else:
                    position(current, phase)
                if phase in {"invalidation", "usage", "externals"}:
                    for _ in range(resumed):
                        current.advance(row_budget=1, byte_budget=1000000)
                assert current.load().phase == phase
                retained = sum(
                    db.execute("SELECT count(*) FROM " + name).fetchone()[0]
                    for name in ("assertions", "changes", "maps", "fixities", "findings")
                )
            with closing(connect(path, factory=MeterConnection)) as db:
                current = work(db)
                admission = 100000 if payload else 65536
                unchanged = contents(db) if measured_operation != "advance" else None
                if measured_operation == "prepare":
                    with db.observe() as metrics:
                        batch = current.prepare(row_budget=quantum, byte_budget=admission)
                    assert not db.in_transaction and batch.before_json == current.load().to_json()
                elif measured_operation == "_apply":
                    batch = current.prepare(row_budget=quantum, byte_budget=admission)
                    db.execute("BEGIN IMMEDIATE").close()
                    try:
                        with db.observe() as metrics:
                            after = current._apply(batch)
                        assert db.in_transaction and after == batch.after
                    finally:
                        db.rollback()
                else:
                    assert measured_operation == "advance"
                    with db.observe() as metrics:
                        current.advance(row_budget=quantum, byte_budget=admission)
                if unchanged is not None:
                    assert contents(db) == unchanged
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": digest,
                    "dimensions": {
                        "retained_rows": retained,
                        "row_budget": quantum,
                        "byte_budget": admission,
                    },
                    "metrics": {
                        bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds
                    },
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
    assert len(samples) == 70


@pytest.mark.growth_witness(
    "riverhog-semantic-finish-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._semantic_finish.SemanticFinish.advance",
)
def test_actual_cold_indexed_semantic_finish_work(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    semantic_finish_samples(tmp_path, "advance", samples=samples)


@pytest.mark.growth_witness(
    "riverhog-semantic-finish-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._semantic_finish.SemanticFinish.prepare",
)
def test_actual_detached_semantic_finish_plan(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    semantic_finish_samples(tmp_path, "prepare", samples=samples)


@pytest.mark.growth_witness(
    "riverhog-semantic-finish-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._semantic_finish.SemanticFinish._apply",
)
def test_actual_owner_transaction_semantic_finish_application(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    semantic_finish_samples(tmp_path, "_apply", samples=samples)


def test_changed_assertion_uuid_order_precedes_later_content_failure(tmp_path):
    path = tmp_path / "assertion-order.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        assertion(db, 20, "observation", address_status="known")
        first = assertion(db, 10)
        external(db, first, 1, "activity")
        db.commit()
        current = work(db)
        current.initialize()
        with pytest.raises(ProvenanceValidationError, match="wrong reference type"):
            while not current.advance(row_budget=1).done:
                pass
        assert current.load().current == first["assertion_id"]


def test_reference_failure_keeps_native_physical_order_over_map_key_order(tmp_path):
    path = tmp_path / "reference-order.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        row = assertion(db, 1)
        external(db, row, 20, "state")
        external(db, row, 10, "activity")
        db.commit()
        native = db.execute(
            "SELECT value FROM maps WHERE name='externals' AND json_extract(value,'$.object_id')=?",
            (row["id"],),
        ).fetchone()[0]
        assert json.loads(native)["object_type"] == "state"
        current = work(db)
        current.initialize()
        position(current, "externals")
        with pytest.raises(ProvenanceValidationError, match=r"expected \('state',\)"):
            current.advance(row_budget=1)


def test_byte_boundary_commits_only_preceding_admitted_scan_rows(tmp_path):
    path = tmp_path / "byte-boundary.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        row = assertion(db, 1)
        small = external(db, row, 1)
        large = external(db, row, 2, payload=65537)
        db.commit()
        current = work(db)
        current.initialize()
        position(current, "externals")
        first = current.advance(row_budget=64, byte_budget=len(canonical_json_bytes(small)))
        assert first.scan == 1
        before = contents(db)
        with pytest.raises(SemanticFinishCapacity):
            current.advance(byte_budget=65536)
        assert contents(db) == before
        second = current.advance(row_budget=64, byte_budget=len(canonical_json_bytes(large)))
        assert second.phase == "row"
