from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from contextlib import closing, contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance._entry_preparation import (
    EntryPreparation,
    EntryPreparationCapacity,
    StaleEntryPreparation,
)
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.journal import JournalFrame
from riverhog_provenance.journal_store import JournalStore
from test_assertion_staging import connect, create, identity


def owner(db):
    store = object.__new__(JournalStore)
    store._storage = SimpleNamespace(connection=db)
    store._cleanup = SimpleNamespace(alive=True)
    return store


def frame(payload=0, **fields):
    return JournalFrame(
        canonical_json_bytes(
            {
                "id": identity(3),
                "journal_id": identity(2),
                "sequence": "1",
                "body": {"assertions": {}},
                "padding": "x" * payload,
                **fields,
            }
        )
    )


def work(db, source, **options):
    return EntryPreparation(
        owner(db),
        **{
            "attempt_id": "entry-attempt",
            "frame": source,
            "catalog_sha256": "a" * 64,
            "require_profiles": False,
            **options,
        },
    )


def queued(db, count):
    db.executemany(
        "INSERT INTO changes VALUES (?)", ((identity(1000 + n),) for n in range(count))
    ).close()
    db.commit()


def snapshot(db):
    return {
        name: db.execute("SELECT * FROM " + name + " ORDER BY 1").fetchall()
        for name in (
            "identities",
            "changes",
            "journal_prefix",
            "frames",
            "journal_entry_preparation",
        )
    }


def finish(current, *, budget=16):
    for _ in range(10000):
        state = current.advance(row_budget=budget)
        assert not current.db.in_transaction
        if state.done:
            current.result()
            return state
    raise AssertionError("entry preparation fixture did not finish")


def position(current, phase):
    for _ in range(10000):
        state = current.load()
        if state.phase == phase:
            return
        assert not state.done
        current.advance(row_budget=1)
    raise AssertionError("fixture did not reach preparation phase")


@pytest.mark.parametrize("count", [0, 1, 17, 1025])
@pytest.mark.parametrize("budget", [1, 15, 16, 17, 256])
def test_reopen_each_unit_reuses_identities_and_retired_queue(tmp_path, count, budget):
    path, source = tmp_path / "entry.sqlite3", frame()
    create(path)
    with closing(connect(path)) as db:
        queued(db, count)
        work(db, source).initialize()
        prefix = snapshot(db)["journal_prefix"]
    for _ in range(10000):
        with closing(connect(path)) as db:
            current = work(db, source)
            state = current.advance(row_budget=budget)
            assert snapshot(db)["journal_prefix"] == prefix
            if state.done:
                current.result()
                assert state.retired == count
                assert db.execute("SELECT * FROM changes").fetchall() == []
                assert db.execute(
                    "SELECT domain,value FROM identities WHERE id=?", (identity(2),)
                ).fetchone() == ("journal", b"")
                assert db.execute(
                    "SELECT domain,value FROM identities WHERE id=?", (identity(3),)
                ).fetchone() == ("entry", source.json_bytes)
                before = db.total_changes
                assert current.advance() == state
                assert db.total_changes == before
                break
    else:
        pytest.fail("reopened entry preparation did not finish")


@pytest.mark.parametrize(
    "domain,value", [("referent", b"large" * 65536), ("journal", ""), ("journal", b"nonempty")]
)
def test_journal_identity_error_precedes_duplicate_entry(tmp_path, domain, value):
    path, source = tmp_path / "entry.sqlite3", frame()
    create(path)
    with closing(connect(path)) as db:
        db.execute("INSERT INTO identities VALUES (?,?,?)", (identity(2), domain, value)).close()
        db.execute(
            "INSERT INTO identities VALUES (?,?,?)", (identity(3), "entry", source.json_bytes)
        ).close()
        db.commit()
        current = work(db, source)
        current.initialize()
        before = snapshot(db)
        with pytest.raises(
            ProvenanceValidationError, match="immutable referent or identity domain"
        ):
            current.advance()
        assert snapshot(db) == before
        assert not db.in_transaction


@pytest.mark.parametrize("same_as_journal", [False, True])
def test_reused_entry_fails_before_queue_retirement(tmp_path, same_as_journal):
    path = tmp_path / "entry.sqlite3"
    source = frame(id=identity(2) if same_as_journal else identity(3))
    create(path)
    with closing(connect(path)) as db:
        queued(db, 17)
        if not same_as_journal:
            db.execute(
                "INSERT INTO identities VALUES (?,?,?)", (identity(3), "entry", source.json_bytes)
            ).close()
            db.commit()
        current = work(db, source)
        current.initialize()
        current.advance()
        before = snapshot(db)
        with pytest.raises(ProvenanceValidationError, match="entry identity reused"):
            current.advance()
        assert snapshot(db) == before


def test_entry_byte_admission_suspends_without_skipping(tmp_path):
    path, source = tmp_path / "entry.sqlite3", frame(1024 * 1024)
    create(path)
    with closing(connect(path)) as db:
        current = work(db, source)
        current.initialize()
        current.advance()
        before = snapshot(db)
        with pytest.raises(EntryPreparationCapacity, match="entry atom"):
            current.advance(byte_budget=16384)
        assert snapshot(db) == before
        finish(current)


def test_stale_proposal_and_ordinary_load_never_reset_progress(tmp_path):
    path, source = tmp_path / "entry.sqlite3", frame()
    create(path)
    with closing(connect(path)) as db:
        current = work(db, source)
        current.initialize()
        batch = current.prepare()
        current.advance()
        before = snapshot(db)
        with pytest.raises(StaleEntryPreparation):
            current.commit(batch)
        assert snapshot(db) == before
        assert work(db, source).load() == current.load()
        with pytest.raises(StaleEntryPreparation):
            current.initialize()
        assert snapshot(db) == before


@pytest.mark.parametrize("change", ["attempt", "source", "catalog", "mode", "prefix"])
def test_source_mode_attempt_and_prefix_replacement_refuse_without_mutation(tmp_path, change):
    path, source = tmp_path / "entry.sqlite3", frame()
    create(path)
    with closing(connect(path)) as db:
        current = work(db, source)
        current.initialize()
        options = {}
        if change == "attempt":
            options["attempt_id"] = "replacement"
        elif change == "source":
            source = frame(1)
        elif change == "catalog":
            options["catalog_sha256"] = "b" * 64
        elif change == "mode":
            options["require_profiles"] = True
        else:
            db.execute("UPDATE journal_prefix SET binding=?", (b"replaced-prefix",)).close()
            db.commit()
        before = snapshot(db)
        with pytest.raises(StaleEntryPreparation):
            work(db, source, **options).advance()
        assert snapshot(db) == before


@pytest.mark.parametrize(
    "change", ["header", "phase", "version", "journal", "entry", "terminal-queue"]
)
def test_missing_or_corrupt_progress_links_fail_closed(tmp_path, change):
    path, source = tmp_path / "entry.sqlite3", frame()
    create(path)
    with closing(connect(path)) as db:
        current = work(db, source)
        current.initialize()
        finish(current)
        if change == "header":
            db.execute("DELETE FROM journal_entry_preparation").close()
        elif change in {"phase", "version"}:
            value = json.loads(current.load().to_json())
            value[change] = "unknown" if change == "phase" else "-1"
            db.execute(
                "UPDATE journal_entry_preparation SET checkpoint=?",
                (canonical_json_bytes(value).decode(),),
            ).close()
        elif change in {"journal", "entry"}:
            db.execute(
                "DELETE FROM identities WHERE id=?", (identity(2 if change == "journal" else 3),)
            ).close()
        else:
            db.execute("INSERT INTO changes VALUES (?)", (identity(99),)).close()
        db.commit()
        before = snapshot(db)
        with pytest.raises((StaleEntryPreparation, RuntimeError)):
            current.advance()
        assert snapshot(db) == before


CRASH_PROGRAM = r"""
import os,sqlite3,sys
from pathlib import Path
sys.path.insert(0,sys.argv[2])
from test_entry_preparation import connect,work,JournalFrame
class CrashConnection(sqlite3.Connection):
    active=False
    def commit(self):
        super().commit()
        if self.active:os._exit(73)
path,_,source_path,seam=sys.argv[1:]
db=connect(path,factory=CrashConnection)
db.create_function('hard_exit',0,lambda:os._exit(73))
if seam in {'init-header','journal-identity','entry-identity','journal-header',
            'entry-header','changes-header','changes-delete'}:
    if seam=='init-header':event,table,condition='INSERT','journal_entry_preparation','1'
    elif seam.endswith('identity'):
        event,table='INSERT','identities'
        condition="NEW.domain='"+seam.split('-')[0]+"'"
    elif seam=='changes-delete':event,table,condition='DELETE','changes','1'
    else:event,table,condition='UPDATE','journal_entry_preparation','1'
    db.execute('CREATE TRIGGER hard_crash AFTER '+event+' ON '+table+
               ' WHEN '+condition+' BEGIN SELECT hard_exit(); END;').close()
sqlite3.Connection.commit(db)
if seam.endswith('before-commit'):
    db.set_trace_callback(lambda sql:os._exit(73) if sql.strip().upper()=='COMMIT' else None)
if seam.endswith('after-commit'):db.active=True
current=work(db,JournalFrame(Path(source_path).read_bytes()))
if seam.startswith('init-'):current.initialize()
else:current.advance(row_budget=1)
raise SystemExit(99)
"""


@pytest.mark.parametrize(
    "seam",
    [
        "init-header",
        "init-before-commit",
        "init-after-commit",
        "journal-identity",
        "entry-identity",
        "journal-header",
        "entry-header",
        "changes-header",
        "changes-delete",
        "journal-before-commit",
        "entry-before-commit",
        "changes-before-commit",
        "done-before-commit",
        "journal-after-commit",
        "entry-after-commit",
        "changes-after-commit",
        "done-after-commit",
    ],
)
def test_real_process_death_preserves_admission_or_its_exact_commit(tmp_path, seam):
    path, source = tmp_path / "entry.sqlite3", frame()
    input_path = tmp_path / "entry.json"
    input_path.write_bytes(source.json_bytes)
    create(path)
    with closing(connect(path)) as db:
        queued(db, 17)
        current = work(db, source)
        if not seam.startswith("init-"):
            current.initialize()
            position(current, seam.split("-")[0])
        before = snapshot(db)
        with closing(sqlite3.connect(":memory:")) as expected_db:
            db.backup(expected_db)
            expected_work = work(expected_db, source)
            if seam.startswith("init-"):
                expected_work.initialize()
            else:
                expected_work.advance(row_budget=1)
            expected = snapshot(expected_db)
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            CRASH_PROGRAM,
            str(path),
            str(Path(__file__).parent),
            str(input_path),
            seam,
        ],
        capture_output=True,
        text=True,
    )
    assert child.returncode == 73, child.stderr
    with closing(connect(path)) as db:
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
        db.execute("DROP TRIGGER IF EXISTS hard_crash").close()
        db.commit()
        after = snapshot(db)
        assert after == (expected if seam.endswith("after-commit") else before)
        assert after["journal_prefix"] == before["journal_prefix"]
        current = work(db, source)
        if seam.startswith("init-") and seam != "init-after-commit":
            assert not after["journal_entry_preparation"]
            current.initialize()
        assert finish(current, budget=1).retired == 17


def logical_octets(values):
    return sum(
        len(v) if isinstance(v, bytes) else len(str(v).encode()) for v in values if v is not None
    )


class WorkCursor(sqlite3.Cursor):
    closed = False

    def record(self, value):
        if value is not None:
            self.connection.metrics["returned_rows"] += 1
            self.connection.metrics["returned_metadata_octets"] += logical_octets(value)
            if "FROM identities" in self.sql:
                self.connection.metrics["historical_payload_octets"] += sum(
                    len(v) for v in value if isinstance(v, bytes)
                )
        return value

    def fetchone(self):
        return self.record(super().fetchone())

    def __next__(self):
        return self.record(super().__next__())

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
        self.metrics["bound_parameter_octets"] += logical_octets(parameters)
        return cursor.execute(sql, parameters)

    @contextmanager
    def observe(self):
        self.metrics = {
            key: 0
            for key in (
                "sql_statements",
                "returned_rows",
                "changed_rows",
                "returned_metadata_octets",
                "bound_parameter_octets",
                "historical_payload_octets",
                "sqlite_vm_instructions",
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
                if action == sqlite3.SQLITE_READ
                and table in {"assertions", "frames"}
                and column == "value"
                else sqlite3.SQLITE_OK
            )

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
        assert not self.in_transaction
        assert all(cursor.closed for _sql, _args, cursor in self.statements)
        for sql, args, _ in self.statements:
            if not sql.startswith(("SELECT", "UPDATE", "DELETE")):
                continue
            with closing(self.execute("EXPLAIN QUERY PLAN " + sql, args)) as cursor:
                plan = " ".join(row[3] for row in cursor.fetchall())
            assert "USE TEMP" not in plan
            if sql.startswith("DELETE FROM changes"):
                assert "ORDER BY assertion_id LIMIT ?" in sql and 1 <= args[0] <= 256
                assert "COVERING INDEX sqlite_autoindex_changes_1" in plan
            else:
                assert "SEARCH" in plan and "SCAN" not in plan, (sql, plan)


@pytest.mark.growth_witness(
    "riverhog-entry-preparation-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_preparation.EntryPreparation.advance",
)
def test_actual_cold_entry_preparation_units(tmp_path, record_property):
    binding = growth_binding(EntryPreparation.advance)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    cases = [
        (phase, count, budget, 0)
        for phase in ("journal", "entry", "changes", "done", "journal-invalid", "entry-invalid")
        for count, budget in ((1, 1), (15, 16), (16, 16), (17, 16), (129, 16), (1025, 64))
    ]
    cases += [(phase, 0, 16, 0) for phase in ("changes", "done")]
    cases += [("entry", 17, 16, size) for size in (1024 * 1024, 7 * 1024 * 1024)]
    samples = []
    for ordinal, (phase, count, budget, payload) in enumerate(cases):
        path, source = tmp_path / f"entry-{ordinal}.sqlite3", frame(payload)
        create(path)
        with closing(sqlite3.connect(":memory:")) as db:
            with closing(connect(path)) as original:
                original.backup(db)
            queued(db, count)
            history = 4097 if count in (17, 1025) else count
            db.executemany(
                "INSERT INTO identities VALUES (?,?,?)",
                (
                    (identity(100000 + n), "entry", b"unread-history" if n else b"unread" * 65536)
                    for n in range(history)
                ),
            ).close()
            if phase == "journal-invalid":
                db.execute(
                    "INSERT INTO identities VALUES (?,?,?)",
                    (identity(2), "referent", b"unread" * 65536),
                ).close()
            if phase == "entry-invalid":
                db.execute(
                    "INSERT INTO identities VALUES (?,?,?)",
                    (identity(3), "entry", b"unread" * 65536),
                ).close()
            db.commit()
            current = work(db, source)
            current.initialize()
            position(current, phase.split("-")[0])
            prefix = snapshot(db)["journal_prefix"]
            state = current.load()
            prefix_values = db.execute("SELECT * FROM journal_prefix").fetchone()
            control = (
                8192
                + len(current.binding)
                + len(current.attempt_id.encode())
                + len(state.to_json().encode())
                + logical_octets(prefix_values)
            )
            retained = sum(
                db.execute("SELECT count(*) FROM " + name).fetchone()[0]
                for name in ("identities", "changes", "journal_prefix", "journal_entry_preparation")
            )
            with closing(connect(path)) as destination:
                db.backup(destination)
        with closing(connect(path, factory=WorkConnection)) as db:
            current = work(db, source)
            byte_budget = 16 * 1024 * 1024
            with db.observe() as metrics:
                if phase.endswith("invalid"):
                    with pytest.raises(ProvenanceValidationError):
                        current.advance(row_budget=budget, byte_budget=byte_budget)
                else:
                    current.advance(row_budget=budget, byte_budget=byte_budget)
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {
                    "retained_rows": retained,
                    "row_budget": budget,
                    "byte_budget": byte_budget,
                    "control_octets": control,
                },
                "metrics": dict(metrics),
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
            assert snapshot(db)["journal_prefix"] == prefix
            if not phase.endswith("invalid"):
                with closing(sqlite3.connect(":memory:")) as suffix_db:
                    db.backup(suffix_db)
                    assert finish(work(suffix_db, source)).retired == count
    assert len(samples) == 40
    record_property("growth_samples", samples)
