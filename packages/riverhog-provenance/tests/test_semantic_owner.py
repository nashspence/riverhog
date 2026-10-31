from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import asdict
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance._generic_semantics import (
    GenericAdmission,
    GenericBatch,
    GenericState,
    StaleGenericSemantics,
)
from test_generic_semantics import connect, create, finish, position, subject, work


def store_batch(batch, path):
    payload = asdict(batch)
    payload["after"] = batch.after.to_json()
    path.write_text(
        json.dumps(
            {"admission": isinstance(batch, GenericAdmission), "value": payload},
            default=lambda value: {"octets": value.hex()},
        )
    )


def load_batch(path):
    def octets(value):
        return bytes.fromhex(value["octets"]) if set(value) == {"octets"} else value

    payload = json.loads(Path(path).read_text(), object_hook=octets)
    value = payload["value"]
    value["prefix"] = tuple(value["prefix"])
    value["after"] = GenericState.from_json(value["after"])
    if payload["admission"]:
        if value["prior"] is not None:
            value["prior"] = tuple(value["prior"])
        return GenericAdmission(**value)
    assert value["edge"] is None
    value["writes"] = tuple((sql, tuple(parameters)) for sql, parameters in value["writes"])
    return GenericBatch(**value)


def owner_table(db):
    db.execute(
        "CREATE TABLE semantic_owner(singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
        "attempt_id TEXT,epoch TEXT,version TEXT,checkpoint TEXT)"
    ).close()
    db.execute("INSERT INTO semantic_owner VALUES (1,'owner','2','0','initial')").close()
    db.commit()


def snapshot(db):
    with closing(
        db.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    ) as cursor:
        names = [row[0] for row in cursor]
    result = {}
    for name in names:
        with closing(db.execute('SELECT * FROM "' + name.replace('"', '""') + '"')) as cursor:
            result[name] = sorted(cursor.fetchall(), key=repr)
    return result


def assert_uncommitted_hidden(path, before):
    with closing(sqlite3.connect(path, timeout=0)) as reader:
        try:
            visible = snapshot(reader)
        except sqlite3.OperationalError as exc:
            assert exc.sqlite_errorcode & 255 in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}
        else:
            assert visible == before


def apply_owned(current, batch, *, admission=False, epoch="2", version="0"):
    db = current.db
    db.execute("BEGIN IMMEDIATE").close()
    try:
        with closing(
            db.execute(
                "SELECT epoch,version FROM semantic_owner WHERE singleton=1 AND attempt_id='owner'"
            )
        ) as cursor:
            if cursor.fetchone() != (epoch, version):
                raise RuntimeError("owner was replaced")
        state = current._admit(batch) if admission else current._apply(batch)
        with closing(
            db.execute(
                "UPDATE semantic_owner SET version=?,checkpoint=? WHERE singleton=1 "
                "AND attempt_id='owner' AND epoch=? AND version=?",
                (str(int(version) + 1), state.to_json(), epoch, version),
            )
        ) as cursor:
            assert cursor.rowcount == 1
        db.commit()
    except BaseException:
        db.rollback()
        raise
    return state


@pytest.mark.parametrize("admission", [False, True])
def test_parent_owns_commit_and_rollback_of_child_progress(tmp_path, catalog, admission):
    row, path = subject(3), tmp_path / "owner.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        owner_table(db)
        current = work(db, row, catalog)
        if admission:
            batch = current.prepare_admission()
        else:
            current.initialize()
            position(current, ("typed", "0"))
            batch = current.prepare(step_budget=1)
            assert any(sql == "@output" for sql, _ in batch.writes)
        before = snapshot(db)
        db.execute("BEGIN IMMEDIATE").close()
        if admission:
            current._admit(batch)
        else:
            current._apply(batch)
        assert db.in_transaction
        assert_uncommitted_hidden(path, before)
        db.rollback()
        assert snapshot(db) == before
        state = apply_owned(current, batch, admission=admission)
        assert not db.in_transaction
        assert db.execute("SELECT version,checkpoint FROM semantic_owner").fetchone() == (
            "1",
            state.to_json(),
        )
        assert current.load() == state


@pytest.mark.parametrize("admission", [False, True])
def test_child_application_requires_an_owner_transaction(tmp_path, catalog, admission):
    row, path = subject(1), tmp_path / "owner.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog)
        if admission:
            batch = current.prepare_admission()
        else:
            current.initialize()
            batch = current.prepare(step_budget=1)
        before = snapshot(db)
        with pytest.raises(RuntimeError, match="requires an owner transaction"):
            (current._admit if admission else current._apply)(batch)
        assert not db.in_transaction and snapshot(db) == before


@pytest.mark.parametrize("change", ["owner-epoch", "owner-version", "prefix", "child"])
@pytest.mark.parametrize("admission", [False, True])
def test_stale_owner_or_child_cannot_publish_the_other_marker(tmp_path, catalog, change, admission):
    row, path = subject(3), tmp_path / "owner.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        owner_table(db)
        current = work(db, row, catalog)
        if admission:
            batch = current.prepare_admission()
        else:
            current.initialize()
            batch = current.prepare(step_budget=1)
        if change.startswith("owner-"):
            column = change.removeprefix("owner-")
            db.execute("UPDATE semantic_owner SET " + column + "='3'").close()
            db.commit()
        elif change == "prefix":
            db.execute("UPDATE journal_prefix SET binding=?", (b"replaced-source",)).close()
            db.commit()
        elif admission:
            work(db, row, catalog, attempt_id="replacement-child").initialize()
        else:
            current.advance(step_budget=1)
        before = snapshot(db)
        with pytest.raises((RuntimeError, StaleGenericSemantics)):
            apply_owned(current, batch, admission=admission)
        assert not db.in_transaction and snapshot(db) == before


CRASH_PROGRAM = r"""
import os,sqlite3,sys,json
from pathlib import Path
sys.path.insert(0,sys.argv[2])
from test_generic_semantics import connect,work
from test_semantic_owner import apply_owned,load_batch
from riverhog_provenance_contracts import ContractCatalog
class DeathConnection(sqlite3.Connection):
    active=False
    def commit(self):
        super().commit()
        if self.active:os._exit(73)
path,_,row_path,batch_path,mode,seam=sys.argv[1:]
db=connect(path,factory=DeathConnection)
db.create_function('hard_exit',0,lambda:os._exit(73))
events={
    'child-header':('INSERT' if mode=='admission' else 'UPDATE','journal_generic_semantics'),
    'child-frame':('INSERT','generic_semantic_frames'),
    'output':('INSERT','maps'),
    'undo':('INSERT','generic_semantic_outputs'),
    'parent':('UPDATE','semantic_owner'),
}
if seam in events:
    event,table=events[seam]
    db.execute('CREATE TRIGGER hard_crash AFTER '+event+' ON '+table+
               ' BEGIN SELECT hard_exit(); END;').close()
sqlite3.Connection.commit(db)
if seam=='before-commit':
    db.set_trace_callback(lambda sql:os._exit(73) if sql.strip().upper()=='COMMIT' else None)
if seam=='after-commit':db.active=True
current=work(db,json.loads(Path(row_path).read_text()),ContractCatalog())
batch=load_batch(batch_path)
apply_owned(current,batch,admission=mode=='admission')
raise SystemExit(99)
"""


CASES = [
    ("admission", seam, 1)
    for seam in ("child-header", "child-frame", "parent", "before-commit", "after-commit")
] + [
    ("apply", seam, budget)
    for seam in ("child-header", "output", "undo", "parent", "before-commit", "after-commit")
    for budget in (1, 16)
]


@pytest.mark.parametrize("mode,seam,budget", CASES)
def test_real_process_death_commits_both_markers_or_neither(tmp_path, catalog, mode, seam, budget):
    row, path = subject(3), tmp_path / "owner.sqlite3"
    row_path = tmp_path / "row.json"
    batch_path = tmp_path / "proposal.json"
    row_path.write_bytes(canonical_json_bytes(row))
    create(path, row)
    with closing(connect(path)) as db:
        owner_table(db)
        current = work(db, row, catalog)
        if mode == "apply":
            current.initialize()
            position(current, ("typed", "0"))
        before = snapshot(db)
        with closing(sqlite3.connect(":memory:")) as expected_db:
            db.backup(expected_db)
            expected_current = work(expected_db, row, catalog)
            batch = (
                expected_current.prepare_admission()
                if mode == "admission"
                else expected_current.prepare(step_budget=budget)
            )
            store_batch(batch, batch_path)
            apply_owned(expected_current, batch, admission=mode == "admission")
            expected = snapshot(expected_db)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            CRASH_PROGRAM,
            str(path),
            str(Path(__file__).parent),
            str(row_path),
            str(batch_path),
            mode,
            seam,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 73, result.stderr
    with closing(connect(path)) as db:
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
        db.execute("DROP TRIGGER IF EXISTS hard_crash").close()
        db.commit()
        assert snapshot(db) == (expected if seam == "after-commit" else before)
        current = work(db, row, catalog)
        if seam != "after-commit":
            batch = (
                current.prepare_admission()
                if mode == "admission"
                else current.prepare(step_budget=budget)
            )
            apply_owned(current, batch, admission=mode == "admission")
        finish(current, budget=1)
        current.result()
