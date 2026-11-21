from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
import test_generic_semantics as cases
from riverhog_provenance import _generic_semantics as semantics
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum
from riverhog_provenance.journal_session import JournalValidationBudget
from riverhog_provenance.journal_store import WORKSPACE_FORMAT, InvalidJournalWorkspace
from test_retained_journal_session import OwnedSource, directory, open_session


@pytest.mark.parametrize("cold", [False, True], ids=["live", "reopened-child"])
def test_actual_stack_steps_preserve_outputs_without_frame_json_codecs(
    tmp_path, catalog, monkeypatch, cold
):
    path, row = tmp_path / "semantic.sqlite3", cases.subject(1)
    cases.create(path, row)

    def forbidden(*args, **kwargs):
        pytest.fail("semantic stack spill must not encode or decode a JSON frame")

    monkeypatch.setattr(semantics, "_encode_frame", forbidden)
    monkeypatch.setattr(semantics, "_decode_frame", forbidden)
    with closing(cases.connect(path)) as db:
        current = cases.work(db, row, catalog)
        current.initialize()
        prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
        for _ in range(512):
            if cold:
                current = cases.work(db, row, catalog)
            state = current.advance(step_budget=1)
            assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
            assert not db.in_transaction
            if state.done:
                current.result()
                break
        else:
            pytest.fail("small semantic stack did not finish")
        cases.require_reclaimed(db)
        assert len(cases.outputs(db)) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("operation", "unknown"),
        ("operation", "x" * 65536),
        ("operation", b"generic"),
        ("node", "not-a-node"),
        ("node", b"0"),
        ("node", 0.5),
        ("node", 2**63 - 1),
        ("stage", -1),
        ("stage", 0.5),
        ("stage", 999),
        ("cursor", -1),
        ("cursor", 999),
        ("parent", "not-a-parent"),
        ("parent", 0),
    ],
    ids=[
        "unknown-operation",
        "wide-operation",
        "operation-type",
        "node-text",
        "node-blob",
        "node-real",
        "node-outside-source",
        "negative-stage",
        "stage-real",
        "unknown-stage",
        "negative-cursor",
        "cursor-outside-source",
        "parent-type",
        "parent-cycle",
    ],
)
def test_typed_stack_corruption_refuses_before_semantic_mutation(tmp_path, catalog, field, value):
    path, row = tmp_path / "semantic.sqlite3", cases.subject(1)
    cases.create(path, row)
    with closing(cases.connect(path)) as db:
        current = cases.work(db, row, catalog)
        current.initialize()
        db.execute("UPDATE generic_semantic_frames SET " + field + "=?", (value,)).close()
        db.commit()
        before = cases.snapshot(db)
        with pytest.raises(RuntimeError, match="corrupt generic semantic frame"):
            current.advance(step_budget=1)
        assert cases.snapshot(db) == before and not db.in_transaction


def test_original_stack_row_admission_precedes_fetch_and_retries_same_owner(tmp_path, catalog):
    path, row = tmp_path / "semantic.sqlite3", cases.subject(1)
    cases.create(path, row)
    with closing(cases.connect(path)) as db:
        current = cases.work(db, row, catalog)
        current.initialize()
        state = current.load()
        stack = semantics._Stack(current, state, semantics._Meter(65536))
        original = WorkQuantum(WorkAmount(rows=1, octets=1, source_octets=1, transitions=1))
        original.consume(WorkAmount(rows=1))
        before, statements = cases.snapshot(db), []
        db.set_trace_callback(statements.append)
        try:
            with pytest.raises(NativeWorkCapacity):
                with current.epoch.execution(None, work=original):
                    stack.peek()
        finally:
            db.set_trace_callback(None)
        assert statements == [] and stack.cache == {}
        assert original.consumed == WorkAmount(rows=1) and original.held == WorkAmount()
        assert cases.snapshot(db) == before
        retry = WorkQuantum(WorkAmount(rows=1, octets=1, source_octets=1, transitions=1))
        with current.epoch.execution(None, work=retry):
            value = stack.peek()
            assert value == semantics.Frame("generic", 0)
            assert stack.peek() is value
        assert retry.consumed == WorkAmount(rows=1) and retry.held == WorkAmount()
        assert current.epoch.quiescent and cases.snapshot(db) == before
        original.require_received()
        retry.require_received()


def test_current_stack_spill_schema_uses_native_semantic_fields(tmp_path):
    path = tmp_path / "semantic.sqlite3"
    cases.create(path, cases.subject(1))
    with closing(cases.connect(path)) as db:
        fields = [
            (row[1], row[2]) for row in db.execute("PRAGMA table_info(generic_semantic_frames)")
        ]
        assert fields == [
            ("attempt_id", "TEXT"),
            ("frame_id", "INTEGER"),
            ("parent", "INTEGER"),
            ("operation", "TEXT"),
            ("node", "INTEGER"),
            ("stage", "INTEGER"),
            ("cursor", "INTEGER"),
        ]


def test_incompatible_retained_format_refuses_without_mutating_or_recreating_state(
    tmp_path, journal, catalog, journal_constructions
):
    path = directory(tmp_path)
    source = OwnedSource(journal, catalog)
    with open_session(
        path, source, catalog, create=True, journal_constructions=journal_constructions
    ) as first:
        first.advance(JournalValidationBudget(source_octets=7))
    assert first.storage_received
    database = path / "snapshot.sqlite3"
    with closing(sqlite3.connect(database)) as db:
        assert db.execute("SELECT format FROM journal_workspace").fetchone() == (WORKSPACE_FORMAT,)
        db.execute("UPDATE journal_workspace SET format=?", ("unsupported-owner-format",)).close()
        db.commit()
    before, inode, files = database.read_bytes(), database.stat().st_ino, set(path.iterdir())
    source.calls.clear()
    with pytest.raises(InvalidJournalWorkspace, match="incomplete or incompatible"):
        open_session(path, source, catalog, epoch=1, journal_constructions=journal_constructions)
    assert journal_constructions.frames[-1].close_step()
    assert database.read_bytes() == before and database.stat().st_ino == inode
    assert set(path.iterdir()) == files and source.calls == []
    status = journal_constructions.admission.status
    assert not (status.connections or status.cursors or status.blobs or status.operations)
