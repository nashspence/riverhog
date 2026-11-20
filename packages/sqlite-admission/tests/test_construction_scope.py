"""Exact original construction receiving under shared SQLite admission."""

from __future__ import annotations

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
import sqlite_admission as owner
from sqlite_admission import SqliteCapacity, SqliteCleanupPending, SqliteConnectionAdmission


def pool(*, verify=lambda: None, connections=2):
    return SqliteConnectionAdmission(
        connection_limit=connections,
        cursor_limit=8,
        operation_limit=2,
        verify_process=verify,
    )


def test_passive_scope_has_no_process_or_driver_effects(tmp_path, monkeypatch):
    events = []
    admission = pool(verify=lambda: events.append("verify"))
    monkeypatch.setattr(owner, "_connect", lambda *_a, **_k: pytest.fail("driver entered"))
    scope = admission.connection_scope()
    assert admission.status.connections == admission.status.operations == 0
    assert not events and not list(tmp_path.iterdir())
    scope.close()
    assert scope.received
    with pytest.raises(RuntimeError, match="single-attempt"):
        scope.connect(tmp_path / "closed.sqlite3")
    assert not events


def test_original_shared_grant_precedes_dependent_file_effects(tmp_path, monkeypatch):
    events = []
    admission = pool(connections=1, verify=lambda: events.append("verify"))
    scope = admission.connection_scope()
    scope.reserve()
    scope.reserve()
    assert admission.status.connections == 1 and not scope.received
    assert events == ["verify"] and not list(tmp_path.iterdir())
    monkeypatch.setattr(owner, "_connect", lambda *_a, **_k: pytest.fail("driver entered"))
    with pytest.raises(SqliteCapacity):
        admission.connect(tmp_path / "other.sqlite3")
    scope.close()
    assert scope.received and admission.status.connections == 0
    assert not list(tmp_path.iterdir())


def test_process_refusal_keeps_files_uncreated_and_grant_unconsumed(tmp_path):
    def refuse():
        raise RuntimeError("process policy rejected")

    admission = pool(verify=refuse)
    scope = admission.connection_scope()
    with pytest.raises(RuntimeError, match="policy rejected"):
        scope.reserve()
    assert admission.status.connections == 0 and not list(tmp_path.iterdir())
    scope.close()
    assert scope.received


def test_receiving_is_exact_even_while_another_connection_remains(tmp_path, monkeypatch):
    admission = pool()
    first, second = admission.connection_scope(), admission.connection_scope()
    one = first.connect(tmp_path / "one.sqlite3")
    two = second.connect(tmp_path / "two.sqlite3")
    monkeypatch.setattr(
        type(admission),
        "pending_connections",
        property(lambda self: pytest.fail("scope scanned unrelated connections")),
    )
    first.close()
    assert first.received and not second.received and admission.status.connections == 1
    assert one.cleanup_complete
    cursor = two.execute("SELECT 17")
    assert cursor.fetchone() == (17,)
    cursor.close()
    second.close()
    assert second.received and admission.status.connections == 0


def test_unreturned_actual_connection_stays_on_its_original_scope(tmp_path, monkeypatch):
    admission = pool()
    original = admission.connection_scope()
    connection = owner._connect

    def lose(*args, **options):
        connection(*args, **options)
        raise InterruptedError("actual connection return was lost")

    with monkeypatch.context() as patch:
        patch.setattr(owner, "_connect", lose)
        with pytest.raises(InterruptedError):
            original.connect(tmp_path / "unreturned.sqlite3")
    assert admission.status.unreturned_connections == 1 and not original.received
    independent = admission.connection_scope()
    other = independent.connect(tmp_path / "independent.sqlite3")
    original.close()
    assert original.received and not independent.received
    cursor = other.execute("SELECT 23")
    assert cursor.fetchone() == (23,)
    cursor.close()
    independent.close()
    assert independent.received and admission.status.connections == 0


@pytest.mark.parametrize("after", [False, True])
def test_original_close_failure_cannot_release_or_close_another_scope(tmp_path, monkeypatch, after):
    admission = pool()
    first, second = admission.connection_scope(), admission.connection_scope()
    actual = first.connect(tmp_path / "first.sqlite3")
    other = second.connect(tmp_path / "second.sqlite3")
    close, calls = owner._Connection._driver_close, []

    def refuse(connection):
        if connection is actual:
            calls.append(True)
            if len(calls) == 1:
                if after:
                    close(connection)
                raise InterruptedError("independent SQLite close not confirmed")
        return close(connection)

    monkeypatch.setattr(owner._Connection, "_driver_close", refuse)
    with pytest.raises(InterruptedError):
        first.close()
    assert not first.received and not second.received and admission.status.connections == 2
    cursor = other.execute("SELECT 29")
    assert cursor.fetchone() == (29,)
    cursor.close()
    first.close()
    assert len(calls) == 2 and first.received and not second.received
    second.close()
    assert second.received and admission.status.connections == 0


def test_receiving_refuses_an_actual_executing_constructor_and_independent_work_progresses(
    tmp_path, monkeypatch
):
    admission = pool()
    first, second = admission.connection_scope(), admission.connection_scope()
    entered, finish = Event(), Event()
    connect = owner._connect

    def held(database, **options):
        if str(database).endswith("first.sqlite3"):
            entered.set()
            if not finish.wait(10):
                raise RuntimeError("fixture driver was not released")
        return connect(database, **options)

    monkeypatch.setattr(owner, "_connect", held)
    with ThreadPoolExecutor(max_workers=1) as worker:
        future = worker.submit(first.connect, tmp_path / "first.sqlite3", check_same_thread=False)
        try:
            assert entered.wait(10)
            with pytest.raises(SqliteCleanupPending, match="still active"):
                first.close()
            assert not first.received
            other = second.connect(tmp_path / "second.sqlite3")
            cursor = other.execute("SELECT 31")
            assert cursor.fetchone() == (31,)
            cursor.close()
            second.close()
            assert second.received and not first.received
        finally:
            finish.set()
            actual = future.result(timeout=10)
    assert admission.status.operations == 0
    first.close()
    assert actual.cleanup_complete and first.received


def test_unknown_unreturned_scope_stays_charged_without_driver_redispatch(tmp_path):
    code = """
from pathlib import Path
import sys
import sqlite_admission as owner
from sqlite_admission import SqliteConnectionAdmission, SqliteCleanupPending
root=Path(sys.argv[1])
admission=SqliteConnectionAdmission(connection_limit=2,cursor_limit=8,operation_limit=2,
                                  verify_process=lambda:None)
unknown=admission.connection_scope()
connect=owner._connect
calls=[]
def lose(*args,**options):
 calls.append(True)
 raise InterruptedError("driver result unknown before any returned connection")
owner._connect=lose
try:unknown.connect(root/"unknown.sqlite3")
except InterruptedError:pass
else:raise AssertionError("unknown returned")
owner._connect=connect
for _ in range(17):
 try:unknown.close()
 except SqliteCleanupPending:pass
 else:raise AssertionError("unknown received")
 assert not unknown.received and admission.status.connections==1
assert len(calls)==1
independent=admission.connection_scope()
db=independent.connect(root/"independent.sqlite3")
rows=db.execute("SELECT 37");assert rows.fetchone()==(37,);rows.close()
independent.close()
assert independent.received and not unknown.received
assert admission.status.connections==1
# Process exit cleans only this fixture; no in-process receipt is invented.
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(tmp_path)], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr[-2800:]


@pytest.mark.parametrize("entry", ["admission", "scope"])
def test_public_options_cannot_dispatch_or_replace_a_retained_scope_ticket(
    tmp_path, monkeypatch, entry
):
    admission = pool()
    scope = admission.connection_scope()
    scope.reserve()
    original = owner._connect
    calls = []

    def observe(*args, **options):
        calls.append(True)
        return original(*args, **options)

    monkeypatch.setattr(owner, "_connect", observe)
    connect = admission.connect if entry == "admission" else scope.connect
    with pytest.raises(ValueError, match="original scope"):
        connect(tmp_path / "forged.sqlite3", _reservation=scope._lease)
    assert not calls and not list(tmp_path.iterdir())
    assert admission.status.connections == 1 and not scope.received
    actual = scope.connect(tmp_path / "original.sqlite3")
    assert len(calls) == 1
    scope.close()
    assert actual.cleanup_complete and scope.received
