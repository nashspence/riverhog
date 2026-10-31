"""Public process admission and actual schema/pager connection composition."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import closing
from types import SimpleNamespace

import pytest
import riverhog_client
from riverhog_client import (
    SqliteOperationBank,
    SqliteOperationGrants,
    SqlitePrimaryLimits,
    WindowsSqliteProcess,
)
from riverhog_client import _prepared_primary_application as application
from sqlite_admission import SqliteConnectionAdmission


@pytest.fixture
def process(monkeypatch):
    events = []

    class Budget:
        def report(self):
            events.append("report")
            return {"heap_limit_bytes": 1048576}

        def native_report(self):
            events.append("native")
            return {"owners": 1, "files": 2}

        def increase(self, amount):
            events.append(("heap", amount))

        def increase_native(self, limits):
            events.append(("population", limits))

    budget = Budget()
    selected = [budget]
    native = SimpleNamespace(current_process_budget=lambda: selected[0])
    monkeypatch.setattr(riverhog_client, "_prepared_primary_native", native, raising=False)

    def admit(heap, *, native_limits):
        events.append(("admit", heap, native_limits))
        return budget

    monkeypatch.setattr(application, "_admit_process_budget", admit)
    owner = WindowsSqliteProcess(heap_bytes=1048576, native_limits=(2, 8))
    return owner, budget, selected, events


def test_public_process_owns_one_admission_and_explicit_increases(process):
    owner, _budget, _selected, events = process
    assert owner.report() == {"heap_limit_bytes": 1048576}
    assert owner.native_report() == {"owners": 1, "files": 2}
    owner.increase_heap(2 * 1048576)
    owner.increase_native((3, 9))
    assert events == [
        ("admit", 1048576, (2, 8)),
        "report",
        "report",
        "native",
        "report",
        ("heap", 2 * 1048576),
        "report",
        ("population", (3, 9)),
    ]


@pytest.mark.parametrize("action", ["verify", "report", "native", "heap", "population", "path"])
def test_foreign_process_owner_cannot_open_or_change_policy(process, tmp_path, action):
    owner, _budget, selected, events = process
    selected[0] = object()
    calls = {
        "verify": owner.verify,
        "report": owner.report,
        "native": owner.native_report,
        "heap": lambda: owner.increase_heap(2 * 1048576),
        "population": lambda: owner.increase_native((3, 9)),
        "path": lambda: owner.primary_location(tmp_path, b"exact allocation"),
    }
    with pytest.raises(RuntimeError, match="different owner"):
        calls[action]()
    assert events == [("admit", 1048576, (2, 8))]
    assert not list(tmp_path.iterdir())


def test_process_verification_precedes_an_actual_ordinary_connection(process):
    owner, _budget, selected, events = process
    pool = SqliteConnectionAdmission(
        operation_limit=8, connection_limit=1, cursor_limit=4, verify_process=owner.verify
    )
    with closing(pool.connect(":memory:")) as db:
        with closing(db.execute("SELECT 17")) as cursor:
            assert cursor.fetchone() == (17,)
        assert events == [("admit", 1048576, (2, 8)), "report"]
    selected[0] = object()
    with pytest.raises(RuntimeError, match="different owner"):
        pool.connect(":memory:")
    assert pool.status.connections == pool.status.cursors == 0


@pytest.mark.parametrize("initialize", [True, False])
def test_public_primary_factory_preserves_exact_process_and_connection_policy(
    process, tmp_path, monkeypatch, initialize
):
    from riverhog_client import _prepared_primary_sqlite_native as native

    owner, budget, _selected, events = process
    limits = SqlitePrimaryLimits(4096, 8192, 4096)
    bank = SqliteOperationBank(*(1,) * 10)
    operations = SqliteOperationGrants(bank, bank)
    connection = object()
    calls = []

    def connect(*args, **options):
        raise AssertionError("factory construction must not open a pager")

    def opened(path, **options):
        calls.append((path, options))
        return connection

    monkeypatch.setattr(native, "open_native", opened)
    factory = owner.primary_factory(
        limits=limits,
        operations=operations,
        retry_profile=(2, 3),
        connection_factory=connect,
    )
    location = owner.primary_location(tmp_path, b"exact allocation")
    assert location.parent == tmp_path and not location.exists()
    assert (
        factory(location, allocation_descriptor=b"exact allocation", initialize=initialize)
        is connection
    )
    assert calls == [
        (
            location,
            dict(
                allocation_descriptor=b"exact allocation",
                limits=limits,
                operations=operations,
                process=budget,
                retry_profile=(2, 3),
                initialize=initialize,
                connection_factory=connect,
            ),
        )
    ]
    assert events == [("admit", 1048576, (2, 8)), "report", "report"]


def test_primary_factory_requires_application_connection_admission(process):
    owner, _budget, _selected, events = process
    bank = SqliteOperationBank(*(1,) * 10)
    with pytest.raises(TypeError, match="connection admission"):
        owner.primary_factory(
            limits=SqlitePrimaryLimits(4096, 4096, 4096),
            operations=SqliteOperationGrants(bank, bank),
            retry_profile=(2, 3),
            connection_factory=None,
        )
    assert events == [("admit", 1048576, (2, 8))]


@pytest.mark.skipif(
    os.name != "posix", reason="actual schema/pager composition uses the Unix VFS fixture"
)
def test_schema_cold_verification_and_replacement_share_actual_connection_admission(tmp_path):
    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles
    from riverhog_client._prepared_primary_sqlite_native import start

    from tests.support.prepared_primary import SqlitePrimaryTestGuard

    class Guard(SqlitePrimaryTestGuard):
        def arm_operation(self, *args, **kwargs):
            assert pool.status.connections == 1  # Only the independent consumer remains.

    verified = []
    pool = SqliteConnectionAdmission(
        operation_limit=8,
        connection_limit=2,
        cursor_limit=32,
        blob_limit=1,
        verify_process=lambda: verified.append(True),
    )

    class Factory:
        def __bool__(self):
            return False

        def __call__(self, *args, **options):
            return pool.connect(*args, **options)

    connection_factory = Factory()
    path = tmp_path / "primary.sqlite3"
    path.touch()
    bank = SqliteOperationBank(*(1 << 24,) * 10)
    operations = SqliteOperationGrants(bank, bank)
    limits = SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024)
    with closing(pool.connect(":memory:")) as other:
        with closing(other.execute("SELECT 29")) as suspended:
            for initialize in (True, False):
                guard = Guard(path)
                with TransactionalPrimaryFiles(
                    guard,
                    allocation_descriptor=b"exact original",
                    limits=limits,
                    operations=operations,
                    initialize=initialize,
                    connection_factory=connection_factory,
                ) as files:
                    assert pool.status.connections == 2
                    if initialize:
                        original = files.put_bytes("original", [b"untouched"], byte_limit=9)
                    before = len(verified)
                    start(files, guard)
                    assert len(verified) == before + 1
                    assert pool.status.connections == 2
                    with files.verified(original) as reader:
                        assert reader.read() == b"untouched"
                assert pool.status.connections == 1
                assert pool.status.cursors == 1
            assert suspended.fetchone() == (29,)
    assert pool.status.connections == pool.status.cursors == 0
    assert pool.status.connection_peak == 2 and len(verified) >= 7


@pytest.mark.skipif(os.name != "nt", reason="qualified loaded Windows SQLite engine")
def test_public_startup_enforces_one_heap_across_actual_admitted_connections():
    code = r"""
import json, sqlite3
from contextlib import closing
from riverhog_client import WindowsSqliteProcess
from sqlite_admission import SqliteConnectionAdmission
limit=2*1024*1024
process=WindowsSqliteProcess(heap_bytes=limit,native_limits=(2,8))
pool=SqliteConnectionAdmission(operation_limit=8,connection_limit=2,cursor_limit=8,verify_process=process.verify)
with closing(pool.connect(":memory:")) as first, closing(pool.connect(":memory:")) as second:
    first.execute("CREATE TABLE original(value)").close()
    first.execute("INSERT INTO original VALUES ('exact original')").close()
    first.commit()
    second.execute("CREATE TABLE holding(value)").close()
    second.execute("INSERT INTO holding VALUES (zeroblob(?))",(limit//4,)).close()
    second.commit()
    try:
        first.execute("SELECT randomblob(?)",(limit,)).fetchone()
    except (MemoryError,sqlite3.Error) as error:
        if isinstance(error,sqlite3.Error): assert error.sqlite_errorcode&255==sqlite3.SQLITE_NOMEM
    else: raise AssertionError("actual aggregate H was not enforced")
    with closing(first.execute("SELECT value FROM original")) as cursor:
        assert cursor.fetchone()==('exact original',)
    assert process.report()['sqlite_heap_peak_bytes']<=limit
    try: WindowsSqliteProcess(heap_bytes=limit*2,native_limits=(2,8))
    except RuntimeError: pass
    else: raise AssertionError("a second application H owner was accepted")
    process.increase_heap(limit*2)
    with closing(first.execute("SELECT length(randomblob(?))",(limit,))) as cursor:
        assert cursor.fetchone()==(limit,)
    assert process.report()['sqlite_heap_peak_bytes']<=limit*2
assert pool.status.connections==pool.status.cursors==0
assert pool.status.connection_peak==2
assert process.native_report()['owners']==0
print(json.dumps({'passed':True,'connections':pool.status.connection_peak,'heap_limit_bytes':process.report()['heap_limit_bytes']}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "passed": True,
        "connections": 2,
        "heap_limit_bytes": 4 * 1024**2,
    }


@pytest.mark.skipif(os.name != "posix", reason="actual rollback/reopen uses the Unix VFS fixture")
@pytest.mark.parametrize("after", [False, True])
def test_primary_barrier_blob_uncertainty_keeps_database_and_guard_until_explicit_close(
    tmp_path, monkeypatch, after
):
    import sqlite_admission as admission
    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles
    from sqlite_admission import SqliteCleanupPending

    from tests.support.prepared_primary import SqlitePrimaryTestGuard

    pool = SqliteConnectionAdmission(
        operation_limit=8,
        connection_limit=1,
        cursor_limit=32,
        blob_limit=1,
        verify_process=lambda: None,
    )
    path = tmp_path / "primary.sqlite3"
    path.touch()
    limits = SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024)
    guard = SqlitePrimaryTestGuard(path)
    files = TransactionalPrimaryFiles(
        guard,
        allocation_descriptor=b"exact original",
        limits=limits,
        initialize=True,
        connection_factory=pool.connect,
    )
    original = files.put_bytes("original", [b"untouched"], byte_limit=9)
    close, attempts = admission._Blob._driver_close, []

    def uncertain(blob):
        attempts.append(True)
        if len(attempts) == 1:
            if after:
                close(blob)
            raise OSError("barrier blob close unconfirmed")
        close(blob)

    monkeypatch.setattr(admission._Blob, "_driver_close", uncertain)
    with pytest.raises((OSError, SqliteCleanupPending)):
        files.sync()
    assert len(attempts) == 1
    assert pool.status.blobs == pool.status.connections == 1 and not guard.closed
    with pytest.raises(SqliteCleanupPending, match="unconfirmed"):
        files.close()
    assert len(attempts) == 1 and not guard.closed
    files.close()
    assert len(attempts) == 2 and guard.closed
    assert pool.status.blobs == pool.status.connections == 0
    with TransactionalPrimaryFiles(
        SqlitePrimaryTestGuard(path),
        allocation_descriptor=b"exact original",
        limits=limits,
        initialize=False,
        connection_factory=pool.connect,
    ) as reopened:
        with reopened.verified(original) as reader:
            assert reader.read() == b"untouched"
    assert pool.status.blobs == pool.status.connections == 0
