"""Allocation authority precedes physical storage, with exact owned-adapter replay."""

from __future__ import annotations

import os

import pytest
from riverhog_client import PrimaryScope, PrimaryStore, PrimaryStoreLimits
from riverhog_client._prepared_primary_factory import posix_primary_files
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits, TransactionalPrimaryFiles
from riverhog_client._prepared_primary_types import PrimaryStateCapacity

from tests.support.prepared_primary import SqlitePrimaryTestGuard
from tests.support.prepared_primary_replay import ALLOCATION, DESCRIPTOR, LIMITS, SCOPE, witness


@pytest.mark.parametrize(
    "changes,error",
    [
        ({"allocation_id": ""}, ValueError),
        ({"scope": {"service": "", "account": "fixture"}}, ValueError),
        ({"initialize": 1}, ValueError),
        (
            {
                "limits": PrimaryStoreLimits(
                    pending_bytes=1024, total_bytes=1024, preparation_bytes=1024, control_bytes=1
                )
            },
            PrimaryStateCapacity,
        ),
    ],
)
def test_invalid_allocation_never_opens_or_initializes_storage(tmp_path, changes, error):
    def forbidden(*_args, **_kwargs):
        pytest.fail("invalid allocation reached physical storage")

    arguments = dict(
        allocation_id=ALLOCATION.allocation_id,
        scope=SCOPE,
        limits=LIMITS,
        initialize=True,
        storage_factory=forbidden,
    )
    arguments.update(changes)
    with pytest.raises(error):
        PrimaryStore(tmp_path / "absent", **arguments)
    assert not (tmp_path / "absent").exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX and Unix-VFS construction witnesses")
@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_shared_store_factory_receives_exact_original_allocation(tmp_path, backend):
    calls = []

    def opened(path, *, allocation_descriptor, initialize):
        assert allocation_descriptor == DESCRIPTOR
        calls.append(initialize)
        if backend == "posix":
            return posix_primary_files(
                path, allocation_descriptor=allocation_descriptor, initialize=initialize
            )
        if initialize:
            path.touch(exist_ok=False)
        return TransactionalPrimaryFiles(
            SqlitePrimaryTestGuard(path),
            allocation_descriptor=allocation_descriptor,
            limits=SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024),
            initialize=initialize,
        )

    witness(tmp_path.resolve() / ("state" if backend == "posix" else "state.sqlite3"), opened)
    assert calls == [True, False, False, False]


def test_factory_failure_preserves_original_allocation_authority(tmp_path):
    calls = []

    def unavailable(path, *, allocation_descriptor, initialize):
        calls.append((path, allocation_descriptor, initialize))
        raise FileNotFoundError("missing original state")

    path = tmp_path / "missing"
    with pytest.raises(FileNotFoundError, match="original state"):
        PrimaryStore(
            path,
            allocation_id=ALLOCATION.allocation_id,
            scope=PrimaryScope(**SCOPE.model_dump()),
            limits=LIMITS,
            initialize=False,
            storage_factory=unavailable,
        )
    assert calls == [(path, DESCRIPTOR, False)]
    assert not path.exists()


@pytest.mark.parametrize("kind", ["database", "journal"])
@pytest.mark.parametrize("observed", [True, -1, 1.5, 4097])
def test_cold_extent_admission_precedes_every_sqlite_connection_and_schema_read(
    tmp_path, monkeypatch, kind, observed
):
    from riverhog_client import PrimaryStateError
    from riverhog_client import _prepared_primary_sqlite as owner
    from riverhog_client import _prepared_primary_windows_native as windows
    from riverhog_client._prepared_primary_types import PrimaryFileSpace

    def forbidden(*_args, **_kwargs):
        pytest.fail("unadmitted cold extent reached SQLite or StateSchema")

    monkeypatch.setattr(owner, "_engine_factory", forbidden)

    class Guard:
        path = tmp_path / "retained.sqlite3"
        closed = False
        observations = 0

        def file_space(self):
            self.observations += 1
            sizes = {"database": 0, "journal": 0}
            sizes[kind] = observed
            return tuple(PrimaryFileSpace(sizes[name], 8192) for name in ("database", "journal"))

        def close(self):
            self.closed = True

    guard = Guard()
    path = windows.windows_container_path(tmp_path, b"retained allocation")
    monkeypatch.setattr(windows, "WindowsContainer", lambda *_args, **_kwargs: guard)
    error = PrimaryStateCapacity if observed == 4097 else PrimaryStateError
    with pytest.raises(error, match="cold recovery|cold extent"):
        TransactionalPrimaryFiles.windows(
            path,
            allocation_descriptor=b"retained allocation",
            limits=SqlitePrimaryLimits(4096, 4096, 4096),
            initialize=False,
        )
    assert guard.closed and guard.observations == 1


@pytest.mark.parametrize("initialize", [True, False])
@pytest.mark.parametrize(
    "state", ["unconfigured", "admitted", "selected", "initial", "quarantined"]
)
def test_native_factory_selects_cold_bank_and_reconciles_retry_policy_before_sql(
    tmp_path, monkeypatch, initialize, state
):
    from types import SimpleNamespace

    import riverhog_client
    from riverhog_client import PrimaryStateError
    from riverhog_client import _prepared_primary_runtime as runtime
    from riverhog_client import _prepared_primary_sqlite as owner
    from riverhog_client import _prepared_primary_sqlite_operations as operations
    from riverhog_client import _prepared_primary_windows_native as windows
    from riverhog_client._prepared_primary_factory import WindowsPrimaryFactory
    from riverhog_client._prepared_primary_types import PrimaryFileSpace

    events = []

    class Budget:
        def report(self):
            events.append("process")

        def retry_report(self):
            events.append("retry")
            return {
                "state": state if state in ("unconfigured", "quarantined") else "admitted",
                "selected_retry_count": 3 if state == "selected" else 2,
                "selected_delay_milliseconds": 3,
                "qualified_initial_retry_count": 9 if state == "initial" else 10,
                "qualified_initial_delay_milliseconds": 25,
            }

    budget = Budget()

    class Gate:
        def __init__(self, *_args):
            events.append("gate")

        def admit_retries(self, initial, selected, bootstrap):
            events.append(("admit", initial, selected, bootstrap))

    native = SimpleNamespace(
        SqliteProcessBudget=Budget,
        current_process_budget=lambda: budget,
        retry_bootstrap_cost=lambda pair: (17, 23),
        WriteVfs=Gate,
    )
    monkeypatch.setattr(riverhog_client, "_prepared_primary_native", native, raising=False)
    monkeypatch.setattr(runtime, "_qualified_runtime", lambda: {"sqlite_source_id": "qualified"})

    class Guard:
        def __init__(self, *_args, **_kwargs):
            events.append("metadata")

        def file_space(self):
            events.append("extent")
            return PrimaryFileSpace(0, 0), PrimaryFileSpace(0, 0)

        def close(self):
            events.append("close")

    monkeypatch.setattr(windows, "NativeWindowsContainer", Guard)

    def bank(_grants, _limits, *, recovering):
        events.append(("bank", recovering))
        return SimpleNamespace(reads=object(), callbacks=object())

    monkeypatch.setattr(operations, "native_bank", bank)

    def sql(*_args):
        events.append("sql")
        raise RuntimeError("SQL boundary observed")

    monkeypatch.setattr(owner, "_engine_factory", sql)
    bank_values = operations.SqliteOperationBank(*(1,) * 10)
    grants = operations.SqliteOperationGrants(bank_values, bank_values)
    factory = WindowsPrimaryFactory(SqlitePrimaryLimits(4096, 4096, 4096), grants, budget, (2, 3))
    path = windows.windows_container_path(tmp_path, b"native fixture")
    if state in ("selected", "initial", "quarantined"):
        with pytest.raises(PrimaryStateError, match="retry admission differs"):
            factory(path, allocation_descriptor=b"native fixture", initialize=initialize)
        assert "sql" not in events
    else:
        with pytest.raises(RuntimeError, match="SQL boundary observed"):
            factory(path, allocation_descriptor=b"native fixture", initialize=initialize)
        assert events.index("retry") < events.index("sql")
    assert events.index(("bank", not initialize)) < events.index("metadata")
    if not initialize:
        assert events.index("extent") < events.index("retry")
    admissions = [event for event in events if isinstance(event, tuple) and event[0] == "admit"]
    assert len(admissions) == int(state == "unconfigured")
    if admissions:
        assert admissions == [("admit", (10, 25), (2, 3), (17, 23))]
        assert events.index(admissions[0]) < events.index("sql")
    assert events[-1] == "close"


@pytest.mark.parametrize("fault", ["foreign", "subclass"])
def test_native_factory_refuses_unowned_process_before_any_metadata(tmp_path, monkeypatch, fault):
    from types import SimpleNamespace

    import riverhog_client
    from riverhog_client import PrimaryStateError
    from riverhog_client import _prepared_primary_runtime as runtime
    from riverhog_client import _prepared_primary_sqlite_operations as operations
    from riverhog_client import _prepared_primary_windows_native as windows
    from riverhog_client._prepared_primary_factory import WindowsPrimaryFactory

    class Budget:
        pass

    class OtherBudget(Budget):
        pass

    budget = Budget()
    offered = Budget() if fault == "foreign" else OtherBudget()
    native = SimpleNamespace(SqliteProcessBudget=Budget, current_process_budget=lambda: budget)
    monkeypatch.setattr(riverhog_client, "_prepared_primary_native", native, raising=False)
    monkeypatch.setattr(runtime, "_qualified_runtime", lambda: {"sqlite_source_id": "qualified"})

    def forbidden(*_args, **_kwargs):
        pytest.fail("unowned process reached metadata or native bank construction")

    monkeypatch.setattr(windows, "NativeWindowsContainer", forbidden)
    monkeypatch.setattr(operations, "native_bank", forbidden)
    bank = operations.SqliteOperationBank(*(1,) * 10)
    factory = WindowsPrimaryFactory(
        SqlitePrimaryLimits(4096, 4096, 4096),
        operations.SqliteOperationGrants(bank, bank),
        offered,
        (2, 3),
    )
    with pytest.raises(PrimaryStateError, match="current application budget"):
        factory(
            windows.windows_container_path(tmp_path, b"native fixture"),
            allocation_descriptor=b"native fixture",
            initialize=False,
        )


@pytest.mark.skipif(os.name != "posix", reason="POSIX shared-owner execution guard")
def test_shared_preparation_port_refuses_reentry_closed_storage_and_foreign_backend(tmp_path):
    from functools import partial

    from riverhog_client import PrimaryStateBusy, PrimaryStateError
    from riverhog_client._prepared_primary_factory import sqlite_primary_preparation_step
    from riverhog_client._prepared_primary_sqlite_preparation import SqliteReadinessPlan

    store = PrimaryStore(
        tmp_path / "state",
        allocation_id=ALLOCATION.allocation_id,
        scope=SCOPE,
        limits=LIMITS,
        initialize=True,
    )
    called = []

    def execute(storage):
        called.append(storage)
        return False

    try:
        store._authoring = True
        with pytest.raises(PrimaryStateBusy, match="authoring"):
            store.prepare_storage_step(execute)
        assert not called
        store._authoring = False
        before = store.usage
        plan = SqliteReadinessPlan((8201, 16), 65536, 8, 128, 256 * 1024)
        with pytest.raises(PrimaryStateError, match="original SQL root"):
            store.prepare_storage_step(partial(sqlite_primary_preparation_step, plan=plan))
        assert store.usage == before
        for invalid in (None, 0, 1, object()):
            with pytest.raises(PrimaryStateError, match="completion boolean"):
                store.prepare_storage_step(lambda _storage, value=invalid: value)
        assert store.prepare_storage_step(execute) is False
        assert len(called) == 1
    finally:
        store.close()
    with pytest.raises(PrimaryStateError):
        store.prepare_storage_step(execute)
    assert len(called) == 1
