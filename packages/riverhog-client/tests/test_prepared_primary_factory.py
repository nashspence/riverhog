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
