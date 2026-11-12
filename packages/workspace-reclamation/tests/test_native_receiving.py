"""Actual native outcomes retain an extent attempt through receiving."""

from __future__ import annotations

import errno
import json
import os
import subprocess
import sys
from threading import Event, Thread

import pytest
import workspace_reclamation as receiving
from workspace_reclamation import InvalidWorkspaceFiles, WorkspaceReclamationCapacity

from tests.support.retained_resource import fault_native as fault_native

pytestmark = pytest.mark.skipif(
    sys.platform not in {"linux", "darwin"}, reason="native POSIX owner backend"
)


@pytest.fixture
def workspace(tmp_path):
    directory = tmp_path / "retired"
    directory.mkdir(mode=0o700)
    data = directory / "data"
    data.write_bytes(b"abcdefgh")
    data.chmod(0o600)
    return directory, data


@pytest.fixture
def owner(monkeypatch, fault_native):
    native, _path = fault_native
    monkeypatch.setattr(receiving, "Resource", native.Resource)
    monkeypatch.setattr(receiving, "Borrow", native.Borrow)
    return receiving.FileReclamationOwner()


def test_new_owner_has_no_native_effect_and_explicitly_receives(owner):
    assert owner._directory.metrics["open_calls"] == owner._file.metrics["open_calls"] == 0
    assert not owner.received
    owner.close()
    owner.close()
    assert owner.received
    assert owner._directory.metrics["close_calls"] == owner._file.metrics["close_calls"] == 0


def test_public_extent_entry_requires_the_retained_original_before_effects(workspace, owner):
    directory, data = workspace
    with pytest.raises(TypeError):
        receiving.reclaim_retired_files(directory, names=("data",), octet_budget=3)
    with pytest.raises(TypeError, match="retained original receiver"):
        receiving.reclaim_retired_files(
            directory, receiver=object(), names=("data",), octet_budget=3
        )
    assert data.read_bytes() == b"abcdefgh"
    assert owner._file.metrics["open_calls"] == owner._directory.metrics["open_calls"] == 0
    result = receiving.reclaim_retired_files(
        directory, receiver=owner, names=("data",), octet_budget=3
    )
    assert owner.received and result.removed_octets == 3 and data.read_bytes() == b"abcde"
    with pytest.raises(RuntimeError, match="fresh retained owner"):
        receiving.reclaim_retired_files(directory, receiver=owner, names=("data",), octet_budget=3)
    assert data.read_bytes() == b"abcde"


@pytest.mark.parametrize("budget", [1, 4, 8, 17])
def test_progress_follows_actual_receiving_and_close_never_repeats_disposal(
    workspace, owner, budget
):
    directory, data = workspace
    result = owner.reclaim(directory, names=("data",), octet_budget=budget)
    assert result.removed_octets == min(8, budget)
    assert result.files_reclaimed == (budget >= 8)
    assert owner.received
    for resource in (owner._directory, owner._file):
        assert resource.metrics["open_calls"] == resource.metrics["close_calls"] == 1
    if data.exists():
        assert data.read_bytes() == b"abcdefgh"[: 8 - budget]
    owner.close()
    with pytest.raises(RuntimeError, match="fresh retained owner"):
        owner.reclaim(directory, names=("data",), octet_budget=budget)
    assert owner._directory.metrics["close_calls"] == owner._file.metrics["close_calls"] == 1


@pytest.mark.parametrize("slot", ["_directory", "_file"])
@pytest.mark.parametrize("fault", ["after_open", "signal_after_open", "stat_refusal"])
def test_interrupted_acquisition_keeps_the_original_resource_before_any_mutation(
    workspace, owner, slot, fault
):
    directory, data = workspace
    resource = getattr(owner, slot)
    resource._test_fault(fault)
    with pytest.raises((RuntimeError, KeyboardInterrupt, OSError)):
        owner.reclaim(directory, names=("data",), octet_budget=3)
    assert not owner.received and not resource.received
    assert resource.state in {"live", "rejected"}
    assert resource.metrics["open_calls"] == 1 and resource.metrics["close_calls"] == 0
    assert data.read_bytes() == b"abcdefgh"
    owner.close()
    assert owner.received and resource.metrics["close_calls"] == 1


@pytest.mark.parametrize("slot", ["_directory", "_file"])
def test_known_close_refusal_keeps_originals_and_retry_only_receives(workspace, owner, slot):
    directory, data = workspace
    resource = getattr(owner, slot)
    resource._test_fault("before_close")
    with pytest.raises(RuntimeError):
        owner.reclaim(directory, names=("data",), octet_budget=3)
    assert not owner.received and resource.state == "live"
    assert resource.metrics["close_calls"] == 0
    assert data.read_bytes() == b"abcde"
    if slot == "_file":
        assert owner._directory_borrow.active
        assert owner._directory.metrics["close_calls"] == 0
    owner.close()
    assert owner.received
    assert data.read_bytes() == b"abcde"
    assert owner._file.metrics["close_calls"] == owner._directory.metrics["close_calls"] == 1


@pytest.mark.parametrize("slot", ["_directory", "_file"])
@pytest.mark.parametrize("same_inode", [False, True])
@pytest.mark.parametrize("fault", ["after_close", "signal_after_close"])
def test_lost_close_reply_never_dispatches_to_a_reused_descriptor(
    workspace, owner, monkeypatch, slot, same_inode, fault
):
    directory, data = workspace
    resource = getattr(owner, slot)
    resource._test_fault(fault)
    original = os.ftruncate
    descriptors = {}

    def truncate(descriptor, extent):
        descriptors["_file"] = descriptor
        descriptors["_directory"] = owner._directory_borrow.fileno()
        return original(descriptor, extent)

    monkeypatch.setattr(os, "ftruncate", truncate)
    with pytest.raises((RuntimeError, KeyboardInterrupt)):
        owner.reclaim(directory, names=("data",), octet_budget=3)
    assert not owner.received and resource.received
    old = descriptors[slot]
    unrelated = directory.parent / "unrelated"
    unrelated.write_bytes(b"independent")
    path = (directory if slot == "_directory" else data) if same_inode else unrelated
    fresh = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
    if fresh != old:
        os.dup2(fresh, old, inheritable=False)
        os.close(fresh)
    try:
        for _ in range(5):
            owner.close()
        os.fstat(old)
        assert owner.received and data.read_bytes() == b"abcde"
        assert resource.metrics["close_calls"] == 1
        if not same_inode:
            assert os.read(old, 100) == b"independent"
    finally:
        os.close(old)


@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT])
def test_uncertain_sync_keeps_borrows_until_explicit_original_receiving(
    workspace, owner, monkeypatch, code
):
    directory, data = workspace

    def failed(_descriptor):
        raise OSError(code, "injected storage synchronization refusal")

    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", failed)
        with pytest.raises(WorkspaceReclamationCapacity):
            owner.reclaim(directory, names=("data",), octet_budget=3)
    assert data.read_bytes() == b"abcde"
    assert owner._file_borrow.active and owner._directory_borrow.active
    assert owner._file.metrics["close_calls"] == owner._directory.metrics["close_calls"] == 0
    owner.close()
    assert owner.received and data.read_bytes() == b"abcde"


def test_replaced_original_file_is_rejected_without_mutating_either_inode(
    workspace, owner, monkeypatch
):
    directory, data = workspace
    files = receiving._files
    calls = 0

    def replaced(descriptor, names):
        nonlocal calls
        result = files(descriptor, names)
        calls += 1
        if calls == 1:
            data.rename(directory / "original")
            data.write_bytes(b"replacement")
            data.chmod(0o600)
        return result

    monkeypatch.setattr(receiving, "_files", replaced)
    with pytest.raises(InvalidWorkspaceFiles, match="replaced"):
        owner.reclaim(directory, names=("data",), octet_budget=3)
    assert not owner.received and owner._file_borrow.active
    assert data.read_bytes() == b"replacement"
    assert (directory / "original").read_bytes() == b"abcdefgh"
    owner.close()
    assert owner.received


def test_active_executor_keeps_its_exact_borrows_until_actual_io_exits(
    workspace, owner, monkeypatch
):
    directory, data = workspace
    entered, release = Event(), Event()
    truncate = os.ftruncate
    outcomes = []

    def paused(descriptor, extent):
        entered.set()
        assert release.wait(10), "test did not release the actual executor"
        return truncate(descriptor, extent)

    def run():
        try:
            outcomes.append(owner.reclaim(directory, names=("data",), octet_budget=3))
        except BaseException as error:
            outcomes.append(error)

    monkeypatch.setattr(os, "ftruncate", paused)
    worker = Thread(target=run)
    worker.start()
    try:
        assert entered.wait(5)
        with pytest.raises(BlockingIOError, match="executor"):
            owner.close()
        assert not owner.received and worker.is_alive()
        assert owner._file_borrow.active and owner._directory_borrow.active
        assert owner._file.metrics["close_calls"] == owner._directory.metrics["close_calls"] == 0
    finally:
        release.set()
        worker.join(10)
    assert not worker.is_alive()
    assert len(outcomes) == 1 and not isinstance(outcomes[0], BaseException)
    assert owner.received and data.read_bytes() == b"abcde"


@pytest.mark.parametrize("slot", ["_directory", "_file"])
@pytest.mark.parametrize("fault", ["unknown_open", "unknown_close_open", "unknown_close_closed"])
def test_unknown_native_outcome_preserves_original_dependency_without_retry(
    workspace, fault_native, slot, fault
):
    directory, _data = workspace
    _native, path = fault_native
    program = r"""
import importlib.util,json,os,sys
import workspace_reclamation as receiving
spec=importlib.util.spec_from_file_location('_retained_resource_test._native',sys.argv[1])
native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
receiving.Resource=native.Resource;receiving.Borrow=native.Borrow
owner=receiving.FileReclamationOwner()
resource=getattr(owner,sys.argv[3]);resource._test_fault(sys.argv[4])
try:owner.reclaim(__import__('pathlib').Path(sys.argv[2]),names=('data',),octet_budget=1)
except (RuntimeError,OSError):pass
else:raise AssertionError('false progress')
assert resource.state=='unknown' and not resource.received and not owner.received
before=resource.metrics.copy()
for _ in range(5):
 try:owner.close()
 except RuntimeError:pass
 else:raise AssertionError('false receiving')
assert resource.metrics==before and not owner.received
if sys.argv[3]=='_file':
 assert owner._directory.state=='live' and owner._directory_borrow.active
 assert owner._directory.metrics['children']==1
 assert owner._directory.metrics['close_calls']==0
print(json.dumps({'unknown':True,'received':owner.received,'close_calls':before['close_calls']}))
# Exiting only cleans up this test process; it grants no product receiving.
"""
    compile(program, "<native extent unknown witness>", "exec")
    result = subprocess.run(
        [sys.executable, "-c", program, str(path), str(directory), slot, fault],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    value = json.loads(result.stdout)
    assert value == {
        "unknown": True,
        "received": False,
        "close_calls": 1 if fault == "unknown_close_closed" else 0,
    }
