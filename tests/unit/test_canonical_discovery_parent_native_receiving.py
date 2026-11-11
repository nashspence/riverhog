from __future__ import annotations

import os
import subprocess
import sys
import threading

import pytest
from riverhog_core import canonical_discovery_parent_reclamation as owner

from tests.support.retained_posix import fault_native as fault_native
from tests.unit.test_canonical_discovery_parent_reclamation import current, prefix
from tests.unit.test_canonical_discovery_retired_metadata import case as case
from tests.unit.test_canonical_discovery_retired_metadata import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_retired_metadata import original as original
from tests.unit.test_canonical_discovery_retired_metadata import qualified as qualified

pytestmark = pytest.mark.skipif(os.name != "posix", reason="native POSIX parent receiving")


def fault_receiver(case, monkeypatch, fault_native):
    native, _ = fault_native
    monkeypatch.setattr(owner, "Resource", native.Resource)
    monkeypatch.setattr(owner, "Borrow", native.Borrow)
    job = prefix(case)
    return owner.DiscoveryParentRetirement(case.factory, job, case.root)


@pytest.mark.parametrize("same_inode", (False, True))
def test_actual_parent_reply_loss_never_closes_reused_or_reopened_inode(
    case, monkeypatch, fault_native, same_inode
):
    retained = fault_receiver(case, monkeypatch, fault_native)
    target = retained._resources["_root"]
    target._test_fault("after_close")
    close = retained._close_owned
    captured = []

    def receive():
        if not captured:
            captured.append((retained._root, os.fstat(retained._root)))
        return close()

    retained._close_owned = receive
    with pytest.raises(RuntimeError, match="lost close reply"):
        retained.finish()
    assert target.received and not retained._closed
    assert current(case, retained.job.workspace_id) is not None
    if same_inode:
        path = case.root
        flags = os.O_RDONLY | os.O_DIRECTORY
    else:
        path = case.root / "unrelated"
        path.write_bytes(b"preserved independent file")
        flags = os.O_RDONLY
    old, identity = captured[0]
    replacement = os.open(path, flags)
    if replacement != old:
        os.dup2(replacement, old, inheritable=False)
        os.close(replacement)
    try:
        if same_inode:
            check = os.fstat(old)
            assert (check.st_dev, check.st_ino) == (identity.st_dev, identity.st_ino)
        for _ in range(17):
            retained.close()
            os.fstat(old)
        assert target.metrics["close_calls"] == 1
        assert retained._closed
    finally:
        os.close(old)
        if not same_inode:
            path.unlink()


@pytest.mark.parametrize("slot", ("_anchor", "_root", "_coordinator", "_directory", "_lease"))
def test_actual_acquisition_reply_loss_receives_same_original_without_reopen(
    case, monkeypatch, fault_native, slot
):
    retained = fault_receiver(case, monkeypatch, fault_native)
    selected = retained._resources[slot]
    selected._test_fault("after_open")
    with pytest.raises(RuntimeError, match="lost acquisition reply"):
        retained.reclaim()
    assert selected.metrics["open_calls"] == selected.metrics["close_calls"] == 1
    assert retained._closed and all(item.received for item in retained._resources.values())
    assert current(case, retained.job.workspace_id) == retained.job
    retained.close()
    assert selected.metrics["close_calls"] == 1


@pytest.mark.parametrize("slot", ("_root", "_directory", "_lease"))
def test_actual_rejected_native_metadata_remains_owned_until_explicit_receiving(
    case, monkeypatch, fault_native, slot
):
    retained = fault_receiver(case, monkeypatch, fault_native)
    selected = retained._resources[slot]
    selected._test_fault("stat_refusal")
    with pytest.raises(OSError):
        retained.reclaim()
    assert selected.received and selected.metrics["close_calls"] == 1
    assert retained._closed and current(case, retained.job.workspace_id) == retained.job


def test_actual_parent_close_cannot_receive_borrows_while_worker_uses_them(case):
    job = prefix(case)
    retained = owner.DiscoveryParentRetirement(case.factory, job, case.root)
    entered, leave = threading.Event(), threading.Event()
    verify = retained._verify_directory
    results, errors = [], []

    def checking():
        verify()
        if not entered.is_set():
            entered.set()
            assert leave.wait(5)
            os.fstat(retained._directory)

    def worker():
        try:
            results.append(retained.finish())
        except BaseException as error:
            errors.append(error)

    retained._verify_directory = checking
    physical = threading.Thread(target=worker)
    physical.start()
    try:
        assert entered.wait(5)
        with pytest.raises(BlockingIOError, match="execution has not returned"):
            retained.close()
        assert all(item.active for item in retained._borrows.values())
    finally:
        leave.set()
        physical.join(timeout=5)
    assert not physical.is_alive() and not errors
    assert results[0].catalog_received and retained._closed


def test_genuine_unknown_native_close_retains_parent_and_catalog_in_owned_process(
    case, fault_native
):
    job = prefix(case)
    _, extension = fault_native
    program = r"""
import importlib.util,json,os,sys
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob
from riverhog_core import canonical_discovery_parent_reclamation as owner
spec=importlib.util.spec_from_file_location("_retained_posix_test._native",sys.argv[4])
native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
owner.Resource,owner.Borrow=native.Resource,native.Borrow
engine=create_catalog_engine(sys.argv[1]);factory=sessionmaker(engine,expire_on_commit=False)
with factory() as session:job=DiscoveryJob.read(session.get(Row,sys.argv[3]))
retained=owner.DiscoveryParentRetirement(factory,job,Path(sys.argv[2]))
selected=retained._resources["_root"]
selected._test_fault("unknown_close_closed")
close=retained._close_owned;captured=[]
def receiving():
 if not captured:captured.append(retained._root)
 return close()
retained._close_owned=receiving
try:retained.finish()
except InterruptedError:pass
else:raise AssertionError("unknown outcome became namespace/catalog completion")
assert selected.state=="unknown" and not selected.received and not retained._closed
assert retained._resources["_anchor"].state=="live"
assert retained._resources["_anchor"].metrics["children"]==1
other=os.open(sys.argv[2],os.O_RDONLY|os.O_DIRECTORY)
old=captured[0]
if other!=old:os.dup2(other,old,inheritable=False);os.close(other)
try:
 for _ in range(17):
  try:retained.close()
  except RuntimeError:pass
  else:raise AssertionError("unknown original became received")
  os.fstat(old)
 assert selected.metrics["close_calls"]==1
 with factory() as session:
  current=session.get(Row,job.workspace_id)
  assert current is not None and current.state=="directory"
finally:os.close(old);engine.dispose()
print(json.dumps({"unknown":True,"original_parent_dependency_retained":True,
                 "catalog_completion_refused":True,"no_integer_retry":True}))
# Process exit cleans this isolated witness; it is not a product receiving receipt.
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            program,
            str(case.engine.url),
            str(case.root),
            job.workspace_id,
            str(extension),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert '"catalog_completion_refused": true' in result.stdout
    assert current(case, job.workspace_id).state == "directory"
