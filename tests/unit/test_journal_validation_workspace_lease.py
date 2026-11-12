from __future__ import annotations

import fcntl
import os
import subprocess
import sys
from threading import Event, Thread

import pytest
from retained_resource import Resource
from riverhog_core.journal_validation_workspace_lease import ValidationWorkspaceLease
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from tests.support.retained_resource import fault_native as fault_native
from tests.support.workspace_reclamation import execution_sample


def acquired(root):
    root.mkdir(mode=0o700, exist_ok=True)
    selected = ValidationWorkspaceLease()
    selected.begin()
    selected.root.acquire(root)
    selected.coordinator.acquire(
        ".coordinator",
        parent=selected.root,
        writable=True,
        create=True,
    )
    (root / "selected").mkdir(mode=0o700)
    selected.directory.acquire("selected", parent=selected.root)
    with pytest.raises(FileNotFoundError):
        selected.existing_lease.acquire(".lease", parent=selected.directory, writable=True)
    selected.select_created_lease().acquire(
        ".lease",
        parent=selected.directory,
        writable=True,
        create=True,
        exclusive=True,
    )
    fcntl.flock(selected.lease.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    return selected


def lock_available(path):
    descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True
    finally:
        os.close(descriptor)


def test_active_executor_and_last_independent_reference_keep_original_parent(tmp_path):
    original = acquired(tmp_path / "first")
    entered, release, exited = Event(), Event(), Event()

    def work():
        os.fstat(original.lease.fileno())
        entered.set()
        assert release.wait(10)
        os.fstat(original.lease.fileno())
        exited.set()

    thread = Thread(target=work)
    thread.start()
    assert entered.wait(10)
    copies = []
    try:
        with pytest.raises(BlockingIOError, match="body has not exited"):
            original.close()
        assert not original.received and original.lease.owner.metrics["close_calls"] == 0
        for _ in range(2):
            copy = Resource("file")
            copies.append(copy)
            copy.duplicate(original.lease.owner)
        assert original.directory.owner.metrics["children"] == 3
        release.set()
        thread.join(10)
        assert exited.is_set() and not thread.is_alive()
        original.finish()
        assert original.lease.owner.received and not original.received
        assert original.directory.owner.state == original.root.owner.state == "live"
        assert original.directory.owner.metrics["children"] == 2
        assert not lock_available(tmp_path / "first/selected/.lease")
        independent = acquired(tmp_path / "independent")
        independent.finish()
        assert independent.received
        copies[0].close()
        with pytest.raises(BlockingIOError):
            original.close()
        assert not lock_available(tmp_path / "first/selected/.lease")
        copies[1].close()
        original.close()
        original.close()
        assert original.received and lock_available(tmp_path / "first/selected/.lease")
        with pytest.raises(RuntimeError, match="single-attempt"):
            original.begin()
    finally:
        release.set()
        thread.join(10)
        for copy in copies:
            copy.close()
        if original._active:
            original.finish()
        original.close()


def test_creation_selection_requires_definite_original_absence():
    original = ValidationWorkspaceLease()
    original.begin()
    with pytest.raises(RuntimeError, match="definite original absence"):
        original.select_created_lease()
    original.finish()
    assert original.received


@pytest.mark.growth_witness(
    "riverhog-server.live-lease-frame-receiving",
    gates=("unit",),
)
def test_receiving_native_work_is_independent_of_reference_and_namespace_population(
    tmp_path, record_property
):
    operations = (ValidationWorkspaceLease.finish, ValidationWorkspaceLease.close)
    projection = project_bindings(tuple(growth_binding(operation) for operation in operations))
    samples = []
    for backlog in (1, 17, 1025):
        root = tmp_path / str(backlog)
        root.mkdir(mode=0o700)
        for index in range(backlog):
            (root / str(index)).touch()
        for references in (0, 1, 17, 129):
            original = acquired(root / f"references-{references}")
            copies = []
            try:
                for _ in range(references):
                    copy = Resource("file")
                    copies.append(copy)
                    copy.duplicate(original.lease.owner)

                def observe(operation, original=original, backlog=backlog, references=references):
                    keys = (
                        "open_calls",
                        "dup_calls",
                        "fstat_calls",
                        "close_calls",
                        "control_octets",
                    )
                    before = [record.owner.metrics for record in original.records]
                    try:
                        operation(original)
                    except BlockingIOError:
                        assert operation is ValidationWorkspaceLease.close
                        assert original.directory.owner.metrics["children"] > 0
                    after = [record.owner.metrics for record in original.records]
                    metrics = {
                        key: sum(
                            right[key] - left[key]
                            for left, right in zip(before, after, strict=True)
                        )
                        for key in keys
                    }
                    binding = growth_binding(operation)
                    sample = execution_sample(
                        binding,
                        projection,
                        {"unrelated_files": backlog, "independent_references": references},
                        metrics,
                        {},
                    )
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)

                observe(ValidationWorkspaceLease.finish)
                for _ in range(17):
                    observe(ValidationWorkspaceLease.close)
                for copy in copies:
                    copy.close()
                observe(ValidationWorkspaceLease.close)
                assert original.received
                assert sum(record.owner.metrics["close_calls"] for record in original.records) == 4
                assert original.lease.owner.metrics["dup_calls"] == 0
            finally:
                for copy in copies:
                    copy.close()
                if original._active:
                    original.finish()
                original.close()
    record_property("growth_samples", samples)


@pytest.mark.parametrize("target", ["root", "coordinator", "directory", "lease"])
@pytest.mark.parametrize("fault", ["unknown_open", "unknown_close_open", "unknown_close_closed"])
def test_unknown_original_live_frame_preserves_dependencies_and_refuses_reuse(
    tmp_path, fault_native, target, fault
):
    _, extension = fault_native
    program = r"""
import importlib.util,os,sys
from pathlib import Path
from riverhog_core import retained_workspace_handles as handles
from riverhog_core.journal_validation_workspace_lease import ValidationWorkspaceLease
spec=importlib.util.spec_from_file_location('_retained_resource_test._native',sys.argv[1])
native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
handles.Resource,handles.Borrow=native.Resource,native.Borrow
root=Path(sys.argv[2]);target,fault=sys.argv[3:5]
selected=ValidationWorkspaceLease();selected.begin()
original=None;parent=None;old=None;original_path=None;directory=False
def acquire(record,name,path,dependency=None,**kwargs):
 global original,parent,old,original_path,directory
 if name==target:
  original,parent,original_path=record,dependency,path
  directory=name in {'root','directory'}
  record.owner._test_fault(fault)
 record.acquire(path,parent=dependency,**kwargs)
 if name==target:old=record.fileno()
try:
 acquire(selected.root,'root',root)
 acquire(selected.coordinator,'coordinator','.coordinator',selected.root,writable=True,create=True)
 (root/'selected').mkdir(mode=0o700)
 acquire(selected.directory,'directory','selected',selected.root)
 try:selected.existing_lease.acquire('.lease',parent=selected.directory,writable=True)
 except FileNotFoundError:pass
 else:raise AssertionError('initial lease was unexpectedly present')
 acquire(selected.select_created_lease(),'lease','.lease',selected.directory,
         writable=True,create=True,exclusive=True)
except RuntimeError:
 assert fault=='unknown_open'
try:selected.finish()
except (RuntimeError,OSError):pass
else:raise AssertionError('unknown native outcome became received')
assert original is not None and original.owner.state=='unknown'
assert not selected.received and not original.owner.received
if parent is not None:
 assert parent.owner.state=='live' and parent.owner.metrics['children']>=1
replacement=None
if fault=='unknown_close_closed':
 path=Path(original_path) if parent is None else (
  root/original_path if target in {'coordinator','directory'} else root/'selected'/original_path)
 fresh=os.open(path,os.O_NOFOLLOW|(os.O_RDONLY|os.O_DIRECTORY if directory else os.O_RDWR))
 if fresh!=old:os.dup2(fresh,old,inheritable=False);os.close(fresh)
 replacement=old;identity=os.fstat(old)
before=[record.owner.metrics for record in selected.records]
for _ in range(17):
 try:selected.close()
 except RuntimeError:pass
 else:raise AssertionError('unknown original frame became received')
 assert not selected.received
 assert [record.owner.metrics for record in selected.records]==before
 if replacement is not None:assert os.fstat(replacement)==identity
independent=ValidationWorkspaceLease();independent.begin()
(root/'independent').mkdir(mode=0o700)
independent.root.acquire(root/'independent');independent.finish()
assert independent.received
if replacement is not None:os.close(replacement)
# Isolated child exit disposes the fixture, never a positive product receipt.
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", program, str(extension), str(tmp_path), target, fault],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (result.stdout + result.stderr)[-2800:]
