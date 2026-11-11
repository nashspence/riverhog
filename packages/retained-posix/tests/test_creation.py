"""Actual private creation shares the retained acquisition outcome boundary."""

from __future__ import annotations

import os
import stat
import subprocess
import sys

import pytest
from retained_posix import Borrow, Resource
from retained_posix.growth import acquire, receive
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from tests.support.retained_posix import fault_native as fault_native
from tests.support.workspace_reclamation import execution_sample

pytestmark = pytest.mark.skipif(
    sys.platform not in {"linux", "darwin"}, reason="native Linux/Darwin POSIX owner backend"
)


@pytest.mark.parametrize("relative", [False, True])
@pytest.mark.parametrize("exclusive", [False, True])
def test_private_creation_preserves_existing_storage_and_exact_parent(
    tmp_path, relative, exclusive
):
    parent = Resource("directory")
    parent.acquire(os.fsencode(tmp_path))
    owner = Resource("file")
    borrow = Borrow(owner)
    path = b"created" if relative else os.fsencode(tmp_path / "created")
    dependency = parent if relative else None
    try:
        owner.acquire(path, parent=dependency, writable=True, create=True, exclusive=exclusive)
        borrow.acquire()
        assert stat.S_IMODE(os.fstat(borrow.fileno()).st_mode) == 0o600
        os.write(borrow.fileno(), b"original")
        borrow.close()
        assert parent.metrics["children"] == int(relative)
        if relative:
            with pytest.raises(BlockingIOError):
                parent.close()
        owner.close()
        assert parent.metrics["children"] == 0
        existing = Resource("file")
        try:
            existing.acquire(path, parent=dependency, writable=True, create=True)
        finally:
            existing.close()
        rejected = Resource("file")
        try:
            with pytest.raises(FileExistsError):
                rejected.acquire(path, parent=dependency, create=True, exclusive=True)
            assert rejected.state == "absent" and rejected.metrics["open_calls"] == 1
        finally:
            rejected.close()
        assert (tmp_path / "created").read_bytes() == b"original"
        assert owner.metrics["open_calls"] == owner.metrics["close_calls"] == 1
    finally:
        borrow.close()
        owner.close()
        parent.close()


@pytest.mark.parametrize(
    "kind,options",
    [
        ("directory", {"create": True}),
        ("file", {"exclusive": True}),
    ],
)
def test_invalid_creation_mode_refuses_before_effect(tmp_path, kind, options):
    owner = Resource(kind)
    with pytest.raises(RuntimeError):
        owner.acquire(os.fsencode(tmp_path / "absent"), **options)
    assert owner.state == "new" and owner.metrics["open_calls"] == 0
    assert not (tmp_path / "absent").exists()
    owner.close()
    assert owner.received


@pytest.mark.parametrize("exclusive", [False, True])
def test_creation_never_follows_or_replaces_a_symlink(tmp_path, exclusive):
    target = tmp_path / "target"
    target.write_bytes(b"original")
    (tmp_path / "link").symlink_to(target)
    parent = Resource("directory")
    parent.acquire(os.fsencode(tmp_path))
    owner = Resource("file")
    try:
        with pytest.raises(OSError):
            owner.acquire(b"link", parent=parent, writable=True, create=True, exclusive=exclusive)
        assert owner.state == "absent" and parent.metrics["children"] == 0
        assert target.read_bytes() == b"original" and (tmp_path / "link").is_symlink()
    finally:
        owner.close()
        parent.close()


@pytest.mark.parametrize(
    "fault",
    [
        "before_open",
        "after_open",
        "signal_after_open",
        "stat_refusal",
    ],
)
def test_creation_commits_original_native_outcome_before_python_reply(
    tmp_path, fault_native, fault
):
    native, _ = fault_native
    parent = native.Resource("directory")
    parent.acquire(os.fsencode(tmp_path))
    owner = native.Resource("file")
    borrow = native.Borrow(owner)
    try:
        owner._test_fault(fault)
        expected = (
            KeyboardInterrupt
            if fault == "signal_after_open"
            else (OSError if fault == "stat_refusal" else RuntimeError)
        )
        with pytest.raises(expected):
            owner.acquire(b"created", parent=parent, writable=True, create=True, exclusive=True)
        if fault == "before_open":
            assert owner.state == "new" and not (tmp_path / "created").exists()
            assert parent.metrics["children"] == 0 and owner.metrics["open_calls"] == 0
        else:
            assert owner.state == ("rejected" if fault == "stat_refusal" else "live")
            assert parent.metrics["children"] == 1 and owner.metrics["open_calls"] == 1
            assert stat.S_IMODE((tmp_path / "created").stat().st_mode) == 0o600
            with pytest.raises(RuntimeError):
                owner.acquire(b"created", parent=parent, create=True)
            if owner.state == "live":
                borrow.acquire()
                os.fstat(borrow.fileno())
                borrow.close()
            with pytest.raises(BlockingIOError):
                parent.close()
        owner.close()
        owner.close()
        assert owner.received and parent.metrics["children"] == 0
        assert owner.metrics["close_calls"] == int(fault != "before_open")
    finally:
        borrow.close()
        owner.close()
        parent.close()


@pytest.mark.parametrize("fault", ["unknown_open", "unknown_close_open", "unknown_close_closed"])
def test_unknown_creation_or_receiving_preserves_original_dependency_in_child(
    tmp_path, fault_native, fault
):
    _, extension = fault_native
    program = r"""
import importlib.util,os,sys
spec=importlib.util.spec_from_file_location('_retained_posix_test._native',sys.argv[1])
native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
parent=native.Resource('directory');parent.acquire(os.fsencode(sys.argv[2]))
owner=native.Resource('file');borrow=native.Borrow(owner)
fault=sys.argv[3];old=None
if fault=='unknown_open':owner._test_fault(fault)
try:owner.acquire(b'created',parent=parent,writable=True,create=True,exclusive=True)
except RuntimeError:
 assert fault=='unknown_open'
else:
 assert fault!='unknown_open'
 borrow.acquire();old=borrow.fileno();borrow.close();owner._test_fault(fault)
 try:owner.close()
 except OSError:pass
 else:raise AssertionError('unknown native close became received')
assert owner.state=='unknown' and not owner.received and parent.metrics['children']==1
replacement=None
if fault=='unknown_close_closed':
 fresh=os.open(os.path.join(sys.argv[2],'created'),os.O_RDONLY|os.O_NOFOLLOW)
 if fresh!=old:os.dup2(fresh,old,inheritable=False);os.close(fresh)
 replacement=old;identity=os.fstat(old)
before=owner.metrics.copy()
for _ in range(17):
 try:owner.close()
 except RuntimeError:pass
 else:raise AssertionError('unknown original became received')
 try:parent.close()
 except BlockingIOError:pass
 else:raise AssertionError('original parent dependency released')
 assert owner.metrics==before and parent.metrics['children']==1
 if replacement is not None:assert os.fstat(replacement)==identity
if replacement is not None:os.close(replacement)
# Child exit is fixture disposal, never a positive product receiving receipt.
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", program, str(extension), str(tmp_path), fault],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (result.stdout + result.stderr)[-2800:]


@pytest.mark.growth_witness(
    "retained-posix.native-creation-work",
    gates=("unit", "client-platform-qualification"),
)
def test_creation_measures_actual_native_entries_and_full_control_operands(
    tmp_path, record_property
):
    bindings = tuple(growth_binding(operation) for operation in (acquire, receive))
    projection = project_bindings(bindings)
    samples = []
    for backlog in (1, 17, 1025):
        root = tmp_path / str(backlog)
        root.mkdir(mode=0o700)
        for index in range(backlog):
            (root / str(index)).touch()
        parent = Resource("directory")
        parent.acquire(os.fsencode(root))
        try:
            for width in (1, 17, 129):
                for relative in (False, True):
                    for exclusive in (False, True):
                        name = "x" * width
                        target = root / name
                        path = os.fsencode(name if relative else target)
                        owner = Resource("file")
                        for operation in (acquire, receive, *([receive] * 17)):
                            before = owner.metrics
                            if operation is acquire:
                                operation(
                                    owner,
                                    path,
                                    parent=parent if relative else None,
                                    writable=True,
                                    create=True,
                                    exclusive=exclusive,
                                )
                            else:
                                operation(owner)
                            after = owner.metrics
                            metrics = {
                                key: after[key] - before[key]
                                for key in (
                                    "open_calls",
                                    "dup_calls",
                                    "fstat_calls",
                                    "close_calls",
                                    "control_octets",
                                )
                            }
                            binding = growth_binding(operation)
                            sample = execution_sample(
                                binding,
                                projection,
                                {"unrelated_files": backlog, "path_octets": len(path)},
                                metrics,
                                {},
                            )
                            validate_execution_samples(projection, binding.declaration.id, [sample])
                            samples.append(sample)
                        assert owner.received and owner.metrics["close_calls"] == 1
                        target.unlink()
                assert parent.metrics["children"] == 0
        finally:
            parent.close()
    record_property("growth_samples", samples)
