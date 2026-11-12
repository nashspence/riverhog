from __future__ import annotations

import os
import subprocess
import sys

import pytest
from riverhog_core import journal_validation_workspace as kernel
from riverhog_core import journal_validation_workspace_reference as references
from riverhog_core import retained_workspace_handles as handles
from riverhog_core.journal_validation_workspace_lease import ValidationWorkspaceLease

from tests.support.retained_resource import fault_native as fault_native


@pytest.mark.parametrize("fault", ["before_open", "after_open", "signal_after_open"])
def test_actual_compiled_duplicate_reply_loss_keeps_original_custody(
    tmp_path, monkeypatch, fault_native, fault
):
    native, _ = fault_native
    monkeypatch.setattr(handles, "Resource", native.Resource)
    monkeypatch.setattr(handles, "Borrow", native.Borrow)
    frame = ValidationWorkspaceLease()
    retained, custody = [], []
    root = tmp_path / "original"

    def retain(original):
        retained.append(original)
        assert original._resource.owner.state == "new"
        original._resource.owner._test_fault(fault)

    try:
        with kernel.lease_journal_validation_workspace(
            root, "a" * 32, receiver=frame, create=True, on_custody=custody.append
        ):
            expected = KeyboardInterrupt if fault == "signal_after_open" else RuntimeError
            with pytest.raises(expected):
                references.retain_validation_workspace(custody[0], on_reference=retain)
            assert len(retained) == 1 and retained[0].closed
            assert retained[0]._resource.owner.received
            assert retained[0]._resource.owner.metrics["dup_calls"] == int(fault != "before_open")
            assert custody[0].verify_current()
            assert frame.directory.owner.metrics["children"] == 1
        frame.close()
        assert frame.received
    finally:
        for original in retained:
            original.close()
        frame.close()


@pytest.mark.parametrize("after", [False, True])
def test_actual_compiled_duplicate_close_receiving_never_closes_a_reused_slot(
    tmp_path, monkeypatch, fault_native, after
):
    native, _ = fault_native
    monkeypatch.setattr(handles, "Resource", native.Resource)
    monkeypatch.setattr(handles, "Borrow", native.Borrow)
    frame = ValidationWorkspaceLease()
    retained, custody = [], []
    root = tmp_path / "original"
    replacement = None
    try:
        with kernel.lease_journal_validation_workspace(
            root, "a" * 32, receiver=frame, create=True, on_custody=custody.append
        ):
            original = references.retain_validation_workspace(
                custody[0], on_reference=retained.append
            )
            descriptor = original._resource.fileno()
        assert not frame.received
        original._resource.owner._test_fault("after_close" if after else "before_close")
        with pytest.raises(RuntimeError):
            original.close()
        assert not original.closed and retained == [original]
        if after:
            replacement = os.open(tmp_path / "replacement", os.O_RDWR | os.O_CREAT, 0o600)
            if replacement != descriptor:
                os.dup2(replacement, descriptor, inheritable=False)
            identity = os.fstat(descriptor)
        original.close()
        assert original.closed and original._resource.owner.received
        assert original._resource.owner.metrics["close_calls"] == 1
        frame.close()
        assert frame.received
        if replacement is not None:
            assert os.fstat(descriptor) == identity
        original.close()
        assert original._resource.owner.metrics["close_calls"] == 1
    finally:
        for original in retained:
            original.close()
        frame.close()
        if replacement is not None:
            if replacement != descriptor:
                os.close(descriptor)
            os.close(replacement)


@pytest.mark.parametrize("fault", ["unknown_open", "unknown_close_open", "unknown_close_closed"])
def test_actual_unknown_duplicate_retains_parent_and_refuses_reuse_until_process_exit(
    tmp_path, fault_native, fault
):
    _, extension = fault_native
    program = r"""
import importlib.util,os,sys
from pathlib import Path
from riverhog_core import journal_validation_workspace as kernel
from riverhog_core import journal_validation_workspace_reference as references
from riverhog_core import retained_workspace_handles as handles
from riverhog_core.journal_validation_workspace_lease import ValidationWorkspaceLease
spec=importlib.util.spec_from_file_location('_retained_resource_test._native',sys.argv[1])
native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
handles.Resource,handles.Borrow=native.Resource,native.Borrow
root=Path(sys.argv[2])/'original';fault=sys.argv[3]
frame=ValidationWorkspaceLease();retained=[];custody=[];old=None
def retain(original):
 retained.append(original)
 if fault=='unknown_open':original._resource.owner._test_fault(fault)
try:
 with kernel.lease_journal_validation_workspace(
  root,'a'*32,receiver=frame,create=True,on_custody=custody.append):
  reference=references.retain_validation_workspace(custody[0],on_reference=retain)
  old=reference._resource.fileno()
except RuntimeError:
 assert fault=='unknown_open'
else:
 assert fault!='unknown_open'
 reference._resource.owner._test_fault(fault)
 try:reference.close()
 except (RuntimeError,OSError):pass
 else:raise AssertionError('unknown duplicate close became received')
reference=retained[0]
assert not reference.closed and reference._resource.owner.state=='unknown'
assert not frame.received and frame.directory.owner.metrics['children']>=1
try:frame.close()
except BlockingIOError:pass
else:raise AssertionError('original namespace lost its unknown child dependency')
replacement=None
if fault=='unknown_close_closed':
 replacement=os.open(root/('a'*32)/'.lease',os.O_RDWR|os.O_NOFOLLOW)
 if replacement!=old:os.dup2(replacement,old,inheritable=False)
 identity=os.fstat(old)
before=[item.owner.metrics for item in frame.records]+[reference._resource.owner.metrics]
for _ in range(17):
 try:reference.close()
 except RuntimeError:pass
 else:raise AssertionError('unknown duplicate was received on retry')
 try:frame.close()
 except BlockingIOError:pass
 else:raise AssertionError('unknown child dependency was removed')
 assert [item.owner.metrics for item in frame.records]+[
  reference._resource.owner.metrics]==before
 if replacement is not None:assert os.fstat(old)==identity
independent=ValidationWorkspaceLease()
with kernel.lease_journal_validation_workspace(
 root/'independent','b'*32,receiver=independent,create=True) as path:
 assert path is not None
independent.close();assert independent.received
if replacement is not None:
 if replacement!=old:os.close(old)
 os.close(replacement)
# This isolated process owns fixture disposal; it is not positive product receiving.
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", program, str(extension), str(tmp_path), fault],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (result.stdout + result.stderr)[-2800:]
