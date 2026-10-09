"""Actual cooperative exclusion and native consumer containment after owner death."""

import fcntl
import os
import subprocess
import sys
import threading
import time

import pytest
from stove0_extension_support import ExecutionOwner
from stove0_extension_support.file_admission import FileExecutionAdmission


def _owner(name="a"):
    return ExecutionOwner(name * 64, 1, "fixture-process", "fixture-probe")


def test_external_lock_defers_bounded_probes_then_grants_exact_reservation(tmp_path):
    path = tmp_path / "resource.lock"
    with path.open("w+b") as external, FileExecutionAdmission(path) as admission:
        fcntl.flock(external, fcntl.LOCK_EX | fcntl.LOCK_NB)
        started = time.monotonic()
        for _ in range(100):
            assert admission.probe(_owner(), deadline=time.monotonic() + 0.1) is None
        assert time.monotonic() - started < 0.5
        fcntl.flock(external, fcntl.LOCK_UN)
        permit = admission.probe(_owner(), deadline=time.monotonic() + 0.1)
        assert permit is not None
        assert admission.probe(_owner(), deadline=time.monotonic() + 0.1) is permit
        assert admission.probe(_owner("b"), deadline=time.monotonic() + 0.1) is None
        permit.activate(threading.Event(), deadline=time.monotonic() + 0.1)
        with pytest.raises(BlockingIOError):
            fcntl.flock(external, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="consumers still own"):
            admission.close()
        permit.release(deadline=time.monotonic() + 0.1)
        permit.release(deadline=time.monotonic() + 0.1)
        fcntl.flock(external, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_deadline_and_cancellation_leave_no_activated_consumer(tmp_path):
    with FileExecutionAdmission(tmp_path / "resource.lock") as admission:
        with pytest.raises(TimeoutError):
            admission.probe(_owner(), deadline=time.monotonic() - 1)
        permit = admission.probe(_owner(), deadline=time.monotonic() + 1)
        assert permit is not None
        canceled = threading.Event()
        canceled.set()
        with pytest.raises(InterruptedError):
            permit.activate(canceled, deadline=time.monotonic() + 1)
        with pytest.raises(TimeoutError):
            permit.activate(threading.Event(), deadline=time.monotonic() - 1)
        permit.release(deadline=time.monotonic() + 1)


def test_lease_rejects_symlink_and_non_regular_file_without_waiting(tmp_path):
    real = tmp_path / "real"
    real.touch()
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(OSError):
        FileExecutionAdmission(link)
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    started = time.monotonic()
    with pytest.raises(ValueError, match="regular local file"):
        FileExecutionAdmission(fifo)
    assert time.monotonic() - started < 0.5


def test_resource_stays_reserved_until_native_consumer_is_contained_after_hard_kill(tmp_path):
    resource = tmp_path / "resource.lock"
    ready = tmp_path / "consumer.pid"
    state = tmp_path / "state"
    state.mkdir()
    tool = (
        "import os,signal,time; from pathlib import Path; "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        f"Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(60)"
    )
    body = (
        "with consumer_scope(owner,permit=permit):\n"
        f" subprocess.Popen([sys.executable, '-c', {tool!r}])\n"
        " time.sleep(60)"
    )
    program = (
        "import sys,threading,time; from pathlib import Path; "
        "from stove0_extension_support import ExclusiveStateOwner,ExecutionOwner; "
        "from stove0_extension_support.file_admission import FileExecutionAdmission; "
        "from stove0_extension_support.consumers import consumer_scope; "
        "from stove0_extension_support import subprocess; "
        f"owner=ExclusiveStateOwner(Path({str(state)!r})); "
        f"admission=FileExecutionAdmission(Path({str(resource)!r})); "
        "permit=admission.probe(ExecutionOwner('a'*64,1,'process','probe'),"
        "deadline=time.monotonic()+1); "
        "permit.activate(threading.Event(),deadline=time.monotonic()+1); "
        f"exec({body!r})"
    )
    parent = subprocess.Popen(
        [sys.executable, "-c", program], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
    )
    try:
        deadline = time.monotonic() + 8
        while not ready.exists():
            assert parent.poll() is None, parent.communicate()[1]
            assert time.monotonic() < deadline
            threading.Event().wait(0.02)
        pid = int(ready.read_text())
        parent.kill()
        parent.wait(timeout=5)
        with FileExecutionAdmission(resource) as replacement:
            assert replacement.probe(_owner("b"), deadline=time.monotonic() + 1) is None
            deadline = time.monotonic() + 8
            while True:
                permit = replacement.probe(_owner("b"), deadline=time.monotonic() + 1)
                if permit is not None:
                    with pytest.raises(ProcessLookupError):
                        os.kill(pid, 0)
                    permit.release(deadline=time.monotonic() + 1)
                    break
                assert time.monotonic() < deadline
                threading.Event().wait(0.02)
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait(timeout=5)
        assert parent.stderr is not None
        parent.stderr.close()
