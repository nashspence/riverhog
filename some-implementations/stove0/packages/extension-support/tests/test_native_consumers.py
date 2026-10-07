from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from stove0_extension_support import ExclusiveStateOwner
from stove0_extension_support import subprocess as native
from stove0_extension_support.consumers import consumer_scope

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Linux component consumer rail")


def _wait_file(path: Path) -> None:
    deadline = time.monotonic() + 5
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert path.exists()


def _gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def test_native_report_retains_real_stdout_stderr_and_exit_status(tmp_path: Path) -> None:
    owner = ExclusiveStateOwner(tmp_path)
    try:
        with consumer_scope(owner):
            result = native.run(
                [
                    sys.executable,
                    "-c",
                    "import sys; print('facts'); print('tool', file=sys.stderr)",
                ],
                capture_output=True,
                text=True,
                check=True,
                timeout=2,
            )
        assert result.stdout == "facts\n" and result.stderr == "tool\n"
        assert result.returncode == 0
    finally:
        owner.close()


def test_timeout_stops_native_consumer_before_returning_to_resource_owner(tmp_path: Path) -> None:
    pid_file = tmp_path / "consumer.pid"
    command = [
        sys.executable,
        "-c",
        "import os,time,pathlib,sys; "
        "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(300)",
        str(pid_file),
    ]
    owner = ExclusiveStateOwner(tmp_path)
    try:
        with consumer_scope(owner), pytest.raises(subprocess.TimeoutExpired):
            native.run(command, capture_output=True, timeout=0.5)
        assert _gone(int(pid_file.read_text()))
    finally:
        owner.close()


def test_lease_cancellation_stops_an_actual_blocking_tool_without_payload_cooperation(
    tmp_path: Path,
) -> None:
    pid_file = tmp_path / "consumer.pid"
    owner = ExclusiveStateOwner(tmp_path)
    canceled = threading.Event()

    def execute():
        with consumer_scope(owner, canceled):
            return native.run(
                [
                    sys.executable,
                    "-c",
                    "import os,pathlib,sys,time; "
                    "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(300)",
                    str(pid_file),
                ],
                capture_output=True,
            )

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            running = pool.submit(execute)
            _wait_file(pid_file)
            canceled.set()
            result = running.result(timeout=3)
            assert result.returncode != 0
            assert _gone(int(pid_file.read_text()))
    finally:
        canceled.set()
        owner.close()


def test_parent_crash_does_not_reallocate_component_ownership_before_child_stops(
    tmp_path: Path,
) -> None:
    child_pid = tmp_path / "consumer.pid"
    heartbeat = tmp_path / "heartbeat"
    child = (
        "import os,signal,time,pathlib,sys; "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); "
        "p=pathlib.Path(sys.argv[2]); "
        "exec('while True:\\n p.write_text(str(time.time_ns()))\\n time.sleep(0.02)')"
    )
    parent = (
        "import pathlib,sys,time; "
        "from stove0_extension_support import ExclusiveStateOwner; "
        "from stove0_extension_support.consumers import consumer_scope; "
        "from stove0_extension_support import subprocess; "
        "owner=ExclusiveStateOwner(pathlib.Path(sys.argv[1])); "
        "exec('with consumer_scope(owner):\\n subprocess.Popen([sys.executa"
        'ble, "-c", sys.argv[2], sys.argv[3], sys.argv[4]])\\n time.sleep(300)\')'
    )
    process = subprocess.Popen(
        [sys.executable, "-c", parent, str(tmp_path), child, str(child_pid), str(heartbeat)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_file(child_pid)
        _wait_file(heartbeat)
        pid = int(child_pid.read_text())
        process.kill()
        process.wait(timeout=2)
        with pytest.raises(RuntimeError, match="active owner"):
            ExclusiveStateOwner(tmp_path)
        deadline = time.monotonic() + 8
        while not _gone(pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert _gone(pid)
        # The child stops before the supervisor finishes reaping and closes
        # its inherited owner handle. Observe that completion, rather than
        # requiring the asynchronous cleanup to finish in the same instant.
        while True:
            try:
                owner = ExclusiveStateOwner(tmp_path)
                break
            except RuntimeError as exc:
                assert "active owner" in str(exc)
                assert time.monotonic() < deadline
                time.sleep(0.01)
        owner.close()
        last = heartbeat.read_text()
        time.sleep(0.1)
        assert heartbeat.read_text() == last
    finally:
        if process.poll() is None:
            process.kill()
        stdout, stderr = process.communicate(timeout=10)
        assert not stdout and not stderr, (stdout, stderr)
