from __future__ import annotations

import ctypes
import os
import subprocess
import sys
from ctypes import wintypes
from types import SimpleNamespace
from unittest.mock import Mock

import a_gogurt_windows_listener as windows
import pytest
from gogurt_listener_runtime.platform import ListenerPlatformError


def _kernel(
    monkeypatch: pytest.MonkeyPatch,
    *,
    handle: int = 1 << 40,
    wait: int = 0,
    error: int = 6,
    close: int = 1,
) -> SimpleNamespace:
    kernel = SimpleNamespace(
        OpenProcess=Mock(return_value=handle),
        WaitForSingleObject=Mock(return_value=wait),
        CloseHandle=Mock(return_value=close),
    )
    loader = Mock(return_value=kernel)
    monkeypatch.setattr(ctypes, "WinDLL", loader, raising=False)
    monkeypatch.setattr(ctypes, "get_last_error", lambda: error, raising=False)
    monkeypatch.setattr(
        ctypes, "WinError", lambda code: OSError(code, "native probe error"), raising=False
    )
    return kernel


@pytest.mark.parametrize(("wait", "expected"), [(0, False), (0x102, True)])
def test_process_proof_preserves_pointer_width_and_closes_handle(
    monkeypatch: pytest.MonkeyPatch,
    wait: int,
    expected: bool,
) -> None:
    kernel = _kernel(monkeypatch, wait=wait)
    assert windows.TaskSchedulerUserAdapter.process_is_running(123) is expected
    assert kernel.OpenProcess.restype is wintypes.HANDLE
    assert kernel.OpenProcess.argtypes == (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    assert kernel.WaitForSingleObject.argtypes == (wintypes.HANDLE, wintypes.DWORD)
    assert kernel.CloseHandle.argtypes == (wintypes.HANDLE,)
    kernel.OpenProcess.assert_called_once_with(0x00100000, False, 123)
    kernel.WaitForSingleObject.assert_called_once_with(1 << 40, 0)
    kernel.CloseHandle.assert_called_once_with(1 << 40)


@pytest.mark.parametrize("wait", [0xFFFFFFFF, 0x80, 19])
def test_failed_or_unknown_wait_never_proves_dead(
    monkeypatch: pytest.MonkeyPatch,
    wait: int,
) -> None:
    kernel = _kernel(monkeypatch, wait=wait)
    with pytest.raises(OSError):
        windows.TaskSchedulerUserAdapter.process_is_running(123)
    kernel.CloseHandle.assert_called_once_with(1 << 40)


@pytest.mark.parametrize(("error", "expected"), [(87, False), (5, True)])
def test_classified_open_process_results(
    monkeypatch: pytest.MonkeyPatch,
    error: int,
    expected: bool,
) -> None:
    kernel = _kernel(monkeypatch, handle=0, error=error)
    assert windows.TaskSchedulerUserAdapter.process_is_running(123) is expected
    kernel.WaitForSingleObject.assert_not_called()
    kernel.CloseHandle.assert_not_called()


def test_unknown_open_and_failed_close_are_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    _kernel(monkeypatch, handle=0, error=8)
    with pytest.raises(OSError) as failure:
        windows.TaskSchedulerUserAdapter.process_is_running(123)
    assert failure.value.errno == 8
    _kernel(monkeypatch, close=0)
    with pytest.raises(OSError):
        windows.TaskSchedulerUserAdapter.process_is_running(123)


@pytest.mark.parametrize("pid", [False, True, 0, -1, 1 << 32, 1.5, "1"])
def test_invalid_pid_never_reaches_native_api(monkeypatch: pytest.MonkeyPatch, pid: object) -> None:
    kernel = _kernel(monkeypatch)
    with pytest.raises(ValueError):
        windows.TaskSchedulerUserAdapter.process_is_running(pid)
    kernel.OpenProcess.assert_not_called()


def test_only_identity_is_cached_and_only_for_one_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = windows.TaskSchedulerUserAdapter()
    commands: list[str] = []
    states = iter(["4", "3"])

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        script = command[-1]
        commands.append(script)
        output = "S-1-5-21-123" if "WindowsIdentity" in script else next(states)
        return subprocess.CompletedProcess(command, 0, output, "")

    monkeypatch.setattr(adapter, "_run", run)
    assert adapter.status(None).running is True
    assert adapter.status(None).running is False
    assert sum("WindowsIdentity" in command for command in commands) == 1
    assert sum("Schedule.Service" in command for command in commands) == 2
    other = windows.TaskSchedulerUserAdapter()
    monkeypatch.setattr(
        other, "_run", lambda *_a, **_kw: subprocess.CompletedProcess([], 0, "S-1-5-21-456", "")
    )
    assert other._current_user_sid() == "S-1-5-21-456"
    assert adapter._current_user_sid() == "S-1-5-21-123"


def test_invalid_identity_is_not_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = windows.TaskSchedulerUserAdapter()
    values = iter(["bad SID", "S-1-5-21-123"])
    monkeypatch.setattr(
        adapter, "_run", lambda *_a, **_kw: subprocess.CompletedProcess([], 0, next(values), "")
    )
    with pytest.raises(ListenerPlatformError):
        adapter._current_user_sid()
    assert adapter._current_user_sid() == "S-1-5-21-123"


@pytest.mark.skipif(sys.platform != "win32", reason="actual Windows process HANDLE proof")
def test_native_windows_live_and_exited_process() -> None:
    adapter = windows.TaskSchedulerUserAdapter()
    assert adapter.process_is_running(os.getpid()) is True
    with subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"]) as child:
        try:
            assert adapter.process_is_running(child.pid) is True
        finally:
            child.terminate()
            child.wait(timeout=5)
        assert adapter.process_is_running(child.pid) is False
