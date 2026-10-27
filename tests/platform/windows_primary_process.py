"""Native subprocess witnesses; no credentials or machine configuration changes."""

from __future__ import annotations

import _sqlite3
import argparse
import ctypes
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
import traceback
from ctypes import wintypes
from pathlib import Path

from riverhog_client import PrimaryStateBusy, PrimaryStateError
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits, TransactionalPrimaryFiles
from riverhog_client._prepared_primary_types import PrimaryByteFile
from riverhog_client._prepared_primary_windows_native import (
    _SecurityAttributes,
    _WindowsAPI,
    windows_container_path,
)

_DWORD, _BOOL, _PTR = ctypes.c_uint32, ctypes.c_int32, ctypes.c_void_p
_HANDLE = wintypes.HANDLE
ALLOCATION = b"native Windows transaction witness allocation"
LIMITS = SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024)
CONTENT = b"original\x00" * 1900


class _Startup(ctypes.Structure):
    _fields_ = [
        ("cb", _DWORD),
        ("reserved", wintypes.LPWSTR),
        ("desktop", wintypes.LPWSTR),
        ("title", wintypes.LPWSTR),
        ("x", _DWORD),
        ("y", _DWORD),
        ("width", _DWORD),
        ("height", _DWORD),
        ("columns", _DWORD),
        ("rows", _DWORD),
        ("fill", _DWORD),
        ("flags", _DWORD),
        ("show", ctypes.c_uint16),
        ("reserved_bytes", ctypes.c_uint16),
        ("reserved_buffer", _PTR),
        ("stdin", _HANDLE),
        ("stdout", _HANDLE),
        ("stderr", _HANDLE),
    ]


class _Process(ctypes.Structure):
    _fields_ = [("process", _HANDLE), ("thread", _HANDLE), ("pid", _DWORD), ("tid", _DWORD)]


def _elevated(api: _WindowsAPI) -> bool:
    token, value, size = _HANDLE(), _DWORD(), _DWORD()
    api._required(
        api.security.OpenProcessToken(api.kernel.GetCurrentProcess(), 8, ctypes.byref(token)),
        "test process token",
    )
    try:
        api._required(
            api.security.GetTokenInformation(
                token, 20, ctypes.byref(value), ctypes.sizeof(value), ctypes.byref(size)
            ),
            "test token elevation",
        )
        return bool(value.value)
    finally:
        api.close(token.value)


def run_unprivileged(arguments: list[str], *, cwd: Path, timeout_seconds: int) -> int:
    """Launch the same user with NORMALUSER restrictions, then verify in the child."""
    api = _WindowsAPI()
    if not _elevated(api):
        return subprocess.run(arguments, cwd=cwd, timeout=timeout_seconds, check=False).returncode
    p = ctypes.POINTER
    a, k = api.security, api.kernel
    bindings = [
        (a, "SaferCreateLevel", _BOOL, [_DWORD, _DWORD, _DWORD, p(_HANDLE), _PTR]),
        (a, "SaferComputeTokenFromLevel", _BOOL, [_HANDLE, _HANDLE, p(_HANDLE), _DWORD, _PTR]),
        (a, "SaferCloseLevel", _BOOL, [_HANDLE]),
        (
            a,
            "CreateProcessAsUserW",
            _BOOL,
            [
                _HANDLE,
                wintypes.LPCWSTR,
                wintypes.LPWSTR,
                _PTR,
                _PTR,
                _BOOL,
                _DWORD,
                _PTR,
                wintypes.LPCWSTR,
                p(_Startup),
                p(_Process),
            ],
        ),
        (k, "WaitForSingleObject", _DWORD, [_HANDLE, _DWORD]),
        (k, "GetExitCodeProcess", _BOOL, [_HANDLE, p(_DWORD)]),
        (k, "TerminateProcess", _BOOL, [_HANDLE, _DWORD]),
    ]
    for library, name, result, parameters in bindings:
        function = getattr(library, name)
        function.restype, function.argtypes = result, parameters
    level, token = _HANDLE(), _HANDLE()
    api._required(
        a.SaferCreateLevel(1, 0x20000, 1, ctypes.byref(level), None), "test NORMALUSER level"
    )
    try:
        api._required(
            a.SaferComputeTokenFromLevel(level, None, ctypes.byref(token), 0, None),
            "test NORMALUSER token",
        )
    finally:
        a.SaferCloseLevel(level)
    try:
        info = _Startup()
        info.cb = ctypes.sizeof(info)
        process = _Process()
        command = ctypes.create_unicode_buffer(subprocess.list2cmdline(arguments))
        api._required(
            a.CreateProcessAsUserW(
                token,
                arguments[0],
                command,
                None,
                None,
                False,
                0,
                None,
                str(cwd),
                ctypes.byref(info),
                ctypes.byref(process),
            ),
            "test standard-user spawn",
        )
        try:
            waited = k.WaitForSingleObject(process.process, timeout_seconds * 1000)
            if waited != 0:
                k.TerminateProcess(process.process, 90)
                k.WaitForSingleObject(process.process, 10000)
                raise AssertionError(f"standard-user witness did not finish ({waited})")
            result = _DWORD()
            api._required(
                k.GetExitCodeProcess(process.process, ctypes.byref(result)), "test process outcome"
            )
            return result.value
        finally:
            api.close(process.thread)
            api.close(process.process)
    finally:
        api.close(token)


def _parent(path: Path) -> None:
    api = _WindowsAPI()
    api.kernel.CreateDirectoryW.argtypes = [wintypes.LPCWSTR, _PTR]
    api.kernel.CreateDirectoryW.restype = _BOOL
    descriptor = api.private_descriptor(directory=True)
    attributes = _SecurityAttributes(ctypes.sizeof(_SecurityAttributes), descriptor, False)
    try:
        api._required(
            api.kernel.CreateDirectoryW(str(path), ctypes.byref(attributes)),
            "test provisioned parent",
        )
    finally:
        api.kernel.LocalFree(descriptor)


def _files(path: Path, *, initialize: bool, allocation: bytes = ALLOCATION):
    return TransactionalPrimaryFiles.windows(
        path, allocation_descriptor=allocation, limits=LIMITS, initialize=initialize
    )


def _crash(path: Path, when: str) -> None:
    with _files(path, initialize=True, allocation=ALLOCATION + when.encode()) as files:
        select = files._select

        def boundary(generation, result):
            if when == "before":
                os._exit(71)
            select(generation, result)
            os._exit(72)

        files._select = boundary
        files.put("journal", (CONTENT[:5000], CONTENT[5000:]), byte_limit=len(CONTENT))
    raise AssertionError("process-death boundary was not reached")


def witness(root: Path) -> dict:
    api = _WindowsAPI()
    assert not _elevated(api), "native qualification must run in a non-elevated process"
    started = time.monotonic()
    cases = []
    _parent(root / "private")
    parent = root / "private"
    file = PrimaryByteFile("journal", len(CONTENT), hashlib.sha256(CONTENT).hexdigest())
    path = windows_container_path(parent, ALLOCATION)
    with _files(path, initialize=True) as files:
        with _files(
            windows_container_path(parent, ALLOCATION + b"independent"),
            initialize=True,
            allocation=ALLOCATION + b"independent",
        ):
            pass
        result = files.put("journal", (CONTENT[:1], CONTENT[1:]), byte_limit=len(CONTENT))
        assert result == file
        with files.verified(result) as stream:
            stream.seek(4090)
            assert stream.read(20) == CONTENT[4090:4110]
        try:
            files.put("journal", [b"replacement"], byte_limit=100)
        except FileExistsError:
            pass
        else:
            raise AssertionError("immutable winner was replaced")
        result = subprocess.run(
            [sys.executable, __file__, "--busy", str(path)], timeout=60, check=False
        )
        assert result.returncode == 42, result.returncode
        cases.extend(
            [
                "fresh-container",
                "multichunk-exact-replay",
                "immutable-winner",
                "independent-allocation",
                "competing-process-live-owner",
            ]
        )
    with _files(path, initialize=False) as files:
        with files.verified(file) as stream:
            assert stream.read() == CONTENT
    cases.append("cold-reopen-exact-winner")
    for when in ("before", "after"):
        allocation = ALLOCATION + when.encode()
        candidate = windows_container_path(parent, allocation)
        # Each terminated process explicitly initializes a genuinely new container.
        result = subprocess.run(
            [sys.executable, __file__, "--crash", str(candidate), "--when", when],
            timeout=120,
            check=False,
        )
        assert result.returncode == (71 if when == "before" else 72), result.returncode
        with _files(candidate, initialize=False, allocation=allocation) as files:
            assert files.exists("journal") is (when == "after")
            if when == "after":
                with files.verified(file) as stream:
                    assert stream.read() == CONTENT
                assert files.recover_step()
            else:
                count = 0
                while not files.recover_step():
                    count += 1
                assert count == (len(CONTENT) + 4095) // 4096
        cases.append("fresh-process-death-" + when + "-selection")
    linked = parent / "linked.sqlite3"
    os.link(path, linked)
    try:
        try:
            _files(path, initialize=False)
        except PrimaryStateError:
            pass
        else:
            raise AssertionError("multiple database links were admitted")
    finally:
        linked.unlink()
    cases.append("hardlink-rejection")
    permissive = root / "unadmitted"
    permissive.mkdir()
    try:
        _files(windows_container_path(permissive, ALLOCATION), initialize=True)
    except PrimaryStateError:
        pass
    else:
        raise AssertionError("unadmitted sidecar parent was accepted")
    cases.append("unadmitted-parent-rejection")
    with sqlite3.connect(":memory:") as runtime:
        source_id = runtime.execute("SELECT sqlite_source_id()").fetchone()[0]
        compile_options = [row[0] for row in runtime.execute("PRAGMA compile_options")]
    return {
        "kind": "native-unprivileged-ntfs-prepared-primary-first-slice",
        "cases": cases,
        "passed": len(cases),
        "elevated": False,
        "python": sys.version,
        "windows": sys.getwindowsversion().build,
        "sqlite_version": sqlite3.sqlite_version,
        "sqlite_source_id": source_id,
        "sqlite_compile_options": compile_options,
        "python_executable_sha256": hashlib.sha256(Path(sys.executable).read_bytes()).hexdigest(),
        "sqlite_extension_sha256": hashlib.sha256(Path(_sqlite3.__file__).read_bytes()).hexdigest(),
        "vfs": "win32",
        "elapsed_seconds": time.monotonic() - started,
        "limitations": [
            "process death is not power loss",
            "producer activation is not yet qualified",
            "VFS fault injection and physical control reserves remain open",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--witness", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--crash", type=Path)
    parser.add_argument("--when", choices=("before", "after"))
    parser.add_argument("--busy", type=Path)
    args = parser.parse_args()
    if args.busy:
        try:
            _files(args.busy, initialize=False)
        except PrimaryStateBusy:
            return 42
        return 43
    if args.crash:
        _crash(args.crash, args.when)
    try:
        result = witness(args.witness)
    except BaseException:
        args.report.write_text(json.dumps({"failed": True, "traceback": traceback.format_exc()}))
        return 1
    args.report.write_text(json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
