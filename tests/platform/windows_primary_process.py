"""Native witnesses with isolated, ephemeral hosted-CI standard-user setup."""

from __future__ import annotations

import _sqlite3
import argparse
import ctypes
import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import subprocess
import sys
import time
import traceback
from ctypes import wintypes
from pathlib import Path

from riverhog_client import PrimaryStateBusy, PrimaryStateError
from riverhog_client._prepared_primary_factory import WindowsPrimaryFactory
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits, TransactionalPrimaryFiles
from riverhog_client._prepared_primary_types import PrimaryByteFile
from riverhog_client._prepared_primary_windows_native import (
    WindowsContainer,
    _SecurityAttributes,
    _WindowsAPI,
    windows_container_path,
)

_DWORD, _BOOL, _PTR = ctypes.c_uint32, ctypes.c_int32, ctypes.c_void_p
_HANDLE = wintypes.HANDLE
ALLOCATION = b"native Windows transaction witness allocation"
LIMITS = SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024)
CONTENT = b"original\x00" * 1900

# A fresh script process starts in tests/platform; shared witnesses live under tests.
# Append the checkout so installed product imports retain their existing priority.
_REPOSITORY_ROOT = str(Path(__file__).resolve().parents[2])
if _REPOSITORY_ROOT not in sys.path:
    sys.path.append(_REPOSITORY_ROOT)


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


class _UserInfo(ctypes.Structure):
    _fields_ = [
        ("name", wintypes.LPWSTR),
        ("password", wintypes.LPWSTR),
        ("password_age", _DWORD),
        ("privilege", _DWORD),
        ("home", wintypes.LPWSTR),
        ("comment", wintypes.LPWSTR),
        ("flags", _DWORD),
        ("script", wintypes.LPWSTR),
    ]


class _MemberName(ctypes.Structure):
    _fields_ = [("name", wintypes.LPWSTR)]


def _status(value: int, operation: str) -> None:
    if value:
        raise PrimaryStateError(f"native qualification {operation} failed ({value})")


def _token_sid(api: _WindowsAPI, token) -> str:
    size = _DWORD()
    api.security.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
    if not size.value or size.value > 1024 * 1024:
        raise PrimaryStateError("native qualification token identity is unavailable")
    buffer = ctypes.create_string_buffer(size.value)
    api._required(
        api.security.GetTokenInformation(token, 1, buffer, size, ctypes.byref(size)),
        "test account identity",
    )
    return api._sid_text(_PTR.from_buffer(buffer))


def _users_group(api: _WindowsAPI) -> str:
    sid = _PTR()
    api._required(
        api.security.ConvertStringSidToSidW("S-1-5-32-545", ctypes.byref(sid)),
        "test Users group SID",
    )
    try:
        name, domain = ctypes.create_unicode_buffer(257), ctypes.create_unicode_buffer(257)
        name_size, domain_size, kind = _DWORD(len(name)), _DWORD(len(domain)), _DWORD()
        api._required(
            api.security.LookupAccountSidW(
                None,
                sid,
                name,
                ctypes.byref(name_size),
                domain,
                ctypes.byref(domain_size),
                ctypes.byref(kind),
            ),
            "test Users group name",
        )
        return name.value
    finally:
        api.kernel.LocalFree(sid)


def _acl(path: Path, *arguments: str, recursive: bool) -> None:
    command = ["icacls", str(path), *arguments]
    if recursive:
        command.append("/T")
    command.append("/Q")
    result = subprocess.run(command, capture_output=True, check=False)
    _status(result.returncode, "fixture ACL")


def run_unprivileged(
    arguments: list[str],
    *,
    cwd: Path,
    timeout_seconds: int,
    writable_directory: Path,
    report: Path,
) -> int:
    """Use a real standard token; elevated fixture setup belongs only to hosted CI.

    The temporary account has an in-memory random credential, minimum fixture
    grants, and no loaded profile. Runtime code receives no setup privileges.
    The parent waits for actual process exit, removes fixture data as its owner,
    removes its temporary grants and deletes the account.
    """
    api = _WindowsAPI()
    if not _elevated(api):
        return subprocess.run(arguments, cwd=cwd, timeout=timeout_seconds, check=False).returncode
    if (
        os.environ.get("GITHUB_ACTIONS") != "true"
        or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
    ):
        raise PrimaryStateError("run native qualification from a standard-user terminal")
    loader = getattr(ctypes, "WinDLL", None)
    net = loader("netapi32", use_last_error=True)
    p = ctypes.POINTER
    a, k = api.security, api.kernel
    bindings = [
        (net, "NetUserAdd", _DWORD, [wintypes.LPCWSTR, _DWORD, _PTR, p(_DWORD)]),
        (net, "NetUserDel", _DWORD, [wintypes.LPCWSTR, wintypes.LPCWSTR]),
        (
            net,
            "NetLocalGroupAddMembers",
            _DWORD,
            [wintypes.LPCWSTR, wintypes.LPCWSTR, _DWORD, _PTR, _DWORD],
        ),
        (a, "ConvertStringSidToSidW", _BOOL, [wintypes.LPCWSTR, p(_PTR)]),
        (
            a,
            "LookupAccountSidW",
            _BOOL,
            [
                wintypes.LPCWSTR,
                _PTR,
                wintypes.LPWSTR,
                p(_DWORD),
                wintypes.LPWSTR,
                p(_DWORD),
                p(_DWORD),
            ],
        ),
        (
            a,
            "LogonUserW",
            _BOOL,
            [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.LPCWSTR, _DWORD, _DWORD, p(_HANDLE)],
        ),
        (a, "ImpersonateLoggedOnUser", _BOOL, [_HANDLE]),
        (a, "RevertToSelf", _BOOL, []),
        (
            a,
            "CreateProcessWithLogonW",
            _BOOL,
            [
                wintypes.LPCWSTR,
                wintypes.LPCWSTR,
                wintypes.LPCWSTR,
                _DWORD,
                wintypes.LPCWSTR,
                wintypes.LPWSTR,
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
    account = "rh-primary-" + secrets.token_hex(4)
    password = "Aa1!" + secrets.token_urlsafe(28)
    info = _UserInfo(account, password, 0, 1, None, "Ephemeral native qualification", 0x10201, None)
    parameter_error = _DWORD()
    _status(
        net.NetUserAdd(None, 1, ctypes.byref(info), ctypes.byref(parameter_error)),
        "standard account creation",
    )
    token = _HANDLE()
    granted: list[tuple[Path, bool]] = []
    sid = None
    try:
        member = _MemberName(account)
        result = net.NetLocalGroupAddMembers(None, _users_group(api), 3, ctypes.byref(member), 1)
        if result not in (0, 1378):
            _status(result, "standard group membership")
        api._required(
            a.LogonUserW(account, ".", password, 2, 0, ctypes.byref(token)), "test standard logon"
        )
        sid = _token_sid(api, token)
        # Traverse access participates in sharing checks; synchronous handles need S.
        # These grants do not allow listing or reading directory contents.
        ancestors = {
            parent
            for target in (cwd, Path(sys.base_prefix), writable_directory, report)
            for parent in target.parents
        }
        for path in sorted(ancestors, key=lambda item: len(item.parts)):
            granted.append((path, False))
            _acl(path, "/grant", f"*{sid}:(RC,RA,S,X)", recursive=False)
        for path in dict.fromkeys((cwd, Path(sys.base_prefix))):
            granted.append((path, True))
            _acl(path, "/grant", f"*{sid}:(OI)(CI)RX", recursive=True)
        granted.append((writable_directory, True))
        _acl(writable_directory, "/grant", f"*{sid}:(OI)(CI)M", recursive=True)
        report.touch(exist_ok=True)
        granted.append((report, False))
        _acl(report, "/grant", f"*{sid}:W", recursive=False)
        info = _Startup()
        info.cb = ctypes.sizeof(info)
        process = _Process()
        command = ctypes.create_unicode_buffer(subprocess.list2cmdline(arguments))
        environment = {
            key: os.environ[key]
            for key in ("SystemRoot", "WINDIR", "PATH", "COMSPEC", "PATHEXT")
            if key in os.environ
        }
        environment.update(
            TEMP=str(writable_directory),
            TMP=str(writable_directory),
            PYTHONDONTWRITEBYTECODE="1",
            PYTHONUTF8="1",
        )
        block = ctypes.create_unicode_buffer(
            "\0".join(f"{key}={value}" for key, value in sorted(environment.items())) + "\0\0"
        )
        api._required(
            a.CreateProcessWithLogonW(
                account,
                ".",
                password,
                0,
                arguments[0],
                command,
                0x08000400,
                block,
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
                exited = k.WaitForSingleObject(process.process, 10000)
                if exited != 0:
                    raise AssertionError("standard-user witness has not exited")
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
        failures = []
        if token.value:
            if a.ImpersonateLoggedOnUser(token):
                try:
                    private = writable_directory / "private"
                    if private.exists():
                        shutil.rmtree(private)
                except OSError:
                    failures.append("fixture data cleanup")
                finally:
                    if not a.RevertToSelf():
                        os._exit(91)
            else:
                failures.append("fixture owner cleanup token")
            api.close(token)
        if sid is not None:
            for path, recursive in reversed(granted):
                try:
                    _acl(path, "/remove:g", "*" + sid, recursive=recursive)
                except PrimaryStateError:
                    failures.append("fixture grant cleanup")
        if net.NetUserDel(None, account):
            failures.append("standard account cleanup")
        if failures:
            raise PrimaryStateError("native qualification cleanup failed: " + ", ".join(failures))


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
        files.put_bytes("journal", (CONTENT[:5000], CONTENT[5000:]), byte_limit=len(CONTENT))
    raise AssertionError("process-death boundary was not reached")


def _guard_rename_exclusion(path: Path) -> None:
    """Isolate native lifetime pins from SQLite's own data-file handles."""
    locations = (path, path.parent)
    guard = WindowsContainer(path, initialize=False)
    try:
        for original in locations:
            moved = original.with_name(original.name + ".relocated")
            try:
                original.rename(moved)
            except OSError as exc:
                if exc.winerror not in (5, 32):
                    raise
            else:
                moved.rename(original)
                raise AssertionError("live native guard allowed container relocation")
    finally:
        guard.close()
    # Prove fixture permissions permit those same operations once the pins close.
    for original in locations:
        moved = original.with_name(original.name + ".relocated")
        original.rename(moved)
        moved.rename(original)


def _vfs_files(shim, path, allocation, *, initialize):
    class Guard:
        vfs = shim.name

        def __init__(self):
            self.path = path
            self.native = WindowsContainer(path, initialize=initialize)

        def check_current(self):
            self.native.check_current()

        def check_journal(self, *, required, recovering=False):
            self.native.check_journal(required=required, recovering=recovering)

        def file_space(self):
            return self.native.file_space()

        def close(self):
            self.native.close()

    return TransactionalPrimaryFiles(
        Guard(), allocation_descriptor=allocation, limits=LIMITS, initialize=initialize
    )


def _vfs_selection_fault(shim, parent: Path, operation: str, kind: str, when: str):
    from dataclasses import asdict

    allocation = ALLOCATION + f"/vfs/{operation}/{kind}/{when}".encode("ascii")
    path = windows_container_path(parent, allocation)

    def opened(*, initialize):
        return _vfs_files(shim, path, allocation, initialize=initialize)

    candidate = b"candidate decision" * 500
    selected = PrimaryByteFile("decision", len(candidate), hashlib.sha256(candidate).hexdigest())
    with opened(initialize=True) as files:
        original = files.put_bytes("journal", (CONTENT,), byte_limit=len(CONTENT))
        select = files._select

        def selection(generation, result):
            shim.arm(operation, kind, when)
            return select(generation, result)

        files._select = selection
        try:
            files.put_bytes("decision", (candidate,), byte_limit=len(candidate))
        except PrimaryStateError as exc:
            cause = exc.__cause__
            assert isinstance(cause, sqlite3.Error)
            error = cause.sqlite_errorcode
            assert error & 255 == sqlite3.SQLITE_IOERR
        else:
            raise AssertionError("VFS failure was reported as a successful selection")
        fault = shim.fault
        assert fault is not None and fault.injected
        assert fault.delegated is (when == "after")
        if fault.delegated:
            assert fault.native_result == sqlite3.SQLITE_OK
        try:
            files.sync()
        except PrimaryStateError:
            pass
        else:
            raise AssertionError("uncertain selection retained a usable owner")
    report = {**asdict(fault), "sqlite_errorcode": error}
    shim.clear_fault()
    with opened(initialize=False) as files:
        assert files._one("PRAGMA journal_mode") == ("persist",)
        assert files._one("PRAGMA synchronous") == (3,)
        with files.verified(original) as stream:
            assert stream.read() == CONTENT
        report["candidate_selected"] = files.exists("decision")
        if report["candidate_selected"]:
            with files.verified(selected) as stream:
                assert stream.read() == candidate
        steps = 0
        while not files.recover_step():
            steps += 1
            assert steps < 128
        report["candidate_cleanup_steps"] = steps
        with files.verified(original) as stream:
            assert stream.read() == CONTENT
    return report


def _vfs_fault_witness(parent: Path):
    from tests.support.sqlite_vfs_faults import FaultVfs

    reports = []
    with FaultVfs.windows() as shim:
        for operation in ("write", "sync"):
            for kind in ("database", "journal"):
                for when in ("before", "after"):
                    try:
                        reports.append(_vfs_selection_fault(shim, parent, operation, kind, when))
                    except BaseException as exc:
                        raise RuntimeError(
                            f"VFS selection fault {operation}/{kind}/{when}: "
                            f"injection={shim.fault}; callback_errors={shim.errors}"
                        ) from exc
        identity = shim.identity
        calls = {operation + ":" + kind: count for (operation, kind), count in shim.calls.items()}
    assert len(reports) == 8
    return {"runtime": identity, "faults": reports, "calls": calls}


def _physical_space_witness(parent: Path):
    from tests.support.prepared_primary_space import witness as space_witness
    from tests.support.sqlite_vfs_faults import FaultVfs

    allocation = ALLOCATION + b"/physical-space"
    path = windows_container_path(parent, allocation)
    with FaultVfs.windows() as shim:

        def opened(*, initialize):
            return _vfs_files(shim, path, allocation, initialize=initialize)

        def io_bytes():
            return {
                operation + ":" + kind: count
                for (operation, kind), count in shim.completed_bytes.items()
            }

        result = space_witness(opened, io_bytes=io_bytes)
        result["runtime"] = shim.identity
        result["completed_vfs_bytes"] = io_bytes()
        result["vfs_write_high_water"] = dict(shim.write_high_water)
        assert all(cycle["total_vacuum_database_write_bytes"] > 0 for cycle in result["cycles"])
    return result


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
        result = files.put_bytes("journal", (CONTENT[:1], CONTENT[1:]), byte_limit=len(CONTENT))
        assert result == file
        with files.extent_reader("journal", byte_count=len(CONTENT)) as stream:
            assert not files._connection.in_transaction
            assert stream.readline(4093) == CONTENT[:4093]
            stream.seek(4090)
            assert stream.readline(20) == CONTENT[4090:4110]
        cases.append("bounded-extent-framing")

        def control_only(action, table, _column, *_rest):
            if action == sqlite3.SQLITE_READ and table == "primary_chunks":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        files._connection.set_authorizer(control_only)
        try:
            files.put_control("receipt-proof", b"original-encoding", byte_limit=100)
            files.put_control("usage", b"before", byte_limit=100)
            files.put_control("usage", b"after", expected=b"before", byte_limit=100)
            assert files.read_control("receipt-proof", byte_limit=100) == b"original-encoding"
            assert files.read_control("usage", byte_limit=100) == b"after"
            files.remove_control("usage", expected=b"after")
        finally:
            files._connection.set_authorizer(None)
        cases.append("control-only-original-encoding")
        with files.verified(result) as stream:
            try:
                files.remove_step("journal")
            except PrimaryStateBusy:
                pass
            else:
                raise AssertionError("reclamation discarded a pinned replay generation")
            cases.append("pinned-generation-reclaim-exclusion")
            stream.seek(4090)
            assert stream.read(20) == CONTENT[4090:4110]
        try:
            files.put_bytes("journal", [b"replacement"], byte_limit=100)
        except FileExistsError:
            pass
        else:
            raise AssertionError("immutable winner was replaced")
        result = subprocess.run(
            [sys.executable, __file__, "--busy", str(path)], timeout=60, check=False
        )
        assert result.returncode == 42, result.returncode
        with files.child("existing", initialize=True):
            pass
        files.put_bytes("retiring", [b"r" * 9000], byte_limit=9000)
        assert not files.remove_step("retiring")
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
        assert not files.exists("retiring")
        assert files.read_control("receipt-proof", byte_limit=100) == b"original-encoding"
        operation = files._one("SELECT generation,cursor,phase FROM primary_operation")
        files.put_control("cleanup-progress", b"before", byte_limit=100)
        files.put_control("cleanup-progress", b"after", expected=b"before", byte_limit=100)
        assert files.read_control("cleanup-progress", byte_limit=100) == b"after"
        files.remove_control("cleanup-progress", expected=b"after")
        assert files._one("SELECT generation,cursor,phase FROM primary_operation") == operation
        try:
            files.put_control("retiring", b"replacement", byte_limit=100)
        except PrimaryStateBusy:
            pass
        else:
            raise AssertionError("control displaced its pending byte generation")
        cases.append("pending-cleanup-control-progress")
        try:
            files.child("existing", initialize=True)
        except FileExistsError:
            pass
        else:
            raise AssertionError("existing namespace was created again during cleanup")
        with files.child("existing", initialize=False) as child:
            assert not child.exists("missing")
        changes = files._connection.total_changes
        assert files.remove_step("missing")
        assert files._connection.total_changes == changes
        try:
            files.child("new-child", initialize=True)
        except PrimaryStateBusy:
            pass
        else:
            raise AssertionError("pending cleanup admitted a new namespace")
        cases.append("pending-cleanup-point-coordination")
        steps = 0
        while not files.remove_step("retiring"):
            steps += 1
        assert steps == 3
        rows = [("b", 2, "b" * 64), ("a", 1, "a" * 64)]

        def inventory():
            for row in rows:
                assert not files._connection.in_transaction
                yield row

        assert list(files.ordered_rows(inventory(), byte_limit=128 * 1024)) == rows[::-1]
        assert files._one("PRAGMA journal_mode") == ("persist",)
        assert files._one("PRAGMA synchronous") == (3,)
    with _files(path, initialize=False) as files:
        assert files.ordering_reserved_bytes() == 128 * 1024
        cleanup_steps = 0
        while not files.ordering_cleanup_step():
            cleanup_steps += 1
            assert files.ordering_reserved_bytes() == 128 * 1024
        assert cleanup_steps == 3
        assert files.ordering_reserved_bytes() == 0
        with files.verified(file) as stream:
            assert stream.read() == CONTENT
    cases.extend(["same-container-derived-ordering", "retained-scratch-admission"])
    from tests.support.prepared_primary_replay import DESCRIPTOR
    from tests.support.prepared_primary_replay import witness as shared_replay

    shared_replay(windows_container_path(parent, DESCRIPTOR), WindowsPrimaryFactory(LIMITS))
    cases.extend(
        [
            "shared-store-cold-exact-replay",
            "shared-store-bounded-custody",
            "shared-store-bounded-recovery",
        ]
    )
    cases.extend(["cold-reopen-exact-winner", "bounded-cold-generation-cleanup"])
    _guard_rename_exclusion(path)
    cases.extend(["live-database-rename-exclusion", "live-parent-rename-exclusion"])
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
                while not files.discard_candidates(("journal",)):
                    count += 1
                assert count == (len(CONTENT) + 4095) // 4096
                cases.append("adapter-owned-candidate-continuation")
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
    vfs_faults = _vfs_fault_witness(parent)
    cases.extend(
        "vfs-" + fault["operation"] + "-" + fault["file"] + "-" + fault["when"]
        for fault in vfs_faults["faults"]
    )
    space = _physical_space_witness(parent)
    cases.append("physical-space-bounded-maintenance")
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
        "vfs_faults": vfs_faults,
        "space": space,
        "elapsed_seconds": time.monotonic() - started,
        "limitations": [
            "process death is not power loss",
            "producer activation is not yet qualified",
            "VFS faults cover byte selection writes and syncs",
            "physical control reserves and machine-reset behavior are not certified",
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
