"""Test-only interposition of SQLite's public VFS and file-method interfaces.

The version-one prefixes follow sqlite.org/c3ref/vfs.html and io_methods.html.
File-name pointers are forwarded unchanged, including SQLite's URI metadata.
"""

from __future__ import annotations

import ctypes as c
import hashlib
import os
import sqlite3
import uuid
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

_INT, _PTR, _I64 = c.c_int, c.c_void_p, c.c_int64


class SqliteFile(c.Structure):
    _fields_ = [("methods", _PTR)]


class SqliteVfs(c.Structure):
    pass


_PF, _PV = c.POINTER(SqliteFile), c.POINTER(SqliteVfs)
_PI, _PI64 = c.POINTER(_INT), c.POINTER(_I64)

_IO = {
    "close": c.CFUNCTYPE(_INT, _PF),
    "read": c.CFUNCTYPE(_INT, _PF, _PTR, _INT, _I64),
    "write": c.CFUNCTYPE(_INT, _PF, _PTR, _INT, _I64),
    "truncate": c.CFUNCTYPE(_INT, _PF, _I64),
    "sync": c.CFUNCTYPE(_INT, _PF, _INT),
    "size": c.CFUNCTYPE(_INT, _PF, _PI64),
    "lock": c.CFUNCTYPE(_INT, _PF, _INT),
    "unlock": c.CFUNCTYPE(_INT, _PF, _INT),
    "reserved": c.CFUNCTYPE(_INT, _PF, _PI),
    "control": c.CFUNCTYPE(_INT, _PF, _INT, _PTR),
    "sector": c.CFUNCTYPE(_INT, _PF),
    "characteristics": c.CFUNCTYPE(_INT, _PF),
}


class IoMethods(c.Structure):
    _fields_ = [("version", _INT), *[(name, kind) for name, kind in _IO.items()]]


_VFS = {
    "open": c.CFUNCTYPE(_INT, _PV, _PTR, _PF, _INT, _PI),
    "delete": c.CFUNCTYPE(_INT, _PV, _PTR, _INT),
    "access": c.CFUNCTYPE(_INT, _PV, _PTR, _INT, _PI),
    "pathname": c.CFUNCTYPE(_INT, _PV, _PTR, _INT, _PTR),
    "dlopen": c.CFUNCTYPE(_PTR, _PV, _PTR),
    "dlerror": c.CFUNCTYPE(None, _PV, _INT, _PTR),
    "dlsym": c.CFUNCTYPE(_PTR, _PV, _PTR, _PTR),
    "dlclose": c.CFUNCTYPE(None, _PV, _PTR),
    "randomness": c.CFUNCTYPE(_INT, _PV, _INT, _PTR),
    "sleep": c.CFUNCTYPE(_INT, _PV, _INT),
    "time": c.CFUNCTYPE(_INT, _PV, c.POINTER(c.c_double)),
    "error": c.CFUNCTYPE(_INT, _PV, _INT, _PTR),
}

SqliteVfs._fields_ = [
    ("version", _INT),
    ("file_bytes", _INT),
    ("pathname_bytes", _INT),
    ("next", _PV),
    ("name", c.c_char_p),
    ("application", _PTR),
    *[(name, kind) for name, kind in _VFS.items()],
]

_LIVE = {}
_MAIN_DB, _MAIN_JOURNAL = 0x00000100, 0x00000800


@dataclass
class Fault:
    operation: str
    file: str
    when: str
    injected: bool = False
    delegated: bool = False
    native_result: int | None = None


class TransactionWrites:
    """Bounded successful-delegation measurements, without retaining payload bytes."""

    def __init__(self, *, page_bytes, tracked_pages):
        if any(type(value) is not int or value <= 0 for value in (page_bytes, tracked_pages)):
            raise ValueError("transaction measurement needs positive page geometry and capacity")
        self.page_bytes, self.tracked_pages = page_bytes, tracked_pages
        self.pages = set()
        self.overflow = False
        self.database_bytes = self.journal_bytes = self.journal_extent = self.other_bytes = 0

    def record(self, kind, amount, offset):
        if type(amount) is not int or type(offset) is not int or min(amount, offset) < 0:
            raise ValueError("transaction write extent is invalid")
        if not amount:
            return
        if kind == "journal":
            self.journal_bytes += amount
            self.journal_extent = max(self.journal_extent, offset + amount)
        elif kind == "database":
            self.database_bytes += amount
            if amount:
                first, last = offset // self.page_bytes, (offset + amount - 1) // self.page_bytes
                for page in range(first, last + 1):
                    if page in self.pages:
                        continue
                    if len(self.pages) == self.tracked_pages:
                        self.overflow = True
                        break
                    self.pages.add(page)
        else:
            self.other_bytes += amount

    def report(self):
        if self.overflow:
            raise RuntimeError("transaction measurement exhausted its declared tracking capacity")
        return {
            "database_pages_written": len(self.pages),
            "database_write_bytes": self.database_bytes,
            "journal_write_bytes": self.journal_bytes,
            "journal_write_extent_bytes": self.journal_extent,
            "other_write_bytes": self.other_bytes,
        }


@dataclass(frozen=True)
class WriteLimits:
    """Experimental xWrite envelope; other allocating VFS methods remain separate."""

    page_bytes: int
    database_pages: int
    database_end: int
    journal_end: int
    write_bytes: int
    calls: int

    def __post_init__(self):
        for value in (
            self.page_bytes,
            self.database_pages,
            self.database_end,
            self.journal_end,
            self.write_bytes,
            self.calls,
        ):
            if type(value) is not int or not 0 < value <= (1 << 63) - 1:
                raise ValueError("write grants must be positive signed-64-bit integers")
        if self.page_bytes & (self.page_bytes - 1):
            raise ValueError("write grant needs power-of-two pages")


class WriteRefusal(Exception):
    pass


class WriteBank:
    """Bounded test bookkeeping: charge attempted delegation, without refunds."""

    def __init__(self, limits):
        self.limits = limits
        self.pages = set()
        self.calls = self.write_bytes = self.database_bytes = self.journal_bytes = 0
        self.journal_extent = 0

    def admit(self, kind, amount, offset):
        maximum = (1 << 63) - 1
        if any(type(value) is not int or value < 0 for value in (amount, offset)):
            raise WriteRefusal("invalid write extent")
        if offset > maximum or amount > maximum - offset:
            raise WriteRefusal("write extent overflow")
        if kind not in ("database", "journal"):
            raise WriteRefusal("unadmitted file class")
        limits = self.limits
        if self.calls == limits.calls or amount > limits.write_bytes - self.write_bytes:
            raise WriteRefusal("write byte/call grant")
        new = set()
        if amount:
            end = offset + amount
            ceiling = limits.database_end if kind == "database" else limits.journal_end
            if end > ceiling:
                raise WriteRefusal(kind + " extent grant")
            if kind == "database":
                first, last = offset // limits.page_bytes, (end - 1) // limits.page_bytes
                if last - first + 1 > limits.database_pages:
                    raise WriteRefusal("database page grant")
                new = set(range(first, last + 1)) - self.pages
                if len(new) > limits.database_pages - len(self.pages):
                    raise WriteRefusal("database page grant")
        self.pages.update(new)
        self.calls += 1
        self.write_bytes += amount
        if kind == "database":
            self.database_bytes += amount
        else:
            self.journal_bytes += amount
            if amount:
                self.journal_extent = max(self.journal_extent, offset + amount)

    def report(self):
        return {
            "database_pages_attempted": len(self.pages),
            "database_write_bytes": self.database_bytes,
            "journal_write_bytes": self.journal_bytes,
            "journal_write_extent_bytes": self.journal_extent,
            "write_bytes": self.write_bytes,
            "write_calls": self.calls,
        }


class WriteQuota:
    """Latch a separate restoration bank before returning an error to SQLite."""

    def __init__(self, forward, recovery):
        if forward is recovery:
            raise ValueError("forward and recovery write banks must be independent")
        self.forward, self.recovery = forward, recovery
        self.recovering = False
        self.refusals = 0
        self.first_refusal = None
        self.native_failures = 0

    def admit(self, kind, amount, offset):
        bank = self.recovery if self.recovering else self.forward
        try:
            bank.admit(kind, amount, offset)
        except WriteRefusal as exc:
            self.refusals += 1
            if self.first_refusal is None:
                self.first_refusal = {
                    "file": kind,
                    "amount": amount,
                    "offset": offset,
                    "reason": str(exc),
                    "bank": "recovery" if self.recovering else "forward",
                }
            self.recovering = True
            return False
        return True

    def failed(self):
        self.native_failures += 1
        self.recovering = True

    def report(self):
        return {
            "forward": self.forward.report(),
            "recovery": self.recovery.report(),
            "recovering": self.recovering,
            "refusals": self.refusals,
            "first_refusal": self.first_refusal,
            "native_failures": self.native_failures,
        }


class FaultVfs:
    """Keep native file/lock operations; inject one write/sync return at its actual callback."""

    offset = 16

    def __init__(self, library, base: _PV) -> None:
        if not base or base.contents.version < 1:
            raise RuntimeError("SQLite native VFS is unavailable")
        self.library, self.base = library, base
        self.name = "riverhog-fault-" + uuid.uuid4().hex
        self._name_bytes = self.name.encode("ascii")
        self._callbacks = []
        self.files = {}
        self.calls = Counter()
        self.completed_bytes = Counter()
        self.write_high_water = Counter()
        self.errors = []
        self._transaction_writes = None
        self._write_quota = None
        self.fault: Fault | None = None
        self.identity = {}
        self.io = IoMethods()
        self.io.version = 1
        for name, kind in _IO.items():
            callback = kind(self._file_callback(name))
            self._callbacks.append(callback)
            setattr(self.io, name, callback)
        self.vfs = SqliteVfs()
        self.vfs.version = 1
        self.vfs.file_bytes = self.offset + base.contents.file_bytes
        self.vfs.pathname_bytes = base.contents.pathname_bytes
        self.vfs.name = self._name_bytes
        for name, kind in _VFS.items():
            callback = kind(self._vfs_callback(name))
            self._callbacks.append(callback)
            setattr(self.vfs, name, callback)

    @classmethod
    def windows(cls) -> FaultVfs:
        """Bind the already loaded runtime and require stdlib-visible VFS registration."""
        if os.name != "nt":
            raise RuntimeError("native Windows SQLite DLL is required")
        kernel = c.WinDLL("kernel32", use_last_error=True)
        kernel.GetModuleHandleW.argtypes = [c.c_wchar_p]
        kernel.GetModuleHandleW.restype = _PTR
        handle = kernel.GetModuleHandleW("sqlite3.dll")
        if not handle:
            raise RuntimeError("the loaded SQLite DLL is unavailable")
        library = c.CDLL("sqlite3.dll", handle=handle)
        library.sqlite3_sourceid.argtypes = []
        library.sqlite3_sourceid.restype = c.c_char_p
        with sqlite3.connect(":memory:") as db:
            expected = db.execute("SELECT sqlite_source_id()").fetchone()[0]
        if library.sqlite3_sourceid().decode("ascii") != expected:
            raise RuntimeError("VFS API differs from the pinned SQLite runtime")
        library.sqlite3_vfs_find.argtypes = [c.c_char_p]
        library.sqlite3_vfs_find.restype = _PV
        library.sqlite3_vfs_register.argtypes = [_PV, _INT]
        library.sqlite3_vfs_register.restype = _INT
        library.sqlite3_vfs_unregister.argtypes = [_PV]
        library.sqlite3_vfs_unregister.restype = _INT
        shim = cls(library, library.sqlite3_vfs_find(b"win32"))
        kernel.GetModuleFileNameW.argtypes = [_PTR, c.c_wchar_p, c.c_uint32]
        kernel.GetModuleFileNameW.restype = c.c_uint32
        path = c.create_unicode_buffer(32768)
        length = kernel.GetModuleFileNameW(handle, path, len(path))
        if not length or length >= len(path):
            raise RuntimeError("loaded SQLite library identity is unavailable")
        shim.identity = {
            "sqlite_source_id": expected,
            "sqlite_library_sha256": hashlib.sha256(Path(path.value).read_bytes()).hexdigest(),
            "base_vfs": shim.base.contents.name.decode("ascii"),
            "interposer": "public-vfs-version-1",
        }
        return shim

    def __enter__(self) -> FaultVfs:
        self._default = c.cast(self.library.sqlite3_vfs_find(None), _PTR).value
        if self.library.sqlite3_vfs_register(c.byref(self.vfs), 0):
            raise RuntimeError("SQLite fault VFS registration failed")
        _LIVE[self.name] = self
        if c.cast(self.library.sqlite3_vfs_find(None), _PTR).value != self._default:
            raise RuntimeError("fault witness changed the default VFS")
        return self

    def __exit__(self, *_args) -> None:
        if self.files:
            # Retain callbacks if a failed fixture still has a native handle.
            raise RuntimeError("fault witness still owns SQLite files")
        if self.library.sqlite3_vfs_unregister(c.byref(self.vfs)):
            raise RuntimeError("SQLite fault VFS removal failed")
        del _LIVE[self.name]
        if self.errors:
            raise RuntimeError("SQLite VFS callback failed: " + self.errors[0])

    def arm(self, operation: str, file: str, when: str) -> Fault:
        if operation not in ("write", "sync") or file not in ("database", "journal"):
            raise ValueError("unknown SQLite fault callback")
        if when not in ("before", "after"):
            raise ValueError("unknown SQLite fault boundary")
        if self.fault is not None:
            raise RuntimeError("the preceding fault must be cleared explicitly")
        self.fault = Fault(operation, file, when)
        return self.fault

    def clear_fault(self) -> None:
        if self.fault is None or not self.fault.injected:
            raise RuntimeError("the requested VFS fault did not occur")
        self.fault = None

    @contextmanager
    def transaction_writes(self, *, page_bytes, tracked_pages):
        if self._transaction_writes is not None:
            raise RuntimeError("transaction measurements must not overlap")
        trace = TransactionWrites(page_bytes=page_bytes, tracked_pages=tracked_pages)
        self._transaction_writes = trace
        try:
            yield trace
        finally:
            self._transaction_writes = None

    @contextmanager
    def write_quota(self, *, forward, recovery):
        if self._write_quota is not None:
            raise RuntimeError("write quotas must not overlap")
        quota = WriteQuota(WriteBank(forward), WriteBank(recovery))
        self._write_quota = quota
        try:
            yield quota
        finally:
            self._write_quota = None

    def _real(self, file: _PF) -> _PF:
        return c.cast(c.cast(file, _PTR).value + self.offset, _PF)

    def _file_callback(self, name):
        def call(file, *arguments):
            address = c.cast(file, _PTR).value
            try:
                real = self._real(file)
                methods = c.cast(real.contents.methods, c.POINTER(IoMethods)).contents
                file_kind = self.files[address]
                self.calls[(name, file_kind)] += 1
                quota = self._write_quota
                if name == "write" and quota is not None:
                    if not quota.admit(file_kind, arguments[1], arguments[2]):
                        return sqlite3.SQLITE_FULL
                fault = self.fault
                hit = (
                    fault is not None
                    and not fault.injected
                    and fault.operation == name
                    and fault.file == file_kind
                )
                code = sqlite3.SQLITE_IOERR_WRITE if name == "write" else sqlite3.SQLITE_IOERR_FSYNC
                if hit and fault.when == "before":
                    fault.injected = True
                    if quota is not None:
                        quota.failed()
                    return code
                result = getattr(methods, name)(real, *arguments)
                if quota is not None and name in ("write", "sync") and result != sqlite3.SQLITE_OK:
                    quota.failed()
                if result == sqlite3.SQLITE_OK and name in ("read", "write"):
                    self.completed_bytes[(name, file_kind)] += arguments[1]
                    if name == "write":
                        if self._transaction_writes is not None:
                            self._transaction_writes.record(file_kind, arguments[1], arguments[2])
                        self.write_high_water[file_kind] = max(
                            self.write_high_water[file_kind], arguments[2] + arguments[1]
                        )
                if name == "close":
                    del self.files[address]
                if hit:
                    fault.delegated = True
                    fault.native_result = result
                    if result == sqlite3.SQLITE_OK:
                        fault.injected = True
                        if quota is not None:
                            quota.failed()
                        return code
                return result
            except BaseException as exc:
                if not self.errors:
                    self.errors.append(type(exc).__name__)
                return sqlite3.SQLITE_IOERR

        return call

    def _vfs_callback(self, name):
        def call(_vfs, *arguments):
            try:
                if name != "open":
                    return getattr(self.base.contents, name)(self.base, *arguments)
                filename, file, flags, output_flags = arguments
                file.contents.methods = None
                real = self._real(file)
                result = self.base.contents.open(self.base, filename, real, flags, output_flags)
                if real.contents.methods:
                    self.files[c.cast(file, _PTR).value] = (
                        "database"
                        if flags & _MAIN_DB
                        else "journal"
                        if flags & _MAIN_JOURNAL
                        else "other"
                    )
                    file.contents.methods = c.addressof(self.io)
                return result
            except BaseException as exc:
                if not self.errors:
                    self.errors.append(type(exc).__name__)
                if name in ("dlerror", "dlclose", "dlopen", "dlsym"):
                    return None
                return sqlite3.SQLITE_IOERR

        return call
