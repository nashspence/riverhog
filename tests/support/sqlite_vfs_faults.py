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
        self.errors = []
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
                    return code
                result = getattr(methods, name)(real, *arguments)
                if name == "close":
                    del self.files[address]
                if hit:
                    fault.delegated = True
                    fault.native_result = result
                    if result == sqlite3.SQLITE_OK:
                        fault.injected = True
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
