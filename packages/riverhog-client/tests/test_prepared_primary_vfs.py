"""Public VFS callback forwarding preserves native buffers and failure boundaries."""

from __future__ import annotations

import ctypes as c
import sqlite3

import pytest

from tests.support.sqlite_vfs_faults import (
    _IO,
    _MAIN_DB,
    _PF,
    _VFS,
    FaultVfs,
    IoMethods,
    SqliteFile,
    SqliteVfs,
)


@pytest.mark.parametrize("operation", ["write", "sync"])
@pytest.mark.parametrize("when", ["before", "after"])
def test_vfs_fault_surrounds_the_native_call_without_replacing_its_file_or_filename(
    operation, when
):
    calls = []
    filename = c.create_string_buffer(b"database\x00uri-key\x00original-uri-value\x00\x00")
    native_methods = IoMethods()
    native_methods.version = 1

    def write(file, body, amount, offset):
        calls.append(("write", c.cast(file, c.c_void_p).value, c.string_at(body, amount), offset))
        return 0

    def sync(file, flags):
        calls.append(("sync", c.cast(file, c.c_void_p).value, flags))
        return 0

    def close(file):
        calls.append(("close", c.cast(file, c.c_void_p).value))
        return 0

    native_methods.write = _IO["write"](write)
    native_methods.sync = _IO["sync"](sync)
    native_methods.close = _IO["close"](close)
    base = SqliteVfs()
    base.version, base.file_bytes, base.pathname_bytes = 1, c.sizeof(SqliteFile), 512

    def opened(vfs, name, file, flags, output):
        calls.append(
            (
                "open",
                c.cast(file, c.c_void_p).value,
                c.addressof(vfs.contents),
                name,
                c.string_at(name, len(filename)),
                flags,
            )
        )
        output[0] = flags
        file.contents.methods = c.addressof(native_methods)
        return 0

    base.open = _VFS["open"](opened)
    shim = FaultVfs(None, c.pointer(base))
    storage = c.create_string_buffer(shim.vfs.file_bytes)
    file = c.cast(storage, _PF)
    flags = c.c_int()
    assert (
        shim.vfs.open(c.byref(shim.vfs), c.addressof(filename), file, _MAIN_DB, c.byref(flags)) == 0
    )
    real = c.addressof(storage) + shim.offset
    assert calls == [
        ("open", real, c.addressof(base), c.addressof(filename), filename.raw, _MAIN_DB)
    ]
    fault = shim.arm(operation, "database", when)
    body = c.create_string_buffer(b"exact bytes")
    if operation == "write":
        result = shim.io.write(file, c.addressof(body), 11, 4096)
    else:
        result = shim.io.sync(file, 3)
    expected = sqlite3.SQLITE_IOERR_WRITE if operation == "write" else sqlite3.SQLITE_IOERR_FSYNC
    assert result == expected and fault.injected
    assert fault.delegated is (when == "after")
    assert shim.completed_bytes[("write", "database")] == (
        11 if operation == "write" and when == "after" else 0
    )
    assert shim.write_high_water["database"] == (
        4107 if operation == "write" and when == "after" else 0
    )
    assert len(calls) == (2 if when == "after" else 1)
    if when == "after":
        assert calls[1][0] == operation and calls[1][1] == real
    shim.clear_fault()
    assert shim.io.close(file) == 0
    assert calls[-1] == ("close", real)
    assert not shim.files and not shim.errors
