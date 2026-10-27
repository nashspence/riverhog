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
    with shim.transaction_writes(page_bytes=4096, tracked_pages=2) as trace:
        if operation == "write":
            result = shim.io.write(file, c.addressof(body), 11, 4096)
        else:
            result = shim.io.sync(file, 3)
    report = trace.report()
    assert report["database_pages_written"] == (
        1 if operation == "write" and when == "after" else 0
    )
    assert report["database_write_bytes"] == (11 if operation == "write" and when == "after" else 0)
    assert report["journal_write_bytes"] == report["journal_write_extent_bytes"] == 0
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


def test_transaction_write_measurement_counts_crossings_and_rewrites_without_payloads():
    from tests.support.sqlite_vfs_faults import TransactionWrites

    trace = TransactionWrites(page_bytes=4096, tracked_pages=2)
    trace.record("database", 4096, 0)
    trace.record("database", 3, 4095)
    trace.record("database", 1, 0)
    trace.record("journal", 40, 0)
    trace.record("journal", 10, 100)
    trace.record("journal", 0, 10**12)
    trace.record("database", 0, 10**12)
    trace.record("other", 13, 0)
    assert trace.report() == {
        "database_pages_written": 2,
        "database_write_bytes": 4100,
        "journal_write_bytes": 50,
        "journal_write_extent_bytes": 110,
        "other_write_bytes": 13,
    }


def test_transaction_measurement_capacity_exhaustion_invalidates_the_report():
    from tests.support.sqlite_vfs_faults import TransactionWrites

    trace = TransactionWrites(page_bytes=4096, tracked_pages=2)
    trace.record("database", 4 * 4096, 0)
    assert len(trace.pages) == 2 and trace.database_bytes == 4 * 4096
    trace.record("database", 1, 8 * 4096)
    assert len(trace.pages) == 2
    with pytest.raises(RuntimeError, match="tracking capacity"):
        trace.report()


def test_transaction_measurement_context_rejects_overlap_and_releases_on_error():
    base = SqliteVfs()
    base.version, base.file_bytes, base.pathname_bytes = 1, c.sizeof(SqliteFile), 512
    shim = FaultVfs(None, c.pointer(base))
    with pytest.raises(OSError, match="interrupted"):
        with shim.transaction_writes(page_bytes=4096, tracked_pages=2) as first:
            first.record("journal", 40, 0)
            with pytest.raises(RuntimeError, match="overlap"):
                with shim.transaction_writes(page_bytes=4096, tracked_pages=2):
                    pytest.fail("overlapping measurements were accepted")
            raise OSError("interrupted")
    with shim.transaction_writes(page_bytes=4096, tracked_pages=2) as second:
        assert second.report()["journal_write_bytes"] == 0
    assert first.report()["journal_write_bytes"] == 40
