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
    with shim.write_quota(forward=_write_limits(), recovery=_write_limits()) as quota:
        with shim.transaction_writes(page_bytes=4096, tracked_pages=2) as trace:
            if operation == "write":
                result = shim.io.write(file, c.addressof(body), 11, 4096)
            else:
                result = shim.io.sync(file, 3)
        assert quota.recovering and quota.native_failures == 1
        charged = quota.forward.report()
        assert charged["database_pages_attempted"] == (1 if operation == "write" else 0)
        assert charged["database_write_bytes"] == (11 if operation == "write" else 0)
        assert not quota.recovery.pages
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


def _write_limits(**changes):
    from tests.support.sqlite_vfs_faults import WriteLimits

    values = {
        "page_bytes": 4096,
        "database_pages": 2,
        "database_end": 16384,
        "journal_end": 10000,
        "write_bytes": 20000,
        "calls": 20,
    }
    return WriteLimits(**(values | changes))


def test_write_bank_admits_exact_prefix_and_rejects_crossing_without_partial_charge():
    from tests.support.sqlite_vfs_faults import WriteBank, WriteRefusal

    bank = WriteBank(_write_limits())
    bank.admit("database", 2, 4095)
    bank.admit("database", 1, 0)
    bank.admit("journal", 10, 9990)
    before = bank.report()
    with pytest.raises(WriteRefusal, match="page grant"):
        bank.admit("database", 1, 8192)
    assert bank.report() == before
    assert len(bank.pages) == 2
    assert before["database_write_bytes"] == 3 and before["write_calls"] == 3
    assert before["journal_write_extent_bytes"] == 10000


@pytest.mark.parametrize(
    ("kind", "amount", "offset"),
    [
        ("database", 1, 16384),
        ("journal", 1, 10000),
        ("other", 1, 0),
        ("other", 0, 0),
        ("journal", 2, (1 << 63) - 1),
        ("database", True, 0),
        ("database", 1, -1),
        ("database", 0, 1 << 63),
    ],
)
def test_write_bank_rejects_invalid_or_unadmitted_extents_before_delegation(kind, amount, offset):
    from tests.support.sqlite_vfs_faults import WriteBank, WriteRefusal

    bank = WriteBank(_write_limits())
    with pytest.raises(WriteRefusal):
        bank.admit(kind, amount, offset)
    assert bank.report()["write_calls"] == 0 and not bank.pages


@pytest.mark.parametrize("grant", [{"calls": 2}, {"write_bytes": 2}])
def test_write_bank_counts_rewrites_toward_byte_and_call_grants(grant):
    from tests.support.sqlite_vfs_faults import WriteBank, WriteRefusal

    bank = WriteBank(_write_limits(**grant))
    bank.admit("database", 1, 0)
    bank.admit("database", 1, 0)
    with pytest.raises(WriteRefusal):
        bank.admit("database", 1, 0)
    assert bank.report()["write_calls"] == bank.report()["write_bytes"] == 2
    assert len(bank.pages) == 1


def test_empty_write_does_not_create_pages_or_journal_extent_but_consumes_a_call():
    from tests.support.sqlite_vfs_faults import WriteBank

    bank = WriteBank(_write_limits())
    bank.admit("database", 0, (1 << 63) - 1)
    bank.admit("journal", 0, (1 << 63) - 1)
    assert bank.report()["write_calls"] == 2
    assert bank.report()["write_bytes"] == bank.report()["journal_write_extent_bytes"] == 0
    assert not bank.pages


@pytest.mark.parametrize("refusal", ["database", "journal"])
def test_actual_sqlite_write_quota_preserves_originals_and_reserved_rollback(tmp_path, refusal):
    import _sqlite3
    import sys

    from riverhog_client._prepared_primary_sqlite import (
        SqlitePrimaryLimits,
        TransactionalPrimaryFiles,
    )

    from tests.support.prepared_primary import SqlitePrimaryTestGuard
    from tests.support.prepared_primary_write_quota import witness
    from tests.support.sqlite_vfs_faults import _PV

    if sys.platform != "linux":
        pytest.skip("portable quota isolation requires Linux; Windows has its native owner witness")
    library = c.CDLL(getattr(_sqlite3, "__file__", None))
    try:
        source_id = library.sqlite3_sourceid
    except AttributeError:
        pytest.skip(
            "pinned Linux runtime hides its SQLite VFS API; actual quota cases run in Windows CI"
        )
    source_id.restype = c.c_char_p
    with sqlite3.connect(":memory:") as connection:
        expected = connection.execute("SELECT sqlite_source_id()").fetchone()[0]
    assert source_id().decode("ascii") == expected
    library.sqlite3_vfs_find.argtypes, library.sqlite3_vfs_find.restype = [c.c_char_p], _PV
    library.sqlite3_vfs_register.argtypes = [_PV, c.c_int]
    library.sqlite3_vfs_register.restype = c.c_int
    library.sqlite3_vfs_unregister.argtypes, library.sqlite3_vfs_unregister.restype = [_PV], c.c_int
    with FaultVfs(library, library.sqlite3_vfs_find(b"unix")) as shim:
        path = tmp_path / "state.sqlite3"

        def opened(*, initialize):
            if initialize:
                path.touch(exist_ok=False)
            guard = SqlitePrimaryTestGuard(path)
            guard.vfs = shim.name
            return TransactionalPrimaryFiles(
                guard,
                allocation_descriptor=b"quota original allocation",
                limits=SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024),
                initialize=initialize,
            )

        result = witness(opened, shim, refusal)
        assert result["larger_grant_retry_coldly_verified"]
        assert result["original_control_exact"] and result["original_payload_exact"]


def test_quota_refuses_the_whole_write_before_delegate_and_keeps_recovery_independent():
    calls = []
    native = IoMethods()
    native.version = 1

    def write(_file, _body, amount, offset):
        calls.append((amount, offset))
        return sqlite3.SQLITE_OK

    native.write = _IO["write"](write)
    base = SqliteVfs()
    base.version, base.file_bytes, base.pathname_bytes = 1, c.sizeof(SqliteFile), 512
    shim = FaultVfs(None, c.pointer(base))
    storage = c.create_string_buffer(shim.vfs.file_bytes)
    file = c.cast(storage, _PF)
    shim._real(file).contents.methods = c.addressof(native)
    shim.files[c.addressof(storage)] = "database"
    body = c.create_string_buffer(b"ab")
    with shim.write_quota(
        forward=_write_limits(), recovery=_write_limits(database_pages=1)
    ) as quota:
        assert shim.io.write(file, c.addressof(body), 2, 4095) == sqlite3.SQLITE_OK
        before = quota.forward.report()
        assert shim.io.write(file, c.addressof(body), 1, 8192) == sqlite3.SQLITE_FULL
        assert calls == [(2, 4095)]
        assert quota.recovering and quota.forward.report() == before
        assert shim.io.write(file, c.addressof(body), 1, 8192) == sqlite3.SQLITE_OK
        assert calls == [(2, 4095), (1, 8192)]
        assert quota.recovery.report()["database_pages_attempted"] == 1
        assert shim.io.write(file, c.addressof(body), 1, 12288) == sqlite3.SQLITE_FULL
        assert calls == [(2, 4095), (1, 8192)]
        assert quota.refusals == 2 and quota.first_refusal["bank"] == "forward"
    assert not shim.errors


def test_write_quota_rejects_overlap_and_exceptional_exit_releases_the_frame():
    base = SqliteVfs()
    base.version, base.file_bytes, base.pathname_bytes = 1, c.sizeof(SqliteFile), 512
    shim = FaultVfs(None, c.pointer(base))
    with pytest.raises(OSError):
        with shim.write_quota(forward=_write_limits(), recovery=_write_limits()):
            with pytest.raises(RuntimeError, match="overlap"):
                with shim.write_quota(forward=_write_limits(), recovery=_write_limits()):
                    pytest.fail("overlapping quota accepted")
            raise OSError("interrupted")
    with shim.write_quota(forward=_write_limits(), recovery=_write_limits()) as quota:
        assert not quota.recovering and quota.forward.report()["write_calls"] == 0
