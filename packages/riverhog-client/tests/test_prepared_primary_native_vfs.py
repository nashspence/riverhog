"""The actual loaded Windows engine invokes C storage callbacks."""

from __future__ import annotations

import ctypes as c
import gc
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

pytestmark = pytest.mark.skipif(os.name != "nt", reason="requires the loaded native Windows SQLite")


def grant():
    return (4096, 128, 1024 * 1024, 1024 * 1024, 8 * 1024 * 1024, 65536)


def runtime():
    from riverhog_client import _prepared_primary_native as native

    with sqlite3.connect(":memory:") as connection:
        source = connection.execute("SELECT sqlite_source_id()").fetchone()[0]
    return native, source


def reads(native, quota, *, forward=(8 * 1024 * 1024, 65536), recovery=(8 * 1024 * 1024, 65536)):
    return native.ReadQuota(quota, forward, recovery)


def opened(path, gate, *, readonly=False):
    uri = path.resolve().as_uri() + "?vfs=" + gate.name
    if readonly:
        uri += "&mode=ro"
    return sqlite3.connect(uri, uri=True)


def test_native_write_vfs_binds_stdlib_and_preserves_default_registration(tmp_path):
    from tests.support.sqlite_vfs_faults import FaultVfs

    native, source = runtime()
    library = FaultVfs.windows().library
    before = library.sqlite3_vfs_find(None)
    import ctypes

    address = ctypes.cast(before, ctypes.c_void_p).value
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(source, reads(native, quota))
    try:
        connection = opened(tmp_path / "primary.sqlite3", gate)
        try:
            with connection:
                assert connection.execute("SELECT sqlite_source_id()").fetchone() == (source,)
                assert connection.execute("PRAGMA journal_mode=PERSIST").fetchone() == ("persist",)
                connection.execute("PRAGMA synchronous=EXTRA")
                connection.execute("PRAGMA journal_size_limit=4096")
                connection.execute("CREATE TABLE sample (value BLOB NOT NULL)")
                connection.execute("INSERT INTO sample VALUES (?)", (b"exact original",))
        finally:
            connection.close()
        assert quota.report()["forward"]["database_pages_attempted"] > 0
        assert quota.resize_report()["forward"]["resize_calls"] > 0
        assert gate.identity()["base_vfs"] == "win32"
        assert gate.identity()["sqlite_source_id"] == source
        assert gate.open_files == 0
        connection = opened(tmp_path / "primary.sqlite3", gate, readonly=True)
        try:
            assert connection.execute("SELECT value FROM sample").fetchone() == (b"exact original",)
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                connection.execute("INSERT INTO sample VALUES ('changed')")
        finally:
            connection.close()
        assert ctypes.cast(library.sqlite3_vfs_find(None), ctypes.c_void_p).value == address
    finally:
        gate.close()


def test_native_write_vfs_cannot_bind_a_different_sqlite_source():
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    with pytest.raises(RuntimeError, match="identity"):
        native.WriteVfs(source + "different", reads(native, quota))
    assert not quota.recovering


def test_native_write_vfs_retains_its_bank_and_refuses_live_unregister(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(source, reads(native, quota))
    connection = opened(tmp_path / "primary.sqlite3", gate)
    try:
        assert gate.open_files > 0
        with pytest.raises(RuntimeError, match="live files"):
            gate.close()
        del quota
        gc.collect()
        connection.execute("CREATE TABLE sample (value BLOB)")
        connection.commit()
    finally:
        connection.close()
        gate.close()
    gate.close()
    with pytest.raises(RuntimeError, match="closed"):
        gate.identity()


def test_native_write_vfs_retains_poisoned_bank_until_native_files_close(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    replacement = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(source, reads(native, quota))
    connection = opened(tmp_path / "primary.sqlite3", gate)
    try:
        quota.failed()
        with pytest.raises(RuntimeError, match="poisoned"):
            gate.arm(reads(native, replacement))
        assert quota.recovering
    finally:
        connection.close()
    try:
        gate.arm(reads(native, replacement))
        assert not replacement.recovering
    finally:
        gate.close()


def test_native_write_vfs_and_attached_bank_reject_other_thread_access(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    replacement = native.WriteQuota(grant(), grant())
    read_bank = reads(native, quota)
    gate = native.WriteVfs(source, read_bank)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            for operation in (
                quota.report,
                quota.resize_report,
                read_bank.report,
                gate.close,
                lambda: gate.arm(reads(native, replacement)),
            ):
                with pytest.raises(RuntimeError, match="thread"):
                    pool.submit(operation).result()

            def other_open():
                connection = opened(tmp_path / "other.sqlite3", gate)
                connection.close()

            with pytest.raises(sqlite3.OperationalError):
                pool.submit(other_open).result()
        assert gate.open_files == 0
    finally:
        gate.close()


def test_native_write_vfs_rejects_uninitialized_and_reinitialized_objects():
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    uninitialized = native.WriteVfs.__new__(native.WriteVfs)
    with pytest.raises(RuntimeError, match="uninitialized"):
        uninitialized.arm(reads(native, quota))
    with pytest.raises(RuntimeError, match="not initialized"):
        native.WriteVfs(source, native.ReadQuota.__new__(native.ReadQuota))
    with pytest.raises(TypeError):
        native.WriteVfs(source, quota)
    gate = native.WriteVfs(source, reads(native, quota))
    try:
        with pytest.raises(TypeError, match="ReadQuota"):
            gate.arm(object())
        with pytest.raises(RuntimeError, match="reinitialized"):
            gate.__init__(source, reads(native, quota))
    finally:
        gate.close()


def test_native_read_refusal_before_first_header_preserves_original_and_reopens(tmp_path):
    native, source = runtime()
    path = tmp_path / "primary.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE sample (value BLOB)")
        connection.execute("INSERT INTO sample VALUES (?)", (b"exact original",))
        connection.commit()
    finally:
        connection.close()
    original = path.read_bytes()
    quota = native.WriteQuota(grant(), grant())
    read_bank = reads(native, quota, forward=(1, 1))
    gate = native.WriteVfs(source, read_bank)
    try:
        with pytest.raises(sqlite3.DatabaseError):
            connection = opened(path, gate, readonly=True)
            try:
                connection.execute("SELECT value FROM sample").fetchone()
            finally:
                connection.close()
        assert quota.recovering
        assert quota.report()["first_refusal"]["reason"] == "read byte grant"
        assert quota.report()["native_failures"] == 0
        assert read_bank.report()["forward"] == {"read_bytes": 0, "read_calls": 0}
        assert gate.open_files == 0
    finally:
        gate.close()
    assert path.read_bytes() == original
    replacement = native.WriteQuota(grant(), grant())
    read_bank = reads(native, replacement)
    gate = native.WriteVfs(source, read_bank)
    try:
        connection = opened(path, gate, readonly=True)
        try:
            assert connection.execute("SELECT value FROM sample").fetchone() == (b"exact original",)
        finally:
            connection.close()
        assert read_bank.report()["forward"]["read_calls"] > 0
        assert not replacement.recovering
    finally:
        gate.close()
    assert path.read_bytes() == original


def test_native_short_read_keeps_full_charge_without_latching_failure(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    read_bank = reads(native, quota)
    gate = native.WriteVfs(source, read_bank)
    try:
        connection = opened(tmp_path / "empty.sqlite3", gate)
        try:
            # The initial database header read is short on a new empty file.
            assert read_bank.report()["forward"]["read_bytes"] > 0
            assert not quota.recovering
            connection.execute("CREATE TABLE sample (value BLOB)")
            connection.commit()
        finally:
            connection.close()
        assert not quota.recovering
    finally:
        gate.close()


@contextmanager
def native_file(path, gate, *, kind="database", flags=None):
    """Call the registered C methods on a real native file with valid SQLite filename metadata."""
    from tests.support.sqlite_vfs_faults import FaultVfs, IoMethods, SqliteFile

    library = FaultVfs.windows().library
    library.sqlite3_create_filename.argtypes = [
        c.c_char_p,
        c.c_char_p,
        c.c_char_p,
        c.c_int,
        c.c_void_p,
    ]
    library.sqlite3_create_filename.restype = c.c_void_p
    library.sqlite3_free_filename.argtypes = [c.c_void_p]
    library.sqlite3_free_filename.restype = None
    library.sqlite3_filename_journal.argtypes = [c.c_void_p]
    library.sqlite3_filename_journal.restype = c.c_void_p
    name = str(path.resolve()).encode("utf-8")
    filename = library.sqlite3_create_filename(name, name + b"-journal", name + b"-wal", 0, None)
    assert filename
    raw = None
    try:
        vfs = library.sqlite3_vfs_find(gate.name.encode("ascii"))
        assert vfs
        # An explicit native-alignment array keeps the delegate correctly aligned.
        allocation = (
            c.c_longdouble
            * ((vfs.contents.file_bytes + c.sizeof(c.c_longdouble) - 1) // c.sizeof(c.c_longdouble))
        )()
        raw = c.cast(allocation, c.POINTER(SqliteFile))
        selected = library.sqlite3_filename_journal(filename) if kind == "journal" else filename
        actual_flags = flags if flags is not None else (0x100 if kind == "database" else 0x800)
        out_flags = c.c_int()
        result = vfs.contents.open(vfs, selected, raw, 0x6 | actual_flags, c.byref(out_flags))
        methods = (
            c.cast(raw.contents.methods, c.POINTER(IoMethods)) if raw.contents.methods else None
        )
        yield result, raw, methods
    finally:
        try:
            if raw is not None and raw.contents.methods:
                methods = c.cast(raw.contents.methods, c.POINTER(IoMethods))
                assert methods.contents.close(raw) == sqlite3.SQLITE_OK
        finally:
            library.sqlite3_free_filename(filename)


@pytest.mark.parametrize("kind,ceiling", [("database", 8192), ("journal", 4096)])
def test_native_resize_growth_shrink_and_refusal_keep_exact_native_file(tmp_path, kind, ceiling):
    native, source = runtime()
    forward = (4096, 128, 8192, 4096, 1024 * 1024, 32)
    recovery = (4096, 128, 16384, 8192, 1024 * 1024, 32)
    quota = native.WriteQuota(forward, recovery)
    gate = native.WriteVfs(source, reads(native, quota))
    path = tmp_path / "resize.sqlite3"
    actual = path if kind == "database" else path.with_name(path.name + "-journal")
    try:
        with native_file(path, gate, kind=kind) as (result, raw, methods):
            assert result == sqlite3.SQLITE_OK
            assert methods.contents.truncate(raw, ceiling) == sqlite3.SQLITE_OK
            assert actual.stat().st_size == ceiling
            assert methods.contents.truncate(raw, 1) == sqlite3.SQLITE_OK
            original = actual.read_bytes()
            assert original == bytes(1)
            assert methods.contents.truncate(raw, ceiling + 1) == sqlite3.SQLITE_FULL
            assert actual.read_bytes() == original
            assert quota.report()["first_refusal"]["reason"] == kind + " extent grant"
            assert quota.resize_report()["forward"]["resize_calls"] == 2
            assert methods.contents.truncate(raw, 2 * ceiling) == sqlite3.SQLITE_OK
            assert actual.stat().st_size == 2 * ceiling
            assert quota.resize_report()["recovery"]["resize_calls"] == 1
        assert gate.open_files == 0
    finally:
        gate.close()


def test_native_size_hint_refuses_before_native_delegation(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota((4096, 128, 8192, 4096, 1048576, 32), grant())
    gate = native.WriteVfs(source, reads(native, quota))
    path = tmp_path / "hint.sqlite3"
    try:
        with native_file(path, gate) as (result, raw, methods):
            assert result == sqlite3.SQLITE_OK
            size = c.c_int64(8192)
            assert methods.contents.control(raw, 5, c.byref(size)) == sqlite3.SQLITE_OK
            before = path.stat().st_size
            size.value = 8193
            assert methods.contents.control(raw, 5, c.byref(size)) == sqlite3.SQLITE_FULL
            assert size.value == 8193 and path.stat().st_size == before
            assert quota.resize_report()["forward"]["resize_calls"] == 1
            assert quota.resize_report()["forward"]["database_resize_extent"] == 8192
            assert quota.report()["first_refusal"]["reason"] == "database extent grant"
    finally:
        gate.close()


def test_native_chunk_and_mmap_controls_cannot_bypass_extent_checks(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(source, reads(native, quota))
    path = tmp_path / "controls.sqlite3"
    try:
        with native_file(path, gate) as (result, raw, methods):
            assert result == sqlite3.SQLITE_OK
            chunk = c.c_int(0)
            assert methods.contents.control(raw, 6, c.byref(chunk)) == sqlite3.SQLITE_OK
            chunk.value = 4096
            assert methods.contents.control(raw, 6, c.byref(chunk)) == sqlite3.SQLITE_IOERR
            assert chunk.value == 4096
            # A delegated chunk setting would round this to 4096.
            assert methods.contents.truncate(raw, 1) == sqlite3.SQLITE_OK
            assert path.stat().st_size == 1
            mapping = c.c_int64(1048576)
            assert methods.contents.control(raw, 18, c.byref(mapping)) == sqlite3.SQLITE_IOERR
            assert mapping.value == 1048576
            mapping.value = -1
            assert methods.contents.control(raw, 18, c.byref(mapping)) == sqlite3.SQLITE_OK
            assert mapping.value == 0
            assert quota.report()["first_refusal"]["reason"] == "disallowed file control"
    finally:
        gate.close()


@pytest.mark.parametrize("operation", [23, 31, 32, 33, 43])
def test_native_alternate_handle_and_atomic_controls_fail_explicitly(tmp_path, operation):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(source, reads(native, quota))
    try:
        with native_file(tmp_path / "bypass.sqlite3", gate) as (result, raw, methods):
            assert result == sqlite3.SQLITE_OK
            assert methods.contents.control(raw, operation, None) == sqlite3.SQLITE_IOERR
            assert quota.recovering
            assert quota.report()["native_failures"] == 0
            assert quota.report()["first_refusal"]["reason"] == "disallowed file control"
    finally:
        gate.close()


def test_native_retry_policy_is_query_only_and_does_not_change_process_settings(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(source, reads(native, quota))
    try:
        with native_file(tmp_path / "retry.sqlite3", gate) as (result, raw, methods):
            assert result == sqlite3.SQLITE_OK
            query = (c.c_int * 2)(-1, -1)
            assert methods.contents.control(raw, 9, query) == sqlite3.SQLITE_OK
            previous = tuple(query)
            assert min(previous) >= 0
            changed = (c.c_int * 2)(previous[0] + 1, previous[1] + 1)
            assert methods.contents.control(raw, 9, changed) == sqlite3.SQLITE_IOERR
            query = (c.c_int * 2)(-1, -1)
            assert methods.contents.control(raw, 9, query) == sqlite3.SQLITE_OK
            assert tuple(query) == previous
    finally:
        gate.close()


@pytest.mark.parametrize(
    "flags", [0x200, 0x400, 0x1000, 0x2000, 0x4000, 0x80000, 0x900, 0x108, 0x180]
)
def test_native_unadmitted_file_class_never_creates_a_native_file(tmp_path, flags):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(source, reads(native, quota))
    path = tmp_path / "unadmitted.sqlite3"
    try:
        with native_file(path, gate, flags=flags) as (result, raw, methods):
            assert result == sqlite3.SQLITE_CANTOPEN
            assert methods is None and not raw.contents.methods
            assert not path.exists()
            assert gate.open_files == 0
            assert quota.report()["first_refusal"]["reason"] == "unadmitted file"
    finally:
        gate.close()
