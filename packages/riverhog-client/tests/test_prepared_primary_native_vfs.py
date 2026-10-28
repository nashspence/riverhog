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


def heap(native, source):
    try:
        budget = native.current_process_budget()
    except RuntimeError as exc:
        if "not configured" not in str(exc):
            raise
        budget = native.SqliteProcessBudget(source, 64 * 1024 * 1024)
    assert budget.report()["heap_limit_bytes"] == 64 * 1024 * 1024
    return budget


def callbacks(native, quota, *, forward=262144, recovery=262144, forward_sleep=0, recovery_sleep=0):
    return native.CallbackQuota(quota, (forward, forward_sleep), (recovery, recovery_sleep))


class BoundConnection:
    def __init__(self, connection, gate, api, handle):
        self.connection, self.gate, self.api, self.handle = connection, gate, api, handle
        self.closed = False

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def __enter__(self):
        return self.connection.__enter__()

    def __exit__(self, *args):
        return self.connection.__exit__(*args)

    def close(self):
        if self.closed:
            return
        self.connection.close()
        self.gate.unbind()
        self.api.close(self.handle)
        self.closed = True


def journal_identity(path, api):
    journal = path.with_name(path.name + "-journal")
    try:
        handle = api.open(journal, journal=True)
    except FileNotFoundError:
        return None
    try:
        return api.information(handle, journal, directory=False)
    finally:
        api.close(handle)


def opened(path, gate, *, readonly=False, query=""):
    from riverhog_client._prepared_primary_windows_native import _WindowsAPI

    assert not gate.bound
    path = path.resolve()
    path.touch(exist_ok=True)
    api = _WindowsAPI()
    handle = api.open(path)
    try:
        gate.bind(str(path), handle, journal_identity(path, api))
        uri = path.as_uri() + "?vfs=" + gate.name
        if readonly:
            uri += "&mode=ro"
        if query:
            uri += "&" + query
        connection = sqlite3.connect(uri, uri=True)
        return BoundConnection(connection, gate, api, handle)
    except BaseException:
        gate.unbind()
        api.close(handle)
        raise


def test_native_write_vfs_binds_stdlib_and_preserves_default_registration(tmp_path):
    from tests.support.sqlite_vfs_faults import FaultVfs

    native, source = runtime()
    library = FaultVfs.windows().library
    before = library.sqlite3_vfs_find(None)
    import ctypes

    address = ctypes.cast(before, ctypes.c_void_p).value
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
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
        assert not gate.bound
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
        native.WriteVfs(
            source + "different",
            reads(native, quota),
            callbacks(native, quota),
            heap(native, source),
        )
    assert not quota.recovering


def test_native_write_vfs_retains_its_bank_and_refuses_live_unregister(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
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
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    connection = opened(tmp_path / "primary.sqlite3", gate)
    try:
        quota.failed()
        with pytest.raises(RuntimeError, match="poisoned"):
            gate.arm(reads(native, replacement), callbacks(native, replacement))
        assert quota.recovering
    finally:
        connection.close()
    try:
        gate.arm(reads(native, replacement), callbacks(native, replacement))
        assert not replacement.recovering
    finally:
        gate.close()


def test_native_write_vfs_and_attached_bank_reject_other_thread_access(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    replacement = native.WriteQuota(grant(), grant())
    read_bank = reads(native, quota)
    gate = native.WriteVfs(source, read_bank, callbacks(native, quota), heap(native, source))
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            for operation in (
                quota.report,
                quota.resize_report,
                read_bank.report,
                gate.close,
                gate.unbind,
                lambda: gate.quarantined,
                lambda: gate.bind(str(tmp_path / "not-opened.sqlite3"), 0, None),
                lambda: gate.arm(reads(native, replacement), callbacks(native, replacement)),
            ):
                with pytest.raises(RuntimeError, match="thread"):
                    pool.submit(operation).result()

            def other_open():
                uri = (tmp_path / "other.sqlite3").resolve().as_uri() + "?vfs=" + gate.name
                connection = sqlite3.connect(uri, uri=True)
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
        uninitialized.arm(reads(native, quota), callbacks(native, quota))
    with pytest.raises(RuntimeError, match="not initialized"):
        native.WriteVfs(
            source,
            native.ReadQuota.__new__(native.ReadQuota),
            callbacks(native, quota),
            heap(native, source),
        )
    with pytest.raises(TypeError):
        native.WriteVfs(source, quota, callbacks(native, quota), heap(native, source))
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    try:
        with pytest.raises(TypeError, match="ReadQuota"):
            gate.arm(object(), callbacks(native, quota))
        with pytest.raises(RuntimeError, match="reinitialized"):
            gate.__init__(source, reads(native, quota), callbacks(native, quota))
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
    gate = native.WriteVfs(source, read_bank, callbacks(native, quota), heap(native, source))
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
    gate = native.WriteVfs(source, read_bank, callbacks(native, replacement), heap(native, source))
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
    gate = native.WriteVfs(source, read_bank, callbacks(native, quota), heap(native, source))
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
def native_file(
    path,
    gate,
    *,
    kind="database",
    flags=None,
    bind_path=None,
    binding=True,
    before_open=None,
    parameters=(),
    create=True,
):
    """Call the registered C methods on a real native file with valid SQLite filename metadata."""
    from riverhog_client._prepared_primary_windows_native import _WindowsAPI

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
    metadata = (c.c_char_p * (2 * len(parameters)))(*(part for pair in parameters for part in pair))
    filename = library.sqlite3_create_filename(
        name, name + b"-journal", name + b"-wal", len(parameters), metadata
    )
    assert filename
    raw = None
    handle = None
    api = _WindowsAPI()
    try:
        if binding:
            admitted = (bind_path if bind_path is not None else path).resolve()
            admitted.touch(exist_ok=True)
            handle = api.open(admitted)
            gate.bind(str(admitted), handle, journal_identity(admitted, api))
        if before_open is not None:
            before_open()
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
        result = vfs.contents.open(
            vfs, selected, raw, (0x6 if create else 0x2) | actual_flags, c.byref(out_flags)
        )
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
            try:
                if binding:
                    gate.unbind()
                if handle is not None:
                    api.close(handle)
            finally:
                library.sqlite3_free_filename(filename)


@pytest.mark.parametrize("kind,ceiling", [("database", 8192), ("journal", 4096)])
def test_native_resize_growth_shrink_and_refusal_keep_exact_native_file(tmp_path, kind, ceiling):
    native, source = runtime()
    forward = (4096, 128, 8192, 4096, 1024 * 1024, 32)
    recovery = (4096, 128, 16384, 8192, 1024 * 1024, 32)
    quota = native.WriteQuota(forward, recovery)
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
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
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
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
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
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


@pytest.mark.parametrize("operation", [16, 23, 31, 32, 33, 43])
def test_native_alternate_handle_and_atomic_controls_fail_explicitly(tmp_path, operation):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
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
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
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
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path = tmp_path / "unadmitted.sqlite3"
    try:
        with native_file(path, gate, flags=flags, bind_path=tmp_path / "admitted.sqlite3") as (
            result,
            raw,
            methods,
        ):
            assert result == sqlite3.SQLITE_CANTOPEN
            assert methods is None and not raw.contents.methods
            assert not path.exists()
            assert gate.open_files == 0
            assert quota.report()["first_refusal"]["reason"] == "unadmitted file"
    finally:
        gate.close()


def test_native_unbound_gate_refuses_disk_open_before_creating_file(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path = tmp_path / "unbound.sqlite3"
    try:
        assert not gate.bound
        with pytest.raises(sqlite3.OperationalError):
            connection = sqlite3.connect(path.as_uri() + "?vfs=" + gate.name, uri=True)
            connection.close()
        assert not path.exists() and gate.open_files == 0
        assert quota.report()["first_refusal"]["reason"] == "unadmitted file"
    finally:
        gate.close()


def test_native_bound_gate_refuses_another_main_database_before_creation(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path = tmp_path / "unadmitted.sqlite3"
    try:
        with native_file(path, gate, bind_path=tmp_path / "admitted.sqlite3") as (rc, raw, methods):
            assert rc == sqlite3.SQLITE_CANTOPEN
            assert methods is None and not raw.contents.methods
            assert not path.exists()
    finally:
        gate.close()


def test_native_binding_rejects_mismatched_handle_and_preserves_pin_until_unbound(tmp_path):
    from riverhog_client._prepared_primary_windows_native import _WindowsAPI

    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path, other = tmp_path / "admitted.sqlite3", tmp_path / "other.sqlite3"
    path.touch()
    other.touch()
    api = _WindowsAPI()
    handle = api.open(path)
    try:
        with pytest.raises(RuntimeError, match="identity"):
            gate.bind(str(other), handle, None)
        assert not gate.bound
        api.close(handle)
        handle = None
        with pytest.raises(OSError):
            path.rename(path.with_name("quarantined.sqlite3"))
        gate.unbind()
        handle = api.open(path)
        gate.bind(str(path), handle, journal_identity(path, api))
        api.close(handle)
        handle = None
        moved = path.with_name("moved.sqlite3")
        with pytest.raises(OSError):
            path.rename(moved)
        assert gate.bound
        with pytest.raises(RuntimeError, match="unbound"):
            gate.bind(str(path), 0, None)
        gate.unbind()
        assert not gate.bound
        path.rename(moved)
        moved.rename(path)
    finally:
        gate.unbind()
        if handle is not None:
            api.close(handle)
        gate.close()


def test_native_live_binding_cannot_unbind_and_rejects_database_delete(tmp_path):
    from tests.support.sqlite_vfs_faults import FaultVfs

    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path = tmp_path / "admitted.sqlite3"
    try:
        with native_file(path, gate) as (rc, raw, methods):
            assert rc == sqlite3.SQLITE_OK
            with pytest.raises(RuntimeError, match="live files"):
                gate.unbind()
            library = FaultVfs.windows().library
            vfs = library.sqlite3_vfs_find(gate.name.encode("ascii"))
            name = c.create_string_buffer(str(path.resolve()).encode("utf-8"))
            assert vfs.contents.delete(vfs, c.cast(name, c.c_void_p), 0) == sqlite3.SQLITE_IOERR
            assert path.exists()
    finally:
        gate.close()


def test_native_binding_close_releases_its_duplicate_database_pin(tmp_path):
    from riverhog_client._prepared_primary_windows_native import _WindowsAPI

    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path = tmp_path / "admitted.sqlite3"
    path.touch()
    api = _WindowsAPI()
    handle = api.open(path)
    try:
        gate.bind(str(path), handle, journal_identity(path, api))
    finally:
        api.close(handle)
    try:
        assert gate.bound
    finally:
        gate.close()
    moved = path.with_name("moved.sqlite3")
    path.rename(moved)
    moved.rename(path)


def test_native_journal_hardlink_is_rejected_before_any_content_write(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path = tmp_path / "admitted.sqlite3"
    foreign = tmp_path / "foreign"
    foreign.write_bytes(b"unrelated original")
    journal = path.with_name(path.name + "-journal")
    try:
        with native_file(
            path, gate, kind="journal", before_open=lambda: os.link(foreign, journal)
        ) as (rc, raw, methods):
            assert rc == sqlite3.SQLITE_CANTOPEN
            assert methods is None and not raw.contents.methods
            assert foreign.read_bytes() == b"unrelated original"
            assert journal.read_bytes() == b"unrelated original"
            assert quota.report()["first_refusal"]["reason"] == "unadmitted file"
        assert gate.open_files == 0
    finally:
        gate.close()


def native_delete(gate, path):
    from tests.support.sqlite_vfs_faults import FaultVfs

    library = FaultVfs.windows().library
    vfs = library.sqlite3_vfs_find(gate.name.encode("ascii"))
    name = c.create_string_buffer(str(path.resolve()).encode("utf-8"))
    return vfs.contents.delete(vfs, c.cast(name, c.c_void_p), 0)


@pytest.mark.parametrize("change", ["replace", "remove", "appear"])
def test_native_journal_generation_change_refuses_before_open_or_creation(tmp_path, change):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path = tmp_path / "admitted.sqlite3"
    journal = path.with_name(path.name + "-journal")
    retained = tmp_path / "retained"
    if change != "appear":
        journal.write_bytes(b"original journal")

    def changed():
        if change == "replace":
            journal.rename(retained)
        elif change == "remove":
            journal.unlink()
            return
        journal.write_bytes(b"unrelated replacement")

    try:
        with native_file(path, gate, kind="journal", before_open=changed) as (rc, raw, methods):
            assert rc == sqlite3.SQLITE_CANTOPEN
            assert methods is None and not raw.contents.methods
            assert quota.report()["first_refusal"]["reason"] == "unadmitted file"
            if change != "remove":
                assert journal.read_bytes() == b"unrelated replacement"
            if change == "replace":
                assert retained.read_bytes() == b"original journal"
        if change == "remove":
            # Delete-sharing metadata pins keep deletion pending until unbind.
            assert not journal.exists()
    finally:
        gate.close()


def test_native_journal_replacement_cannot_be_deleted_as_the_original(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path = tmp_path / "admitted.sqlite3"
    journal = path.with_name(path.name + "-journal")
    retained = tmp_path / "retained"
    journal.write_bytes(b"original journal")
    try:
        with native_file(path, gate) as (rc, _raw, _methods):
            assert rc == sqlite3.SQLITE_OK
            journal.rename(retained)
            journal.write_bytes(b"unrelated replacement")
            assert native_delete(gate, journal) == sqlite3.SQLITE_IOERR
            assert journal.read_bytes() == b"unrelated replacement"
            assert retained.read_bytes() == b"original journal"
            assert quota.report()["first_refusal"]["reason"] == "unadmitted file"
    finally:
        gate.close()


def test_native_successful_journal_delete_authorizes_one_native_recreation(tmp_path):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path = tmp_path / "admitted.sqlite3"
    journal = path.with_name(path.name + "-journal")
    try:
        with native_file(path, gate, kind="journal") as (rc, raw, methods):
            assert rc == sqlite3.SQLITE_OK
            assert methods.contents.truncate(raw, 4096) == sqlite3.SQLITE_OK
            assert methods.contents.close(raw) == sqlite3.SQLITE_OK
            assert not raw.contents.methods and gate.open_files == 0
            assert native_delete(gate, journal) == sqlite3.SQLITE_OK
            assert not journal.exists()
            with native_file(path, gate, kind="journal", binding=False) as (rc, raw, methods):
                assert rc == sqlite3.SQLITE_OK
                assert methods.contents.truncate(raw, 2048) == sqlite3.SQLITE_OK
                assert journal.stat().st_size == 2048
            assert not quota.recovering
    finally:
        gate.close()


def test_native_failed_journal_delete_retains_generation_for_retry(tmp_path):
    from riverhog_client._prepared_primary_windows_native import _WindowsAPI

    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path = tmp_path / "admitted.sqlite3"
    journal = path.with_name(path.name + "-journal")
    api = _WindowsAPI()
    try:
        with native_file(path, gate, kind="journal") as (rc, raw, methods):
            assert rc == sqlite3.SQLITE_OK
            assert methods.contents.truncate(raw, 4096) == sqlite3.SQLITE_OK
            assert methods.contents.close(raw) == sqlite3.SQLITE_OK
            original = journal.read_bytes()
            pin = api.open(journal)
            try:
                assert native_delete(gate, journal) != sqlite3.SQLITE_OK
                assert quota.recovering and quota.report()["native_failures"] == 1
                assert journal.read_bytes() == original
            finally:
                api.close(pin)
            with native_file(path, gate, kind="journal", binding=False) as (rc, _raw, _methods):
                assert rc == sqlite3.SQLITE_OK
                assert journal.read_bytes() == original
            assert native_delete(gate, journal) == sqlite3.SQLITE_OK
            assert not journal.exists()
    finally:
        gate.close()


def test_native_binding_requires_the_original_journal_identity_or_explicit_absence(tmp_path):
    from riverhog_client._prepared_primary_windows_native import _WindowsAPI

    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    path = tmp_path / "admitted.sqlite3"
    path.touch()
    journal = path.with_name(path.name + "-journal")
    journal.write_bytes(b"original journal")
    api = _WindowsAPI()
    handle = api.open(path)
    try:
        expected = journal_identity(path, api)
        assert expected is not None
        wrong = (expected[0], expected[1], expected[2] ^ 1)
        for identity in (None, wrong):
            with pytest.raises(RuntimeError, match="identity"):
                gate.bind(str(path), handle, identity)
            assert not gate.bound and journal.read_bytes() == b"original journal"
            with pytest.raises(RuntimeError, match="unbound"):
                gate.bind(str(path), handle, expected)
            gate.unbind()
        gate.bind(str(path), handle, expected)
        assert gate.bound
    finally:
        gate.unbind()
        api.close(handle)
        gate.close()


@pytest.mark.parametrize(
    "identity,error",
    [
        (True, TypeError),
        ((1, 2), TypeError),
        ([1, 2, 3], TypeError),
        ((1, 2, True), TypeError),
        ((-1, 2, 3), OverflowError),
        ((2**32, 2, 3), OverflowError),
    ],
)
def test_native_journal_identity_rejects_malformed_authority_before_binding(
    tmp_path, identity, error
):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    try:
        with pytest.raises(error, match="journal|unsigned"):
            gate.bind(str(tmp_path / "unopened.sqlite3"), 0, identity)
        assert not gate.bound
    finally:
        gate.close()


@pytest.mark.parametrize(
    "method",
    [
        "size",
        "lock",
        "unlock",
        "reserved",
        "control",
        "sector",
        "characteristics",
        "sync",
        "read",
        "write",
        "truncate",
    ],
)
def test_native_callback_exhaustion_blocks_file_delegation_but_pays_for_close(tmp_path, method):
    native, source = runtime()
    initial = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, initial), callbacks(native, initial), heap(native, source)
    )
    quota = native.WriteQuota(grant(), grant())
    calls = callbacks(native, quota, forward=1, recovery=1)
    try:
        with native_file(tmp_path / "callbacks.sqlite3", gate) as (rc, raw, methods):
            assert rc == sqlite3.SQLITE_OK
            gate.arm(reads(native, quota), calls)
            assert calls.report()["forward"]["callback_slots_charged"] == 1
            assert calls.report()["reserved_recovery_closes"] == 1
            value = c.c_int(42)
            size = c.c_int64(42)
            buffer = c.create_string_buffer(b"canary")
            arguments = {
                "size": (c.byref(size),),
                "lock": (1,),
                "unlock": (0,),
                "reserved": (c.byref(value),),
                "control": (1, c.byref(value)),
                "sector": (),
                "characteristics": (),
                "sync": (3,),
                "read": (buffer, 1, 0),
                "write": (buffer, 1, 0),
                "truncate": (4096,),
            }[method]
            result = getattr(methods.contents, method)(raw, *arguments)
            if method in ("sector", "characteristics"):
                assert result == 0 and calls.report()["pending_error"]
                assert methods.contents.size(raw, c.byref(size)) == sqlite3.SQLITE_FULL
            else:
                assert result == sqlite3.SQLITE_FULL
            assert not calls.report()["pending_error"]
            assert calls.report()["recovery"]["callback_slots_charged"] == 0
            assert size.value == value.value == 42 and buffer.value == b"canary"
            assert quota.report()["first_refusal"]["reason"] == "VFS callback grant"
            assert quota.report()["native_failures"] == 0
        assert gate.open_files == 0
        assert calls.report()["reserved_recovery_closes"] == 0
        assert calls.report()["recovery"]["callback_slots_charged"] == 1
    finally:
        gate.close()


@pytest.mark.parametrize("method", ["access", "pathname", "randomness", "sleep", "time", "error"])
def test_native_callback_exhaustion_blocks_vfs_delegation_and_surfaces_infallible_error(
    tmp_path, method
):
    from tests.support.sqlite_vfs_faults import FaultVfs

    native, source = runtime()
    initial = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, initial), callbacks(native, initial), heap(native, source)
    )
    quota = native.WriteQuota(grant(), grant())
    calls = callbacks(native, quota, forward=1, recovery=1)
    path = tmp_path / "callbacks.sqlite3"
    try:
        with native_file(path, gate) as (rc, raw, methods):
            assert rc == sqlite3.SQLITE_OK
            gate.arm(reads(native, quota), calls)
            vfs = FaultVfs.windows().library.sqlite3_vfs_find(gate.name.encode("ascii"))
            name = c.create_string_buffer(str(path.resolve()).encode("utf-8"))
            output = c.create_string_buffer(b"canary", 2048)
            value = c.c_int(42)
            time = c.c_double(42)
            arguments = {
                "access": (name, 0, c.byref(value)),
                "pathname": (name, len(output), output),
                "randomness": (6, output),
                "sleep": (1,),
                "time": (c.byref(time),),
                "error": (len(output), output),
            }[method]
            result = getattr(vfs.contents, method)(vfs, *arguments)
            if method in ("access", "pathname", "time"):
                assert result == sqlite3.SQLITE_FULL
            else:
                assert result == 0 and calls.report()["pending_error"]
                size = c.c_int64()
                assert methods.contents.size(raw, c.byref(size)) == sqlite3.SQLITE_FULL
            assert value.value == time.value == 42
            assert output.value == (b"" if method == "error" else b"canary")
            assert calls.report()["recovery"]["callback_slots_charged"] == 0
        assert calls.report()["recovery"]["callback_slots_charged"] == 1
    finally:
        gate.close()


def test_native_dynamic_loading_never_acquires_a_library_handle(tmp_path):
    from tests.support.sqlite_vfs_faults import FaultVfs

    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    calls = callbacks(native, quota)
    gate = native.WriteVfs(source, reads(native, quota), calls, heap(native, source))
    try:
        with native_file(tmp_path / "callbacks.sqlite3", gate) as (rc, raw, methods):
            assert rc == sqlite3.SQLITE_OK
            vfs = FaultVfs.windows().library.sqlite3_vfs_find(gate.name.encode("ascii"))
            library = c.create_string_buffer(b"kernel32.dll")
            symbol = c.create_string_buffer(b"GetCurrentProcess")
            assert not vfs.contents.dlopen(vfs, library)
            assert not vfs.contents.dlsym(vfs, None, symbol)
            output = c.create_string_buffer(b"canary")
            vfs.contents.dlerror(vfs, len(output), output)
            assert output.value == b""
            vfs.contents.dlclose(vfs, None)
            assert calls.report()["pending_error"] and quota.recovering
            size = c.c_int64()
            assert methods.contents.size(raw, c.byref(size)) == sqlite3.SQLITE_FULL
            assert not calls.report()["pending_error"]
            assert quota.report()["first_refusal"]["reason"] == "disallowed file control"
            assert quota.report()["native_failures"] == 0
    finally:
        gate.close()


def test_native_rearm_refuses_inadequate_close_capacity_without_replacing_the_bank(tmp_path):
    native, source = runtime()
    initial = native.WriteQuota(grant(), grant())
    initial_calls = callbacks(native, initial)
    gate = native.WriteVfs(source, reads(native, initial), initial_calls, heap(native, source))
    quota = native.WriteQuota(grant(), grant())
    short = callbacks(native, quota, forward=16, recovery=1)
    path = tmp_path / "callbacks.sqlite3"
    try:
        with native_file(path, gate) as (rc, _raw, _methods):
            assert rc == sqlite3.SQLITE_OK
            with native_file(path, gate, kind="journal", binding=False) as (rc, raw, methods):
                assert rc == sqlite3.SQLITE_OK and gate.open_files == 2
                with pytest.raises(RuntimeError, match="close capacity"):
                    gate.arm(reads(native, quota), short)
                assert short.report()["forward"]["callback_slots_charged"] == 0
                assert short.report()["reserved_recovery_closes"] == 0
                assert initial_calls.report()["reserved_recovery_closes"] == 2
                assert methods.contents.truncate(raw, 4096) == sqlite3.SQLITE_OK
                assert not initial.recovering
        assert initial_calls.report()["reserved_recovery_closes"] == 0
    finally:
        gate.close()


def test_native_attached_callback_reservations_cannot_be_mutated_from_python():
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    calls = callbacks(native, quota)
    gate = native.WriteVfs(source, reads(native, quota), calls, heap(native, source))
    try:
        for mutate in (
            calls.admit,
            lambda: calls.admit_sleep(0),
            calls.opened,
            lambda: calls.closed(True),
            lambda: calls.abandoned(True),
        ):
            with pytest.raises(RuntimeError, match="attached"):
                mutate()
    finally:
        gate.close()


def test_native_read_and_callback_grants_require_the_same_write_owner():
    native, source = runtime()
    first = native.WriteQuota(grant(), grant())
    other = native.WriteQuota(grant(), grant())
    with pytest.raises(ValueError, match="same write owner"):
        native.WriteVfs(
            source, reads(native, first), callbacks(native, other), heap(native, source)
        )


@pytest.mark.parametrize("scenario", ["aggregate", "increase", "existing", "lowered", "accounting"])
def test_native_process_heap_policy_runs_in_an_isolated_application(tmp_path, scenario):
    import json
    import subprocess
    import sys

    code = r"""
import ctypes as c,json,sqlite3,sys
from pathlib import Path
from riverhog_client import _prepared_primary_native as native
connection=sqlite3.connect(":memory:")
source=connection.execute("SELECT sqlite_source_id()").fetchone()[0]
connection.close()
kernel=c.WinDLL("kernel32",use_last_error=True)
kernel.GetModuleHandleW.argtypes=[c.c_wchar_p]
kernel.GetModuleHandleW.restype=c.c_void_p
handle=kernel.GetModuleHandleW("sqlite3.dll")
assert handle
library=c.CDLL("sqlite3.dll",handle=handle)
library.sqlite3_malloc64.argtypes=[c.c_uint64]
library.sqlite3_malloc64.restype=c.c_void_p
library.sqlite3_free.argtypes=[c.c_void_p]
library.sqlite3_free.restype=None
library.sqlite3_hard_heap_limit64.argtypes=[c.c_int64]
library.sqlite3_hard_heap_limit64.restype=c.c_int64
scenario=sys.argv[1]
limit=2*1024*1024
if scenario=="accounting":
    library.sqlite3_shutdown.restype=c.c_int
    library.sqlite3_initialize.restype=c.c_int
    library.sqlite3_config.restype=c.c_int
    assert library.sqlite3_shutdown()==0
    assert library.sqlite3_config(9,0)==0
    assert library.sqlite3_initialize()==0
    try:native.SqliteProcessBudget(source,limit)
    except RuntimeError as exc:assert "accounting" in str(exc)
    else:raise AssertionError("disabled accounting was accepted")
    assert library.sqlite3_hard_heap_limit64(-1)==0
elif scenario=="existing":
    assert library.sqlite3_hard_heap_limit64(limit)==0
    try:native.SqliteProcessBudget(source,2*limit)
    except RuntimeError as exc:assert "unconfigured" in str(exc)
    else:raise AssertionError("existing global authority was replaced")
    assert library.sqlite3_hard_heap_limit64(-1)==limit
else:
    # A failed undersized admission does not arm a limit or retain an owner.
    try:native.SqliteProcessBudget(source,1)
    except RuntimeError as exc:assert "below current" in str(exc)
    else:raise AssertionError("an undersized grant was accepted")
    assert library.sqlite3_hard_heap_limit64(-1)==0
    owner=native.SqliteProcessBudget(source,limit)
    assert native.current_process_budget() is owner
    try:native.SqliteProcessBudget(source,2*limit)
    except RuntimeError as exc:assert "already" in str(exc)
    else:raise AssertionError("second process owner was admitted")
    if scenario=="lowered":
        library.sqlite3_hard_heap_limit64(limit//2)
        try:owner.report()
        except RuntimeError as exc:assert "changed" in str(exc)
        else:raise AssertionError("changed global profile was accepted")
        writes=native.WriteQuota((4096,8,32768,32768,65536,32),(4096,8,32768,32768,65536,32))
        reads=native.ReadQuota(writes,(65536,32),(65536,32))
        calls=native.CallbackQuota(writes,(128, 0),(128, 0))
        try:native.WriteVfs(source,reads,calls,owner)
        except RuntimeError as exc:assert "heap owner" in str(exc)
        else:raise AssertionError("native files admitted under a changed global profile")
    else:
        primary=sqlite3.connect("primary.sqlite3")
        other=sqlite3.connect(":memory:")
        primary.execute("CREATE TABLE original(value BLOB)")
        primary.execute("INSERT INTO original VALUES (?)",(b"exact original",))
        primary.commit()
        held=library.sqlite3_malloc64(limit//2)
        assert held
        before=owner.report()
        assert before["sqlite_heap_bytes"]>=limit//2
        try:
            primary.execute("UPDATE original SET value=?",(b"uncommitted",))
            try:primary.execute("SELECT randomblob(?)",(limit//2,)).fetchone()
            except (MemoryError,sqlite3.Error) as exc:
                if isinstance(exc,sqlite3.Error):
                    assert exc.sqlite_errorcode&255==sqlite3.SQLITE_NOMEM
            else:raise AssertionError("aggregate heap grant was not enforced")
            if primary.in_transaction:primary.rollback()
            assert primary.execute("SELECT value FROM original").fetchone()==(b"exact original",)
            assert owner.report()["sqlite_heap_peak_bytes"]<=limit
            if scenario=="increase":
                for smaller in (limit,limit//2):
                    try:owner.increase(smaller)
                    except ValueError:pass
                    else:raise AssertionError("a repeated or lower heap grant was accepted")
                owner.increase(2*limit)
                assert owner.report()["heap_limit_bytes"]==2*limit
                row=primary.execute("SELECT randomblob(?)",(limit//2,)).fetchone()
                assert len(row[0])==limit//2
                del row
                limit*=2
        finally:library.sqlite3_free(held)
        assert len(primary.execute("SELECT randomblob(?)",(limit//2,)).fetchone()[0])==limit//2
        primary.close();other.close()
        cold=sqlite3.connect("primary.sqlite3")
        assert cold.execute("SELECT value FROM original").fetchone()==(b"exact original",)
        cold.close()
        assert owner.report()["sqlite_heap_peak_bytes"]<=limit
print(json.dumps({"scenario":scenario,"passed":True}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, scenario],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {"scenario": scenario, "passed": True}


def test_native_infallible_refusal_can_surface_on_close_without_stranding_the_file(tmp_path):
    native, source = runtime()
    initial = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, initial), callbacks(native, initial), heap(native, source)
    )
    writes = native.WriteQuota(grant(), grant())
    calls = callbacks(native, writes, forward=1, recovery=1)
    try:
        with native_file(tmp_path / "close.sqlite3", gate) as (rc, raw, methods):
            assert rc == sqlite3.SQLITE_OK
            gate.arm(reads(native, writes), calls)
            assert methods.contents.sector(raw) == 0
            assert calls.report()["pending_error"]
            assert methods.contents.close(raw) == sqlite3.SQLITE_FULL
            assert not raw.contents.methods and gate.open_files == 0
            assert not gate.quarantined
            assert not calls.report()["pending_error"]
            assert calls.report()["recovery"]["callback_slots_charged"] == 1
            assert writes.report()["native_failures"] == 0
    finally:
        gate.close()


@pytest.mark.parametrize(
    "query",
    [
        "immutable=1",
        "immutable=0",
        "im%6Dutable=1",
        "nolock=1",
        "psow=1",
        "psow=0",
        "exclusive=1",
        "cache=shared",
        "cache=private",
        "unknown=value",
        "mode=rw&mode=ro",
        "vfs={gate}",
    ],
)
def test_native_uri_policy_rejects_before_open_and_preserves_original(tmp_path, query):
    native, source = runtime()
    path = tmp_path / "uri.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE original(value BLOB)")
        connection.execute("INSERT INTO original VALUES (?)", (b"exact original",))
        connection.commit()
    finally:
        connection.close()
    original = path.read_bytes()
    quota = native.WriteQuota(grant(), grant())
    calls = callbacks(native, quota)
    gate = native.WriteVfs(source, reads(native, quota), calls, heap(native, source))
    try:
        with pytest.raises(sqlite3.DatabaseError):
            connection = opened(path, gate, query=query.format(gate=gate.name))
            connection.close()
        assert gate.open_files == 0 and not gate.bound
        assert quota.recovering
        assert quota.report()["first_refusal"]["reason"] == "unadmitted file"
        assert quota.report()["native_failures"] == 0
        # Only bounded path expansion can precede admission; open/close was not charged.
        assert calls.report()["reserved_recovery_closes"] == 0
        assert path.read_bytes() == original
        cold = sqlite3.connect(path)
        try:
            assert cold.execute("SELECT value FROM original").fetchone() == (b"exact original",)
        finally:
            cold.close()
    finally:
        gate.close()


@pytest.mark.parametrize("kind", ["database", "journal"])
@pytest.mark.parametrize(
    "parameters",
    [
        ((b"immutable", b"1"),),
        ((b"vfs", b"win32"),),
        ((b"mode", b"rw"), (b"mode", b"ro")),
        ((b"vfs", b""),),
        ((b"mode", b""),),
    ],
)
def test_native_uri_metadata_refusal_never_delegates_open(tmp_path, kind, parameters):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    calls = callbacks(native, quota)
    gate = native.WriteVfs(source, reads(native, quota), calls, heap(native, source))
    path = tmp_path / "metadata.sqlite3"
    try:
        with native_file(path, gate, kind=kind, parameters=parameters) as (result, raw, methods):
            assert result == sqlite3.SQLITE_CANTOPEN
            assert methods is None and not raw.contents.methods
            assert gate.open_files == 0
            assert quota.recovering and quota.report()["native_failures"] == 0
            assert calls.report()["forward"]["callback_slots_charged"] == 0
            assert calls.report()["reserved_recovery_closes"] == 0
            assert not path.with_name(path.name + "-journal").exists()
    finally:
        gate.close()


@pytest.mark.parametrize("kind", ["database", "journal"])
@pytest.mark.parametrize("mode", [b"ro", b"rw", b"rwc"])
def test_native_admitted_uri_metadata_reaches_the_original_native_file(tmp_path, kind, mode):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    try:
        parameters = ((b"mode", mode), (b"vfs", gate.name.encode("ascii")))
        with native_file(
            tmp_path / "admitted-uri.sqlite3", gate, kind=kind, parameters=parameters
        ) as (result, _raw, methods):
            assert result == sqlite3.SQLITE_OK and methods is not None
            assert gate.open_files == 1 and not quota.recovering
    finally:
        gate.close()


@pytest.mark.parametrize("operation", [10, 13])
@pytest.mark.parametrize("setting", [None, 0, 1])
def test_native_mode_controls_are_query_only_and_preserve_native_flags(
    tmp_path, operation, setting
):
    native, source = runtime()
    quota = native.WriteQuota(grant(), grant())
    gate = native.WriteVfs(
        source, reads(native, quota), callbacks(native, quota), heap(native, source)
    )
    try:
        with native_file(tmp_path / "native-flags.sqlite3", gate) as (result, raw, methods):
            assert result == sqlite3.SQLITE_OK
            query = c.c_int(-1)
            assert methods.contents.control(raw, operation, c.byref(query)) == sqlite3.SQLITE_OK
            previous = query.value
            device = methods.contents.characteristics(raw)
            value = None if setting is None else c.c_int(setting)
            argument = None if value is None else c.byref(value)
            assert methods.contents.control(raw, operation, argument) == sqlite3.SQLITE_IOERR
            query.value = -1
            assert methods.contents.control(raw, operation, c.byref(query)) == sqlite3.SQLITE_OK
            assert query.value == previous
            assert methods.contents.characteristics(raw) == device
            assert quota.recovering and quota.report()["native_failures"] == 0
            assert quota.report()["first_refusal"]["reason"] == "disallowed file control"
    finally:
        gate.close()


@pytest.mark.parametrize("failed_kind", ["database", "journal"])
def test_native_close_failure_quarantines_owner_until_application_exit(tmp_path, failed_kind):
    import json
    import subprocess
    import sys
    from pathlib import Path

    path = tmp_path / "close-quarantine.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE original(value BLOB)")
        connection.execute("INSERT INTO original VALUES (?)", (b"exact original",))
        connection.commit()
    finally:
        connection.close()
    original = path.read_bytes()
    code = r"""
import ctypes as c, json, sqlite3, sys
from pathlib import Path

sys.path.insert(0, sys.argv[1])
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_windows_native import _WindowsAPI

import importlib.util

spec = importlib.util.spec_from_file_location(
    "native_vfs_probe",
    Path(sys.argv[1]) / "packages/riverhog-client/tests/test_prepared_primary_native_vfs.py",
)
helpers = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = helpers
spec.loader.exec_module(helpers)
from tests.support.sqlite_vfs_faults import FaultVfs, windows_system_call

connection = sqlite3.connect(":memory:")
try:
    source = connection.execute("SELECT sqlite_source_id()").fetchone()[0]
finally:
    connection.close()
heap = native.SqliteProcessBudget(source, 64 * 1024 * 1024)
quota = native.WriteQuota(helpers.grant(), helpers.grant())
reads = native.ReadQuota(quota, (1048576, 65536), (1048576, 65536))
calls = native.CallbackQuota(quota, (262144, 0), (262144, 0))
gate = native.WriteVfs(source, reads, calls, heap)
path = Path(sys.argv[2])
failed_kind = sys.argv[3]
api = _WindowsAPI()
borrowed = api.open(path)
gate.bind(str(path), borrowed, None)
api.close(borrowed)
library = FaultVfs.windows().library
close_type = c.WINFUNCTYPE(c.c_int, c.c_void_p, use_last_error=True)
target = None
attempts = 0


def refuse_close(real_close, handle):
    global attempts
    if handle == target:
        attempts += 1
        c.set_last_error(5)
        return 0
    return real_close(handle)


with helpers.native_file(path, gate, binding=False) as database:
    with helpers.native_file(path, gate, kind="journal", binding=False) as journal:
        assert database[0] == journal[0] == sqlite3.SQLITE_OK
        selected = database if failed_kind == "database" else journal
        other = journal if failed_kind == "database" else database
        handle = c.c_void_p()
        assert selected[2].contents.control(selected[1], 29, c.byref(handle)) == sqlite3.SQLITE_OK
        target = handle.value
        assert target
        assert gate.open_files == 2 and not gate.quarantined
        assert calls.report()["reserved_recovery_closes"] == 2
        with windows_system_call(b"CloseHandle", close_type, refuse_close) as real_close:
            rc = selected[2].contents.close(selected[1])
        assert rc == sqlite3.SQLITE_IOERR_CLOSE and attempts == 3
        assert gate.quarantined and gate.open_files == 1 and gate.bound
        assert not selected[1].contents.methods
        assert quota.recovering and quota.report()["native_failures"] == 1
        before = calls.report()
        size = c.c_int64(-7)
        assert other[2].contents.size(other[1], c.byref(size)) == sqlite3.SQLITE_FULL
        assert size.value == -7 and calls.report() == before
        with helpers.native_file(path, gate, binding=False) as refused:
            assert refused[0] == sqlite3.SQLITE_IOERR and refused[2] is None
        assert calls.report() == before
        replacement = native.WriteQuota(helpers.grant(), helpers.grant())
        replacement_reads = native.ReadQuota(replacement, (1048576, 65536), (1048576, 65536))
        replacement_calls = native.CallbackQuota(replacement, (262144, 0), (262144, 0))
        for operation in (
            lambda: gate.arm(reads, calls),
            lambda: gate.arm(replacement_reads, replacement_calls),
            gate.unbind,
            gate.close,
        ):
            try:
                operation()
            except RuntimeError as exc:
                assert "quarantined" in str(exc)
            else:
                raise AssertionError("quarantined owner was released or rearmed")
        assert replacement_calls.report()["forward"]["callback_slots_charged"] == 0
        assert other[2].contents.close(other[1]) == sqlite3.SQLITE_OK
        assert not other[1].contents.methods and gate.open_files == 0
        assert calls.report()["reserved_recovery_closes"] == 0
        assert calls.report()["recovery"]["callback_slots_charged"] == 1
        # The isolated hook never delegated this handle's close. Test-only cleanup
        # can therefore release that known still-live handle after restoring it.
        assert real_close(target)
        for operation in (gate.unbind, gate.close):
            try:
                operation()
            except RuntimeError as exc:
                assert "quarantined" in str(exc)
            else:
                raise AssertionError("uncertain native teardown was reported complete")
        assert gate.quarantined and gate.bound
        assert library.sqlite3_vfs_find(gate.name.encode("ascii"))
        try:
            path.rename(path.with_suffix(".moved"))
        except PermissionError:
            pass
        else:
            raise AssertionError("quarantined database pin was released")
print(json.dumps({"passed": True, "failed_kind": failed_kind, "native_close_attempts": attempts}))
"""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            code,
            str(Path(__file__).resolve().parents[3]),
            str(path),
            failed_kind,
        ],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {
        "passed": True,
        "failed_kind": failed_kind,
        "native_close_attempts": 3,
    }
    assert path.read_bytes() == original
    moved = path.with_suffix(".moved")
    path.rename(moved)
    cold = sqlite3.connect(moved)
    try:
        assert cold.execute("SELECT value FROM original").fetchone() == (b"exact original",)
    finally:
        cold.close()


@pytest.mark.parametrize("scenario", ["rounding", "range", "reserved_close"])
def test_native_sleep_envelope_runs_in_an_isolated_application(tmp_path, scenario):
    import json
    import subprocess
    import sys
    from pathlib import Path

    code = r"""
import ctypes as c, importlib.util, json, sqlite3, sys
from pathlib import Path

sys.path.insert(0, sys.argv[1])
from riverhog_client import _prepared_primary_native as native
from tests.support.sqlite_vfs_faults import FaultVfs, windows_system_call

spec = importlib.util.spec_from_file_location(
    "native_vfs_probe",
    Path(sys.argv[1]) / "packages/riverhog-client/tests/test_prepared_primary_native_vfs.py",
)
helpers = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = helpers
spec.loader.exec_module(helpers)
connection = sqlite3.connect(":memory:")
try:
    source = connection.execute("SELECT sqlite_source_id()").fetchone()[0]
finally:
    connection.close()
heap = native.SqliteProcessBudget(source, 64 * 1024 * 1024)
library = FaultVfs.windows().library
observed = []


def nominal_sleep(original, milliseconds):
    observed.append(milliseconds)


def setup(forward, recovery):
    quota = native.WriteQuota(helpers.grant(), helpers.grant())
    calls = native.CallbackQuota(quota, (32, forward), (32, recovery))
    gate = native.WriteVfs(source, helpers.reads(native, quota), calls, heap)
    vfs = library.sqlite3_vfs_find(gate.name.encode("ascii"))
    return quota, calls, gate, vfs


def surface(vfs):
    time = c.c_double(-7)
    assert vfs.contents.time(vfs, c.byref(time)) == sqlite3.SQLITE_FULL
    assert time.value == -7


scenario = sys.argv[3]
vectors = 0
with windows_system_call(
    b"Sleep", c.WINFUNCTYPE(None, c.c_ulong, use_last_error=True), nominal_sleep
):
    if scenario == "rounding":
        for requested in (0, 1, 999, 1000, 1001, 2**31 - 1 - 999):
            rounded = ((requested + 999) // 1000) * 1000
            quota, calls, gate, vfs = setup(rounded, 1000)
            before = list(observed)
            try:
                assert vfs.contents.sleep(vfs, requested) == rounded
                assert observed == before + [rounded // 1000]
                assert calls.report()["forward"]["sleep_microseconds_charged"] == rounded
                assert not quota.recovering
                before = list(observed)
                assert vfs.contents.sleep(vfs, 1) == 0
                assert observed == before and calls.report()["pending_error"]
                assert quota.report()["first_refusal"]["reason"] == "VFS sleep grant"
                assert vfs.contents.sleep(vfs, 1) == 0 and observed == before
                assert calls.report()["recovery"]["sleep_microseconds_charged"] == 0
                surface(vfs)
                assert vfs.contents.sleep(vfs, 1) == 1000 and observed == before + [1]
                assert calls.report()["recovery"]["sleep_microseconds_charged"] == 1000
                assert not gate.quarantined
            finally:
                gate.close()
            vectors += 1
    elif scenario == "range":
        for requested in (-1, 2**31 - 999, 2**31 - 1):
            quota, calls, gate, vfs = setup(2**63 - 1, 2**63 - 1)
            before = list(observed)
            try:
                assert vfs.contents.sleep(vfs, requested) == 0 and observed == before
                assert calls.report()["pending_error"] and quota.recovering
                assert quota.report()["first_refusal"]["reason"] == "invalid VFS sleep range"
                assert calls.report()["forward"]["sleep_microseconds_charged"] == 0
                surface(vfs)
                assert not gate.quarantined
            finally:
                gate.close()
            vectors += 1
    else:
        quota, calls, gate, vfs = setup(0, 0)
        before = list(observed)
        try:
            with helpers.native_file(Path(sys.argv[2]) / "sleep-close.sqlite3", gate) as opened:
                assert opened[0] == sqlite3.SQLITE_OK
                before = list(observed)
                assert vfs.contents.sleep(vfs, 1) == 0 and observed == before
                assert opened[2].contents.close(opened[1]) == sqlite3.SQLITE_FULL
                assert not opened[1].contents.methods and gate.open_files == 0
                assert not gate.quarantined
                assert calls.report()["reserved_recovery_closes"] == 0
                assert calls.report()["recovery"]["callback_slots_charged"] == 1
            assert not gate.bound
        finally:
            gate.close()
        vectors = 1
print(json.dumps({"passed": True, "scenario": scenario, "vectors": vectors}))
"""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            code,
            str(Path(__file__).resolve().parents[3]),
            str(tmp_path),
            scenario,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {
        "passed": True,
        "scenario": scenario,
        "vectors": {"rounding": 6, "range": 3, "reserved_close": 1}[scenario],
    }


@pytest.mark.parametrize("policy", ["default", "configured"])
@pytest.mark.parametrize(
    "scenario", ["bootstrap", "read", "partial_write", "zero_write", "nested_open"]
)
def test_native_win32_internal_retries_run_in_an_isolated_application(tmp_path, policy, scenario):
    import json
    import subprocess
    import sys
    from pathlib import Path

    path = tmp_path / "internal-retries.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE original(value BLOB)")
        connection.execute("INSERT INTO original VALUES (?)", (b"exact original",))
        connection.commit()
    finally:
        connection.close()
    original = path.read_bytes()
    code = r"""
import ctypes as c, importlib.util, json, sqlite3, sys
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, sys.argv[1])
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_windows_native import _WindowsAPI
from tests.support.sqlite_vfs_faults import FaultVfs, windows_system_call

spec = importlib.util.spec_from_file_location(
    "native_vfs_probe",
    Path(sys.argv[1]) / "packages/riverhog-client/tests/test_prepared_primary_native_vfs.py",
)
helpers = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = helpers
spec.loader.exec_module(helpers)
connection = sqlite3.connect(":memory:")
try:
    source = connection.execute("SELECT sqlite_source_id()").fetchone()[0]
finally:
    connection.close()
heap = native.SqliteProcessBudget(source, 64 * 1024 * 1024)
quota = native.WriteQuota(helpers.grant(), helpers.grant())
calls = helpers.callbacks(native, quota)
gate = native.WriteVfs(source, helpers.reads(native, quota), calls, heap)
path = Path(sys.argv[2])
policy, scenario = sys.argv[3:]
before_bytes = path.read_bytes()
api = _WindowsAPI()
borrowed = api.open(path)
gate.bind(str(path), borrowed, None)
api.close(borrowed)
base_gate = SimpleNamespace(name="win32")
dword = c.c_ulong
pointer = c.c_void_p
io_type = c.WINFUNCTYPE(
    c.c_int, pointer, pointer, dword, c.POINTER(dword), pointer, use_last_error=True
)
sleep_type = c.WINFUNCTYPE(None, dword, use_last_error=True)
create_type = c.WINFUNCTYPE(
    pointer, c.c_wchar_p, dword, dword, pointer, dword, dword, pointer,
    use_last_error=True,
)
attributes_type = c.WINFUNCTYPE(
    c.c_int, c.c_wchar_p, c.c_int, pointer, use_last_error=True
)
observed = {"read": 0, "write": 0, "create": 0, "attributes": 0, "sleeps": []}
creation_modes = []


def nominal_sleep(original, milliseconds):
    observed["sleeps"].append(milliseconds)


def refuse_read(original, handle, buffer, amount, completed, overlapped):
    observed["read"] += 1
    c.set_last_error(5)
    return 0


def refuse_write(original, handle, buffer, amount, completed, overlapped):
    observed["write"] += 1
    c.set_last_error(5)
    return 0


def query(methods, raw):
    value = (c.c_int * 2)(-1, -1)
    assert methods.contents.control(raw, 9, value) == sqlite3.SQLITE_OK
    return tuple(value)


# The normal base VFS receives an existing file and SQLite-created filename,
# without CREATE. No pager/header/schema/recovery operation is requested.
with helpers.native_file(path, base_gate, binding=False, create=False) as bootstrap:
    assert bootstrap[0] == sqlite3.SQLITE_OK and bootstrap[2]
    assert query(bootstrap[2], bootstrap[1]) == (10, 25)
    if policy == "configured":
        selected = (c.c_int * 2)(2, 3)
        assert bootstrap[2].contents.control(bootstrap[1], 9, selected) == sqlite3.SQLITE_OK
    retry_count, delay = query(bootstrap[2], bootstrap[1])
    assert (retry_count, delay) == ((10, 25) if policy == "default" else (2, 3))
    assert gate.open_files == 0 and gate.bound

# No reset after the native bootstrap close: the configuration is process-global.
if scenario == "bootstrap":
    def observe_create(original, name, access, sharing, security, creation, flags, template):
        observed["create"] += 1
        creation_modes.append(creation)
        return original(name, access, sharing, security, creation, flags, template)

    with ExitStack() as stack:
        stack.enter_context(windows_system_call(b"Sleep", sleep_type, nominal_sleep))
        stack.enter_context(windows_system_call(b"ReadFile", io_type, refuse_read))
        stack.enter_context(windows_system_call(b"WriteFile", io_type, refuse_write))
        stack.enter_context(windows_system_call(b"CreateFileW", create_type, observe_create))
        with helpers.native_file(path, base_gate, binding=False, create=False) as bootstrap:
            assert bootstrap[0] == sqlite3.SQLITE_OK
            assert query(bootstrap[2], bootstrap[1]) == (retry_count, delay)
    assert observed == {"read": 0, "write": 0, "create": 1, "attributes": 0, "sleeps": []}
    assert creation_modes == [3]  # OPEN_EXISTING
    assert calls.report()["forward"]["callback_slots_charged"] == 0

elif scenario == "nested_open":
    def refuse_create(original, *arguments):
        observed["create"] += 1
        c.set_last_error(5)
        return pointer(-1).value

    def refuse_attributes(original, *arguments):
        observed["attributes"] += 1
        c.set_last_error(5)
        return 0

    with ExitStack() as stack:
        stack.enter_context(windows_system_call(b"Sleep", sleep_type, nominal_sleep))
        stack.enter_context(windows_system_call(b"CreateFileW", create_type, refuse_create))
        stack.enter_context(windows_system_call(
            b"GetFileAttributesExW", attributes_type, refuse_attributes
        ))
        with helpers.native_file(path, gate, binding=False, create=False) as failed:
            assert failed[0] == sqlite3.SQLITE_CANTOPEN and failed[2] is None
    assert observed["create"] == 2 * (retry_count + 1), observed
    # Both read-write and fallback read-only opens first call winIsDir,
    # which has its own complete attribute retry loop.
    assert observed["attributes"] == (retry_count + 1) * (retry_count + 3), observed
    expected_sleeps = [delay * i for i in range(1, retry_count + 1)]
    for outer_attempt in range(retry_count + 1):
        expected_sleeps.extend(delay * i for i in range(1, retry_count + 1))
        if outer_attempt < retry_count:
            expected_sleeps.append(delay * (outer_attempt + 1))
    expected_sleeps.extend(delay * i for i in range(1, retry_count + 1))  # fallback winIsDir
    expected_sleeps.extend(delay * i for i in range(1, retry_count + 1))  # fallback CreateFile
    assert observed["sleeps"] == expected_sleeps, observed
    assert len(expected_sleeps) == retry_count * (retry_count + 5)
    assert sum(expected_sleeps) == delay * retry_count * (retry_count + 1) // 2 * (retry_count + 5)
    assert quota.recovering and quota.report()["native_failures"] == 1
    assert gate.open_files == 0 and not gate.quarantined
    assert calls.report()["reserved_recovery_closes"] == 0

else:
    with helpers.native_file(path, gate, binding=False, create=False) as opened:
        assert opened[0] == sqlite3.SQLITE_OK
        raw, methods = opened[1:]
        assert query(methods, raw) == (retry_count, delay)
        with ExitStack() as stack:
            stack.enter_context(windows_system_call(b"Sleep", sleep_type, nominal_sleep))
            if scenario == "read":
                stack.enter_context(windows_system_call(b"ReadFile", io_type, refuse_read))
                buffer = c.create_string_buffer(7)
                assert methods.contents.read(raw, buffer, 7, 0) == sqlite3.SQLITE_IOERR_READ
                assert observed["read"] == retry_count + 1
            else:
                progress = {"bytes": 0, "failures": 0}

                def partial_write(original, handle, buffer, amount, completed, overlapped):
                    observed["write"] += 1
                    if scenario == "zero_write":
                        completed[0] = 0
                        c.set_last_error(5)
                        return 1
                    if progress["failures"] < retry_count and (
                        progress["failures"] <= progress["bytes"] or progress["bytes"] == 6
                    ):
                        progress["failures"] += 1
                        c.set_last_error(5)
                        return 0
                    result = original(handle, buffer, 1, completed, overlapped)
                    if result and completed[0] == 1:
                        progress["bytes"] += 1
                    return result

                stack.enter_context(windows_system_call(b"WriteFile", io_type, partial_write))
                buffer = c.create_string_buffer(before_bytes[:7], 7)
                result = methods.contents.write(raw, buffer, 7, 0)
                if scenario == "partial_write":
                    assert result == sqlite3.SQLITE_OK
                    assert progress == {"bytes": 7, "failures": retry_count}
                    assert observed["write"] == 7 + retry_count
                    assert not quota.recovering
                else:
                    assert result == sqlite3.SQLITE_IOERR_WRITE
                    assert observed["write"] == 1
            if scenario != "partial_write":
                assert quota.recovering and quota.report()["native_failures"] == 1
        assert observed["sleeps"] == (
            [] if scenario == "zero_write" else
            [delay * i for i in range(1, retry_count + 1)]
        )
        # Internal file retries bypass xSleep; zero selected-VFS sleep charge
        # cannot certify zero native waits.
        assert calls.report()["forward"]["sleep_microseconds_charged"] == 0
        assert calls.report()["recovery"]["sleep_microseconds_charged"] == 0
    assert gate.open_files == 0 and not gate.quarantined

assert path.read_bytes() == before_bytes
gate.unbind()
gate.close()
print(json.dumps({
    "passed": True, "policy": policy, "scenario": scenario,
    "retry_count": retry_count, "delay_milliseconds": delay, "observed": observed,
}))
"""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            code,
            str(Path(__file__).resolve().parents[3]),
            str(path),
            policy,
            scenario,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    proof = json.loads(result.stdout)
    assert proof["passed"] and proof["policy"] == policy and proof["scenario"] == scenario
    assert path.read_bytes() == original
    cold = sqlite3.connect(path)
    try:
        assert cold.execute("SELECT value FROM original").fetchone() == (b"exact original",)
    finally:
        cold.close()


@pytest.mark.parametrize(
    "scenario",
    [
        "success",
        "call_grant",
        "sleep_grant",
        "profile_range",
        "malformed",
        "unbound",
        "live_file",
        "foreign_baseline",
        "heap_refusal",
        "open_failure",
        "close_failure",
        "sibling_quarantine",
        "readonly_fallback",
        "close_retry_success",
    ],
)
def test_native_prepaid_retry_bootstrap_runs_in_an_isolated_application(tmp_path, scenario):
    import json
    import subprocess
    import sys
    from pathlib import Path

    path = tmp_path / "retry-bootstrap.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE original(value BLOB)")
        connection.execute("INSERT INTO original VALUES (?)", (b"exact original",))
        connection.commit()
    finally:
        connection.close()
    original = path.read_bytes()
    code = r"""
import ctypes as c, importlib.util, json, sqlite3, sys
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, sys.argv[1])
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_windows_native import _WindowsAPI
from tests.support.sqlite_vfs_faults import FaultVfs, windows_system_call

spec = importlib.util.spec_from_file_location(
    "native_vfs_probe",
    Path(sys.argv[1]) / "packages/riverhog-client/tests/test_prepared_primary_native_vfs.py",
)
helpers = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = helpers
spec.loader.exec_module(helpers)
native, source = helpers.runtime()
heap = native.SqliteProcessBudget(source, 64 * 1024 * 1024)
quota = native.WriteQuota(helpers.grant(), helpers.grant())
calls = helpers.callbacks(native, quota)
gate = native.WriteVfs(source, helpers.reads(native, quota), calls, heap)
path, scenario = Path(sys.argv[2]), sys.argv[3]
if scenario == "sibling_quarantine":
    sibling_quota = native.WriteQuota(helpers.grant(), helpers.grant())
    sibling_calls = helpers.callbacks(native, sibling_quota)
    sibling = native.WriteVfs(source, helpers.reads(native, sibling_quota), sibling_calls, heap)
original_bytes = path.read_bytes()
api = _WindowsAPI()
borrowed = api.open(path)
gate.bind(str(path), borrowed, None)
api.close(borrowed)
assert heap.retry_report() == {
    "state": "unconfigured",
    "qualified_initial_retry_count": 0,
    "qualified_initial_delay_milliseconds": 0,
    "selected_retry_count": 0,
    "selected_delay_milliseconds": 0,
    "bootstrap_filesystem_api_attempts_prepaid": 0,
    "bootstrap_sleep_microseconds_prepaid": 0,
    "first_failure_stage": "",
    "native_result": 0,
    "native_close_result": 0,
    "native_close_attempted": False,
}
initial, selected = (10, 25), (2, 3)
cost = native.retry_bootstrap_cost(initial)
assert cost == (171, 20825000)
pointer, dword = c.c_void_p, c.c_ulong
create_type = c.WINFUNCTYPE(
    pointer,
    c.c_wchar_p,
    dword,
    dword,
    pointer,
    dword,
    dword,
    pointer,
    use_last_error=True,
)
io_type = c.WINFUNCTYPE(
    c.c_int, pointer, pointer, dword, c.POINTER(dword), pointer, use_last_error=True
)
close_type = c.WINFUNCTYPE(c.c_int, pointer, use_last_error=True)
sleep_type = c.WINFUNCTYPE(None, dword, use_last_error=True)
seen = {"creates": [], "prepaid": [], "reads": 0, "writes": 0, "closes": [], "sleeps": []}


def create(original, name, access, sharing, security, creation, flags, template):
    seen["creates"].append(creation)
    seen["prepaid"].append(heap.retry_report())
    if scenario == "open_failure" or (scenario == "readonly_fallback" and access & 0x40000000):
        c.set_last_error(5)
        return pointer(-1).value
    return original(name, access, sharing, security, creation, flags, template)


def read(original, *args):
    seen["reads"] += 1
    c.set_last_error(5)
    return 0


def write(original, *args):
    seen["writes"] += 1
    c.set_last_error(5)
    return 0


def close(original, handle):
    seen["closes"].append(handle)
    if scenario in ("close_failure", "sibling_quarantine") or (
        scenario == "close_retry_success" and len(seen["closes"]) < 3
    ):
        c.set_last_error(5)
        return 0
    return original(handle)


def sleep(original, milliseconds):
    seen["sleeps"].append(milliseconds)


def refuse(operation, message):
    try:
        operation()
    except RuntimeError as exc:
        assert message in str(exc), str(exc)
    else:
        raise AssertionError("invalid retry admission succeeded")


def query(methods, raw):
    values = (c.c_int * 2)(-1, -1)
    assert methods.contents.control(raw, 9, values) == sqlite3.SQLITE_OK
    return tuple(values)


if scenario == "foreign_baseline":
    with helpers.native_file(
        path, SimpleNamespace(name="win32"), binding=False, create=False
    ) as opened:
        values = (c.c_int * 2)(1, 1)
        assert opened[0] == sqlite3.SQLITE_OK
        assert opened[2].contents.control(opened[1], 9, values) == sqlite3.SQLITE_OK
        assert query(opened[2], opened[1]) == (1, 1)

with ExitStack() as stack:
    stack.enter_context(windows_system_call(b"Sleep", sleep_type, sleep))
    stack.enter_context(windows_system_call(b"CreateFileW", create_type, create))
    stack.enter_context(windows_system_call(b"ReadFile", io_type, read))
    stack.enter_context(windows_system_call(b"WriteFile", io_type, write))
    real_close = stack.enter_context(windows_system_call(b"CloseHandle", close_type, close))
    if scenario in ("call_grant", "sleep_grant"):
        smaller = (cost[0] - 1, cost[1]) if scenario == "call_grant" else (cost[0], cost[1] - 1)
        refuse(lambda: gate.admit_retries(initial, selected, smaller), "grant")
        assert not seen["creates"] and not seen["closes"]
        assert heap.retry_report()["state"] == "unconfigured" and not gate.quarantined
        assert quota.recovering
        assert quota.report()["first_refusal"]["reason"] == "native retry admission"
        # A separately authorized larger bootstrap grant can make control progress.
        gate.admit_retries(initial, selected, cost)
    elif scenario in ("profile_range", "malformed"):
        invalid = (
            ((initial, (1, 1073741824), cost), (initial, (46341, 1), cost))
            if scenario == "profile_range"
            else (
                (list(initial), selected, cost),
                (initial, (True, 3), cost),
                (initial, selected, (False, cost[1])),
                (initial, selected, (1, -1)),
            )
        )
        for arguments in invalid:
            try:
                gate.admit_retries(*arguments)
            except (ValueError, TypeError, OverflowError):
                pass
            else:
                raise AssertionError("invalid retry input was admitted")
        assert not seen["creates"] and not seen["closes"] and not quota.recovering
        assert heap.retry_report()["state"] == "unconfigured" and not gate.quarantined
        gate.admit_retries(initial, selected, cost)
    elif scenario == "heap_refusal":
        library = FaultVfs.windows().library
        library.sqlite3_malloc64.argtypes = [c.c_ulonglong]
        library.sqlite3_malloc64.restype = pointer
        library.sqlite3_free.argtypes = [pointer]
        library.sqlite3_free.restype = None
        current = heap.report()
        ballast = library.sqlite3_malloc64(
            current["heap_limit_bytes"] - current["sqlite_heap_bytes"] - 16
        )
        assert ballast
        try:
            refuse(lambda: gate.admit_retries(initial, selected, cost), "preallocated")
            assert not seen["creates"] and not seen["closes"]
            assert quota.recovering and not gate.quarantined
        finally:
            library.sqlite3_free(ballast)
        assert heap.retry_report()["state"] == "unconfigured"
        gate.admit_retries(initial, selected, cost)
    elif scenario == "unbound":
        gate.unbind()
        refuse(lambda: gate.admit_retries(initial, selected, cost), "bound")
        assert not seen["creates"] and not seen["closes"]
        assert heap.retry_report()["state"] == "unconfigured"
        handle = api.open(path)
        gate.bind(str(path), handle, None)
        api.close(handle)
        gate.admit_retries(initial, selected, cost)
    elif scenario == "live_file":
        # The first native open itself is successful and performs no pager IO.
        with helpers.native_file(path, gate, binding=False, create=False) as opened:
            assert opened[0] == sqlite3.SQLITE_OK
            before = list(seen["creates"])
            refuse(lambda: gate.admit_retries(initial, selected, cost), "file-free")
            assert seen["creates"] == before and heap.retry_report()["state"] == "unconfigured"
        seen["creates"].clear()
        seen["prepaid"].clear()
        seen["closes"].clear()
        gate.admit_retries(initial, selected, cost)
    elif scenario in (
        "foreign_baseline",
        "open_failure",
        "close_failure",
        "sibling_quarantine",
        "readonly_fallback",
    ):
        refuse(lambda: gate.admit_retries(initial, selected, cost), "restart required")
    else:
        assert scenario in ("success", "close_retry_success")
        gate.admit_retries(initial, selected, cost)

assert seen["reads"] == seen["writes"] == 0
assert seen["creates"] and all(value == 3 for value in seen["creates"])  # OPEN_EXISTING
assert all(
    report["state"] == "admitting"
    and report["bootstrap_filesystem_api_attempts_prepaid"] == cost[0]
    and report["bootstrap_sleep_microseconds_prepaid"] == cost[1]
    for report in seen["prepaid"]
)
report = heap.retry_report()
assert report["bootstrap_filesystem_api_attempts_prepaid"] == cost[0]
assert report["bootstrap_sleep_microseconds_prepaid"] == cost[1]
failed = scenario in (
    "foreign_baseline",
    "open_failure",
    "close_failure",
    "sibling_quarantine",
    "readonly_fallback",
)
if failed:
    assert report["state"] == "quarantined" and gate.quarantined and gate.bound
    assert gate.open_files == 0 and quota.recovering
    refuse(lambda: gate.admit_retries(initial, selected, cost), "file-free")
    refuse(gate.unbind, "quarantined")
    refuse(gate.close, "quarantined")
    try:
        native.WriteVfs(
            source, helpers.reads(native, quota), helpers.callbacks(native, quota), heap
        )
    except RuntimeError as exc:
        assert "retry quarantine" in str(exc)
    else:
        raise AssertionError("a new VFS accepted quarantined application retry state")
    if scenario == "foreign_baseline":
        assert report["first_failure_stage"] == "baseline" and report["native_result"] == 0
        assert report["native_close_attempted"]
        assert report["native_close_result"] == sqlite3.SQLITE_OK
    elif scenario == "readonly_fallback":
        assert report["first_failure_stage"] == "identity" and report["native_result"] == 0
        assert seen["creates"] == [3, 3]
        assert report["native_close_attempted"]
        assert report["native_close_result"] == sqlite3.SQLITE_OK
    elif scenario == "open_failure":
        assert report["first_failure_stage"] == "open"
        assert report["native_result"] == sqlite3.SQLITE_CANTOPEN
        assert not report["native_close_attempted"]
    if scenario in ("close_failure", "sibling_quarantine"):
        assert report["first_failure_stage"] == "close"
        assert report["native_result"] == sqlite3.SQLITE_IOERR_CLOSE
        assert report["native_close_result"] == sqlite3.SQLITE_IOERR_CLOSE
        assert report["native_close_attempted"]
        assert len(seen["closes"]) == 3 and len(set(seen["closes"])) == 1
        assert seen["sleeps"] == [100, 100]
        # The hook never closed this handle; this cleanup is qualification-only.
        assert real_close(seen["closes"][0])
    if scenario == "sibling_quarantine":
        library = FaultVfs.windows().library
        raw_vfs = library.sqlite3_vfs_find(sibling.name.encode("ascii"))
        before = c.c_double(-7)
        assert raw_vfs.contents.time(raw_vfs, c.byref(before)) == sqlite3.SQLITE_FULL
        assert before.value == -7 and sibling_quota.recovering
        assert sibling_quota.report()["first_refusal"]["reason"] == "native retry admission"
        assert not sibling.quarantined
        sibling.close()  # no uncertain sibling resource blocks its safe release
    try:
        path.rename(path.with_suffix(".moved"))
    except PermissionError:
        pass
    else:
        raise AssertionError("failed bootstrap released its binding pin")
else:
    assert report["state"] == "admitted"
    assert report["native_result"] == report["native_close_result"] == sqlite3.SQLITE_OK
    assert report["native_close_attempted"] and report["first_failure_stage"] == ""
    assert report["qualified_initial_retry_count"] == 10
    assert report["qualified_initial_delay_milliseconds"] == 25
    assert report["selected_retry_count"] == 2 and report["selected_delay_milliseconds"] == 3
    assert len(seen["creates"]) == 1
    if scenario == "close_retry_success":
        assert len(seen["closes"]) == 3 and seen["sleeps"] == [100, 100]
    else:
        assert len(seen["closes"]) == 1 and not seen["sleeps"]
    assert not gate.quarantined and gate.open_files == 0
    refuse(lambda: gate.admit_retries(initial, selected, cost), "already")
    with helpers.native_file(path, gate, binding=False, create=False) as opened:
        assert opened[0] == sqlite3.SQLITE_OK and query(opened[2], opened[1]) == selected
    assert heap.retry_report() == report  # no per-store reset or cumulative recharge
    gate.unbind()
    gate.close()
    moved = path.with_suffix(".moved")
    path.rename(moved)
    moved.rename(path)
    assert heap.retry_report() == report  # process owner has no successful bootstrap path pin

assert path.read_bytes() == original_bytes
assert heap.report()["sqlite_heap_peak_bytes"] <= heap.report()["heap_limit_bytes"]
print(json.dumps({"passed": True, "scenario": scenario, "retry": report}))
"""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            code,
            str(Path(__file__).resolve().parents[3]),
            str(path),
            scenario,
        ],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    proof = json.loads(result.stdout)
    assert proof["passed"] and proof["scenario"] == scenario
    assert path.read_bytes() == original
    moved = path.with_suffix(".moved")
    path.rename(moved)
    cold = sqlite3.connect(moved)
    try:
        assert cold.execute("SELECT value FROM original").fetchone() == (b"exact original",)
    finally:
        cold.close()
