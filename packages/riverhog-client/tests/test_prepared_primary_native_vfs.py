"""The actual loaded Windows engine invokes C storage callbacks."""

from __future__ import annotations

import ctypes as c
import gc
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.name != "nt", reason="requires the loaded native Windows SQLite")


def grant():
    return (4096, 128, 1024 * 1024, 1024 * 1024, 8 * 1024 * 1024, 65536)


def runtime():
    from riverhog_client import _prepared_primary_native as native

    with closing(sqlite3.connect(":memory:")) as connection:
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
        from riverhog_client._prepared_primary_runtime import _admit_process_budget

        budget = _admit_process_budget(64 * 1024 * 1024, native_limits=(128, 512))
    assert budget.report()["heap_limit_bytes"] == 64 * 1024 * 1024
    return budget


def callbacks(native, quota, *, forward=262144, recovery=262144, forward_sleep=0, recovery_sleep=0):
    return native.CallbackQuota(
        quota,
        (forward, forward_sleep),
        (recovery, recovery_sleep),
        work=native.NativeWorkQuota(quota, (1 << 40, 1 << 60), (1 << 40, 1 << 60)),
        buffers=(1 << 40, 1 << 40),
    )


def admit_fixture_retries(gate):
    from riverhog_client import _prepared_primary_native as native

    state = native.current_process_budget().retry_report()["state"]
    if state == "unconfigured":
        from riverhog_client._prepared_primary_runtime import _QUALIFIED_INITIAL_RETRIES

        initial = _QUALIFIED_INITIAL_RETRIES
        gate.admit_retries(initial, (2, 3), native.retry_bootstrap_cost(initial))
    else:
        assert state == "admitted"


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
        admit_fixture_retries(gate)
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
            admit_fixture_retries(gate)
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
    try:native.SqliteProcessBudget(source,limit,(128,512))
    except RuntimeError as exc:assert "accounting" in str(exc)
    else:raise AssertionError("disabled accounting was accepted")
    assert library.sqlite3_hard_heap_limit64(-1)==0
elif scenario=="existing":
    assert library.sqlite3_hard_heap_limit64(limit)==0
    try:native.SqliteProcessBudget(source,2*limit,(128,512))
    except RuntimeError as exc:assert "unconfigured" in str(exc)
    else:raise AssertionError("existing global authority was replaced")
    assert library.sqlite3_hard_heap_limit64(-1)==limit
else:
    # A failed undersized admission does not arm a limit or retain an owner.
    try:native.SqliteProcessBudget(source,1,(128,512))
    except RuntimeError as exc:assert "below current" in str(exc)
    else:raise AssertionError("an undersized grant was accepted")
    assert library.sqlite3_hard_heap_limit64(-1)==0
    owner=native.SqliteProcessBudget(source,limit,(128,512))
    assert native.current_process_budget() is owner
    try:native.SqliteProcessBudget(source,2*limit,(128,512))
    except RuntimeError as exc:assert "already" in str(exc)
    else:raise AssertionError("second process owner was admitted")
    if scenario=="lowered":
        library.sqlite3_hard_heap_limit64(limit//2)
        try:owner.report()
        except RuntimeError as exc:assert "changed" in str(exc)
        else:raise AssertionError("changed global profile was accepted")
        writes=native.WriteQuota((4096,8,32768,32768,65536,32),(4096,8,32768,32768,65536,32))
        reads=native.ReadQuota(writes,(65536,32),(65536,32))
        calls=native.CallbackQuota(
            writes,(128, 0),(128, 0),
            work=native.NativeWorkQuota(writes, (1 << 40, 1 << 60), (1 << 40, 1 << 60)),
            buffers=(1 << 40, 1 << 40),
        )
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
from riverhog_client._prepared_primary_runtime import _admit_process_budget
heap = _admit_process_budget(64 * 1024 * 1024,native_limits=(128,512))
quota = native.WriteQuota(helpers.grant(), helpers.grant())
reads = native.ReadQuota(quota, (1048576, 65536), (1048576, 65536))
calls = native.CallbackQuota(
    quota, (262144, 0), (262144, 0),
    work=native.NativeWorkQuota(quota, (1 << 40, 1 << 60), (1 << 40, 1 << 60)),
    buffers=(1 << 40, 1 << 40),
)
gate = native.WriteVfs(source, reads, calls, heap)
path = Path(sys.argv[2])
failed_kind = sys.argv[3]
api = _WindowsAPI()
borrowed = api.open(path)
gate.bind(str(path), borrowed, None)
api.close(borrowed)
helpers.admit_fixture_retries(gate)
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
        replacement_calls = native.CallbackQuota(
            replacement, (262144, 0), (262144, 0),
            work=native.NativeWorkQuota(replacement, (1 << 40, 1 << 60), (1 << 40, 1 << 60)),
            buffers=(1 << 40, 1 << 40),
        )
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
from riverhog_client._prepared_primary_runtime import _admit_process_budget
heap = _admit_process_budget(64 * 1024 * 1024,native_limits=(128,512))
library = FaultVfs.windows().library
observed = []


def nominal_sleep(original, milliseconds):
    observed.append(milliseconds)


def setup(forward, recovery):
    quota = native.WriteQuota(helpers.grant(), helpers.grant())
    calls = native.CallbackQuota(
        quota, (32, forward), (32, recovery),
        work=native.NativeWorkQuota(quota, (1 << 40, 1 << 60), (1 << 40, 1 << 60)),
        buffers=(1 << 40, 1 << 40),
    )
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
from riverhog_client._prepared_primary_runtime import _admit_process_budget
heap = _admit_process_budget(64 * 1024 * 1024,native_limits=(128,512))
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
    assert gate.open_files == 0 and gate.bound

retry_count, delay = (10, 25) if policy == "default" else (2, 3)
gate.admit_retries((10, 25), (retry_count, delay), native.retry_bootstrap_cost((10, 25)))
before_probe_work = calls.work.report()

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
    assert calls.work.report() == before_probe_work

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
    work = calls.work.report()
    failed_cost = native.retry_bootstrap_cost((retry_count, delay))
    assert tuple(
        work["forward"][key] - before_probe_work["forward"][key]
        for key in ("filesystem_api_attempts_charged", "internal_sleep_microseconds_charged")
    ) == failed_cost
    assert tuple(
        work["recovery"][key] - before_probe_work["recovery"][key]
        for key in ("filesystem_api_attempts_charged", "internal_sleep_microseconds_charged")
    ) == (3, 200000)

else:
    with helpers.native_file(path, gate, binding=False, create=False) as opened:
        assert opened[0] == sqlite3.SQLITE_OK
        raw, methods = opened[1:]
        assert query(methods, raw) == (retry_count, delay)
        before_io = calls.work.report()["forward"]
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
        work = calls.work.report()["forward"]
        charged = tuple(
            work[key] - before_io[key]
            for key in ("filesystem_api_attempts_charged", "internal_sleep_microseconds_charged")
        )
        assert charged == native.filesystem_cost(
            "read" if scenario == "read" else "write", (retry_count, delay), 7
        ), (scenario, charged, work)
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
from riverhog_client._prepared_primary_runtime import _admit_process_budget
heap = _admit_process_budget(64 * 1024 * 1024,native_limits=(128,512))
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
        # Ordinary filesystem delegation is forbidden before owned retry
        # admission, including a raw wrapper open without pager IO.
        with helpers.native_file(path, gate, binding=False, create=False) as opened:
            assert opened[0] == sqlite3.SQLITE_FULL and opened[2] is None
            assert not seen["creates"] and not seen["closes"]
            assert heap.retry_report()["state"] == "unconfigured"
            assert quota.report()["first_refusal"]["reason"] == "native retry admission"
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
population = heap.native_report()
assert population["owners"] >= 1 and population["file_peak"] == 1
assert population["files"] == int(scenario in ("close_failure", "sibling_quarantine"))
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
        if scenario == "live_file":
            refuse(lambda: gate.admit_retries(initial, selected, cost), "file-free")
            assert heap.retry_report() == report
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


def test_native_attached_work_bank_rejects_manual_mutation_and_foreign_registration():
    native, source = runtime()
    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (0, 0), (0, 0))
    first = native.CallbackQuota(
        writes,
        (32, 0),
        (32, 0),
        work,
        buffers=(1 << 40, 1 << 40),
    )
    alias = native.CallbackQuota(
        writes,
        (32, 0),
        (32, 0),
        work,
        buffers=(1 << 40, 1 << 40),
    )
    gate = native.WriteVfs(source, reads(native, writes), first, heap(native, source))
    before = work.report()
    try:
        for action in (
            lambda: work.admit((0, 0)),
            lambda: work.acquire((0, 0), (1, 0)),
            lambda: native.CallbackQuota(
                writes,
                (32, 0),
                (32, 0),
                work,
                buffers=(1 << 40, 1 << 40),
            ),
            lambda: native.WriteVfs(source, reads(native, writes), alias, heap(native, source)),
        ):
            with pytest.raises(RuntimeError, match="attached native work"):
                action()
        assert work.report() == before and not writes.recovering
    finally:
        gate.close()
    assert work.admit((0, 0))
    assert work.report() == before


@pytest.mark.parametrize(
    "scenario",
    [
        "read",
        "write",
        "truncate",
        "sync",
        "size",
        "lock",
        "unlock",
        "reserved",
        "control",
        "open",
        "delete",
        "access",
        "fullpath",
        "connect",
        "wait_read",
        "wait_write",
        "wait_lock",
        "wait_open",
        "wait_delete",
        "wait_access",
    ],
)
def test_native_filesystem_work_refusal_and_prepaid_closes_are_isolated(tmp_path, scenario):
    import json
    import subprocess
    import sys
    from pathlib import Path

    path = tmp_path / "filesystem-work.sqlite3"
    connection = sqlite3.connect(path)
    try:
        assert connection.execute("PRAGMA journal_mode=PERSIST").fetchone() == ("persist",)
        connection.execute("PRAGMA synchronous=EXTRA")
        connection.execute("CREATE TABLE original(body BLOB NOT NULL)")
        connection.execute("INSERT INTO original VALUES (?)", (b"exact original",))
        connection.commit()
    finally:
        connection.close()
    journal = path.with_name(path.name + "-journal")
    before = (path.read_bytes(), journal.read_bytes())
    code = r"""
import ctypes as c, importlib.util, json, sqlite3, sys
from contextlib import ExitStack
from pathlib import Path

sys.path.insert(0, sys.argv[1])
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_windows_native import _WindowsAPI
from tests.support.sqlite_vfs_faults import FaultVfs, windows_system_call

spec=importlib.util.spec_from_file_location(
    "native_vfs_probe",
    Path(sys.argv[1])/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py",
)
helpers=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=helpers
spec.loader.exec_module(helpers)
path=Path(sys.argv[2])
journal=path.with_name(path.name+"-journal")
scenario=sys.argv[3]
waiting=scenario.startswith("wait_")
method=scenario.removeprefix("wait_")
file_method=method in {"read","write","truncate","sync","size","lock","unlock","reserved","control"}
original=(path.read_bytes(),journal.read_bytes())
native,source=helpers.runtime()
from riverhog_client._prepared_primary_runtime import _admit_process_budget
heap = _admit_process_budget(64 * 1024 * 1024,native_limits=(128,512))
initial=native.WriteQuota(helpers.grant(),helpers.grant())
initial_calls=helpers.callbacks(native,initial)
gate=native.WriteVfs(source,helpers.reads(native,initial),initial_calls,heap)
api=_WindowsAPI()
borrowed=api.open(path)
try:
    gate.bind(str(path),borrowed,helpers.journal_identity(path,api))
finally:
    api.close(borrowed)
helpers.admit_fixture_retries(gate)
profile=(2,3)
recipe={"open":"open_database","delete":"delete_journal","connect":"fullpath"}.get(method,method)
cost=native.filesystem_cost(recipe,profile,7 if method in ("read","write") else 0)
close=native.filesystem_cost("close",profile) if method=="open" else (0,0)
complete=tuple(a+b for a,b in zip(cost,close,strict=True))
forward=(complete[0],complete[1]-1) if waiting else (0,1<<60)
recovery=complete if method=="open" else native.filesystem_cost(
    "read" if file_method else "access",profile,7 if file_method else 0
)
dword=c.c_ulong
pointer=c.c_void_p
signature=lambda result,*args:c.WINFUNCTYPE(result,*args,use_last_error=True)
hooks={
    b"ReadFile":signature(c.c_int,pointer,pointer,dword,c.POINTER(dword),pointer),
    b"WriteFile":signature(c.c_int,pointer,pointer,dword,c.POINTER(dword),pointer),
    b"SetFilePointer":signature(dword,pointer,c.c_long,c.POINTER(c.c_long),dword),
    b"SetEndOfFile":signature(c.c_int,pointer),
    b"FlushFileBuffers":signature(c.c_int,pointer),
    b"GetFileSize":signature(dword,pointer,c.POINTER(dword)),
    b"LockFileEx":signature(c.c_int,pointer,dword,dword,dword,dword,pointer),
    b"UnlockFileEx":signature(c.c_int,pointer,dword,dword,dword,pointer),
    b"CreateFileW":signature(pointer,c.c_wchar_p,dword,dword,pointer,dword,dword,pointer),
    b"DeleteFileW":signature(c.c_int,c.c_wchar_p),
    b"GetFileAttributesW":signature(dword,c.c_wchar_p),
    b"GetFileAttributesExW":signature(c.c_int,c.c_wchar_p,c.c_int,pointer),
    b"GetFullPathNameW":signature(dword,c.c_wchar_p,dword,pointer,pointer),
    b"CloseHandle":signature(c.c_int,pointer),
}
observed={name.decode():0 for name in hooks}
def observer(name):
    def call(original,*args):
        observed[name.decode()]+=1
        return original(*args)
    return call

writes=native.WriteQuota(helpers.grant(),helpers.grant())
work=native.NativeWorkQuota(writes,forward,recovery)
calls=native.CallbackQuota(writes,(128,0),(128,0),work,
    buffers=(1 << 40, 1 << 40),
)
library=FaultVfs.windows().library
vfs=library.sqlite3_vfs_find(gate.name.encode())
raw_name=c.create_string_buffer(str(path.resolve()).encode())
journal_name=c.create_string_buffer(str(journal.resolve()).encode())
output=c.create_string_buffer(b"########")
size=c.c_int64(-7)
reserved=c.c_int(-7)
handle=c.c_void_p()
def file_action(raw,methods):
    operations={
        "read":lambda:methods.contents.read(raw,output,7,0),
        "write":lambda:methods.contents.write(raw,c.create_string_buffer(b"changed"),7,0),
        "truncate":lambda:methods.contents.truncate(raw,0),
        "sync":lambda:methods.contents.sync(raw,3),
        "size":lambda:methods.contents.size(raw,c.byref(size)),
        "lock":lambda:methods.contents.lock(raw,1),
        "unlock":lambda:methods.contents.unlock(raw,0),
        "reserved":lambda:methods.contents.reserved(raw,c.byref(reserved)),
        "control":lambda:methods.contents.control(raw,29,c.byref(handle)),
    }
    return operations[method]()

def verify_refusal():
    assert not any(observed.values()),observed
    assert writes.recovering
    assert writes.report()["first_refusal"]["reason"]==(
        "native internal sleep grant" if waiting else "native filesystem attempt grant"
    )
    assert all(
        not any(bank.values()) for bank in work.report().values()
    ),work.report()
    assert output.raw==b"########\0" and size.value==reserved.value==-7 and handle.value is None
    assert not gate.quarantined

if file_method:
    with helpers.native_file(path,gate,binding=False,create=False) as opened:
        assert opened[0]==sqlite3.SQLITE_OK
        raw,methods=opened[1:]
        gate.arm(helpers.reads(native,writes),calls)
        del initial_calls,initial
        with ExitStack() as stack:
            for name,kind in hooks.items():
                stack.enter_context(windows_system_call(name,kind,observer(name)))
            assert file_action(raw,methods)==sqlite3.SQLITE_FULL
            verify_refusal()
            assert methods.contents.read(raw,output,7,0)==sqlite3.SQLITE_OK
            assert output.raw[:7]==original[0][:7]
            spent=work.report()
            snapshot=dict(observed)
            assert methods.contents.read(raw,output,7,0)==sqlite3.SQLITE_FULL
            assert observed==snapshot and work.report()==spent
            assert methods.contents.close(raw)==sqlite3.SQLITE_OK
            assert not raw.contents.methods and gate.open_files==0 and not gate.quarantined
            assert observed["CloseHandle"]==1 and work.report()==spent
else:
    gate.arm(helpers.reads(native,writes),calls)
    del initial_calls,initial
    with ExitStack() as stack:
        for name,kind in hooks.items():
            stack.enter_context(windows_system_call(name,kind,observer(name)))
        if method=="open":
            with helpers.native_file(path,gate,binding=False,create=False) as refused:
                assert refused[0]==sqlite3.SQLITE_FULL and refused[2] is None
        elif method=="delete":
            assert vfs.contents.delete(vfs,journal_name,0)==sqlite3.SQLITE_FULL
        elif method=="access":
            assert vfs.contents.access(vfs,raw_name,0,c.byref(reserved))==sqlite3.SQLITE_FULL
        elif method=="connect":
            try:
                unexpected=sqlite3.connect(path.as_uri()+"?vfs="+gate.name,uri=True)
            except sqlite3.Error as exc:
                assert exc.sqlite_errorcode==sqlite3.SQLITE_FULL
            else:
                unexpected.close()
                raise AssertionError("SQLite connection bypassed an empty native work grant")
            assert gate.open_files==0
        else:
            assert method=="fullpath"
            assert vfs.contents.pathname(vfs,raw_name,len(output),output)==sqlite3.SQLITE_FULL
        verify_refusal()
        if method=="open":
            with helpers.native_file(path,gate,binding=False,create=False) as reopened:
                assert reopened[0]==sqlite3.SQLITE_OK
                spent=work.report()
                assert tuple(spent["recovery"][key] for key in (
                    "filesystem_api_attempts_charged","internal_sleep_microseconds_charged"
                ))==recovery
            assert work.report()==spent and observed["CloseHandle"]==1
        else:
            assert vfs.contents.access(vfs,raw_name,0,c.byref(reserved))==sqlite3.SQLITE_OK
            assert reserved.value==1
            spent=work.report()
            snapshot=dict(observed)
            reserved.value=-7
            assert vfs.contents.access(vfs,raw_name,0,c.byref(reserved))==sqlite3.SQLITE_FULL
            assert reserved.value==-7 and observed==snapshot and work.report()==spent
before_close=work.report()
gate.unbind()
assert not gate.bound and not gate.quarantined and work.report()==before_close
gate.close()
for item in (path,journal):
    moved=item.with_suffix(item.suffix+".moved")
    item.rename(moved)
    moved.rename(item)
assert (path.read_bytes(),journal.read_bytes())==original
assert heap.report()["sqlite_heap_peak_bytes"]<=64*1024*1024
print(json.dumps({"passed":True,"scenario":scenario,"closes_prepaid":True,"original_exact":True}))
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
    assert json.loads(result.stdout) == {
        "passed": True,
        "scenario": scenario,
        "closes_prepaid": True,
        "original_exact": True,
    }
    assert (path.read_bytes(), journal.read_bytes()) == before
    with closing(sqlite3.connect(path)) as cold:
        assert cold.execute("SELECT body FROM original").fetchone() == (b"exact original",)
    assert (path.read_bytes(), journal.read_bytes()) == before


@pytest.mark.parametrize(
    "scenario",
    [
        "binding_forward_absent",
        "binding_recovery_absent",
        "binding_forward_present",
        "binding_recovery_present",
        "journal_generations",
    ],
)
def test_native_binding_and_journal_pin_work_runs_in_an_isolated_application(tmp_path, scenario):
    import json
    import subprocess
    import sys
    from pathlib import Path

    path = tmp_path / "pin-work.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode=PERSIST")
        connection.execute("CREATE TABLE original(body BLOB NOT NULL)")
        connection.execute("INSERT INTO original VALUES (?)", (b"exact original",))
    connection.close()
    journal = path.with_name(path.name + "-journal")
    if not scenario.endswith("_present"):
        journal.unlink()
    original = (path.read_bytes(), journal.read_bytes() if journal.exists() else None)
    code = r"""
import ctypes as c, importlib.util, json, sqlite3, sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_windows_native import _WindowsAPI
from tests.support.sqlite_vfs_faults import FaultVfs
spec=importlib.util.spec_from_file_location(
    "native_vfs_probe",
    Path(sys.argv[1])/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py",
)
helpers=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=helpers
spec.loader.exec_module(helpers)
path=Path(sys.argv[2])
journal=path.with_name(path.name+"-journal")
scenario=sys.argv[3]
original=(path.read_bytes(),journal.read_bytes() if journal.exists() else None)
native,source=helpers.runtime()
from riverhog_client._prepared_primary_runtime import _admit_process_budget
heap = _admit_process_budget(64 * 1024 * 1024,native_limits=(128,512))
initial=native.WriteQuota(helpers.grant(),helpers.grant())
calls=helpers.callbacks(native,initial)
gate=native.WriteVfs(source,helpers.reads(native,initial),calls,heap)
api=_WindowsAPI()
library=FaultVfs.windows().library
vfs=library.sqlite3_vfs_find(gate.name.encode())
name=c.create_string_buffer(str(journal.resolve()).encode())
kernel=c.WinDLL("kernel32",use_last_error=True)
kernel.GetCurrentProcess.argtypes=[]
kernel.GetCurrentProcess.restype=c.c_void_p
kernel.GetProcessHandleCount.argtypes=[c.c_void_p,c.POINTER(c.c_ulong)]
kernel.GetProcessHandleCount.restype=c.c_int
def handles():
    # https://learn.microsoft.com/windows/win32/api/processthreadsapi/nf-processthreadsapi-getprocesshandlecount
    result=c.c_ulong()
    assert kernel.GetProcessHandleCount(kernel.GetCurrentProcess(),c.byref(result))
    return result.value

def arm(forward,recovery):
    writes=native.WriteQuota(helpers.grant(),helpers.grant())
    work=native.NativeWorkQuota(writes,forward,recovery)
    callbacks=native.CallbackQuota(writes,(128,0),(128,0),work,
        buffers=(1 << 40, 1 << 40),
    )
    gate.arm(helpers.reads(native,writes),callbacks)
    return writes,callbacks

if scenario.startswith("binding_"):
    present=scenario.endswith("_present")
    cost=native.filesystem_cost("bind_journal" if present else "bind_database",(1,1))
    closes=2 if present else 1
    forward=(cost[0]+closes-(1 if "_forward_" in scenario else 0),0)
    recovery=(closes-(1 if "_recovery_" in scenario else 0),0)
    writes,calls=arm(forward,recovery)
    baseline=handles()
    borrowed=api.open(path)
    identity=helpers.journal_identity(path,api)
    before=handles()
    try:
        try:
            gate.bind(str(path),borrowed,identity)
        except RuntimeError:
            pass
        else:
            raise AssertionError("binding delegated without complete acquisition/close funding")
        assert not gate.bound and not gate.quarantined and handles()==before
        assert writes.recovering
        assert writes.report()["first_refusal"]["reason"]=="native filesystem attempt grant"
        assert all(not any(bank.values()) for bank in calls.work.report().values())
    finally:
        api.close(borrowed)
    assert handles()==baseline
    moved=path.with_suffix(".moved")
    path.rename(moved)
    moved.rename(path)
    writes,calls=arm((cost[0]+closes,0),(closes,0))
    borrowed=api.open(path)
    try:
        gate.bind(str(path),borrowed,identity)
    finally:
        api.close(borrowed)
    assert gate.bound and handles()==baseline+closes
    paid=calls.work.report()
    assert paid["forward"]["filesystem_api_attempts_charged"]==cost[0]+closes
    assert paid["recovery"]["filesystem_api_attempts_charged"]==closes
    assert paid["forward"]["close_filesystem_api_attempts_prepaid"]==closes
    assert paid["recovery"]["close_filesystem_api_attempts_prepaid"]==closes
    gate.unbind()
    assert handles()==baseline and calls.work.report()==paid
    assert heap.retry_report()["state"]=="unconfigured"
else:
    assert scenario=="journal_generations" and not journal.exists()
    baseline=handles()
    borrowed=api.open(path)
    try:
        gate.bind(str(path),borrowed,None)
    finally:
        api.close(borrowed)
    helpers.admit_fixture_retries(gate)
    assert handles()==baseline+1
    for generation in range(2):
        open_cost=native.filesystem_cost("open_journal_new",(2,3))
        writes,calls=arm((open_cost[0]+4,open_cost[1]+200000),(4,200000))
        with helpers.native_file(path,gate,kind="journal",binding=False) as opened:
            assert opened[0]==sqlite3.SQLITE_OK and handles()==baseline+3
            paid=calls.work.report()
            assert paid["forward"]["filesystem_api_attempts_charged"]==open_cost[0]+4
            assert paid["recovery"]["filesystem_api_attempts_charged"]==4
            assert paid["forward"]["close_filesystem_api_attempts_prepaid"]==4
            assert paid["recovery"]["close_filesystem_api_attempts_prepaid"]==4
        assert handles()==baseline+2 and journal.read_bytes()==b""
        assert calls.work.report()==paid
        writes,calls=arm((0,0),(0,0))
        assert vfs.contents.delete(vfs,name,0)==sqlite3.SQLITE_FULL
        assert handles()==baseline+2 and journal.exists() and not gate.quarantined
        assert all(not any(bank.values()) for bank in calls.work.report().values())
        deletion=native.filesystem_cost("delete_journal",(2,3))
        writes,calls=arm(deletion,(0,0))
        assert vfs.contents.delete(vfs,name,0)==sqlite3.SQLITE_OK
        assert not journal.exists() and handles()==baseline+1
        report=calls.work.report()
        assert report["forward"]["filesystem_api_attempts_charged"]==deletion[0]
        assert report["forward"]["internal_sleep_microseconds_charged"]==deletion[1]
        assert report["forward"]["close_filesystem_api_attempts_prepaid"]==0
        assert not any(report["recovery"].values())
    # The original database pin's close was funded by the initial binding, not
    # either replacement generation/deletion operation bank.
    paid=calls.work.report()
    gate.unbind()
    assert handles()==baseline and calls.work.report()==paid

gate.close()
assert (path.read_bytes(),journal.read_bytes() if journal.exists() else None)==original
assert heap.report()["sqlite_heap_peak_bytes"]<=64*1024*1024
print(json.dumps({"passed":True,"scenario":scenario,"pins_released":True,"original_exact":True}))
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
    assert json.loads(result.stdout) == {
        "passed": True,
        "scenario": scenario,
        "pins_released": True,
        "original_exact": True,
    }
    assert (path.read_bytes(), journal.read_bytes() if journal.exists() else None) == original
    with closing(sqlite3.connect(path)) as cold:
        assert cold.execute("SELECT body FROM original").fetchone() == (b"exact original",)


@pytest.mark.parametrize("scenario", ["source", "binary", "qualified"])
def test_native_binary_admission_precedes_global_policy_in_a_fresh_process(scenario):
    import json
    import subprocess
    import sys

    code = r"""
import ctypes as c, json, sqlite3, sys
from riverhog_client import _prepared_primary_native as native
from riverhog_client import _prepared_primary_runtime as runtime
scenario=sys.argv[1]
identity=native.runtime_identity()
dll=c.CDLL(identity["sqlite_library_path"])
dll.sqlite3_hard_heap_limit64.argtypes=[c.c_longlong]
dll.sqlite3_hard_heap_limit64.restype=c.c_longlong
assert dll.sqlite3_hard_heap_limit64(-1)==0
observed_logs=[]
logger_type=c.CFUNCTYPE(None,c.c_void_p,c.c_int,c.c_char_p)
@logger_type
def logger(context,code,message):
    observed_logs.append((code,message))
dll.sqlite3_config.argtypes=[c.c_int]
dll.sqlite3_config.restype=c.c_int
dll.sqlite3_log.argtypes=[c.c_int,c.c_char_p]
dll.sqlite3_log.restype=None
assert dll.sqlite3_config(16,logger,c.c_void_p())==sqlite3.SQLITE_OK
dll.sqlite3_log(1,b"before application admission")
assert len(observed_logs)==1
connection=sqlite3.connect(":memory:")
try:
    observed=connection.execute("SELECT sqlite_source_id()").fetchone()[0]
    assert observed==identity["sqlite_source_id"]
finally:
    connection.close()
source,digest=runtime._SQLITE_SOURCE_ID,runtime._SQLITE_LIBRARY_SHA256
if scenario!="qualified":
    if scenario=="source":
        runtime._SQLITE_SOURCE_ID="unqualified source"
    else:
        runtime._SQLITE_LIBRARY_SHA256="0"*64
    try:
        runtime._admit_process_budget(64*1024*1024,native_limits=(128,512))
    except RuntimeError as exc:
        assert ("source" if scenario=="source" else "binary") in str(exc)
    else:
        raise AssertionError("unqualified runtime changed global resource policy")
    assert dll.sqlite3_hard_heap_limit64(-1)==0
    before=len(observed_logs)
    dll.sqlite3_log(1,b"rejected binary retains existing logger")
    assert len(observed_logs)==before+1
    try:
        native.current_process_budget()
    except RuntimeError as exc:
        assert "not configured" in str(exc)
    else:
        raise AssertionError("rejected runtime acquired an application heap owner")
    runtime._SQLITE_SOURCE_ID,runtime._SQLITE_LIBRARY_SHA256=source,digest
qualified=runtime._qualified_runtime()
assert qualified=={"sqlite_source_id":source,"sqlite_library_sha256":digest}
assert dll.sqlite3_hard_heap_limit64(-1)==0
owner=runtime._admit_process_budget(64*1024*1024,native_limits=(128,512))
assert native.current_process_budget() is owner
assert owner.report()["heap_limit_bytes"]==64*1024*1024
assert owner.report()["logger_disabled_at_admission"] is True
before=len(observed_logs)
dll.sqlite3_log(1,b"owned admission cannot invoke a foreign logger")
assert len(observed_logs)==before
assert owner.retry_report()["state"]=="unconfigured"
try:
    runtime._admit_process_budget(128*1024*1024,native_limits=(128,512))
except RuntimeError as exc:
    assert "already" in str(exc)
else:
    raise AssertionError("runtime admission replaced an existing global heap grant")
assert dll.sqlite3_hard_heap_limit64(-1)==64*1024*1024
print(json.dumps({"passed":True,"scenario":scenario,"binary_verified_before_heap":True}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, scenario],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {
        "passed": True,
        "scenario": scenario,
        "binary_verified_before_heap": True,
    }


@pytest.mark.parametrize(
    "scenario",
    [
        "random",
        "error",
        "fullpath",
        "recovery_random",
        "recovery_error",
        "recovery_fullpath",
        "zero_random",
        "null_random",
        "negative_error",
        "zero_fullpath",
        "long_access",
        "long_fullpath",
        "long_binding",
    ],
)
def test_native_output_buffers_and_bounded_paths_preserve_originals_and_paid_closes(
    tmp_path, scenario
):
    import json
    import subprocess
    import sys
    from pathlib import Path

    path = tmp_path / "primary.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode=PERSIST")
        connection.execute("PRAGMA synchronous=EXTRA")
        connection.execute("CREATE TABLE original(body BLOB NOT NULL)")
        connection.execute("INSERT INTO original VALUES (?)", (b"exact original",))
        connection.commit()
    finally:
        connection.close()
    journal = path.with_name(path.name + "-journal")
    original = (path.read_bytes(), journal.read_bytes())
    code = r"""
import ctypes as c, importlib.util, json, sqlite3, sys
from contextlib import ExitStack
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_windows_native import _WindowsAPI
from tests.support.sqlite_vfs_faults import FaultVfs, windows_system_call
spec=importlib.util.spec_from_file_location(
    "native_output_helpers",
    Path(sys.argv[1])/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py",
)
helpers=importlib.util.module_from_spec(spec);sys.modules[spec.name]=helpers
spec.loader.exec_module(helpers)
path=Path(sys.argv[2]);scenario=sys.argv[3]
journal=path.with_name(path.name+"-journal")
original=(path.read_bytes(),journal.read_bytes())
source=native.runtime_identity()["sqlite_source_id"]
heap=_admit_process_budget(64*1024*1024,native_limits=(128,512))
initial=native.WriteQuota(helpers.grant(),helpers.grant())
initial_calls=helpers.callbacks(native,initial)
gate=native.WriteVfs(source,helpers.reads(native,initial),initial_calls,heap)
library=FaultVfs.windows().library
vfs=library.sqlite3_vfs_find(gate.name.encode("ascii"))
api=_WindowsAPI();handle=api.open(path)
original_journal=helpers.journal_identity(path,api)
bad_name=c.create_string_buffer(b"x"*(vfs.contents.pathname_bytes+9))
name=c.create_string_buffer(str(path).encode("utf-8"))
output=c.create_string_buffer(b"?"*(vfs.contents.pathname_bytes+1))
if scenario=="long_binding":
    kernel=c.WinDLL("kernel32",use_last_error=True)
    kernel.GetCurrentProcess.restype=c.c_void_p
    kernel.GetProcessHandleCount.argtypes=[c.c_void_p,c.POINTER(c.c_ulong)]
    kernel.GetProcessHandleCount.restype=c.c_int
    def handles():
        value=c.c_ulong()
        assert kernel.GetProcessHandleCount(kernel.GetCurrentProcess(),c.byref(value))
        return value.value
    before=handles()
    try:
        gate.bind(bad_name.value.decode("ascii"),handle,original_journal)
    except RuntimeError:
        pass
    else:
        raise AssertionError("oversized binding path delegated native work")
    assert handles()==before and not gate.bound and not gate.quarantined
    assert initial.report()["first_refusal"]["reason"]=="invalid VFS buffer range"
    assert all(not any(bank.values()) for bank in initial_calls.work.report().values())
else:
    gate.bind(str(path),handle,original_journal)
    helpers.admit_fixture_retries(gate)
    with helpers.native_file(path,gate,binding=False,create=False) as opened:
        assert opened[0]==sqlite3.SQLITE_OK
        raw,methods=opened[1],opened[2]
        method=scenario.removeprefix("recovery_")
        amount=len(output) if method=="fullpath" else 64
        regular=method in ("random","error","fullpath")
        recovering=scenario.startswith("recovery_")
        grants=(amount-1,amount-1) if recovering else (amount-1,amount)
        if not regular:
            grants=(4096,4096)
        writes=native.WriteQuota(helpers.grant(),helpers.grant())
        work=native.NativeWorkQuota(writes,(1<<40,1<<60),(1<<40,1<<60))
        calls=native.CallbackQuota(
            writes,(128,0),(128,0),work,buffers=grants,
        )
        gate.arm(helpers.reads(native,writes),calls)
        del initial,initial_calls
        if recovering:writes.failed()
        observed={}
        hooks={
            b"GetSystemTime":c.WINFUNCTYPE(None,c.c_void_p,use_last_error=True),
            b"GetCurrentProcessId":c.WINFUNCTYPE(c.c_ulong),
            b"GetTickCount":c.WINFUNCTYPE(c.c_ulong),
            b"QueryPerformanceCounter":c.WINFUNCTYPE(c.c_int,c.c_void_p,use_last_error=True),
            b"GetLastError":c.WINFUNCTYPE(c.c_ulong),
            b"FormatMessageW":c.WINFUNCTYPE(
                c.c_ulong,c.c_ulong,c.c_void_p,c.c_ulong,c.c_ulong,
                c.c_void_p,c.c_ulong,c.c_void_p,use_last_error=True,
            ),
            b"GetFullPathNameW":c.WINFUNCTYPE(
                c.c_ulong,c.c_wchar_p,c.c_ulong,c.c_void_p,c.c_void_p,use_last_error=True,
            ),
            b"GetFileAttributesExW":c.WINFUNCTYPE(
                c.c_int,c.c_wchar_p,c.c_int,c.c_void_p,use_last_error=True,
            ),
            b"CloseHandle":c.WINFUNCTYPE(c.c_int,c.c_void_p,use_last_error=True),
        }
        def observer(name):
            def observe(original,*args):
                observed[name]=observed.get(name,0)+1
                return 2 if name==b"GetLastError" else original(*args)
            return observe
        def surface():
            value=c.c_double(-7)
            assert vfs.contents.time(vfs,c.byref(value))==sqlite3.SQLITE_FULL
            assert value.value==-7 and not calls.report()["pending_error"]
        def invoke():
            if method=="random":return vfs.contents.randomness(vfs,amount,output)
            if method=="error":return vfs.contents.error(vfs,amount,output)
            return vfs.contents.pathname(vfs,name,amount,output)
        with ExitStack() as stack:
            for hook,kind in hooks.items():
                stack.enter_context(windows_system_call(hook,kind,observer(hook)))
            before=output.raw
            if regular:
                refused=invoke()
                assert refused==(sqlite3.SQLITE_FULL if method=="fullpath" else 0)
                assert not observed and writes.recovering
                assert writes.report()["first_refusal"]["reason"]=="VFS output-buffer grant"
                assert all(
                    not calls.report()[bank]["output_buffer_bytes_charged"]
                    for bank in ("forward","recovery")
                )
                assert output.raw==(b"\0"+before[1:] if method=="error" else before)
                assert all(not any(bank.values()) for bank in work.report().values())
                if calls.report()["pending_error"]:surface()
                if not recovering:
                    result=invoke()
                    if method=="random":
                        assert 0<result<=amount and observed[b"GetSystemTime"]==1
                        assert observed[b"GetCurrentProcessId"]==1
                        assert observed[b"GetTickCount"]==1
                        assert observed[b"QueryPerformanceCounter"]==1
                    elif method=="error":
                        assert result==2 and observed[b"FormatMessageW"]==1
                    else:
                        assert result==sqlite3.SQLITE_OK and observed[b"GetFullPathNameW"]<=2
                    assert calls.report()["recovery"]["output_buffer_bytes_charged"]==amount
                    snapshot=dict(observed);paid=work.report()
                    assert invoke()==(sqlite3.SQLITE_FULL if method=="fullpath" else 0)
                    assert observed==snapshot and work.report()==paid
                    if calls.report()["pending_error"]:surface()
            elif scenario=="zero_random":
                assert vfs.contents.randomness(vfs,0,None)==0
                assert not observed and not writes.recovering
                assert calls.report()["forward"]["output_buffer_bytes_charged"]==0
            else:
                if scenario=="null_random":
                    assert vfs.contents.randomness(vfs,7,None)==0
                elif scenario=="negative_error":
                    assert vfs.contents.error(vfs,-1,output)==0
                elif scenario=="zero_fullpath":
                    assert vfs.contents.pathname(vfs,name,0,output)==sqlite3.SQLITE_FULL
                elif scenario=="long_access":
                    value=c.c_int(-7)
                    assert vfs.contents.access(vfs,bad_name,0,c.byref(value))==sqlite3.SQLITE_FULL
                    assert value.value==-7
                else:
                    assert scenario=="long_fullpath"
                    assert vfs.contents.pathname(
                        vfs,bad_name,len(output),output
                    )==sqlite3.SQLITE_FULL
                assert not observed and output.raw==before
                assert writes.report()["first_refusal"]["reason"]=="invalid VFS buffer range"
                assert all(not any(bank.values()) for bank in work.report().values())
                if calls.report()["pending_error"]:surface()
            paid=work.report()
            charged=calls.report()
            assert methods.contents.close(raw)==sqlite3.SQLITE_OK
            assert observed[b"CloseHandle"]==1
            assert work.report()==paid and gate.open_files==0 and not gate.quarantined
            for bank in ("forward","recovery"):
                assert calls.report()[bank]["output_buffer_bytes_charged"]==(
                    charged[bank]["output_buffer_bytes_charged"]
                )
        gate.unbind()
        assert work.report()==paid and not gate.bound and not gate.quarantined
gate.close();api.close(handle)
assert (path.read_bytes(),journal.read_bytes())==original
print(json.dumps({"passed":True,"scenario":scenario,"original_exact":True,"pins_released":True}))
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
        timeout=60,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {
        "passed": True,
        "scenario": scenario,
        "original_exact": True,
        "pins_released": True,
    }
    assert (path.read_bytes(), journal.read_bytes()) == original
    with closing(sqlite3.connect(path)) as cold:
        assert cold.execute("SELECT body FROM original").fetchone() == (b"exact original",)


@pytest.mark.parametrize(
    "scenario",
    [
        "owner-zero",
        "owner-limit",
        "owner-threads",
        "file-limit",
        "file-threads",
        "bootstrap-zero",
        "close-failure",
        "deferred-close",
    ],
)
def test_native_population_grants_cover_lifetime_quarantine_and_concurrent_admission(
    tmp_path, scenario
):
    import json
    import subprocess
    import sys
    from pathlib import Path

    path = tmp_path / "primary.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA journal_mode=PERSIST")
        connection.execute("PRAGMA synchronous=EXTRA")
        connection.execute("CREATE TABLE original(body BLOB NOT NULL)")
        connection.execute("INSERT INTO original VALUES (?)", (b"exact original",))
        connection.commit()
    journal = path.with_name(path.name + "-journal")
    original = (path.read_bytes(), journal.read_bytes())
    code = r"""
import ctypes as c, importlib.util, json, sqlite3, sys, threading
from contextlib import ExitStack
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_windows_native import _WindowsAPI
from tests.support.sqlite_vfs_faults import FaultVfs, windows_system_call
spec=importlib.util.spec_from_file_location("population_helpers",
    Path(sys.argv[1])/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py")
helpers=importlib.util.module_from_spec(spec);sys.modules[spec.name]=helpers
spec.loader.exec_module(helpers)
path=Path(sys.argv[2]);scenario=sys.argv[3]
journal=path.with_name(path.name+"-journal")
original=(path.read_bytes(),journal.read_bytes())
limits={"owner-zero":(0,8),"owner-limit":(1,8),"owner-threads":(2,8),
    "file-limit":(2,1),"file-threads":(8,2),"bootstrap-zero":(1,0),
    "close-failure":(1,1),"deferred-close":(1,1)}[scenario]
heap=_admit_process_budget(64*1024*1024,native_limits=limits)
source=native.runtime_identity()["sqlite_source_id"]
def reject(action,message):
    try:action()
    except (RuntimeError,ValueError) as exc:assert message in str(exc),str(exc)
    else:raise AssertionError("native admission should have stopped: "+message)
def make(source_id=source):
    writes=native.WriteQuota(helpers.grant(),helpers.grant())
    calls=helpers.callbacks(native,writes)
    return native.WriteVfs(source_id,helpers.reads(native,writes),calls,heap),writes,calls
def bound():
    gate,writes,calls=make()
    api=_WindowsAPI();handle=api.open(path)
    try:gate.bind(str(path),handle,helpers.journal_identity(path,api))
    finally:api.close(handle)
    return gate,writes,calls
def fresh(gate,buffers=(1<<40,1<<40)):
    writes=native.WriteQuota(helpers.grant(),helpers.grant())
    work=native.NativeWorkQuota(writes,(1<<40,1<<60),(1<<40,1<<60))
    calls=native.CallbackQuota(writes,(262144,0),(262144,0),work,buffers=buffers)
    gate.arm(helpers.reads(native,writes),calls)
    return writes,calls
def report():
    value=heap.native_report()
    assert 0<=value["owners"]<=value["owner_peak"]<=value["owner_limit"]
    assert 0<=value["files"]<=value["file_peak"]<=value["file_limit"]
    return value

if scenario=="owner-zero":
    reject(make,"owner population")
    assert report()["owners"]==report()["owner_peak"]==0
    heap.increase_native((1,8))
    gate,_,_=make();assert report()["owners"]==1
    gate.close()
elif scenario=="owner-limit":
    gate,_,_=make()
    reject(make,"owner population")
    assert report()["owners"]==1 and report()["owner_refusals"]==1
    for invalid in ((1,8),(0,8),(2,7)):
        reject(lambda:heap.increase_native(invalid),"no decrease")
    assert report()["owner_limit"]==1
    gate.close();assert report()["owners"]==0
    reject(lambda:make("unqualified source"),"source identity")
    assert report()["owners"]==0
    gate,_,_=make();gate.close()
elif scenario in ("owner-threads","file-threads"):
    if scenario=="file-threads":
        first,_,_=bound();helpers.admit_fixture_retries(first);first.close()
    barrier=threading.Barrier(9,timeout=20);release=threading.Event()
    errors=[];outcomes=[]
    def run():
        gate=None
        try:
            with ExitStack() as stack:
                if scenario=="owner-threads":
                    try:gate,_,_=make()
                    except RuntimeError as exc:
                        assert "owner population" in str(exc)
                        outcomes.append(False)
                    else:outcomes.append(True)
                else:
                    gate,_,_=bound()
                    result,raw,methods=stack.enter_context(
                        helpers.native_file(path,gate,binding=False,create=False))
                    assert result in (sqlite3.SQLITE_OK,sqlite3.SQLITE_FULL)
                    assert bool(methods)==(result==sqlite3.SQLITE_OK)
                    outcomes.append(result==sqlite3.SQLITE_OK)
                barrier.wait()
                assert release.wait(20)
        except BaseException as exc:
            errors.append(repr(exc))
            barrier.abort()
        finally:
            if gate is not None:
                try:gate.close()
                except BaseException as exc:errors.append(repr(exc))
    threads=[threading.Thread(target=run) for _ in range(8)]
    for thread in threads:thread.start()
    try:
        barrier.wait()
        held=report()
        assert len(outcomes)==8 and sum(outcomes)==2,outcomes
        key="owners" if scenario=="owner-threads" else "files"
        assert held[key]==2
        assert held["owner_refusals" if key=="owners" else "file_refusals"]==6
    finally:
        release.set()
        for thread in threads:thread.join(25)
    assert not any(thread.is_alive() for thread in threads)
    assert not errors,errors
elif scenario=="bootstrap-zero":
    gate,writes,calls=bound()
    reject(lambda:helpers.admit_fixture_retries(gate),"file population")
    assert heap.retry_report()["state"]=="unconfigured"
    assert not gate.quarantined and writes.recovering
    assert report()["files"]==report()["file_peak"]==0
    assert report()["file_refusals"]==1
    assert writes.report()["first_refusal"]["reason"]=="native SQLite file population grant"
    heap.increase_native((1,1))
    helpers.admit_fixture_retries(gate)
    assert report()["file_peak"]==1 and report()["files"]==0
    gate.close()
else:
    gate,writes,calls=bound()
    helpers.admit_fixture_retries(gate)
    if scenario=="file-limit":
        with helpers.native_file(path,gate,binding=False,create=False) as first:
            assert first[0]==sqlite3.SQLITE_OK and report()["files"]==1
            before=calls.work.report()
            with helpers.native_file(path,gate,binding=False,create=False) as stopped:
                assert stopped[0]==sqlite3.SQLITE_FULL and stopped[2] is None
                assert calls.work.report()==before
            assert writes.recovering and not gate.quarantined
            assert writes.report()["first_refusal"]["reason"]=="native SQLite file population grant"
        assert report()["files"]==0
        heap.increase_native((2,2));fresh(gate)
        with ExitStack() as stack:
            for _ in range(2):
                assert stack.enter_context(helpers.native_file(
                    path,gate,binding=False,create=False))[0]==sqlite3.SQLITE_OK
            assert report()["files"]==2
        assert report()["files"]==0
        gate.close()
    elif scenario=="close-failure":
        with helpers.native_file(path,gate,binding=False,create=False) as opened:
            assert opened[0]==sqlite3.SQLITE_OK
            signature=c.WINFUNCTYPE(c.c_int,c.c_void_p,use_last_error=True)
            def fail_close(original,handle):
                c.set_last_error(5)
                return 0
            with windows_system_call(b"CloseHandle",signature,fail_close):
                assert opened[2].contents.close(opened[1])==sqlite3.SQLITE_IOERR|(16<<8)
            assert not opened[1].contents.methods
        assert gate.open_files==0 and gate.quarantined
        assert report()["owners"]==report()["files"]==1
        reject(gate.close,"quarantined")
        reject(make,"owner population")
        del gate,writes,calls
        import gc;gc.collect()
        assert report()["owners"]==report()["files"]==1
        heap.increase_native((2,2))
        other,_,_=bound()
        with helpers.native_file(path,other,binding=False,create=False) as opened:
            assert opened[0]==sqlite3.SQLITE_OK and report()["files"]==2
        other.close()
        assert report()["owners"]==report()["files"]==1
    else:
        assert scenario=="deferred-close"
        with helpers.native_file(path,gate,binding=False,create=False) as opened:
            assert opened[0]==sqlite3.SQLITE_OK
            fresh(gate,buffers=(0,0))
            vfs=FaultVfs.windows().library.sqlite3_vfs_find(gate.name.encode())
            output=c.create_string_buffer(1)
            assert vfs.contents.randomness(vfs,1,output)==0
            assert opened[2].contents.close(opened[1])==sqlite3.SQLITE_FULL
            assert not opened[1].contents.methods
        assert report()["files"]==0 and not gate.quarantined
        gate.close()
final=report()
if scenario!="close-failure":
    assert final["owners"]==final["files"]==0,final
assert (path.read_bytes(),journal.read_bytes())==original
print(json.dumps({"passed":True,"scenario":scenario,"population":final}))
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(Path(__file__).resolve().parents[3]), str(path), scenario],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    proof = json.loads(result.stdout)
    assert proof["passed"] is True and proof["scenario"] == scenario
    assert proof["population"]["owner_peak"] <= proof["population"]["owner_limit"]
    assert proof["population"]["file_peak"] <= proof["population"]["file_limit"]
    assert (path.read_bytes(), journal.read_bytes()) == original
    with closing(sqlite3.connect(path)) as reopened:
        assert reopened.execute("SELECT body FROM original").fetchall() == [(b"exact original",)]


@pytest.mark.parametrize("operation", ["reader", "writer", "close", "multiple"])
def test_native_memory_observer_covers_implicit_recovery_and_connection_lifetimes(
    tmp_path, operation
):
    import json
    import subprocess
    import sys

    code = r"""
import json,runpy,sqlite3,sys
from pathlib import Path
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_runtime import _SQLITE_SOURCE_ID as source
helpers=runpy.run_path(sys.argv[1])
heap=_admit_process_budget(64*1024*1024,native_limits=(2,8))
def bank():
    writes=native.WriteQuota(helpers["grant"](),helpers["grant"]())
    return writes,helpers["reads"](native,writes),helpers["callbacks"](native,writes)
writes,reads,calls=bank()
gate=native.WriteVfs(source,reads,calls,heap)
path=Path("primary.sqlite3").resolve()
connection=helpers["opened"](path,gate)
original=b"exact original"*4096
other=None
try:
    connection.execute("PRAGMA journal_mode=PERSIST").close()
    connection.execute("PRAGMA synchronous=EXTRA").close()
    connection.execute("PRAGMA cache_spill=OFF").close()
    connection.execute("CREATE TABLE sample(value BLOB NOT NULL)").close()
    connection.execute("INSERT INTO sample VALUES (?)",(original,)).close()
    connection.commit()
    if sys.argv[2]=="multiple":
        other=sqlite3.connect(path.as_uri()+"?vfs="+gate.name,uri=True)
    writes,reads,calls=bank()
    gate.arm(reads,calls)
    connection.execute("BEGIN IMMEDIATE").close()
    connection.execute("UPDATE sample SET value=? WHERE rowid=1",(b"uncommitted",)).close()
    before=writes.report()
    assert not before["recovering"] and before["native_failures"]==0
    if sys.argv[2]=="close":
        connection.close()
    else:
        statement=("UPDATE sample SET value=randomblob(?) WHERE rowid=1" if sys.argv[2]=="writer"
            else "SELECT randomblob(?) FROM sample WHERE rowid=1")
        try:
            connection.execute(statement,(128*1024*1024,)).fetchone()
        except (MemoryError,sqlite3.Error) as exc:
            if isinstance(exc,sqlite3.Error):
                assert exc.sqlite_errorcode in (sqlite3.SQLITE_NOMEM,sqlite3.SQLITE_IOERR_NOMEM)
        else:
            raise AssertionError("actual SQLite heap exhaustion did not reject the operation")
    # No explicit rollback or Python-side quota.failed() precedes these facts.
    after=writes.report()
    assert after["recovering"],after
    assert after["native_failures"]==(0 if sys.argv[2]=="close" else 1),after
    assert after["refusals"]==0 and after["first_refusal"] is None,after
    if sys.argv[2]!="close":
        assert not connection.in_transaction
    assert after["recovery"]["write_calls"]>0,after
    assert after["forward"]["write_calls"]==before["forward"]["write_calls"],(before,after)
    assert heap.report()["heap_limit_bytes"]==64*1024*1024
    assert heap.report()["sqlite_heap_peak_bytes"]<=64*1024*1024
    if sys.argv[2]!="close":
        replacement,replacement_reads,replacement_calls=bank()
        try:
            gate.arm(replacement_reads,replacement_calls)
        except RuntimeError as exc:
            assert "poisoned" in str(exc)
        else:
            raise AssertionError("live poisoned connection allowed a replacement operation")
    if other is not None:
        # Remove the older main-file observer, then exercise the remaining one.
        # Its C callback must never retain a pointer to the now-closed connection.
        connection.connection.close()
        assert other.execute("SELECT value FROM sample").fetchone()==(original,)
finally:
    if other is not None:
        other.close()
    connection.close()
assert gate.open_files==0 and not gate.bound
replacement,replacement_reads,replacement_calls=bank()
gate.arm(replacement_reads,replacement_calls)
connection=helpers["opened"](path,gate)
try:
    assert connection.execute("SELECT value FROM sample").fetchone()==(original,)
    connection.execute("UPDATE sample SET value=? WHERE rowid=1",(b"later progress",)).close()
    connection.commit()
    assert connection.execute("SELECT value FROM sample").fetchone()==(b"later progress",)
finally:
    connection.close()
gate.close()
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
print(json.dumps({"passed":True,"operation":sys.argv[2],
    "recovery_bank_verified":True,"original_exact":True,"heap_limit_unchanged":True}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(Path(__file__).resolve()), operation],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {
        "passed": True,
        "operation": operation,
        "recovery_bank_verified": True,
        "original_exact": True,
        "heap_limit_unchanged": True,
    }


@pytest.mark.parametrize(
    "scenario",
    [
        "success",
        "live",
        "metadata-before",
        "metadata-after",
        "native-close",
        "constructor",
        "construction-quarantine",
        "thread",
    ],
)
def test_native_metadata_custody_covers_quarantine_population_and_process_exit(tmp_path, scenario):
    import json
    import subprocess
    import sys

    code = r'''
import ctypes as c,gc,importlib.util,json,sqlite3,subprocess,sys,threading,weakref
from contextlib import closing
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from riverhog_client import PrimaryStateBusy,PrimaryStateError
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_windows_native import (
    NativeWindowsContainer,windows_container_path)
from tests.support.sqlite_vfs_faults import windows_system_call
spec=importlib.util.spec_from_file_location("custody_helpers",root/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py")
helpers=importlib.util.module_from_spec(spec);sys.modules[spec.name]=helpers;spec.loader.exec_module(helpers)
spec2=importlib.util.spec_from_file_location("custody_parent",root/"tests/platform/windows_primary_process.py")
parent_helpers=importlib.util.module_from_spec(spec2);sys.modules[spec2.name]=parent_helpers;spec2.loader.exec_module(parent_helpers)
parent=Path(sys.argv[2])/"custody-private";parent_helpers._parent(parent)
path=windows_container_path(parent,b"native metadata custody");scenario=sys.argv[3]
heap=_admit_process_budget(64*1024*1024,native_limits=(1,2))
source=native.runtime_identity()["sqlite_source_id"]
def make():
    writes=native.WriteQuota(helpers.grant(),helpers.grant())
    return native.WriteVfs(
        source,helpers.reads(native,writes),helpers.callbacks(native,writes),heap)
def reject(action,message):
    try:action()
    except RuntimeError as exc:assert message in str(exc),str(exc)
    else:raise AssertionError("native custody refusal missing: "+message)
gate=make()
if scenario=="construction-quarantine":
    from riverhog_client._prepared_primary_windows_native import _WindowsAPI
    info=_WindowsAPI.information;close=_WindowsAPI.close;uncertain=[]
    def information(api,handle,candidate,*,directory):
        if candidate==path:
            uncertain.append(handle)
            raise PrimaryStateError("injected metadata admission failure")
        return info(api,handle,candidate,directory=directory)
    def unknown_close(api,handle):
        if handle in uncertain:
            raise PrimaryStateError("injected admission close response loss")
        return close(api,handle)
    owner=object.__new__(NativeWindowsContainer);reference=weakref.ref(owner)
    _WindowsAPI.information=information;_WindowsAPI.close=unknown_close
    try:
        try:owner.__init__(path,initialize=True,gate=gate)
        except PrimaryStateError:pass
        else:raise AssertionError("uncertain metadata admission reported success")
    finally:
        _WindowsAPI.information=info;_WindowsAPI.close=close
    assert uncertain and gate.quarantined and owner.quarantined
    assert path.stat().st_size==0 and not Path(str(path)+"-journal").exists()
    assert heap.native_report()["owners"]==1 and heap.native_report()["files"]==0
    reject(gate.close,"custody");reject(make,"owner population")
    try:owner.close()
    except PrimaryStateError:pass
    else:raise AssertionError("failed admission metadata was retried")
    del owner;gc.collect();assert reference() is not None
    probe="\n".join([
        "from pathlib import Path", "import sys",
        "from riverhog_client import PrimaryStateBusy",
        "from riverhog_client._prepared_primary_windows_native import WindowsContainer",
        "try:WindowsContainer(Path(sys.argv[1]),initialize=False)",
        "except PrimaryStateBusy:raise SystemExit(0)",
        "raise AssertionError('failed admission released exclusive custody')"])
    result=subprocess.run([sys.executable,"-I","-c",probe,str(path)],
        capture_output=True,text=True,timeout=30)
    assert result.returncode==0,(result.stdout,result.stderr)
    print(json.dumps({"passed":True,"scenario":scenario,"quarantined":True,
        "filename":path.name,"population":heap.native_report(),"admission_failed":True}))
    raise SystemExit(0)
if scenario=="constructor":
    try:NativeWindowsContainer(parent/"unsupported.sqlite3",initialize=True,gate=gate)
    except PrimaryStateError:pass
    else:raise AssertionError("invalid metadata placement accepted")
    assert heap.native_report()["owners"]==0
    reject(lambda:gate.name,"closed")
    gate=make()
owner=NativeWindowsContainer(path,initialize=True,gate=gate)
assert owner.vfs==gate.name and gate.bound and heap.native_report()["owners"]==1
reference=weakref.ref(owner)
reject(lambda:gate.retain_custodian(object()),"custody")
reject(lambda:gate.complete_custody(object()),"custody")
reject(lambda:gate.complete_custody(owner),"custody")
reject(gate.close,"custody")
helpers.admit_fixture_retries(gate)
connection=sqlite3.connect(path.as_uri()+"?vfs="+owner.vfs,uri=True)
connection.execute("PRAGMA journal_mode=PERSIST")
connection.execute("PRAGMA synchronous=EXTRA")
connection.execute("CREATE TABLE original(body BLOB NOT NULL)")
connection.execute("INSERT INTO original VALUES (?)",(b"exact original",));connection.commit()
if scenario=="live":
    try:owner.close()
    except PrimaryStateBusy:pass
    else:raise AssertionError("metadata owner closed while SQLite files remain")
    assert not owner._closed and gate.bound
    reject(lambda:gate.complete_custody(owner),"custody")
if scenario=="thread":
    failures=[]
    def wrong_thread():
        for action in (owner.close,lambda:gate.complete_custody(owner),gate.quarantine):
            try:action()
            except RuntimeError:failures.append(True)
            else:failures.append(False)
    thread=threading.Thread(target=wrong_thread);thread.start();thread.join(10)
    assert not thread.is_alive() and failures==[True,True,True]
    assert not owner._closed and not gate.quarantined
connection.close()
assert gate.open_files==0
original=(path.read_bytes(),Path(str(path)+"-journal").read_bytes())
if scenario=="native-close":
    signature=c.WINFUNCTYPE(c.c_int,c.c_void_p,use_last_error=True)
    def fail_close(original_close,handle):
        c.set_last_error(5);return 0
    with helpers.native_file(path,gate,binding=False,create=False) as opened:
        assert opened[0]==sqlite3.SQLITE_OK
        with windows_system_call(b"CloseHandle",signature,fail_close):
            assert opened[2].contents.close(opened[1])==sqlite3.SQLITE_IOERR|(16<<8)
        assert not opened[1].contents.methods
    assert gate.quarantined and gate.open_files==0
    try:owner.close()
    except RuntimeError:pass
    else:raise AssertionError("native quarantine released metadata custody")
    assert not owner._closed
elif scenario.startswith("metadata-"):
    api=owner._api;real_close=api.close;database=owner._database
    def uncertain(handle):
        if handle==database:
            if scenario=="metadata-after":real_close(handle)
            raise PrimaryStateError("injected uncertain metadata close")
        real_close(handle)
    api.close=uncertain
    try:owner.close()
    except PrimaryStateError:pass
    else:raise AssertionError("uncertain metadata close reported complete")
    assert owner.quarantined and gate.quarantined and gate.open_files==0
    api.close=real_close
else:
    owner.close();owner.close()
    assert heap.native_report()["owners"]==heap.native_report()["files"]==0
    del owner;gc.collect();assert reference() is None
quarantined=scenario in ("metadata-before","metadata-after","native-close")
if quarantined:
    reject(gate.close,"custody")
    reject(make,"owner population")
    reject(lambda:gate.complete_custody(owner),"custody")
    del owner,gate
    gc.collect()
    assert reference() is not None
    population=heap.native_report()
    assert population["owners"]==1 and population["files"]==int(scenario=="native-close")
    probe=r"""
import sys
from pathlib import Path
from riverhog_client import PrimaryStateBusy
from riverhog_client._prepared_primary_windows_native import WindowsContainer
try:WindowsContainer(Path(sys.argv[1]),initialize=False)
except PrimaryStateBusy:pass
else:raise AssertionError("native quarantine released exclusive metadata custody")
"""
    result=subprocess.run([sys.executable,"-I","-c",probe,str(path)],capture_output=True,text=True,timeout=30)
    assert result.returncode==0,(result.stdout,result.stderr)
assert (path.read_bytes(),Path(str(path)+"-journal").read_bytes())==original
print(json.dumps({"passed":True,"scenario":scenario,"quarantined":quarantined,
    "filename":path.name,"population":heap.native_report()}))
'''
    # The test process owns final release of deliberately quarantined handles.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            str(Path(__file__).resolve().parents[3]),
            str(tmp_path),
            scenario,
        ],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    proof = json.loads(result.stdout)
    assert proof["passed"] is True and proof["scenario"] == scenario
    path = tmp_path / "custody-private" / proof["filename"]
    if proof.get("admission_failed"):
        from riverhog_client._prepared_primary_windows_native import _WindowsAPI

        assert path.stat().st_size == 0
        api = _WindowsAPI()
        handle = api.open(Path(str(path) + ".owner"), owner=True)
        api.close(handle)
        return
    with closing(sqlite3.connect(path)) as reopened:
        assert reopened.execute("SELECT body FROM original").fetchall() == [(b"exact original",)]


def test_native_cold_recovery_uses_reserved_bank_before_first_pager_read(tmp_path):
    import json
    import subprocess
    import sys

    code = r'''
import hashlib,importlib.util,json,sqlite3,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_windows_native import (
    NativeWindowsContainer,windows_container_path)
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module
    spec.loader.exec_module(module);return module
helpers=load("cold_helpers",root/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py")
parent_helpers=load("cold_parent",root/"tests/platform/windows_primary_process.py")
parent=Path(sys.argv[2])/"cold-private";parent_helpers._parent(parent)
path=windows_container_path(parent,b"native cold recovery")
journal=Path(str(path)+"-journal")
heap=_admit_process_budget(64*1024*1024,native_limits=(1,2))
source=native.runtime_identity()["sqlite_source_id"]
def bank(*,cold=False):
    writes=native.WriteQuota(helpers.grant(),helpers.grant())
    if cold:writes.begin_recovery()
    return writes,helpers.reads(native,writes),helpers.callbacks(native,writes)
def connect(gate):
    return sqlite3.connect(path.as_uri()+"?vfs="+gate.name,uri=True,isolation_level=None)
writes,reads,calls=bank()
gate=native.WriteVfs(source,reads,calls,heap)
owner=NativeWindowsContainer(path,initialize=True,gate=gate)
helpers.admit_fixture_retries(gate)
connection=connect(gate)
original=b"exact original"+b"x"*(4096-len(b"exact original"))
expected=[(i,original) for i in range(12)]
try:
    assert connection.execute("PRAGMA journal_mode=PERSIST").fetchone()==("persist",)
    connection.execute("PRAGMA synchronous=EXTRA")
    connection.execute("PRAGMA cache_spill=OFF")
    connection.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,body BLOB NOT NULL)")
    connection.execute("BEGIN IMMEDIATE")
    connection.executemany("INSERT INTO sample VALUES (?,?)",expected)
    connection.commit()
finally:
    connection.close()
owner.check_journal(required=True,recovering=True);owner.close()
original_database=hashlib.sha256(path.read_bytes()).hexdigest()
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
# Only this crash fixture enables spilling to leave an actual hot journal.
crash=r"""
import os,sqlite3,sys
connection=sqlite3.connect(sys.argv[1],isolation_level=None)
assert connection.execute("PRAGMA journal_mode=PERSIST").fetchone()==("persist",)
connection.execute("PRAGMA synchronous=EXTRA")
connection.execute("PRAGMA cache_size=1")
connection.execute("PRAGMA cache_spill=ON")
connection.execute("BEGIN IMMEDIATE")
connection.execute("UPDATE sample SET body=zeroblob(length(body))")
os._exit(73)
"""
result=subprocess.run([sys.executable,"-I","-c",crash,str(path)],
    capture_output=True,text=True,timeout=30)
assert result.returncode==73,(result.stdout,result.stderr)
assert journal.stat().st_size>4096
assert journal.read_bytes()[:8]==bytes.fromhex("d9d505f920a163d7")
assert hashlib.sha256(path.read_bytes()).hexdigest()!=original_database
writes,reads,calls=bank(cold=True)
gate=native.WriteVfs(source,reads,calls,heap)
owner=NativeWindowsContainer(path,initialize=False,gate=gate)
before=writes.report()
try:writes.begin_recovery()
except RuntimeError as exc:assert "before native attachment" in str(exc)
else:raise AssertionError("attached owner accepted a cold-mode transition")
assert writes.report()==before
connection=connect(gate)
try:
    assert connection.execute("PRAGMA journal_mode=PERSIST").fetchone()==("persist",)
    assert connection.execute("SELECT id,body FROM sample ORDER BY id").fetchall()==expected
    assert connection.execute("PRAGMA integrity_check").fetchone()==("ok",)
    owner.check_journal(required=False,recovering=True)
finally:
    connection.close()
assert gate.open_files==0 and gate.bound and not gate.quarantined
after=writes.report()
assert after["recovering"] and after["recovery"]["write_calls"]>0,after
assert after["native_failures"]==after["refusals"]==0 and after["first_refusal"] is None,after
assert not any(after["forward"].values()),after
assert reads.report()["recovery"]["read_calls"]>0
assert not any(reads.report()["forward"].values())
assert calls.report()["recovery"]["callback_slots_charged"]>0
assert not any(calls.report()["forward"].values())
assert calls.work.report()["recovery"]["filesystem_api_attempts_charged"]>0
assert not any(calls.work.report()["forward"].values())
assert hashlib.sha256(path.read_bytes()).hexdigest()==original_database
# Closing every cold connection permits a fresh forward operation while the
# same metadata custodian still holds exclusive ownership of the database.
forward,forward_reads,forward_calls=bank()
gate.arm(forward_reads,forward_calls)
connection=connect(gate)
try:
    assert connection.execute("PRAGMA journal_mode=PERSIST").fetchone()==("persist",)
    connection.execute("PRAGMA synchronous=EXTRA")
    connection.execute("PRAGMA cache_spill=OFF")
    connection.execute("BEGIN IMMEDIATE")
    connection.execute("INSERT INTO sample VALUES (?,?)",(20,b"later progress"))
    connection.commit()
    originals=connection.execute("SELECT id,body FROM sample WHERE id<20 ORDER BY id").fetchall()
    assert originals==expected
    later=connection.execute("SELECT body FROM sample WHERE id=20").fetchone()
    assert later==(b"later progress",)
    owner.check_journal(required=True,recovering=True)
    owner.check_journal(required=True)
finally:
    connection.close()
assert forward.report()["forward"]["write_calls"]>0 and not forward.recovering
owner.close()
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
assert heap.report()["heap_limit_bytes"]==64*1024*1024
print(json.dumps({"passed":True,"hot_journal":True,"original_exact":True,
    "cold_forward_untouched":True,"recovery_without_failure":True,
    "quiescent_forward_rearm":True,"population_released":True}))
'''
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(Path(__file__).resolve().parents[3]), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {
        "passed": True,
        "hot_journal": True,
        "original_exact": True,
        "cold_forward_untouched": True,
        "recovery_without_failure": True,
        "quiescent_forward_rearm": True,
        "population_released": True,
    }


@pytest.mark.parametrize("scenario", ["healthy", "capacity", "native"])
def test_native_recorded_operation_failure_is_checked_before_returning_sql_results(
    tmp_path, scenario
):
    import json
    import subprocess
    import sys

    code = r"""
import ctypes as c,hashlib,importlib.util,json,sqlite3,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from riverhog_client import PrimaryStateCapacity,PrimaryStateError
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits,TransactionalPrimaryFiles
from riverhog_client._prepared_primary_windows_native import (
    NativeWindowsContainer,windows_container_path)
from tests.support.sqlite_vfs_faults import FaultVfs
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module
    spec.loader.exec_module(module);return module
helpers=load("failure_helpers",root/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py")
parent_helpers=load("failure_parent",root/"tests/platform/windows_primary_process.py")
parent=Path(sys.argv[2])/"failure-private";parent_helpers._parent(parent)
path=windows_container_path(parent,b"native operation failure")
heap=_admit_process_budget(64*1024*1024,native_limits=(1,2))
source=native.runtime_identity()["sqlite_source_id"]
writes=native.WriteQuota(helpers.grant(),helpers.grant())
gate=native.WriteVfs(source,helpers.reads(native,writes),helpers.callbacks(native,writes),heap)
guard=NativeWindowsContainer(path,initialize=True,gate=gate)
helpers.admit_fixture_retries(gate)
files=TransactionalPrimaryFiles(guard,allocation_descriptor=b"native operation failure",
    limits=SqlitePrimaryLimits(1024*1024,1024*1024,128*1024),initialize=True)
files.put_bytes("original",(b"exact original",),byte_limit=14)
original=hashlib.sha256(path.read_bytes()).hexdigest()
scenario=sys.argv[3]
writes=native.WriteQuota(helpers.grant(),helpers.grant())
held_files=gate.open_files
assert held_files>0
calls=helpers.callbacks(native,writes,forward=held_files if scenario=="capacity" else 262144)
gate.arm(helpers.reads(native,writes),calls)
assert calls.report()["forward"]["callback_slots_charged"]==held_files
assert gate.operation_failure() is None
vfs=FaultVfs.windows().library.sqlite3_vfs_find(gate.name.encode())
touched=[];results=[]
def trace(query):
    if query=="SELECT 7":
        touched.append(True)
        if scenario=="capacity":
            output=c.create_string_buffer(b"canary")
            result=vfs.contents.randomness(vfs,6,output)
            results.append((result,output.value,calls.report()["pending_error"]))
        elif scenario=="native":
            writes.failed()
files._connection.set_trace_callback(trace)
try:
    if scenario=="healthy":
        assert files._one("SELECT 7")== (7,)
        assert gate.operation_failure() is None
        guard.check_operation()
    else:
        error=PrimaryStateCapacity if scenario=="capacity" else PrimaryStateError
        try:files._one("SELECT 7")
        except error as exc:first=exc
        else:raise AssertionError("SQLite result escaped a recorded native operation failure")
        assert gate.operation_failure()==scenario
        try:guard.check_operation()
        except error as exc:assert exc is first
        else:raise AssertionError("recorded operation failure was forgotten")
        assert gate.bound and gate.open_files==held_files and not gate.quarantined
        assert not guard._closed and heap.native_report()["owners"]==1
        if scenario=="capacity":
            assert results==[(0,b"canary",True)],results
            name=c.create_string_buffer(str(path).encode())
            out=c.c_int(42)
            assert vfs.contents.access(vfs,name,0,c.byref(out))==sqlite3.SQLITE_FULL
            assert out.value==42 and not calls.report()["pending_error"]
            assert gate.operation_failure()=="capacity"
            try:guard.check_operation()
            except PrimaryStateCapacity as exc:assert exc is first
            else:raise AssertionError("acknowledged infallible refusal erased the first cause")
    assert touched==[True]
finally:
    files._connection.set_trace_callback(None)
    files.close()
assert hashlib.sha256(path.read_bytes()).hexdigest()==original
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
print(json.dumps({"passed":True,"scenario":scenario,"result_withheld":scenario!="healthy",
    "original_exact":True,"population_released":True}))
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
        timeout=90,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {
        "passed": True,
        "scenario": scenario,
        "result_withheld": scenario != "healthy",
        "original_exact": True,
        "population_released": True,
    }


@pytest.mark.parametrize(
    "scenario",
    [
        "observed",
        "busy",
        "read-refusal",
        "metadata-refusal",
        "invalid-header",
        "short-header",
        "fixed-control",
    ],
)
def test_native_geometry_and_fixed_control_envelope_preserve_ownership(
    tmp_path, scenario, record_testsuite_property
):
    import json
    import subprocess
    import sys

    code = r"""
import hashlib,importlib.util,json,os,sqlite3,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from riverhog_client import PrimaryStateBusy,PrimaryStateCapacity,PrimaryStateError
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits,TransactionalPrimaryFiles
from riverhog_client._prepared_primary_windows_native import (
    NativeWindowsContainer,windows_container_path)
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module
    spec.loader.exec_module(module);return module
helpers=load("geometry_helpers",root/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py")
parent_helpers=load("geometry_parent",root/"tests/platform/windows_primary_process.py")
parent=Path(sys.argv[2])/"geometry-private";parent_helpers._parent(parent)
path=windows_container_path(parent,b"native geometry")
heap=_admit_process_budget(64*1024*1024,native_limits=(1,2))
source=native.runtime_identity()["sqlite_source_id"]
writes=native.WriteQuota(helpers.grant(),helpers.grant())
initial_reads=helpers.reads(native,writes)
initial_calls=helpers.callbacks(native,writes)
gate=native.WriteVfs(source,initial_reads,initial_calls,heap)
guard=NativeWindowsContainer(path,initialize=True,gate=gate)
helpers.admit_fixture_retries(gate)
files=TransactionalPrimaryFiles(guard,allocation_descriptor=b"native geometry",
    limits=SqlitePrimaryLimits(1024*1024,1024*1024,128*1024),initialize=True)
selected=files.put_bytes("original",(b"exact original",),byte_limit=14)
scenario=sys.argv[3]
if scenario=="fixed-control":files.prepare_controls(("slot",),byte_limit=8192)
original_image=path.read_bytes()
original=hashlib.sha256(original_image).hexdigest()
header=original_image[:24]
geometry=None;proof={}
try:
    if scenario=="busy":
        files._connection.execute("BEGIN IMMEDIATE").close()
        before=(writes.report(),initial_reads.report(),initial_calls.report(),initial_calls.work.report())
        try:guard.control_envelope((9,))
        except PrimaryStateBusy:pass
        else:raise AssertionError("active writer allowed a geometry observation")
        after=(writes.report(),initial_reads.report(),initial_calls.report(),initial_calls.work.report())
        assert after==before
        files._connection.rollback()
        geometry=gate.geometry()
    elif scenario in ("read-refusal","metadata-refusal"):
        writes=native.WriteQuota(helpers.grant(),helpers.grant())
        reads=native.ReadQuota(writes,(23,65536) if scenario=="read-refusal" else
            (8*1024*1024,65536),(8*1024*1024,65536))
        cost=native.filesystem_cost("read",(2,3),24)
        work=native.NativeWorkQuota(writes,cost if scenario=="metadata-refusal" else
            (1<<40,1<<60),(1<<40,1<<60))
        calls=native.CallbackQuota(writes,(262144,0),(262144,0),work=work,
            buffers=(1<<40,1<<40))
        gate.arm(reads,calls)
        try:guard.control_envelope((9,))
        except PrimaryStateCapacity:pass
        else:raise AssertionError("geometry escaped an insufficient read/metadata grant")
        assert gate.operation_failure()=="capacity" and writes.report()["native_failures"]==0
        assert reads.report()["forward"]["read_bytes"]==(0 if scenario=="read-refusal" else 24)
        assert not any(reads.report()["recovery"].values())
        assert not any(work.report()["recovery"].values())
        if scenario=="metadata-refusal":
            assert work.report()["forward"]["filesystem_api_attempts_charged"]==cost[0]
    elif scenario in ("invalid-header","short-header"):
        with path.open("r+b") as damaged:
            if scenario=="short-header":damaged.truncate(8)
            else:damaged.write(b"X")
            damaged.flush();os.fsync(damaged.fileno())
        try:guard.control_envelope((9,))
        except PrimaryStateError:pass
        else:raise AssertionError("invalid durable header supplied qualified geometry")
        assert gate.operation_failure()=="native"
    else:
        before=writes.report()
        geometry=gate.geometry()
        assert geometry["page_bytes"]==4096 and geometry["reserved_bytes"]==header[20]
        assert writes.report()==before
        assert all(geometry[k]>0 and not(geometry[k]&(geometry[k]-1)) for k in (
            "sector_bytes","logical_sector_bytes","atomic_sector_bytes",
            "filesystem_atomic_sector_bytes"))
        envelope=guard.control_envelope((8192+9,16))
        sector=max(geometry[k] for k in (
            "sector_bytes","logical_sector_bytes","atomic_sector_bytes",
            "filesystem_atomic_sector_bytes"))
        peers=max(1,(sector+4095)//4096)
        payload=4096-geometry["reserved_bytes"]-4
        pages=peers*(1+2+(8192+9+payload-1)//payload+2+(16+payload-1)//payload)
        assert envelope.database_pages==pages
        assert envelope.journal_bytes==2*sector+pages*(4096+8)+16
        if scenario=="fixed-control":
            for deleting,length in ((False,8192+9),(True,9)):
                bound=guard.control_envelope((length,))
                forward=(4096,bound.database_pages,1024*1024,bound.journal_bytes,
                    8*1024*1024,65536)
                writes=native.WriteQuota(forward,helpers.grant())
                calls=helpers.callbacks(native,writes)
                gate.arm(helpers.reads(native,writes),calls)
                if deleting:files.remove_control("slot",expected=b"c"*8192)
                else:files.put_control("slot",b"c"*8192,byte_limit=8192)
                report=writes.report()
                assert gate.operation_failure() is None and not report["recovering"]
                assert 0<report["forward"]["database_pages_attempted"]<=bound.database_pages
                assert report["forward"]["journal_write_extent_bytes"]<=bound.journal_bytes
                proof["remove" if deleting else "put"]={
                    "page_bound":bound.database_pages,"journal_bound":bound.journal_bytes,
                    "pages":report["forward"]["database_pages_attempted"],
                    "journal_extent":report["forward"]["journal_write_extent_bytes"]}
            with files.verified(selected) as stream:assert stream.read()==b"exact original"
        else:
            assert writes.report()==before
finally:
    files.close()
if scenario in ("invalid-header","short-header"):
    with path.open("r+b") as restored:
        restored.write(original_image);restored.flush();os.fsync(restored.fileno())
if scenario!="fixed-control":
    assert hashlib.sha256(path.read_bytes()).hexdigest()==original
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
print(json.dumps({"passed":True,"scenario":scenario,"geometry":geometry,"writes":proof,
    "original_exact":True,"population_released":True}))
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
        timeout=90,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    proof = json.loads(result.stdout)
    assert proof["passed"] is True and proof["scenario"] == scenario
    assert proof["original_exact"] is True and proof["population_released"] is True
    if scenario == "observed":
        record_testsuite_property("prepared_primary_native_geometry", json.dumps(proof["geometry"]))
    if scenario == "fixed-control":
        assert set(proof["writes"]) == {"put", "remove"}
        record_testsuite_property("prepared_primary_control_envelopes", json.dumps(proof["writes"]))


def test_native_resource_grants_survive_recovery_and_explicit_increase(tmp_path):
    import json
    import subprocess
    import sys

    code = r"""
import hashlib,importlib.util,json,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from riverhog_client import PrimaryStateCapacity
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits,TransactionalPrimaryFiles
from riverhog_client._prepared_primary_windows_native import (
    NativeWindowsContainer,windows_container_path)
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    m=importlib.util.module_from_spec(spec);sys.modules[name]=m;spec.loader.exec_module(m);return m
helpers=load("resource_helpers",root/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py")
parents=load("resource_parents",root/"tests/platform/windows_primary_process.py")
parent=Path(sys.argv[2])/"resources-private";parents._parent(parent)
path=windows_container_path(parent,b"native retained resources")
heap=_admit_process_budget(64*1024*1024,native_limits=(1,2))
source=native.runtime_identity()["sqlite_source_id"]
small=SqlitePrimaryLimits(256*1024,256*1024,128*1024)
large=SqlitePrimaryLimits(512*1024,512*1024,128*1024)
def opened(initialize,limits):
    writes=native.WriteQuota(helpers.grant(),helpers.grant())
    if not initialize:writes.begin_recovery()
    gate=native.WriteVfs(source,helpers.reads(native,writes),helpers.callbacks(native,writes),heap)
    guard=NativeWindowsContainer(path,initialize=initialize,gate=gate)
    helpers.admit_fixture_retries(gate)
    return TransactionalPrimaryFiles(guard,allocation_descriptor=b"native retained resources",
        limits=limits,initialize=initialize),gate,writes
files,gate,writes=opened(True,small)
with files:
    selected=files.put_bytes("original",(b"exact original",),byte_limit=14)
    first=files.resource_grants()
    child=files.child("child",initialize=True)
    changed=files.increase_resource_grants(database_bytes=large.database_bytes,journal_bytes=large.journal_bytes)
    assert changed.revision==1 and changed.identity==first.identity
    assert child.resource_grants()==changed and child._limits is files._limits
    assert gate.operation_failure() is None and not writes.recovering
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
files,gate,writes=opened(False,large)
with files:
    assert writes.recovering and not writes.report()["refusals"]
    assert not writes.report()["native_failures"]
    assert files.resource_grants()==changed
    with files.verified(selected) as stream:assert stream.read()==b"exact original"
    assert gate.operation_failure() is None
original=hashlib.sha256(path.read_bytes()).hexdigest()
try:opened(False,small)
except PrimaryStateCapacity as exc:assert "original resource grants" in str(exc)
else:raise AssertionError("cold open lowered original ceilings")
assert hashlib.sha256(path.read_bytes()).hexdigest()==original
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
print(json.dumps({"passed":True,"original_exact":True,"cold_recovery_bank":True,
    "explicit_increase":True,"lowered_offer_rejected":True,"population_released":True}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(Path(__file__).resolve().parents[3]), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {
        "passed": True,
        "original_exact": True,
        "cold_recovery_bank": True,
        "explicit_increase": True,
        "lowered_offer_rejected": True,
        "population_released": True,
    }


def test_native_database_reserve_prepares_real_backing_and_survives_cold_recovery(
    tmp_path, record_testsuite_property
):
    import json
    import subprocess
    import sys

    code = r"""
import importlib.util,json,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from riverhog_client import PrimaryStateCapacity
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits,TransactionalPrimaryFiles
from riverhog_client._prepared_primary_windows_native import (
    NativeWindowsContainer,windows_container_path)
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[name]=module
    spec.loader.exec_module(module);return module
helpers=load("reserve_helpers",root/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py")
parents=load("reserve_parents",root/"tests/platform/windows_primary_process.py")
parent=Path(sys.argv[2])/"reserve-private";parents._parent(parent)
path=windows_container_path(parent,b"native database reserve")
heap=_admit_process_budget(64*1024*1024,native_limits=(1,2))
source=native.runtime_identity()["sqlite_source_id"]
small=SqlitePrimaryLimits(512*1024,512*1024,128*1024)
large=SqlitePrimaryLimits(1024*1024,1024*1024,128*1024)
def opened(initialize,limits):
    writes=native.WriteQuota(helpers.grant(),helpers.grant())
    if not initialize:writes.begin_recovery()
    gate=native.WriteVfs(source,helpers.reads(native,writes),helpers.callbacks(native,writes),heap)
    guard=NativeWindowsContainer(path,initialize=initialize,gate=gate)
    helpers.admit_fixture_retries(gate)
    files=TransactionalPrimaryFiles(guard,allocation_descriptor=b"native database reserve",
        limits=limits,initialize=initialize)
    return files,gate,writes
files,gate,writes=opened(True,small)
with files:
    original=files.put_bytes("original",(b"exact original",),byte_limit=14)
    target=files.space_usage().database_pages+8
    plan=dict(free_pages=4,target_pages=target,workspace_bytes=65536)
    for calls in range(128):
        result=files.reserve_database_step(**plan)
        assert gate.operation_failure() is None
        if result.done:break
    else:raise AssertionError("fixture reserve did not finish")
    assert result.reserve.inserted==result.reserve.released>0
    assert files.require_database_reserve()==result.reserve
    files.increase_resource_grants(database_bytes=large.database_bytes,
        journal_bytes=large.journal_bytes)
    assert files.database_reserve().phase=="depleted"
    try:files.require_database_reserve()
    except PrimaryStateCapacity:pass
    else:raise AssertionError("increased grant silently retained prepared backing")
    target+=8;plan["target_pages"]=target
    first=files.reserve_database_step(**plan)
    assert first.reserve.inserted==1 and first.reserve.released==0
    for _ in range(128):
        result=files.reserve_database_step(**plan)
        if result.done:break
    else:raise AssertionError("fixture replenishment did not finish")
    assert result.done and result.reserve.grant_revision==1
    assert not writes.recovering and gate.operation_failure() is None
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
files,gate,writes=opened(False,large)
with files:
    reserve=files.require_database_reserve()
    assert reserve.phase=="prepared" and reserve.grant_revision==1
    assert reserve.inserted==reserve.released
    assert writes.recovering and not writes.report()["refusals"]
    assert not writes.report()["native_failures"] and gate.operation_failure() is None
    with files.verified(original) as stream:assert stream.read()==b"exact original"
    space=files.space_usage()
    assert space.reusable_pages>=4 and space.database_pages==reserve.backed_pages>=target
    assert space.database.allocated_bytes>=space.database.end_bytes==4096*reserve.backed_pages
    proof=dict(target_pages=target,backed_pages=reserve.backed_pages,
        free_pages=space.reusable_pages,free_floor=reserve.free_pages,
        end_bytes=space.database.end_bytes,allocated_bytes=space.database.allocated_bytes,
        inserted=reserve.inserted,released=reserve.released,grant_revision=reserve.grant_revision)
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
print(json.dumps({"passed":True,"original_exact":True,"population_released":True,
    "cold_recovery_bank":True,"database_reserve":proof}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(Path(__file__).resolve().parents[3]), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    proof = json.loads(result.stdout)
    assert proof["passed"] is True and proof["original_exact"] is True
    assert proof["population_released"] is True and proof["cold_recovery_bank"] is True
    record_testsuite_property(
        "prepared_primary_database_reserve", json.dumps(proof["database_reserve"])
    )


def test_native_progress_journal_derives_minimum_and_retains_real_backing_on_cold_open(
    tmp_path, record_testsuite_property
):
    import json
    import subprocess
    import sys

    code = r"""
import importlib.util,json,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from riverhog_client import PrimaryStateCapacity
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits,TransactionalPrimaryFiles
from riverhog_client._prepared_primary_windows_native import (
    NativeWindowsContainer,windows_container_path)
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[name]=module
    spec.loader.exec_module(module);return module
helpers=load("journal_helpers",root/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py")
parents=load("journal_parents",root/"tests/platform/windows_primary_process.py")
parent=Path(sys.argv[2])/"journal-private";parents._parent(parent)
path=windows_container_path(parent,b"native progress journal")
heap=_admit_process_budget(64*1024*1024,native_limits=(1,2))
source=native.runtime_identity()["sqlite_source_id"]
small=SqlitePrimaryLimits(512*1024,512*1024,128*1024)
large=SqlitePrimaryLimits(1024*1024,1024*1024,128*1024)
plan=dict(prefix_lengths=(8201,16),workspace_bytes=65536)
def opened(initialize,limits):
    writes=native.WriteQuota(helpers.grant(),helpers.grant())
    if not initialize:writes.begin_recovery()
    gate=native.WriteVfs(source,helpers.reads(native,writes),helpers.callbacks(native,writes),heap)
    guard=NativeWindowsContainer(path,initialize=initialize,gate=gate)
    helpers.admit_fixture_retries(gate)
    files=TransactionalPrimaryFiles(guard,allocation_descriptor=b"native progress journal",
        limits=limits,initialize=initialize)
    return files,guard,gate,writes
def finish(files,gate):
    for _ in range(32):
        result=files.reserve_journal_step(**plan)
        assert gate.operation_failure() is None
        if result.done:return result.reserve
    raise AssertionError("fixture journal did not finish its explicit workspace")
files,guard,gate,writes=opened(True,small)
with files:
    original=files.put_bytes("original",(b"exact original",),byte_limit=14)
    reserve=finish(files,gate)
    envelope=guard.control_envelope((8201,16,64))
    assert reserve.minimum_bytes==envelope.journal_bytes
    assert reserve.minimum_pages==envelope.database_pages
    assert reserve.backed_bytes>=reserve.minimum_bytes
    assert files.require_progress_journal()==reserve
    # The independent progress pad must survive control initialization's policy.
    files.prepare_controls(("receipt",),byte_limit=8192)
    files.put_control("receipt",b"exact receipt",byte_limit=8192)
    assert files.require_progress_journal()==reserve
    files.increase_resource_grants(database_bytes=large.database_bytes,
        journal_bytes=large.journal_bytes)
    assert files.progress_journal().phase=="depleted"
    try:files.require_progress_journal()
    except PrimaryStateCapacity:pass
    else:raise AssertionError("resource increase silently retained journal preparation")
    reserve=finish(files,gate)
    assert reserve.grant_revision==1
    assert not writes.recovering and gate.operation_failure() is None
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
files,guard,gate,writes=opened(False,large)
with files:
    reserve=files.require_progress_journal()
    assert reserve.phase=="prepared" and reserve.grant_revision==1
    assert writes.recovering and not writes.report()["refusals"]
    assert not writes.report()["native_failures"] and gate.operation_failure() is None
    with files.verified(original) as stream:assert stream.read()==b"exact original"
    assert files.read_control("receipt",byte_limit=8192)==b"exact receipt"
    space=files.space_usage()
    assert space.journal.end_bytes>=reserve.backed_bytes>=reserve.minimum_bytes
    assert space.journal.allocated_bytes>=reserve.backed_bytes
    assert files.journal_reserved_bytes()>=reserve.backed_bytes
    proof=dict(minimum_bytes=reserve.minimum_bytes,minimum_pages=reserve.minimum_pages,
        backed_bytes=reserve.backed_bytes,end_bytes=space.journal.end_bytes,
        allocated_bytes=space.journal.allocated_bytes,workspace_bytes=reserve.workspace_bytes,
        pad_rows=reserve.pad_rows,grant_revision=reserve.grant_revision)
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
print(json.dumps({"passed":True,"original_exact":True,"population_released":True,
    "cold_recovery_bank":True,"progress_journal":proof}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(Path(__file__).resolve().parents[3]), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    proof = json.loads(result.stdout)
    assert proof["passed"] is True and proof["original_exact"] is True
    assert proof["population_released"] is True and proof["cold_recovery_bank"] is True
    record_testsuite_property(
        "prepared_primary_progress_journal", json.dumps(proof["progress_journal"])
    )


def test_native_physical_ready_protects_payload_and_rearms_after_cold_recovery(
    tmp_path, record_testsuite_property
):
    import json
    import subprocess
    import sys

    code = r"""
import importlib.util,json,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from riverhog_client import PrimaryStateCapacity
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_sqlite import (
    SqlitePrimaryLimits,TransactionalPrimaryFiles,_connect)
from riverhog_client._prepared_primary_windows_native import (
    NativeWindowsContainer,windows_container_path)
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[name]=module
    spec.loader.exec_module(module);return module
helpers=load("ready_helpers",root/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py")
parents=load("ready_parents",root/"tests/platform/windows_primary_process.py")
parent=Path(sys.argv[2])/"ready-private";parents._parent(parent)
path=windows_container_path(parent,b"native physical ready")
heap=_admit_process_budget(64*1024*1024,native_limits=(1,2))
source=native.runtime_identity()["sqlite_source_id"]
small=SqlitePrimaryLimits(512*1024,512*1024,128*1024)
large=SqlitePrimaryLimits(1024*1024,1024*1024,128*1024)
journal_plan=dict(prefix_lengths=(8201,16),workspace_bytes=65536)
def bank():
    writes=native.WriteQuota(helpers.grant(),helpers.grant())
    return writes,helpers.reads(native,writes),helpers.callbacks(native,writes)
def opened(initialize,limits):
    writes,reads,calls=bank()
    if not initialize:writes.begin_recovery()
    gate=native.WriteVfs(source,reads,calls,heap)
    guard=NativeWindowsContainer(path,initialize=initialize,gate=gate)
    helpers.admit_fixture_retries(gate)
    files=TransactionalPrimaryFiles(guard,allocation_descriptor=b"native physical ready",
        limits=limits,initialize=initialize)
    return files,guard,gate,writes
def finish_journal(files,gate):
    for _ in range(32):
        result=files.reserve_journal_step(**journal_plan)
        assert gate.operation_failure() is None
        if result.done:return
    raise AssertionError("fixture journal did not finish")
def finish_database(files,gate,plan):
    for _ in range(256):
        result=files.reserve_database_step(**plan)
        assert gate.operation_failure() is None
        if result.done:return
    raise AssertionError("fixture database headroom did not finish")
files,guard,gate,writes=opened(True,small)
with files:
    original=files.put_bytes("original",(b"exact original",),byte_limit=14)
    files.prepare_controls(("receipt",),byte_limit=8192)
    files.put_control("receipt",b"exact receipt",byte_limit=8192)
    finish_journal(files,gate)
    plan=dict(free_pages=8,target_pages=files.space_usage().database_pages+32,
        workspace_bytes=128*1024)
    finish_database(files,gate,plan)
    ready=files.prepare_physical_ready()
    assert ready.minimum_journal_bytes>=guard.control_envelope((64,72,64,64,184)).journal_bytes
    with files._transaction(payload=True):
        assert files._one("PRAGMA max_page_count")== (ready.backed_pages,)
    payload=files.put_bytes("payload",(b"payload"*1000,),byte_limit=7000)
    assert files.require_physical_ready()==ready
    files.increase_resource_grants(database_bytes=large.database_bytes,
        journal_bytes=large.journal_bytes)
    assert files.physical_readiness().phase=="depleted"
    try:files.require_physical_ready()
    except PrimaryStateCapacity:pass
    else:raise AssertionError("resource increase silently retained Ready")
    finish_journal(files,gate);finish_database(files,gate,plan)
    assert files.prepare_physical_ready().grant_revision==1
    assert not writes.recovering and gate.operation_failure() is None
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
files,guard,gate,recovery=opened(False,large)
with files:
    ready=files.require_physical_ready()
    assert recovery.recovering and recovery.report()["forward"]["write_calls"]==0
    assert not recovery.report()["refusals"] and not recovery.report()["native_failures"]
    # Close the recovered pager before selecting a fresh F bank under the same custodian.
    files._connection.close();files._connection=None
    assert gate.open_files==0
    forward,reads,calls=bank();gate.arm(reads,calls)
    files._connection=_connect(guard,files._limits)
    assert not forward.recovering and files.require_physical_ready()==ready
    with files.verified(original) as stream:assert stream.read()==b"exact original"
    with files.verified(payload) as stream:assert stream.read()==b"payload"*1000
    assert files.read_control("receipt",byte_limit=8192)==b"exact receipt"
    files.put_bytes("after-cold",(b"confirmed cold payload",),byte_limit=22)
    assert files.require_physical_ready()==ready
    assert forward.report()["forward"]["write_calls"]>0
    assert not forward.report()["refusals"] and not forward.report()["native_failures"]
    assert gate.operation_failure() is None
    space=files.space_usage()
    assert space.database_pages==ready.backed_pages
    assert space.reusable_pages>=ready.free_pages
    assert space.database.allocated_bytes>=space.database.end_bytes==4096*ready.backed_pages
    assert space.journal.end_bytes>=ready.journal_bytes>=ready.minimum_journal_bytes
    assert space.journal.allocated_bytes>=ready.journal_bytes
    proof=dict(backed_pages=ready.backed_pages,free_floor=ready.free_pages,
        free_pages=space.reusable_pages,database_end_bytes=space.database.end_bytes,
        database_allocated_bytes=space.database.allocated_bytes,
        journal_backed_bytes=ready.journal_bytes,journal_end_bytes=space.journal.end_bytes,
        journal_allocated_bytes=space.journal.allocated_bytes,
        minimum_journal_bytes=ready.minimum_journal_bytes,minimum_pages=ready.minimum_pages,
        grant_revision=ready.grant_revision)
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
print(json.dumps({"passed":True,"original_exact":True,"payload_exact":True,
    "population_released":True,"cold_recovery_rearmed":True,"physical_ready":proof}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(Path(__file__).resolve().parents[3]), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    proof = json.loads(result.stdout)
    assert all(
        proof[key] is True
        for key in (
            "passed",
            "original_exact",
            "payload_exact",
            "population_released",
            "cold_recovery_rearmed",
        )
    )
    record_testsuite_property(
        "prepared_primary_physical_ready", json.dumps(proof["physical_ready"])
    )


def test_native_retained_operation_grants_preserve_cold_authority_and_recovery_caps(
    tmp_path, record_testsuite_property
):
    import json
    import subprocess
    import sys

    code = r"""
import importlib.util,json,hashlib,sys
from pathlib import Path
from dataclasses import replace
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from riverhog_client import PrimaryStateCapacity
from riverhog_client import _prepared_primary_native as native
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits,TransactionalPrimaryFiles
from riverhog_client._prepared_primary_sqlite_operations import (
    SqliteOperationBank,SqliteOperationGrants,native_bank)
from riverhog_client._prepared_primary_windows_native import (
    NativeWindowsContainer,windows_container_path)
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[name]=module
    spec.loader.exec_module(module);return module
helpers=load("operation_helpers",root/"packages/riverhog-client/tests/test_prepared_primary_native_vfs.py")
parents=load("operation_parents",root/"tests/platform/windows_primary_process.py")
parent=Path(sys.argv[2])/"operations-private";parents._parent(parent)
allocation=b"native original operation grants"
path=windows_container_path(parent,allocation)
heap=_admit_process_budget(64*1024*1024,native_limits=(1,2))
source=native.runtime_identity()["sqlite_source_id"]
limits=SqlitePrimaryLimits(512*1024,512*1024,128*1024)
bank=SqliteOperationBank(128,8*1024*1024,65536,8*1024*1024,65536,
    262144,0,1<<40,1<<60,1<<40)
grants=SqliteOperationGrants(bank,bank)
larger=SqliteOperationGrants(replace(bank,write_bytes=16*1024*1024),
    replace(bank,read_bytes=16*1024*1024))
# Exercise actual C admission counters, separately from file-backed SQL below.
caps=native_bank(grants,limits,database_pages=4,journal_bytes=8192)
assert not caps.writes.admit("journal",1,8192)
assert caps.writes.recovering
assert caps.writes.report()["first_refusal"]["bank"]=="forward"
assert caps.writes.admit("journal",1,limits.journal_bytes-1)
assert caps.writes.admit("database",4096,limits.database_bytes-4096)
assert caps.writes.report()["forward"]["write_calls"]==0
assert caps.writes.report()["recovery"]["write_calls"]==2
def opened(initialize,offered):
    counters=native_bank(offered,limits,recovering=not initialize)
    gate=native.WriteVfs(source,counters.reads,counters.callbacks,heap)
    guard=NativeWindowsContainer(path,initialize=initialize,gate=gate)
    helpers.admit_fixture_retries(gate)
    files=TransactionalPrimaryFiles(guard,allocation_descriptor=allocation,
        limits=limits,initialize=initialize,operations=offered)
    return files,gate,counters
files,gate,counters=opened(True,grants)
with files:
    original=files.put_bytes("original",(b"exact original",),byte_limit=14)
    assert files.operation_grants().grants==grants
    files.increase_resource_grants(database_bytes=limits.database_bytes,
        journal_bytes=limits.journal_bytes,operations=larger)
    assert files.operation_grants().grants==larger
    assert files.operation_grants().revision==1
    assert gate.operation_failure() is None
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
before=hashlib.sha256(path.read_bytes()).digest()
lower=replace(larger,recovery=replace(larger.recovery,write_bytes=larger.recovery.write_bytes-1))
try:opened(False,lower)
except PrimaryStateCapacity as exc:assert "original operation grants" in str(exc)
else:raise AssertionError("lowered original restoration grant was accepted")
assert hashlib.sha256(path.read_bytes()).digest()==before
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
offered=replace(larger,forward=replace(larger.forward,write_calls=larger.forward.write_calls+16))
files,gate,counters=opened(False,offered)
with files:
    retained=files.operation_grants()
    assert retained.grants==larger and retained.revision==1
    assert len(retained.encode())==184
    assert counters.writes.recovering
    assert counters.writes.report()["forward"]["write_calls"]==0
    assert counters.writes.report()["recovery"]["write_calls"]>0
    assert not counters.writes.report()["refusals"]
    assert not counters.writes.report()["native_failures"] and gate.operation_failure() is None
    with files.verified(original) as stream:assert stream.read()==b"exact original"
    proof=dict(record_bytes=184,revision=retained.revision,
        forward_write_bytes=retained.grants.forward.write_bytes,
        recovery_read_bytes=retained.grants.recovery.read_bytes,
        cold_forward_write_calls=counters.writes.report()["forward"]["write_calls"],
        cold_recovery_write_calls=counters.writes.report()["recovery"]["write_calls"],
        original_database_bytes=limits.database_bytes,original_journal_bytes=limits.journal_bytes,
        forward_database_bytes=4*4096,forward_journal_bytes=8192,
        counter_forward_write_calls=caps.writes.report()["forward"]["write_calls"],
        counter_recovery_write_calls=caps.writes.report()["recovery"]["write_calls"])
assert heap.native_report()["owners"]==heap.native_report()["files"]==0
print(json.dumps({"passed":True,"original_exact":True,"population_released":True,
    "lower_offer_preserved":True,"counter_caps_verified":True,"operation_grants":proof}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(Path(__file__).resolve().parents[3]), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    proof = json.loads(result.stdout)
    assert all(
        proof[key] is True
        for key in (
            "passed",
            "original_exact",
            "population_released",
            "lower_offer_preserved",
            "counter_caps_verified",
        )
    )
    record_testsuite_property(
        "prepared_primary_operation_grants", json.dumps(proof["operation_grants"])
    )
