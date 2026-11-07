"""Application-owned admission for actual ordinary SQLite connection lifetimes."""

from __future__ import annotations

import sqlite3
import weakref
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from threading import RLock
from types import TracebackType
from typing import Any, Literal, Self, overload

from sqlite_admission.settings import (
    SqliteConfigurationError,
    SqliteConnectionSettings,
    _configure,
)


def configure_sqlite_connection(
    connection: sqlite3.Connection,
    *,
    defaults: SqliteConnectionSettings,
    read_rows: Callable[[str, int], list[Any]] | None = None,
) -> None:
    """Apply the admitted application's selected profile after early retention.

    Owners call this on a fresh connection before schema/input work. Their
    defaults apply when its factory selected no profile. Settings on a shared
    admission are immutable; closing/reopening does not change that selection.
    A supplied read_rows executes the exact statement with its bounded row
    count through existing owner cursor retention. Failed cleanup must leave
    that cursor retained for explicit retry. This does not install a process
    heap ceiling or admit filesystem scratch.
    """
    if not isinstance(defaults, SqliteConnectionSettings):
        raise TypeError("SQLite owner defaults require an explicit profile")
    selected = defaults
    if isinstance(connection, _Connection):
        offered = connection._lease.admission._connection_settings
        if offered is not None:
            selected = offered
    _configure(connection, selected, read_rows)


class SqliteCapacity(RuntimeError):
    """An explicit active resource grant cannot admit more work."""


class SqliteCleanupPending(RuntimeError):
    """Storage is closing but an actual operation or cleanup is not settled."""


@dataclass(frozen=True, slots=True)
class SqliteAdmissionStatus:
    connection_limit: int
    cursor_limit: int
    connections: int
    connection_peak: int
    cursors: int
    cursor_peak: int
    unreturned_connections: int
    closing_connections: int
    blob_limit: int
    blobs: int
    blob_peak: int
    unreturned_blobs: int
    operation_limit: int
    operations: int
    operation_peak: int


@dataclass(eq=False)
class _ConnectionLease:
    admission: SqliteConnectionAdmission
    connection: _Connection | None = None
    returned: bool = False


@dataclass(eq=False)
class _CursorLease:
    owner: _Connection
    reference: weakref.ReferenceType[_Cursor] | None = None
    retained: _Cursor | None = None
    unconfirmed: bool = False
    quarantined: bool = False

    def actual(self) -> _Cursor | None:
        if self.retained is not None:
            return self.retained
        return None if self.reference is None else self.reference()


@dataclass(eq=False)
class _BlobLease:
    owner: _Connection
    reference: weakref.ReferenceType[_Blob] | None = None
    retained: _Blob | None = None
    returned: bool = False
    unconfirmed: bool = False
    quarantined: bool = False

    def actual(self) -> _Blob | None:
        if self.retained is not None:
            return self.retained
        return None if self.reference is None else self.reference()


class SqliteConnectionAdmission:
    """Bound actual SQLite work and retained handles after process admission.

    verify_process must verify the application's already installed process-wide
    engine/heap policy without opening a database. The application owns that
    policy, per-operation native budgets, priorities and per-owner cache/temp settings.
    A finite shared operation grant covers actual work calls, including opens,
    cursor/blob work and transactions. One backup call retains both connections
    under one operation grant. Physical close uses the already retained finite
    handle grants, so independent cleanup can proceed while work is full.
    This counter supplies neither heap enforcement nor scheduling fairness.
    Blob handles have their own finite active grant; zero explicitly disables
    blob acquisition until the application supplies a larger grant.

    Admission precedes the driver call. Unreturned construction stays charged,
    even when no closeable Python handle was returned. Actual handles remain
    reachable through pending_connections. Their directories and persistent
    files retain their existing owner; this factory never removes them.
    """

    def __init__(
        self,
        *,
        connection_limit: int,
        cursor_limit: int,
        operation_limit: int,
        verify_process: Callable[[], object],
        blob_limit: int = 0,
        connection_settings: SqliteConnectionSettings | None = None,
    ) -> None:
        if any(type(value) is not int or value < 1 for value in (connection_limit, cursor_limit)):
            raise ValueError("SQLite connection and cursor grants must be positive integers")
        if type(blob_limit) is not int or blob_limit < 0:
            raise ValueError("SQLite blob grant must be a nonnegative integer")
        if not callable(verify_process):
            raise TypeError("SQLite process verification is required")
        if type(operation_limit) is not int or operation_limit < 1:
            raise ValueError("SQLite operation grant must be a positive integer")
        if connection_settings is not None and not isinstance(
            connection_settings, SqliteConnectionSettings
        ):
            raise TypeError("SQLite connection settings require an explicit profile")
        self._connection_settings = connection_settings
        self._operation_limit = operation_limit
        self._operations = self._operation_peak = 0
        self._connection_limit = connection_limit
        self._cursor_limit = cursor_limit
        self._blob_limit = blob_limit
        self._verify_process = verify_process
        self._lock = RLock()
        self._connections: set[_ConnectionLease] = set()
        self._cursors: set[_CursorLease] = set()
        self._blobs: set[_BlobLease] = set()
        self._connection_peak = self._cursor_peak = self._blob_peak = 0

    @property
    def status(self) -> SqliteAdmissionStatus:
        """Observe retained obligations without engine calls or cleanup retries."""
        with self._lock:
            return SqliteAdmissionStatus(
                self._connection_limit,
                self._cursor_limit,
                len(self._connections),
                self._connection_peak,
                len(self._cursors),
                self._cursor_peak,
                sum(not lease.returned for lease in self._connections),
                sum(
                    lease.connection is not None and lease.connection._closing
                    for lease in self._connections
                ),
                self._blob_limit,
                len(self._blobs),
                self._blob_peak,
                sum(not lease.returned for lease in self._blobs),
                self._operation_limit,
                self._operations,
                self._operation_peak,
            )

    def increase(
        self,
        *,
        connection_limit: int,
        cursor_limit: int,
        blob_limit: int | None = None,
        operation_limit: int | None = None,
    ) -> None:
        """Raise explicit active grants without forgetting any retained obligation."""
        if any(type(value) is not int or value < 1 for value in (connection_limit, cursor_limit)):
            raise ValueError("SQLite connection and cursor grants must be positive integers")
        if blob_limit is not None and (type(blob_limit) is not int or blob_limit < 0):
            raise ValueError("SQLite blob grant must be a nonnegative integer")
        if operation_limit is not None and (
            type(operation_limit) is not int or operation_limit < 1
        ):
            raise ValueError("SQLite operation grant must be a positive integer")
        with self._lock:
            blobs = self._blob_limit if blob_limit is None else blob_limit
            operations = self._operation_limit if operation_limit is None else operation_limit
            if (
                operations < self._operation_limit
                or blobs < self._blob_limit
                or connection_limit < self._connection_limit
                or cursor_limit < self._cursor_limit
                or (connection_limit, cursor_limit, blobs, operations)
                == (
                    self._connection_limit,
                    self._cursor_limit,
                    self._blob_limit,
                    self._operation_limit,
                )
            ):
                raise ValueError("SQLite grants require no decrease and at least one increase")
            self._connection_limit, self._cursor_limit = connection_limit, cursor_limit
            self._blob_limit = blobs
            self._operation_limit = operations

    @property
    def pending_connections(self) -> tuple[sqlite3.Connection, ...]:
        """Actual retained handles, including construction failures, for explicit cleanup."""
        with self._lock:
            return tuple(
                lease.connection for lease in self._connections if lease.connection is not None
            )

    def connect(self, database: Any, **options: Any) -> sqlite3.Connection:
        """Admit the actual open call before process verification or driver work."""
        self._begin_operation()
        try:
            return self._connect(database, **options)
        finally:
            self._end_operation()

    def _begin_operation(self) -> None:
        with self._lock:
            if self._operations >= self._operation_limit:
                raise SqliteCapacity("active SQLite operation grant exhausted")
            self._operations += 1
            self._operation_peak = max(self._operation_peak, self._operations)
            _admissions.add(self)

    def _forget_if_settled(self) -> None:
        if not self._connections and not self._cursors and not self._blobs and not self._operations:
            _admissions.discard(self)

    def _end_operation(self) -> None:
        with self._lock:
            self._operations -= 1
            self._forget_if_settled()

    def _connect(self, database: Any, **options: Any) -> sqlite3.Connection:
        """Use the selected actual driver and preserve owner URI/thread/transaction options."""
        if "factory" in options:
            raise ValueError("SQLite admission owns the connection and cursor factories")
        with self._lock:
            if len(self._connections) >= self._connection_limit:
                raise SqliteCapacity("active SQLite connection grant exhausted")
        self._verify_process()
        with self._lock:
            if len(self._connections) >= self._connection_limit:
                raise SqliteCapacity("active SQLite connection grant exhausted")
            lease = _ConnectionLease(self)
            self._connections.add(lease)
            _admissions.add(self)
            self._connection_peak = max(self._connection_peak, len(self._connections))

        class AdmittedConnection(_Connection):
            _reservation = lease

        connection = _connect(database, factory=AdmittedConnection, **options)
        if not isinstance(connection, _Connection) or connection is not lease.connection:
            raise RuntimeError("SQLite driver returned a different unadmitted connection")
        connection._require_open()
        lease.returned = True
        return connection

    def _reserve_cursor(self, owner: _Connection) -> _CursorLease:
        with self._lock:
            if len(self._cursors) >= self._cursor_limit:
                raise SqliteCapacity("active SQLite cursor grant exhausted")
            lease = _CursorLease(owner)
            self._cursors.add(lease)
            owner._cursors.add(lease)
            self._cursor_peak = max(self._cursor_peak, len(self._cursors))
            return lease

    def _release_cursor(self, lease: _CursorLease) -> None:
        with self._lock:
            lease.owner._cursors.discard(lease)
            self._cursors.discard(lease)
            lease.retained = None

    def _reserve_blob(self, owner: _Connection) -> _BlobLease:
        with self._lock:
            if len(self._blobs) >= self._blob_limit:
                raise SqliteCapacity("active SQLite blob grant exhausted")
            lease = _BlobLease(owner)
            self._blobs.add(lease)
            owner._blobs.add(lease)
            self._blob_peak = max(self._blob_peak, len(self._blobs))
            return lease

    def _release_blob(self, lease: _BlobLease) -> None:
        with self._lock:
            lease.owner._blobs.discard(lease)
            self._blobs.discard(lease)
            lease.retained = None

    def _release_connection(self, lease: _ConnectionLease) -> None:
        with self._lock:
            self._connections.discard(lease)
            self._forget_if_settled()


_connect = sqlite3.connect
# Actual obligations outlive a lost application reference until confirmed cleanup.
_admissions: set[SqliteConnectionAdmission] = set()


class _Connection(sqlite3.Connection):
    _reservation: _ConnectionLease

    def __init__(self, *args: Any, **options: Any) -> None:
        if hasattr(self, "_lease"):
            raise RuntimeError("SQLite admission cannot reinitialize an existing connection")
        self._lease = self._reservation
        self._lease.connection = self
        self._closing = self._cleanup_complete = self._close_active = False
        self._calls = 0
        self._cleanup_notice_delivered = False
        self._lock = RLock()
        self._cursors: set[_CursorLease] = set()
        self._blobs: set[_BlobLease] = set()
        super().__init__(*args, **options)

    @property
    def cleanup_complete(self) -> bool:
        return self._cleanup_complete

    def _require_open(self) -> None:
        if self._closing:
            raise SqliteCleanupPending("SQLite connection is closing")

    def _begin(self, *, operation: bool = True) -> None:
        with self._lock:
            self._require_open()
            if operation:
                self._lease.admission._begin_operation()
            self._calls += 1

    def _end(self, *, operation: bool = True) -> None:
        with self._lock:
            self._calls -= 1
            if operation:
                self._lease.admission._end_operation()

    @overload
    def cursor(self, factory: None = None) -> sqlite3.Cursor: ...

    @overload
    def cursor[C: sqlite3.Cursor](self, factory: Callable[[sqlite3.Connection], C]) -> C: ...

    def cursor(
        self, factory: Callable[[sqlite3.Connection], sqlite3.Cursor] | None = None
    ) -> sqlite3.Cursor:
        if factory is not None:
            raise ValueError("SQLite admission owns the cursor factory")
        self._begin()
        lease = None
        try:
            reserved = self._lease.admission._reserve_cursor(self)
            lease = reserved

            def construct(_connection: sqlite3.Connection) -> _Cursor:
                return _Cursor(self, reserved)

            # The connection factory also retires dead driver cursor references.
            # Reserve and retain our exact owner before its constructor can run.
            cursor = super().cursor(factory=construct)
            cursor.row_factory = self.row_factory
            self._require_open()
            lease.reference = weakref.ref(cursor)
            lease.retained = None
            return cursor
        except BaseException:
            if lease is not None:
                self._closing = True
            raise
        finally:
            self._end()

    def _shortcut(self, method: str, *args: Any) -> sqlite3.Cursor:
        cursor = self.cursor()
        try:
            getattr(cursor, method)(*args)
        except BaseException:
            if not self._closing:
                cursor.close()
            else:
                assert isinstance(cursor, _Cursor)
                cursor._lease.retained = cursor
            raise
        return cursor

    def execute(self, sql: str, parameters: Any = (), /) -> sqlite3.Cursor:
        return self._shortcut("execute", sql, parameters)

    def executemany(self, sql: str, parameters: Iterable[Any], /) -> sqlite3.Cursor:
        return self._shortcut("executemany", sql, parameters)

    def executescript(self, sql: str, /) -> sqlite3.Cursor:
        return self._shortcut("executescript", sql)

    def _transaction(self, method: str) -> None:
        self._begin()
        try:
            getattr(super(), method)()
            self._require_open()
        finally:
            self._end()

    def commit(self) -> None:
        self._transaction("commit")

    def rollback(self) -> None:
        self._transaction("rollback")

    def backup(
        self,
        target: sqlite3.Connection,
        *,
        pages: int = -1,
        progress: Callable[[int, int, int], object] | None = None,
        name: str = "main",
        sleep: float = 0.250,
    ) -> None:
        if (
            not isinstance(target, _Connection)
            or target._lease.admission is not self._lease.admission
        ):
            raise ValueError("SQLite backup requires the same application's admitted target")
        self._begin()
        try:
            target._begin(operation=False)
            try:
                super().backup(target, pages=pages, progress=progress, name=name, sleep=sleep)
                self._require_open()
                target._require_open()
            finally:
                target._end(operation=False)
        finally:
            self._end()

    def serialize(self, *, name: str = "main") -> bytes:
        self._begin()
        try:
            result = super().serialize(name=name)
            self._require_open()
            return result
        finally:
            self._end()

    def deserialize(self, data: Any, /, *, name: str = "main") -> None:
        self._begin()
        try:
            super().deserialize(data, name=name)
            self._require_open()
        finally:
            self._end()

    def _driver_blobopen(self, *args: Any, **options: Any) -> sqlite3.Blob:
        return super().blobopen(*args, **options)

    def blobopen(self, *args: Any, **options: Any) -> Any:
        """Admit a wrapper retaining the actual SQLite blob until confirmed close."""
        self._begin()
        lease = None
        try:
            lease = self._lease.admission._reserve_blob(self)
            blob = _Blob(self, lease)
            blob._actual = self._driver_blobopen(*args, **options)
            self._require_open()
            lease.reference = weakref.ref(blob)
            lease.retained = None
            lease.returned = True
            return blob
        except BaseException:
            if lease is not None:
                self._closing = True
            raise
        finally:
            self._end()

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Literal[False]:
        if not self._closing:
            self._begin()
            try:
                super().__exit__(kind, error, traceback)
                self._require_open()
            finally:
                self._end()
        return False

    def _driver_close(self) -> None:
        super().close()

    def close(self) -> None:
        with self._lock:
            self._closing = True
            if self._cleanup_complete:
                self._lease.admission._release_connection(self._lease)
                return
            if self._calls or self._close_active:
                raise SqliteCleanupPending("SQLite operation is still active")
            # A failed finalizer has no caller to authorize another close attempt.
            resources: tuple[_CursorLease | _BlobLease, ...] = (*self._cursors, *self._blobs)
            if any(lease.quarantined for lease in resources):
                raise SqliteCleanupPending(
                    "SQLite finalizer cleanup is quarantined until process exit"
                )
            if any(lease.unconfirmed for lease in resources) and not self._cleanup_notice_delivered:
                # Notify a containing owner before any explicit retry can occur.
                self._cleanup_notice_delivered = True
                raise SqliteCleanupPending("SQLite cursor or blob close remains unconfirmed")
            self._close_active = True
        try:
            for lease in resources:
                resource = lease.actual()
                if resource is None:
                    raise SqliteCleanupPending(
                        "SQLite cursor or blob construction remains unconfirmed"
                    )
                resource.close()
            with self._lock:
                if self._calls or self._cursors or self._blobs:
                    raise SqliteCleanupPending("SQLite cursor or blob cleanup is still active")
            self._driver_close()
            self._cleanup_complete = True
            self._lease.admission._release_connection(self._lease)
        except BaseException:
            self._cleanup_notice_delivered = True
            raise
        finally:
            self._close_active = False


class _Cursor(sqlite3.Cursor):
    def __init__(self, owner: _Connection, lease: _CursorLease) -> None:
        self._owner, self._lease, self._cleanup_complete = owner, lease, False
        self._close_active = False
        lease.retained = self
        super().__init__(owner)

    def _call(self, method: str, *args: Any) -> Any:
        self._owner._begin()
        try:
            result = getattr(super(), method)(*args)
            self._owner._require_open()
            return result
        finally:
            self._owner._end()

    def execute(self, sql: str, parameters: Any = (), /) -> Self:
        self._call("execute", sql, parameters)
        return self

    def executemany(self, sql: str, parameters: Iterable[Any], /) -> Self:
        def admitted() -> Iterator[Any]:
            for row in parameters:
                self._owner._require_open()
                yield row

        self._call("executemany", sql, admitted())
        return self

    def executescript(self, sql: str, /) -> Self:
        self._call("executescript", sql)
        return self

    def fetchone(self) -> Any:
        return self._call("fetchone")

    def fetchmany(self, size: int | None = None) -> list[Any]:
        result: list[Any] = self._call("fetchmany", *(() if size is None else (size,)))
        return result

    def fetchall(self) -> list[Any]:
        result: list[Any] = self._call("fetchall")
        return result

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Any:
        return self._call("__next__")

    def _driver_close(self) -> None:
        super().close()

    def close(self) -> None:
        with self._owner._lock:
            if self._cleanup_complete:
                self._lease.owner._lease.admission._release_cursor(self._lease)
                return
            if self._lease.quarantined:
                raise SqliteCleanupPending(
                    "SQLite finalizer cleanup is quarantined until process exit"
                )
            if self._close_active:
                raise SqliteCleanupPending("SQLite cursor close is still active")
            self._lease.retained = self
            self._close_active = True
            self._owner._calls += 1
        try:
            self._driver_close()
        except BaseException:
            self._owner._closing = True
            self._lease.unconfirmed = True
            if not self._owner._close_active:
                self._owner._cleanup_notice_delivered = False
            raise
        else:
            self._cleanup_complete = True
            self._lease.owner._lease.admission._release_cursor(self._lease)
        finally:
            self._close_active = False
            self._owner._end(operation=False)

    def __del__(self) -> None:
        # Confirm the actual close before retirement. Failed finalization keeps
        # a strong owner reference and its grants, rather than refunding them
        # from a weak-reference callback before the driver has released storage.
        if not getattr(self, "_cleanup_complete", True):
            try:
                self.close()
            except BaseException:
                self._lease.quarantined = True


class _Blob:
    def __init__(self, owner: _Connection, lease: _BlobLease) -> None:
        self._owner, self._lease = owner, lease
        self._actual: sqlite3.Blob | None = None
        self._cleanup_complete = self._close_active = False
        lease.retained = self

    def _call(self, method: str, *args: Any) -> Any:
        self._owner._begin()
        try:
            if self._actual is None:
                raise SqliteCleanupPending("SQLite blob construction remains unconfirmed")
            result = getattr(self._actual, method)(*args)
            self._owner._require_open()
            return result
        finally:
            self._owner._end()

    def read(self, length: int = -1, /) -> bytes:
        result: bytes = self._call("read", length)
        return result

    def write(self, data: Any, /) -> None:
        self._call("write", data)

    def seek(self, offset: int, origin: int = 0, /) -> None:
        self._call("seek", offset, origin)

    def tell(self) -> int:
        result: int = self._call("tell")
        return result

    def __len__(self) -> int:
        result: int = self._call("__len__")
        return result

    def __getitem__(self, key: int | slice) -> Any:
        return self._call("__getitem__", key)

    def __setitem__(self, key: int | slice, value: Any) -> None:
        self._call("__setitem__", key, value)

    def __enter__(self) -> Self:
        self._call("__enter__")
        return self

    def __exit__(self, *args: Any) -> Literal[False]:
        if not self._lease.unconfirmed and not self._lease.quarantined:
            self.close()
        return False

    def _driver_close(self) -> None:
        if self._actual is None:
            raise SqliteCleanupPending("SQLite blob construction remains unconfirmed")
        self._actual.close()

    def close(self) -> None:
        with self._owner._lock:
            if self._cleanup_complete:
                self._owner._lease.admission._release_blob(self._lease)
                return
            if self._lease.quarantined:
                raise SqliteCleanupPending(
                    "SQLite finalizer cleanup is quarantined until process exit"
                )
            if self._close_active:
                raise SqliteCleanupPending("SQLite blob close is still active")
            self._lease.retained = self
            self._close_active = True
            self._owner._calls += 1
        try:
            self._driver_close()
        except BaseException:
            self._owner._closing = True
            self._lease.unconfirmed = True
            if not self._owner._close_active:
                self._owner._cleanup_notice_delivered = False
            raise
        else:
            self._cleanup_complete = True
            self._owner._lease.admission._release_blob(self._lease)
        finally:
            self._close_active = False
            self._owner._end(operation=False)

    def __del__(self) -> None:
        if not getattr(self, "_cleanup_complete", True):
            try:
                self.close()
            except BaseException:
                self._lease.quarantined = True


__all__ = [
    "SqliteAdmissionStatus",
    "SqliteCapacity",
    "SqliteCleanupPending",
    "SqliteConnectionAdmission",
    "SqliteConfigurationError",
    "SqliteConnectionSettings",
    "configure_sqlite_connection",
]
