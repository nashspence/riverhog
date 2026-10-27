"""Bounded transactional byte selection under one native container owner."""

from __future__ import annotations

import hashlib
import io
import operator
import sqlite3
import threading
import uuid
from collections.abc import Iterable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, Protocol, cast

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import NullPool
from state_schema import EngineFactory

from riverhog_client._prepared_primary_sqlite_schema import _primary_state_schema, _verify_shape
from riverhog_client._prepared_primary_types import (
    PrimaryByteFile,
    PrimaryFileSpace,
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStateError,
    primary_file_name,
)

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_sqlite_space import SqlitePrimarySpace, SqliteVacuumStep

_BLOCK = 4096
_GEOMETRY = (
    "riverhog-prepared-primary-sqlite/v1;page=4096;chunk=4096;controls=fixed/v2;journal=prepared/v1"
)


class ContainerGuard(Protocol):
    path: Path
    vfs: str

    def check_current(self) -> None: ...
    def check_journal(self, *, required: bool, recovering: bool = False) -> None: ...
    def close(self) -> None: ...
    def file_space(self) -> tuple[PrimaryFileSpace, PrimaryFileSpace]: ...


@dataclass(frozen=True, slots=True)
class SqlitePrimaryLimits:
    """Page ceiling, retained-journal setting and SQLite cache tuning.

    Journal retention is distinct from peak transaction space and from the
    calling application's reservations for pending bytes and control progress.
    """

    database_bytes: int
    journal_bytes: int
    cache_bytes: int

    def __post_init__(self) -> None:
        for value in (self.database_bytes, self.journal_bytes, self.cache_bytes):
            if type(value) is not int or value < _BLOCK or value % _BLOCK:
                raise ValueError("SQLite prepared-primary grants must be positive page multiples")


def _connect(guard: ContainerGuard, limits: SqlitePrimaryLimits) -> sqlite3.Connection:
    guard.check_current()
    uri = guard.path.as_uri() + "?mode=rw&vfs=" + guard.vfs
    connection = sqlite3.connect(
        uri,
        uri=True,
        timeout=0,
        isolation_level=None,
        check_same_thread=False,
        cached_statements=32,
    )
    try:
        connection.enable_load_extension(False)
        settings = {
            "busy_timeout": 0,
            # Select retention before a pragma opens the pager in its default delete mode.
            "journal_mode": "PERSIST",
            "page_size": _BLOCK,
            "auto_vacuum": 2,
            "synchronous": 3,
            "foreign_keys": 1,
            "cache_size": -(limits.cache_bytes // 1024),
            "mmap_size": 0,
            "temp_store": 2,
            "cache_spill": 0,
            "trusted_schema": 0,
            "max_page_count": limits.database_bytes // _BLOCK,
            "journal_size_limit": limits.journal_bytes,
        }
        for key, value in settings.items():
            with closing(connection.execute(f"PRAGMA {key}={value}")):
                pass
        for key, value in settings.items():
            with closing(connection.execute(f"PRAGMA {key}")) as cursor:
                observed = cursor.fetchone()[0]
            if str(observed).casefold() != str(value).casefold():
                raise PrimaryStateError(f"prepared-primary SQLite setting rejected: {key}")
        guard.check_journal(required=False, recovering=True)
        return connection
    except BaseException:
        connection.close()
        raise


def _engine_factory(guard: ContainerGuard, limits: SqlitePrimaryLimits) -> EngineFactory:
    def factory() -> Engine:
        engine = create_engine(
            "sqlite://", creator=lambda: _connect(guard, limits), poolclass=NullPool
        )

        @event.listens_for(engine, "begin")
        def begin(connection: Connection) -> None:
            connection.exec_driver_sql("BEGIN IMMEDIATE")

        return engine

    return factory


class TransactionalPrimaryFiles:
    """Logical names and immutable chunk generations in one synchronized database.

    This storage owner does not certify archive custody or authorize a producer.
    An interrupted candidate remains an exact singleton operation; callers drive
    its bounded cleanup before admitting another publication.
    """

    def __init__(
        self,
        guard: ContainerGuard,
        *,
        allocation_descriptor: bytes,
        limits: SqlitePrimaryLimits,
        initialize: bool,
    ) -> None:
        if not allocation_descriptor:
            guard.close()
            raise ValueError("prepared-primary allocation descriptor is required")
        self._guard = guard
        self._limits = limits
        self._mutex = threading.RLock()
        self._closed = False
        self._poisoned = False
        self._namespace = 1
        self._root = self
        self._readers: set[_ChunkReader] = set()
        self._pins: dict[str, int] = {}
        self._ordering_guard = threading.Lock()
        self._connection: sqlite3.Connection | None = None
        factory = _engine_factory(guard, limits)
        schema = _primary_state_schema(factory)
        try:
            if initialize:
                engine = factory()
                try:
                    with engine.begin() as connection:
                        schema.upgrade_connection(connection)
                        connection.exec_driver_sql(
                            "INSERT INTO primary_format VALUES (1, ?, ?, 0, zeroblob(16))",
                            (allocation_descriptor, _GEOMETRY),
                        )
                        connection.exec_driver_sql(
                            "INSERT INTO primary_namespaces VALUES (1, NULL, 'root')"
                        )
                finally:
                    engine.dispose()
            else:
                status = schema.status()
                if status.condition != "current":
                    raise PrimaryStateError("prepared-primary schema is not current")
                engine = factory()
                try:
                    with engine.connect() as connection:
                        _verify_shape(connection)
                finally:
                    engine.dispose()
            self._connection = _connect(guard, limits)
            identity = self._one("SELECT allocation, geometry FROM primary_format WHERE id=1")
            if identity != (allocation_descriptor, _GEOMETRY):
                raise PrimaryStateError("prepared-primary allocation or storage geometry changed")
            self.journal_reserved_bytes()
            self.sync()
        except BaseException:
            self.close()
            raise

    @classmethod
    def windows(
        cls,
        path: Path,
        *,
        allocation_descriptor: bytes,
        limits: SqlitePrimaryLimits,
        initialize: bool,
    ) -> TransactionalPrimaryFiles:
        from riverhog_client._prepared_primary_windows_native import (
            WindowsContainer,
            windows_container_path,
        )

        if not allocation_descriptor:
            raise ValueError("prepared-primary allocation descriptor is required")
        if path != windows_container_path(path.parent, allocation_descriptor):
            raise PrimaryStateError("prepared-primary container name differs from its allocation")
        return cls(
            WindowsContainer(path, initialize=initialize),
            allocation_descriptor=allocation_descriptor,
            limits=limits,
            initialize=initialize,
        )

    def __enter__(self) -> TransactionalPrimaryFiles:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def check_current(self) -> None:
        if self._closed or self._root._closed:
            raise PrimaryStateError("prepared-primary construction owner is closed")
        if self._root._poisoned:
            raise PrimaryStateError("prepared-primary uncertain write requires close and reopen")
        self._guard.check_current()

    def _db(self) -> sqlite3.Connection:
        self.check_current()
        connection = self._root._connection
        if connection is None:
            raise PrimaryStateError("prepared-primary database is closed")
        return connection

    def _one(self, query: str, parameters: tuple[Any, ...] = ()) -> tuple[Any, ...] | None:
        with self._mutex, closing(self._db().execute(query, parameters)) as cursor:
            row = cursor.fetchone()
            return None if row is None else tuple(row)

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._mutex:
            db = self._db()
            try:
                db.execute("BEGIN IMMEDIATE").close()
                yield db
                self._guard.check_current()
                db.execute("COMMIT").close()
                self._guard.check_journal(required=True)
            except BaseException as exc:
                self._root._poisoned = True
                if db.in_transaction:
                    try:
                        db.rollback()
                    except sqlite3.Error:
                        pass
                if isinstance(exc, sqlite3.Error):
                    if getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL:
                        raise PrimaryStateCapacity(
                            "prepared-primary database admission exhausted"
                        ) from exc
                    raise PrimaryStateError("prepared-primary SQLite mutation failed") from exc
                raise

    def sync(self) -> None:
        """A successful real update, including after an uncertain prior process."""
        with self._transaction() as db:
            cursor = db.execute(
                "UPDATE primary_format SET barrier=barrier+1 "
                "WHERE id=1 AND barrier<9223372036854775807"
            )
            if cursor.rowcount != 1:
                cursor.close()
                raise PrimaryStateError("prepared-primary durable barrier is unavailable")
            cursor.close()

    def journal_reserved_bytes(self) -> int:
        from riverhog_client._prepared_primary_sqlite_journal import reserved_bytes

        return reserved_bytes(self)

    def require_journal_ready(self) -> None:
        from riverhog_client._prepared_primary_sqlite_journal import require

        require(self)

    def space_usage(self) -> SqlitePrimarySpace:
        """Observe database/reusable pages and actual owned file allocation."""
        from riverhog_client._prepared_primary_sqlite_space import space_usage

        return space_usage(self)

    def vacuum_step(self, *, keep_free_pages: int) -> SqliteVacuumStep:
        """Explicit execution only: one freelist page, preserving the caller's free-page floor."""
        from riverhog_client._prepared_primary_sqlite_space import vacuum_step

        return vacuum_step(self, keep_free_pages=keep_free_pages)

    def child(self, name: str, *, initialize: bool) -> TransactionalPrimaryFiles:
        primary_file_name(name)
        with self._mutex:
            self.check_current()
            if initialize:
                if (
                    self._one(
                        "SELECT id FROM primary_namespaces WHERE parent=? AND name=?",
                        (self._namespace, name),
                    )
                    is not None
                ):
                    raise FileExistsError(name)
                self._require_idle()
                with self._transaction() as db:
                    cursor = db.execute(
                        "INSERT INTO primary_namespaces(parent,name) VALUES (?,?)",
                        (self._namespace, name),
                    )
                    identifier = cursor.lastrowid
                    cursor.close()
                    if identifier is None:
                        raise PrimaryStateError("prepared-primary child identity is unavailable")
            else:
                row = self._one(
                    "SELECT id FROM primary_namespaces WHERE parent=? AND name=?",
                    (self._namespace, name),
                )
                if row is None:
                    raise FileNotFoundError(name)
                identifier = row[0]
            child = object.__new__(TransactionalPrimaryFiles)
            child._guard, child._limits = self._guard, self._limits
            child._mutex, child._root = self._mutex, self._root
            child._closed, child._namespace = False, identifier
            return child

    def _require_idle(self) -> None:
        if self._one("SELECT generation FROM primary_operation WHERE id=1") is not None:
            raise PrimaryStateBusy(
                "prepared-primary interrupted candidate requires bounded cleanup"
            )
        self._require_ordering_idle()

    def _require_ordering_idle(self) -> None:
        ordering = self._one("SELECT phase FROM primary_ordering WHERE id=1")
        if ordering is not None and ordering != ("ready",):
            raise PrimaryStateBusy("prepared-primary derived ordering requires bounded cleanup")

    def _require_control_idle(self, name: str) -> None:
        self._require_ordering_idle()
        candidate = self._one(
            "SELECT o.namespace,o.name FROM primary_operation p JOIN primary_objects o "
            "ON o.generation=p.generation WHERE p.id=1"
        )
        if candidate == (self._namespace, name):
            raise PrimaryStateBusy("prepared-primary control conflicts with its byte candidate")

    def ordered_rows(
        self, rows: Iterable[tuple[str, int, str]], *, byte_limit: int
    ) -> Iterator[tuple[str, int, str]]:
        """Keep derived ordering inside this container under one live reader."""
        from riverhog_client._prepared_primary_sqlite_ordering import ordered_rows

        self.check_current()
        guard = self._root._ordering_guard
        if not guard.acquire(blocking=False):
            raise PrimaryStateBusy("prepared-primary inventory ordering has a live reader")
        try:
            yield from ordered_rows(self, rows, byte_limit=byte_limit)
        finally:
            guard.release()

    def ordering_reserved_bytes(self) -> int:
        """Retain the original grant across ready, interrupted and cleanup phases."""
        from riverhog_canonical_json import parse_scalar

        row = self._one("SELECT byte_limit FROM primary_ordering WHERE id=1")
        if row is None:
            return 0
        try:
            return parse_scalar("nonnegative", row[0])
        except (TypeError, ValueError) as exc:
            raise PrimaryStateError("prepared-primary scratch admission is invalid") from exc

    def recover_ordering_step(self) -> bool:
        """A ready cache needs no work; interrupted scratch advances one cleanup step."""
        with self._mutex:
            phase = self._one("SELECT phase FROM primary_ordering WHERE id=1")
            if phase is None or phase == ("ready",):
                return True
            return self.ordering_cleanup_step()

    def ordering_cleanup_step(self) -> bool:
        """Explicit execution/maintenance only; at most one derived row per call."""
        from riverhog_client._prepared_primary_sqlite_ordering import cleanup_step

        self.check_current()
        guard = self._root._ordering_guard
        if not guard.acquire(blocking=False):
            raise PrimaryStateBusy("prepared-primary inventory ordering has a live reader")
        try:
            return cleanup_step(self)
        finally:
            guard.release()

    def discard_candidates(self, names: tuple[str, ...]) -> bool:
        """Advance one unselected candidate step, never selected-byte reclamation."""
        for name in names:
            primary_file_name(name)
        with self._mutex:
            row = self._one(
                "SELECT o.namespace,o.name,o.complete,p.phase FROM primary_operation p "
                "JOIN primary_objects o ON o.generation=p.generation WHERE p.id=1"
            )
            if row is None or row[0] != self._namespace or row[2] == 1:
                return True
            if row[1] not in names or row[2] != 0 or row[3] not in ("writing", "cleanup"):
                raise PrimaryStateError("prepared-primary candidate differs from its logical owner")
            return self.recover_step()

    def put_bytes(
        self,
        name: str,
        chunks: Iterable[bytes],
        *,
        byte_limit: int,
        expected: tuple[int, str] | None = None,
    ) -> PrimaryByteFile:
        primary_file_name(name)
        if type(byte_limit) is not int or byte_limit < 0:
            raise ValueError("prepared-primary byte admission must be explicit and nonnegative")
        with self._mutex:
            self._require_idle()
            if self.exists(name):
                raise FileExistsError(name)
            from riverhog_client._prepared_primary_sqlite_controls import reserved

            if reserved(self, name):
                raise PrimaryStateError(
                    "prepared-primary byte role conflicts with reserved control"
                )
            self.require_journal_ready()
            generation = uuid.uuid4().hex
            with self._transaction() as db:
                db.execute(
                    "INSERT INTO primary_objects VALUES (?,?,?,0,'',0)",
                    (generation, self._namespace, name),
                ).close()
                db.execute(
                    "INSERT INTO primary_operation VALUES (1,?,0,'writing')", (generation,)
                ).close()
            digest, size, ordinal = hashlib.sha256(), 0, 0
            pending = bytearray()
            try:
                for chunk in chunks:
                    if not isinstance(chunk, bytes):
                        raise TypeError("prepared-primary chunks must be bytes")
                    if len(chunk) > byte_limit - size:
                        raise PrimaryStateCapacity(
                            "prepared-primary durable-byte admission exhausted"
                        )
                    digest.update(chunk)
                    size += len(chunk)
                    position = 0
                    while position < len(chunk):
                        take = min(_BLOCK - len(pending), len(chunk) - position)
                        pending.extend(memoryview(chunk)[position : position + take])
                        position += take
                        if len(pending) == _BLOCK:
                            self._append(generation, ordinal, bytes(pending))
                            ordinal += 1
                            pending.clear()
                if pending:
                    self._append(generation, ordinal, bytes(pending))
                result = PrimaryByteFile(name, size, digest.hexdigest())
                if expected is not None and (size, result.sha256) != expected:
                    raise PrimaryStateError(
                        "prepared-primary bytes differ from the expected identity"
                    )
                self._select(generation, result)
                return result
            except BaseException:
                self._root._poisoned = True
                raise

    def _append(self, generation: str, ordinal: int, body: bytes) -> None:
        with self._transaction() as db:
            if self._one("SELECT generation,cursor,phase FROM primary_operation WHERE id=1") != (
                generation,
                ordinal,
                "writing",
            ):
                raise PrimaryStateError("prepared-primary candidate progress changed")
            db.execute(
                "INSERT INTO primary_chunks VALUES (?,?,?)", (generation, ordinal, body)
            ).close()
            db.execute(
                "UPDATE primary_objects SET bytes=bytes+? WHERE generation=?",
                (len(body), generation),
            ).close()
            db.execute(
                "UPDATE primary_operation SET cursor=? WHERE id=1 AND generation=?",
                (ordinal + 1, generation),
            ).close()

    def _select(self, generation: str, file: PrimaryByteFile) -> None:
        with self._transaction() as db:
            if self._one("SELECT generation,cursor,phase FROM primary_operation WHERE id=1") != (
                generation,
                (file.bytes + _BLOCK - 1) // _BLOCK,
                "writing",
            ):
                raise PrimaryStateError("prepared-primary candidate selection changed")
            cursor = db.execute(
                "UPDATE primary_objects SET sha256=?,complete=1 "
                "WHERE generation=? AND bytes=? AND complete=0 AND namespace=? AND name=?",
                (file.sha256, generation, file.bytes, self._namespace, file.name),
            )
            if cursor.rowcount != 1:
                cursor.close()
                raise PrimaryStateError("prepared-primary candidate extent changed")
            cursor.close()
            db.execute(
                "INSERT INTO primary_names VALUES (?,?,?)", (self._namespace, file.name, generation)
            ).close()
            db.execute(
                "DELETE FROM primary_operation WHERE id=1 AND generation=?", (generation,)
            ).close()

    def recover_step(self) -> bool:
        """Delete at most one unselected candidate chunk; no allocation enumeration."""
        with self._mutex:
            row = self._one("SELECT generation,cursor,phase FROM primary_operation WHERE id=1")
            if row is None:
                return True
            generation, cursor, phase = row
            if self._one("SELECT 1 FROM primary_names WHERE generation=?", (generation,)):
                raise PrimaryStateError(
                    "prepared-primary selected generation has a candidate operation"
                )
            if phase == "writing":
                cursor = 0
            chunk = self._one(
                "SELECT ordinal FROM primary_chunks WHERE generation=? AND ordinal>=? "
                "ORDER BY ordinal LIMIT 1",
                (generation, cursor),
            )
            with self._transaction() as db:
                if chunk is not None:
                    db.execute(
                        "DELETE FROM primary_chunks WHERE generation=? AND ordinal=?",
                        (generation, chunk[0]),
                    ).close()
                    db.execute(
                        "UPDATE primary_operation SET cursor=?,phase='cleanup' WHERE id=1",
                        (chunk[0] + 1,),
                    ).close()
                else:
                    db.execute("DELETE FROM primary_operation WHERE id=1").close()
                    db.execute(
                        "DELETE FROM primary_objects WHERE generation=?", (generation,)
                    ).close()
            return chunk is None

    def _control_selected(self, name: str) -> bool:
        from riverhog_client._prepared_primary_sqlite_controls import selected

        return selected(self, name)

    def exists(self, name: str) -> bool:
        primary_file_name(name)
        parameters = (self._namespace, name)
        return self._one(
            "SELECT 1 FROM primary_names WHERE namespace=? AND name=?", parameters
        ) is not None or self._control_selected(name)

    def prepare_controls(self, names: tuple[str, ...], *, byte_limit: int) -> None:
        from riverhog_client._prepared_primary_sqlite_controls import prepare

        prepare(self, names, byte_limit=byte_limit)

    def read_control(self, name: str, *, byte_limit: int) -> bytes:
        from riverhog_client._prepared_primary_sqlite_controls import read

        return read(self, name, byte_limit=byte_limit)

    def put_control(
        self, name: str, body: bytes, *, byte_limit: int, expected: bytes | None = None
    ) -> None:
        from riverhog_client._prepared_primary_sqlite_controls import put

        put(self, name, body, byte_limit=byte_limit, expected=expected)

    def remove_control(self, name: str, *, expected: bytes) -> None:
        from riverhog_client._prepared_primary_sqlite_controls import remove

        remove(self, name, expected=expected)

    def remove_step(self, name: str) -> bool:
        """Unselect one unpinned generation, then delete at most one chunk per step."""
        primary_file_name(name)
        with self._mutex:
            operation = self._one(
                "SELECT o.generation,o.namespace,o.name,o.complete,p.phase "
                "FROM primary_operation p JOIN primary_objects o "
                "ON o.generation=p.generation WHERE p.id=1"
            )
            if operation is not None:
                if operation[1:] == (self._namespace, name, 1, "cleanup"):
                    return self.recover_step()
                if operation[1:3] != (self._namespace, name) and not self.exists(name):
                    return True
                raise PrimaryStateBusy("another prepared-primary mutation requires cleanup")
            row = self._one(
                "SELECT generation FROM primary_names WHERE namespace=? AND name=?",
                (self._namespace, name),
            )
            if row is None:
                return True
            generation = row[0]
            self._require_idle()
            if self._root._pins.get(generation, 0):
                raise PrimaryStateBusy("prepared-primary replay still pins this generation")
            with self._transaction() as db:
                cursor = db.execute(
                    "DELETE FROM primary_names WHERE namespace=? AND name=? AND generation=?",
                    (self._namespace, name, generation),
                )
                if cursor.rowcount != 1:
                    cursor.close()
                    raise PrimaryStateError("prepared-primary selected generation changed")
                cursor.close()
                db.execute(
                    "INSERT INTO primary_operation VALUES (1,?,0,'cleanup')", (generation,)
                ).close()
            return False

    @contextmanager
    def _selected_reader(
        self, name: str, *, byte_count: int, sha256: str | None = None
    ) -> Iterator[_ChunkReader]:
        primary_file_name(name)
        if type(byte_count) is not int or byte_count < 0:
            raise ValueError("prepared-primary extent length must be nonnegative")
        with self._mutex:
            row = self._one(
                "SELECT o.generation,o.bytes,o.sha256,o.complete FROM primary_names n "
                "JOIN primary_objects o ON o.generation=n.generation "
                "WHERE n.namespace=? AND n.name=?",
                (self._namespace, name),
            )
            if row is None:
                raise FileNotFoundError(name)
            if row[1] != byte_count or row[3] != 1 or (sha256 is not None and row[2] != sha256):
                raise PrimaryStateError(
                    "prepared-primary selected extent differs from its commitment"
                )
            stream = _ChunkReader(self, row[0], byte_count)
            self._root._readers.add(stream)
            self._root._pins[row[0]] = self._root._pins.get(row[0], 0) + 1
        try:
            yield stream
            self.check_current()
        finally:
            stream.close()
            with self._mutex:
                self._root._readers.discard(stream)
                remaining = self._root._pins.get(row[0], 0)
                if remaining > 1:
                    self._root._pins[row[0]] = remaining - 1
                else:
                    self._root._pins.pop(row[0], None)

    @contextmanager
    def extent_reader(self, name: str, *, byte_count: int) -> Iterator[BinaryIO]:
        """Pin an exact extent; the caller verifies its original framed commitment."""
        with self._selected_reader(name, byte_count=byte_count) as stream:
            yield cast(BinaryIO, stream)

    @contextmanager
    def verified(self, file: PrimaryByteFile) -> Iterator[BinaryIO]:
        """Verify all original bytes before exposing a replay reader."""
        with self._selected_reader(file.name, byte_count=file.bytes, sha256=file.sha256) as stream:
            digest = hashlib.sha256()
            while chunk := stream.read(_BLOCK):
                digest.update(chunk)
            if digest.hexdigest() != file.sha256:
                raise PrimaryStateError(
                    "prepared-primary selected bytes differ from their commitment"
                )
            stream.seek(0)
            yield cast(BinaryIO, stream)

    def close(self) -> None:
        with self._mutex:
            self._closed = True
            if self._root is not self:
                return
            for reader in self._readers:
                reader.close()
            self._readers.clear()
            self._pins.clear()
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            self._guard.close()


class _ChunkReader(io.RawIOBase):
    def __init__(self, owner: TransactionalPrimaryFiles, generation: str, length: int) -> None:
        super().__init__()
        self._owner, self._generation, self._length = owner, generation, length
        self._position = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        self._checkClosed()
        self._owner.check_current()
        return self._position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._checkClosed()
        self._owner.check_current()
        if whence not in (io.SEEK_SET, io.SEEK_CUR, io.SEEK_END):
            raise ValueError("invalid prepared-primary seek origin")
        position = (
            offset
            + {io.SEEK_SET: 0, io.SEEK_CUR: self._position, io.SEEK_END: self._length}[whence]
        )
        if position < 0:
            raise ValueError("negative prepared-primary seek")
        self._position = position
        return position

    def readline(self, size: int | None = -1) -> bytes:
        self._checkClosed()
        self._owner.check_current()
        requested = -1 if size is None else operator.index(size)
        remaining = self._length - self._position
        limit = remaining if requested < 0 else min(remaining, requested)
        result = bytearray()
        while len(result) < limit:
            count = min(_BLOCK - self._position % _BLOCK, limit - len(result))
            fragment = self.read(count)
            newline = fragment.find(b"\n")
            if newline >= 0:
                used = newline + 1
                self._position -= len(fragment) - used
                result.extend(fragment[:used])
                break
            result.extend(fragment)
        return bytes(result)

    def readinto(self, target: Any) -> int:
        self._checkClosed()
        self._owner.check_current()
        if self._position >= self._length or not len(target):
            return 0
        written = 0
        while written < len(target) and self._position < self._length:
            ordinal, within = divmod(self._position, _BLOCK)
            row = self._owner._one(
                "SELECT body FROM primary_chunks WHERE generation=? AND ordinal=?",
                (self._generation, ordinal),
            )
            expected = min(_BLOCK, self._length - ordinal * _BLOCK)
            if row is None or not isinstance(row[0], bytes) or len(row[0]) != expected:
                raise PrimaryStateError("prepared-primary chunk extent changed")
            count = min(len(target) - written, expected - within)
            target[written : written + count] = memoryview(row[0])[within : within + count]
            self._position += count
            written += count
        return written
