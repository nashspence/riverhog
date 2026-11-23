"""Original passive construction and receiving for optional Native objects.

The composing owner invokes construction only after its surviving recovery
grant. This module supplies neither that grant nor source/selection authority.
Working, capture and restore files retain their distinct original owners.
"""

from __future__ import annotations

import re
import sqlite3
import weakref
from _thread import LockType
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from threading import Lock, get_ident
from typing import Self
from uuid import uuid4

from sqlite_admission import SqliteConnectionSettings, configure_sqlite_connection

from ._generation_envelope import GenerationEnvelopes
from ._generation_objects import GenerationObjectStore
from ._generation_page_map import InvalidGenerationMap
from ._generation_roots import GenerationRoots
from ._native_epoch import EpochContext
from ._native_storage import NativeStorage, NativeStorageLease, StorageFinalizer
from ._native_work import WorkQuantum
from .journal_construction import JournalConstruction

_FORMAT = "riverhog-native-generation-objects/v3"
_NAME = "generations.sqlite3"


class NativeGenerationStorage:
    """One separately constructed retained connection under its original scope.

    Storage attaches to the passive construction before configuration/schema or
    early callbacks. A successful constructor receives creator/directory files
    before transferring its final callback. Failed or uncertain closure keeps
    that same handle and scope available to the caller's original receiver.
    Existing storage is inspected, never repaired, upgraded or recreated here.
    """

    _storage: NativeStorage
    _lease: NativeStorageLease
    _cleanup: StorageFinalizer
    _closing: bool
    _operation_thread: int | None
    _operation_gate: LockType
    roots: GenerationRoots
    epoch: EpochContext
    objects: GenerationObjectStore
    envelopes: GenerationEnvelopes
    store_id: str

    @classmethod
    def open(
        cls,
        directory: Path,
        *,
        construction: JournalConstruction,
        page_size: int,
        reader_limit: int,
        metadata_octets: int,
        create: bool = False,
        on_storage_open: Callable[[Self], None] | None = None,
        on_storage_close: Callable[[], None] | None = None,
    ) -> Self:
        if not isinstance(construction, JournalConstruction):
            raise TypeError("generation storage requires its original passive construction")
        if (
            type(page_size) is not int
            or not 512 <= page_size <= 65536
            or page_size & (page_size - 1)
        ):
            raise ValueError("generation storage requires an exact SQLite page size")
        if any(type(value) is not int or value < 1 for value in (reader_limit, metadata_octets)):
            raise ValueError(
                "generation storage requires positive original reader/metadata admission"
            )
        if type(create) is not bool:
            raise TypeError("generation storage creation requires explicit owner selection")
        construction.begin()
        try:
            directory = Path(directory)
            if directory.is_symlink() or not directory.is_dir():
                raise InvalidGenerationMap("generation owner directory is unavailable")
            path = directory / _NAME
            if path.is_symlink():
                raise InvalidGenerationMap("generation storage must be owner-created")
            if create:
                construction.create_exclusive(path)
                construction.sync_directory(directory)
            elif not path.is_file():
                raise InvalidGenerationMap("original generation storage is missing")
            connection = construction.scope.connect(
                path.resolve().as_uri() + "?mode=rw",
                uri=True,
                check_same_thread=False,
                cached_statements=0,
            )
            storage = cls.__new__(cls)
            storage._storage = NativeStorage(connection, directory, remove_on_release=False)
            storage._storage.retain()
            storage._lease = NativeStorageLease(storage._storage)
            storage._closing = False
            storage._operation_gate = Lock()
            storage._operation_thread = None
            storage._cleanup = weakref.finalize(storage, storage._lease.close)
            construction.retain_storage(storage)
            try:
                if on_storage_open is not None:
                    on_storage_open(storage)
                configure_sqlite_connection(
                    connection, defaults=SqliteConnectionSettings(cache_kib=512)
                )
                cls._durability(connection, create=create)
                storage.epoch = EpochContext(connection)
                storage.objects = GenerationObjectStore(storage.epoch, page_size=page_size)
                storage.roots = GenerationRoots(
                    storage, reader_limit=reader_limit, metadata_octets=metadata_octets
                )
                storage.envelopes = GenerationEnvelopes(storage, metadata_octets=metadata_octets)
                if create:
                    storage.store_id = uuid4().hex

                    def initialize(epoch: EpochContext) -> None:
                        epoch.require(connection)
                        storage.objects.initialize()
                        storage.roots.initialize()
                        connection.execute(
                            "CREATE TABLE generation_metadata(singleton INTEGER PRIMARY KEY "
                            "CHECK(singleton=1),format TEXT NOT NULL,store_id TEXT NOT NULL,"
                            "page_size INTEGER NOT NULL)"
                        ).close()
                        connection.execute(
                            "INSERT INTO generation_metadata VALUES(1,?,?,?)",
                            (_FORMAT, storage.store_id, page_size),
                        ).close()

                    with storage.execution():
                        storage.epoch.apply(initialize, receive=lambda result: None)
                    construction.sync_directory(directory)
                else:
                    with closing(
                        connection.execute(
                            "SELECT substr(format,1,?),substr(store_id,1,33),page_size "
                            "FROM generation_metadata WHERE singleton=1",
                            (len(_FORMAT) + 1,),
                        )
                    ) as cursor:
                        row = cursor.fetchone()
                    if (
                        row is None
                        or row[0] != _FORMAT
                        or type(row[1]) is not str
                        or re.fullmatch(r"[0-9a-f]{32}", row[1]) is None
                        or type(row[2]) is not int
                        or row[2] != page_size
                    ):
                        raise InvalidGenerationMap(
                            "generation identity, format or page width differs"
                        )
                    storage.store_id = row[1]
                    # These fixed point schema probes materialize no object prefix.
                    connection.execute(
                        "SELECT address,kind,body,refs FROM generation_objects LIMIT 0"
                    ).close()
                    connection.execute("SELECT address FROM generation_garbage LIMIT 0").close()
                    connection.execute(
                        "SELECT identity,root,status,envelope,envelope_sha256 "
                        "FROM generation_roots LIMIT 0"
                    ).close()
                construction.ready()
                storage._storage.on_close = on_storage_close
                return storage
            except BaseException:
                storage.close()
                raise
        finally:
            construction.end()

    @staticmethod
    def _durability(db: sqlite3.Connection, *, create: bool) -> None:
        if create:
            db.execute("PRAGMA auto_vacuum=INCREMENTAL").close()
        for command, expected in (
            ("PRAGMA journal_mode=DELETE" if create else "PRAGMA journal_mode", ("delete",)),
            ("PRAGMA auto_vacuum", (2,)),
        ):
            with closing(db.execute(command)) as cursor:
                if cursor.fetchone() != expected:
                    raise InvalidGenerationMap("generation storage durability profile differs")
        db.execute("PRAGMA synchronous=EXTRA").close()
        db.execute("PRAGMA fullfsync=ON").close()
        for command, expected in (("PRAGMA synchronous", (3,)), ("PRAGMA fullfsync", (1,))):
            with closing(db.execute(command)) as cursor:
                if cursor.fetchone() != expected:
                    raise InvalidGenerationMap("generation synchronization profile is unavailable")

    @property
    def db(self) -> sqlite3.Connection:
        if self._closing or self._lease.closed or not self._cleanup.alive:
            raise RuntimeError("generation storage is closed")
        return self._storage.connection

    @property
    def storage_received(self) -> bool:
        with self._storage._lock:
            return self._storage._closed

    @contextmanager
    def _operation(self) -> Iterator[None]:
        if not self._operation_gate.acquire(blocking=False):
            raise RuntimeError("original generation storage operation is active")
        self._operation_thread = get_ident()
        try:
            yield
        finally:
            self._operation_thread = None
            self._operation_gate.release()

    def _require_operation(self) -> None:
        if self._operation_thread != get_ident():
            raise RuntimeError("generation work requires its original operation gate")

    @contextmanager
    def execution(
        self, check: Callable[[], None] | None = None, *, work: WorkQuantum | None = None
    ) -> Iterator[EpochContext]:
        """Serialize actual SQLite/epoch work, independently of resource lease counts."""
        with self._operation():
            if self._closing or self._lease.closed:
                raise RuntimeError("generation storage is closed")
            with self.epoch.execution(check, work=work):
                yield self.epoch

    def close(self) -> None:
        self._closing = True
        with self._operation():
            self._lease.close()
            self._cleanup.detach()
