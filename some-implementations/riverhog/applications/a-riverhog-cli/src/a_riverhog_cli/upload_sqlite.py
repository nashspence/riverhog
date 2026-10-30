"""Application custody and finite admission for upload projection databases."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock, get_ident
from types import TracebackType
from typing import Any, Literal, Protocol, Self

from pydantic import BaseModel, ConfigDict, Field
from riverhog_client import WindowsSqliteProcess
from riverhog_provenance.journal_store import JournalStore
from sqlite_admission import (
    SqliteAdmissionStatus,
    SqliteCleanupPending,
    SqliteConnectionAdmission,
    SqliteConnectionSettings,
)


class UploadSqlitePolicy(BaseModel):
    """Active grants for the upload's co-resident tags and provenance work.

    These grants bound ordinary handles and calls. They do not impose a heap,
    filesystem-scratch or source-inventory limit, or select a primary backend.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
    connections: int = Field(default=8, strict=True, gt=0)
    cursors: int = Field(default=32, strict=True, gt=0)
    operations: int = Field(default=4, strict=True, gt=0)
    blobs: int = Field(default=1, strict=True, ge=0)
    cache_kib: int = Field(default=512, strict=True, ge=0)


class _Storage(Protocol):
    @property
    def cleanup_complete(self) -> bool: ...

    def close(self) -> None: ...


@dataclass(eq=False)
class _Resource:
    directory: Path
    connection: sqlite3.Connection | None = None
    owners: list[_Storage] = field(default_factory=list)


_process_lock = RLock()
_live: set[UploadSqliteResources] = set()


class UploadSqliteResources:
    """Retain actual projection owners and their exact disposable directories.

    A supplied Windows process must already own the qualified engine's aggregate
    heap and native population policy before this owner or any consumer opens.
    Memory temporary storage requires that process; every admitted open checks
    the same owner. Cleanup settles handles without resetting its process grants.
    Cache targets do not provide heap enforcement or scheduling priorities.

    Primary factories share this admission through connect_primary. Their
    persistent files retain primary custody and are never projection scratch.
    Callers close primary handles before settling this application owner.

    Both projection connection and early-retention ports use this same application
    owner. An unsuccessful factory call retains the exact supplied directory,
    without guessing ownership from a filename. Unknown construction remains
    charged through process exit. Borrowed journal readers retain the original
    database until their own leases close; closing the original handle alone
    does not settle the application's connection or directory obligation.
    """

    def __init__(
        self,
        policy: UploadSqlitePolicy,
        *,
        process: WindowsSqliteProcess | None = None,
        temporary_storage: Literal["FILE", "MEMORY"] = "FILE",
    ) -> None:
        if temporary_storage not in ("FILE", "MEMORY"):
            raise ValueError("upload SQLite temporary storage must be FILE or MEMORY")
        if temporary_storage == "MEMORY" and process is None:
            raise ValueError("memory temporary storage requires the admitted SQLite process")
        with _process_lock:
            if _live:
                raise SqliteCleanupPending("previous upload SQLite ownership is unsettled")
            self._lock = RLock()
            self._closing = False
            self._process = process
            self._resources: list[_Resource] = []
            self._awaiting: dict[int, _Resource] = {}
            self._admission = SqliteConnectionAdmission(
                connection_limit=policy.connections,
                cursor_limit=policy.cursors,
                blob_limit=policy.blobs,
                operation_limit=policy.operations,
                verify_process=self._require_open,
                connection_settings=SqliteConnectionSettings(
                    cache_kib=policy.cache_kib, temporary_storage=temporary_storage
                ),
            )
            self._require_open()
            _live.add(self)

    @property
    def status(self) -> SqliteAdmissionStatus:
        return self._admission.status

    @property
    def pending_directories(self) -> tuple[Path, ...]:
        """Observe retained exact scratch obligations without advancing cleanup."""
        with self._lock:
            return tuple(resource.directory for resource in self._resources)

    def _require_open(self) -> None:
        with self._lock:
            if self._closing:
                raise SqliteCleanupPending("upload SQLite ownership is closing")
            if self._process is not None:
                self._process.verify()

    @staticmethod
    def _directory_absent(directory: Path) -> bool:
        try:
            directory.lstat()
        except FileNotFoundError:
            return True
        except OSError:
            return False
        return False

    def _prune(self) -> None:
        pending = set(self._admission.pending_connections)
        self._resources = [
            resource
            for resource in self._resources
            if not (
                resource.owners
                and all(owner.cleanup_complete for owner in resource.owners)
                and resource.connection not in pending
                and self._directory_absent(resource.directory)
            )
        ]

    def connect(self, database: Path, **options: object) -> sqlite3.Connection:
        """Admit the exact disposable database path before its actual driver call."""
        if not isinstance(database, Path) or not database.is_absolute():
            raise ValueError("upload projection storage requires its exact absolute database path")
        with self._lock:
            self._require_open()
            self._prune()
            thread = get_ident()
            if thread in self._awaiting:
                raise SqliteCleanupPending("upload SQLite open lacks its early owner retention")
            resource = _Resource(database.parent)
            self._resources.append(resource)
            self._awaiting[thread] = resource
            try:
                resource.connection = self._admission.connect(database, **options)
            except BaseException:
                self._closing = True
                raise
            return resource.connection

    def connect_primary(self, database: Any, **options: Any) -> sqlite3.Connection:
        """Share actual pager/schema admission without acquiring disposable custody.

        The selected primary factory owns its exact URI, native handles,
        allocation identity and persistent files. It must close those handles
        before this application's admission can settle. No primary path is
        inferred, parsed or added to projection-directory cleanup.
        """
        with self._lock:
            self._require_open()
            if self._process is None:
                raise ValueError("primary SQLite admission requires the admitted process")
            if get_ident() in self._awaiting:
                raise SqliteCleanupPending("upload SQLite open lacks its early owner retention")
            try:
                return self._admission.connect(database, **options)
            except BaseException:
                self._closing = True
                raise

    def retain(self, owner: _Storage) -> None:
        """Retain constructor and independently handed-off result-reader leases."""
        with self._lock:
            resource = self._awaiting.pop(get_ident(), None)
            if resource is None and isinstance(owner, JournalStore):
                connection = owner.db
                resource = next(
                    (item for item in self._resources if item.connection is connection), None
                )
            if resource is None or resource.connection is None:
                raise SqliteCleanupPending("upload SQLite owner lacks its admitted connection")
            resource.owners = [item for item in resource.owners if not item.cleanup_complete]
            if not any(item is owner for item in resource.owners):
                resource.owners.append(owner)
            self._require_open()

    def _finish(self) -> None:
        self._prune()
        if self._resources or self.status.connections or self.status.operations:
            raise SqliteCleanupPending("upload SQLite cleanup remains unconfirmed")
        with _process_lock:
            _live.discard(self)

    def close(self) -> None:
        """Explicitly settle owned handles without closing another reader's lease."""
        with self._lock:
            self._closing = True
            first_error: BaseException | None = None
            for resource in self._resources:
                for owner in resource.owners:
                    if not owner.cleanup_complete:
                        try:
                            owner.close()
                        except BaseException as error:
                            if first_error is None:
                                first_error = error
            self._prune()
            if first_error is not None:
                raise first_error
            self._finish()

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            with self._lock:
                if self._closing:
                    self._finish()
                else:
                    self.close()
        else:
            # Preserve the original error. Uncertain cleanup needs an explicit
            # retry, never an implicit second close during stack unwinding.
            with self._lock:
                self._closing = True
                self._prune()
                if (
                    not self._resources
                    and not self.status.connections
                    and not self.status.operations
                ):
                    with _process_lock:
                        _live.discard(self)
