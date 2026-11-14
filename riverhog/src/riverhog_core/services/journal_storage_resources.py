"""Application-owned admission for retained Native journal constructions."""

from __future__ import annotations

import os
import sqlite3
from threading import RLock

from riverhog_provenance import CorpusConstruction, JournalConstruction
from sqlite_admission import (
    SqliteAdmissionStatus,
    SqliteCleanupPending,
    SqliteConnectionAdmission,
    SqliteConnectionSettings,
)

from riverhog_core.journal_storage_grants import JournalStorageGrants


class JournalStorageResources:
    """Share exact tickets under the original application/process lifetime.

    Passive frame allocation installs no process policy and opens no storage.
    Each reservation verifies the same live owning process and serialized
    SQLite engine before file effects. The existing FILE temporary-storage and
    Native cache/durability profile remains selected. This owner supplies no
    aggregate heap ceiling or scratch/worker admission.
    """

    def __init__(self, grants: JournalStorageGrants) -> None:
        self._grants = grants
        self._pid = os.getpid()
        self._engine = (sqlite3.sqlite_version, sqlite3.threadsafety)
        self._lock = RLock()
        self._closing = False
        self._admission = SqliteConnectionAdmission(
            connection_limit=grants.connections,
            cursor_limit=grants.cursors,
            operation_limit=grants.operations,
            verify_process=self.verify_process,
            connection_settings=SqliteConnectionSettings(cache_kib=512, temporary_storage="FILE"),
        )

    def verify_process(self) -> None:
        # An inherited owner cannot supply a fresh process's grants or identity.
        if os.getpid() != self._pid:
            raise SqliteCleanupPending("Native storage belongs to another original process")
        with self._lock:
            if self._closing:
                raise SqliteCleanupPending("Native storage admission is closing")
            if self._engine != (sqlite3.sqlite_version, sqlite3.threadsafety):
                raise RuntimeError("the selected Native SQLite engine changed")
            if sqlite3.threadsafety != 3:
                raise RuntimeError("Native storage requires serialized SQLite threading")

    def journal(self) -> JournalConstruction:
        """Allocate only passive bookkeeping; reserve after caller retention."""
        return JournalConstruction(
            self._admission.connection_scope(), resource_limit=self._grants.file_records
        )

    def corpus(self) -> CorpusConstruction:
        """Allocate the POSIX corpus's separate long-lived guard profile."""
        return CorpusConstruction(
            self._admission.connection_scope(), resource_limit=self._grants.corpus_file_records
        )

    @property
    def status(self) -> SqliteAdmissionStatus:
        return self._admission.status

    def close(self) -> None:
        with self._lock:
            self._closing = True
            status = self._admission.status
            if status.connections or status.cursors or status.blobs or status.operations:
                raise SqliteCleanupPending("original Native storage tickets remain unreceived")
