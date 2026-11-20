"""Disposable seek index for an exact verified archive metadata sequence."""

from __future__ import annotations

import shutil
import sqlite3
from collections.abc import Callable, Iterator
from typing import Self

from riverhog_archive_contracts import (
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    format_archive_sequence,
)

from riverhog_core.scratch_workspace import capacity_errors, create_scratch_directory

Document = ProvenanceVolumeDocument | ProvenanceTerminalDocument


class ProvenanceMetadataIndex:
    """Retain actual cursors, connection and private scratch until confirmed close.

    A supplied factory owns any connection and directory it fails to return.
    The index is a disposable projection; only the caller's complete verified
    archive scan can make it ready for reads.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[..., sqlite3.Connection] | None = None,
        on_storage_open: Callable[[Self], None] | None = None,
    ) -> None:
        self._closing = False
        self._connection_closed = False
        self._directory_removed = False
        self._ready = False
        self._cursors: set[sqlite3.Cursor] = set()
        self._root = create_scratch_directory(prefix="riverhog-provenance-metadata-")
        connect = sqlite3.connect if connection_factory is None else connection_factory
        try:
            with capacity_errors():
                self._db = connect(self._root / "metadata.sqlite3", check_same_thread=False)
        except BaseException:
            if connection_factory is None:
                self._remove_directory()
            raise
        try:
            if on_storage_open is not None:
                on_storage_open(self)
            self.require_open()
            self._run(
                "PRAGMA cache_size = -512; PRAGMA temp_store = FILE; "
                "CREATE TABLE volumes(sequence TEXT PRIMARY KEY, kind TEXT, "
                "journal TEXT, body BLOB); "
                "CREATE INDEX volume_journals ON volumes(journal, sequence); "
                "CREATE INDEX volume_kinds ON volumes(kind, sequence);",
                script=True,
            )
        except BaseException:
            if not self._closing:
                self.close()
            raise

    @property
    def closing(self) -> bool:
        return self._closing

    @property
    def cleanup_complete(self) -> bool:
        return self._directory_removed

    def require_open(self) -> None:
        if self._closing:
            raise RuntimeError("provenance metadata index is closed")

    def __enter__(self) -> Self:
        self.require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        if not self._closing:
            self.close()

    def _remove_directory(self) -> None:
        with capacity_errors():
            try:
                shutil.rmtree(self._root)
            except FileNotFoundError:
                if self._root.exists():
                    raise

    def _close_cursor(self, cursor: sqlite3.Cursor) -> None:
        if cursor in self._cursors:
            try:
                cursor.close()
            except BaseException:
                self._closing = True
                raise
            self._cursors.remove(cursor)

    def close(self) -> None:
        self._closing = True
        with capacity_errors():
            for cursor in tuple(self._cursors):
                self._close_cursor(cursor)
            if not self._connection_closed:
                self._db.close()
                self._connection_closed = True
            if not self._directory_removed:
                self._remove_directory()
                self._directory_removed = True

    def _run(
        self, statement: str, parameters: tuple[object, ...] = (), *, script: bool = False
    ) -> None:
        self.require_open()
        with capacity_errors():
            cursor = (
                self._db.executescript(statement)
                if script
                else self._db.execute(statement, parameters)
            )
            self._cursors.add(cursor)
            try:
                self.require_open()
            finally:
                if not self._closing:
                    self._close_cursor(cursor)
            self.require_open()

    def remember(self, document: Document) -> None:
        self.require_open()
        if self._ready:
            raise RuntimeError("provenance metadata index is sealed")
        if isinstance(document, ProvenanceTerminalDocument):
            kind, journal_id = "terminal", None
        else:
            kind, journal_id = document.payload.kind, document.journal_id
        self._run(
            "INSERT INTO volumes VALUES (?, ?, ?, ?)",
            (
                format_archive_sequence(document.sequence),
                kind,
                journal_id,
                document.to_json_bytes(),
            ),
        )

    def seal(self) -> None:
        self.require_open()
        with capacity_errors():
            self._db.commit()
            self.require_open()
            self._run("PRAGMA query_only = ON")
            self._ready = True

    def descriptors(
        self, *, journal_id: str | None = None, kind: str | None = None
    ) -> Iterator[Document]:
        self.require_open()
        if not self._ready:
            raise RuntimeError("provenance metadata index is not ready")
        filters = []
        values = []
        if journal_id is not None:
            filters.append("journal = ?")
            values.append(journal_id)
        if kind is not None:
            filters.append("kind = ?")
            values.append(kind)
        where = " WHERE " + " AND ".join(filters) if filters else ""
        with capacity_errors():
            cursor = self._db.execute(
                "SELECT kind, body FROM volumes" + where + " ORDER BY sequence", values
            )
            self._cursors.add(cursor)
            try:
                while True:
                    self.require_open()
                    row = cursor.fetchone()
                    self.require_open()
                    if row is None:
                        break
                    selected_kind, raw = row
                    document = (
                        ProvenanceTerminalDocument.from_json_bytes(raw)
                        if selected_kind == "terminal"
                        else ProvenanceVolumeDocument.from_json_bytes(raw)
                    )
                    self.require_open()
                    yield document
            finally:
                if not self._closing:
                    self._close_cursor(cursor)
