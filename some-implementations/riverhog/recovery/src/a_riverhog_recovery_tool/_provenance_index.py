"""Attempt-owned seek index for an independently verified descriptor sequence."""

from __future__ import annotations

import shutil
import sqlite3
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Self

from riverhog_archive_contracts import (
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    format_archive_sequence,
)

Document = ProvenanceVolumeDocument | ProvenanceTerminalDocument


class ProvenanceMetadataIndex:
    """Keep original cursors and storage until their actual close is confirmed.

    Rows are disposable projections, never archive authority. Only a complete
    verified scan may seal the index. No payload or structure bytes are cached.
    Preparation inserts D bounded rows with SQLite B-tree construction costs;
    keyed lookups pay a B-tree seek and visit only the selected descriptor rows.
    """

    def __init__(
        self, workspace: Path, *, on_storage_open: Callable[[Self], None] | None = None
    ) -> None:
        self._closing = False
        self._ready = False
        self._connection_closed = False
        self._directory_removed = False
        self._cursors: set[sqlite3.Cursor] = set()
        self._root = Path(tempfile.mkdtemp(prefix="provenance-metadata-", dir=workspace))
        try:
            self._db = sqlite3.connect(self._root / "metadata.sqlite3")
        except BaseException:
            self._remove_directory()
            raise
        retained = False
        try:
            if on_storage_open is not None:
                on_storage_open(self)
                retained = True
            self._run(
                "PRAGMA cache_size = -512; PRAGMA temp_store = FILE; BEGIN; "
                "CREATE TABLE volumes(sequence TEXT PRIMARY KEY, kind TEXT, "
                "journal TEXT, body BLOB); "
                "CREATE INDEX volume_journals ON volumes(journal, sequence); "
                "CREATE INDEX volume_kinds ON volumes(kind, sequence); COMMIT;",
                script=True,
            )
        except BaseException:
            if not retained:
                self.close()
            raise

    def require_open(self) -> None:
        if self._closing:
            raise RuntimeError("provenance metadata index is closed")

    def _remove_directory(self) -> None:
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
        cursor = (
            self._db.executescript(statement) if script else self._db.execute(statement, parameters)
        )
        self._cursors.add(cursor)
        try:
            self.require_open()
        finally:
            self._close_cursor(cursor)

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
        self._db.commit()
        self._run("PRAGMA query_only = ON")
        self._ready = True

    def descriptors(
        self, *, journal_id: str | None = None, kind: str | None = None
    ) -> Iterator[Document]:
        self.require_open()
        if not self._ready:
            raise RuntimeError("provenance metadata index is not ready")
        # Journal identities occur only on journal rows, as the admitted scan
        # proved. Do not offer the planner a competing kind-wide index here.
        values: tuple[str, ...]
        if journal_id is not None:
            where, values = " WHERE journal = ?", (journal_id,)
        elif kind is not None:
            where, values = " WHERE kind = ?", (kind,)
        else:
            where, values = "", ()
        cursor = self._db.execute(
            "SELECT kind, body FROM volumes" + where + " ORDER BY sequence", values
        )
        self._cursors.add(cursor)
        try:
            while True:
                self.require_open()
                row = cursor.fetchone()
                if row is None:
                    break
                selected_kind, raw = row
                if kind is not None and selected_kind != kind:
                    continue
                document = (
                    ProvenanceTerminalDocument.from_json_bytes(raw)
                    if selected_kind == "terminal"
                    else ProvenanceVolumeDocument.from_json_bytes(raw)
                )
                yield document
        finally:
            self._close_cursor(cursor)
