"""Bounded ordering of exact encrypted provenance custody receipts."""

from __future__ import annotations

import contextlib
import sqlite3
import sys
from collections.abc import Iterator
from pathlib import Path
from types import TracebackType
from typing import Self

from riverhog_protocol import CollectionUploadProvenanceCustodyObjectDocument

from riverhog_core.scratch_workspace import scratch_directory


class ProvenanceCustodySet:
    def __init__(self) -> None:
        self._scratch = contextlib.ExitStack()
        try:
            scratch = self._scratch.enter_context(
                scratch_directory(prefix="riverhog-provenance-custody-")
            )
            self._db = sqlite3.connect(Path(scratch) / "receipts.sqlite3")
            self._scratch.callback(self._db.close)
            self._db.execute("PRAGMA cache_size = -512")
            self._db.execute("CREATE TABLE receipts (path TEXT PRIMARY KEY, value TEXT)")
        except BaseException:
            self._scratch.__exit__(*sys.exc_info())
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return self._scratch.__exit__(exc_type, exc, traceback)

    def add(self, receipt: CollectionUploadProvenanceCustodyObjectDocument) -> None:
        old = self._db.execute(
            "SELECT value FROM receipts WHERE path = ?", (receipt.relative_path,)
        ).fetchone()
        encoded = receipt.model_dump_json()
        if old is not None and old[0] != encoded:
            raise ValueError("custody object path has conflicting immutable receipts")
        self._db.execute(
            "INSERT OR IGNORE INTO receipts VALUES (?, ?)", (receipt.relative_path, encoded)
        )

    def objects(self) -> Iterator[CollectionUploadProvenanceCustodyObjectDocument]:
        for (encoded,) in self._db.execute("SELECT value FROM receipts ORDER BY path"):
            yield CollectionUploadProvenanceCustodyObjectDocument.model_validate_json(encoded)


__all__ = ["ProvenanceCustodySet"]
