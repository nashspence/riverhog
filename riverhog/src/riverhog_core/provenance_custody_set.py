"""Bounded ordering of exact encrypted provenance custody receipts."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Self

from riverhog_protocol import CollectionUploadProvenanceCustodyObjectDocument


class ProvenanceCustodySet:
    def __init__(self) -> None:
        self._scratch = TemporaryDirectory(prefix="riverhog-provenance-custody-")
        self._db = sqlite3.connect(Path(self._scratch.name) / "receipts.sqlite3")
        self._db.execute("PRAGMA cache_size = -512")
        self._db.execute("CREATE TABLE receipts (path TEXT PRIMARY KEY, value TEXT)")

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self._db.close()
        self._scratch.cleanup()

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
