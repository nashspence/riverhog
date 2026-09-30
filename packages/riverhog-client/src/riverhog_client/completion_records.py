"""Bounded local spooling of exact immutable completion record preimages."""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Self

from riverhog_canonical_json import canonical_json_bytes, require_canonical_json

from riverhog_client.canonical_completion import CompletionRecord


class CompletionRecords:
    def __init__(self) -> None:
        self._scratch = TemporaryDirectory(prefix="riverhog-completion-records-")
        self._root = Path(self._scratch.name)
        self._db = sqlite3.connect(self._root / "records.sqlite3")
        self._db.execute("PRAGMA cache_size = -512")
        self._db.execute(
            "CREATE TABLE records (kind TEXT PRIMARY KEY, size TEXT, sha256 TEXT, path TEXT)"
        )
        self._db.execute(
            "CREATE TABLE outputs (artifact_id TEXT PRIMARY KEY, value BLOB, imports BLOB)"
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self._db.close()
        self._scratch.cleanup()

    def add(self, kind: str, chunks: Iterable[bytes]) -> CompletionRecord:
        path = self._root / hashlib.sha256(kind.encode("utf-8")).hexdigest()
        digest = hashlib.sha256()
        size = 0
        with path.open("wb") as stream:
            for chunk in chunks:
                stream.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        if size == 0:
            raise ValueError("required completion record has no bytes")
        try:
            self._db.execute(
                "INSERT INTO records VALUES (?, ?, ?, ?)",
                (kind, str(size), digest.hexdigest(), str(path)),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError("duplicate completion record kind") from exc
        return self.record(kind)

    def add_output(self, output: Mapping[str, Any], imports: Mapping[str, Any]) -> None:
        try:
            self._db.execute(
                "INSERT INTO outputs VALUES (?, ?, ?)",
                (
                    output["artifact_id"],
                    canonical_json_bytes(dict(output)),
                    canonical_json_bytes(dict(imports)),
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError("duplicate completion output artifact") from exc

    def output_rows(self, *, imports: bool = False) -> Iterator[dict[str, Any]]:
        column = "imports" if imports else "value"
        for (content,) in self._db.execute(f"SELECT {column} FROM outputs ORDER BY artifact_id"):
            row = require_canonical_json(content)
            if not isinstance(row, dict):
                raise ValueError("completion output mapping is not an exact object")
            yield row

    def record(self, kind: str) -> CompletionRecord:
        row = self._db.execute(
            "SELECT size, sha256, path FROM records WHERE kind = ?", (kind,)
        ).fetchone()
        if row is None:
            raise KeyError(kind)
        path = Path(row[2])

        def read() -> Iterator[bytes]:
            with path.open("rb") as source:
                while chunk := source.read(128 * 1024):
                    yield chunk

        return CompletionRecord(kind, int(row[0]), row[1], read)

    def records(self) -> Iterator[CompletionRecord]:
        for (kind,) in self._db.execute("SELECT kind FROM records ORDER BY kind"):
            yield self.record(kind)


__all__ = ["CompletionRecords"]
