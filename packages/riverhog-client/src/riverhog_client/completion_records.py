"""Bounded local spooling of exact immutable completion record preimages."""

from __future__ import annotations

import hashlib
import shutil
import sqlite3
from collections.abc import Callable, Iterable, Iterator, Mapping
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Self

from riverhog_canonical_json import canonical_json_bytes, require_canonical_json

from riverhog_client.canonical_completion import CompletionRecord


class CompletionRecords:
    """Own one disposable spool until connection close and file removal are confirmed.

    A supplied factory owns unreturned connections and their private directory.
    Early retention gives the caller a close-ready owner before any schema work.
    Use the context manager or close explicitly; an uncertain close needs an
    explicit retry through the retained owner.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[..., sqlite3.Connection] | None = None,
        on_storage_open: Callable[[Self], None] | None = None,
    ) -> None:
        self._root = Path(mkdtemp(prefix="riverhog-completion-records-"))
        connect = sqlite3.connect if connection_factory is None else connection_factory
        try:
            self._db = connect(self._root / "records.sqlite3")
        except BaseException:
            if connection_factory is None:
                shutil.rmtree(self._root)
            raise
        self._closing = False
        self._connection_closed = False
        self._directory_removed = False
        try:
            if on_storage_open is not None:
                on_storage_open(self)
            self._require_open()
            self._db.execute("PRAGMA cache_size = -512")
            self._db.execute(
                "CREATE TABLE records (kind TEXT PRIMARY KEY, size TEXT, sha256 TEXT, path TEXT)"
            )
            self._db.execute(
                "CREATE TABLE outputs (artifact_id TEXT PRIMARY KEY, output_id TEXT UNIQUE, "
                "value BLOB, imports BLOB)"
            )
        except BaseException:
            if not self._closing:
                self.close()
            raise

    def _require_open(self) -> None:
        if self._closing:
            raise RuntimeError("completion records are closed")

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        if not self._closing:
            self.close()

    def close(self) -> None:
        self._closing = True
        if not self._connection_closed:
            self._db.close()
            self._connection_closed = True
        if not self._directory_removed:
            try:
                shutil.rmtree(self._root)
            except FileNotFoundError:
                if self._root.exists():
                    raise
            self._directory_removed = True

    def add(self, kind: str, chunks: Iterable[bytes]) -> CompletionRecord:
        self._require_open()
        path = self._root / hashlib.sha256(kind.encode("utf-8")).hexdigest()
        digest = hashlib.sha256()
        size = 0
        try:
            stream = path.open("xb")
        except FileExistsError as exc:
            raise ValueError("duplicate completion record kind") from exc
        try:
            with stream:
                for chunk in chunks:
                    self._require_open()
                    stream.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
            self._require_open()
            if size == 0:
                raise ValueError("required completion record has no bytes")
            self._db.execute(
                "INSERT INTO records VALUES (?, ?, ?, ?)",
                (kind, str(size), digest.hexdigest(), str(path)),
            )
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        return self.record(kind)

    def add_output(self, output: Mapping[str, Any], imports: Mapping[str, Any]) -> None:
        self._require_open()
        try:
            self._db.execute(
                "INSERT INTO outputs VALUES (?, ?, ?, ?)",
                (
                    output["artifact_id"],
                    output["output_id"],
                    canonical_json_bytes(dict(output)),
                    canonical_json_bytes(dict(imports)),
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError("duplicate completion output artifact") from exc

    @property
    def output_count(self) -> int:
        self._require_open()
        return int(self._db.execute("SELECT count(*) FROM outputs").fetchone()[0])

    def output_for_key(self, output_id: str) -> dict[str, Any]:
        self._require_open()
        row = self._db.execute(
            "SELECT value FROM outputs WHERE output_id = ?", (output_id,)
        ).fetchone()
        if row is None:
            raise ValueError("accepted operation refers to an unproduced output key")
        value = require_canonical_json(row[0])
        if not isinstance(value, dict):
            raise ValueError("completion output is not an exact object")
        return value

    def output_rows(self, *, imports: bool = False) -> Iterator[dict[str, Any]]:
        self._require_open()
        column = "imports" if imports else "value"
        rows = self._db.execute(f"SELECT {column} FROM outputs ORDER BY artifact_id")
        while True:
            self._require_open()
            item = rows.fetchone()
            if item is None:
                break
            row = require_canonical_json(item[0])
            if not isinstance(row, dict):
                raise ValueError("completion output mapping is not an exact object")
            yield row

    def record(self, kind: str) -> CompletionRecord:
        self._require_open()
        row = self._db.execute(
            "SELECT size, sha256, path FROM records WHERE kind = ?", (kind,)
        ).fetchone()
        if row is None:
            raise KeyError(kind)
        path = Path(row[2])

        def read() -> Iterator[bytes]:
            self._require_open()
            with path.open("rb") as source:
                while True:
                    self._require_open()
                    chunk = source.read(128 * 1024)
                    if not chunk:
                        break
                    yield chunk

        return CompletionRecord(kind, int(row[0]), row[1], read)

    def records(self) -> Iterator[CompletionRecord]:
        self._require_open()
        rows = self._db.execute("SELECT kind FROM records ORDER BY kind")
        while True:
            self._require_open()
            item = rows.fetchone()
            if item is None:
                break
            yield self.record(item[0])


__all__ = ["CompletionRecords"]
