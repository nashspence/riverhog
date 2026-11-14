"""Bounded local spooling of exact immutable completion record preimages."""

from __future__ import annotations

import hashlib
import shutil
import sqlite3
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from contextlib import closing
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, BinaryIO, Literal, Self

from riverhog_canonical_json import canonical_json_bytes, require_canonical_json

from riverhog_client.canonical_completion import CompletionRecord


class CompletionRecords:
    """Own actual cursors, streams and spool storage until cleanup is confirmed.

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
        self._acquisitions = 0
        self._cursors: set[sqlite3.Cursor] = set()
        self._streams: set[BinaryIO] = set()
        try:
            if on_storage_open is not None:
                on_storage_open(self)
            self._require_open()
            self._run("PRAGMA cache_size = -512")
            self._run(
                "CREATE TABLE records (kind TEXT PRIMARY KEY, size TEXT, sha256 TEXT, path TEXT)"
            )
            self._run(
                "CREATE TABLE outputs (artifact_id TEXT PRIMARY KEY, output_id TEXT UNIQUE, "
                "value BLOB, imports BLOB)"
            )
        except BaseException:
            if not self._closing:
                self.close()
            raise

    @property
    def cleanup_complete(self) -> bool:
        """Whether all owned cursors, streams, connection and spool are released."""
        return (
            not self._acquisitions
            and not self._cursors
            and not self._streams
            and self._connection_closed
            and self._directory_removed
        )

    def _require_open(self) -> None:
        if self._closing:
            raise RuntimeError("completion records are closed")

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        if not self._closing:
            self.close()

    def _release_cursor(self, cursor: sqlite3.Cursor) -> None:
        if cursor in self._cursors:
            try:
                cursor.close()
            except BaseException:
                self._closing = True
                raise
            self._cursors.remove(cursor)

    def _release_stream(self, stream: BinaryIO) -> None:
        if stream in self._streams:
            try:
                stream.close()
            except BaseException:
                self._closing = True
                raise
            self._streams.remove(stream)

    def _execute(self, statement: str, parameters: tuple[object, ...] = ()) -> sqlite3.Cursor:
        self._require_open()
        self._acquisitions += 1
        try:
            cursor = self._db.cursor()
            self._cursors.add(cursor)
        finally:
            self._acquisitions -= 1
        try:
            self._require_open()
            cursor.execute(statement, parameters)
            self._require_open()
        except BaseException:
            if not self._closing:
                self._release_cursor(cursor)
            raise
        return cursor

    def _run(self, statement: str, parameters: tuple[object, ...] = ()) -> None:
        cursor = self._execute(statement, parameters)
        if not self._closing:
            self._release_cursor(cursor)
        self._require_open()

    def _one(self, statement: str, parameters: tuple[object, ...] = ()) -> tuple[Any, ...] | None:
        cursor = self._execute(statement, parameters)
        try:
            row: tuple[Any, ...] | None = cursor.fetchone()
            self._require_open()
        finally:
            if not self._closing:
                self._release_cursor(cursor)
        self._require_open()
        return row

    def _rows(self, statement: str) -> Generator[tuple[Any, ...], None, None]:
        cursor = self._execute(statement)
        try:
            while True:
                self._require_open()
                row = cursor.fetchone()
                self._require_open()
                if row is None:
                    break
                yield row
        finally:
            if not self._closing:
                self._release_cursor(cursor)

    def _open_stream(self, path: Path, mode: Literal["rb", "xb"]) -> BinaryIO:
        self._require_open()
        self._acquisitions += 1
        try:
            stream: BinaryIO = path.open(mode)
            self._streams.add(stream)
        finally:
            self._acquisitions -= 1
        self._require_open()
        return stream

    def close(self) -> None:
        self._closing = True
        if self._acquisitions:
            raise RuntimeError("completion storage acquisition is still active")
        for cursor in tuple(self._cursors):
            self._release_cursor(cursor)
        for stream in tuple(self._streams):
            self._release_stream(stream)
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
            stream = self._open_stream(path, "xb")
        except FileExistsError as exc:
            raise ValueError("duplicate completion record kind") from exc
        try:
            try:
                for chunk in chunks:
                    self._require_open()
                    stream.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
            finally:
                if not self._closing:
                    self._release_stream(stream)
            self._require_open()
            if size == 0:
                raise ValueError("required completion record has no bytes")
            self._run(
                "INSERT INTO records VALUES (?, ?, ?, ?)",
                (kind, str(size), digest.hexdigest(), str(path)),
            )
        except BaseException:
            if not self._closing:
                path.unlink(missing_ok=True)
            raise
        return self.record(kind)

    def add_output(self, output: Mapping[str, Any], imports: Mapping[str, Any]) -> None:
        self._require_open()
        try:
            self._run(
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
        row = self._one("SELECT count(*) FROM outputs")
        assert row is not None
        return int(row[0])

    def output_for_key(self, output_id: str) -> dict[str, Any]:
        row = self._one("SELECT value FROM outputs WHERE output_id = ?", (output_id,))
        if row is None:
            raise ValueError("accepted operation refers to an unproduced output key")
        value = require_canonical_json(row[0])
        if not isinstance(value, dict):
            raise ValueError("completion output is not an exact object")
        self._require_open()
        return value

    def output_rows(self, *, imports: bool = False) -> Iterator[dict[str, Any]]:
        column = "imports" if imports else "value"
        with closing(self._rows(f"SELECT {column} FROM outputs ORDER BY artifact_id")) as rows:
            for item in rows:
                row = require_canonical_json(item[0])
                if not isinstance(row, dict):
                    raise ValueError("completion output mapping is not an exact object")
                self._require_open()
                yield row

    def record(self, kind: str) -> CompletionRecord:
        row = self._one("SELECT size, sha256, path FROM records WHERE kind = ?", (kind,))
        if row is None:
            raise KeyError(kind)
        path = Path(row[2])

        def read() -> Iterator[bytes]:
            source = self._open_stream(path, "rb")
            try:
                while True:
                    self._require_open()
                    chunk = source.read(128 * 1024)
                    self._require_open()
                    if not chunk:
                        break
                    yield chunk
            finally:
                if not self._closing:
                    self._release_stream(source)

        return CompletionRecord(kind, int(row[0]), row[1], read)

    def records(self) -> Iterator[CompletionRecord]:
        with closing(self._rows("SELECT kind FROM records ORDER BY kind")) as rows:
            for item in rows:
                record = self.record(item[0])
                self._require_open()
                yield record


__all__ = ["CompletionRecords"]
