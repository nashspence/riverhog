"""Bounded-memory preparation of complete initial collection-tag intent."""

from __future__ import annotations

import shutil
import sqlite3
import tempfile
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from pydantic import TypeAdapter
from riverhog_protocol import (
    COLLECTION_TAG_REQUEST_MEMBERS_MAX,
    CollectionTag,
    CollectionTagIntegrityError,
    CollectionTagNodeMissing,
    CollectionTagSet,
    collection_tag_node_digest,
    validate_collection_tag,
)
from riverhog_protocol.paths import CollectionId
from sqlite_admission import SqliteConnectionSettings, configure_sqlite_connection


class _SqliteTagNodeStore:
    def __init__(self, owner: PreparedInitialCollectionTags) -> None:
        self._owner = owner

    def get(self, digest: str) -> bytes:
        row = self._owner._one("SELECT encoded FROM nodes WHERE digest = ?", (digest,))
        if row is None:
            raise CollectionTagNodeMissing(digest)
        encoded = bytes(row[0])
        if collection_tag_node_digest(encoded) != digest:
            raise CollectionTagIntegrityError("prepared tag node digest differs")
        return encoded

    def put(self, digest: str, encoded: bytes) -> None:
        payload = bytes(encoded)
        if collection_tag_node_digest(payload) != digest:
            raise CollectionTagIntegrityError("prepared tag node digest differs")
        try:
            self._owner._run("INSERT INTO nodes(digest, encoded) VALUES (?, ?)", (digest, payload))
        except sqlite3.IntegrityError:
            row = self._owner._one("SELECT encoded FROM nodes WHERE digest = ?", (digest,))
            if row is None or bytes(row[0]) != payload:
                raise CollectionTagIntegrityError("prepared immutable tag node differs") from None


class PreparedInitialCollectionTags:
    """Complete tag intent with actual cursor, connection and scratch custody.

    Supplied factories own unreturned connection files. Early retention receives
    the close-ready owner before SQL or input consumption. Uncertain cleanup
    freezes further work and requires explicit close retry.
    """

    def __init__(
        self,
        tags: Iterable[str],
        *,
        connection_factory: Callable[..., sqlite3.Connection] | None = None,
        on_storage_open: Callable[[Self], None] | None = None,
    ) -> None:
        self._closing = False
        self._connection_closed = False
        self._directory_removed = False
        self._ready = False
        self._cursors: set[sqlite3.Cursor] = set()
        self._cursor_openings = 0
        self._root = Path(tempfile.mkdtemp(prefix="riverhog-initial-tags-"))
        connect = sqlite3.connect if connection_factory is None else connection_factory
        try:
            self._connection = connect(self._root / "tags.sqlite3")
        except BaseException:
            if connection_factory is None:
                self._remove_directory()
            raise
        try:
            if on_storage_open is not None:
                on_storage_open(self)
            self._require_open()
            configure_sqlite_connection(
                self._connection,
                defaults=SqliteConnectionSettings(),
                read_rows=self._configuration_rows,
            )
            self._run(
                """
                PRAGMA journal_mode = DELETE;
                PRAGMA synchronous = FULL;
                CREATE TABLE tags (
                    tag_key BLOB PRIMARY KEY,
                    tag TEXT NOT NULL UNIQUE
                );
                CREATE TABLE nodes (
                    digest TEXT PRIMARY KEY,
                    encoded BLOB NOT NULL
                );
                """,
                script=True,
            )
            count = 0
            for supplied in tags:
                self._require_open()
                tag = validate_collection_tag(supplied)
                try:
                    self._run(
                        "INSERT INTO tags(tag_key, tag) VALUES (?, ?)",
                        (tag.encode("utf-8"), tag),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ValueError("initial collection tags must not contain duplicates") from exc
                count += 1
                if count % COLLECTION_TAG_REQUEST_MEMBERS_MAX == 0:
                    self._commit()
            self._commit()
            tag_set = CollectionTagSet(_SqliteTagNodeStore(self))
            for batch in self._batches():
                for tag in batch:
                    self._require_open()
                    tag_set = tag_set.insert(tag)
                self._commit()
            self._require_open()
            self.tag_set_identity = tag_set.identity
            self.count = count
            self._ready = True
        except BaseException:
            if not self._closing:
                self.close()
            raise

    @property
    def cleanup_complete(self) -> bool:
        return (
            not self._cursor_openings
            and not self._cursors
            and self._connection_closed
            and self._directory_removed
        )

    def _require_open(self) -> None:
        if self._closing:
            raise RuntimeError("prepared initial collection tags are closed")

    def _release_cursor(self, cursor: sqlite3.Cursor) -> None:
        if cursor in self._cursors:
            try:
                cursor.close()
            except BaseException:
                self._closing = True
                raise
            self._cursors.remove(cursor)

    def _execute(
        self, statement: str, parameters: tuple[object, ...] = (), *, script: bool = False
    ) -> sqlite3.Cursor:
        self._require_open()
        self._cursor_openings += 1
        try:
            cursor = self._connection.cursor()
            self._cursors.add(cursor)
        finally:
            self._cursor_openings -= 1
        try:
            self._require_open()
            if script:
                cursor.executescript(statement)
            else:
                cursor.execute(statement, parameters)
            self._require_open()
        except BaseException:
            if not self._closing:
                self._release_cursor(cursor)
            raise
        return cursor

    def _run(
        self, statement: str, parameters: tuple[object, ...] = (), *, script: bool = False
    ) -> None:
        cursor = self._execute(statement, parameters, script=script)
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

    def _configuration_rows(self, statement: str, count: int) -> list[Any]:
        cursor = self._execute(statement)
        try:
            rows = cursor.fetchmany(count)
            self._require_open()
            return rows
        finally:
            if not self._closing:
                self._release_cursor(cursor)

    def _commit(self) -> None:
        self._require_open()
        self._connection.commit()
        self._require_open()

    def _batches(self) -> Iterator[tuple[CollectionTag, ...]]:
        cursor = self._execute("SELECT tag FROM tags ORDER BY tag_key")
        try:
            while True:
                self._require_open()
                rows = cursor.fetchmany(COLLECTION_TAG_REQUEST_MEMBERS_MAX)
                self._require_open()
                if not rows:
                    break
                batch = tuple(str(row[0]) for row in rows)
                self._require_open()
                yield batch
        finally:
            if not self._closing:
                self._release_cursor(cursor)

    def iter_batches(self) -> Iterator[tuple[CollectionTag, ...]]:
        self._require_open()
        if not self._ready:
            raise RuntimeError("prepared initial collection tags are not ready")
        yield from self._batches()

    def _remove_directory(self) -> None:
        try:
            shutil.rmtree(self._root)
        except FileNotFoundError:
            if self._root.exists():
                raise

    def close(self) -> None:
        self._closing = True
        if self._cursor_openings:
            raise RuntimeError("initial tag cursor acquisition is still active")
        for cursor in tuple(self._cursors):
            self._release_cursor(cursor)
        if not self._connection_closed:
            self._connection.close()
            self._connection_closed = True
        if not self._directory_removed:
            self._remove_directory()
            self._directory_removed = True

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if not self._closing:
            self.close()


def prepare_initial_collection_tags(
    tags: Iterable[str],
    *,
    connection_factory: Callable[..., sqlite3.Connection] | None = None,
    on_storage_open: Callable[[PreparedInitialCollectionTags], None] | None = None,
) -> PreparedInitialCollectionTags:
    """Validate and seal one complete initial tag set before remote mutation."""

    return PreparedInitialCollectionTags(
        tags, connection_factory=connection_factory, on_storage_open=on_storage_open
    )


def create_or_resume_with_initial_collection_tags(
    tags: Iterable[str],
    *,
    create_or_resume: Callable[[Sequence[CollectionTag], str], Mapping[str, Any]],
    add_tags: Callable[[int, Sequence[CollectionTag]], object],
    connection_factory: Callable[..., sqlite3.Connection] | None = None,
    on_storage_open: Callable[[PreparedInitialCollectionTags], None] | None = None,
) -> dict[str, Any]:
    """Reconcile complete tag intent and stage bounded batches only while open."""

    with prepare_initial_collection_tags(
        tags, connection_factory=connection_factory, on_storage_open=on_storage_open
    ) as prepared:
        batches = prepared.iter_batches()
        first_batch = next(batches, ())
        session = dict(create_or_resume(first_batch, prepared.tag_set_identity))
        if str(session.get("state") or "") != "open":
            return session
        collection_id: int = TypeAdapter(CollectionId).validate_python(session.get("collection_id"))
        for batch in batches:
            add_tags(collection_id, batch)
        return session


__all__ = [
    "PreparedInitialCollectionTags",
    "create_or_resume_with_initial_collection_tags",
    "prepare_initial_collection_tags",
]
