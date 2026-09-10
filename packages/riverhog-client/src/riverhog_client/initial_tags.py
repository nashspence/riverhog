"""Bounded-memory preparation of complete initial collection-tag intent."""

from __future__ import annotations

import sqlite3
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from types import TracebackType

from riverhog_protocol import (
    COLLECTION_TAG_REQUEST_MEMBERS_MAX,
    CollectionTag,
    CollectionTagIntegrityError,
    CollectionTagNodeMissing,
    CollectionTagSet,
    collection_tag_node_digest,
    validate_collection_tag,
)


class _SqliteTagNodeStore:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def get(self, digest: str) -> bytes:
        row = self._connection.execute(
            "SELECT encoded FROM nodes WHERE digest = ?", (digest,)
        ).fetchone()
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
            self._connection.execute(
                "INSERT INTO nodes(digest, encoded) VALUES (?, ?)", (digest, payload)
            )
        except sqlite3.IntegrityError:
            row = self._connection.execute(
                "SELECT encoded FROM nodes WHERE digest = ?", (digest,)
            ).fetchone()
            if row is None or bytes(row[0]) != payload:
                raise CollectionTagIntegrityError("prepared immutable tag node differs") from None


class PreparedInitialCollectionTags:
    """A locally durable canonical tag set and deterministic bounded replay."""

    def __init__(self, tags: Iterable[str]) -> None:
        self._temporary = tempfile.TemporaryDirectory(prefix="riverhog-initial-tags-")
        self._connection = sqlite3.connect(Path(self._temporary.name) / "tags.sqlite3")
        try:
            self._connection.executescript(
                """
                PRAGMA journal_mode = DELETE;
                PRAGMA synchronous = FULL;
                PRAGMA temp_store = FILE;
                CREATE TABLE tags (
                    tag_key BLOB PRIMARY KEY,
                    tag TEXT NOT NULL UNIQUE
                );
                CREATE TABLE nodes (
                    digest TEXT PRIMARY KEY,
                    encoded BLOB NOT NULL
                );
                """
            )
            count = 0
            for supplied in tags:
                tag = validate_collection_tag(supplied)
                try:
                    self._connection.execute(
                        "INSERT INTO tags(tag_key, tag) VALUES (?, ?)",
                        (tag.encode("utf-8"), tag),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ValueError("initial collection tags must not contain duplicates") from exc
                count += 1
                if count % COLLECTION_TAG_REQUEST_MEMBERS_MAX == 0:
                    self._connection.commit()
            self._connection.commit()
            tag_set = CollectionTagSet(_SqliteTagNodeStore(self._connection))
            cursor = self._connection.execute("SELECT tag FROM tags ORDER BY tag_key")
            while rows := cursor.fetchmany(COLLECTION_TAG_REQUEST_MEMBERS_MAX):
                for row in rows:
                    tag_set = tag_set.insert(str(row[0]))
                self._connection.commit()
            self.tag_set_identity = tag_set.identity
            self.count = count
        except BaseException:
            self.close()
            raise

    def iter_batches(self) -> Iterator[tuple[CollectionTag, ...]]:
        cursor = self._connection.execute("SELECT tag FROM tags ORDER BY tag_key")
        while rows := cursor.fetchmany(COLLECTION_TAG_REQUEST_MEMBERS_MAX):
            yield tuple(str(row[0]) for row in rows)

    def close(self) -> None:
        connection = getattr(self, "_connection", None)
        if connection is not None:
            connection.close()
            self._connection = None  # type: ignore[assignment]
        temporary = getattr(self, "_temporary", None)
        if temporary is not None:
            temporary.cleanup()
            self._temporary = None  # type: ignore[assignment]

    def __enter__(self) -> PreparedInitialCollectionTags:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


def prepare_initial_collection_tags(tags: Iterable[str]) -> PreparedInitialCollectionTags:
    """Validate and seal one complete initial tag set before remote mutation."""

    return PreparedInitialCollectionTags(tags)


__all__ = ["PreparedInitialCollectionTags", "prepare_initial_collection_tags"]
