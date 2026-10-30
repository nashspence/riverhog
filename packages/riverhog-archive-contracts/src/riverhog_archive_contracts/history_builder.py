"""Producer-authored history sets with bounded, disk-backed canonical ordering."""

from __future__ import annotations

import shutil
import sqlite3
from collections.abc import Callable, Generator, Iterator
from contextlib import closing
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Self

from riverhog_canonical_json import canonical_json_bytes, require_canonical_json

from .member_history import (
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    MemberHistoryBinding,
    MemberHistoryDocument,
    MemberHistoryHeads,
    MemberHistoryImport,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    verify_member_history_sets,
)
from .structural_record_set import PAGE_RECORDS_MAX, RecordPage, RecordSetCommitment, RecordSetRef


class MemberHistoryBuilder:
    """Author a selection, sort on disk and emit exact bounded structure objects.

    The producer explicitly supplies the already accepted primary. The builder
    never infers source ancestry, finds other journals, or chooses transfer extent.
    Exact bytes are then durably staged by the construction service before sealing.
    A supplied factory admits the selection connection before schema setup and
    owns unreturned connection files. Early retention receives a close-ready owner;
    unconfirmed cleanup freezes further work until explicit close retry.
    Verification has separate factory and retention ports; uncertain child cleanup
    stays attached to the builder and freezes subsequent work.
    """

    def __init__(
        self,
        *,
        artifact_id: str,
        bytes: int,
        sha256: str,
        primary: MemberHistoryPrimary,
        connection_factory: Callable[..., sqlite3.Connection] | None = None,
        on_storage_open: Callable[[Self], None] | None = None,
        verification_connection_factory: Callable[..., sqlite3.Connection] | None = None,
        on_verification_storage_open: Callable[[MemberHistoryHeads], None] | None = None,
    ) -> None:
        self.member = (artifact_id, bytes, sha256)
        self.primary = primary
        self._sealed: MemberHistoryDocument | None = None
        self._verification_connection_factory = verification_connection_factory
        self._on_verification_storage_open = on_verification_storage_open
        self._verification: MemberHistoryHeads | None = None
        self._closing = False
        self._connection_closed = False
        self._directory_removed = False
        self._cursors: set[sqlite3.Cursor] = set()
        self._root = Path(mkdtemp(prefix="riverhog-history-builder-"))
        connect = sqlite3.connect if connection_factory is None else connection_factory
        try:
            self._db = connect(self._root / "selection.sqlite3")
        except BaseException:
            if connection_factory is None:
                self._remove_directory()
            raise
        try:
            if on_storage_open is not None:
                on_storage_open(self)
            self._require_open()
            self._run("PRAGMA cache_size = -512")
            self._run(
                "CREATE TABLE records (domain TEXT, key TEXT, value BLOB NOT NULL, "
                "PRIMARY KEY (domain, key))"
            )
            self.add_root(MemberHistoryRoot(primary.journal, "bound"))
        except BaseException:
            if not self._closing:
                self.close()
            raise

    def _require_open(self) -> None:
        if self._closing:
            raise RuntimeError("member history builder is closed")

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        if not self._closing:
            self.close()

    def _remove_directory(self) -> None:
        try:
            shutil.rmtree(self._root)
        except FileNotFoundError:
            if self._root.exists():
                raise

    def _retain_verification(self, heads: MemberHistoryHeads) -> None:
        self._verification = heads
        self._require_open()
        if self._on_verification_storage_open is not None:
            self._on_verification_storage_open(heads)

    def close(self) -> None:
        self._closing = True
        if self._verification is not None:
            self._verification.close()
            self._verification = None
        for cursor in tuple(self._cursors):
            cursor.close()
            self._cursors.remove(cursor)
        if not self._connection_closed:
            self._db.close()
            self._connection_closed = True
        if not self._directory_removed:
            self._remove_directory()
            self._directory_removed = True

    def _execute(self, statement: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        self._require_open()
        cursor = self._db.execute(statement, parameters)
        self._cursors.add(cursor)
        return cursor

    def _release_cursor(self, cursor: sqlite3.Cursor) -> None:
        if not self._closing and cursor in self._cursors:
            try:
                cursor.close()
            except BaseException:
                self._closing = True
                raise
            self._cursors.remove(cursor)

    def _run(self, statement: str, parameters: tuple[Any, ...] = ()) -> None:
        self._release_cursor(self._execute(statement, parameters))

    def _rows(self, domain: str) -> Generator[tuple[Any, ...], None, None]:
        cursor = self._execute(
            "SELECT key, value FROM records WHERE domain = ? ORDER BY key", (domain,)
        )
        try:
            while True:
                self._require_open()
                row = cursor.fetchone()
                if row is None:
                    break
                yield row
        finally:
            self._release_cursor(cursor)

    def _add(self, domain: str, key: str, value: dict[str, object]) -> None:
        self._require_open()
        if self._sealed is not None:
            raise ValueError("member history builder is sealed")
        try:
            self._run(
                "INSERT INTO records VALUES (?, ?, ?)", (domain, key, canonical_json_bytes(value))
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError("duplicate member history selection") from exc

    def add_root(self, selected: MemberHistoryRoot) -> None:
        self._add(MEMBER_HISTORY_ROOTS_SCHEMA, selected.key, selected.to_mapping())

    def add_import(self, selected: MemberHistoryImport) -> None:
        self._add(MEMBER_HISTORY_IMPORTS_SCHEMA, selected.key, selected.to_mapping())

    def _ref(self, domain: str) -> RecordSetRef:
        commitment = RecordSetCommitment(domain)
        with closing(self._rows(domain)) as rows:
            for key, value in rows:
                parsed = require_canonical_json(value)
                if not isinstance(parsed, dict):
                    raise ValueError("member history record is not an object")
                commitment.update(key, parsed)
        return commitment.ref()

    def seal(self) -> tuple[MemberHistoryBinding, MemberHistoryDocument]:
        self._require_open()
        if self._sealed is None:
            artifact_id, byte_count, sha256 = self.member
            history = MemberHistoryDocument(
                artifact_id,
                byte_count,
                sha256,
                self.primary,
                self._ref(MEMBER_HISTORY_ROOTS_SCHEMA),
                self._ref(MEMBER_HISTORY_IMPORTS_SCHEMA),
            )
            try:
                verify_member_history_sets(
                    history,
                    root_pages=self.pages(history.roots),
                    import_pages=self.pages(history.imports),
                    connection_factory=self._verification_connection_factory,
                    on_storage_open=self._retain_verification,
                )
            except BaseException:
                if self._verification is not None:
                    if self._verification.cleanup_complete:
                        self._verification = None
                    else:
                        self._closing = True
                raise
            self._require_open()
            self._verification = None
            self._sealed = history
        history = self._sealed
        return MemberHistoryBinding(
            history.artifact_id,
            history.bytes,
            history.sha256,
            history.identity,
            len(history.to_json_bytes()),
        ), history

    def pages(self, authority: RecordSetRef) -> Generator[RecordPage, None, None]:
        self._require_open()
        rows: list[dict[str, object]] = []
        ordinal = 0
        with closing(self._rows(authority.schema_id)) as selected:
            for key, value in selected:
                rows.append({"key": key, "value": require_canonical_json(value)})
                if len(rows) == PAGE_RECORDS_MAX:
                    yield RecordPage(authority, ordinal, tuple(rows), False)
                    ordinal += 1
                    rows = []
        if rows:
            self._require_open()
            yield RecordPage(authority, ordinal, tuple(rows), False)
            ordinal += 1
        self._require_open()
        yield RecordPage(authority, ordinal, (), True)

    def objects(self) -> Iterator[bytes]:
        _, history = self.seal()
        for authority in (history.roots, history.imports):
            with closing(self.pages(authority)) as pages:
                for page in pages:
                    self._require_open()
                    yield page.to_json_bytes()
        self._require_open()
        yield history.to_json_bytes()


__all__ = ["MemberHistoryBuilder"]
