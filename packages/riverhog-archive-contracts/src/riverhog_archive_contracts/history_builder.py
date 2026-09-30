"""Producer-authored history sets with bounded, disk-backed canonical ordering."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Self

from riverhog_canonical_json import canonical_json_bytes, require_canonical_json

from .member_history import (
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    MemberHistoryBinding,
    MemberHistoryDocument,
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
    """

    def __init__(
        self, *, artifact_id: str, bytes: int, sha256: str, primary: MemberHistoryPrimary
    ) -> None:
        self.member = (artifact_id, bytes, sha256)
        self.primary = primary
        self._scratch = TemporaryDirectory(prefix="riverhog-history-builder-")
        self._db = sqlite3.connect(Path(self._scratch.name) / "selection.sqlite3")
        self._db.execute("PRAGMA cache_size = -512")
        self._db.execute(
            "CREATE TABLE records (domain TEXT, key TEXT, value BLOB NOT NULL, "
            "PRIMARY KEY (domain, key))"
        )
        self._sealed: MemberHistoryDocument | None = None
        self.add_root(MemberHistoryRoot(primary.journal, "bound"))

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self._db.close()
        self._scratch.cleanup()

    def _add(self, domain: str, key: str, value: dict[str, object]) -> None:
        if self._sealed is not None:
            raise ValueError("member history builder is sealed")
        try:
            self._db.execute(
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
        for key, value in self._db.execute(
            "SELECT key, value FROM records WHERE domain = ? ORDER BY key", (domain,)
        ):
            parsed = require_canonical_json(value)
            if not isinstance(parsed, dict):
                raise ValueError("member history record is not an object")
            commitment.update(key, parsed)
        return commitment.ref()

    def seal(self) -> tuple[MemberHistoryBinding, MemberHistoryDocument]:
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
            verify_member_history_sets(
                history,
                root_pages=self._pages(history.roots),
                import_pages=self._pages(history.imports),
            )
            self._sealed = history
        history = self._sealed
        return MemberHistoryBinding(
            history.artifact_id,
            history.bytes,
            history.sha256,
            history.identity,
            len(history.to_json_bytes()),
        ), history

    def _pages(self, authority: RecordSetRef) -> Iterator[RecordPage]:
        rows: list[dict[str, object]] = []
        ordinal = 0
        for key, value in self._db.execute(
            "SELECT key, value FROM records WHERE domain = ? ORDER BY key", (authority.schema_id,)
        ):
            rows.append({"key": key, "value": require_canonical_json(value)})
            if len(rows) == PAGE_RECORDS_MAX:
                yield RecordPage(authority, ordinal, tuple(rows), False)
                ordinal += 1
                rows = []
        if rows:
            yield RecordPage(authority, ordinal, tuple(rows), False)
            ordinal += 1
        yield RecordPage(authority, ordinal, (), True)

    def objects(self) -> Iterator[bytes]:
        _, history = self.seal()
        for authority in (history.roots, history.imports):
            for page in self._pages(authority):
                yield page.to_json_bytes()
        yield history.to_json_bytes()


__all__ = ["MemberHistoryBuilder"]
