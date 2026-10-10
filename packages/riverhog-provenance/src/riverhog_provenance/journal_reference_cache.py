"""Bounded reuse of documentary addresses from freshly read exact journal bytes."""

from __future__ import annotations

import hashlib
import shutil
import sqlite3
import weakref
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryFile, mkdtemp
from threading import Lock
from typing import Any, BinaryIO

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_provenance_contracts import ExternalReference

from .errors import ProvenanceValidationError
from .graph import iter_assertions
from .journal import iter_journal_frames


class _ReferenceIndex:
    """Immutable documentary coordinates, without effective or causal graph authority."""

    def __init__(self, source: BinaryIO) -> None:
        directory = Path(mkdtemp(prefix="riverhog-journal-reference-"))
        self._cleanup = weakref.finalize(self, shutil.rmtree, directory)
        self.path = directory / "references.sqlite3"
        self.journal_id: str | None = None
        try:
            with closing(sqlite3.connect(self.path)) as db:
                db.executescript(
                    "PRAGMA synchronous = OFF; PRAGMA cache_size = -32; "
                    "CREATE TABLE assertions(entry TEXT, assertion TEXT, sequence TEXT, "
                    "json_sha256 TEXT, object TEXT, type TEXT, prefix_sha256 TEXT, "
                    "prefix_bytes TEXT, PRIMARY KEY(entry, assertion));"
                )
                digest = hashlib.sha256()
                size = 0
                for frame in iter_journal_frames(iter(lambda: source.read(64 * 1024), b"")):
                    document = frame.document
                    if self.journal_id is None:
                        self.journal_id = document["journal_id"]
                    if document["journal_id"] != self.journal_id:
                        raise ProvenanceValidationError("foreign journal identity changed")
                    digest.update(frame.encoded)
                    size += len(frame.encoded)
                    for _, row in iter_assertions(document["body"].get("assertions", {})):
                        if "id" in row:
                            db.execute(
                                "INSERT INTO assertions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (
                                    document["id"],
                                    row["assertion_id"],
                                    document["sequence"],
                                    frame.sha256,
                                    row["id"],
                                    row["type"],
                                    digest.hexdigest(),
                                    str(size),
                                ),
                            )
                db.commit()
                self.cost = (
                    db.execute("PRAGMA page_count").fetchone()[0]
                    * db.execute("PRAGMA page_size").fetchone()[0]
                    + 32 * 1024
                )
        except BaseException:
            self._cleanup()
            raise

    def lookup(self, selected: Mapping[str, Any]) -> HistoryJournalAnchor:
        if selected["journal_id"] != self.journal_id:
            raise ProvenanceValidationError("foreign journal identity changed")
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA cache_size = -32")
            row = db.execute(
                "SELECT sequence, json_sha256, object, type, prefix_sha256, prefix_bytes "
                "FROM assertions WHERE entry = ? AND assertion = ?",
                (selected["entry"]["entry_id"], selected["assertion_id"]),
            ).fetchone()
        if row is None:
            raise ProvenanceValidationError("required foreign entry or assertion is absent")
        if row[:4] != (
            selected["entry"]["sequence"],
            selected["entry"]["json_sha256"],
            selected["object_id"],
            selected["object_type"],
        ):
            raise ProvenanceValidationError("foreign reference differs from its exact assertion")
        return HistoryJournalAnchor.from_mapping(
            {
                "journal_id": self.journal_id,
                "through": selected["entry"],
                "prefix_sha256": row[4],
                "prefix_bytes": row[5],
            }
        )


class JournalReferenceCache:
    """Share bounded address projections, never history selection or read authority.

    Each lookup consumes and hashes its current authenticated provider completely.
    The coordinates include documentary retracted assertions; graph validation and
    member/causal traversal remain separate. Larger valid journals bypass capacity.
    """

    def __init__(self, *, byte_budget: int = 64 * 1024 * 1024, entry_budget: int = 1024) -> None:
        if byte_budget < 0 or entry_budget < 0:
            raise ValueError("reference cache capacities cannot be negative")
        self.byte_budget = byte_budget
        self.entry_budget = entry_budget
        self._entries: OrderedDict[tuple[str, int], _ReferenceIndex] = OrderedDict()
        self._bytes = 0
        self._lock = Lock()

    def reference_anchor(
        self,
        chunks: Iterable[bytes],
        reference: Mapping[str, Any],
        *,
        expected_identity: tuple[str, str] | None = None,
    ) -> tuple[HistoryJournalAnchor, tuple[str, str]]:
        selected = ExternalReference.model_validate(reference).model_dump(mode="json")
        source = iter(chunks)
        with TemporaryFile() as spool:
            digest = hashlib.sha256()
            size = 0
            try:
                for chunk in source:
                    digest.update(chunk)
                    size += len(chunk)
                    spool.write(chunk)
            finally:
                close = getattr(source, "close", None)
                if close is not None:
                    close()
            identity = (digest.hexdigest(), str(size))
            if expected_identity is not None and identity != expected_identity:
                raise ProvenanceValidationError("canonical reference journal identity changed")
            key = (identity[0], size)
            with self._lock:
                index = self._entries.get(key)
                if index is not None:
                    self._entries.move_to_end(key)
            if index is None:
                spool.seek(0)
                index = _ReferenceIndex(spool)
                self._put(key, index)
            return index.lookup(selected), identity

    def _put(self, key: tuple[str, int], index: _ReferenceIndex) -> None:
        if index.cost > self.byte_budget or self.entry_budget == 0:
            return
        with self._lock:
            if key in self._entries:
                self._entries.move_to_end(key)
                return
            while self._entries and (
                len(self._entries) >= self.entry_budget
                or self._bytes + index.cost > self.byte_budget
            ):
                _, old = self._entries.popitem(last=False)
                self._bytes -= old.cost
            self._entries[key] = index
            self._bytes += index.cost
