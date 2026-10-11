"""Look-ahead topological certificates for complete journal validation.

A correction starts a new epoch. Within an epoch no assertion is retracted,
so every actual graph prefix is a subgraph of its final epoch graph. Ranks
accelerate cycle checks only; ordinary prefix semantics still execute in order.
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Self

from riverhog_provenance_contracts import ENTRY_SCHEMA, ContractCatalog

from .graph import iter_assertions
from .journal import JournalFrame, iter_journal_frames


class EpochFrames:
    def __init__(self, chunks: Iterable[bytes], catalog: ContractCatalog) -> None:
        self.upstream = iter(iter_journal_frames(chunks))
        self.catalog = catalog
        self.scratch = TemporaryDirectory(prefix="riverhog-journal-epochs-")
        try:
            self.db = sqlite3.connect(Path(self.scratch.name) / "epochs.sqlite3")
            self.db.execute("PRAGMA synchronous=OFF")
            self.db.execute("PRAGMA cache_size=-512")
            self.db.execute("PRAGMA temp_store=FILE")
            self.db.executescript(
                "CREATE TABLE frames(position INTEGER PRIMARY KEY, value BLOB);"
                "CREATE TABLE active(assertion TEXT PRIMARY KEY, kind TEXT, source TEXT, target TEXT);"
                "CREATE TABLE arcs(kind TEXT, source TEXT, target TEXT, PRIMARY KEY(kind,source,target));"
                "CREATE TABLE ranks(kind TEXT, vertex TEXT, value TEXT, PRIMARY KEY(kind,vertex));"
                "CREATE TABLE degrees(kind TEXT, vertex TEXT, degree INTEGER, PRIMARY KEY(kind,vertex));"
                "CREATE TABLE ready(kind TEXT, vertex TEXT, PRIMARY KEY(kind,vertex));"
            )
        except BaseException:
            if hasattr(self, "db"):
                self.db.close()
            self.scratch.cleanup()
            raise
        self.lookahead: JournalFrame | None = None
        self.certified: set[str] = set()
        self.counts: dict[str, int] = {}

    def _count(self, name: str, n: int = 1) -> None:
        self.counts[name] = self.counts.get(name, 0) + n

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: Any) -> None:
        try:
            close = getattr(self.upstream, "close", None)
            if close is not None:
                close()
        finally:
            try:
                self.db.close()
            finally:
                self.scratch.cleanup()

    def check(self, kind: str, source: str, target: str) -> bool | None:
        if kind not in self.certified:
            return None
        self._count("certificate_checks")
        left = self.db.execute("SELECT value FROM ranks WHERE kind=? AND vertex=?",
                               (kind, source)).fetchone()
        right = self.db.execute("SELECT value FROM ranks WHERE kind=? AND vertex=?",
                                (kind, target)).fetchone()
        return left is not None and right is not None and int(left[0]) < int(right[0])

    def __iter__(self) -> Iterator[JournalFrame]:
        finished = False
        while not finished:
            for table in ("frames", "arcs", "ranks", "degrees", "ready"):
                self.db.execute("DELETE FROM " + table)
            self.certified.clear()
            error: Exception | None = None
            position = 0
            # Do not pre-accept anything. Save a parse/schema failure until the
            # earlier frames have had their ordinary semantic validation.
            while True:
                try:
                    frame = self.lookahead if self.lookahead is not None else next(self.upstream)
                    self.lookahead = None
                except StopIteration:
                    finished = True
                    break
                except Exception as exc:
                    # Source/framing failures are re-raised after replaying earlier
                    # readable frames. No result escapes a late provider fence.
                    error = exc
                    finished = True
                    break
                document = frame.document
                if position and document.get("entry_kind") == "correction":
                    self.lookahead = frame
                    break
                try:
                    self.catalog.validate(ENTRY_SCHEMA, document)
                except (ValueError, TypeError) as exc:
                    # Replay this exact frame so existing validation reports it.
                    self.db.execute("INSERT INTO frames VALUES (?,?)", (position, frame.json_bytes))
                    position += 1
                    error = exc
                    finished = True
                    break
                if not position:
                    if document["entry_kind"] == "correction":
                        for target in document["body"]["retracts"]:
                            self.db.execute("DELETE FROM active WHERE assertion=?", (target["assertion_id"],))
                    self.db.execute("INSERT OR IGNORE INTO arcs SELECT kind,source,target FROM active")
                for _, row in iter_assertions(document["body"].get("assertions", {})):
                    if row["type"] not in ("derivation", "specialization"):
                        continue
                    fields = ("used_state", "generated_state") if row["type"] == "derivation" else ("specific", "general")
                    source, target = (row[field]["object_id"] for field in fields)
                    # This is a speculative certificate builder, never identity
                    # authority. Duplicate IDs and wrong retractions fail in replay.
                    self.db.execute("INSERT OR REPLACE INTO active VALUES (?,?,?,?)",
                                    (row["assertion_id"], row["type"], source, target))
                    self.db.execute("INSERT OR IGNORE INTO arcs VALUES (?,?,?)",
                                    (row["type"], source, target))
                    self._count("staged_edges")
                self.db.execute("INSERT INTO frames VALUES (?,?)", (position, frame.json_bytes))
                position += 1
                self._count("staged_frames")
            if position:
                self._count("epochs")
                self._rank()
                for (raw,) in self.db.execute("SELECT value FROM frames ORDER BY position"):
                    yield JournalFrame(bytes(raw))
            if error is not None:
                raise error

    def _rank(self) -> None:
        for kind in ("derivation", "specialization"):
            self.db.execute("INSERT OR IGNORE INTO degrees SELECT kind,source,0 FROM arcs WHERE kind=?", (kind,))
            self.db.execute("INSERT OR IGNORE INTO degrees SELECT kind,target,0 FROM arcs WHERE kind=?", (kind,))
            for vertex, degree in self.db.execute("SELECT target,count(*) FROM arcs WHERE kind=? GROUP BY target", (kind,)):
                self.db.execute("UPDATE degrees SET degree=? WHERE kind=? AND vertex=?", (degree,kind,vertex))
            self.db.execute("INSERT INTO ready SELECT kind,vertex FROM degrees WHERE kind=? AND degree=0", (kind,))
            count = 0
            while (row := self.db.execute("SELECT vertex FROM ready WHERE kind=? LIMIT 1", (kind,)).fetchone()) is not None:
                vertex = row[0]
                self.db.execute("DELETE FROM ready WHERE kind=? AND vertex=?", (kind,vertex))
                self.db.execute("INSERT INTO ranks VALUES (?,?,?)", (kind,vertex,str(count)))
                count += 1
                self._count("rank_vertices")
                for (target,) in self.db.execute("SELECT target FROM arcs WHERE kind=? AND source=?", (kind,vertex)):
                    self.db.execute("UPDATE degrees SET degree=degree-1 WHERE kind=? AND vertex=?", (kind,target))
                    if self.db.execute("SELECT degree FROM degrees WHERE kind=? AND vertex=?", (kind,target)).fetchone()[0] == 0:
                        self.db.execute("INSERT INTO ready VALUES (?,?)", (kind,target))
                    self._count("rank_edges")
            total = self.db.execute("SELECT count(*) FROM degrees WHERE kind=?", (kind,)).fetchone()[0]
            if count == total:
                self.certified.add(kind)
            # Cyclic/invalid epochs use the ordinary online replay path. Do not
            # report a later graph failure before an earlier semantic failure.
