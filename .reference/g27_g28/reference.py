"""Non-authoritative G27/G28 algorithms and SQLite lifecycle model.

The matcher and non-null seek call the actual inspected Riverhog dependencies.
ProjectionStore is an executable state-machine model, NOT a product query engine,
canonical read-identity implementation, authorization service or production DDL.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from sqlalchemy import and_, asc, desc, tuple_
from riverhog_core.browse import keyset_statement
from riverhog_core.canonical_discovery_extraction import (
    assertion_postings, declared_profile_sections, predicate_matches,
)


def seek(statement: Any, *, columns: Sequence[Any], position: Sequence[Any] | None,
         order: str, page_size: int, nonnull: bool) -> Any:
    """Explicit owner proof opts into tuple seeking; preserve native validation.

    Do not infer non-nullness from the cursor or an outer-joined ORM column.
    The original helper is retained for nullable/unproven expressions.
    """
    checked = keyset_statement(statement, columns=columns, position=position,
                               order=order, page_size=page_size)
    if not nonnull or (position is not None and any(x is None for x in position)):
        return checked
    if position is not None:
        boundary = tuple_(*columns)
        predicate = boundary > tuple(position) if order == "asc" else boundary < tuple(position)
        statement = statement.where(predicate)
    # Explicit non-null proof makes NULLS modifiers redundant; SQLite may sort them.
    ordering = [asc(c) if order == "asc" else desc(c) for c in columns]
    return statement.order_by(*ordering).limit(page_size + 1)


def seek_partitions(statement: Any, *, columns: Sequence[Any], position: Sequence[Any] | None,
                    order: str, page_size: int, nonnull: bool) -> tuple[Any, ...]:
    """Disjoint ordered prefix ranges for owners whose tuple plan only partly seeks.

    Consume ranges in this order, stopping at page_size+1 combined rows. Each
    query has at most page_size+1 rows. Execute in the same request snapshot.
    """
    checked = keyset_statement(statement, columns=columns, position=position,
                               order=order, page_size=page_size)
    if not nonnull or (position is not None and any(x is None for x in position)):
        return (checked,)
    ordering = [asc(c) if order == "asc" else desc(c) for c in columns]
    if position is None:
        return (statement.order_by(*ordering).limit(page_size + 1),)
    return tuple(statement.where(and_(
        *(columns[j] == position[j] for j in range(i)),
        columns[i] > position[i] if order == "asc" else columns[i] < position[i]
    )).order_by(*ordering).limit(page_size + 1) for i in reversed(range(len(columns))))


def clause_matches_once(row: Mapping[str, Any], clause: Any, *, assertion_state: str,
                        counts: dict[str, int] | None = None) -> tuple[Any, ...] | None:
    """One posting pass; preserve first section and first posting per predicate.

    The maintained core's declared profile data slots are disjoint. A character
    trie avoids scanning every section (or repeatedly slicing long pointers).
    Retained winners are O(sections * predicates), not all section/posting pairs.
    This still receives one materialized assertion; it is not a streaming parser.
    """
    counts = counts if counts is not None else {}
    def tick(name: str, value: int = 1) -> None:
        counts[name] = counts.get(name, 0) + value

    if clause.kind is not None and row["type"] != clause.kind:
        return None
    if clause.assertion_state != "any" and clause.assertion_state != assertion_state:
        return None
    trie: dict[Any, Any] = {}
    groups = 1
    if clause.profile is not None:
        groups = 0
        for section in declared_profile_sections(row):
            tick("sections")
            if section.profile != clause.profile:
                continue
            node = trie
            for char in section.pointer + "/data":
                tick("trie_insert_chars")
                node = node.setdefault(char, {})
            if None in node:
                raise ValueError("duplicate core profile data slot")
            node[None] = groups
            groups += 1
    found: list[list[Any | None]] = [[None] * len(clause.values) for _ in range(groups)]
    for posting in assertion_postings(row):
        tick("postings")
        group = 0
        if clause.profile is not None:
            node, group = trie, -1
            for offset, char in enumerate(posting.pointer):
                tick("trie_lookup_chars")
                child = node.get(char)
                if child is None:
                    break
                node = child
                end = offset + 1
                if None in node and (end == len(posting.pointer) or posting.pointer[end] == "/"):
                    group = node[None]
                    break
            if group < 0:
                continue
        for index, predicate in enumerate(clause.values):
            if found[group][index] is None:
                tick("predicate_calls")
                if predicate_matches(posting, predicate):
                    found[group][index] = posting
    for winners in found:  # native declared-section order, NOT first completion
        if all(posting is not None for posting in winners):
            return tuple(winners)
    return None


class ReadChanged(RuntimeError):
    pass


class Unavailable(RuntimeError):
    pass


class LostLease(RuntimeError):
    pass


class RebuildRequired(Unavailable):
    """Internal cache miss; service rebuilds at the unchanged public identity."""
    pass


class ProjectionStore:
    """Disk result/fence model with transactionally updated coarse invalidation.

    Model inputs are already-selected catalog transcript rows. Production must
    supply the native canonical transcript, real authority checks and G26 query
    evaluation. Never adopt this JSON model hash as a Riverhog wire identity.
    """
    def __init__(self, path: str = ":memory:") -> None:
        self.db = sqlite3.connect(path)
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS fence(id INTEGER PRIMARY KEY CHECK(id=1),
          source TEXT NOT NULL, revision INTEGER NOT NULL);
        INSERT OR IGNORE INTO fence VALUES(1,'model-source',0);
        CREATE TABLE IF NOT EXISTS catalog(id INTEGER PRIMARY KEY, root TEXT NOT NULL,
          generation TEXT, tag_revision INTEGER NOT NULL, description_revision INTEGER NOT NULL);
        CREATE TRIGGER IF NOT EXISTS catalog_insert AFTER INSERT ON catalog BEGIN
          UPDATE fence SET revision=revision+1 WHERE id=1; END;
        CREATE TRIGGER IF NOT EXISTS catalog_update AFTER UPDATE ON catalog BEGIN
          UPDATE fence SET revision=revision+1 WHERE id=1; END;
        CREATE TRIGGER IF NOT EXISTS catalog_delete AFTER DELETE ON catalog BEGIN
          UPDATE fence SET revision=revision+1 WHERE id=1; END;
        CREATE TABLE IF NOT EXISTS workspace(key TEXT PRIMARY KEY, query TEXT NOT NULL,
          principal TEXT NOT NULL, read_id TEXT NOT NULL, epoch INTEGER NOT NULL,
          source TEXT NOT NULL, attempt INTEGER NOT NULL, phase TEXT NOT NULL,
          expires INTEGER NOT NULL, row_count INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS result(key TEXT NOT NULL, attempt INTEGER NOT NULL,
          collection_id INTEGER NOT NULL, artifact_id TEXT NOT NULL, support TEXT NOT NULL,
          PRIMARY KEY(key,attempt,collection_id,artifact_id));
        ''')
        self.counts: dict[str, int] = {"manifest_rows": 0, "validations": 0}

    def close(self) -> None:
        self.db.close()

    def put_catalog(self, cid: int, *, generation: str = "g", tag: int = 0) -> None:
        with self.db:
            self.db.execute("INSERT INTO catalog VALUES(?,?,?,?,0) ON CONFLICT(id) "
                            "DO UPDATE SET generation=excluded.generation,tag_revision=excluded.tag_revision",
                            (cid, f"root-{cid}", generation, tag))

    def stamp(self) -> tuple[str, int]:
        return self.db.execute("SELECT source,revision FROM fence WHERE id=1").fetchone()

    def identity(self, query: str, principal: str, selected: tuple[int, ...] | None) -> str:
        # Length-framed compact model transcript. NOT RFC8785/product identity.
        source, _ = self.stamp()
        h = hashlib.sha256(json.dumps([source, query, principal]).encode())
        if selected is None:
            rows = self.db.execute("SELECT * FROM catalog ORDER BY id")
        else:
            rows = self.db.execute("SELECT * FROM catalog WHERE id IN (" +
                                   ",".join("?" for _ in selected) + ") ORDER BY id", selected)
        for row in rows:
            self.counts["manifest_rows"] += 1
            encoded = json.dumps(row).encode()
            h.update(len(encoded).to_bytes(8, "big")); h.update(encoded)
        return h.hexdigest()

    def start(self, key: str, query: str, principal: str, *, selected: tuple[int, ...] | None = None,
              expected_read: str | None = None, expires: int = 1000) -> tuple[int, str]:
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            rid = self.identity(query, principal, selected)
            if expected_read is not None and expected_read != rid:
                raise ReadChanged()
            prior = self.db.execute("SELECT attempt FROM workspace WHERE key=?", (key,)).fetchone()
            attempt = 1 if prior is None else prior[0] + 1
            source, epoch = self.stamp()
            self.db.execute("DELETE FROM result WHERE key=?", (key,))
            self.db.execute("INSERT INTO workspace VALUES(?,?,?,?,?,?,?,?,?,0) ON CONFLICT(key) "
                            "DO UPDATE SET query=excluded.query,principal=excluded.principal,"
                            "read_id=excluded.read_id,epoch=excluded.epoch,source=excluded.source,"
                            "attempt=excluded.attempt,phase='building',expires=excluded.expires,row_count=0",
                            (key, query, principal, rid, epoch, source, attempt, "building", expires))
            return attempt, rid

    def append(self, key: str, attempt: int, rows: Sequence[tuple[int, str, str]], *, max_rows: int) -> None:
        with self.db:
            live = self.db.execute("SELECT attempt,phase FROM workspace WHERE key=?", (key,)).fetchone()
            if live != (attempt, "building"):
                raise LostLease()
            added = 0
            for cid, aid, support in rows:
                prior = self.db.execute("SELECT support FROM result WHERE key=? AND attempt=? AND "
                                        "collection_id=? AND artifact_id=?", (key, attempt, cid, aid)).fetchone()
                if prior is not None and prior[0] != support:
                    raise ValueError("conflicting replay")
                cursor = self.db.execute("INSERT OR IGNORE INTO result VALUES(?,?,?,?,?)", (key, attempt, cid, aid, support))
                added += cursor.rowcount
            self.db.execute("UPDATE workspace SET row_count=row_count+? WHERE key=?", (added,key))
            count = self.db.execute("SELECT row_count FROM workspace WHERE key=?", (key,)).fetchone()[0]
            if count > max_rows:
                raise Unavailable("capacity: never publish a truncated projection")

    def seal(self, key: str, attempt: int, *, selected: tuple[int, ...] | None = None,
             terminal: bool, expected_count: int) -> None:
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            row = self.db.execute("SELECT * FROM workspace WHERE key=?", (key,)).fetchone()
            if row is None or row[6:8] != (attempt, "building"):
                raise LostLease()
            if not terminal:
                raise Unavailable("complete traversal witness missing")
            source, epoch = self.stamp()
            if (source, epoch) != (row[5], row[4]):
                if self.identity(row[1], row[2], selected) != row[3]:
                    raise ReadChanged()
                raise RebuildRequired("snapshot changed during preparation; restart before publication")
            count = self.db.execute("SELECT COUNT(*) FROM result WHERE key=? AND attempt=?", (key, attempt)).fetchone()[0]
            if count != expected_count:
                raise Unavailable("result count differs")
            self.db.execute("UPDATE workspace SET phase='ready',epoch=?,source=?,row_count=? WHERE key=?",
                            (epoch, source, count, key))

    def page(self, key: str, query: str, principal: str, *, authorized: bool,
             expected_read: str, selected: tuple[int, ...] | None = None,
             after: tuple[int, str] = (0, ""), size: int = 2, now: int = 0) -> dict[str, Any]:
        if not authorized:
            raise PermissionError("live authority was checked even on a cache hit")
        if size < 1:
            raise ValueError("invalid page size")
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")  # model only; product read path must not serialize all readers
            row = self.db.execute("SELECT * FROM workspace WHERE key=?", (key,)).fetchone()
            if row is None or row[7] != "ready" or row[8] < now:
                raise Unavailable("rebuild needed; not an empty successful page")
            if row[1:3] != (query, principal) or expected_read != row[3]:
                raise ReadChanged()
            self.counts["validations"] += 1
            source, epoch = self.stamp()
            if (source, epoch) != (row[5], row[4]):
                if self.identity(query, principal, selected) != expected_read:
                    raise ReadChanged()
                raise RebuildRequired("epoch changed but native-equivalent model transcript did not; rebuild, not 409")
            rows = self.db.execute("SELECT collection_id,artifact_id,support FROM result WHERE key=? "
                                   "AND attempt=? AND (collection_id,artifact_id)>(?,?) "
                                   "ORDER BY collection_id,artifact_id LIMIT ?",
                                   (key, row[6], *after, size + 1)).fetchall()
            more = len(rows) > size
            items = rows[:size]
            return {"items": items, "complete": not more,
                    "next": tuple(items[-1][:2]) if more else None}
