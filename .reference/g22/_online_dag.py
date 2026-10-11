"""Disk-backed online cycle detection with level-bounded backward search.

Uses the genuinely online level-fuel algorithm of Gueneau et al. (ITP 2019).
This implementation is independently tested, not covered by their Coq proof.
Private level/frontier tables are disposable; callers own accepted journal bytes.
"""
from __future__ import annotations

import sqlite3
from collections.abc import Callable

from .errors import ProvenanceValidationError


def install(db: sqlite3.Connection) -> None:
    for sql in (
        "CREATE TABLE dag_levels(kind TEXT, vertex TEXT, level TEXT NOT NULL, "
        "PRIMARY KEY(kind, vertex))",
        "CREATE TABLE dag_incoming(kind TEXT, target TEXT, source TEXT, "
        "PRIMARY KEY(kind, target, source))",
        "CREATE TABLE dag_seen(vertex TEXT PRIMARY KEY)",
        "CREATE TABLE dag_queue(vertex TEXT PRIMARY KEY)",
    ):
        db.execute(sql)


class OnlineDag:
    def __init__(self, db: sqlite3.Connection, kind: str,
                 count: Callable[[str, int], None] | None = None) -> None:
        self.db, self.kind, self.count = db, kind, count

    def _count(self, name: str, amount: int = 1) -> None:
        if self.count is not None:
            self.count(name, amount)

    def _level(self, vertex: str) -> int:
        row = self.db.execute("SELECT level FROM dag_levels WHERE kind=? AND vertex=?",
                              (self.kind, vertex)).fetchone()
        assert row is not None
        return int(row[0])

    def _seen(self, vertex: str) -> bool:
        return self.db.execute("SELECT 1 FROM dag_seen WHERE vertex=?", (vertex,)).fetchone() is not None

    def _enqueue(self, vertex: str) -> None:
        self.db.execute("INSERT OR IGNORE INTO dag_queue VALUES (?)", (vertex,))

    def _pop(self) -> str | None:
        row = self.db.execute("SELECT vertex FROM dag_queue LIMIT 1").fetchone()
        if row is None:
            return None
        self.db.execute("DELETE FROM dag_queue WHERE vertex=?", row)
        self._count("queue_pops")
        return str(row[0])

    def _incoming(self, target: str, source: str) -> None:
        self.db.execute("INSERT OR IGNORE INTO dag_incoming VALUES (?,?,?)",
                        (self.kind, target, source))

    def _promote(self, vertex: str, level: int) -> None:
        self.db.execute("UPDATE dag_levels SET level=? WHERE kind=? AND vertex=?",
                        (str(level), self.kind, vertex))
        removed = self.db.execute("DELETE FROM dag_incoming WHERE kind=? AND target=?",
                                  (self.kind, vertex)).rowcount
        self._count("incoming_removed", removed)
        self._count("promotions")

    def add(self, source: str, target: str) -> None:
        self._count("edge_requests")
        if source == target:
            raise ProvenanceValidationError(f"{self.kind} cannot relate an entity to itself")
        if self.db.execute("SELECT 1 FROM edges WHERE kind=? AND source=? AND target=?",
                           (self.kind, source, target)).fetchone():
            self._count("duplicates")
            return
        # A rejected insertion must not poison the caller's previous graph.
        self.db.execute("SAVEPOINT dag_insert")
        try:
            for vertex in (source, target):
                self.db.execute("INSERT OR IGNORE INTO dag_levels VALUES (?,?,?)",
                                (self.kind, vertex, "1"))
            a, b = self._level(source), self._level(target)
            if a >= b:
                self._repair(source, target, a, b)
            self.db.execute("INSERT INTO edges VALUES (?,?,?)", (self.kind, source, target))
            if self._level(source) == self._level(target):
                self._incoming(target, source)
            self.db.execute("DELETE FROM dag_seen")
            self.db.execute("DELETE FROM dag_queue")
            self.db.execute("RELEASE dag_insert")
        except BaseException:
            self.db.execute("ROLLBACK TO dag_insert")
            self.db.execute("RELEASE dag_insert")
            raise

    def _repair(self, source: str, target: str, a: int, b: int) -> None:
        fuel = a
        interrupted = False
        self.db.execute("INSERT INTO dag_seen VALUES (?)", (source,))
        self._enqueue(source)
        while (vertex := self._pop()) is not None:
            for (predecessor,) in self.db.execute(
                "SELECT source FROM dag_incoming WHERE kind=? AND target=?",
                (self.kind, vertex),
            ):
                if fuel == 0:
                    interrupted = True
                    break
                self._count("backward_edges")
                if predecessor == target:
                    raise ProvenanceValidationError(f"{self.kind} graph contains a cycle")
                fuel -= 1
                if not self._seen(predecessor):
                    self.db.execute("INSERT INTO dag_seen VALUES (?)", (predecessor,))
                    self._enqueue(predecessor)
            if interrupted:
                break
        if not interrupted and a == b:
            return
        new_level = a + 1 if interrupted else a
        self.db.execute("DELETE FROM dag_queue")
        self._promote(target, new_level)
        self._enqueue(target)
        while (vertex := self._pop()) is not None:
            for (successor,) in self.db.execute(
                "SELECT target FROM edges WHERE kind=? AND source=?", (self.kind, vertex)
            ):
                self._count("forward_edges")
                if self._seen(successor):
                    raise ProvenanceValidationError(f"{self.kind} graph contains a cycle")
                level = self._level(successor)
                if level < new_level:
                    self._promote(successor, new_level)
                    self._incoming(successor, vertex)
                    self._enqueue(successor)
                elif level == new_level:
                    self._incoming(successor, vertex)
