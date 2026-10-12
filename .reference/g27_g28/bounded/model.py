"""Diagnostic pinned-root model; NOT Riverhog schema, crypto or authorization.

Exercises immutable path copying, root retention, bounded resumable work,
transaction release, CAS checkpoints and bounded reclamation in real SQLite.
Payloads are small synthetic values. Native G26 facts/guards and production
streamed storage are deliberately not implemented by this model.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Callable, Iterator


class InvalidView(RuntimeError):
    pass


class LostAttempt(RuntimeError):
    pass


class NotReady(RuntimeError):
    pass


def encoded(value: object) -> bytes:
    # Model-only encoding; never substitute for Riverhog's canonical codecs.
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value: object) -> str:
    return hashlib.sha256(b"diagnostic-only\0" + encoded(value)).hexdigest()


@dataclass(frozen=True)
class Ticket:
    query_id: str
    epoch: int
    version: int
    root: str | None
    stack: tuple[str, ...]
    predicate: str
    count: int
    used: int
    chain: str


@dataclass(frozen=True)
class Unit:
    ticket: Ticket
    stack: tuple[str, ...]
    rows: tuple[tuple[int, str], ...]
    visits: int
    chain: str


class Store:
    def __init__(self, path: str | Path, *, width: int = 63) -> None:
        if not 1 <= width <= 63:
            raise ValueError("model width must fit the test SQL key")
        self.width = width
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS node(
          hash TEXT PRIMARY KEY, kind TEXT NOT NULL, depth INTEGER NOT NULL,
          left_id TEXT, right_id TEXT, item_key INTEGER, value TEXT,
          refs INTEGER NOT NULL DEFAULT 0 CHECK(refs>=0));
        CREATE TABLE IF NOT EXISTS current_view(
          id INTEGER PRIMARY KEY CHECK(id=1), root TEXT, incarnation TEXT NOT NULL,
          width INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS reap(hash TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS query(
          id TEXT PRIMARY KEY, root TEXT, incarnation TEXT NOT NULL,
          principal TEXT NOT NULL, authority TEXT NOT NULL, predicate TEXT NOT NULL,
          phase TEXT NOT NULL, epoch INTEGER NOT NULL, version INTEGER NOT NULL,
          stack TEXT NOT NULL, hits INTEGER NOT NULL, used INTEGER NOT NULL,
          chain TEXT NOT NULL, expires INTEGER NOT NULL, lease_end INTEGER NOT NULL,
          pinned INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS result(
          query_id TEXT NOT NULL, item_key INTEGER NOT NULL, value TEXT NOT NULL,
          PRIMARY KEY(query_id,item_key));
        """)
        self.db.execute("INSERT OR IGNORE INTO current_view VALUES(1,NULL,'source-1',?)", (width,))
        if self.db.execute("SELECT width FROM current_view").fetchone()[0] != width:
            raise ValueError("model width changed")
        self.counters: dict[str, int] = {
            "nodes_read": 0, "nodes_created": 0, "transactions": 0,
            "admission_node_reads": 0, "work_visits": 0, "checkpoint_writes": 0,
            "gc_nodes": 0, "stale_commits": 0,
        }
        self.max_stack = 0

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def tx(self, *, write: bool = False) -> Iterator[None]:
        if self.db.in_transaction:
            raise AssertionError("nested transaction")
        self.counters["transactions"] += 1
        self.db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield
        except BaseException:
            self.db.rollback()
            raise
        else:
            self.db.commit()

    def _node(self, h: str) -> sqlite3.Row:
        row = self.db.execute("SELECT * FROM node WHERE hash=?", (h,)).fetchone()
        if row is None:
            raise InvalidView("immutable model node missing")
        fields = tuple(row[name] for name in ("kind", "depth", "left_id", "right_id", "item_key", "value"))
        if digest(fields) != h:
            raise InvalidView("immutable model digest mismatch")
        self.counters["nodes_read"] += 1
        return row

    def _ref(self, h: str | None, delta: int) -> None:
        if h is None:
            return
        row = self.db.execute("UPDATE node SET refs=refs+? WHERE hash=? RETURNING refs", (delta, h)).fetchone()
        if row is None:
            raise InvalidView("missing retained node")
        if row[0] == 0:
            self.db.execute("INSERT OR IGNORE INTO reap VALUES(?)", (h,))

    def _intern(self, kind: str, depth: int, left: str | None = None,
                right: str | None = None, key: int | None = None,
                value: str | None = None) -> str:
        fields = (kind, depth, left, right, key, value)
        h = digest(fields)
        old = self.db.execute("SELECT kind,depth,left_id,right_id,item_key,value FROM node WHERE hash=?", (h,)).fetchone()
        if old is not None:
            if tuple(old) != fields:
                raise InvalidView("immutable digest collision")
            return h
        self.db.execute("INSERT INTO node VALUES(?,?,?,?,?,?,?,0)", (h, *fields))
        self._ref(left, 1)
        self._ref(right, 1)
        self.counters["nodes_created"] += 1
        return h

    def put(self, key: int, value: str | None, *, fail: bool = False) -> None:
        """One bounded path-copy mutation; None removes a key, never old versions."""
        if type(key) is not int or not 0 <= key < 1 << self.width:
            raise ValueError("key is outside model domain")
        with self.tx(write=True):
            old = self.db.execute("SELECT root FROM current_view").fetchone()[0]
            ancestors = []
            here = old
            for depth in range(self.width):
                row = self._node(here) if here is not None else None
                side = (key >> (self.width - depth - 1)) & 1
                left, right = (row["left_id"], row["right_id"]) if row else (None, None)
                ancestors.append((depth, side, left, right))
                here = right if side else left
            updated = None if value is None else self._intern("leaf", self.width, key=key, value=value)
            for depth, side, left, right in reversed(ancestors):
                if side:
                    right = updated
                else:
                    left = updated
                updated = None if left is None and right is None else self._intern("branch", depth, left, right)
            self._ref(updated, 1)
            self.db.execute("UPDATE current_view SET root=?", (updated,))
            self._ref(old, -1)
            if fail:
                raise RuntimeError("injected rollback")

    def start(self, query_id: str, *, principal: str = "p", authority: str = "a",
              predicate: str = "", expires: int = 100000) -> str | None:
        before = self.counters["nodes_read"]
        with self.tx(write=True):
            existing = self.db.execute("SELECT * FROM query WHERE id=?", (query_id,)).fetchone()
            if existing:
                if (existing["principal"], existing["authority"], existing["predicate"]) != (principal, authority, predicate):
                    raise InvalidView("conflicting admission retry")
                if existing["phase"] in ("CANCELLED", "EXPIRED"):
                    raise InvalidView("terminated admission")
                return existing["root"]
            view = self.db.execute("SELECT * FROM current_view").fetchone()
            self._ref(view["root"], 1)
            stack = [view["root"]] if view["root"] else []
            self.db.execute("INSERT INTO query VALUES(?,?,?,?,?,?,'SCAN',0,0,?,0,0,?,?,0,1)",
                            (query_id, view["root"], view["incarnation"], principal, authority,
                             predicate, json.dumps(stack), digest([]), expires))
        self.counters["admission_node_reads"] += self.counters["nodes_read"] - before
        return view["root"]

    def claim(self, query_id: str, *, now: int, lease: int = 10) -> int:
        with self.tx(write=True):
            q = self.db.execute("SELECT * FROM query WHERE id=?", (query_id,)).fetchone()
            if q is None or q["expires"] <= now or q["phase"] != "SCAN":
                raise InvalidView("query not runnable")
            if q["lease_end"] > now:
                raise LostAttempt("worker still holds lease")
            epoch = q["epoch"] + 1
            self.db.execute("UPDATE query SET epoch=?,lease_end=? WHERE id=?", (epoch, now + lease, query_id))
            return epoch

    def ticket(self, query_id: str, epoch: int, *, now: int) -> Ticket:
        with self.tx():
            q = self.db.execute("SELECT * FROM query WHERE id=?", (query_id,)).fetchone()
            if (q is None or q["epoch"] != epoch or q["lease_end"] <= now
                    or q["expires"] <= now or q["phase"] != "SCAN"):
                raise LostAttempt("stale worker")
            view = self.db.execute("SELECT incarnation FROM current_view").fetchone()[0]
            if q["incarnation"] != view:
                raise InvalidView("source changed")
            return Ticket(query_id, epoch, q["version"], q["root"], tuple(json.loads(q["stack"])),
                          q["predicate"], q["hits"], q["used"], q["chain"])

    def compute(self, ticket: Ticket, *, budget: int,
                observer: Callable[[sqlite3.Row], None] | None = None) -> Unit:
        if budget <= 0:
            raise ValueError("budget must be positive")
        if self.db.in_transaction:
            raise AssertionError("compute entered with live transaction")
        stack = list(ticket.stack)
        rows = []
        visits, chain = 0, ticket.chain
        while stack and visits < budget:
            h = stack.pop()
            with self.tx():
                # The root is retained; each immutable load releases its snapshot.
                row = self._node(h)
            assert not self.db.in_transaction
            if observer:
                observer(row)
            visits += 1
            if row["kind"] == "branch":
                if row["right_id"]:
                    stack.append(row["right_id"])
                if row["left_id"]:
                    stack.append(row["left_id"])
            elif ticket.predicate in row["value"]:
                result = (row["item_key"], row["value"])
                rows.append(result)
                chain = digest([chain, result])
            self.max_stack = max(self.max_stack, len(stack))
        self.counters["work_visits"] += visits
        return Unit(ticket, tuple(stack), tuple(rows), visits, chain)

    def commit(self, unit: Unit, *, now: int, quota_bytes: int = 10**9,
               fail: bool = False) -> str:
        t = unit.ticket
        with self.tx(write=True):
            q = self.db.execute("SELECT * FROM query WHERE id=?", (t.query_id,)).fetchone()
            if (q is None or q["epoch"] != t.epoch or q["version"] != t.version
                    or q["phase"] != "SCAN" or q["lease_end"] <= now or q["expires"] <= now):
                self.counters["stale_commits"] += 1
                raise LostAttempt("checkpoint fenced")
            if q["incarnation"] != self.db.execute("SELECT incarnation FROM current_view").fetchone()[0]:
                raise InvalidView("source changed")
            used = t.used + sum(len(encoded(row)) for row in unit.rows)
            if used > quota_bytes:
                self.db.execute("UPDATE query SET phase='BLOCKED',lease_end=0 WHERE id=?", (t.query_id,))
                return "BLOCKED"
            for key, value in unit.rows:
                previous = self.db.execute("SELECT value FROM result WHERE query_id=? AND item_key=?", (t.query_id, key)).fetchone()
                if previous is not None and previous[0] != value:
                    raise InvalidView("conflicting result replay")
                self.db.execute("INSERT OR IGNORE INTO result VALUES(?,?,?)", (t.query_id, key, value))
            phase = "SCAN" if unit.stack else "READY"
            self.db.execute("UPDATE query SET stack=?,hits=?,used=?,chain=?,phase=?,version=version+1,lease_end=0 WHERE id=?",
                            (json.dumps(unit.stack), t.count + len(unit.rows), used, unit.chain, phase, t.query_id))
            if fail:
                raise RuntimeError("injected checkpoint rollback")
            self.counters["checkpoint_writes"] += 1
        return phase

    def resume_capacity(self, query_id: str) -> None:
        with self.tx(write=True):
            self.db.execute("UPDATE query SET phase='SCAN' WHERE id=? AND phase='BLOCKED'", (query_id,))

    def page(self, query_id: str, *, now: int, principal: str = "p", authority: str = "a",
             after: int = -1, size: int = 20,
             allow: Callable[[int], bool] | None = None) -> tuple[list[tuple[int, str]], bool]:
        if size <= 0:
            raise ValueError("invalid model page size")
        with self.tx():
            q = self.db.execute("SELECT * FROM query WHERE id=?", (query_id,)).fetchone()
            source = self.db.execute("SELECT incarnation FROM current_view").fetchone()[0]
            if (q is None or q["principal"] != principal or q["authority"] != authority
                    or q["incarnation"] != source or q["expires"] <= now
                    or q["phase"] in ("CANCELLED", "EXPIRED")):
                raise InvalidView("no current query authority")
            if q["phase"] != "READY":
                raise NotReady(q["phase"])
            rows = [tuple(row) for row in self.db.execute(
                "SELECT item_key,value FROM result WHERE query_id=? AND item_key>? ORDER BY item_key LIMIT ?",
                (query_id, after, size + 1))]
            if allow and not all(allow(key) for key, _ in rows):
                raise InvalidView("live authorization removed; never skip")
        return rows[:size], len(rows) <= size

    def terminate(self, query_id: str, *, reason: str = "CANCELLED") -> None:
        if reason not in ("CANCELLED", "EXPIRED"):
            raise ValueError("invalid termination")
        with self.tx(write=True):
            q = self.db.execute("SELECT * FROM query WHERE id=?", (query_id,)).fetchone()
            if q is not None and q["pinned"]:
                self._ref(q["root"], -1)
                self.db.execute("UPDATE query SET pinned=0,phase=?,epoch=epoch+1,lease_end=0 WHERE id=?", (reason, query_id))

    def cleanup_results(self, query_id: str, *, limit: int) -> int:
        if limit <= 0:
            raise ValueError("invalid batch")
        with self.tx(write=True):
            q = self.db.execute("SELECT phase FROM query WHERE id=?", (query_id,)).fetchone()
            if q is None or q[0] not in ("CANCELLED", "EXPIRED"):
                raise InvalidView("cannot reap live results")
            keys = [row[0] for row in self.db.execute("SELECT item_key FROM result WHERE query_id=? ORDER BY item_key LIMIT ?", (query_id, limit))]
            self.db.executemany("DELETE FROM result WHERE query_id=? AND item_key=?", [(query_id, key) for key in keys])
            return len(keys)

    def reap(self, *, limit: int) -> int:
        if limit <= 0:
            raise ValueError("invalid batch")
        removed = 0
        with self.tx(write=True):
            for _ in range(limit):
                candidate = self.db.execute("SELECT hash FROM reap ORDER BY hash LIMIT 1").fetchone()
                if candidate is None:
                    break
                h = candidate[0]
                self.db.execute("DELETE FROM reap WHERE hash=?", (h,))
                row = self.db.execute("SELECT * FROM node WHERE hash=?", (h,)).fetchone()
                if row is not None and row["refs"] == 0:
                    self.db.execute("DELETE FROM node WHERE hash=?", (h,))
                    self._ref(row["left_id"], -1)
                    self._ref(row["right_id"], -1)
                    removed += 1
        self.counters["gc_nodes"] += removed
        return removed

    def reset_source(self, incarnation: str) -> None:
        with self.tx(write=True):
            self.db.execute("UPDATE current_view SET incarnation=?", (incarnation,))


def contains_chunks(chunks: Iterator[str], needle: str, *, ascii_fold: bool = False) -> bool:
    """Streaming literal KMP control; state grows with query, not source value."""
    def fold(c: str) -> str:
        return chr(ord(c) + 32) if ascii_fold and "A" <= c <= "Z" else c
    pattern = "".join(map(fold, needle))
    if not pattern:
        return True
    prefix = [0] * len(pattern)
    j = 0
    for i in range(1, len(pattern)):
        while j and pattern[i] != pattern[j]:
            j = prefix[j - 1]
        if pattern[i] == pattern[j]:
            j += 1
        prefix[i] = j
    j = 0
    for chunk in chunks:
        for char in chunk:
            char = fold(char)
            while j and char != pattern[j]:
                j = prefix[j - 1]
            if char == pattern[j]:
                j += 1
            if j == len(pattern):
                return True
    return False
