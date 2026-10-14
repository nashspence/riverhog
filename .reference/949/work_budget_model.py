"""Non-authoritative bounded metadata/scheduler witness using standard SQLite.

One fixed configured scope isolates bounds; production must also admit/bound the
number of scopes. Sequential transactions/reopen tests are NOT distributed worker
fencing or crash-kill evidence. Prefix retirement is a witness, not a chosen API.
SQLite page/index/journal overhead is not counted as encoded logical record bytes.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from semantic_model import Deferred, Rejected

MAX_SCALAR = 2**63 - 1
MAX_RECORD_BYTES = 512


class WorkBudget:
    def __init__(self, path: Path | str, *, capacity: int = 16,
                 maximum_pins: int = 2, quantum: int = 4, interval: int = 10):
        for value in (capacity, maximum_pins, quantum, interval):
            if type(value) is not int or not 1 <= value <= MAX_SCALAR:
                raise Rejected("invalid configuration")
        self.db = sqlite3.connect(str(path))
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS control (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                retired INTEGER NOT NULL, used INTEGER NOT NULL,
                capacity INTEGER NOT NULL, max_pins INTEGER NOT NULL,
                quantum INTEGER NOT NULL, interval INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS work (
                ordinal INTEGER PRIMARY KEY, object_ref TEXT NOT NULL,
                terminal INTEGER NOT NULL DEFAULT 0,
                released INTEGER NOT NULL DEFAULT 0,
                pins INTEGER NOT NULL DEFAULT 0, due INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS due_work ON work(due, ordinal);
        """)
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO control VALUES(1,0,0,?,?,?,?)",
                            (capacity, maximum_pins, quantum, interval))
        self.capacity, self.maximum_pins, self.quantum, self.interval = (
            capacity, maximum_pins, quantum, interval)
        if self.db.execute("SELECT capacity,max_pins,quantum,interval FROM control").fetchone() != (
            capacity, maximum_pins, quantum, interval):
            self.db.close()
            raise Rejected("configuration transition is not modeled")
        self.visited = 0
        self.peak_used = self.used
        self.db_ops = 0

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    @property
    def used(self) -> int:
        return self.db.execute("SELECT used FROM control WHERE singleton=1").fetchone()[0]

    @property
    def retired(self) -> int:
        return self.db.execute("SELECT retired FROM control WHERE singleton=1").fetchone()[0]

    def _ordinal(self, ordinal: int) -> None:
        if type(ordinal) is not int or not 1 <= ordinal <= MAX_SCALAR:
            raise Rejected("invalid/exhausted identity; never wrap")

    def admit(self, ordinal: int, object_ref: str) -> None:
        self._ordinal(ordinal)
        if not isinstance(object_ref, str) or not 1 <= len(object_ref.encode()) <= 128:
            raise Rejected("object reference envelope exceeded")
        if len(json.dumps(object_ref, ensure_ascii=False).encode()) > 258:
            raise Rejected("encoded reference envelope exceeded")
        with self.transaction():
            retired, used = self.db.execute(
                "SELECT retired,used FROM control WHERE singleton=1").fetchone()
            if ordinal <= retired:
                raise Rejected("retired identity cannot recreate control debt")
            previous = self.db.execute("SELECT object_ref FROM work WHERE ordinal=?", (ordinal,)).fetchone()
            if previous is not None:
                if previous[0] != object_ref:
                    raise Rejected("identity conflict")
                return
            if used >= self.capacity or ordinal > retired + self.capacity:
                raise Deferred("metadata admission/window capacity")
            self.db.execute("INSERT INTO work(ordinal,object_ref) VALUES(?,?)", (ordinal, object_ref))
            self.db.execute("UPDATE control SET used=used+1 WHERE singleton=1")
        self.peak_used = max(self.peak_used, used + 1)

    def pin(self, ordinal: int) -> None:
        with self.transaction():
            row = self._get(ordinal)
            if row[2] or row[3]:
                raise Rejected("closed admission")
            if row[4] >= self.maximum_pins:
                raise Deferred("pin capacity")
            self.db.execute("UPDATE work SET pins=pins+1 WHERE ordinal=?", (ordinal,))

    def unpin(self, ordinal: int) -> None:
        # Evidence supplied by an authoritative worker closure, NOT by a timer.
        with self.transaction():
            row = self._get(ordinal)
            if row[4] < 1:
                raise Rejected("pin underflow")
            self.db.execute("UPDATE work SET pins=pins-1 WHERE ordinal=?", (ordinal,))

    def finish_parent(self, ordinal: int) -> None:
        with self.transaction():
            self._get(ordinal)
            self.db.execute("UPDATE work SET terminal=1 WHERE ordinal=?", (ordinal,))

    def confirm_release(self, ordinal: int) -> None:
        with self.transaction():
            row = self._get(ordinal)
            if not row[2] or row[4]:
                raise Deferred("parent or stream still active")
            self.db.execute("UPDATE work SET released=1 WHERE ordinal=?", (ordinal,))

    def _get(self, ordinal: int) -> tuple:
        self._ordinal(ordinal)
        row = self.db.execute("SELECT * FROM work WHERE ordinal=?", (ordinal,)).fetchone()
        if row is None:
            raise Rejected("unknown work")
        return row

    def tick(self, now: int, *, limit: int) -> list[int]:
        """Select/update at most B due rows; no provider call/body read/sleep.

        due_work index bounds discovery to eligible work, not whole historical
        jobs. Production must use real claimed generations and cumulative control
        budgets around separately executed effects. Repeated ticks at a fixed
        clock cannot contact the same row before interval; no per-poll history.
        """
        if type(now) is not int or not 0 <= now <= MAX_SCALAR - self.interval:
            raise Rejected("invalid/exhausted clock")
        if type(limit) is not int or not 1 <= limit <= self.quantum:
            raise Rejected("work quantum exceeded")
        with self.transaction():
            rows = self.db.execute(
                "SELECT ordinal FROM work WHERE due<=? ORDER BY due,ordinal LIMIT ?",
                (now, limit)).fetchall()
            for (ordinal,) in rows:
                self.db.execute("UPDATE work SET due=? WHERE ordinal=?", (now + self.interval, ordinal))
        self.visited += len(rows)
        return [row[0] for row in rows]

    def collect(self, *, limit: int) -> int:
        if type(limit) is not int or not 1 <= limit <= self.quantum:
            raise Rejected("retirement quantum exceeded")
        removed = 0
        with self.transaction():
            low = self.retired
            while removed < limit:
                row = self.db.execute("SELECT terminal,released,pins FROM work WHERE ordinal=?", (low + 1,)).fetchone()
                if row != (1, 1, 0):
                    break  # Never skip absent, pinned, or unresolved ownership.
                low += 1
                self.db.execute("DELETE FROM work WHERE ordinal=?", (low,))
                removed += 1
            self.db.execute("UPDATE control SET retired=?,used=used-? WHERE singleton=1", (low, removed))
        return removed

    def encoded_rows(self) -> list[bytes]:
        # Audit operation is explicitly O(Q), NOT the scheduler or a status call.
        return [json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()
                for row in self.db.execute("SELECT * FROM work ORDER BY ordinal")]

    def count_vm_steps(self, operation) -> int:
        steps = 0
        def counted():
            nonlocal steps
            steps += 1
            return 0
        self.db.set_progress_handler(counted, 1)
        try:
            operation()
        finally:
            self.db.set_progress_handler(None, 0)
        self.db_ops += steps
        return steps
