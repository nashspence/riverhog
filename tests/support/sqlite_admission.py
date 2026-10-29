"""Real SQLite connection admission and confirmed-close accounting for owner tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

_CONNECT = sqlite3.connect


class Capacity(RuntimeError):
    pass


class Population:
    """Count real opens and release capacity only on confirmed connection close."""

    def __init__(self, limit=3):
        self.limit = limit
        self.active = self.peak = 0
        self.attempts, self.connections = [], []
        self.refuse = self.unreturned = False
        self.fail_schema = False

    def __call__(self, database, **options):
        self.attempts.append((Path(database), options))
        if self.refuse or self.active >= self.limit:
            raise Capacity("connection population exhausted")
        self.active += 1
        self.peak = max(self.peak, self.active)
        owner = self

        class Connection(sqlite3.Connection):
            released = False
            close_failures = 0
            close_after = False
            close_calls = 0

            def execute(self, statement, parameters=()):
                if owner.fail_schema and statement.startswith("CREATE TABLE"):
                    raise InterruptedError("schema initialization interrupted")
                return super().execute(statement, parameters)

            def close(self):
                self.close_calls += 1
                if self.close_failures:
                    self.close_failures -= 1
                    if self.close_after:
                        super().close()
                    raise OSError("connection close unconfirmed")
                super().close()
                if not self.released:
                    self.released = True
                    owner.active -= 1

        db = _CONNECT(database, factory=Connection, **options)
        self.connections.append(db)
        if self.unreturned:
            raise OSError("connection construction unconfirmed")
        return db
