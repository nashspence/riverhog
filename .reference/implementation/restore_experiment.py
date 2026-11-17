"""Reference experiment: retain one possible paid entry across lost replies.

The small SQLite journal is a test vehicle, not a proposed second product store.
Production must put these fields and CAS operations with the existing retrieval
owner, use the adapter invocation/closure protocol, and reserve receiving space.
This experiment drives the actual AWS policy through a controlled client seam.
It proves no provider behavior, power-loss guarantee, or complete workflow.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from pathlib import Path


class RestoreIdentityChanged(ValueError):
    pass


class RestoreGuard:
    """One durable original, with no transition from entered back to prepared."""

    def __init__(self, path: Path) -> None:
        self.db = sqlite3.connect(path)
        self.statements: list[str] = []
        self.db.set_trace_callback(self.statements.append)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS originals ("
            "call_id TEXT PRIMARY KEY, identity_json TEXT NOT NULL, state TEXT NOT NULL)"
        )
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def accept(self, call_id: str, identity: dict[str, str]) -> None:
        encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"))
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO originals VALUES (?, ?, 'prepared')",
                (call_id, encoded),
            )
            actual = self.db.execute(
                "SELECT identity_json FROM originals WHERE call_id = ?", (call_id,)
            ).fetchone()
            if actual != (encoded,):
                raise RestoreIdentityChanged(call_id)

    def state(self, call_id: str) -> str:
        result = self.db.execute(
            "SELECT state FROM originals WHERE call_id = ?", (call_id,)
        ).fetchone()
        if result is None:
            raise KeyError(call_id)
        return str(result[0])

    def enter(
        self,
        call_id: str,
        *,
        current: Callable[[], None],
        invoke: Callable[[], str],
        lose_commit_reply: bool = False,
    ) -> bool:
        current()
        # CAS is durable before possible external entry. A lost commit reply must
        # not be interpreted as permission to run another call.
        with self.db:
            changed = self.db.execute(
                "UPDATE originals SET state = 'entered' WHERE call_id = ? AND state = 'prepared'",
                (call_id,),
            ).rowcount
        if lose_commit_reply:
            raise OSError("reference: intent commit reply lost before external entry")
        if not changed:
            return False
        readiness = invoke()
        current()
        self.observe(call_id, readiness)
        return True

    def observe(self, call_id: str, readiness: str) -> None:
        if readiness not in {"requested", "ready", "expired"}:
            raise ValueError(readiness)
        before = self.state(call_id)
        if before == "prepared":
            raise ValueError("no original possible entry exists")
        if before == "ready":
            return
        after = "attention_required" if readiness == "expired" else readiness
        # Waiting is not useful new work. No SQL write or commit for an identical
        # observation. Scheduling remains volatile and bounded in the proposal.
        if before != after:
            with self.db:
                self.db.execute(
                    "UPDATE originals SET state = ? WHERE call_id = ? AND state = ?",
                    (after, call_id, before),
                )

    def poll(self, call_id: str, *, current: Callable[[], None], status: Callable[[], str]) -> str:
        current()
        readiness = status()
        current()
        self.observe(call_id, readiness)
        return readiness

    @property
    def update_count(self) -> int:
        return sum(s.lstrip().upper().startswith("UPDATE ") for s in self.statements)

    @property
    def commit_count(self) -> int:
        return sum(s.strip().upper() == "COMMIT" for s in self.statements)
