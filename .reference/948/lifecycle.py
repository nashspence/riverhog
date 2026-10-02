"""Executable design model for #948, NOT a Stove0 implementation.

SQLite makes acceptance/reopening observable. Explicit admission and completion
steps model interleavings; there are no real threads, HTTP, GPUs, or capabilities.
The JSON hashing below is fixture identity, not Riverhog's canonical JSON codec.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path

KINDS = {"observer", "transform", "effect", "departure"}
REPLAY_SAFE = {"observer", "transform"}
TERMINAL = {"complete", "canceled", "failed"}


@dataclass(frozen=True)
class Binding:
    kind: str
    semantic_id: str
    claim_id: str = "fixture-claim"
    fence: int = 1
    descriptor: str = "fixture-descriptor"

    def document(self) -> str:
        if self.kind not in KINDS or self.fence < 1:
            raise ValueError("invalid fixture binding")
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))

    @property
    def key(self) -> str:
        return hashlib.sha256(self.document().encode()).hexdigest()


@dataclass(frozen=True)
class Ticket:
    key: str
    epoch: int
    serial: int
    attempt: int
    authority_expires: float
    probe_expires: float


@dataclass
class Permit:
    """Fake owner-specific handle; release cannot free somebody else's permit."""
    owner: str
    valid: bool = True
    released: bool = False

    def release(self) -> None:
        self.released = True


class Store:
    def __init__(self, path: Path) -> None:
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        with self.db:
            self.db.executescript(
                "CREATE TABLE IF NOT EXISTS runtime (id INTEGER PRIMARY KEY, epoch INTEGER);"
                "INSERT OR IGNORE INTO runtime VALUES (1, 1);"
                "CREATE TABLE IF NOT EXISTS jobs ("
                "key TEXT PRIMARY KEY, binding TEXT NOT NULL, kind TEXT NOT NULL,"
                "state TEXT NOT NULL, attempt INTEGER NOT NULL, result TEXT);"
            )

    @property
    def epoch(self) -> int:
        return self.db.execute("SELECT epoch FROM runtime WHERE id=1").fetchone()[0]

    def get(self, key: str) -> dict:
        row = self.db.execute("SELECT * FROM jobs WHERE key=?", (key,)).fetchone()
        if row is None:
            raise KeyError(key)
        return dict(row)

    def close(self) -> None:
        self.db.close()


class Service:
    """Single-owner state model. A production kernel must add actual CAS/locking.

    Control methods: accept, status, cancel. Extension-local actor methods:
    prepare, admit, finish, resume, recover. No control method executes payload.
    """
    def __init__(self, store: Store, capacity: int = 1, backlog_limit: int = 256) -> None:
        if capacity < 1 or backlog_limit < 1:
            raise ValueError("limits must be positive")
        self.store, self.capacity, self.backlog_limit = store, capacity, backlog_limit
        self.epoch = store.epoch
        self.serial = 0
        self.probes: dict[str, Ticket] = {}
        self.active: dict[str, tuple[Ticket, Permit]] = {}
        self.starts: list[str] = []

    def _owner(self) -> None:
        if self.epoch != self.store.epoch:
            raise RuntimeError("stale service owner")

    def accept(self, binding: Binding, *, key: str | None = None) -> dict:
        self._owner()
        key = binding.key if key is None else key
        if key != binding.key:
            raise ValueError("path identity does not match declaration")
        existing = self.store.db.execute("SELECT binding FROM jobs WHERE key=?", (key,)).fetchone()
        if existing is not None:
            if existing[0] != binding.document():
                raise ValueError("identity conflict")
            return self.status(key)
        count = self.store.db.execute(
            "SELECT count(*) FROM jobs WHERE state NOT IN ('complete','canceled','failed')"
        ).fetchone()[0]
        if count >= self.backlog_limit:
            raise OverflowError("not accepted: extension backlog full")
        with self.store.db:
            self.store.db.execute(
                "INSERT INTO jobs VALUES (?, ?, ?, 'queued', 1, NULL)",
                (key, binding.document(), binding.kind),
            )
        return self.status(key)

    def status(self, key: str) -> dict:
        self._owner()
        return self.store.get(key)

    def cancel(self, key: str) -> dict:
        self._owner()
        row = self.store.get(key)
        state = row["state"]
        stopped_replay_safe = state == "interrupted" and row["kind"] in REPLAY_SAFE
        replacement = (
            "canceled" if state == "queued" or stopped_replay_safe
            else "canceling" if state == "running" else state
        )
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET state=? WHERE key=?", (replacement, key))
        return self.status(key)

    def expire_probes(self, now: float) -> None:
        self._owner()
        self.probes = {key: ticket for key, ticket in self.probes.items()
                       if ticket.probe_expires > now}

    def prepare(self, key: str, *, authority_expires: float, now: float,
                probe_budget: float = 1.0) -> Ticket | None:
        """Reserve only a short local probe/dispatch slot, never a waiting worker."""
        self._owner()
        if probe_budget <= 0:
            raise ValueError("probe budget must be positive")
        self.expire_probes(now)
        row = self.store.get(key)
        if (row["state"] != "queued" or key in self.probes or key in self.active
                or len(self.probes) + len(self.active) >= self.capacity
                or authority_expires <= now):
            return None
        self.serial += 1
        ticket = Ticket(key, self.epoch, self.serial, row["attempt"], authority_expires, now + probe_budget)
        self.probes[key] = ticket
        return ticket

    def admit(self, ticket: Ticket, permit: Permit | None, *, now: float) -> bool:
        """Receive one bounded external probe outcome; None means not yet granted.

        No lease is acquired inside the DB transaction. A canceled/stale/expired
        probe releases only its own grant and cannot start payload execution.
        """
        current = self.probes.get(ticket.key)
        if current == ticket:
            self.probes.pop(ticket.key)
        valid = (self.epoch == self.store.epoch == ticket.epoch and current == ticket)
        row = self.store.get(ticket.key)
        valid = valid and row["state"] == "queued" and row["attempt"] == ticket.attempt
        valid = valid and ticket.authority_expires > now and ticket.probe_expires > now
        if permit is None:
            return False
        if permit.owner != str(ticket):
            # Reject a foreign grant without releasing someone else's resource.
            # A real adapter must diagnose/reconcile the malformed broker reply.
            return False
        valid = valid and permit.valid and not permit.released
        if not valid:
            # A duplicate callback must not release the permit of live execution.
            active = self.active.get(ticket.key)
            if active is None or active[1] is not permit:
                permit.release()
            return False
        with self.store.db:
            self.store.db.execute("UPDATE jobs SET state='running' WHERE key=?", (ticket.key,))
        self.active[ticket.key] = (ticket, permit)
        self.starts.append(ticket.key)
        return True

    def finish(self, ticket: Ticket, *, outcome: str, result: str | None = None,
               workers_stopped: bool, no_effect: bool = False,
               completion_proven: bool = False) -> dict:
        """completion_proven is a fixture input, not a proposed wire/API flag.

        It stands for exact terminal evidence independently validated under the
        owning operation's rules. A permit is not archive publication authority.
        """
        self._owner()
        if not workers_stopped:
            raise ValueError("do not release resources before execution has stopped")
        if outcome not in {"complete", "failed", "canceled", "uncertain"}:
            raise ValueError("invalid outcome")
        row = self.store.get(ticket.key)
        if row["state"] in TERMINAL:
            if row["state"] != outcome or row["result"] != result:
                raise ValueError("terminal evidence is immutable")
            return row
        active = self.active.get(ticket.key)
        if active is None or active[0] != ticket or ticket.epoch != self.epoch:
            raise ValueError("stale completion")
        if (outcome == "complete") != (result is not None):
            raise ValueError("completion requires result; pending is not evidence")
        if outcome in {"canceled", "failed"} and row["kind"] not in REPLAY_SAFE and not no_effect:
            raise ValueError("no proof of absence of an effect")
        if not active[1].valid and outcome == "complete" and not completion_proven:
            raise ValueError("lost lease is not completion authority")
        with self.store.db:
            self.store.db.execute(
                "UPDATE jobs SET state=?, result=? WHERE key=?", (outcome, result, ticket.key)
            )
        self.active.pop(ticket.key)[1].release()
        return self.status(ticket.key)

    def recover(self, *, workers_stopped: bool) -> None:
        """Simulated crash recovery, requiring external proof old consumers stopped.

        This is NOT a claim that process death, lease expiry, or a TTL proves it.
        There are no separate completion checkpoints in this model. Production
        recovery must reconcile those first, before settling cancellation.
        """
        self._owner()
        if not workers_stopped:
            raise ValueError("recovery needs an exclusive owner and stopped old workers")
        for _, permit in self.active.values():
            permit.release()
        self.active.clear()
        self.probes.clear()
        with self.store.db:
            self.store.db.execute("UPDATE runtime SET epoch=epoch+1 WHERE id=1")
            self.store.db.execute(
                "UPDATE jobs SET state=CASE "
                "WHEN state='canceling' AND kind IN ('observer','transform') THEN 'canceled' "
                "WHEN kind IN ('observer','transform') THEN 'interrupted' "
                "ELSE 'uncertain' END WHERE state IN ('running','canceling')"
            )
        # Caller constructs a new service. This owner intentionally becomes stale.

    def resume(self, key: str) -> dict:
        self._owner()
        row = self.store.get(key)
        if row["state"] != "interrupted" or row["kind"] not in REPLAY_SAFE:
            raise ValueError("no generic replay for an uncertain external effect")
        with self.store.db:
            self.store.db.execute(
                "UPDATE jobs SET state='queued', attempt=attempt+1 WHERE key=?", (key,)
            )
        return self.status(key)
