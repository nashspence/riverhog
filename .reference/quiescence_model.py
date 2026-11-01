"""Reference-only SQLite state machine; not Riverhog code or a provider guarantee.

ClosedEffect represents trusted evidence from the model driver. Real adapters must
establish execution closure; observation, timeout and process death cannot mint it.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class Conflict(RuntimeError):
    pass


@dataclass(frozen=True)
class Ticket:
    scope: str
    sequence: int
    epoch: int
    worker: str
    descriptor: str


@dataclass(frozen=True)
class ClosedEffect:
    ticket: Ticket
    outcome: str
    result: str


class Ledger:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path, timeout=1, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
        PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS construction(
            scope TEXT PRIMARY KEY, epoch INTEGER NOT NULL DEFAULT 1,
            gate TEXT NOT NULL DEFAULT 'open', next_call INTEGER NOT NULL DEFAULT 0,
            sealed INTEGER NOT NULL DEFAULT 0, claim_live INTEGER NOT NULL DEFAULT 0,
            terminal TEXT, owner_present INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS effects(
            scope TEXT NOT NULL, sequence INTEGER NOT NULL, epoch INTEGER NOT NULL,
            worker TEXT NOT NULL, descriptor TEXT NOT NULL, state TEXT NOT NULL,
            result TEXT, PRIMARY KEY(scope,sequence));
        CREATE INDEX IF NOT EXISTS unsettled ON effects(scope,state,sequence);
        CREATE TABLE IF NOT EXISTS terminal(
            id TEXT PRIMARY KEY, scope TEXT NOT NULL, request TEXT NOT NULL,
            kind TEXT NOT NULL, sealed_snapshot INTEGER NOT NULL,
            phase TEXT NOT NULL DEFAULT 'draining', result TEXT,
            provider_empty INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS retained(
            scope TEXT NOT NULL, ordinal INTEGER NOT NULL, PRIMARY KEY(scope,ordinal));
        ''')

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        else:
            self.db.execute("COMMIT")

    def construction(self, scope: str) -> sqlite3.Row:
        row = self.db.execute("SELECT * FROM construction WHERE scope=?", (scope,)).fetchone()
        if row is None:
            raise Conflict("unknown construction")
        return row

    def create(self, scope: str, rows: int = 1) -> None:
        with self.tx():
            self.db.execute("INSERT INTO construction(scope) VALUES (?)", (scope,))
            self.db.executemany("INSERT INTO retained VALUES (?,?)", ((scope, i) for i in range(rows)))

    def prepare(self, scope: str, worker: str, descriptor: dict) -> Ticket:
        """The model descriptor includes original crypto/plan identity, before begin."""
        encoded = canonical(descriptor)
        with self.tx():
            c = self.construction(scope)
            if c['gate'] != 'open':
                raise Conflict("admissions closed")
            n = c['next_call']
            self.db.execute("UPDATE construction SET next_call=next_call+1 WHERE scope=?", (scope,))
            self.db.execute("INSERT INTO effects VALUES (?,?,?,?,?,'ready',NULL)",
                            (scope, n, c['epoch'], worker, encoded))
            return Ticket(scope, n, c['epoch'], worker, encoded)

    def _effect(self, ticket: Ticket) -> sqlite3.Row:
        e = self.db.execute("SELECT * FROM effects WHERE scope=? AND sequence=?",
                            (ticket.scope, ticket.sequence)).fetchone()
        if e is None or (e['epoch'], e['worker'], e['descriptor']) != (
                ticket.epoch, ticket.worker, ticket.descriptor):
            raise Conflict("effect identity changed")
        return e

    def arm(self, ticket: Ticket, *, maximum_live: int = 8) -> None:
        with self.tx():
            c, e = self.construction(ticket.scope), self._effect(ticket)
            if c['gate'] != 'open' or c['epoch'] != ticket.epoch or e['state'] != 'ready':
                raise Conflict("not a new admitted invocation")
            # LIMIT makes the model capacity probe bounded by the explicit grant.
            live = self.db.execute("SELECT sequence FROM effects WHERE scope=? AND state='armed' "
                                   "ORDER BY sequence LIMIT ?", (ticket.scope, maximum_live)).fetchall()
            if len(live) == maximum_live:
                raise Conflict("unresolved-effect capacity")
            self.db.execute("UPDATE effects SET state='armed' WHERE scope=? AND sequence=?",
                            (ticket.scope, ticket.sequence))

    def settle(self, evidence: ClosedEffect, *, fail_before_commit: bool = False) -> None:
        t = evidence.ticket
        if evidence.outcome not in {'no-effect', 'begun', 'part', 'sealed'}:
            raise Conflict("not execution-closure evidence")
        encoded = canonical({'outcome': evidence.outcome, 'result': evidence.result})
        with self.tx():
            e = self._effect(t)
            if e['state'] == 'settled':
                if e['result'] != encoded:
                    raise Conflict("confirmed outcome changed")
                return
            if e['state'] != 'armed':
                raise Conflict("no potentially executing invocation")
            # Reporting an exact older outcome is allowed even after gate closure.
            self.db.execute("UPDATE effects SET state='settled',result=? WHERE scope=? AND sequence=?",
                            (encoded, t.scope, t.sequence))
            if evidence.outcome == 'sealed':
                self.db.execute("UPDATE construction SET sealed=1 WHERE scope=?", (t.scope,))
            if fail_before_commit:
                raise OSError("injected checkpoint transaction failure")

    def observe(self, ticket: Ticket, *, sealed: bool = False) -> None:
        """An observation is NOT evidence that an earlier invocation cannot still act."""
        with self.tx():
            self._effect(ticket)
            if sealed:
                self.db.execute("UPDATE construction SET sealed=1 WHERE scope=?", (ticket.scope,))

    def pending(self, scope: str, limit: int = 3) -> list[int]:
        if limit <= 0:
            raise ValueError("positive quantum required")
        return [r[0] for r in self.db.execute(
            "SELECT sequence FROM effects WHERE scope=? AND state='armed' ORDER BY sequence LIMIT ?",
            (scope, limit))]

    def select_terminal(self, scope: str, kind: str, *, principal: str = 'authorized-owner',
                        challenge: str = '', expires: int = 100, now: int = 1) -> str:
        if kind not in {'cancel', 'discard'}:
            raise ValueError(kind)
        request = canonical({'scope': scope, 'kind': kind, 'principal': principal,
                             'challenge': challenge, 'expires': expires if kind == 'discard' else None})
        key = digest(request)
        with self.tx():
            c = self.construction(scope)
            # A selected exact request is resumed before checking its original TTL.
            old = self.db.execute("SELECT * FROM terminal WHERE id=?", (key,)).fetchone()
            if old:
                if old['request'] != request:
                    raise Conflict("request preimage mismatch")
                return key
            if c['terminal'] is not None or c['gate'] != 'open':
                raise Conflict("different terminal selection")
            if kind == 'discard' and (not challenge or now > expires or c['claim_live']):
                raise Conflict("discard guards failed")
            if kind == 'cancel' and c['sealed']:
                raise Conflict("sealed custody cannot be canceled")
            self.db.execute("INSERT INTO terminal(id,scope,request,kind,sealed_snapshot) VALUES (?,?,?,?,?)",
                            (key, scope, request, kind, c['sealed']))
            self.db.execute("UPDATE construction SET gate='closed',epoch=epoch+1,terminal=? WHERE scope=?",
                            (key, scope))
        return key

    def adjudicate(self, key: str) -> str:
        with self.tx():
            t = self.db.execute("SELECT * FROM terminal WHERE id=?", (key,)).fetchone()
            c = self.construction(t['scope'])
            if t['phase'] != 'draining':
                return t['phase']
            if self.pending(t['scope'], 1):
                return 'draining'
            if t['kind'] == 'cancel' and c['sealed']:
                phase = 'refused-sealed'
            elif t['kind'] == 'discard' and (c['claim_live'] or c['sealed'] != t['sealed_snapshot']):
                phase = 'needs-reconfirmation'
            else:
                phase = 'provider-cleanup'
            self.db.execute("UPDATE terminal SET phase=? WHERE id=?", (phase, key))
            return phase

    def confirm_provider_empty(self, key: str) -> None:
        """Model-only trusted cleanup-driver evidence, never a caller-provided bool."""
        with self.tx():
            t = self.db.execute("SELECT * FROM terminal WHERE id=?", (key,)).fetchone()
            if t['phase'] != 'provider-cleanup' or self.pending(t['scope'], 1):
                raise Conflict("not quiescent and authorized for cleanup")
            self.db.execute("UPDATE terminal SET provider_empty=1,phase='catalog-cleanup' WHERE id=?", (key,))

    def cleanup(self, key: str, limit: int = 3, *, fail: bool = False) -> int:
        if limit < 1:
            raise ValueError("positive cleanup grant required")
        with self.tx():
            t = self.db.execute("SELECT * FROM terminal WHERE id=?", (key,)).fetchone()
            if t['phase'] == 'done':
                return 0
            if t['phase'] != 'catalog-cleanup' or not t['provider_empty'] or self.pending(t['scope'], 1):
                raise Conflict("cleanup not authorized")
            rows = self.db.execute("SELECT ordinal FROM retained WHERE scope=? ORDER BY ordinal LIMIT ?",
                                   (t['scope'], limit)).fetchall()
            self.db.executemany("DELETE FROM retained WHERE scope=? AND ordinal=?",
                                ((t['scope'], r[0]) for r in rows))
            if not rows:
                result = canonical({'scope': t['scope'], 'status': 'canceled' if t['kind'] == 'cancel' else 'discarded',
                                    'request': digest(t['request'])})
                self.db.execute("UPDATE terminal SET phase='done',result=? WHERE id=?", (result, key))
                self.db.execute("UPDATE construction SET owner_present=0 WHERE scope=?", (t['scope'],))
            if fail:
                raise OSError("injected cleanup rollback")
            return len(rows)

    def outcome(self, key: str) -> str | None:
        row = self.db.execute("SELECT result FROM terminal WHERE id=?", (key,)).fetchone()
        return None if row is None else row[0]
