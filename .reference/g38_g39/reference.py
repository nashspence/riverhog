"""Diagnostic SQLite lifecycle, not the native observer protocol or security layer.

Small typed fixtures model segmented integrity, exact coverage, taint and CAS.
Production JSON/profile validation, authenticated RPC and power-loss are outside
this model. SHA continuation reuses the earlier diagnostic, never production crypto.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
from typing import Callable, Iterator

_spec = importlib.util.spec_from_file_location(
    "g38_diagnostic_sha", Path(__file__).parents[1] / "g22_g26" / "sha256_state.py"
)
assert _spec and _spec.loader
_sha = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_sha)
Sha256 = _sha.Sha256


def encode(value: object) -> bytes:
    """Only fixture JSON; NOT a replacement for native JCS."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def identity(domain: str, value: object) -> str:
    return hashlib.sha256(domain.encode() + b"\0" + encode(value)).hexdigest()


def request_id(question: str, selection: str, ports: str, descriptor: str) -> str:
    return identity("fixture-request", [question, selection, ports, descriptor])


def job_id(request: str, claim: str, fence: int) -> str:
    if type(fence) is not int or fence < 1:
        raise ValueError("fence")
    return identity("fixture-job", [request, claim, fence])


@dataclass(frozen=True)
class Grant:
    job: str
    principal: str
    fence: int
    live: bool = True


class Store:
    def __init__(self, path: Path, *, chunk_bytes: int = 256) -> None:
        if type(chunk_bytes) is not int or chunk_bytes < 1:
            raise ValueError("chunk budget")
        self.path, self.chunk_bytes = path, chunk_bytes
        self.connections = 0
        with self.db() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript('''
CREATE TABLE IF NOT EXISTS blob(id TEXT PRIMARY KEY, total INTEGER NOT NULL,
 received INTEGER NOT NULL, sha BLOB NOT NULL, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS segment(blob TEXT, offset INTEGER, data BLOB NOT NULL,
 PRIMARY KEY(blob,offset));
CREATE TABLE IF NOT EXISTS binding(job TEXT, blob TEXT, principal TEXT, fence INTEGER,
 PRIMARY KEY(job,blob));
CREATE TABLE IF NOT EXISTS work(job TEXT PRIMARY KEY, state TEXT, epoch INTEGER,
 version INTEGER, cursor TEXT, visited INTEGER, taint INTEGER, accepted INTEGER,
 semantic_ok INTEGER);
CREATE TABLE IF NOT EXISTS asked(job TEXT, subject TEXT, identity TEXT,
 PRIMARY KEY(job,subject));
CREATE TABLE IF NOT EXISTS answer(job TEXT, subject TEXT, identity TEXT, status TEXT,
 PRIMARY KEY(job,subject));
CREATE TABLE IF NOT EXISTS edge(job TEXT, primary_id TEXT, associated_id TEXT,
 PRIMARY KEY(job,primary_id,associated_id));
CREATE TABLE IF NOT EXISTS taint(job TEXT, subject TEXT, PRIMARY KEY(job,subject));
CREATE TABLE IF NOT EXISTS source_relation(job TEXT, ordinal INTEGER, payload BLOB,
 PRIMARY KEY(job,ordinal));
''')

    @contextmanager
    def db(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path)
        self.connections += 1
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA busy_timeout=1000")
            with db:
                yield db
        finally:
            db.close()
            self.connections -= 1

    def reserve(self, grant: Grant, digest: str, size: int) -> None:
        if not grant.live or size < 0:
            raise PermissionError("reservation")
        with self.db() as db:
            old = db.execute("SELECT total FROM blob WHERE id=?", (digest,)).fetchone()
            if old and old[0] != size:
                raise ValueError("length rebound")
            db.execute("INSERT OR IGNORE INTO blob VALUES(?, ?, 0, ?, 'receiving')",
                       (digest, size, Sha256().export()))
            previous = db.execute("SELECT principal,fence FROM binding WHERE job=? AND blob=?",
                                  (grant.job, digest)).fetchone()
            if previous and previous != (grant.principal, grant.fence):
                raise PermissionError("binding rebound")
            db.execute("INSERT OR IGNORE INTO binding VALUES(?,?,?,?)",
                       (grant.job, digest, grant.principal, grant.fence))

    def _authorize(self, db: sqlite3.Connection, grant: Grant, digest: str) -> None:
        row = db.execute("SELECT principal,fence FROM binding WHERE job=? AND blob=?",
                         (grant.job, digest)).fetchone()
        if not grant.live or row != (grant.principal, grant.fence):
            raise PermissionError("current authority")

    def put(self, grant: Grant, digest: str, offset: int, data: bytes,
            *, before_commit: Callable[[], None] | None = None) -> int:
        if type(offset) is not int or offset < 0 or not data or len(data) > self.chunk_bytes:
            raise ValueError("chunk")
        with self.db() as db:
            self._authorize(db, grant, digest)
            total, received, state, phase = db.execute(
                "SELECT total,received,sha,state FROM blob WHERE id=?", (digest,)).fetchone()
            if offset < received:
                previous = db.execute("SELECT data FROM segment WHERE blob=? AND offset=?",
                                      (digest, offset)).fetchone()
                if previous != (data,):
                    raise ValueError("conflicting replay")
                return received
            if phase != 'receiving' or offset != received or received + len(data) > total:
                raise ValueError("gap or overrun")
            state = bytes(state)
        # Hash detached from the source connection.
        digest_state = Sha256.restore(state)
        digest_state.update(data)
        with self.db() as db:
            self._authorize(db, grant, digest)
            row = db.execute("SELECT received,sha,state FROM blob WHERE id=?", (digest,)).fetchone()
            if row != (received, state, 'receiving'):
                raise ValueError("stale chunk writer")
            db.execute("INSERT INTO segment VALUES(?,?,?)", (digest, offset, data))
            db.execute("UPDATE blob SET received=?,sha=? WHERE id=?",
                       (received + len(data), digest_state.export(), digest))
            if before_commit:
                before_commit()
        return received + len(data)

    def finish(self, grant: Grant, digest: str) -> None:
        with self.db() as db:
            self._authorize(db, grant, digest)
            total, received, raw, state = db.execute(
                "SELECT total,received,sha,state FROM blob WHERE id=?", (digest,)).fetchone()
        if received != total or Sha256.restore(raw).digest().hex() != digest:
            raise ValueError("incomplete or corrupt")
        with self.db() as db:
            self._authorize(db, grant, digest)
            db.execute("UPDATE blob SET state='verified' WHERE id=? AND received=? AND sha=?",
                       (digest, received, raw))

    def chunk(self, grant: Grant, digest: str, offset: int) -> bytes:
        with self.db() as db:
            self._authorize(db, grant, digest)
            state = db.execute("SELECT state FROM blob WHERE id=?", (digest,)).fetchone()
            if state != ('verified',):
                raise ValueError("not verified")
            row = db.execute("SELECT data FROM segment WHERE blob=? AND offset=?",
                             (digest, offset)).fetchone()
            if row is None:
                raise ValueError("offset")
            data = row[0]
        return data

    def begin_work(self, job: str) -> None:
        with self.db() as db:
            db.execute("INSERT OR IGNORE INTO work VALUES(?,'staging',1,0,'',0,0,0,0)", (job,))

    def asked_row(self, job: str, subject: str, member_identity: str) -> None:
        """Fixture source preparation; production consumes sealed G37 authority."""
        with self.db() as db:
            if db.execute("SELECT state FROM work WHERE job=?", (job,)).fetchone() != ('staging',):
                raise ValueError("sealed input")
            db.execute("INSERT INTO asked VALUES(?,?,?)", (job, subject, member_identity))

    def answer_row(self, job: str, subject: str, member_identity: str, status: str) -> None:
        if status not in {'complete', 'unsupported', 'ambiguous', 'insufficient'}:
            raise ValueError("status")
        with self.db() as db:
            if db.execute("SELECT state FROM work WHERE job=?", (job,)).fetchone() != ('staging',):
                raise ValueError("sealed output")
            # Duplicate testimony is not a transport retry and must fail.
            db.execute("INSERT INTO answer VALUES(?,?,?,?)", (job, subject, member_identity, status))

    def relation(self, job: str, ordinal: int, primary: str | None,
                 associated: str | None, usable: bool) -> None:
        payload = encode([primary, associated, usable])
        with self.db() as db:
            if db.execute("SELECT state FROM work WHERE job=?", (job,)).fetchone() != ('staging',):
                raise ValueError("sealed output")
            old = db.execute("SELECT payload FROM source_relation WHERE job=? AND ordinal=?",
                             (job, ordinal)).fetchone()
            if old:
                if old[0] != payload:
                    raise ValueError("relation replay conflict")
                return
            for subject in (primary, associated):
                if subject is not None and not db.execute(
                    "SELECT 1 FROM asked WHERE job=? AND subject=?", (job, subject)).fetchone():
                    raise ValueError("unknown endpoint")
            if usable and primary is not None and associated is not None:
                if primary == associated:
                    raise ValueError("self edge")
                db.execute("INSERT OR IGNORE INTO edge VALUES(?,?,?)", (job, primary, associated))
            elif associated is None:
                # Native rule: unresolved associated endpoint taints all covered.
                # One shared marker, not N row updates for each invalid claim.
                db.execute("UPDATE work SET taint=1 WHERE job=?", (job,))
            else:
                db.execute("INSERT OR IGNORE INTO taint VALUES(?,?)", (job, associated))
            db.execute("INSERT INTO source_relation VALUES(?,?,?)", (job, ordinal, payload))

    def seal_stage(self, job: str, *, independently_validated: bool) -> None:
        with self.db() as db:
            row = db.execute("SELECT state FROM work WHERE job=?", (job,)).fetchone()
            if row != ('staging',):
                raise ValueError("phase")
            db.execute("UPDATE work SET state='checking',semantic_ok=? WHERE job=?",
                       (int(independently_validated), job))

    def takeover(self, job: str) -> int:
        with self.db() as db:
            db.execute("UPDATE work SET epoch=epoch+1 WHERE job=?", (job,))
            return db.execute("SELECT epoch FROM work WHERE job=?", (job,)).fetchone()[0]

    def check_step(self, job: str, epoch: int, limit: int,
                   *, detached: Callable[[], None] | None = None) -> int:
        if type(limit) is not int or limit < 1:
            raise ValueError("work budget")
        with self.db() as db:
            state, current, version, cursor = db.execute(
                "SELECT state,epoch,version,cursor FROM work WHERE job=?", (job,)).fetchone()
            if current != epoch or state != 'checking':
                raise ValueError("stale or inactive validator")
            asked = db.execute("SELECT subject,identity FROM asked WHERE job=? AND subject>? "
                               "ORDER BY subject LIMIT ?", (job, cursor, limit)).fetchall()
            answers = db.execute("SELECT subject,identity FROM answer WHERE job=? AND subject>? "
                                 "ORDER BY subject LIMIT ?", (job, cursor, limit)).fetchall()
        if detached:
            detached()
        if asked != answers:
            raise ValueError("missing extra replaced or wrong-root subject")
        cursor = asked[-1][0] if asked else cursor
        with self.db() as db:
            changed = db.execute("UPDATE work SET cursor=?,visited=visited+?,version=version+1,"
                                 "state=? WHERE job=? AND epoch=? AND version=? AND state='checking'",
                                 (cursor, len(asked), 'ready' if not asked else 'checking',
                                  job, epoch, version)).rowcount
            if changed != 1:
                raise ValueError("stale checkpoint")
        return len(asked)

    def publish(self, job: str, epoch: int, *, current_authority: bool) -> None:
        if not current_authority:
            raise PermissionError("publication authority")
        with self.db() as db:
            changed = db.execute("UPDATE work SET state='accepted',accepted=1 WHERE job=? "
                                 "AND state='ready' AND epoch=? AND semantic_ok=1",
                                 (job, epoch)).rowcount
            if changed != 1:
                raise ValueError("incomplete validation")

    def status(self, job: str) -> bytes:
        with self.db() as db:
            state, epoch = db.execute("SELECT state,epoch FROM work WHERE job=?", (job,)).fetchone()
        return encode({'job': job, 'state': state, 'attempt': epoch})

    def view(self, job: str, after: str, limit: int, *, current_authority: bool) -> list:
        if not current_authority:
            raise PermissionError("disclosure")
        if type(limit) is not int or limit < 1:
            raise ValueError("page")
        with self.db() as db:
            state, taint = db.execute("SELECT state,taint FROM work WHERE job=?", (job,)).fetchone()
            if state != 'accepted':
                raise ValueError("unaccepted evidence")
            rows = db.execute("SELECT a.subject,a.identity,a.status,t.subject IS NOT NULL "
                              "FROM answer a LEFT JOIN taint t ON t.job=a.job AND t.subject=a.subject "
                              "WHERE a.job=? AND a.subject>? ORDER BY a.subject LIMIT ?",
                              (job, after, limit)).fetchall()
        return [(s, i, 'unsupported' if (taint or local) and status == 'complete' else status)
                for s, i, status, local in rows]

    def cancel(self, job: str) -> None:
        with self.db() as db:
            # Completed evidence precedence; cancellation cannot erase acceptance.
            db.execute("UPDATE work SET state='canceled',epoch=epoch+1 WHERE job=? "
                       "AND accepted=0", (job,))

    def gc_step(self, job: str, limit: int) -> int:
        if type(limit) is not int or limit < 1:
            raise ValueError("cleanup budget")
        total = 0
        with self.db() as db:
            state = db.execute("SELECT state FROM work WHERE job=?", (job,)).fetchone()
            if state != ('canceled',):
                raise ValueError("not disposable")
            for table in ('source_relation', 'edge', 'taint', 'answer', 'asked'):
                rows = db.execute(f"SELECT rowid FROM {table} WHERE job=? LIMIT ?",
                                  (job, limit - total)).fetchall()
                for rowid, in rows:
                    db.execute(f"DELETE FROM {table} WHERE rowid=?", (rowid,))
                total += len(rows)
                if total == limit:
                    break
        return total


def validate_delivery(question: dict[str, str], delivery: dict[str, str],
                      *, whole_scope: bool) -> None:
    """Small exact-set oracle only; production uses sealed roots and bounded joins."""
    if not delivery or any(question.get(k) != v for k, v in delivery.items()):
        raise ValueError("unasked or replaced subject")
    if whole_scope and delivery != question:
        raise ValueError("whole scope partition")
