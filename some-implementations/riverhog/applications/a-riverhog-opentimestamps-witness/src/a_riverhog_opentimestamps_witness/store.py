"""Atomic catalog progress, retained observations, and independent proof work."""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from a_riverhog_witness_contract_lib import CollectionWitnessStatement
from riverhog_client.following import (
    CatalogFollowApi,
    CatalogFollowBatch,
    CatalogFollower,
    CatalogFollowPosition,
)
from riverhog_protocol import CatalogSyncDeparture, CatalogSyncDescriptor

from a_riverhog_opentimestamps_witness import proof
from a_riverhog_opentimestamps_witness.schema import validate_state


class StaleProposal(RuntimeError):
    """Another worker advanced the durable generation or serial."""


@dataclass(frozen=True)
class Progress:
    generation: int
    serial: int
    position: CatalogFollowPosition


def _connection(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(path, timeout=30, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA busy_timeout=30000")
    return db


@contextmanager
def _db(path: Path) -> Iterator[sqlite3.Connection]:
    connection = _connection(path)
    try:
        yield connection
    finally:
        connection.close()


def _progress(db: sqlite3.Connection) -> Progress:
    row = db.execute("SELECT * FROM progress WHERE id=1").fetchone()
    if row is None:
        return Progress(0, 0, CatalogFollowPosition())
    return Progress(
        row["generation"],
        row["serial"],
        CatalogFollowPosition(
            phase=row["phase"],
            source_identity=row["source_identity"],
            authorization_view_identity=row["authorization_view_identity"],
            cursor=row["cursor"],
            through_revision=row["through_revision"],
            last_collection_id=row["last_collection_id"],
            reset_reason=row["reset_reason"],
        ),
    )


def _save_progress(db: sqlite3.Connection, before: Progress, after: CatalogFollowPosition) -> None:
    if _progress(db) != before:
        raise StaleProposal("catalog progress changed while fetching a page")
    values = (
        before.generation,
        before.serial + 1,
        after.phase,
        after.source_identity,
        after.authorization_view_identity,
        after.cursor,
        after.through_revision,
        after.last_collection_id,
        after.reset_reason,
    )
    db.execute(
        "INSERT INTO progress VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET "
        "generation=excluded.generation, serial=excluded.serial, phase=excluded.phase, "
        "source_identity=excluded.source_identity, "
        "authorization_view_identity=excluded.authorization_view_identity, "
        "cursor=excluded.cursor, through_revision=excluded.through_revision, "
        "last_collection_id=excluded.last_collection_id, reset_reason=excluded.reset_reason",
        values,
    )


def _document(value: CatalogSyncDescriptor | CatalogSyncDeparture) -> bytes:
    return json.dumps(
        value.model_dump(mode="json", exclude={"operation"}),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _observe(
    db: sqlite3.Connection,
    generation: int,
    source_identity: str,
    value: CatalogSyncDescriptor | CatalogSyncDeparture,
    now: int,
    calendars: tuple[str, ...],
) -> None:
    row = db.execute(
        "SELECT revision, document, departed FROM observations "
        "WHERE generation=? AND collection_id=?",
        (generation, value.collection_id),
    ).fetchone()
    document = _document(value)
    departed = isinstance(value, CatalogSyncDeparture)
    if row is not None:
        old, new = int(row["revision"]), int(value.revision)
        if new < old:
            return
        if new == old:
            if row["document"] != document or bool(row["departed"]) != departed:
                raise ValueError("conflicting equal-revision catalog observation")
            return
    statement_digest: str | None = None
    if isinstance(value, CatalogSyncDescriptor):
        statement = CollectionWitnessStatement.from_catalog(source_identity, value)
        payload = statement.serialize()
        statement_digest = hashlib.sha256(payload).hexdigest()
        job = proof.prepare(
            bytes.fromhex(statement_digest), secrets.token_bytes(32), calendars, now=now
        )
        db.execute(
            "INSERT OR IGNORE INTO statements "
            "(digest, payload, job_state, job_version, due) VALUES (?, ?, ?, 0, ?)",
            (statement_digest, payload, proof.dump_job(job), job.next_due),
        )
        existing = db.execute(
            "SELECT payload FROM statements WHERE digest=?", (statement_digest,)
        ).fetchone()
        if existing is None or existing["payload"] != payload:
            raise ValueError("statement digest collision or corrupt retained evidence")
    db.execute(
        "INSERT INTO observations VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(generation, collection_id) DO UPDATE SET "
        "revision=excluded.revision, document=excluded.document, "
        "statement_digest=excluded.statement_digest, departed=excluded.departed, "
        "departure_cause=excluded.departure_cause",
        (
            generation,
            value.collection_id,
            value.revision,
            document,
            statement_digest,
            int(departed),
            value.cause if isinstance(value, CatalogSyncDeparture) else None,
        ),
    )


class WitnessStore:
    def __init__(self, database: Path, calendars: tuple[str, ...] = ()) -> None:
        self.database = Path(database)
        if len(set(calendars)) != len(calendars):
            raise ValueError("calendar URLs must be distinct")
        for url in calendars:
            proof.calendar_url(url)
        self.calendars = calendars
        validate_state(self.database)

    def progress(self) -> Progress:
        with _db(self.database) as db:
            return _progress(db)

    def ingest_once(
        self, api: CatalogFollowApi, *, now: int | None = None, limit: int = 100
    ) -> CatalogFollowBatch:
        """Commit one page and its proof intents with the cursor in one transaction."""
        if not self.calendars:
            raise ValueError("catalog ingestion requires at least one calendar")
        tick = int(time.time()) if now is None else now
        if tick < 0:
            raise ValueError("now must be nonnegative")
        before = self.progress()
        batch = CatalogFollower(api).step(before.position, limit=limit)
        with _db(self.database) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                if batch.kind in {"catalog", "changes"}:
                    source = batch.after.source_identity
                    if source is None:
                        raise ValueError("catalog observations lack source authority")
                    for value in (*batch.collections, *batch.changes):
                        _observe(db, before.generation, source, value, tick, self.calendars)
                _save_progress(db, before, batch.after)
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return batch

    def rebaseline(self) -> Progress:
        """Explicitly abandon this view, preserving its observations and evidence."""
        with _db(self.database) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                before = _progress(db)
                after = CatalogFollowPosition()
                db.execute(
                    "INSERT INTO progress VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET generation=excluded.generation, "
                    "serial=excluded.serial, phase=excluded.phase, "
                    "source_identity=NULL, authorization_view_identity=NULL, cursor=NULL, "
                    "through_revision='0', last_collection_id=NULL, reset_reason=NULL",
                    (
                        before.generation + 1,
                        before.serial + 1,
                        "new",
                        None,
                        None,
                        None,
                        "0",
                        None,
                        None,
                    ),
                )
                db.commit()
                return Progress(before.generation + 1, before.serial + 1, after)
            except BaseException:
                db.rollback()
                raise

    def mature_once(self, calendar: proof.Calendar, *, now: int | None = None) -> str | None:
        """Bound one calendar exchange; persist a new proof revision before advancing."""
        if not self.calendars:
            raise ValueError("proof maturation requires at least one calendar")
        tick = int(time.time()) if now is None else now
        with _db(self.database) as db:
            row = db.execute(
                "SELECT digest, job_state, job_version FROM statements "
                "WHERE due<=? ORDER BY due, digest LIMIT 1",
                (tick,),
            ).fetchone()
        if row is None:
            return None
        digest, state, version = str(row["digest"]), row["job_state"], row["job_version"]
        old = proof.load_job(state, bytes.fromhex(digest))
        new = proof.step(old, calendar, now=tick, allow=frozenset(self.calendars))
        new_state = proof.dump_job(new)
        due = min(
            (work.due for work in new.work if work.url in self.calendars and work.due is not None),
            default=None,
        )
        with _db(self.database) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                current = db.execute(
                    "SELECT job_state, job_version FROM statements WHERE digest=?", (digest,)
                ).fetchone()
                if (
                    current is None
                    or current["job_version"] != version
                    or current["job_state"] != state
                ):
                    raise StaleProposal("another worker accepted the proof step first")
                if new.proof is not None and new.proof != old.proof:
                    revision = db.execute(
                        "SELECT COALESCE(MAX(revision), 0)+1 FROM proof_history WHERE digest=?",
                        (digest,),
                    ).fetchone()[0]
                    db.execute(
                        "INSERT INTO proof_history VALUES (?, ?, ?, ?, ?)",
                        (digest, revision, new.proof, hashlib.sha256(new.proof).hexdigest(), tick),
                    )
                db.execute(
                    "UPDATE statements SET job_state=?, job_version=job_version+1, due=? "
                    "WHERE digest=? AND job_version=?",
                    (new_state, due, digest, version),
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return digest

    def reschedule(self, digest: str) -> bool:
        """Re-evaluate a named paused job after an explicit calendar allowlist change."""
        if not self.calendars:
            raise ValueError("rescheduling requires at least one calendar")
        with _db(self.database) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT job_state FROM statements WHERE digest=? AND due IS NULL",
                    (digest,),
                ).fetchone()
                if row is None:
                    db.rollback()
                    return False
                job = proof.enable_calendars(
                    proof.load_job(row["job_state"], bytes.fromhex(digest)),
                    allow=frozenset(self.calendars),
                    now=int(time.time()),
                )
                due = min(
                    (
                        work.due
                        for work in job.work
                        if work.url in self.calendars and work.due is not None
                    ),
                    default=None,
                )
                if due is None:
                    db.rollback()
                    return False
                db.execute(
                    "UPDATE statements SET due=?, job_state=?, job_version=job_version+1 "
                    "WHERE digest=? AND due IS NULL",
                    (due, proof.dump_job(job), digest),
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return True

    def verify(
        self, digest: str, bitcoin: proof.Bitcoin, *, minimum_confirmations: int | None = None
    ) -> tuple[proof.Verification, bool]:
        evidence = self.evidence(digest)
        if evidence is None:
            raise KeyError(digest)
        raw = evidence["proof"]
        if not isinstance(raw, bytes):
            raise ValueError("statement has no retained proof yet")
        result = proof.verify(raw, bytes.fromhex(digest), bitcoin)
        return result, proof.confirmation_policy(result, minimum_confirmations)

    def evidence(self, digest: str) -> dict[str, object] | None:
        with _db(self.database) as db:
            row = db.execute("SELECT * FROM statements WHERE digest=?", (digest,)).fetchone()
            if row is None:
                return None
            history = db.execute(
                "SELECT revision, proof_digest, recorded_at FROM proof_history "
                "WHERE digest=? ORDER BY revision",
                (digest,),
            ).fetchall()
        payload = bytes(row["payload"])
        statement = CollectionWitnessStatement.parse(payload)
        if statement.sha256().hex() != digest:
            raise ValueError("stored statement digest mismatch")
        job = proof.load_job(row["job_state"], bytes.fromhex(digest))
        return {
            "statement": payload,
            "proof": job.proof,
            "job": job,
            "proof_revisions": tuple(dict(item) for item in history),
            "due": row["due"],
        }
