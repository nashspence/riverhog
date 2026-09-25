"""Atomic catalog progress, retained observations, and independent signing work."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from a_riverhog_witness_contract_lib import CollectionWitnessStatement
from riverhog_client.following import (
    CatalogFollowApi,
    CatalogFollowBatch,
    CatalogFollower,
    CatalogFollowPosition,
)
from riverhog_protocol import CatalogSyncDeparture, CatalogSyncDescriptor

from a_riverhog_minisign_witness.schema import validate_state


class StaleProposal(RuntimeError):
    """Another worker advanced the durable generation or serial."""


class SignerError(RuntimeError):
    """The signer failed without providing a signature claim."""

    def __init__(self, code: str, *, retryable: bool) -> None:
        if code not in {"unavailable", "invalid_signature", "key_unavailable"}:
            raise ValueError("invalid signer error code")
        self.code = code
        self.retryable = retryable
        super().__init__(code)


@dataclass(frozen=True)
class Signature:
    payload: bytes
    key_identity: str


class Signer(Protocol):
    def sign(self, statement: bytes) -> Signature: ...


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
        db.execute(
            "INSERT OR IGNORE INTO statements "
            "(digest, payload, state, attempts, due, error, signature, key_identity) "
            "VALUES (?, ?, 'pending', 0, ?, NULL, NULL, NULL)",
            (statement_digest, payload, now),
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
    def __init__(self, database: Path) -> None:
        self.database = Path(database)
        validate_state(self.database)

    def progress(self) -> Progress:
        with _db(self.database) as db:
            return _progress(db)

    def ingest_once(
        self, api: CatalogFollowApi, *, now: int | None = None, limit: int = 100
    ) -> CatalogFollowBatch:
        """Commit one page and its sign intents with the cursor in one transaction."""
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
                        _observe(db, before.generation, source, value, tick)
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

    def sign_once(self, signer: Signer, *, now: int | None = None) -> str | None:
        """Bound one signing attempt; a crash before commit leaves a retryable job."""
        tick = int(time.time()) if now is None else now
        with _db(self.database) as db:
            row = db.execute(
                "SELECT digest, payload, attempts FROM statements "
                "WHERE state='pending' AND due<=? ORDER BY due, digest LIMIT 1",
                (tick,),
            ).fetchone()
        if row is None:
            return None
        digest, payload, attempts = str(row["digest"]), row["payload"], row["attempts"]
        try:
            result = signer.sign(payload)
            if not result.payload or not result.key_identity.startswith("sha256:"):
                raise SignerError("invalid_signature", retryable=False)
            state, error, signature, key_identity, due = (
                "signed",
                None,
                result.payload,
                result.key_identity,
                None,
            )
        except SignerError as exc:
            state, error, signature, key_identity = (
                "pending" if exc.retryable else "blocked",
                exc.code,
                None,
                None,
            )
            due = tick + min(86_400, 60 * (1 << min(attempts, 10))) if exc.retryable else None
        with _db(self.database) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                changed = db.execute(
                    "UPDATE statements SET state=?, attempts=attempts+1, due=?, error=?, "
                    "signature=?, key_identity=? "
                    "WHERE digest=? AND state='pending' AND attempts=?",
                    (state, due, error, signature, key_identity, digest, attempts),
                ).rowcount
                db.commit()
            except BaseException:
                db.rollback()
                raise
        if changed != 1:
            raise StaleProposal("another signer accepted the work first")
        return digest

    def evidence(self, digest: str) -> dict[str, object] | None:
        with _db(self.database) as db:
            row = db.execute("SELECT * FROM statements WHERE digest=?", (digest,)).fetchone()
        if row is None:
            return None
        payload = bytes(row["payload"])
        statement = CollectionWitnessStatement.parse(payload)
        if statement.sha256().hex() != digest:
            raise ValueError("stored statement digest mismatch")
        return {
            "statement": payload,
            "signature": row["signature"],
            "key_identity": row["key_identity"],
            "state": row["state"],
            "attempts": row["attempts"],
            "due": row["due"],
            "error": row["error"],
        }
