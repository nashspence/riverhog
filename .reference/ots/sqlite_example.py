# SPDX-License-Identifier: Apache-2.0
"""Application-owned durability example, NOT an adopted Riverhog database contract.

Explicit initialization, idempotent enqueue, atomic proof/progress CAS, and append-only
proof revisions. Network effects happen outside transactions. A stale worker discards
its result and reloads; CAS is not an exactly-once network guarantee.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from ots_core import Job, StateError, dump_job, load_job


class StaleWorker(RuntimeError):
    pass


def initialize(path: str | Path) -> None:
    """Explicit one-time deployment operation. Refuse existing/unknown schemas."""
    with sqlite3.connect(path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        if (connection.execute("PRAGMA user_version").fetchone()[0] != 0
                or connection.execute("SELECT name FROM sqlite_master").fetchone()):
            raise StateError("database already initialized")
        connection.execute("""
            CREATE TABLE jobs (
                digest BLOB PRIMARY KEY,
                revision INTEGER NOT NULL CHECK(revision >= 0),
                state BLOB NOT NULL
            )
        """)
        connection.execute("""
            CREATE TABLE proofs (
                digest BLOB NOT NULL,
                revision INTEGER NOT NULL,
                proof BLOB NOT NULL,
                PRIMARY KEY(digest, revision),
                FOREIGN KEY(digest) REFERENCES jobs(digest)
            )
        """)
        connection.execute("PRAGMA user_version=1")


class Store:
    def __init__(self, path: str | Path):
        # mode=rw: startup must never silently create or migrate a database.
        self.connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=rw", uri=True)
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA synchronous=FULL")
        if self.connection.execute("PRAGMA user_version").fetchone()[0] != 1:
            self.connection.close()
            raise StateError("unsupported reference schema")
        try:
            self.connection.execute("SELECT digest, revision, state FROM jobs LIMIT 0")
            self.connection.execute("SELECT digest, revision, proof FROM proofs LIMIT 0")
        except sqlite3.DatabaseError as exc:
            self.connection.close()
            raise StateError("corrupt reference schema") from exc

    def close(self) -> None:
        self.connection.close()

    def enqueue(self, job: Job) -> tuple[int, Job]:
        """Replayed ingestion preserves the first nonce and existing progress."""
        data = dump_job(job)
        with self.connection:
            cursor = self.connection.execute(
                "INSERT OR IGNORE INTO jobs VALUES (?, 0, ?)", (job.statement_digest, data)
            )
            if cursor.rowcount and job.proof is not None:
                self.connection.execute("INSERT INTO proofs VALUES (?, 0, ?)",
                                        (job.statement_digest, job.proof))
        return self.load(job.statement_digest)

    def load(self, digest: bytes) -> tuple[int, Job]:
        row = self.connection.execute(
            "SELECT revision, state FROM jobs WHERE digest=?", (digest,)
        ).fetchone()
        if row is None:
            raise KeyError("unknown witness digest")
        return row[0], load_job(row[1], digest)

    def save(self, expected_revision: int, job: Job) -> int:
        """One transaction for proof bytes, retry progress, and historical evidence."""
        data = dump_job(job)
        with self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            revision, previous = self.load(job.statement_digest)
            if revision != expected_revision:
                raise StaleWorker("reload before retrying")
            if (previous.nonce != job.nonce or previous.submit_urls != job.submit_urls
                    or (previous.proof is not None and job.proof is None)):
                raise StateError("immutable identity or retained proof changed")
            self.connection.execute(
                "UPDATE jobs SET revision=?, state=? WHERE digest=? AND revision=?",
                (revision + 1, data, job.statement_digest, expected_revision),
            )
            if job.proof is not None and job.proof != previous.proof:
                self.connection.execute("INSERT INTO proofs VALUES (?, ?, ?)",
                                        (job.statement_digest, revision + 1, job.proof))
        return revision + 1
