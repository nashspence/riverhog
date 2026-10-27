"""Reference-only SQLite ordering experiment; not a Windows backend or Native test."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import platform
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

SCHEMA = """
CREATE TABLE controls (role TEXT PRIMARY KEY, body BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE objects (generation TEXT PRIMARY KEY, length INTEGER NOT NULL,
                      digest TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE chunks (generation TEXT NOT NULL, ordinal INTEGER NOT NULL,
                     body BLOB NOT NULL, PRIMARY KEY (generation, ordinal)) WITHOUT ROWID;
CREATE TABLE names (name TEXT PRIMARY KEY, generation TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE cleanup (singleton INTEGER PRIMARY KEY CHECK (singleton=1),
                      next_ordinal INTEGER NOT NULL, pending_bytes INTEGER NOT NULL,
                      done INTEGER NOT NULL CHECK (done IN (0,1)));
"""


def connect(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path, isolation_level=None, timeout=0)
    assert con.execute("PRAGMA journal_mode=PERSIST").fetchone()[0] == "persist"
    con.execute("PRAGMA synchronous=EXTRA")
    assert con.execute("PRAGMA synchronous").fetchone()[0] == 3
    con.execute("PRAGMA cache_size=-512")
    con.execute("PRAGMA mmap_size=0")
    return con


def candidate(path: Path, length: int) -> None:
    with connect(path) as con:
        con.execute("PRAGMA auto_vacuum=INCREMENTAL")
        con.executescript(SCHEMA)
        body = b"x" * length  # Only a synthetic test oracle; not production buffering.
        digest = hashlib.sha256(body).hexdigest()
        con.execute("INSERT INTO objects VALUES ('g',?,?)", (length, digest))
        for ordinal, offset in enumerate(range(0, length, 4096)):
            con.execute("BEGIN IMMEDIATE")
            con.execute("INSERT INTO chunks VALUES ('g',?,?)", (ordinal, body[offset:offset+4096]))
            con.execute("COMMIT")
        manifest = json.dumps({"bytes": length, "sha256": digest}, sort_keys=True).encode()
        con.execute("INSERT INTO controls VALUES ('prepared',?)", (manifest,))
        con.execute("INSERT INTO cleanup VALUES (1,0,?,0)", (length,))
    con.close()


def child(path: Path, operation: str, cut: str) -> None:
    con = connect(path)
    con.execute("BEGIN IMMEDIATE")
    if operation == "select":
        con.execute("INSERT INTO names VALUES ('journal','g')")
    elif operation == "delete":
        next_ordinal, pending, done = con.execute(
            "SELECT next_ordinal,pending_bytes,done FROM cleanup WHERE singleton=1"
        ).fetchone()
        if not done:
            row = con.execute(
                "SELECT ordinal FROM chunks WHERE generation='g' AND ordinal>=? "
                "ORDER BY ordinal LIMIT 1", (next_ordinal,)
            ).fetchone()
            if row:
                con.execute("DELETE FROM chunks WHERE generation='g' AND ordinal=?", row)
                con.execute("UPDATE cleanup SET next_ordinal=? WHERE singleton=1", (row[0]+1,))
            else:
                con.execute("DELETE FROM names WHERE name='journal'")
                con.execute("UPDATE cleanup SET pending_bytes=0,done=1 WHERE singleton=1")
    else:
        raise ValueError(operation)
    if cut == "before":
        os._exit(71)
    con.execute("COMMIT")
    if cut == "after":
        os._exit(72)
    con.close()


class OrderingProbe(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "state.db"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def crash(self, operation: str, cut: str) -> None:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--child", str(self.path), operation, cut],
            check=False, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, 71 if cut == "before" else 72, result.stderr)

    def test_uncommitted_selection_is_absent(self) -> None:
        candidate(self.path, 9000)
        self.crash("select", "before")
        con = connect(self.path)
        try:
            self.assertIsNone(con.execute("SELECT generation FROM names WHERE name='journal'").fetchone())
            self.assertEqual(con.execute("SELECT count(*) FROM chunks").fetchone()[0], 3)
        finally:
            con.close()

    def test_committed_selection_survives_lost_response(self) -> None:
        candidate(self.path, 9000)
        self.crash("select", "after")
        con = connect(self.path)
        try:
            self.assertEqual(con.execute("SELECT generation FROM names WHERE name='journal'").fetchone(), ("g",))
            body = b"".join(row[0] for row in con.execute(
                "SELECT body FROM chunks WHERE generation='g' ORDER BY ordinal"
            ))
            self.assertEqual(body, b"x" * 9000)
        finally:
            con.close()

    def test_conflicting_selection_does_not_replace(self) -> None:
        candidate(self.path, 1)
        self.crash("select", "after")
        con = connect(self.path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute("INSERT INTO names VALUES ('journal','other')")
            self.assertEqual(con.execute("SELECT generation FROM names").fetchone(), ("g",))
        finally:
            con.close()

    def test_control_admission_never_queries_chunk_table(self) -> None:
        for length in (1024, 1048576):
            with self.subTest(length=length):
                path = Path(self.temp.name) / f"controls-{length}.db"
                candidate(path, length)
                con = connect(path)
                try:
                    def authorize(action, table, column, database, trigger):
                        if action == sqlite3.SQLITE_READ and table == "chunks":
                            return sqlite3.SQLITE_DENY
                        return sqlite3.SQLITE_OK
                    con.set_authorizer(authorize)
                    prepared = con.execute("SELECT body FROM controls WHERE role='prepared'").fetchone()[0]
                    synthetic_proof = hashlib.sha256(prepared).digest()
                    con.execute("INSERT INTO controls VALUES ('synthetic-custody',?)", (synthetic_proof,))
                    self.assertEqual(con.execute("SELECT body FROM controls WHERE role='synthetic-custody'").fetchone()[0], synthetic_proof)
                finally:
                    con.close()

    def test_cleanup_cursor_rolls_back_with_deletion(self) -> None:
        candidate(self.path, 9000)
        self.crash("delete", "before")
        con = connect(self.path)
        try:
            self.assertEqual(con.execute("SELECT next_ordinal,pending_bytes,done FROM cleanup").fetchone(), (0, 9000, 0))
            self.assertEqual(con.execute("SELECT count(*) FROM chunks").fetchone()[0], 3)
        finally:
            con.close()

    def test_cleanup_cursor_survives_lost_response(self) -> None:
        candidate(self.path, 9000)
        self.crash("delete", "after")
        con = connect(self.path)
        try:
            self.assertEqual(con.execute("SELECT next_ordinal,pending_bytes,done FROM cleanup").fetchone(), (1, 9000, 0))
            self.assertEqual(con.execute("SELECT min(ordinal) FROM chunks").fetchone()[0], 1)
        finally:
            con.close()

    def test_final_accounting_is_idempotent(self) -> None:
        candidate(self.path, 9000)
        self.crash("select", "after")
        for _ in range(5):
            child(self.path, "delete", "return")
        con = connect(self.path)
        try:
            self.assertEqual(con.execute("SELECT pending_bytes,done FROM cleanup").fetchone(), (0, 1))
            self.assertEqual(con.execute("SELECT count(*) FROM chunks").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT count(*) FROM names").fetchone()[0], 0)
        finally:
            con.close()


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "--child":
        child(Path(sys.argv[2]), sys.argv[3], sys.argv[4])
    else:
        start = time.perf_counter()
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(OrderingProbe))
        con = sqlite3.connect(":memory:")
        source = con.execute("SELECT sqlite_source_id()").fetchone()[0]
        con.close()
        print(json.dumps({
            "kind": "reference_sql_ordering_probe_not_windows_qualification",
            "python": sys.version, "platform": platform.platform(),
            "sqlite_version": sqlite3.sqlite_version, "sqlite_source_id": source,
            "tests": result.testsRun, "successful": result.wasSuccessful(),
            "elapsed_seconds": round(time.perf_counter()-start, 6),
            "limitations": ["synthetic SQL only", "no Riverhog imports", "no Win32 ownership or ACLs", "no Native/authenticated receipts", "no VFS byte-I/O measurement", "process exits, not power loss"],
        }, indent=2))
        raise SystemExit(0 if result.wasSuccessful() else 1)
