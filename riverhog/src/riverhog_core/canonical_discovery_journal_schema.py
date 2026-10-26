"""Private normalized framing and child-registration schema, without runtime owners."""

from __future__ import annotations

import sqlite3


def initialize_archive_framing(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("archive framing initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_archive_framing (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, algorithm TEXT NOT NULL,
            source_bytes TEXT NOT NULL, source_sha256 TEXT NOT NULL,
            version TEXT NOT NULL, byte_offset TEXT NOT NULL, frame_start TEXT,
            frame_count TEXT NOT NULL, phase TEXT NOT NULL, failure TEXT,
            PRIMARY KEY(build_id,admission_sha256,journal_id),
            CHECK(phase IN ('scanning','complete','deferred')),
            CHECK((phase='deferred') = (failure IS NOT NULL))
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_archive_frames (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            ordinal TEXT NOT NULL, byte_offset TEXT NOT NULL, json_bytes TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id,ordinal)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_archive_pages (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            ordinal TEXT NOT NULL, content BLOB NOT NULL, sha256 TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id,ordinal),
            CHECK(length(content) > 0 AND length(content) <= 65536)
        ) WITHOUT ROWID;
    """)


def initialize_journal_registration(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("journal registration initialization requires no active transaction")
    db.execute("""
        CREATE TABLE IF NOT EXISTS discovery_journal_children (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, directory_id TEXT NOT NULL, binding_json TEXT NOT NULL,
            confirmed INTEGER NOT NULL CHECK(confirmed IN (0,1)),
            PRIMARY KEY(build_id,admission_sha256,journal_id),
            UNIQUE(build_id,admission_sha256,directory_id)
        ) WITHOUT ROWID
    """).close()


def initialize_journal_epochs(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("journal epoch initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_journal_epochs (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL, directory_id TEXT NOT NULL, algorithm TEXT NOT NULL,
            binding_json TEXT NOT NULL, version TEXT NOT NULL, next_frame TEXT NOT NULL,
            byte_offset TEXT NOT NULL, epoch TEXT NOT NULL, phase TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id),
            CHECK(phase IN ('scanning','complete','deferred'))
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_journal_epoch_frames (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL, journal_id TEXT NOT NULL,
            ordinal TEXT NOT NULL, byte_offset TEXT NOT NULL, json_bytes TEXT NOT NULL,
            epoch TEXT NOT NULL, entry_kind TEXT NOT NULL, json_sha256 TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,journal_id,ordinal)
        ) WITHOUT ROWID;
    """)
