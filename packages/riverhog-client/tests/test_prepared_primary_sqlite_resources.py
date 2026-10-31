"""Retained original grants, explicit increases and nondestructive cold rejection."""

from __future__ import annotations

import hashlib
import os
import sqlite3

import pytest
from riverhog_client import PrimaryStateCapacity, PrimaryStateError
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits, TransactionalPrimaryFiles

from tests.support.prepared_primary import SqlitePrimaryTestGuard

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SQL core isolation uses the Unix VFS")

LIMITS = SqlitePrimaryLimits(1024 * 1024, 2 * 1024 * 1024, 128 * 1024)
LARGER = SqlitePrimaryLimits(2 * 1024 * 1024, 4 * 1024 * 1024, 256 * 1024)
ALLOCATION = b"retained resource owner"


def opened(path, *, initialize=False, limits=LIMITS):
    if initialize:
        path.touch()
    return TransactionalPrimaryFiles(
        SqlitePrimaryTestGuard(path),
        allocation_descriptor=ALLOCATION,
        limits=limits,
        initialize=initialize,
    )


def test_larger_recovery_offer_preserves_grants_until_explicit_increase(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        selected = files.put_bytes("original", (b"original",), byte_limit=8)
        first = files.resource_grants()
    with opened(path, limits=LARGER) as files:
        assert files.resource_grants() == first
        assert files._one("PRAGMA max_page_count") == (LIMITS.database_bytes // 4096,)
        assert files._one("PRAGMA journal_size_limit") == (LIMITS.journal_bytes,)
        assert files._limits.cache_bytes == LARGER.cache_bytes
        changed = files.increase_resource_grants(
            database_bytes=LARGER.database_bytes, journal_bytes=LARGER.journal_bytes
        )
        assert changed.revision == first.revision + 1 and changed.identity == first.identity
        assert files._one("PRAGMA max_page_count") == (LARGER.database_bytes // 4096,)
        assert files._one("PRAGMA journal_size_limit") == (LARGER.journal_bytes,)
        total = files._db().total_changes
        assert (
            files.increase_resource_grants(
                database_bytes=LARGER.database_bytes, journal_bytes=LARGER.journal_bytes
            )
            == changed
        )
        assert files._db().total_changes == total
    with opened(path, limits=LARGER) as files:
        assert files.resource_grants() == changed
        with files.verified(selected) as stream:
            assert stream.read() == b"original"


@pytest.mark.parametrize("field", ["database_bytes", "journal_bytes"])
def test_cold_open_cannot_lower_original_grants_even_when_actual_extents_fit(
    tmp_path, monkeypatch, field
):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files.put_bytes("original", (b"original",), byte_limit=8)
        assert files.space_usage().database.end_bytes < LIMITS.database_bytes // 2
        assert files.space_usage().journal.end_bytes < LIMITS.journal_bytes // 2
    original = hashlib.sha256(path.read_bytes()).digest()
    values = dict(
        database_bytes=LIMITS.database_bytes,
        journal_bytes=LIMITS.journal_bytes,
        cache_bytes=LIMITS.cache_bytes,
    )
    values[field] //= 2
    offered = SqlitePrimaryLimits(**values)
    barriers = []
    monkeypatch.setattr(TransactionalPrimaryFiles, "sync", lambda self: barriers.append(True))
    with pytest.raises(PrimaryStateCapacity, match="original resource grants"):
        opened(path, limits=offered)
    assert barriers == []
    assert hashlib.sha256(path.read_bytes()).digest() == original
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT body FROM primary_chunks").fetchall() == [(b"original",)]


def test_increase_keeps_pending_cleanup_and_updates_existing_children(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files.child("child", initialize=True)
        selected = files.put_bytes("original", (b"original",), byte_limit=8)

        def interrupted():
            yield b"pending"
            raise RuntimeError("interrupted source")

        with pytest.raises(RuntimeError, match="interrupted source"):
            files.put_bytes("pending", interrupted(), byte_limit=8192)
    with opened(path) as files:
        child = files.child("child", initialize=False)
        operation = files._one("SELECT generation,cursor,phase FROM primary_operation")
        assert operation is not None
        chunks = files._one("SELECT count(*) FROM primary_chunks")
        files.increase_resource_grants(
            database_bytes=LARGER.database_bytes, journal_bytes=LARGER.journal_bytes
        )
        assert files._one("SELECT generation,cursor,phase FROM primary_operation") == operation
        assert files._one("SELECT count(*) FROM primary_chunks") == chunks
        assert child._limits is files._limits
        assert child.resource_grants() == files.resource_grants()
        with pytest.raises(PrimaryStateError, match="container root"):
            child.increase_resource_grants(
                database_bytes=LARGER.database_bytes, journal_bytes=LARGER.journal_bytes
            )
        with files.verified(selected) as stream:
            assert stream.read() == b"original"


@pytest.mark.parametrize("field", ["database_bytes", "journal_bytes"])
def test_explicit_increase_cannot_revoke_another_retained_grant(tmp_path, field):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        original = files.resource_grants()
        values = dict(database_bytes=LARGER.database_bytes, journal_bytes=LARGER.journal_bytes)
        values[field] = getattr(LIMITS, field) // 2
        before = files._db().total_changes
        with pytest.raises(ValueError, match="cannot lower"):
            files.increase_resource_grants(**values)
        assert files.resource_grants() == original and files._db().total_changes == before


@pytest.mark.parametrize("corruption", ["tag", "engine", "database", "journal"])
def test_invalid_resource_record_stops_before_barrier_without_recreating_state(
    tmp_path, monkeypatch, corruption
):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files.put_bytes("original", (b"original",), byte_limit=8)
        raw = bytearray(files._one("SELECT resource_grants FROM primary_format")[0])
    if corruption == "tag":
        raw[0] ^= 1
    elif corruption == "engine":
        raw[8] ^= 1
    elif corruption == "database":
        raw[40:48] = b"\0" * 8
    else:
        raw[48:56] = (4097).to_bytes(8, "big")
    with sqlite3.connect(path) as db:
        db.execute("UPDATE primary_format SET resource_grants=?", (bytes(raw),))
    original = hashlib.sha256(path.read_bytes()).digest()
    barriers = []
    monkeypatch.setattr(TransactionalPrimaryFiles, "sync", lambda self: barriers.append(True))
    with pytest.raises(PrimaryStateError, match="resource"):
        opened(path)
    assert not barriers and hashlib.sha256(path.read_bytes()).digest() == original
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT body FROM primary_chunks").fetchall() == [(b"original",)]


@pytest.mark.parametrize("raw", [b"", b"x" * 63, b"x" * 65, "x" * 64])
def test_resource_record_has_fixed_binary_extent(tmp_path, raw):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        original = files.resource_grants()
        with pytest.raises(sqlite3.IntegrityError, match="ck_primary_format_resource_grants"):
            files._db().execute("UPDATE primary_format SET resource_grants=?", (raw,))
        assert files.resource_grants() == original


class UncertainCommit:
    def __init__(self, connection, committed):
        self.connection = connection
        self.committed = committed

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def execute(self, sql, *args):
        if sql == "COMMIT":
            if self.committed:
                self.connection.execute(sql).close()
            raise sqlite3.OperationalError("uncertain resource commit")
        return self.connection.execute(sql, *args)


@pytest.mark.parametrize("committed", [False, True])
def test_uncertain_increase_reconciles_one_durable_revision_on_reopen(tmp_path, committed):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        selected = files.put_bytes("original", (b"original",), byte_limit=8)
        previous = files.resource_grants()
        files._connection = UncertainCommit(files._connection, committed)
        with pytest.raises(PrimaryStateError, match="mutation failed"):
            files.increase_resource_grants(
                database_bytes=LARGER.database_bytes, journal_bytes=LARGER.journal_bytes
            )
        with pytest.raises(PrimaryStateError, match="uncertain"):
            files.resource_grants()
    with opened(path, limits=LARGER) as files:
        retained = files.resource_grants()
        assert retained.revision == previous.revision + int(committed)
        assert retained.database_bytes == (
            LARGER.database_bytes if committed else previous.database_bytes
        )
        assert files._limits.database_bytes == retained.database_bytes
        with files.verified(selected) as stream:
            assert stream.read() == b"original"


def test_exhausted_resource_revision_is_readable_but_cannot_wrap(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        raw = bytearray(files._one("SELECT resource_grants FROM primary_format")[0])
        raw[56:64] = b"\xff" * 8
        files._db().execute("UPDATE primary_format SET resource_grants=?", (bytes(raw),))
        original = files.resource_grants()
        changes = files._db().total_changes
        with pytest.raises(PrimaryStateCapacity, match="revision is exhausted"):
            files.increase_resource_grants(
                database_bytes=LARGER.database_bytes, journal_bytes=LARGER.journal_bytes
            )
        assert files.resource_grants() == original and files._db().total_changes == changes


@pytest.mark.parametrize("value", [True, 0, -4096, 4097, 1 << 63])
def test_unrepresentable_grants_are_rejected_before_sqlite_entry(value):
    with pytest.raises(ValueError, match="signed-64-bit"):
        SqlitePrimaryLimits(value, LIMITS.journal_bytes, LIMITS.cache_bytes)


@pytest.mark.parametrize("geometry", ["page_size", "auto_vacuum"])
def test_cold_open_refuses_persistent_geometry_changes_without_rewriting_state(tmp_path, geometry):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files.put_bytes("original", (b"original",), byte_limit=8)
    with sqlite3.connect(path) as db:
        if geometry == "page_size":
            db.execute("PRAGMA page_size=2048").close()
            db.execute("VACUUM").close()
        else:
            db.execute("PRAGMA auto_vacuum=1").close()
    original = hashlib.sha256(path.read_bytes()).digest()
    with pytest.raises(PrimaryStateError, match="persistent storage geometry"):
        opened(path)
    assert hashlib.sha256(path.read_bytes()).digest() == original
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT body FROM primary_chunks").fetchall() == [(b"original",)]


def test_accepted_cold_open_is_read_only_until_its_explicit_barrier(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files.put_bytes("original", (b"original",), byte_limit=8)
    original = hashlib.sha256(path.read_bytes()).digest()
    barrier = TransactionalPrimaryFiles.sync
    called = []

    def observed(files):
        assert hashlib.sha256(path.read_bytes()).digest() == original
        called.append(True)
        barrier(files)

    monkeypatch.setattr(TransactionalPrimaryFiles, "sync", observed)
    with opened(path) as files:
        assert files.resource_grants().revision == 0
    assert called == [True]
    assert hashlib.sha256(path.read_bytes()).digest() != original
