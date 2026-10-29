"""Bounded, resumable SQL headroom preparation under retained resource grants."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import replace

import pytest
from riverhog_client import PrimaryStateCapacity, PrimaryStateError
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits, TransactionalPrimaryFiles
from riverhog_client._prepared_primary_types import PrimaryFileSpace

from tests.support.prepared_primary import SqlitePrimaryTestGuard

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SQL core isolation uses the Unix VFS")
LIMITS = SqlitePrimaryLimits(2 * 1024 * 1024, 4 * 1024 * 1024, 128 * 1024)


def opened(path, *, initialize=False, limits=LIMITS):
    if initialize:
        path.touch()
    return TransactionalPrimaryFiles(
        SqlitePrimaryTestGuard(path),
        allocation_descriptor=b"owned database reserve",
        limits=limits,
        initialize=initialize,
    )


def plan(files, *, free_pages=4, extra_pages=8, workspace_bytes=65536):
    return dict(
        free_pages=free_pages,
        target_pages=files.space_usage().database_pages + extra_pages,
        workspace_bytes=workspace_bytes,
    )


def finish(files, selected):
    results = []
    for _ in range(128):
        result = files.reserve_database_step(**selected)
        results.append(result)
        if result.done:
            return results
    pytest.fail("fixture reserve did not finish within its explicit small profile")


def test_each_step_mutates_one_known_row_and_retains_backing_after_release(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = plan(files)
        original = files.put_bytes("original", (b"confirmed original",), byte_limit=18)
        previous = files._one("SELECT count(*) FROM primary_reserve_rows")[0]
        observed = []
        files._db().set_trace_callback(observed.append)
        results = finish(files, selected)
        for result in results:
            assert (
                result.reserve.inserted - result.reserve.released
                <= selected["workspace_bytes"] // 4096
            )
        mutations = [
            sql
            for sql in observed
            if sql.startswith(
                ("INSERT INTO primary_reserve_rows", "DELETE FROM primary_reserve_rows")
            )
        ]
        assert len(mutations) == results[-1].reserve.inserted + results[-1].reserve.released
        assert results[-1].reserve.inserted == results[-1].reserve.released
        assert files._one("SELECT count(*) FROM primary_reserve_rows") == (previous,)
        assert results[-1].space.reusable_pages >= selected["free_pages"]
        assert results[-1].space.database_pages >= selected["target_pages"]
        assert results[-1].space.database.allocated_bytes >= results[-1].space.database.end_bytes
        assert files.require_database_reserve() == results[-1].reserve
        changes = files._db().total_changes
        assert files.reserve_database_step(**selected).done
        assert files._db().total_changes == changes
        with files.verified(original) as stream:
            assert stream.read() == b"confirmed original"


def test_allocation_and_deletion_cursors_resume_exactly_after_every_reopen(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        selected = plan(files)
        current = files.database_reserve()
    for _ in range(128):
        with opened(path) as files:
            assert files.database_reserve() == current
            before = files._one("SELECT count(*) FROM primary_reserve_rows")[0]
            result = files.reserve_database_step(**selected)
            after = files._one("SELECT count(*) FROM primary_reserve_rows")[0]
            assert -1 <= after - before <= 1
            assert result.reserve.inserted - current.inserted in (0, 1)
            assert result.reserve.released - current.released in (0, 1)
            assert (result.reserve.inserted - current.inserted) + (
                result.reserve.released - current.released
            ) <= 1
            current = result.reserve
            if result.done:
                break
    else:
        pytest.fail("fixture cursor did not finish")
    with opened(path) as files:
        assert files.require_database_reserve() == current


def test_workspace_increase_preserves_live_rows_and_pending_candidate(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        original = files.put_bytes("original", (b"original",), byte_limit=8)

        def interrupted():
            yield b"pending original"
            raise RuntimeError("source stopped")

        with pytest.raises(RuntimeError, match="source stopped"):
            files.put_bytes("candidate", interrupted(), byte_limit=8192)
    with opened(path) as files:
        pending = files._one("SELECT generation,cursor,phase FROM primary_operation")
        chunks = files._one("SELECT count(*) FROM primary_chunks")
        selected = plan(files, workspace_bytes=4096)
        first = files.reserve_database_step(**selected)
        assert first.reserve.inserted == 1
        with pytest.raises(PrimaryStateCapacity, match="larger workspace"):
            files.reserve_database_step(**selected)
        assert files.database_reserve() == first.reserve
        selected["workspace_bytes"] = 65536
        finish(files, selected)
        assert files._one("SELECT generation,cursor,phase FROM primary_operation") == pending
        assert files._one("SELECT count(*) FROM primary_chunks") == chunks
        with files.verified(original) as stream:
            assert stream.read() == b"original"


def test_status_and_admission_do_not_prepare_or_consume_a_reserve(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        before = files._db().total_changes
        assert files.database_reserve().phase == "unprepared"
        with pytest.raises(PrimaryStateCapacity, match="unavailable"):
            files.require_database_reserve()
        assert files._db().total_changes == before
        assert files._one("SELECT count(*) FROM primary_reserve_rows") == (0,)


def test_claimed_eof_without_observed_allocation_cannot_complete_preparation(tmp_path, monkeypatch):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = plan(files, free_pages=0, extra_pages=0, workspace_bytes=4096)
        observed = files._guard.file_space

        def unbacked():
            database, journal = observed()
            return PrimaryFileSpace(database.end_bytes, 0), journal

        monkeypatch.setattr(files._guard, "file_space", unbacked)
        result = files.reserve_database_step(**selected)
        assert not result.done and result.reserve.phase == "building"
        with pytest.raises(PrimaryStateCapacity, match="unavailable"):
            files.require_database_reserve()
        monkeypatch.setattr(files._guard, "file_space", observed)
        assert finish(files, selected)[-1].done


def test_post_commit_backing_loss_marks_depleted_before_refusing(tmp_path, monkeypatch):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = plan(files, free_pages=0, extra_pages=0)
        observed = files._guard.file_space
        calls = []

        def lost_after_barrier():
            database, journal = observed()
            calls.append(True)
            if len(calls) == 2:
                database = PrimaryFileSpace(database.end_bytes, 0)
            return database, journal

        monkeypatch.setattr(files._guard, "file_space", lost_after_barrier)
        with pytest.raises(PrimaryStateCapacity, match="lost backing"):
            files.reserve_database_step(**selected)
        assert files.database_reserve().phase == "depleted"
        monkeypatch.setattr(files._guard, "file_space", observed)
        assert finish(files, selected)[-1].done


def test_resource_increase_keeps_cursor_and_requires_explicit_backing_reconciliation(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = plan(files)
        finish(files, selected)
        previous = files.database_reserve()
        files.increase_resource_grants(
            database_bytes=2 * LIMITS.database_bytes, journal_bytes=2 * LIMITS.journal_bytes
        )
        retained = files.database_reserve()
        assert retained == replace(
            previous, phase="depleted", grant_revision=previous.grant_revision + 1
        )
        with pytest.raises(PrimaryStateCapacity, match="unavailable"):
            files.require_database_reserve()
        assert finish(files, selected)[-1].done


@pytest.mark.parametrize("field", ["free_pages", "target_pages", "workspace_bytes"])
def test_preparation_cannot_lower_retained_reserve_grants(tmp_path, field):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = plan(files)
        files.reserve_database_step(**selected)
        before = files.database_reserve()
        selected[field] -= 1 if field != "workspace_bytes" else 4096
        with pytest.raises(ValueError, match="cannot lower"):
            files.reserve_database_step(**selected)
        assert files.database_reserve() == before


class UncertainCommit:
    def __init__(self, db, committed):
        self.db, self.committed = db, committed

    def __getattr__(self, name):
        return getattr(self.db, name)

    def execute(self, sql, *args):
        if sql == "COMMIT":
            if self.committed:
                self.db.execute(sql).close()
            raise sqlite3.OperationalError("uncertain reserve commit")
        return self.db.execute(sql, *args)


@pytest.mark.parametrize("committed", [False, True])
def test_uncertain_quantum_reconciles_its_row_and_cursor_together(tmp_path, committed):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        selected = plan(files)
        files._connection = UncertainCommit(files._connection, committed)
        with pytest.raises(PrimaryStateError, match="mutation failed"):
            files.reserve_database_step(**selected)
    with opened(path) as files:
        assert files.database_reserve().inserted == int(committed)
        assert files._one("SELECT count(*) FROM primary_reserve_rows") == (int(committed),)
        assert finish(files, selected)[-1].done


def test_missing_cursor_row_is_not_guessed_or_forgotten(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        selected = plan(files, free_pages=1, extra_pages=1, workspace_bytes=4096)
        current = files.reserve_database_step(**selected).reserve
        assert current.inserted == 1
        files._db().execute("DELETE FROM primary_reserve_rows WHERE id=1").close()
        with pytest.raises(PrimaryStateError, match="cursor lost its row"):
            files.reserve_database_step(**selected)
    with opened(path) as files:
        assert files.database_reserve() == current


@pytest.mark.parametrize("corruption", ["tag", "phase", "revision", "cursor"])
def test_invalid_reserve_record_stops_cold_open_without_recreating_state(tmp_path, corruption):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files.put_bytes("original", (b"original",), byte_limit=8)
        raw = bytearray(files._one("SELECT database_reserve FROM primary_format")[0])
    if corruption == "tag":
        raw[0] ^= 1
    elif corruption == "phase":
        raw[8:16] = (99).to_bytes(8, "big")
    elif corruption == "revision":
        raw[16:24] = (1).to_bytes(8, "big")
    else:
        raw[56:64] = (1).to_bytes(8, "big")
    with sqlite3.connect(path) as db:
        db.execute("UPDATE primary_format SET database_reserve=?", (bytes(raw),))
    before = hashlib.sha256(path.read_bytes()).digest()
    with pytest.raises(PrimaryStateError, match="database reserve"):
        opened(path)
    assert hashlib.sha256(path.read_bytes()).digest() == before
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT body FROM primary_chunks").fetchall() == [(b"original",)]


def test_vacuum_preserves_retained_free_floor_and_invalidates_the_old_backed_ceiling(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = plan(files, free_pages=2, extra_pages=12)
        finish(files, selected)
        assert files.space_usage().reusable_pages > selected["free_pages"]
        for _ in range(128):
            result = files.vacuum_step(keep_free_pages=0)
            assert result.after.reusable_pages >= selected["free_pages"]
            if result.done:
                break
        else:
            pytest.fail("fixture vacuum did not finish")
        assert files.database_reserve().phase == "depleted"
        with pytest.raises(PrimaryStateCapacity, match="unavailable"):
            files.require_database_reserve()
        assert finish(files, selected)[-1].done


def test_preparation_never_reads_archive_payloads_or_another_candidate(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = plan(files)

        def authorize(action, table, column, *_rest):
            if action == sqlite3.SQLITE_READ and table in (
                "primary_chunks",
                "primary_objects",
                "primary_operation",
                "primary_names",
            ):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        files._db().set_authorizer(authorize)
        assert finish(files, selected)[-1].done


def test_preparation_refuses_an_active_transaction_before_mutating_its_cursor(tmp_path):
    from riverhog_client import PrimaryStateBusy

    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = plan(files)
        original = files.database_reserve()
        files._db().execute("BEGIN IMMEDIATE").close()
        try:
            before = files._db().total_changes
            with pytest.raises(PrimaryStateBusy, match="settled transaction"):
                files.reserve_database_step(**selected)
            assert files._db().total_changes == before
        finally:
            files._db().rollback()
        assert files.database_reserve() == original


def test_preparation_requires_the_container_root(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = plan(files)
        child = files.child("member", initialize=True)
        with pytest.raises(PrimaryStateError, match="container root"):
            child.reserve_database_step(**selected)
        assert files.database_reserve().phase == "unprepared"


@pytest.mark.parametrize("field", ["free_pages", "target_pages"])
def test_preparation_rejects_targets_beyond_the_retained_database_grant(tmp_path, field):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = plan(files)
        selected[field] = LIMITS.database_bytes // 4096 + 1
        previous = files.database_reserve()
        with pytest.raises(PrimaryStateCapacity, match="exceeds its database grant"):
            files.reserve_database_step(**selected)
        assert files.database_reserve() == previous
        assert files._one("SELECT count(*) FROM primary_reserve_rows") == (0,)


def test_prepared_record_does_not_replace_a_current_backing_observation(tmp_path, monkeypatch):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = plan(files)
        finish(files, selected)
        previous = files.database_reserve()
        observed = files._guard.file_space

        def lost():
            database, journal = observed()
            return PrimaryFileSpace(database.end_bytes, 0), journal

        monkeypatch.setattr(files._guard, "file_space", lost)
        changes = files._db().total_changes
        with pytest.raises(PrimaryStateCapacity, match="unavailable"):
            files.require_database_reserve()
        assert files.database_reserve() == previous and files._db().total_changes == changes
