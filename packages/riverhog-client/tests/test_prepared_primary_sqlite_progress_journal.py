"""Physical progress-journal preparation, exact cursor recovery and refusal."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import replace

import pytest
from riverhog_client import PrimaryStateCapacity, PrimaryStateError
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits, TransactionalPrimaryFiles
from riverhog_client._prepared_primary_types import PrimaryFileSpace, PrimaryStateBusy

from tests.support.prepared_primary import SqlitePrimaryTestGuard

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SQL core isolation uses the Unix VFS")
LIMITS = SqlitePrimaryLimits(2 * 1024 * 1024, 1024 * 1024, 128 * 1024)
PLAN = dict(prefix_lengths=(8201, 16), workspace_bytes=65536)


def opened(path, *, initialize=False, limits=LIMITS):
    if initialize:
        path.touch()
    return TransactionalPrimaryFiles(
        SqlitePrimaryTestGuard(path),
        allocation_descriptor=b"owned progress journal",
        limits=limits,
        initialize=initialize,
    )


def finish(files, plan=PLAN):
    for _ in range(128):
        result = files.reserve_journal_step(**plan)
        if result.done:
            return result.reserve
    pytest.fail("fixture journal did not finish its explicit workspace")


def test_derived_minimum_requires_actual_backing_and_preserves_original_controls(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        files.prepare_controls(("receipt",), byte_limit=8192)
        files.put_control("receipt", b"exact original", byte_limit=8192)
        reserve = finish(files)
        envelope = files._guard.control_envelope((*PLAN["prefix_lengths"], 64))
        assert reserve.minimum_bytes == envelope.journal_bytes
        assert reserve.minimum_pages == envelope.database_pages
        assert reserve.backed_bytes >= reserve.minimum_bytes
        assert reserve.pad_rows == 16
        assert files._one("SELECT count(*) FROM primary_journal_pad") == (16,)
        space = files.space_usage()
        assert space.journal.end_bytes >= reserve.backed_bytes
        assert space.journal.allocated_bytes >= reserve.backed_bytes
        assert files.require_progress_journal() == reserve
        assert files.read_control("receipt", byte_limit=8192) == b"exact original"


def test_every_padding_cursor_quantum_survives_cold_reopen(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        previous = files.progress_journal()
    for _ in range(128):
        with opened(path) as files:
            assert files.progress_journal() == previous
            result = files.reserve_journal_step(**PLAN)
            assert result.reserve.pad_rows - previous.pad_rows in (0, 1)
            assert files._one("SELECT count(*) FROM primary_journal_pad") == (
                result.reserve.pad_rows,
            )
            previous = result.reserve
            if result.done:
                break
    else:
        pytest.fail("fixture cursor did not finish")
    with opened(path) as files:
        assert files.require_progress_journal() == previous


def test_status_and_repeated_prepared_call_are_read_only(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        before = files._db().total_changes
        assert files.progress_journal().phase == "unprepared"
        with pytest.raises(PrimaryStateCapacity, match="unavailable"):
            files.require_progress_journal()
        assert files._db().total_changes == before
        finish(files)
        before = files._db().total_changes
        assert files.reserve_journal_step(**PLAN).done
        files.require_progress_journal()
        assert files._db().total_changes == before


def test_insufficient_workspace_retains_padding_and_larger_grant_continues(tmp_path):
    path = tmp_path / "state.sqlite3"
    small = dict(prefix_lengths=(262153, 16), workspace_bytes=4096)
    with opened(path, initialize=True) as files:
        assert not files.reserve_journal_step(**small).done
        with pytest.raises(PrimaryStateCapacity, match="larger backed workspace"):
            files.reserve_journal_step(**small)
        assert files.progress_journal().phase == "building"
        assert files.progress_journal().pad_rows == 1
        assert files._one("SELECT length(body) FROM primary_journal_pad WHERE id=1") == (4096,)
    with opened(path) as files:
        reserve = finish(files, dict(small, workspace_bytes=512 * 1024))
        assert reserve.minimum_bytes > 256 * 1024
        assert reserve.backed_bytes >= reserve.minimum_bytes


def test_pending_candidate_and_payloads_are_neither_read_nor_advanced(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        original = files.put_bytes("original", (b"original",), byte_limit=8)

        def stopped():
            yield b"pending"
            raise RuntimeError("interrupted source")

        with pytest.raises(RuntimeError, match="interrupted source"):
            files.put_bytes("candidate", stopped(), byte_limit=8192)
    with opened(path) as files:
        pending = files._one("SELECT generation,cursor,phase FROM primary_operation")
        chunks = files._one("SELECT count(*) FROM primary_chunks")

        def authorize(action, table, *_rest):
            if action == sqlite3.SQLITE_READ and table in (
                "primary_chunks",
                "primary_objects",
                "primary_operation",
                "primary_names",
            ):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        files._db().set_authorizer(authorize)
        try:
            finish(files)
        finally:
            files._db().set_authorizer(None)
        assert files._one("SELECT generation,cursor,phase FROM primary_operation") == pending
        assert files._one("SELECT count(*) FROM primary_chunks") == chunks
        with files.verified(original) as stream:
            assert stream.read() == b"original"


@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize("phase", ["allocation", "warming", "readiness"])
def test_uncertain_commits_reconcile_padding_and_phase_without_guessing(tmp_path, committed, phase):
    class Uncertain:
        def __init__(self, db):
            self.db = db
            self.commits = 0

        def __getattr__(self, name):
            return getattr(self.db, name)

        def execute(self, sql, *args):
            if sql == "COMMIT":
                self.commits += 1
            if sql == "COMMIT" and self.commits == (2 if phase == "readiness" else 1):
                if committed:
                    self.db.execute(sql).close()
                raise sqlite3.OperationalError("uncertain journal commit")
            return self.db.execute(sql, *args)

    path = tmp_path / "state.sqlite3"
    plan = dict(prefix_lengths=(), workspace_bytes=16384)
    with opened(path, initialize=True) as files:
        if phase != "allocation":
            for _ in range(4):
                assert not files.reserve_journal_step(**plan).done
        files._connection = Uncertain(files._connection)
        with pytest.raises(PrimaryStateError, match="mutation failed"):
            files.reserve_journal_step(**plan)
        with pytest.raises(PrimaryStateError, match="uncertain"):
            files.progress_journal()
    with opened(path) as files:
        reserve = files.progress_journal()
        assert reserve.pad_rows == (int(committed) if phase == "allocation" else 4)
        assert files._one("SELECT count(*) FROM primary_journal_pad") == (reserve.pad_rows,)
        assert (reserve.phase == "prepared") is (phase == "readiness" and committed)
        finish(files, plan)


def test_missing_padding_stops_without_adopting_or_forgetting_the_cursor(tmp_path):
    path = tmp_path / "state.sqlite3"
    plan = dict(prefix_lengths=(), workspace_bytes=4096)
    with opened(path, initialize=True) as files:
        files.reserve_journal_step(**plan)
        previous = files.progress_journal()
        files._db().execute("DELETE FROM primary_journal_pad WHERE id=1").close()
        with pytest.raises(PrimaryStateError, match="mutation failed"):
            files.reserve_journal_step(**plan)
    with opened(path) as files:
        assert files.progress_journal() == previous
        assert files._one("SELECT count(*) FROM primary_journal_pad") == (0,)


@pytest.mark.parametrize("kind", ["warming", "admission", "after-readiness"])
def test_eof_without_allocated_backing_cannot_publish_or_admit_prepared(
    tmp_path, monkeypatch, kind
):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        if kind == "admission":
            finish(files)
        else:
            for _ in range(16):
                files.reserve_journal_step(**PLAN)
        actual = files._guard.file_space

        def unbacked():
            database, journal = actual()
            depleted = kind != "after-readiness" or files.progress_journal().phase == "prepared"
            return database, PrimaryFileSpace(
                journal.end_bytes, 0 if depleted else journal.allocated_bytes
            )

        monkeypatch.setattr(files._guard, "file_space", unbacked)
        with pytest.raises(PrimaryStateCapacity):
            if kind == "admission":
                files.require_progress_journal()
            else:
                files.reserve_journal_step(**PLAN)
        assert (
            files.progress_journal().phase
            == {"warming": "building", "admission": "prepared", "after-readiness": "depleted"}[kind]
        )
        monkeypatch.setattr(files._guard, "file_space", actual)
        finish(files)


def test_control_preparation_preserves_the_independent_progress_floor(tmp_path, monkeypatch):
    from riverhog_client import _prepared_primary_sqlite_journal as journal

    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        reserve = finish(files)
        limit = journal._limit

        def checked(db, value):
            assert value == -1 or value >= reserve.backed_bytes
            limit(db, value)

        monkeypatch.setattr(journal, "_limit", checked)
        files.prepare_controls(("receipt",), byte_limit=8192)
        files.put_control("receipt", b"original", byte_limit=8192)
        assert files.require_progress_journal() == reserve
        assert files.journal_reserved_bytes() >= reserve.backed_bytes


def test_resource_increase_preserves_the_floor_and_requires_explicit_reconciliation(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        previous = finish(files)
        files.increase_resource_grants(
            database_bytes=2 * LIMITS.database_bytes, journal_bytes=2 * LIMITS.journal_bytes
        )
        assert files.progress_journal() == replace(previous, phase="depleted", grant_revision=1)
        assert files.journal_reserved_bytes() == previous.backed_bytes
        with pytest.raises(PrimaryStateCapacity, match="unavailable"):
            files.require_progress_journal()
        assert finish(files).grant_revision == 1


@pytest.mark.parametrize("corruption", ["tag", "phase", "revision", "cursor"])
def test_invalid_retained_profile_rejects_cold_open_before_barrier(tmp_path, corruption):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files.put_bytes("original", (b"original",), byte_limit=8)
        raw = bytearray(files._one("SELECT progress_journal FROM primary_format")[0])
    start = {"tag": 0, "phase": 8, "revision": 16, "cursor": 40}[corruption]
    raw[start] = 99
    with sqlite3.connect(path) as db:
        db.execute("UPDATE primary_format SET progress_journal=?", (bytes(raw),))
    before = hashlib.sha256(path.read_bytes()).digest()
    with pytest.raises(PrimaryStateError, match="progress journal"):
        opened(path)
    assert hashlib.sha256(path.read_bytes()).digest() == before


def test_retention_below_derived_minimum_stops_before_any_padding_allocation(tmp_path):
    with opened(
        tmp_path / "state.sqlite3",
        initialize=True,
        limits=SqlitePrimaryLimits(LIMITS.database_bytes, 4096, LIMITS.cache_bytes),
    ) as files:
        with pytest.raises(PrimaryStateCapacity, match="retained grants"):
            files.reserve_journal_step(**PLAN)
        assert files.progress_journal().phase == "unprepared"
        assert files._one("SELECT count(*) FROM primary_journal_pad") == (0,)


def test_children_and_unsettled_transactions_cannot_prepare_the_root_reserve(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        child = files.child("child", initialize=True)
        with pytest.raises(PrimaryStateError, match="root"):
            child.reserve_journal_step(**PLAN)
        with files._transaction():
            with pytest.raises(PrimaryStateBusy, match="settled"):
                files.reserve_journal_step(**PLAN)


def test_wider_minimum_needs_explicit_workspace_without_lowering_existing_backing(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        previous = finish(files)
        wider = dict(prefix_lengths=(262153, 16), workspace_bytes=65536)
        with pytest.raises(PrimaryStateCapacity, match="larger backed workspace"):
            files.reserve_journal_step(**wider)
        assert files.progress_journal().backed_bytes == previous.backed_bytes
        assert files.progress_journal().minimum_bytes > previous.minimum_bytes
        with pytest.raises(ValueError, match="lower"):
            files.reserve_journal_step(**dict(PLAN, workspace_bytes=4096))
        reserve = finish(files, dict(wider, workspace_bytes=512 * 1024))
        assert reserve.backed_bytes > previous.backed_bytes


def test_lost_cold_journal_defers_input_until_exact_padding_is_rewarmed(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        original = files.put_bytes("original", (b"original",), byte_limit=8)
        previous = finish(files)
    # Damage only the closed, committed, non-hot test fixture.
    path.with_name(path.name + "-journal").write_bytes(b"")
    with opened(path) as files:
        with pytest.raises(PrimaryStateCapacity, match="unavailable"):
            files.require_progress_journal()
        consumed = []

        def payload():
            consumed.append(True)
            yield b"new"

        with pytest.raises(PrimaryStateCapacity, match="journal backing"):
            files.put_bytes("new", payload(), byte_limit=3)
        assert not consumed
        reserve = finish(files)
        assert reserve.pad_rows == previous.pad_rows
        assert reserve.backed_bytes >= previous.backed_bytes
        with files.verified(original) as stream:
            assert stream.read() == b"original"
