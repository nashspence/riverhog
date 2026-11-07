"""Physical dependency barriers and payload isolation from progress headroom."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import replace

import pytest
from riverhog_client import PrimaryStateCapacity, PrimaryStateError
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits, TransactionalPrimaryFiles
from riverhog_client._prepared_primary_types import PrimaryFileSpace

from tests.support.prepared_primary import SqlitePrimaryTestGuard, ordered_primary_rows

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SQL core isolation uses the Unix VFS")
LIMITS = SqlitePrimaryLimits(2 * 1024 * 1024, 1024 * 1024, 128 * 1024)
JOURNAL = dict(prefix_lengths=(8201, 16), workspace_bytes=65536)


def opened(path, *, initialize=False, limits=LIMITS):
    if initialize:
        path.touch()
    return TransactionalPrimaryFiles(
        SqlitePrimaryTestGuard(path),
        allocation_descriptor=b"owned physical ready",
        limits=limits,
        initialize=initialize,
    )


def finish_database(files, plan):
    for _ in range(256):
        result = files.reserve_database_step(**plan)
        if result.done:
            return result.reserve
    pytest.fail("fixture database reserve did not finish")


def finish_journal(files):
    for _ in range(32):
        if files.reserve_journal_step(**JOURNAL).done:
            return
    pytest.fail("fixture journal reserve did not finish")


def dependencies(files):
    finish_journal(files)
    plan = dict(
        free_pages=8,
        target_pages=files.space_usage().database_pages + 32,
        workspace_bytes=128 * 1024,
    )
    finish_database(files, plan)
    return plan


def test_ready_covers_both_backings_and_atomic_grant_update_minimum(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        files.prepare_controls(("receipt",), byte_limit=8192)
        files.put_control("receipt", b"original receipt", byte_limit=8192)
        dependencies(files)
        ready = files.prepare_physical_ready()
        assert ready.phase == "ready"
        assert ready.backed_pages == files.database_reserve().backed_pages
        assert ready.free_pages == 8
        assert ready.journal_bytes == files.progress_journal().backed_bytes
        assert ready.minimum_journal_bytes >= files.progress_journal().minimum_bytes
        envelope = files._guard.control_envelope((64, 72, 64, 64, 184))
        assert ready.minimum_journal_bytes >= envelope.journal_bytes
        assert ready.minimum_pages >= envelope.database_pages
        assert ready.journal_bytes >= ready.minimum_journal_bytes
        assert files.require_physical_ready() == ready
        assert files.read_control("receipt", byte_limit=8192) == b"original receipt"


def test_status_never_prepares_and_ready_noop_performs_no_writes(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        changes = files._db().total_changes
        assert files.physical_readiness().phase == "unprepared"
        with pytest.raises(PrimaryStateCapacity, match="preparation"):
            files.require_physical_ready()
        assert files._db().total_changes == changes
        dependencies(files)
        ready = files.prepare_physical_ready()
        changes = files._db().total_changes
        assert files.prepare_physical_ready() == ready
        assert files.require_physical_ready() == ready
        assert files._db().total_changes == changes


@pytest.mark.parametrize("missing", ["database", "journal", "both"])
def test_missing_dependency_keeps_preparing_and_cannot_claim_ready(tmp_path, missing):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        if missing == "database":
            finish_journal(files)
        elif missing == "journal":
            finish_database(
                files,
                dict(
                    free_pages=8,
                    target_pages=files.space_usage().database_pages + 16,
                    workspace_bytes=65536,
                ),
            )
        with pytest.raises(PrimaryStateCapacity):
            files.prepare_physical_ready()
        assert files.physical_readiness().phase == "preparing"
        with pytest.raises(PrimaryStateCapacity):
            files.require_physical_ready()


@pytest.mark.parametrize("operation", ["candidate", "namespace", "ordering"])
def test_selected_preparation_blocks_new_payload_before_input(tmp_path, operation):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files.reserve_journal_step(**JOURNAL)
        consumed = []

        def payload():
            consumed.append(True)
            yield b"new"

        def rows():
            consumed.append(True)
            yield "member", 3, "a" * 64

        with pytest.raises(PrimaryStateCapacity, match="readiness"):
            if operation == "candidate":
                files.put_bytes("new", payload(), byte_limit=3)
            elif operation == "namespace":
                files.child("new", initialize=True)
            else:
                list(ordered_primary_rows(files, rows(), byte_limit=65536))
        assert not consumed
    with opened(path) as files:
        assert not files.exists("new")
        assert files._one("SELECT generation FROM primary_operation") is None


def test_payload_quantum_uses_backed_ceiling_and_restores_owner_grant_after_commit(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        dependencies(files)
        ready = files.prepare_physical_ready()
        with files._transaction(payload=True):
            assert files._one("PRAGMA max_page_count") == (ready.backed_pages,)
        assert files._one("PRAGMA max_page_count") == (LIMITS.database_bytes // 4096,)
        original = files.put_bytes("new", (b"confirmed new" * 500,), byte_limit=6500)
        assert files.require_physical_ready() == ready
        assert files.space_usage().reusable_pages >= ready.free_pages
        with files.verified(original) as stream:
            assert stream.read() == b"confirmed new" * 500


def test_precommit_floor_refusal_rolls_back_chunk_and_preserves_original_and_cursor(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        original = files.put_bytes("original", (b"confirmed original",), byte_limit=18)
        plan = dependencies(files)
        plan["free_pages"] = files.space_usage().reusable_pages
        finish_database(files, plan)
        ready = files.prepare_physical_ready()
        before = files.space_usage()
        assert before.reusable_pages == ready.free_pages
        with pytest.raises(PrimaryStateCapacity):
            files.put_bytes("new", (b"x" * 8192,), byte_limit=8192)
    with opened(path) as files:
        assert not files.exists("new")
        assert files.space_usage().database_pages == before.database_pages
        assert files.space_usage().reusable_pages >= ready.free_pages
        pending = files._one("SELECT generation,cursor,phase FROM primary_operation")
        if pending is not None:
            assert pending[1:] == (0, "writing")
            assert files._one(
                "SELECT count(*) FROM primary_chunks WHERE generation=?", (pending[0],)
            ) == (0,)
        with files.verified(original) as stream:
            assert stream.read() == b"confirmed original"
        # Replenishment and grant increases do not require candidate cleanup.
        files.increase_resource_grants(
            database_bytes=4 * 1024 * 1024, journal_bytes=2 * 1024 * 1024
        )
        assert files._one("SELECT generation,cursor,phase FROM primary_operation") == pending


@pytest.mark.parametrize("kind", ["database", "journal"])
def test_point_admission_rechecks_real_backing_without_mutating_state(tmp_path, monkeypatch, kind):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        dependencies(files)
        ready = files.prepare_physical_ready()
        actual = files._guard.file_space

        def unbacked():
            database, journal = actual()
            if kind == "database":
                database = PrimaryFileSpace(database.end_bytes, 0)
            else:
                journal = PrimaryFileSpace(journal.end_bytes, 0)
            return database, journal

        monkeypatch.setattr(files._guard, "file_space", unbacked)
        changes = files._db().total_changes
        with pytest.raises(PrimaryStateCapacity, match="backing"):
            files.require_physical_ready()
        assert files._db().total_changes == changes
        monkeypatch.setattr(files._guard, "file_space", actual)
        assert files.require_physical_ready() == ready


def test_dependency_and_maintenance_changes_invalidate_ready_atomically(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        plan = dependencies(files)
        files.prepare_physical_ready()
        files.vacuum_step(keep_free_pages=0)
        assert files.physical_readiness().phase == "depleted"
        with pytest.raises(PrimaryStateCapacity):
            files.require_physical_ready()
        finish_database(files, plan)
        files.prepare_physical_ready()
        files.prepare_controls(("new-control",), byte_limit=8192)
        assert files.physical_readiness().phase == "depleted"


def test_grant_increase_retains_profile_obligations_and_requires_reconciliation(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        plan = dependencies(files)
        previous = files.prepare_physical_ready()
        files.increase_resource_grants(
            database_bytes=2 * LIMITS.database_bytes, journal_bytes=2 * LIMITS.journal_bytes
        )
        assert files.physical_readiness() == replace(previous, phase="depleted", grant_revision=1)
        with pytest.raises(PrimaryStateCapacity):
            files.require_physical_ready()
        finish_journal(files)
        finish_database(files, plan)
        assert files.prepare_physical_ready().grant_revision == 1


@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize("phase", ["preparing", "ready"])
def test_uncertain_barrier_commit_reconciles_durable_phase_on_reopen(tmp_path, phase, committed):
    class Uncertain:
        def __init__(self, db):
            self.db, self.commits = db, 0

        def __getattr__(self, name):
            return getattr(self.db, name)

        def execute(self, sql, *args):
            if sql == "COMMIT":
                self.commits += 1
            if sql == "COMMIT" and self.commits == (1 if phase == "preparing" else 2):
                if committed:
                    self.db.execute(sql).close()
                raise sqlite3.OperationalError("uncertain readiness commit")
            return self.db.execute(sql, *args)

    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        dependencies(files)
        files._connection = Uncertain(files._connection)
        with pytest.raises(PrimaryStateError, match="mutation failed"):
            files.prepare_physical_ready()
    with opened(path) as files:
        expected = (
            ("preparing" if committed else "unprepared")
            if phase == "preparing"
            else ("ready" if committed else "preparing")
        )
        assert files.physical_readiness().phase == expected
        assert files.prepare_physical_ready().phase == "ready"


def test_readiness_status_does_not_read_payload_or_candidate_tables(tmp_path):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        dependencies(files)
        ready = files.prepare_physical_ready()

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
            assert files.require_physical_ready() == ready
        finally:
            files._db().set_authorizer(None)


@pytest.mark.parametrize("corruption", ["tag", "phase", "revision", "profile"])
def test_invalid_readiness_profile_rejects_cold_open_before_barrier(tmp_path, corruption):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        raw = bytearray(files._one("SELECT physical_readiness FROM primary_format")[0])
    start = {"tag": 0, "phase": 8, "revision": 16, "profile": 24}[corruption]
    raw[start] = 99
    with sqlite3.connect(path) as db:
        db.execute("UPDATE primary_format SET physical_readiness=?", (bytes(raw),))
    before = hashlib.sha256(path.read_bytes()).digest()
    with pytest.raises(PrimaryStateError, match="readiness"):
        opened(path)
    assert hashlib.sha256(path.read_bytes()).digest() == before


def test_readiness_minimum_must_fit_verified_floor_before_ready_commit(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True):
        pass
    # Remove only historical space from a closed, committed non-hot test journal.
    path.with_name(path.name + "-journal").write_bytes(b"")
    small = dict(prefix_lengths=(), workspace_bytes=16384)
    with opened(path) as files:
        for _ in range(8):
            if files.reserve_journal_step(**small).done:
                break
        else:
            pytest.fail("fixture small journal did not prepare")
        finish_database(
            files,
            dict(
                free_pages=0, target_pages=files.space_usage().database_pages, workspace_bytes=4096
            ),
        )
        required = files._guard.control_envelope((64, 72, 64, 64, 184)).journal_bytes
        assert files.progress_journal().backed_bytes < required
        with pytest.raises(PrimaryStateCapacity, match="exceeds journal backing"):
            files.prepare_physical_ready()
        assert files.physical_readiness().phase == "preparing"
        finish_journal(files)
        # Growing the pad changes database backing too; reconcile both dependencies.
        finish_database(
            files,
            dict(
                free_pages=0, target_pages=files.space_usage().database_pages, workspace_bytes=4096
            ),
        )
        assert files.prepare_physical_ready().minimum_journal_bytes >= required


def test_post_ready_commit_backing_loss_leaves_depleted_until_explicit_recheck(
    tmp_path, monkeypatch
):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        dependencies(files)
        actual = files._guard.file_space

        def lost_after_ready():
            database, journal = actual()
            if files.physical_readiness().phase == "ready":
                database = PrimaryFileSpace(database.end_bytes, 0)
            return database, journal

        monkeypatch.setattr(files._guard, "file_space", lost_after_ready)
        with pytest.raises(PrimaryStateCapacity, match="backing"):
            files.prepare_physical_ready()
        assert files.physical_readiness().phase == "depleted"
        monkeypatch.setattr(files._guard, "file_space", actual)
        assert files.prepare_physical_ready().phase == "ready"


def test_transaction_write_instrumentation_preserves_payload_admission_and_failure(tmp_path):
    from contextlib import nullcontext
    from types import SimpleNamespace

    from tests.support.prepared_primary_transaction_writes import TransactionWriteTotals

    class SyntheticShim:
        """Test wrapper forwarding; native qualification owns actual write measurements."""

        def transaction_writes(self, *, page_bytes, tracked_pages):
            assert page_bytes == 4096 and tracked_pages == 128
            return nullcontext(SimpleNamespace(report=lambda: {}))

    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        original = files.put_bytes("original", (b"original",), byte_limit=8)
        plan = dependencies(files)
        plan["free_pages"] = files.space_usage().reusable_pages
        finish_database(files, plan)
        ready = files.prepare_physical_ready()
        totals = TransactionWriteTotals(tracked_pages=128)
        totals.attach(files, SyntheticShim())
        with files._transaction(payload=True):
            assert files._one("PRAGMA max_page_count") == (ready.backed_pages,)
        files.sync()
        with pytest.raises(PrimaryStateCapacity, match="progress headroom"):
            files.put_bytes("new", (b"x" * 8192,), byte_limit=8192)
        assert totals.report()["completed_transactions"] >= 2
        assert totals.report()["failed_transactions"] == 1
    with opened(path) as files:
        assert not files.exists("new")
        assert files.space_usage().reusable_pages >= ready.free_pages
        with files.verified(original) as stream:
            assert stream.read() == b"original"
