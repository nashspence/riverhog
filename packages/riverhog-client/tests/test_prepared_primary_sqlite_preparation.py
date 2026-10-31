"""Real-SQL composition, refusal ordering and restart of bounded reserve execution."""

from __future__ import annotations

import os
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_client import PrimaryStateCapacity, PrimaryStateError
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits, TransactionalPrimaryFiles
from riverhog_client._prepared_primary_sqlite_native import start
from riverhog_client._prepared_primary_sqlite_operations import (
    SqliteOperationBank,
    SqliteOperationGrants,
)
from riverhog_client._prepared_primary_sqlite_preparation import SqliteReadinessPlan
from riverhog_client._prepared_primary_types import PrimaryStateBusy

from tests.support.prepared_primary import SqlitePrimaryTestGuard

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SQL composition uses the Unix VFS")
LIMITS = SqlitePrimaryLimits(4 * 1024 * 1024, 2 * 1024 * 1024, 128 * 1024)
BANK = SqliteOperationBank(
    1024, 8 * 1024 * 1024, 65536, 8 * 1024 * 1024, 65536, 262144, 0, 1 << 40, 1 << 60, 1 << 40
)
GRANTS = SqliteOperationGrants(BANK, BANK)


class Guard(SqlitePrimaryTestGuard):
    def __init__(self, path):
        super().__init__(path)
        self.arms = 0

    def arm_operation(self, *_args, **_kwargs):
        self.arms += 1
        return SimpleNamespace()


def opened(path, *, initialize=False):
    if initialize:
        path.touch(exist_ok=False)
    guard = Guard(path)
    files = TransactionalPrimaryFiles(
        guard,
        allocation_descriptor=b"compound preparation",
        limits=LIMITS,
        initialize=initialize,
        operations=GRANTS,
    )
    return start(files, guard), guard


def plan_for(files):
    return SqliteReadinessPlan(
        prefix_lengths=(8201, 16),
        journal_workspace_bytes=65536,
        free_pages=8,
        target_pages=files.space_usage().database_pages + 32,
        database_workspace_bytes=128 * 1024,
    )


def drain(files, plan):
    stages = set()
    for _ in range(256):
        before_journal, before_database = files.progress_journal(), files.database_reserve()
        result = files.prepare_payload_step(plan)
        stages.add(result.stage)
        journal, database = files.progress_journal(), files.database_reserve()
        assert journal.pad_rows - before_journal.pad_rows <= 1
        assert database.inserted - before_database.inserted <= 1
        assert database.released - before_database.released <= 1
        if result.done:
            assert result.stage == "ready"
            return stages
    pytest.fail("fixture preparation did not finish")


def test_compound_preparation_advances_cursors_and_blocks_payload_before_ready(tmp_path):
    files, guard = opened(tmp_path / "state.sqlite3", initialize=True)
    with files:
        files.prepare_controls(("receipt",), byte_limit=8192)
        plan = plan_for(files)
        first = files.prepare_payload_step(plan)
        assert first.stage == "journal" and not first.done
        assert files.progress_journal().pad_rows == 1
        assert files.database_reserve().phase == "unprepared"
        consumed = []

        def source():
            consumed.append(True)
            yield b"payload"

        arms = guard.arms
        with pytest.raises(PrimaryStateCapacity, match="readiness"):
            files.put_bytes("payload", source(), byte_limit=7)
        assert not consumed and guard.arms == arms and not files._poisoned
        assert drain(files, plan) == {"journal", "database", "ready"}
        ready = files.require_physical_ready()
        assert ready.minimum_pages >= 16 and ready.minimum_journal_bytes >= 73872
        changes, arms = files._db().total_changes, guard.arms
        repeated = files.prepare_payload_step(plan)
        assert repeated.done and repeated.stage == "ready"
        assert files._db().total_changes == changes and guard.arms == arms
        stored = files.put_bytes("payload", (b"payload",), byte_limit=7)
        files.prepare_controls(("later-control",), byte_limit=8192)
        assert files.physical_readiness().phase == "depleted"
        drain(files, plan)
        assert files.require_physical_ready().phase == "ready"
        with files.verified(stored) as stream:
            assert stream.read() == b"payload"


def test_compound_preparation_cold_resume_preserves_pending_and_original_bytes(tmp_path):
    path = tmp_path / "state.sqlite3"
    files, _guard = opened(path, initialize=True)
    with files:
        original = files.put_bytes("original", (b"exact original",), byte_limit=14)
        plan = plan_for(files)
        assert not files.prepare_payload_step(plan).done
        cursor = files.progress_journal()
        with files._transaction(preparing=True) as db:
            db.execute("INSERT INTO primary_objects VALUES ('pending',1,'pending',0,'',0)").close()
            db.execute("INSERT INTO primary_operation VALUES (1,'pending',0,'writing')").close()
        pending = files._one("SELECT generation,cursor,phase FROM primary_operation")
    files, _guard = opened(path)
    with files:
        assert files.progress_journal() == cursor
        assert files.operation_grants().grants == GRANTS
        drain(files, plan)
        assert files._one("SELECT generation,cursor,phase FROM primary_operation") == pending
        assert files.require_physical_ready().phase == "ready"
        with files.verified(original) as stream:
            assert stream.read() == b"exact original"


@pytest.mark.parametrize(
    "fault",
    [
        "journal-workspace",
        "free-pages",
        "target",
        "database-workspace",
        "database-cap",
        "journal-workspace-cap",
        "journal-profile-cap",
    ],
)
def test_compound_preparation_preflights_every_grant_before_any_cursor_mutation(tmp_path, fault):
    files, guard = opened(tmp_path / "state.sqlite3", initialize=True)
    with files:
        plan = plan_for(files)
        drain(files, plan)
        changes = {
            "journal-workspace": dict(journal_workspace_bytes=32768),
            "free-pages": dict(free_pages=7),
            "target": dict(target_pages=plan.target_pages - 1),
            "database-workspace": dict(database_workspace_bytes=65536),
            "database-cap": dict(target_pages=LIMITS.database_bytes // 4096 + 1),
            "journal-workspace-cap": dict(journal_workspace_bytes=LIMITS.database_bytes + 4096),
            "journal-profile-cap": dict(prefix_lengths=(4 * LIMITS.journal_bytes,)),
        }[fault]
        bad = replace(plan, **changes)
        before = (files.progress_journal(), files.database_reserve(), files.physical_readiness())
        total, arms = files._db().total_changes, guard.arms
        with pytest.raises((ValueError, PrimaryStateCapacity)):
            files.prepare_payload_step(bad)
        assert files._db().total_changes == total and guard.arms == arms
        assert (
            files.progress_journal(),
            files.database_reserve(),
            files.physical_readiness(),
        ) == before
        assert not files._poisoned


@pytest.mark.parametrize("stage", ["journal", "database", "ready"])
def test_compound_preparation_propagates_component_failure_without_advancing_later_stages(
    tmp_path, monkeypatch, stage
):
    files, _guard = opened(tmp_path / "state.sqlite3", initialize=True)
    with files:
        plan = plan_for(files)
        if stage != "journal":
            for _ in range(64):
                files.prepare_payload_step(plan)
                if files.progress_journal().phase == "prepared":
                    break
            else:
                pytest.fail("fixture journal did not finish")
        if stage == "ready":
            for _ in range(128):
                files.prepare_payload_step(plan)
                if files.database_reserve().phase == "prepared":
                    break
            else:
                pytest.fail("fixture database did not finish")
        method = {
            "journal": "reserve_journal_step",
            "database": "reserve_database_step",
            "ready": "prepare_physical_ready",
        }[stage]
        before = (files.progress_journal(), files.database_reserve(), files.physical_readiness())

        def failed(*_args, **_kwargs):
            raise RuntimeError("component stopped")

        monkeypatch.setattr(files, method, failed)
        with pytest.raises(RuntimeError, match="component stopped"):
            files.prepare_payload_step(plan)
        assert (
            files.progress_journal(),
            files.database_reserve(),
            files.physical_readiness(),
        ) == before
        assert files.physical_readiness().phase != "ready" and not files._poisoned
        monkeypatch.undo()
        drain(files, plan)


@pytest.mark.parametrize("fault", ["child", "transaction", "foreign-plan"])
def test_compound_preparation_requires_original_root_exact_plan_and_settled_execution(
    tmp_path, fault
):
    files, _guard = opened(tmp_path / "state.sqlite3", initialize=True)
    with files:
        plan = plan_for(files)
        target = files.child("child", initialize=True) if fault == "child" else files
        if fault == "transaction":
            files._db().execute("BEGIN").close()
        before = files._db().total_changes
        with pytest.raises((TypeError, PrimaryStateBusy, PrimaryStateError)):
            target.prepare_payload_step(object() if fault == "foreign-plan" else plan)
        assert files._db().total_changes == before
        if fault == "transaction":
            files._db().rollback()


@pytest.mark.parametrize(
    "changes",
    [
        dict(prefix_lengths=[1]),
        dict(prefix_lengths=(True,)),
        dict(journal_workspace_bytes=4097),
        dict(free_pages=True),
        dict(target_pages=0),
        dict(database_workspace_bytes=True),
    ],
)
def test_compound_plan_rejects_invalid_configuration_before_storage_work(changes):
    arguments = dict(
        prefix_lengths=(8201, 16),
        journal_workspace_bytes=65536,
        free_pages=8,
        target_pages=64,
        database_workspace_bytes=128 * 1024,
    )
    arguments.update(changes)
    with pytest.raises((TypeError, ValueError)):
        SqliteReadinessPlan(**arguments)


def test_compound_journal_rewarm_retains_its_quantum_even_when_the_record_is_unchanged(
    tmp_path, monkeypatch
):
    from riverhog_client import _prepared_primary_sqlite_progress_journal as journal_owner

    files, guard = opened(tmp_path / "state.sqlite3", initialize=True)
    with files:
        plan = plan_for(files)
        for _ in range(64):
            files.prepare_payload_step(plan)
            if files.progress_journal().phase == "prepared":
                break
        else:
            pytest.fail("fixture journal did not finish")
        before_journal, before_database = files.progress_journal(), files.database_reserve()
        backed = journal_owner._backed
        observed = 0

        def lost_backing(owner, floor):
            nonlocal observed
            observed += 1
            return observed > 2 and backed(owner, floor)

        monkeypatch.setattr(journal_owner, "_backed", lost_backing)
        arms = guard.arms
        result = files.prepare_payload_step(plan)
        assert result.stage == "journal" and not result.done
        assert files.progress_journal() == before_journal
        assert files.database_reserve() == before_database
        assert guard.arms > arms
        monkeypatch.undo()
        drain(files, plan)


def test_shared_owner_preparation_resumes_after_callback_failure_without_status_work(tmp_path):
    from functools import partial

    from riverhog_client import PrimaryStore
    from riverhog_client._prepared_primary_factory import sqlite_primary_preparation_step

    from tests.support.prepared_primary_replay import ALLOCATION, DESCRIPTOR, SCOPE
    from tests.support.prepared_primary_replay import LIMITS as STORE_LIMITS

    roots = []

    def factory(path, *, allocation_descriptor, initialize):
        assert allocation_descriptor == DESCRIPTOR
        if initialize:
            path.touch(exist_ok=False)
        guard = Guard(path)
        files = TransactionalPrimaryFiles(
            guard,
            allocation_descriptor=allocation_descriptor,
            limits=LIMITS,
            initialize=initialize,
            operations=GRANTS,
        )
        roots.append(start(files, guard))
        return files

    def opened_store(initialize=False):
        return PrimaryStore(
            tmp_path / "shared.sqlite3",
            allocation_id=ALLOCATION.allocation_id,
            scope=SCOPE,
            limits=STORE_LIMITS,
            initialize=initialize,
            storage_factory=factory,
        )

    plan = SqliteReadinessPlan((8201, 16), 65536, 8, 128, 256 * 1024)
    execute = partial(sqlite_primary_preparation_step, plan=plan)
    calls = []

    def interrupted(storage):
        calls.append(storage)
        assert not execute(storage)
        raise RuntimeError("execution interrupted after committed cursor")

    with opened_store(initialize=True) as store:
        usage = store.usage
        with pytest.raises(RuntimeError, match="after committed cursor"):
            store.prepare_storage_step(interrupted)
        assert calls == [roots[-1]]
        cursor = roots[-1].progress_journal()
        assert cursor.pad_rows == 1
        assert roots[-1].database_reserve().phase == "unprepared"
        assert store.usage == usage
        assert not store.publication_started and not store.session_bound
        assert roots[-1].progress_journal() == cursor
    with opened_store() as store:
        assert roots[-1].progress_journal() == cursor
        assert roots[-1].operation_grants().grants == GRANTS
        assert store.allocation == ALLOCATION and store.usage == usage
        for _ in range(256):
            journal, database = roots[-1].progress_journal(), roots[-1].database_reserve()
            done = store.prepare_storage_step(execute)
            assert roots[-1].progress_journal().pad_rows - journal.pad_rows <= 1
            after = roots[-1].database_reserve()
            assert after.inserted - database.inserted <= 1
            assert after.released - database.released <= 1
            if done:
                break
        else:
            pytest.fail("shared preparation fixture did not finish")
        assert roots[-1].require_physical_ready().phase == "ready"
        assert store.session_for(b"exact creation") is None
        assert store.publication_started and store.usage == usage
    with opened_store() as store:
        import hashlib

        from riverhog_client._prepared_primary_types import PrimaryByteFile

        original = PrimaryByteFile(
            "create-request", 14, hashlib.sha256(b"exact creation").hexdigest()
        )
        with roots[-1].verified(original) as stream:
            assert stream.read() == b"exact creation"
        with pytest.raises(PrimaryStateError, match="uncertain outcome"):
            store.session_for(b"exact creation")
        assert store.allocation == ALLOCATION and store.usage == usage


def test_shared_member_preparation_runs_before_authorship_and_cold_replays_sealed_bytes(tmp_path):
    from functools import partial

    from riverhog_client._prepared_primary_factory import sqlite_primary_preparation_step

    from tests.support.prepared_primary_replay import DESCRIPTOR, witness

    roots = []
    preparations = []

    def factory(path, *, allocation_descriptor, initialize):
        assert allocation_descriptor == DESCRIPTOR
        if initialize:
            path.touch(exist_ok=False)
        guard = Guard(path)
        files = TransactionalPrimaryFiles(
            guard,
            allocation_descriptor=allocation_descriptor,
            limits=LIMITS,
            initialize=initialize,
            operations=GRANTS,
        )
        roots[:] = [start(files, guard)]
        return files

    plan = SqliteReadinessPlan((8201, 16), 65536, 8, 192, 512 * 1024)
    execute = partial(sqlite_primary_preparation_step, plan=plan)

    def prepare(storage):
        before_journal, before_database = roots[0].progress_journal(), roots[0].database_reserve()
        done = execute(storage)
        journal, database = roots[0].progress_journal(), roots[0].database_reserve()
        assert journal.pad_rows - before_journal.pad_rows <= 1
        assert database.inserted - before_database.inserted <= 1
        assert database.released - before_database.released <= 1
        preparations.append(done)
        return done

    witness(tmp_path / "member.sqlite3", factory, prepare_storage=prepare)
    assert len(preparations) >= 16 and preparations[-1] is True
    assert False in preparations
