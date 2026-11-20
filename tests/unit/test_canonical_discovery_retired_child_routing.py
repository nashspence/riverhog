from __future__ import annotations

import os
import sqlite3
import subprocess
import sys

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace

from tests.support.retained_resource import fault_native as fault_native
from tests.support.retirement_namespace import (
    lease_retiring_journal_workspace,
    native_namespace_close_fault,
)
from tests.unit.test_canonical_discovery_retired_metadata import case as case
from tests.unit.test_canonical_discovery_retired_metadata import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_retired_metadata import original as original
from tests.unit.test_canonical_discovery_retired_metadata import prepare_retired, retired
from tests.unit.test_canonical_discovery_retired_metadata import qualified as qualified

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX retired Core custody")


def selected_child(case, *, count=2, confirmed=True):
    job, path, bindings = prepare_retired(case, count=count, confirmed=confirmed)
    with case.factory() as session, session.begin():
        selected = jobs.advance_discovery_reclamation(
            session,
            job,
            "child_directory",
            journal_id=bindings[0].journal_id,
            directory_id=bindings[0].directory_id,
        )
    assert selected is not None
    return selected, path, bindings


def rows(path):
    with sqlite3.connect(path / "discovery.sqlite3") as db:
        return db.execute(
            "SELECT journal_id FROM discovery_journal_children ORDER BY journal_id"
        ).fetchall()


@pytest.mark.parametrize("confirmed", (False, True))
@pytest.mark.parametrize("root_present", (False, True))
def test_retired_routing_receives_exact_absence_before_only_selected_delete(
    case, confirmed, root_present
):
    job, path, bindings = selected_child(case, confirmed=confirmed)
    if root_present:
        (path / "journals").mkdir(mode=0o700)
        (path / "journals" / "unrelated").write_bytes(b"retained")
    before = rows(path)
    with retired(case, job) as metadata:
        metadata.open()
        assert metadata.retire_child()
        assert metadata.retire_child()
        assert metadata.child() is None and metadata.verify_current()
        with case.factory() as session:
            assert jobs.retire_discovery_job(session, job) == job
        assert job.reclamation_directory_id == bindings[0].directory_id
        assert rows(path) == before[1:]
    if root_present:
        assert (path / "journals" / "unrelated").read_bytes() == b"retained"
    else:
        assert not (path / "journals").exists()
    assert (path / ".lease").exists()


@pytest.mark.parametrize("routing_present", (False, True))
@pytest.mark.parametrize("entry", ("directory", "file", "symlink"))
def test_routing_absence_never_substitutes_for_exact_namespace_receiving(
    case, routing_present, entry
):
    job, path, bindings = selected_child(case)
    root = path / "journals"
    root.mkdir(mode=0o700)
    child = root / bindings[0].directory_id
    if entry == "directory":
        child.mkdir(mode=0o700)
    elif entry == "file":
        child.write_bytes(b"unknown")
    else:
        child.symlink_to(path / "discovery.sqlite3")
    if not routing_present:
        with sqlite3.connect(path / "discovery.sqlite3") as db:
            db.execute(
                "DELETE FROM discovery_journal_children WHERE journal_id=?",
                (bindings[0].journal_id,),
            ).close()
    before = rows(path)
    with retired(case, job) as metadata:
        metadata.open()
        with pytest.raises(LostJournalValidationWorkspace, match="namespace still exists"):
            metadata.retire_child()
        assert not metadata._namespace_handles._callbacks and metadata.verify_current()
    assert rows(path) == before and child.lstat()


def test_routing_retirement_rejects_nonselected_phase_and_stale_owner(case):
    job, path, _ = prepare_retired(case)
    before = rows(path)
    with retired(case, job) as metadata:
        metadata.open()
        with pytest.raises(StaleArchiveTask, match="durable directory phase"):
            metadata.retire_child()
        metadata._custody._held = False
        try:
            with pytest.raises(StaleArchiveTask):
                metadata.retire_child()
        finally:
            metadata._custody._held = True
    assert rows(path) == before


@pytest.mark.parametrize("root_present", (False, True))
@pytest.mark.parametrize("after", (False, True))
def test_uncertain_namespace_sync_preserves_routing_for_exact_retry(
    case, monkeypatch, root_present, after
):
    job, path, bindings = selected_child(case)
    if root_present:
        (path / "journals").mkdir(mode=0o700)
    actual = os.fsync

    def uncertain(descriptor):
        if after:
            actual(descriptor)
        raise OSError("namespace sync uncertain")

    with retired(case, job) as metadata:
        metadata.open()
        with monkeypatch.context() as patch:
            patch.setattr(os, "fsync", uncertain)
            with pytest.raises(OSError, match="sync uncertain"):
                metadata.retire_child()
        assert not metadata._namespace_handles._callbacks and metadata.verify_current()
        assert metadata.child().binding == bindings[0]
        assert metadata.retire_child()
        assert metadata.child() is None


@pytest.mark.parametrize("after", (False, True))
def test_uncertain_root_close_retains_handle_and_parent_until_actual_receiving(
    case, monkeypatch, after, fault_native
):
    native, _ = fault_native
    job, path, _ = selected_child(case)
    (path / "journals").mkdir(mode=0o700)
    before = rows(path)
    with (
        monkeypatch.context() as patch,
        native_namespace_close_fault(
            patch, native, parent_inode=path.stat().st_ino, name="journals", after=after
        ) as (selected, descriptors),
        retired(case, job) as metadata,
    ):
        metadata.open()
        with pytest.raises((RuntimeError, OSError)):
            metadata.retire_child()
        assert len(selected) == 1 and len(descriptors) == 1
        original = selected[0]
        assert not metadata.verify_current()
        assert original.owner.state == ("received" if after else "live")
        assert rows(path) == before
        with lease_retiring_journal_workspace(case.root, job.workspace_id) as busy:
            assert busy is None
        with pytest.raises(StaleArchiveTask):
            metadata.retire_child()
        metadata.close()
        assert not metadata._namespace_handles._callbacks
        assert original.owner.received and original.owner.metrics["close_calls"] == 1


@pytest.mark.parametrize("after", (False, True))
def test_routing_transaction_rollback_or_committed_lost_reply_retries_original_phase(case, after):
    job, path, bindings = selected_child(case)
    with retired(case, job) as metadata:
        metadata.open()
        actual = metadata._db

        class Connection:
            attempted = False

            @property
            def in_transaction(self):
                return actual.in_transaction

            def execute(self, statement, parameters=()):
                if statement == "COMMIT" and not self.attempted:
                    self.attempted = True
                    if after:
                        actual.execute(statement, parameters).close()
                    raise OSError("routing commit uncertain")
                return actual.execute(statement, parameters)

            def close(self):
                actual.close()

        metadata._db = Connection()
        with pytest.raises(OSError, match="commit uncertain"):
            metadata.retire_child()
        assert not actual.in_transaction
        assert (metadata.child() is None) is after
        assert metadata.retire_child()
        assert metadata.child() is None
        assert rows(path) == [(bindings[1].journal_id,)]
        with case.factory() as session:
            assert jobs.retire_discovery_job(session, job) == job


def test_routing_retirement_recovers_actual_hot_sqlite_rollback_without_rebuilding(case):
    job, path, bindings = selected_child(case, count=1025)
    before = rows(path)
    with retired(case, job) as metadata:
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                "import os,sqlite3,sys; "
                "db=sqlite3.connect(sys.argv[1]); "
                "db.execute('PRAGMA cache_size=1'); "
                "db.execute('BEGIN IMMEDIATE'); "
                "db.execute('DELETE FROM discovery_journal_children'); "
                "os._exit(0)",
                str(path / "discovery.sqlite3"),
            ],
            check=False,
            timeout=30,
        )
        assert child.returncode == 0
        journal = path / "discovery.sqlite3-journal"
        assert journal.is_file() and journal.stat().st_size > 512
        metadata.open()
        assert rows(path) == before
        assert metadata.child().binding == bindings[0]
        assert metadata.retire_child() and metadata.child() is None
        assert rows(path) == before[1:]
