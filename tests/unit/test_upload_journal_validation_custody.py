from __future__ import annotations

import pytest
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_provenance import JournalValidationCapacity, JournalValidationSession
from riverhog_provenance.journal_store import JournalStore

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_certificates import ready
from tests.unit.test_upload_journal_validation_owner import finish, selected
from tests.unit.test_upload_journal_validation_release_growth import build_cases, live_owner


class UncertainClose:
    def __init__(self, connection, *, after=False, failures=1):
        self.connection, self.after, self.failures = connection, after, failures
        self.calls = 0

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def close(self):
        self.calls += 1
        if self.failures:
            self.failures -= 1
            if self.after:
                self.connection.close()
            raise OSError("Native database close unconfirmed")
        self.connection.close()


def assert_lease(root, workspace_id, *, busy):
    with lease_journal_validation_workspace(root, workspace_id) as path:
        assert (path is None) is busy


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("cleanup", ["close", "release_stale"])
def test_unconfirmed_close_retains_the_actual_slot_and_kernel_lease(tmp_path, after, cleanup):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(factory, root)
        try:
            owner.advance(work, catalog=source_owner.admission_provenance_catalog())
            slot = next(iter(owner._slots.values()))
            native = slot.session
            storage = native._store._storage
            fault = UncertainClose(storage.connection, after=after)
            storage.connection = fault
            before = selected(factory)
            if cleanup == "release_stale":
                slot.closing = True
            with pytest.raises(OSError, match="unconfirmed"):
                getattr(owner, cleanup)()
            assert fault.calls == 1 and slot.closing
            assert owner.active_keys() == ((work.collection_id, work.journal_id),)
            assert selected(factory) == before
            assert_lease(root, before.workspace_id, busy=True)
            with pytest.raises(RuntimeError, match="closed"):
                native.advance()
            getattr(owner, cleanup)()
            assert fault.calls == 2 and owner.active_keys() == ()
            assert selected(factory) == before
            assert_lease(root, before.workspace_id, busy=False)
        finally:
            owner.close()


@pytest.mark.parametrize("phase", ["store", "session"])
def test_failed_initialization_keeps_early_native_storage_and_application_custody(
    tmp_path, monkeypatch, phase
):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(factory, root)
        retained_storage = []
        target = JournalStore if phase == "store" else JournalValidationSession
        name = "_initialize_schema" if phase == "store" else "_initialize"
        initialize = getattr(target, name)

        def interrupted(current, *args, **kwargs):
            initialize(current, *args, **kwargs)
            store = current if phase == "store" else current._store
            fault = UncertainClose(store._storage.connection, failures=2)
            store._storage.connection = fault
            retained_storage.append(fault)
            raise InterruptedError("Native initialization interrupted")

        try:
            with monkeypatch.context() as patch:
                patch.setattr(target, name, interrupted)
                with pytest.raises(OSError, match="unconfirmed"):
                    owner.advance(work, catalog=source_owner.admission_provenance_catalog())
            assert len(retained_storage) == 1 and retained_storage[0].calls == 2
            slot = next(iter(owner._slots.values()))
            assert slot.session is not None and slot.closing
            job = selected(factory)
            path = root / job.workspace_id / "snapshot.sqlite3"
            identity = path.stat().st_ino
            assert_lease(root, job.workspace_id, busy=True)
            owner.release_stale()
            assert retained_storage[0].calls == 3 and owner.active_keys() == ()
            assert path.stat().st_ino == identity and selected(factory) == job
            assert_lease(root, job.workspace_id, busy=False)
        finally:
            owner.close()


def test_terminal_cleanup_attempts_once_and_preserves_confirmed_outcome(tmp_path):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(factory, root)
        try:
            owner.advance(work, catalog=source_owner.admission_provenance_catalog())
            slot = next(iter(owner._slots.values()))
            storage = slot.session._store._storage
            fault = UncertainClose(storage.connection, failures=2)
            storage.connection = fault
            with pytest.raises(OSError, match="unconfirmed"):
                finish(owner, work)
            terminal = selected(factory)
            assert terminal.state == "complete" and fault.calls == 1 and slot.closing
            assert_lease(root, terminal.workspace_id, busy=True)
            with pytest.raises(OSError, match="unconfirmed"):
                owner.advance(work, catalog=source_owner.admission_provenance_catalog())
            assert fault.calls == 2 and selected(factory) == terminal
            owner.release_stale()
            assert fault.calls == 3 and owner.active_keys() == ()
            assert owner.advance(
                work, catalog=source_owner.admission_provenance_catalog()
            ).outcome == jobs.outcome(terminal)
            assert selected(factory) == terminal
            assert_lease(root, terminal.workspace_id, busy=False)
        finally:
            owner.close()


@pytest.mark.parametrize("cleanup", ["close", "release_stale"])
def test_one_uncertain_slot_does_not_prevent_independent_slot_release(tmp_path, cleanup):
    with build_cases(tmp_path, large=False) as cases:
        case = cases[2]
        with live_owner(case) as (owner, _recorder, slots, _borrowed):
            first, second = slots
            storage = first[1].session._store._storage
            fault = UncertainClose(storage.connection)
            storage.connection = fault
            if cleanup == "release_stale":
                first[1].closing = second[1].closing = True
            with pytest.raises(OSError, match="unconfirmed"):
                getattr(owner, cleanup)()
            assert fault.calls == 1
            assert owner.active_keys() == ((first[0][0], first[0][1]),)
            root = case.path / "native"
            assert_lease(root, first[1].job.workspace_id, busy=True)
            assert_lease(root, second[1].job.workspace_id, busy=False)
            getattr(owner, cleanup)()
            assert fault.calls == 2 and owner.active_keys() == ()


@pytest.mark.parametrize("failures", [0, 2])
def test_initialization_capacity_retains_cleanup_then_resumes_the_same_file(
    tmp_path, monkeypatch, failures
):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(factory, root)
        initialize = JournalValidationSession._initialize
        retained_storage = []

        def capacity(current, *args, **kwargs):
            initialize(current, *args, **kwargs)
            storage = current._store._storage
            fault = UncertainClose(storage.connection, failures=failures)
            storage.connection = fault
            retained_storage.append(fault)
            raise JournalValidationCapacity("initialization capacity exhausted")

        try:
            with monkeypatch.context() as patch:
                patch.setattr(JournalValidationSession, "_initialize", capacity)
                if failures:
                    with pytest.raises(OSError, match="unconfirmed"):
                        owner.advance(work, catalog=source_owner.admission_provenance_catalog())
                else:
                    assert not owner.advance(
                        work, catalog=source_owner.admission_provenance_catalog()
                    ).progressed
            job = selected(factory)
            path = root / job.workspace_id / "snapshot.sqlite3"
            identity = path.stat().st_ino
            if failures:
                assert retained_storage[0].calls == 2
                assert owner.active_keys() == ((work.collection_id, work.journal_id),)
                assert_lease(root, job.workspace_id, busy=True)
                owner.release_stale()
                assert retained_storage[0].calls == 3
            assert owner.active_keys() == ()
            assert_lease(root, job.workspace_id, busy=False)
            assert owner.advance(
                work, catalog=source_owner.admission_provenance_catalog()
            ).progressed
            assert path.stat().st_ino == identity
            assert selected(factory).worker_epoch == job.worker_epoch + 1
        finally:
            owner.close()
