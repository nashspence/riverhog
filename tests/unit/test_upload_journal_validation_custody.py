from __future__ import annotations

import pytest
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_provenance import JournalValidationCapacity, JournalValidationSession
from riverhog_provenance.journal_store import JournalStore

from tests.support.validation_workspace import leased_workspace
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
    with leased_workspace(root, workspace_id) as path:
        assert (path is None) is busy


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("cleanup", ["close", "release_stale"])
def test_unconfirmed_close_retains_the_actual_slot_and_kernel_lease(
    tmp_path, after, cleanup, *, journal_constructions
):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(
            factory, root, construction_factory=journal_constructions.retain
        )
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
    tmp_path, monkeypatch, phase, *, journal_constructions
):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(
            factory, root, construction_factory=journal_constructions.retain
        )
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


def test_terminal_cleanup_attempts_once_and_preserves_confirmed_outcome(
    tmp_path, *, journal_constructions
):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(
            factory, root, construction_factory=journal_constructions.retain
        )
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
def test_one_uncertain_slot_does_not_prevent_independent_slot_release(
    tmp_path, cleanup, *, journal_constructions
):
    with build_cases(tmp_path, large=False) as cases:
        case = cases[2]
        with live_owner(case, journal_constructions=journal_constructions) as (
            owner,
            _recorder,
            slots,
            _borrowed,
        ):
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
    tmp_path, monkeypatch, failures, *, journal_constructions
):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(
            factory, root, construction_factory=journal_constructions.retain
        )
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


def test_final_independent_native_readers_keep_the_original_slot_and_live_body(
    tmp_path, *, journal_constructions
):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(
            factory, root, construction_factory=journal_constructions.retain
        )
        readers = []
        try:
            key = work.collection_id, work.journal_id, work.attempt_id
            slot = owner._reserve(key)
            assert slot is not None
            try:
                assert owner._open(work, slot, source_owner.admission_provenance_catalog()) is None
            finally:
                slot.lock.release()
            native = slot.session
            for _ in range(10000):
                if native.advance().complete:
                    break
            else:
                pytest.fail("actual retained fixture did not reach its successful result")
            original = slot.construction
            assert original is not None and original.storage is native._store
            readers.extend((native.result(), native.result()))
            with pytest.raises(BlockingIOError, match="original Native construction"):
                owner.close()
            assert owner.active_keys() == ((work.collection_id, work.journal_id),)
            assert slot.session is native and slot.transferred
            assert slot.lease._active and not slot.lease.received
            assert not native.storage_received
            assert not original.scope.received and not original.received
            assert not readers[0].storage_received and not readers[1].storage_received
            assert_lease(root, slot.job.workspace_id, busy=True)
            readers[0].close()
            with pytest.raises(BlockingIOError, match="original Native construction"):
                owner.close()
            assert slot.lease._active and not native.storage_received
            assert not original.scope.received and not original.received
            assert_lease(root, slot.job.workspace_id, busy=True)
            readers[1].close()
            assert native.storage_received and slot.lease.received
            assert original.scope.received and not original.received
            assert all(reader.storage_received for reader in readers)
            owner.close()
            assert owner.active_keys() == ()
            assert original.received
            assert_lease(root, slot.job.workspace_id, busy=False)
        finally:
            for reader in readers:
                reader.close()
            owner.close()


def test_retained_native_readers_do_not_block_independent_journal_advancement(
    tmp_path, *, journal_constructions
):
    with build_cases(tmp_path, large=False) as cases:
        case = cases[2]
        with live_owner(case, journal_constructions=journal_constructions) as (
            owner,
            _recorder,
            slots,
            readers,
        ):
            first_key, first_slot, _storage = slots[0]
            first_native = first_slot.session
            for _ in range(10000):
                if first_native.advance().complete:
                    break
            else:
                pytest.fail("actual retained fixture did not reach its successful result")
            original = first_slot.construction
            assert original is not None and original.storage is first_native._store
            readers.extend((first_native.result(), first_native.result()))
            first_slot.closing = True
            assert len(owner._slots) == owner.concurrency == 2
            assert owner._reserve((9000, JOURNAL, "f" * 32)) is None
            second_outcome = finish(owner, case.works[1])
            assert second_outcome.failure is None
            assert owner.active_keys() == ((first_key[0], first_key[1]),)
            assert owner._slots[first_key] is first_slot
            assert first_slot.construction is original and first_slot.session is first_native
            assert not first_native.storage_received and not original.received
            root = case.path / "native"
            assert_lease(root, first_slot.job.workspace_id, busy=True)
            readers[0].close()
            with pytest.raises(BlockingIOError, match="original Native construction"):
                owner.release_stale()
            assert owner._slots[first_key] is first_slot and not original.received
            assert_lease(root, first_slot.job.workspace_id, busy=True)
            readers[1].close()
            assert first_native.storage_received and first_slot.lease.received
            assert not original.received
            owner.release_stale()
            assert owner.active_keys() == () and original.received
            assert_lease(root, first_slot.job.workspace_id, busy=False)


@pytest.mark.parametrize("pending_index", (0, 1))
def test_pending_reader_cleanup_does_not_hide_an_independent_unknown_close(
    tmp_path, pending_index, *, journal_constructions
):
    with build_cases(tmp_path, large=False) as cases:
        case = cases[2]
        with live_owner(case, journal_constructions=journal_constructions) as (
            owner,
            _recorder,
            slots,
            readers,
        ):
            pending_key, pending_slot, _storage = slots[pending_index]
            native = pending_slot.session
            for _ in range(10000):
                if native.advance().complete:
                    break
            else:
                pytest.fail("actual retained fixture did not reach its successful result")
            readers.append(native.result())
            original = pending_slot.construction
            pending_slot.closing = True
            other_index = 1 - pending_index
            _, other_slot, other_storage = slots[other_index]
            fault = UncertainClose(other_storage.connection)
            other_storage.connection = fault
            other_slot.closing = True
            with pytest.raises(OSError, match="unconfirmed"):
                owner.advance(case.works[other_index], catalog=case.catalog)
            assert fault.calls == 1 and len(owner._slots) == owner.concurrency == 2
            assert owner._slots[pending_key] is pending_slot
            assert pending_slot.construction is original and not original.received
            with pytest.raises(BlockingIOError, match="original Native construction"):
                owner.release_stale()
            assert fault.calls == 2
            assert owner.active_keys() == ((pending_key[0], pending_key[1]),)
            assert not native.storage_received and not original.received
            assert_lease(case.path / "native", pending_slot.job.workspace_id, busy=True)
            readers[0].close()
            owner.release_stale()
            assert owner.active_keys() == () and original.received
