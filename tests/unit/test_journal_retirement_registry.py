from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest
import workspace_reclamation as files
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.services import physical_execution as physical
from riverhog_core.services import workspace_retirement_registry as shared

from tests.support.journal_retirement import cleanups as cleanups
from tests.support.retained_resource import fault_native as fault_native
from tests.support.validation_workspace import leased_workspace
from tests.unit.test_archive_upload_execution import _joined
from tests.unit.test_archive_upload_execution import owned as owned
from tests.unit.test_discovery_validation_owner import UncertainClose
from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_journal_validation_retirement import ONE, TWO, detached_job, selected
from tests.unit.test_staged_journal_range import JOURNAL

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original Native receiving")
RETRY = timedelta(microseconds=1)


@contextmanager
def native_case(tmp_path, *, identities=(ONE,), state="retiring"):
    root = tmp_path / "native"
    paths = {}
    for identity in identities:
        with leased_workspace(root, identity, create=True) as path:
            assert path is not None
            file = path / "snapshot.sqlite3"
            file.write_bytes(b"x" * 17)
            file.chmod(0o600)
            paths[identity] = file
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        with factory() as session, session.begin():
            session.add_all(detached_job(identity, state=state) for identity in identities)
        yield SimpleNamespace(root=root, files=paths, engine=engine, factory=factory)


def test_actual_native_extent_receives_before_withdrawing_ordinary_charge(tmp_path, cleanups):
    with native_case(tmp_path) as case:
        f = cleanups(case)
        before = selected(case.factory, ONE)
        assert f.cleanup.advance(octet_budget=3) == 1
        context = f.registry._contexts[ONE]
        assert not context.reservation.terminal and f.ledger._counts[False] == 1
        _joined(f.pool)
        assert context.actor.received and context.actor._files.received
        assert context.actor._files._file.metrics["close_calls"] == 1
        assert context.actor._files._directory.metrics["close_calls"] == 1
        assert f.ledger._counts[False] == 1 and f.registry.retained(ONE)
        assert case.files[ONE].read_bytes() == b"x" * 14
        assert selected(case.factory, ONE) == before
        assert f.registry.collect(limit=1) == 2
        assert context.progress is True and context.original is context.actor
        assert not f.registry.retained(ONE) and f.ledger._counts == {False: 0, True: 0}


def test_stalled_original_file_io_preserves_custody_and_allows_another_workspace(
    tmp_path, cleanups, monkeypatch
):
    with native_case(tmp_path, identities=(ONE, TWO)) as case:
        f = cleanups(case)
        entered, release = threading.Event(), threading.Event()
        actual_sync = os.fsync
        inode = case.files[ONE].stat().st_ino

        def sync(descriptor):
            if os.fstat(descriptor).st_ino == inode:
                entered.set()
                assert release.wait(10), "original Native I/O was not released"
            return actual_sync(descriptor)

        with monkeypatch.context() as patch:
            patch.setattr(os, "fsync", sync)
            try:
                assert f.cleanup.advance(octet_budget=3) == 1 and entered.wait(5)
                first = f.registry._contexts[ONE]
                assert first.actor._files._file_borrow.active
                with pytest.raises(BlockingIOError):
                    first.actor._files.close()
                assert f.cleanup.advance(octet_budget=3) == 1
                second = f.registry._contexts[TWO]
                worker = f.pool._slots[second.reservation.key].worker
                worker.join(timeout=10)
                assert not worker.is_alive()
                assert f.registry.collect(limit=2) == 2
                assert second.progress is True and f.ledger._counts[False] == 1
                assert not first.received and first.actor._files._file.metrics["close_calls"] == 0
                with leased_workspace(case.root, ONE, shared=True) as reader:
                    assert reader is None
                f.cleanup.close()
                assert not f.cleanup.receive_stopped() and f.ledger._counts[False] == 1
            finally:
                release.set()
                _joined(f.pool)
        assert f.cleanup.receive_stopped()
        assert f.ledger._counts == {False: 0, True: 0}
        assert all(path.read_bytes() == b"x" * 14 for path in case.files.values())
        assert case.engine.pool.checkedout() == 0


def test_native_close_refusal_keeps_original_lease_and_retries_only_receiving(
    tmp_path, cleanups, monkeypatch, fault_native
):
    native, _ = fault_native
    with monkeypatch.context() as patch:
        patch.setattr(files, "Resource", native.Resource)
        patch.setattr(files, "Borrow", native.Borrow)
        patch.setattr(shared, "monotonic", lambda: 1.0)
        with native_case(tmp_path) as case:
            f = cleanups(case)
            create = f.registry._create
            refusing = [True]

            def original(job):
                actor = create(job)
                actor._files._file._test_fault("before_close")
                close = actor._files.close

                def receive():
                    if refusing[0]:
                        raise UncertainClose("original Native file close remains refused")
                    return close()

                actor._files.close = receive
                return actor

            patch.setattr(f.registry, "_create", original)
            try:
                assert f.cleanup.advance(octet_budget=3) == 1
                context = f.registry._contexts[ONE]
                _joined(f.pool)
                assert f.registry.collect(limit=1) == 1
                assert context.closing and context.unknown is not None
                assert f.ledger._counts[False] == 1
                actor = context.actor
                assert actor._files._file.state == actor._files._directory.state == "live"
                assert actor._files._file.metrics["close_calls"] == 0
                assert case.files[ONE].read_bytes() == b"x" * 14
                with leased_workspace(case.root, ONE, shared=True) as reader:
                    assert reader is None

                def forbidden(*_args, **_kwargs):
                    pytest.fail("receiving reconstructed original Native file work")

                patch.setattr(f.cleanup, "_advance", forbidden)
                refusing[0] = False
                patch.setattr(shared, "monotonic", lambda: 2.0)
                assert f.registry.resume_ready(limit=1, retry_after=RETRY) == 1
                _joined(f.pool)
                assert f.registry.collect(limit=1) == 2
                assert context.actor is actor and actor.received
                assert actor._files._file.metrics["close_calls"] == 1
                assert actor._files._directory.metrics["close_calls"] == 1
                assert case.files[ONE].read_bytes() == b"x" * 14
                assert f.ledger._counts[False] == 0 and not f.registry.retained(ONE)
                with leased_workspace(case.root, ONE, shared=True) as reader:
                    assert reader is not None
            finally:
                refusing[0] = False


def test_native_metadata_capacity_rejects_before_original_construction(
    tmp_path, cleanups, monkeypatch
):
    with native_case(tmp_path) as case:
        f = cleanups(case, metadata_budget=1)

        def forbidden(*_args):
            pytest.fail("unadmitted Native metadata entered original construction")

        monkeypatch.setattr(f.registry, "_create", forbidden)
        with pytest.raises(VolumeEmissionCapacity):
            f.cleanup.advance(octet_budget=3)
        assert not f.registry._contexts and not f.pool._slots
        assert f.ledger._counts == {False: 0, True: 0}
        assert case.files[ONE].read_bytes() == b"x" * 17


def test_native_orphan_transition_precedes_every_file_extent(tmp_path, cleanups):
    with native_case(tmp_path, state="opening") as case:
        f = cleanups(case)
        assert f.cleanup.advance(octet_budget=3) == 1
        context = f.registry._contexts[ONE]
        _joined(f.pool)
        assert context.actor.received
        assert selected(case.factory, ONE).state == "retiring"
        assert case.files[ONE].read_bytes() == b"x" * 17
        assert context.actor._files._file.metrics["open_calls"] == 0
        assert f.registry.collect(limit=1) == 2
        assert f.cleanup.advance(octet_budget=3) == 1
        _joined(f.pool)
        assert f.registry.collect(limit=1) == 2
        assert case.files[ONE].read_bytes() == b"x" * 14


def test_native_directory_phase_waits_for_actual_original_worker_exit(
    tmp_path, cleanups, owned, monkeypatch
):
    with native_case(tmp_path) as case:
        f = cleanups(case)
        release, returned = owned.event(), threading.Event()
        actual_thread = physical.Thread

        class LateExit(actual_thread):
            def run(self):
                super().run()
                returned.set()
                assert release.wait(10), "original Native worker exit was not released"

        with monkeypatch.context() as patch:
            patch.setattr(physical, "Thread", LateExit)
            try:
                assert f.cleanup.advance(octet_budget=17) == 1 and returned.wait(5)
                context = f.registry._contexts[ONE]
                assert (
                    context.actor.received and f.pool._slots[context.reservation.key].future.done()
                )
                assert selected(case.factory, ONE).state == "directory"
                assert not case.files[ONE].exists() and (case.root / ONE / ".lease").exists()
                assert f.cleanup.advance(octet_budget=17) == 0
                assert f.ledger._counts[False] == 1 and f.registry.collect(limit=1) == 0
            finally:
                release.set()
                _joined(f.pool)
            assert f.registry.collect(limit=1) == 2
            assert f.cleanup.advance(octet_budget=17) == 1
            _joined(f.pool)
            assert f.registry.collect(limit=1) == 2
        assert selected(case.factory, ONE) is None and not (case.root / ONE).exists()


def test_native_wrong_progress_retains_original_until_explicit_receiving(
    tmp_path, cleanups, monkeypatch
):
    with native_case(tmp_path) as case:
        f = cleanups(case)
        monkeypatch.setattr(shared, "monotonic", lambda: 1.0)
        monkeypatch.setattr(
            f.cleanup, "_advance", lambda *_args, **_kwargs: "wrong original result"
        )
        assert f.cleanup.advance(octet_budget=3) == 1
        context = f.registry._contexts[ONE]
        _joined(f.pool)
        assert f.registry.collect(limit=1) == 1
        assert context.closing and context.progress is None and context.actor.received
        assert f.ledger._counts[False] == 1
        assert case.files[ONE].read_bytes() == b"x" * 17
        monkeypatch.setattr(shared, "monotonic", lambda: 2.0)
        assert f.registry.resume_ready(limit=1, retry_after=RETRY) == 1
        _joined(f.pool)
        assert f.registry.collect(limit=1) == 2 and context.progress is None
        assert f.ledger._counts[False] == 0


def test_large_epoch_preserves_exact_scope_and_actual_native_execution(tmp_path, cleanups):
    with native_case(tmp_path) as case:
        with case.factory() as session, session.begin():
            session.get(Job, ONE).worker_epoch = 2**60
        f = cleanups(case)
        before = selected(case.factory, ONE)
        key, target, required = f.registry._scope(before, 1024 * 1024)
        changed, _, _ = f.registry._scope(replace(before, worker_epoch=2**60 + 1), 1024 * 1024)
        assert key != changed and target.endswith(ONE) and required > 0
        assert f.cleanup.advance(octet_budget=3) == 1
        context = f.registry._contexts[ONE]
        assert context.reservation.key == key and context.job.worker_epoch == 2**60
        _joined(f.pool)
        assert f.registry.collect(limit=1) == 2
        assert context.actor.received and context.progress is True
        assert selected(case.factory, ONE) == before
        assert case.files[ONE].read_bytes() == b"x" * 14
        assert not f.registry.retained(ONE) and f.ledger._counts == {False: 0, True: 0}
