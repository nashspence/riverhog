from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from threading import Event

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import (
    StaleArchiveTask,
    apply_archive_descriptor,
    archive_metadata_progress,
    prepare_archive_descriptor,
)
from riverhog_core.canonical_discovery_index import begin_index_build
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    lease_journal_validation_workspace,
)
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import StaleCorpusWorkspace
from riverhog_provenance import corpus_workspace as native_owner

from tests.unit.test_canonical_discovery_corpus_source import case as case
from tests.unit.test_canonical_discovery_corpus_source import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_source import original as original
from tests.unit.test_canonical_discovery_corpus_source import qualified as qualified
from tests.unit.test_canonical_discovery_corpus_source import seed
from tests.unit.test_canonical_discovery_index import _collection


@contextmanager
def unit(owner, case):
    with owner.work(build_id=case.build_id, admission=case.admission, catalog=case.catalog) as work:
        yield work


def busy(case):
    with lease_journal_validation_workspace(case.root, case.job.workspace_id) as path:
        assert path is None


def available(case):
    with lease_journal_validation_workspace(case.root, case.job.workspace_id) as path:
        assert path is not None


def test_core_handle_and_original_checkpoint_stay_warm_across_units(case, qualified):
    owner = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(owner, case) as first:
            assert first is not None
            metadata = first.metadata
            epoch = metadata.job.worker_epoch
            reader = qualified[1][2]
            with metadata.read() as (db, key):
                prepared = prepare_archive_descriptor(db, key, case.admission, reader)
            with metadata.transaction() as (db, key):
                assert apply_archive_descriptor(db, key, case.admission, prepared).version == 1
            with unit(owner, case) as blocked:
                assert blocked is None
            assert jobs.discovery_worker_snapshot(case.factory, metadata.job).worker_epoch == epoch
        with pytest.raises(StaleArchiveTask):
            _ = first.metadata
        with unit(owner, case) as second:
            assert second is not None and second.metadata is metadata
            assert second.metadata.job.worker_epoch == epoch
            with second.metadata.read() as (db, key):
                assert archive_metadata_progress(db, key).version == 1
        busy(case)
    finally:
        owner.close()
    assert owner.active_build_ids() == ()
    available(case)


def test_actual_native_input_progress_stays_warm_and_resumes_after_owner_replacement(
    case, qualified, original
):
    owner = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(owner, case) as first:
            assert first is not None
            selection, _ = seed(first.metadata, qualified)
            native = first.corpus(selection)
            assert first.metadata.job.state == "active"
            assert native.advance_source(original[1], frame_budget=1).accepted_frames == 1
            epoch = first.metadata.job.worker_epoch
        with unit(owner, case) as second:
            assert second is not None and second.corpus(selection) is native
            assert native.advance_source(original[1], frame_budget=1).accepted_frames == 2
    finally:
        owner.close()
    reopened = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(reopened, case) as work:
            assert work is not None and work.metadata.job.worker_epoch == epoch + 1
            resumed = work.corpus(selection)
            assert resumed.advance_source(original[1], frame_budget=1).accepted_frames == 3
    finally:
        reopened.close()


class UncertainClose:
    def __init__(self, db, *, lost_reply, failures=1):
        self.db, self.lost_reply, self.failures = db, lost_reply, failures

    def __getattr__(self, name):
        return getattr(self.db, name)

    def close(self):
        if self.failures:
            self.failures -= 1
            if self.lost_reply:
                self.db.close()
            raise OSError("owned close is unconfirmed")
        self.db.close()


@pytest.mark.parametrize("storage", ["native", "core"])
@pytest.mark.parametrize("lost_reply", [False, True])
def test_close_keeps_the_slot_and_lease_until_both_actual_handles_confirm(
    case, qualified, storage, lost_reply
):
    owner = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(owner, case) as work:
            assert work is not None
            metadata = work.metadata
            selection, _ = seed(metadata, qualified)
            native = work.corpus(selection)
            if storage == "native":
                native._corpus._db = UncertainClose(native._db, lost_reply=lost_reply)
            else:
                metadata._db = UncertainClose(metadata._db, lost_reply=lost_reply)
        with pytest.raises(OSError, match="unconfirmed"):
            owner.close()
        assert owner.active_build_ids() == (case.build_id,)
        busy(case)
        with pytest.raises(RuntimeError, match="closed"):
            native.advance_semantics()
        owner.close()
        assert owner.active_build_ids() == ()
        available(case)
    finally:
        owner.close()


def test_failed_native_initialization_retains_uncertain_storage_for_cleanup_retry(
    case, qualified, monkeypatch
):
    owner = DiscoveryValidationOwner(case.factory, case.root)
    initialize = native_owner.RetainedCorpusValidator._initialize

    def interrupted(native):
        initialize(native)
        native._corpus._db = UncertainClose(native._db, lost_reply=False, failures=2)
        raise InterruptedError("Native initialization interrupted")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(native_owner.RetainedCorpusValidator, "_initialize", interrupted)
            with pytest.raises(OSError, match="unconfirmed"):
                with unit(owner, case) as work:
                    assert work is not None
                    selection, _ = seed(work.metadata, qualified)
                    work.corpus(selection)
        assert owner.active_build_ids() == (case.build_id,)
        busy(case)
        owner.release_stale()
        assert owner.active_build_ids() == ()
        available(case)
        with pytest.raises(StaleCorpusWorkspace):
            with unit(owner, case) as work:
                assert work is not None
                before = (work.metadata.custody.directory / "corpus.sqlite3").read_bytes()
                work.corpus(selection)
        current = jobs.select_discovery_job(
            case.factory, build_id=case.build_id, admission=case.admission, catalog=case.catalog
        )
        assert current is not None and current.state == "lost"
        path = case.root / case.job.workspace_id / "corpus.sqlite3"
        assert path.read_bytes() == before
        available(case)
    finally:
        owner.close()


@pytest.mark.parametrize("boundary", ["native-open", "activation"])
def test_lost_confirmation_reopens_actual_native_storage_without_reset(
    case, qualified, monkeypatch, boundary
):
    owner = DiscoveryValidationOwner(case.factory, case.root)
    synchronize, activate = native_owner._sync_directory, jobs.activate_discovery_corpus

    def lost_sync(directory):
        synchronize(directory)
        raise TimeoutError("Native opening confirmation lost")

    def lost_activation(factory, job):
        assert activate(factory, job) is not None
        raise TimeoutError("Native activation confirmation lost")

    try:
        with monkeypatch.context() as patch:
            if boundary == "native-open":
                patch.setattr(native_owner, "_sync_directory", lost_sync)
            else:
                patch.setattr(jobs, "activate_discovery_corpus", lost_activation)
            with pytest.raises(TimeoutError, match="confirmation lost"):
                with unit(owner, case) as work:
                    assert work is not None
                    selection, _ = seed(work.metadata, qualified)
                    directory = work.metadata.custody.directory
                    epoch = work.metadata.job.worker_epoch
                    work.corpus(selection)
        assert owner.active_build_ids() == ()
        available(case)
        inode = (directory / "corpus.sqlite3").stat().st_ino
        with unit(owner, case) as work:
            assert work is not None and work.metadata.job.worker_epoch == epoch + 1
            native = work.corpus(selection)
            assert work.metadata.job.state == "active"
            assert (directory / "corpus.sqlite3").stat().st_ino == inode
            assert native is not None
    finally:
        owner.close()


def independent_build(case, number):
    with case.factory() as session, session.begin():
        collection = _collection()
        collection.id = number
        collection.creation_idempotency_key = "controlled-discovery-" + str(number)
        collection.creation_identity_sha256 = f"{number:064x}"
        collection.archive_generation = case.admission.root.archive_generation
        collection.archive_root_sha256 = case.admission.archive_sha256
        collection.provenance_identity = case.admission.provenance_sha256
        collection.artifact_set_identity = case.admission.root.artifact_set_sha256
        session.add(collection)
        session.flush()
        return begin_index_build(session, collection_id=number)


@pytest.mark.parametrize("concurrency", [1, 2])
def test_busy_work_obeys_capacity_and_leaves_independent_slots_responsive(case, concurrency):
    other = independent_build(case, 2)
    owner = DiscoveryValidationOwner(case.factory, case.root, concurrency=concurrency)
    entered, release = Event(), Event()

    def waiting_unit():
        with unit(owner, case) as work:
            assert work is not None
            entered.set()
            assert release.wait(timeout=15)

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            waiting = executor.submit(waiting_unit)
            try:
                assert entered.wait(timeout=15)
                with unit(owner, case) as same:
                    assert same is None
                with owner.work(
                    build_id=other, admission=case.admission, catalog=case.catalog
                ) as independent:
                    assert (independent is not None) is (concurrency == 2)
                    if independent is not None:
                        with independent.metadata.transaction() as (db, key):
                            assert archive_metadata_progress(db, key).version == 0
            finally:
                release.set()
            waiting.result(timeout=15)
    finally:
        release.set()
        owner.close()


@pytest.mark.parametrize("cleanup", ["close", "release_stale"])
def test_uncertain_slot_close_does_not_skip_closing_independent_slots(case, qualified, cleanup):
    other = independent_build(case, 2)
    owner = DiscoveryValidationOwner(case.factory, case.root, concurrency=2)
    try:
        with unit(owner, case) as work:
            selection, _ = seed(work.metadata, qualified)
            first = work.corpus(selection)
        with owner.work(build_id=other, admission=case.admission, catalog=case.catalog) as work:
            selection, _ = seed(work.metadata, qualified)
            second = work.corpus(selection)
            second_id = work.metadata.job.workspace_id
        if cleanup == "release_stale":
            for slot in owner._slots.values():
                slot.metadata.custody._held = False
        first._corpus._db = UncertainClose(first._db, lost_reply=False)
        with pytest.raises(OSError, match="unconfirmed"):
            getattr(owner, cleanup)()
        assert owner.active_build_ids() == (case.build_id,)
        busy(case)
        with lease_journal_validation_workspace(case.root, second_id) as released:
            assert released is not None
        with pytest.raises(RuntimeError, match="closed"):
            second.advance_semantics()
        owner.close()
        available(case)
    finally:
        owner.close()


@pytest.mark.parametrize(
    ("storage", "damage"),
    [
        ("lease", "missing"),
        ("core", "missing"),
        ("native", "missing"),
        ("core", "incompatible"),
        ("native", "incompatible"),
    ],
)
def test_accepted_custody_loss_is_retained_without_recreating_storage(
    case, qualified, storage, damage
):
    owner = DiscoveryValidationOwner(case.factory, case.root)
    with unit(owner, case) as work:
        assert work is not None
        selection, _ = seed(work.metadata, qualified)
        work.corpus(selection)
        directory = work.metadata.custody.directory
    owner.close()
    path = (
        directory
        / {"lease": ".lease", "core": "discovery.sqlite3", "native": "corpus.sqlite3"}[storage]
    )
    if damage == "missing":
        path.unlink()
    else:
        table = "discovery_workspace" if storage == "core" else "corpus_workspace"
        with closing(sqlite3.connect(path)) as db:
            db.execute(f"UPDATE {table} SET format='incompatible'")
            db.commit()
    retained = {
        entry.name: (entry.stat().st_ino, entry.read_bytes())
        for entry in directory.iterdir()
        if entry.is_file()
    }
    replacement = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with pytest.raises(
            (LostJournalValidationWorkspace, FileNotFoundError, StaleCorpusWorkspace)
        ):
            with unit(replacement, case) as work:
                assert work is not None
                work.corpus(selection)
        current = jobs.select_discovery_job(
            case.factory, build_id=case.build_id, admission=case.admission, catalog=case.catalog
        )
        assert current is not None and current.state == "lost"
        with pytest.raises(LostJournalValidationWorkspace):
            with unit(replacement, case):
                pytest.fail("lost accepted storage reopened work")
        assert {
            entry.name: (entry.stat().st_ino, entry.read_bytes())
            for entry in directory.iterdir()
            if entry.is_file()
        } == retained
        assert replacement.active_build_ids() == ()
    finally:
        replacement.close()


@pytest.mark.parametrize("storage", ["native", "core"])
@pytest.mark.parametrize("lost_reply", [False, True])
def test_stale_work_attempts_cleanup_once_and_keeps_each_unconfirmed_reply(
    case, qualified, storage, lost_reply
):
    owner = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(owner, case) as work:
            selection, _ = seed(work.metadata, qualified)
            native = work.corpus(selection)
            metadata = work.metadata
        target = native._corpus if storage == "native" else metadata
        uncertain = UncertainClose(target._db, lost_reply=lost_reply, failures=2)
        target._db = uncertain
        metadata.custody._held = False
        for remaining in (1, 0):
            with pytest.raises(OSError, match="unconfirmed"):
                with unit(owner, case):
                    pytest.fail("unconfirmed cleanup reopened work")
            assert uncertain.failures == remaining
            assert owner.active_build_ids() == (case.build_id,)
            busy(case)
        with unit(owner, case) as work:
            assert work is None
        assert owner.active_build_ids() == ()
        available(case)
    finally:
        owner.close()
