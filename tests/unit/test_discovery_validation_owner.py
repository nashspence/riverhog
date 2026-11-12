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
)
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import StaleCorpusWorkspace
from riverhog_provenance import corpus_workspace as native_owner

from tests.support.retained_resource import fault_native as fault_native
from tests.support.validation_workspace import leased_workspace
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
    with leased_workspace(case.root, case.job.workspace_id) as path:
        assert path is None


def available(case):
    with leased_workspace(case.root, case.job.workspace_id) as path:
        assert path is not None


def test_core_handle_and_original_checkpoint_stay_warm_across_units(
    case, qualified, *, journal_constructions
):
    owner = DiscoveryValidationOwner(
        case.factory,
        case.root,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
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
    case, qualified, original, *, journal_constructions
):
    owner = DiscoveryValidationOwner(
        case.factory,
        case.root,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
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
    reopened = DiscoveryValidationOwner(
        case.factory,
        case.root,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
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
    case, qualified, storage, lost_reply, *, journal_constructions
):
    owner = DiscoveryValidationOwner(
        case.factory,
        case.root,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
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
    case, qualified, monkeypatch, *, journal_constructions
):
    owner = DiscoveryValidationOwner(
        case.factory,
        case.root,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
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
    case, qualified, monkeypatch, boundary, *, journal_constructions
):
    owner = DiscoveryValidationOwner(
        case.factory,
        case.root,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
    synchronize, activate = (
        native_owner.CorpusConstruction.sync_directory,
        jobs.activate_discovery_corpus,
    )

    def lost_sync(construction, directory):
        synchronize(construction, directory)
        raise TimeoutError("Native opening confirmation lost")

    def lost_activation(factory, job):
        assert activate(factory, job) is not None
        raise TimeoutError("Native activation confirmation lost")

    try:
        with monkeypatch.context() as patch:
            if boundary == "native-open":
                patch.setattr(native_owner.CorpusConstruction, "sync_directory", lost_sync)
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
def test_busy_work_obeys_capacity_and_leaves_independent_slots_responsive(
    case, concurrency, *, journal_constructions
):
    other = independent_build(case, 2)
    owner = DiscoveryValidationOwner(
        case.factory,
        case.root,
        concurrency=concurrency,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
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
def test_uncertain_slot_close_does_not_skip_closing_independent_slots(
    case, qualified, cleanup, *, journal_constructions
):
    other = independent_build(case, 2)
    owner = DiscoveryValidationOwner(
        case.factory,
        case.root,
        concurrency=2,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
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
        with leased_workspace(case.root, second_id) as released:
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
    case, qualified, storage, damage, *, journal_constructions
):
    owner = DiscoveryValidationOwner(
        case.factory,
        case.root,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
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
    replacement = DiscoveryValidationOwner(
        case.factory,
        case.root,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
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
    case, qualified, storage, lost_reply, *, journal_constructions
):
    owner = DiscoveryValidationOwner(
        case.factory,
        case.root,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
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


@pytest.mark.parametrize("storage", ("none", "native", "core"))
def test_closed_native_source_keeps_busy_and_uncertain_handles_under_the_original_lease(
    case, qualified, storage, *, journal_constructions
):
    from tests.unit.test_canonical_discovery_corpus_jobs import _source_gate

    owner = DiscoveryValidationOwner(
        case.factory,
        case.root,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=journal_constructions.retain_corpus,
    )
    metadata = None
    expected = StaleArchiveTask if storage == "none" else OSError
    try:
        with pytest.raises(expected):
            with unit(owner, case) as work:
                assert work is not None
                metadata = work.metadata
                selection, _ = seed(metadata, qualified)
                native = work.corpus(selection)
                if storage != "none":
                    target = native._corpus if storage == "native" else metadata
                    target._db = UncertainClose(target._db, lost_reply=False)
                marker = case.root / metadata.job.workspace_id / "original-custody"
                marker.write_bytes(b"retained original workspace")
                _source_gate(case.factory)
                assert not jobs.discovery_worker_current(case.factory, metadata.job)
                # Maintenance cannot acquire the busy quantum's mutex or release
                # its independently owned lease before its handles actually close.
                owner.release_stale()
                assert owner.active_build_ids() == (case.build_id,)
                busy(case)
        assert metadata is not None and marker.read_bytes() == b"retained original workspace"
        if storage != "none":
            assert owner.active_build_ids() == (case.build_id,)
            busy(case)
            owner.release_stale()
        assert owner.active_build_ids() == ()
        available(case)
        with case.factory() as session:
            from riverhog_core.catalog_discovery_validation_models import (
                DiscoveryValidationJobRecord,
            )

            job = session.get(DiscoveryValidationJobRecord, metadata.job.workspace_id)
            assert job is not None and job.build_id == case.build_id
        assert marker.read_bytes() == b"retained original workspace"
    finally:
        owner.close()


def test_unreturned_native_corpus_keeps_original_frame_and_lease_until_guard_receives(
    case, qualified, monkeypatch, fault_native, journal_constructions
):
    import retained_resource

    native, _extension = fault_native
    monkeypatch.setattr(retained_resource, "Resource", native.Resource)
    monkeypatch.setattr(retained_resource, "Borrow", native.Borrow)
    frames = []

    def retain():
        original = journal_constructions.retain_corpus()
        frames.append(original)
        return original

    owner = DiscoveryValidationOwner(
        case.factory,
        case.root,
        construction_factory=journal_constructions.retain,
        corpus_construction_factory=retain,
    )

    def refuse_wrapper(*args, **kwargs):
        assert len(frames) == 1 and frames[0].storage is not None
        frames[0].guard._test_fault("before_close")
        raise MemoryError("Native corpus wrapper allocation failed")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(native_owner.CanonicalCorpusValidator, "_from_workspace", refuse_wrapper)
            with pytest.raises(RuntimeError, match="before-close refusal"):
                with unit(owner, case) as work:
                    selection, _anchors = seed(work.metadata, qualified)
                    work.corpus(selection)
        original = frames[0]
        slot = owner._slots[case.build_id]
        assert slot.closing and slot.corpus is None and slot.corpus_construction is original
        assert original.scope.received and original.borrow.received and not original.received
        assert original.guard.state == "live"
        assert original.guard.metrics["close_calls"] == 0
        assert owner.active_build_ids() == (case.build_id,)
        busy(case)
        owner.release_stale()
        assert original.received and original.guard.metrics["close_calls"] == 1
        assert owner.active_build_ids() == ()
        available(case)
    finally:
        owner.close()


def test_unknown_native_corpus_retains_original_while_independent_owner_work_receives(
    tmp_path, fault_native
):
    import subprocess
    import sys

    _native, extension = fault_native
    code = r"""
import importlib.util,os,sys
from pathlib import Path
import retained_resource
from riverhog_core.journal_storage_grants import JournalStorageGrants
from riverhog_core.services.journal_storage_resources import JournalStorageResources
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from tests.unit import test_canonical_discovery_corpus_jobs as fixtures
from tests.unit.test_canonical_discovery_selection import setup
from tests.unit.test_canonical_discovery_corpus_source import seed
from tests.unit.test_discovery_validation_owner import independent_build,busy
from tests.support.validation_workspace import leased_workspace
spec=importlib.util.spec_from_file_location("_retained_resource_test._native",sys.argv[2])
native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
retained_resource.Resource=native.Resource
retained_resource.Borrow=native.Borrow
root=Path(sys.argv[1])
originals=fixtures.original.__wrapped__();original=next(originals)
path=root/"qualified.sqlite"
qualified=(path,setup(path,original,inherited=True))
fixed=fixtures.fixed_inputs.__wrapped__(qualified)
cases=fixtures.case.__wrapped__(root,fixed);case=next(cases)
other=independent_build(case,2)
resources=JournalStorageResources(JournalStorageGrants(connections=1))
frames=[]
def retain():
 frame=resources.corpus();frames.append(frame);return frame
owner=DiscoveryValidationOwner(case.factory,case.root,concurrency=2,
 construction_factory=resources.journal,corpus_construction_factory=retain)
with owner.work(build_id=case.build_id,admission=case.admission,catalog=case.catalog) as work:
 selection,_=seed(work.metadata,qualified)
 first=work.corpus(selection)
 first_metadata=work.metadata
assert resources.status.connections==1
slot=owner._slots[case.build_id];frame=frames[0]
assert slot.corpus_construction is frame and slot.corpus is first
frame.guard._test_fault("unknown_close_open")
lost=jobs.lose_discovery_corpus(case.factory,first_metadata.job,failure="controlled custody loss")
assert lost is not None
try:owner.release_stale()
except (RuntimeError,OSError):pass
else:raise AssertionError("unknown original guard returned success")
assert owner._slots[case.build_id] is slot and slot.closing
assert slot.corpus is first and slot.corpus_construction is frame
assert frame.scope.received and frame.borrow.received
assert frame.guard.state=="unknown" and not frame.received
assert not slot.lease.received
busy(case)
assert resources.status.connections==0
calls=frame.guard.metrics["close_calls"]
with owner.work(build_id=other,admission=case.admission,catalog=case.catalog) as work:
 assert work is not None
 selection,anchors=seed(work.metadata,qualified)
 second=work.corpus(selection)
 second_id=work.metadata.job.workspace_id
 assert resources.status.connections==1
 while not second.advance_source(original[1],frame_budget=1).complete:pass
 for anchor in anchors:second.select_source(anchor)
 while not second.advance_semantics(row_budget=1).complete:pass
 while not second.advance_dependencies(row_budget=1).complete:pass
 for kind in ("derivation","specialization"):
  while not second.advance_order(kind,row_budget=1).complete:pass
assert len(frames)==2 and not frames[1].received
assert owner._slots[case.build_id] is slot and slot.corpus_construction is frame
try:owner.close()
except (RuntimeError,OSError):pass
else:raise AssertionError("unknown original became received during close")
assert frames[1].received and resources.status.connections==0
assert owner.active_build_ids()==(case.build_id,)
with leased_workspace(case.root,second_id) as released:assert released is not None
for _ in range(17):
 try:owner.release_stale()
 except (RuntimeError,OSError):pass
 else:raise AssertionError("unknown original became received during retry")
assert owner._slots[case.build_id] is slot and slot.corpus is first
assert slot.corpus_construction is frame and not frame.received and not slot.lease.received
assert frame.guard.metrics["close_calls"]==calls
busy(case)
print("ACTUAL_CORPUS_UNKNOWN_RETAINED;INDEPENDENT_CORPUS_RECEIVED",flush=True)
# Process exit cleans the isolated fixture; it grants no original receiving.
os._exit(0)
"""
    run = subprocess.run(
        [sys.executable, "-c", code, str(tmp_path), str(extension)],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert run.returncode == 0, (run.stdout + run.stderr)[-5000:]
    assert "ACTUAL_CORPUS_UNKNOWN_RETAINED;INDEPENDENT_CORPUS_RECEIVED" in run.stdout
