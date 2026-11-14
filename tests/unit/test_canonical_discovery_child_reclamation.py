from __future__ import annotations

import errno
import os

import pytest
from riverhog_core import canonical_discovery_child_reclamation as owner
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
)
from riverhog_core.retained_workspace_handles import RetainedWorkspaceResource

from tests.support.retirement_namespace import lease_retiring_journal_workspace
from tests.support.validation_workspace import leased_workspace
from tests.unit.test_canonical_discovery_metadata import owned
from tests.unit.test_canonical_discovery_retired_metadata import case as case
from tests.unit.test_canonical_discovery_retired_metadata import (
    child_binding,
    prepare_retired,
    register_children,
    retired,
)
from tests.unit.test_canonical_discovery_retired_metadata import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_retired_metadata import original as original
from tests.unit.test_canonical_discovery_retired_metadata import qualified as qualified

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original child reclamation")


def prepare_child(case, *, prefix="data", confirmed=True, extent=7, count=2):
    with owned(case) as (live, path):
        live.open()
        bindings = [child_binding(live.job, index + 1) for index in range(count)]
        with live.transaction() as (db, _):
            register_children(db, live.job, bindings, confirmed=confirmed)
        root = path / "journals"
        child = root / bindings[0].directory_id
        actual_acquire, actual_mkdir = RetainedWorkspaceResource.acquire, os.mkdir
        with pytest.MonkeyPatch.context() as patch:
            if prefix in {"root", "root-control", "directory"}:

                def acquired(handle, name, **kwargs):
                    if prefix == "root" and name == ".coordinator":
                        raise OSError(errno.EIO, "actual root-control creation interrupted")
                    if prefix == "directory" and name == ".lease" and kwargs.get("create"):
                        raise OSError(errno.EIO, "actual child-lease creation interrupted")
                    return actual_acquire(handle, name, **kwargs)

                def made(name, *args, **kwargs):
                    if prefix == "root-control" and name == bindings[0].directory_id:
                        raise OSError(errno.EIO, "actual child-directory creation interrupted")
                    return actual_mkdir(name, *args, **kwargs)

                patch.setattr(RetainedWorkspaceResource, "acquire", acquired)
                patch.setattr(os, "mkdir", made)
            if prefix != "absent-root":
                if prefix in {"root", "root-control", "directory"}:
                    with pytest.raises(OSError, match="creation interrupted"):
                        with leased_workspace(
                            root,
                            bindings[0].directory_id,
                            create=True,
                            creation_current=live.verify_current,
                        ):
                            pytest.fail("injected actual creation prefix unexpectedly completed")
                else:
                    with leased_workspace(
                        root,
                        bindings[0].directory_id,
                        create=True,
                        creation_current=live.verify_current,
                    ) as directory:
                        assert directory == child
                        if prefix == "data":
                            (child / "snapshot.sqlite3").write_bytes(b"x" * extent)
                            (child / "snapshot.sqlite3").chmod(0o600)
        with case.factory() as session, session.begin():
            job = jobs.retire_discovery_job(session, live.job)
        assert job is not None
    return job, path, bindings


def current_job(case, workspace):
    with case.factory() as session:
        return jobs.DiscoveryJob.read(session.get(Job, workspace))


def step(case, job, *, budget=3):
    with retired(case, job) as metadata:
        metadata.open()
        return owner.reclaim_discovery_child(metadata, octet_budget=budget)


def directory_job(case, job, *, budget=64):
    for _ in range(128):
        result = step(case, job, budget=budget)
        job = result.job
        if job.reclamation_phase == "child_directory":
            return job
        assert result.advanced
    pytest.fail("small exact child did not reach its directory phase")


def test_actual_frozen_native_borrower_prevents_parent_reclamation_then_allows_same_child(
    tmp_path, *, journal_constructions
):
    from tests.unit.test_discovery_journal_owner import finish, prepared, quantum

    retained = []
    try:
        with prepared(
            tmp_path / "actual-borrower", journal_constructions=journal_constructions
        ) as case:
            with quantum(case) as (work, framing):
                native = work.journal(framing)
                finish(native)
                first = native.result()
                last = first.borrow()
                retained.extend((first, last))
                binding = work.journal_source(framing).binding
                original_job = work.metadata.job
            with case.factory() as session, session.begin():
                job = jobs.retire_discovery_job(session, original_job)
            assert job is not None
            with pytest.raises(BlockingIOError, match="original journal construction"):
                case.pool.close()
            first.close()
            with lease_retiring_journal_workspace(case.root, job.workspace_id) as busy:
                assert busy is None
            assert last.journal_sha256 == binding.source_sha256
            last.close()
            case.pool.close()
            job = directory_job(case, job, budget=1024 * 1024)
            result = step(case, job)
            assert result.job.reclamation_phase == "children"
            empty = step(case, result.job)
            assert empty.children_empty and not empty.advanced
            assert (case.root / job.workspace_id).is_dir()
    finally:
        for result in retained:
            result.close()


def test_byte_units_keep_same_original_child_then_receive_directory_and_routing(case):
    job, path, bindings = prepare_child(case, extent=17)
    child = path / "journals" / bindings[0].directory_id
    lease = (child / ".lease").stat().st_ino
    remaining = 17
    while remaining:
        result = step(case, job)
        assert result.advanced and result.files is not None
        assert result.files.removed_octets <= 3
        remaining -= result.files.removed_octets
        if remaining:
            assert result.job == job and result.job.reclamation_phase == "children"
            assert (child / "snapshot.sqlite3").stat().st_size == remaining
        else:
            job = result.job
            assert job.reclamation_phase == "child_directory"
        assert (child / ".lease").stat().st_ino == lease
    assert job.reclamation_journal_id == bindings[0].journal_id
    result = step(case, job)
    assert result.advanced and result.job.reclamation_phase == "children"
    assert not child.exists() and (path / "journals" / ".coordinator").is_file()
    with retired(case, result.job) as metadata:
        metadata.open()
        assert metadata.child().binding == bindings[1]
    assert current_job(case, job.workspace_id) == result.job and path.is_dir()


@pytest.mark.parametrize("prefix", ("absent-root", "root", "root-control", "directory", "lease"))
def test_actual_unconfirmed_creation_prefix_is_received_without_repair(case, prefix):
    job, path, bindings = prepare_child(case, prefix=prefix, confirmed=False)
    selected = step(case, job)
    assert selected.advanced and selected.job.reclamation_phase == "child_directory"
    assert selected.files is None or selected.files.files_reclaimed
    result = step(case, selected.job)
    assert result.advanced and result.job.reclamation_phase == "children"
    assert not (path / "journals" / bindings[0].directory_id).exists()
    with retired(case, result.job) as metadata:
        metadata.open()
        assert metadata.child().binding == bindings[1]
    assert path.is_dir()


@pytest.mark.parametrize("damage", ("confirmed-lease", "unopened-data"))
def test_missing_lease_cannot_authorize_existing_native_files(case, damage):
    job, path, bindings = prepare_child(
        case,
        prefix="data" if damage == "confirmed-lease" else "directory",
        confirmed=damage == "confirmed-lease",
    )
    child = path / "journals" / bindings[0].directory_id
    if damage == "confirmed-lease":
        (child / ".lease").unlink()
    else:
        (child / "snapshot.sqlite3").write_bytes(b"preserved")
        (child / "snapshot.sqlite3").chmod(0o600)
    before = (child / "snapshot.sqlite3").read_bytes()
    with pytest.raises(LostJournalValidationWorkspace):
        step(case, job)
    assert (child / "snapshot.sqlite3").read_bytes() == before
    assert current_job(case, job.workspace_id) == job


def test_unclassified_entry_blocks_namespace_but_preserves_selected_routing(case):
    job, path, bindings = prepare_child(case)
    child = path / "journals" / bindings[0].directory_id
    (child / "unclassified").write_bytes(b"retained")
    selected = directory_job(case, job)
    with pytest.raises(LostJournalValidationWorkspace, match="unclassified"):
        step(case, selected)
    assert (child / "unclassified").read_bytes() == b"retained"
    assert current_job(case, job.workspace_id) == selected
    with retired(case, selected) as metadata:
        metadata.open()
        assert metadata.child().binding == bindings[0]


def test_busy_original_child_is_prompt_and_does_not_change_extents_or_catalog(case):
    job, path, bindings = prepare_child(case)
    root = path / "journals"
    with leased_workspace(root, bindings[0].directory_id, shared=True) as held:
        assert held is not None
        result = step(case, job)
        assert not result.advanced and result.job == job
        assert (held / "snapshot.sqlite3").stat().st_size == 7
    assert step(case, job).advanced


@pytest.mark.parametrize("after", (False, True))
def test_last_unlink_sync_uncertainty_does_not_promote_phase_until_received(
    case, monkeypatch, after
):
    job, path, bindings = prepare_child(case, extent=2)
    child = path / "journals" / bindings[0].directory_id
    actual = os.fsync

    def uncertain(descriptor):
        if after:
            actual(descriptor)
        raise OSError(errno.EIO, "actual child extent sync uncertain")

    # The lease helper has no fsync on a present retired child. The first
    # synchronization here is the actual Native unlink durability boundary.
    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", uncertain)
        with pytest.raises(OSError, match="extent sync uncertain"):
            step(case, job, budget=64)
    assert not (child / "snapshot.sqlite3").exists()
    assert current_job(case, job.workspace_id) == job
    result = step(case, job, budget=64)
    assert result.job.reclamation_phase == "child_directory"
    assert step(case, result.job).job.reclamation_phase == "children"


@pytest.mark.parametrize("after", (False, True))
def test_phase_rollback_or_lost_commit_retains_exact_selected_child(case, monkeypatch, after):
    job, path, bindings = prepare_child(case, extent=2)
    actual_advance = owner._advance
    actual_catalog = jobs.advance_discovery_reclamation

    def rollback(session, selected, phase, **kwargs):
        result = actual_catalog(session, selected, phase, **kwargs)
        assert result is not None
        raise OSError("phase reply uncertain")

    def lost(metadata, phase, selected=None):
        actual_advance(metadata, phase, selected)
        raise OSError("phase reply uncertain")

    with monkeypatch.context() as patch:
        if after:
            patch.setattr(owner, "_advance", lost)
        else:
            patch.setattr(jobs, "advance_discovery_reclamation", rollback)
        with pytest.raises(OSError, match="phase reply uncertain"):
            step(case, job, budget=64)
    current = current_job(case, job.workspace_id)
    if after:
        assert current.reclamation_phase == "child_directory"
        assert current.reclamation_directory_id == bindings[0].directory_id
    else:
        assert current == job
        current = step(case, current, budget=64).job
    assert step(case, current).job.reclamation_phase == "children"
    assert not (path / "journals" / bindings[0].directory_id).exists()


@pytest.mark.parametrize("operation", ("unlink", "rmdir", "fsync"))
@pytest.mark.parametrize("after", (False, True))
def test_namespace_interruption_retries_same_durable_selection(case, monkeypatch, operation, after):
    job, path, bindings = prepare_child(case, extent=0)
    job = directory_job(case, job)
    actual = getattr(os, operation)
    attempted = []

    def uncertain(*args, **kwargs):
        if operation != "unlink" or args[0] == ".lease":
            attempted.append(True)
            if after:
                actual(*args, **kwargs)
            raise OSError(errno.EIO, "namespace receiving uncertain")
        return actual(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(os, operation, uncertain)
        with pytest.raises(OSError, match="namespace receiving uncertain"):
            step(case, job)
    assert attempted and current_job(case, job.workspace_id) == job
    result = step(case, job)
    assert result.job.reclamation_phase == "children"
    assert not (path / "journals" / bindings[0].directory_id).exists()
    with retired(case, result.job) as metadata:
        metadata.open()
        assert metadata.child().binding == bindings[1]


def test_lost_routing_reply_keeps_selected_catalog_until_namespace_reconfirmed(case, monkeypatch):
    job, path, bindings = prepare_child(case, extent=0)
    job = directory_job(case, job)
    actual = owner.RetiredDiscoveryMetadata.retire_child

    def uncertain(metadata):
        assert actual(metadata)
        raise OSError("routing reply lost")

    with monkeypatch.context() as patch:
        patch.setattr(owner.RetiredDiscoveryMetadata, "retire_child", uncertain)
        with pytest.raises(OSError, match="routing reply lost"):
            step(case, job)
    assert current_job(case, job.workspace_id) == job
    assert not (path / "journals" / bindings[0].directory_id).exists()
    with retired(case, job) as metadata:
        metadata.open()
        assert metadata.child() is None
    assert step(case, job).job.reclamation_phase == "children"


@pytest.mark.parametrize("budget", (0, -1, True))
def test_extent_admission_rejects_before_retirement_work(case, budget):
    job, path, bindings = prepare_child(case)
    with retired(case, job) as metadata:
        metadata.open()
        with pytest.raises(ValueError, match="positive octet budget"):
            owner.reclaim_discovery_child(metadata, octet_budget=budget)
    assert current_job(case, job.workspace_id) == job
    assert (path / "journals" / bindings[0].directory_id / "snapshot.sqlite3").stat().st_size == 7


def test_no_registered_child_retains_parent_and_does_not_advance_corpus(case):
    job, path, _ = prepare_retired(case, confirmed=False, count=0)
    result = step(case, job)
    assert result.children_empty and not result.advanced and result.job == job
    assert path.is_dir() and current_job(case, job.workspace_id) == job
