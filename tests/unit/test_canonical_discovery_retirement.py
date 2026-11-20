from __future__ import annotations

import fcntl
import os
import sqlite3
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_retirement as owner
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.retained_workspace_handles import RetainedWorkspaceResource

from tests.support.retirement_namespace import refuse_native_child_resource
from tests.support.validation_workspace import leased_workspace
from tests.unit.test_canonical_discovery_child_reclamation import prepare_child
from tests.unit.test_canonical_discovery_metadata import owned
from tests.unit.test_canonical_discovery_metadata_reclamation import prepare_metadata
from tests.unit.test_canonical_discovery_parent_reclamation import prefix
from tests.unit.test_canonical_discovery_retired_metadata import case as case
from tests.unit.test_canonical_discovery_retired_metadata import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_retired_metadata import original as original
from tests.unit.test_canonical_discovery_retired_metadata import qualified as qualified
from tests.unit.test_discovery_validation_owner import UncertainClose

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX retained discovery retirement")


def current(case, identity):
    with case.factory() as session:
        value = session.get(Row, identity)
        return None if value is None else jobs.DiscoveryJob.read(value)


def step(case, job, *, budget=1024 * 1024):
    retained = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
    try:
        result = retained.advance(octet_budget=budget)
        assert retained._closed and not retained._handles._callbacks
        assert retained._custody is None or not retained._custody._held
        return result
    finally:
        retained.close()


def finish(case, job):
    phases = []
    for _ in range(32):
        phases.append(job.reclamation_phase)
        result = step(case, job)
        assert result.advanced
        job = result.job
        if result.catalog_received:
            return result, phases
    pytest.fail("small original discovery did not complete its bounded turns")


def test_actual_full_core_child_metadata_parent_and_catalog_chain(case):
    job, path, bindings = prepare_child(case, extent=17, count=1)
    original = job.storage_key
    result = step(case, job, budget=3)
    assert result.job == job and result.removed_octets == 3
    assert (path / "journals" / bindings[0].directory_id / "snapshot.sqlite3").stat().st_size == 14
    result, phases = finish(case, result.job)
    assert set(phases) == {
        "children",
        "child_directory",
        "corpus",
        "journals_root",
        "metadata",
        "directory",
    }
    assert result.job.storage_key == original and not path.exists()
    assert current(case, job.workspace_id) is None
    assert step(case, result.job) == result
    assert (case.root / ".coordinator").is_file()


@pytest.mark.parametrize("variant", ("absent", "root", "directory", "lease"))
def test_actual_unconfirmed_creation_prefix_has_noncreating_receiving(case, variant):
    job = prefix(case, variant)
    result, _ = finish(case, job)
    assert result.catalog_received and current(case, job.workspace_id) is None
    assert not (case.root / job.workspace_id).exists()
    assert case.root.exists() is (variant != "absent")


def test_partial_metadata_is_never_reopened_after_an_extent(case, monkeypatch):
    job, path = prepare_metadata(case)
    first = step(case, job, budget=3)
    assert first.removed_octets == 3 and first.job == job
    actual = sqlite3.connect

    def opened(database, *args, **kwargs):
        assert "discovery.sqlite3" not in str(database)
        return actual(database, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", opened)
        result, _ = finish(case, first.job)
    assert result.catalog_received and not path.exists()


@pytest.mark.parametrize("target", ("root", "control", "lease"))
def test_busy_selected_custody_returns_without_progress_and_releases_partial_handles(case, target):
    job, path, _ = prepare_child(case, count=1)
    name = {"root": case.root, "control": case.root / ".coordinator", "lease": path / ".lease"}[
        target
    ]
    descriptor = os.open(name, os.O_RDONLY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        result = step(case, job)
        assert not result.advanced and result.job == job
        assert current(case, job.workspace_id) == job
    finally:
        os.close(descriptor)
    assert finish(case, job)[0].catalog_received


def test_root_coordination_is_released_before_child_work_and_other_identity_can_progress(
    case, monkeypatch
):
    job, _, _ = prepare_child(case, count=1)
    actual = owner.reclaim_discovery_child
    observed = []

    def responsive(metadata, **kwargs):
        with leased_workspace(
            case.root, "f" * 32, create=True, creation_current=lambda: True
        ) as sibling:
            assert sibling is not None
            observed.append(sibling)
        return actual(metadata, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(owner, "reclaim_discovery_child", responsive)
        assert step(case, job).advanced
    assert observed and (observed[0] / ".lease").exists()


@pytest.mark.parametrize("field,value", (("version", "f" * 64), ("attempt_id", "f" * 32)))
def test_stale_original_is_rejected_before_storage_acquisition(case, monkeypatch, field, value):
    job, path, _ = prepare_child(case, count=1)
    retained = owner.DiscoveryRetirementTurn(
        case.factory, replace(job, **{field: value}), case.root
    )

    def forbidden(*_args, **_kwargs):
        pytest.fail("stale original acquired storage")

    with monkeypatch.context() as patch:
        patch.setattr(os, "open", forbidden)
        with pytest.raises(owner.StaleArchiveTask):
            retained.advance(octet_budget=3)
    retained.close()
    assert path.exists() and current(case, job.workspace_id) == job


@pytest.mark.parametrize("target", ("coordinator", "lease"))
def test_confirmed_missing_controls_never_acquire_prefix_disposal(case, target):
    job, path, _ = prepare_child(case, count=1)
    selected = case.root / ".coordinator" if target == "coordinator" else path / ".lease"
    selected.unlink()
    retained = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
    try:
        with pytest.raises(owner.LostJournalValidationWorkspace, match="lost original controls"):
            retained.advance(octet_budget=3)
    finally:
        retained.close()
    assert (path / "discovery.sqlite3").is_file()
    assert current(case, job.workspace_id) == job and not selected.exists()


@pytest.mark.parametrize("lost_reply", (False, True))
def test_actual_core_close_refusal_retains_exclusive_parent_until_receiving(
    case, monkeypatch, lost_reply
):
    job, path, _ = prepare_child(case, count=1)
    retained = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
    actual = owner.RetiredDiscoveryMetadata.open

    def opened(metadata):
        actual(metadata)
        metadata._db = UncertainClose(metadata._db, lost_reply=lost_reply)

    with monkeypatch.context() as patch:
        patch.setattr(owner.RetiredDiscoveryMetadata, "open", opened)
        with pytest.raises(OSError):
            retained.advance(octet_budget=3)
    assert not retained._closed and retained._custody.verify_current()
    with leased_workspace(case.root, job.workspace_id, shared=True) as busy:
        assert busy is None
    with pytest.raises(RuntimeError, match="fresh retained turn"):
        retained.advance(octet_budget=3)
    retained.close()
    assert not retained._custody._held and retained._closed
    assert finish(case, current(case, job.workspace_id))[0].catalog_received
    assert not path.exists()


@pytest.mark.parametrize("mode", ("prefix-probe", "child-lease", "child-directory"))
def test_failed_child_descriptor_close_retains_child_and_parent_ownership(case, monkeypatch, mode):
    job, path, bindings = prepare_child(case, count=1)
    if mode == "child-directory":
        result = step(case, job)
        assert result.job.reclamation_phase == "child_directory"
        job = result.job
    retained = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
    child_inode = (path / "journals" / bindings[0].directory_id).stat().st_ino
    refusing = True

    with (
        monkeypatch.context() as patch,
        refuse_native_child_resource(
            patch,
            child_inode,
            lambda: refusing,
            lease=mode != "prefix-probe",
            message="actual child close not received",
        ) as native_triggered,
    ):
        with pytest.raises(OSError, match="child close not received"):
            retained.advance(octet_budget=1024 * 1024)
        assert native_triggered and retained._metadata._child_handles._callbacks
        assert not retained._metadata.verify_current()
        assert retained._custody.verify_current()
        with pytest.raises(OSError, match="child close not received"):
            retained.close()
        assert retained._metadata._db is not None and retained._custody.verify_current()
        with leased_workspace(case.root, job.workspace_id, shared=True) as busy:
            assert busy is None
        refusing = False
        retained.close()
    assert retained._closed and not retained._metadata._child_handles._callbacks
    assert finish(case, current(case, job.workspace_id))[0].catalog_received
    assert not path.exists()


def test_parent_lease_close_refusal_keeps_custody_held_until_actual_success(case, monkeypatch):
    job, _, _ = prepare_child(case, count=1)
    retained = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
    actual = RetainedWorkspaceResource.close
    refusing = True

    def closed(handle):
        if (
            refusing
            and retained._custody is not None
            and handle.borrow.active
            and handle.fileno() == retained._custody._lease
        ):
            raise OSError("actual parent close not received")
        return actual(handle)

    with monkeypatch.context() as patch:
        patch.setattr(RetainedWorkspaceResource, "close", closed)
        with pytest.raises(OSError, match="parent close not received"):
            retained.advance(octet_budget=3)
        assert retained._metadata._closed and retained._custody.verify_current()
        refusing = False
        retained.close()
    assert retained._closed and not retained._custody._held


@pytest.mark.parametrize("budget", (0, -1, True, 1.5))
def test_invalid_admission_never_starts_an_attempt(case, budget):
    job = prefix(case)
    retained = owner.DiscoveryRetirementTurn(case.factory, job, case.root)
    with pytest.raises(ValueError, match="positive octet"):
        retained.advance(octet_budget=budget)
    assert not retained._attempted
    retained.close()


def test_actual_unconfirmed_partial_core_creation_uses_file_receiving(case, monkeypatch):
    from riverhog_core import canonical_discovery_metadata as live

    def interrupted(_db):
        raise OSError("actual partial schema creation")

    with owned(case) as (metadata, path):
        with monkeypatch.context() as patch:
            patch.setattr(live, "initialize_archive_framing", interrupted)
            with pytest.raises(OSError, match="partial schema"):
                metadata.open()
        metadata.close()
        with case.factory() as session, session.begin():
            job = jobs.retire_discovery_job(session, metadata.job)
    assert job.storage_state == "unconfirmed" and (path / "discovery.sqlite3").is_file()
    assert finish(case, job)[0].catalog_received


def test_actual_pinned_corpus_extent_keeps_original_binding_and_finishes(case):
    from tests.unit.test_canonical_discovery_corpus_reclamation import prepare_corpus

    job, path = prepare_corpus(case, extent=17)
    binding = job.binding
    result = step(case, job, budget=3)
    assert result.job == job and result.removed_octets == 3
    assert result.job.binding == binding and (path / "corpus.sqlite3").stat().st_size == 14
    assert finish(case, result.job)[0].catalog_received
