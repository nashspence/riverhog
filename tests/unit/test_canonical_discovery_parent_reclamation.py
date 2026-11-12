from __future__ import annotations

import errno
import fcntl
import os
import subprocess
import sys
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_parent_reclamation as owner
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.retained_workspace_handles import RetainedWorkspaceResource

from tests.support.retained_resource import fault_native as fault_native
from tests.support.validation_workspace import leased_workspace
from tests.unit.test_canonical_discovery_corpus_jobs import case as case
from tests.unit.test_canonical_discovery_corpus_jobs import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_jobs import original as original
from tests.unit.test_canonical_discovery_corpus_jobs import qualified as qualified

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original parent receiving")


def current(case, identity):
    with case.factory() as session:
        return jobs.DiscoveryJob.read(session.get(Row, identity))


def prefix(case, variant="lease"):
    actual = RetainedWorkspaceResource.acquire

    def interrupted(handle, name, **kwargs):
        if (variant == "root" and name == ".coordinator") or (
            variant == "directory" and name == ".lease"
        ):
            raise OSError("actual creator interrupted")
        return actual(handle, name, **kwargs)

    if variant != "absent":
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(RetainedWorkspaceResource, "acquire", interrupted)
            if variant == "lease":
                with leased_workspace(
                    case.root,
                    case.job.workspace_id,
                    create=True,
                    creation_current=lambda: jobs.discovery_worker_current(case.factory, case.job),
                ) as path:
                    assert path is not None
            else:
                with pytest.raises(OSError, match="creator interrupted"):
                    with leased_workspace(
                        case.root,
                        case.job.workspace_id,
                        create=True,
                        creation_current=lambda: jobs.discovery_worker_current(
                            case.factory, case.job
                        ),
                    ):
                        pytest.fail("interrupted creator entered its body")
    with case.factory() as session, session.begin():
        job = jobs.retire_discovery_job(session, case.job)
    assert job is not None and job.storage_state == "unconfirmed"
    return job


def receive(case, job):
    retained = owner.DiscoveryParentRetirement(case.factory, job, case.root)
    try:
        result = retained.reclaim()
        assert retained._closed
        assert all(
            getattr(retained, name) is None
            for name in ("_anchor", "_root", "_coordinator", "_directory", "_lease")
        )
        return result
    finally:
        retained.close()


@pytest.mark.parametrize("variant", ("absent", "root", "directory", "lease"))
def test_actual_initial_prefix_is_received_without_creation_or_repair(case, variant):
    job = prefix(case, variant)
    control = case.root / ".coordinator"
    inode = control.stat().st_ino if control.exists() else None
    result = receive(case, job)
    assert result.namespace_received and result.job.state == "directory"
    assert result.job.storage_key == job.storage_key
    assert current(case, job.workspace_id) == result.job
    assert not (case.root / job.workspace_id).exists()
    assert (control.stat().st_ino if control.exists() else None) == inode
    assert case.root.exists() is (variant != "absent")
    again = receive(case, result.job)
    assert again == result


@pytest.mark.parametrize("name", owner._DATA_NAMES)
@pytest.mark.parametrize("lease_present", (False, True))
def test_every_known_data_name_blocks_empty_prefix_without_disposal(case, name, lease_present):
    job = prefix(case, "lease" if lease_present else "directory")
    path = case.root / job.workspace_id
    entry = path / name
    entry.write_bytes(b"retained-original")
    with pytest.raises(owner.LostJournalValidationWorkspace, match="data still require"):
        receive(case, job)
    assert entry.read_bytes() == b"retained-original"
    assert (path / ".lease").exists() is lease_present
    assert current(case, job.workspace_id) == job


@pytest.mark.parametrize("name", ("order.sqlite3", "unclassified"))
def test_unknown_parent_entries_preserved_and_durable_directory_phase_remains(case, name):
    job = prefix(case)
    path = case.root / job.workspace_id
    (path / name).write_bytes(b"keep")
    with pytest.raises(owner.LostJournalValidationWorkspace, match="unclassified"):
        receive(case, job)
    successor = current(case, job.workspace_id)
    assert successor.state == successor.reclamation_phase == "directory"
    assert (path / name).read_bytes() == b"keep"
    assert not (path / ".lease").exists()
    with pytest.raises(owner.LostJournalValidationWorkspace, match="unclassified"):
        receive(case, successor)
    (path / name).unlink()
    assert receive(case, successor).namespace_received


@pytest.mark.parametrize("lock", ("root", "coordinator", "lease"))
def test_independent_actual_custody_returns_busy_without_mutation(case, lock):
    job = prefix(case)
    path = {
        "root": case.root,
        "coordinator": case.root / ".coordinator",
        "lease": case.root / job.workspace_id / ".lease",
    }[lock]
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = receive(case, job)
        assert not result.namespace_received and result.job == job
        assert current(case, job.workspace_id) == job
        assert path.exists()
    finally:
        os.close(descriptor)
    assert receive(case, job).namespace_received


def test_missing_root_control_with_existing_selected_directory_is_fault(case):
    job = prefix(case)
    (case.root / ".coordinator").unlink()
    with pytest.raises(owner.LostJournalValidationWorkspace, match="lost its root coordinator"):
        receive(case, job)
    assert (case.root / job.workspace_id / ".lease").exists()
    assert current(case, job.workspace_id) == job


@pytest.mark.parametrize("damage", ("phase", "identity", "version", "catalog-missing"))
def test_exact_original_catalog_fence_precedes_namespace_authority(case, damage):
    job = prefix(case)
    if damage == "phase":
        value = replace(job, storage_state="confirmed")
    elif damage == "identity":
        value = replace(job, original_build_id="other")
    elif damage == "version":
        value = replace(job, version="f" * 64)
    else:
        with case.factory() as session, session.begin():
            session.delete(session.get(Row, job.workspace_id))
        value = job
    with pytest.raises(owner.StaleArchiveTask):
        receive(case, value)
    assert (case.root / job.workspace_id / ".lease").exists()


@pytest.mark.parametrize(
    "damage",
    ("root-mode", "control-mode", "lease-mode", "lease-link", "root-symlink", "directory-symlink"),
)
def test_unsafe_namespace_is_preserved(case, damage):
    job = prefix(case)
    path = case.root / job.workspace_id
    if damage == "root-mode":
        case.root.chmod(0o755)
    elif damage == "control-mode":
        (case.root / ".coordinator").chmod(0o644)
    elif damage == "lease-mode":
        (path / ".lease").chmod(0o644)
    elif damage == "lease-link":
        os.link(path / ".lease", path / "retained-alias")
    else:
        selected = case.root if damage == "root-symlink" else path
        original = selected.with_name(selected.name + "-original")
        selected.rename(original)
        selected.symlink_to(original, target_is_directory=True)
    with pytest.raises(owner.LostJournalValidationWorkspace):
        receive(case, job)
    assert os.path.lexists(path)
    assert current(case, job.workspace_id) == job


def test_owner_parent_may_be_nonprivate_but_not_mutable_by_others(case):
    job = prefix(case, "absent")
    case.root.parent.chmod(0o755)
    assert receive(case, job).namespace_received


@pytest.mark.parametrize("after", (False, True))
@pytest.mark.parametrize("stage", ("before-phase", "after-unlink", "after-rmdir"))
def test_sync_uncertainty_never_returns_namespace_receiving(case, monkeypatch, after, stage):
    job = prefix(case)
    retained = owner.DiscoveryParentRetirement(case.factory, job, case.root)
    actual = os.fsync
    calls = 0

    def uncertain(descriptor):
        nonlocal calls
        calls += 1
        selected = {"before-phase": 1, "after-unlink": 2, "after-rmdir": 3}[stage]
        if calls == selected:
            if after:
                actual(descriptor)
            raise OSError(errno.ENOSPC, "actual sync uncertain")
        return actual(descriptor)

    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", uncertain)
        with pytest.raises(owner.JournalValidationCapacity):
            retained.reclaim()
    assert retained._closed
    successor = current(case, job.workspace_id)
    assert (successor.state == "directory") is (stage != "before-phase")
    assert receive(case, successor).namespace_received


@pytest.mark.parametrize("committed", (False, True))
def test_phase_rollback_or_committed_lost_reply_preserves_cold_receiving(
    case, monkeypatch, committed
):
    job = prefix(case)
    actual = jobs.advance_discovery_reclamation
    advance = owner.DiscoveryParentRetirement._advance

    def rollback(session, before, phase):
        assert actual(session, before, phase) is not None
        raise OSError("phase reply uncertain")

    def lost(retained):
        advance(retained)
        raise OSError("phase reply uncertain")

    with monkeypatch.context() as patch:
        patch.setattr(owner.DiscoveryParentRetirement, "_advance", lost) if committed else (
            patch.setattr(jobs, "advance_discovery_reclamation", rollback)
        )
        with pytest.raises(OSError, match="phase reply uncertain"):
            receive(case, job)
    successor = current(case, job.workspace_id)
    assert (successor.state == "directory") is committed
    assert (case.root / job.workspace_id / ".lease").exists()
    assert receive(case, successor).namespace_received


@pytest.mark.growth_witness(
    "canonical-discovery-parent-close/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_parent_reclamation.DiscoveryParentRetirement.close",
)
def test_close_refusal_retains_owner_and_kernel_custody_until_actual_receiving(
    case, monkeypatch, record_property, fault_native
):
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    from tests.support.workspace_reclamation import execution_sample

    native, _ = fault_native
    monkeypatch.setattr(owner, "Resource", native.Resource)
    monkeypatch.setattr(owner, "Borrow", native.Borrow)
    job = prefix(case)
    retained = owner.DiscoveryParentRetirement(case.factory, job, case.root)
    binding = growth_binding(owner.DiscoveryParentRetirement.close)
    projection = project_bindings((binding,))
    samples = []
    close = retained._close_owned

    def measured_close():
        occupied = sum(
            resource.state not in {"new", "absent", "received"}
            for resource in retained._resources.values()
        )
        before = sum(resource.metrics["close_calls"] for resource in retained._resources.values())
        try:
            return close()
        finally:
            calls = (
                sum(resource.metrics["close_calls"] for resource in retained._resources.values())
                - before
            )
            metrics = {bound.metric: 0 for bound in binding.declaration.bounds}
            metrics.update(close_calls=calls, descriptor_slots=occupied)
            observed = execution_sample(
                binding, projection, {"retained_descriptors": occupied}, metrics, {}
            )
            validate_execution_samples(projection, binding.declaration.id, [observed])
            samples.append(observed)

    retained._close_owned = measured_close
    retained._resources["_lease"]._test_fault("before_close")
    with pytest.raises(RuntimeError, match="before-close refusal"):
        retained.reclaim()
    assert not retained._closed and retained._lease is not None
    assert retained._coordinator is not None
    independent = os.open(case.root / ".coordinator", os.O_RDWR)
    try:
        with pytest.raises(BlockingIOError):
            fcntl.flock(independent, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(independent)
    retained.close()
    assert retained._closed
    retained.close()
    assert [s["metrics"]["close_calls"] for s in samples] == [0, 5, 0]
    record_property("growth_samples", samples)
    assert receive(case, current(case, job.workspace_id)).namespace_received


@pytest.mark.parametrize("point", ("unlink", "rmdir"))
def test_actual_process_exit_after_parent_mutation_receives_same_original_job(case, point):
    job = prefix(case)
    script = """import os,sys
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob
from riverhog_core.canonical_discovery_parent_reclamation import DiscoveryParentRetirement
engine=create_catalog_engine(sys.argv[1]);factory=sessionmaker(engine,expire_on_commit=False)
with factory() as session: job=DiscoveryJob.read(session.get(Row,sys.argv[3]))
actual=getattr(os,sys.argv[4])
def exited(*args,**kwargs):
 actual(*args,**kwargs)
 os._exit(67)
setattr(os,sys.argv[4],exited)
retained=DiscoveryParentRetirement(factory,job,Path(sys.argv[2]))
retained.reclaim()
raise AssertionError("exit point not exercised")
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(case.engine.url),
            str(case.root),
            job.workspace_id,
            point,
        ],
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 67, result.stderr[-1000:]
    successor = current(case, job.workspace_id)
    assert successor.state == "directory"
    assert receive(case, successor).namespace_received
    assert current(case, job.workspace_id) == successor


@pytest.mark.parametrize("point", ("root", "directory", "lease"))
def test_actual_process_exit_during_generic_creation_is_received_without_repair(case, point):
    script = """import os,sys
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob,discovery_worker_current
from tests.support.validation_workspace import leased_workspace
engine=create_catalog_engine(sys.argv[1]);factory=sessionmaker(engine,expire_on_commit=False)
with factory() as session: job=DiscoveryJob.read(session.get(Row,sys.argv[3]))
actual=os.open
def interrupted(name,*args,**kwargs):
 if (sys.argv[4]=="root" and name==".coordinator") or (
  sys.argv[4]=="directory" and name==".lease"
 ): os._exit(68)
 result=actual(name,*args,**kwargs)
 if sys.argv[4]=="lease" and name==".lease": os._exit(68)
 return result
os.open=interrupted
with leased_workspace(
 Path(sys.argv[2]),job.workspace_id,create=True,
 creation_current=lambda: discovery_worker_current(factory,job)
): raise AssertionError("creation exit point not exercised")
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(case.engine.url),
            str(case.root),
            case.job.workspace_id,
            point,
        ],
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 68, result.stderr[-1000:]
    with case.factory() as session, session.begin():
        job = jobs.retire_discovery_job(session, case.job)
    assert job is not None
    result = receive(case, job)
    assert result.namespace_received and result.job.state == "directory"
    assert not (case.root / job.workspace_id).exists()


def test_actual_confirmed_core_file_and_handle_receiving_precede_parent_removal(case):
    from tests.unit.test_canonical_discovery_metadata_reclamation import finish, prepare_metadata

    job, path = prepare_metadata(case)
    job = finish(case, job)
    assert job.storage_state == "confirmed" and job.state == "directory"
    inode = (case.root / ".coordinator").stat().st_ino
    result = receive(case, job)
    assert result.namespace_received and result.job == job
    assert not path.exists()
    assert (case.root / ".coordinator").stat().st_ino == inode
    assert current(case, job.workspace_id) == job


def test_mutable_root_parent_never_supplies_missing_root_receiving(case):
    job = prefix(case, "absent")
    case.root.parent.chmod(0o775)
    with pytest.raises(owner.LostJournalValidationWorkspace, match="parent requires owner"):
        receive(case, job)
    assert current(case, job.workspace_id) == job


@pytest.mark.parametrize("selected_absent", (False, True))
def test_configured_root_replaced_after_actual_sync_never_reports_receiving(
    case, monkeypatch, selected_absent
):
    job = prefix(case)
    path = case.root / job.workspace_id
    if selected_absent:
        (path / ".lease").unlink()
        path.rmdir()
    original = case.root.stat()
    moved = case.root.with_name("original-owned")
    actual = os.fsync
    swapped = False

    def replaced(descriptor):
        nonlocal swapped
        actual(descriptor)
        held = os.fstat(descriptor)
        if not swapped and (held.st_dev, held.st_ino) == (original.st_dev, original.st_ino):
            swapped = True
            case.root.rename(moved)
            case.root.mkdir(mode=0o700)
            replacement = case.root / job.workspace_id
            replacement.mkdir(mode=0o700)
            (replacement / "unclassified").write_bytes(b"foreign-retained")

    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", replaced)
        with pytest.raises(owner.LostJournalValidationWorkspace, match="replaced"):
            receive(case, job)
    assert swapped
    assert (moved / ".coordinator").is_file()
    assert (path / "unclassified").read_bytes() == b"foreign-retained"
    assert current(case, job.workspace_id).state == "directory"
