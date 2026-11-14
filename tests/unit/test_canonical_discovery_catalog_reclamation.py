from __future__ import annotations

import fcntl
import os
import subprocess
import sys
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_parent_reclamation as owner
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from sqlalchemy import update

from tests.support.retained_resource import fault_native as fault_native
from tests.unit.test_canonical_discovery_parent_reclamation import case as case
from tests.unit.test_canonical_discovery_parent_reclamation import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_parent_reclamation import original as original
from tests.unit.test_canonical_discovery_parent_reclamation import prefix, receive
from tests.unit.test_canonical_discovery_parent_reclamation import qualified as qualified

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original catalog receiving")


def row(case, identity):
    with case.factory() as session:
        value = session.get(Row, identity)
        return None if value is None else jobs.DiscoveryJob.read(value)


def finish(case, job):
    retained = owner.DiscoveryParentRetirement(case.factory, job, case.root)
    try:
        return retained.finish()
    finally:
        retained.close()


@pytest.mark.parametrize("variant", ("absent", "root", "directory", "lease"))
def test_actual_parent_close_precedes_exact_catalog_deletion_and_cold_receiving(case, variant):
    job = prefix(case, variant)
    control = case.root / ".coordinator"
    inode = control.stat().st_ino if control.exists() else None
    result = finish(case, job)
    assert result.namespace_received and result.catalog_received
    assert result.job.state == result.job.reclamation_phase == "directory"
    assert row(case, job.workspace_id) is None
    assert not (case.root / job.workspace_id).exists()
    assert (control.stat().st_ino if control.exists() else None) == inode
    assert finish(case, result.job) == result
    assert not case.root.exists() if variant == "absent" else case.root.is_dir()


def test_actual_confirmed_core_chain_finishes_without_reopening_disposed_metadata(case):
    from tests.unit.test_canonical_discovery_metadata_reclamation import finish as metadata_finish
    from tests.unit.test_canonical_discovery_metadata_reclamation import prepare_metadata

    job, path = prepare_metadata(case)
    job = metadata_finish(case, job)
    assert job.storage_state == "confirmed"
    result = finish(case, job)
    assert result.namespace_received and result.catalog_received
    assert not path.exists() and row(case, job.workspace_id) is None
    assert finish(case, result.job) == result


@pytest.mark.parametrize("unknown", (False, True))
def test_missing_catalog_cannot_dispose_even_an_empty_surviving_namespace(case, unknown):
    job = prefix(case)
    path = case.root / job.workspace_id
    with case.factory() as session, session.begin():
        job = jobs.advance_discovery_reclamation(session, job, "directory")
        assert job is not None
        assert jobs.delete_retired_discovery_job(session, job)
    if unknown:
        (path / "unclassified").write_bytes(b"retained")
    lease = (path / ".lease").stat().st_ino
    with pytest.raises(owner.StaleArchiveTask, match="surviving parent"):
        finish(case, job)
    assert (path / ".lease").stat().st_ino == lease
    if unknown:
        assert (path / "unclassified").read_bytes() == b"retained"


@pytest.mark.parametrize(
    "field,value",
    (
        ("version", "f" * 64),
        ("original_build_id", "other"),
        ("storage_state", "confirmed"),
        ("failure", "different"),
        ("policy_sha256", "e" * 64),
        ("attempt_id", "f" * 32),
    ),
)
def test_exact_delete_distinguishes_present_stale_row_from_catalog_absence(case, field, value):
    job = receive(case, prefix(case)).job
    with case.factory() as session, session.begin():
        assert not jobs.delete_retired_discovery_job(session, replace(job, **{field: value}))
    assert row(case, job.workspace_id) == job
    assert finish(case, job).catalog_received


def test_unfinished_metadata_phase_never_acquires_catalog_deletion(case):
    job = prefix(case)
    with case.factory() as session, session.begin():
        assert not jobs.delete_retired_discovery_job(session, job)
    assert row(case, job.workspace_id) == job
    assert (case.root / job.workspace_id / ".lease").exists()


def test_busy_actual_parent_keeps_catalog_until_independent_lease_closes(case):
    job = prefix(case)
    descriptor = os.open(case.root / job.workspace_id / ".lease", os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        result = finish(case, job)
        assert not result.namespace_received and not result.catalog_received
        assert row(case, job.workspace_id) == job
    finally:
        os.close(descriptor)
    assert finish(case, job).catalog_received


def test_unknown_parent_entry_prevents_catalog_deletion(case):
    job = prefix(case)
    path = case.root / job.workspace_id
    (path / "unclassified").write_bytes(b"retained")
    with pytest.raises(owner.LostJournalValidationWorkspace, match="unclassified"):
        finish(case, job)
    assert row(case, job.workspace_id).state == "directory"
    assert (path / "unclassified").read_bytes() == b"retained"


def test_actual_close_refusal_keeps_original_catalog_and_remaining_custody(
    case, monkeypatch, fault_native
):
    native, _path = fault_native
    monkeypatch.setattr(owner, "Resource", native.Resource)
    monkeypatch.setattr(owner, "Borrow", native.Borrow)
    job = prefix(case)
    retained = owner.DiscoveryParentRetirement(case.factory, job, case.root)
    original = retained._resources["_lease"]
    original._test_fault("before_close")
    with pytest.raises(RuntimeError):
        retained.finish()
    assert retained._lease is not None and not retained._closed
    assert original.state == "live" and original.metrics["close_calls"] == 0
    assert all(
        retained._resources[slot].state == "live"
        for slot in ("_anchor", "_root", "_coordinator", "_directory")
    )
    assert row(case, job.workspace_id) == retained.job
    retained.close()
    retained.close()
    assert sum(resource.metrics["close_calls"] for resource in retained._resources.values()) == 5
    assert finish(case, retained.job).catalog_received


@pytest.mark.parametrize("committed", (False, True))
def test_catalog_delete_rollback_or_lost_reply_requires_new_physical_receiving(
    case, monkeypatch, committed
):
    job = prefix(case)
    retained = owner.DiscoveryParentRetirement(case.factory, job, case.root)
    actual_delete = jobs.delete_retired_discovery_job
    actual_finish = owner.DiscoveryParentRetirement._delete_catalog

    def rollback(session, value):
        assert actual_delete(session, value)
        raise OSError("catalog delete reply uncertain")

    def lost(value):
        actual_finish(value)
        raise OSError("catalog delete reply uncertain")

    with monkeypatch.context() as patch:
        if committed:
            patch.setattr(owner.DiscoveryParentRetirement, "_delete_catalog", lost)
        else:
            patch.setattr(jobs, "delete_retired_discovery_job", rollback)
        with pytest.raises(OSError, match="delete reply uncertain"):
            retained.finish()
    assert retained._closed
    assert not (case.root / job.workspace_id).exists()
    assert (row(case, job.workspace_id) is None) is committed
    result = finish(case, retained.job)
    assert result.namespace_received and result.catalog_received
    assert row(case, job.workspace_id) is None


def test_catalog_change_after_physical_receiving_never_deletes_a_stale_row(case, monkeypatch):
    job = prefix(case)
    actual = jobs.delete_retired_discovery_job

    def changed(session, value):
        session.execute(
            update(Row).where(Row.workspace_id == value.workspace_id).values(version="f" * 64)
        )
        assert not actual(session, value)
        return False

    with monkeypatch.context() as patch:
        patch.setattr(jobs, "delete_retired_discovery_job", changed)
        with pytest.raises(owner.StaleArchiveTask, match="original fence"):
            finish(case, job)
    current = row(case, job.workspace_id)
    assert current is not None and current.state == "directory"
    assert finish(case, current).catalog_received


def test_actual_process_exit_after_committed_catalog_delete_receives_original_absence(case):
    job = receive(case, prefix(case)).job
    script = """import os,sys
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob
from riverhog_core.canonical_discovery_parent_reclamation import DiscoveryParentRetirement
engine=create_catalog_engine(sys.argv[1]);factory=sessionmaker(engine,expire_on_commit=False)
with factory() as session:job=DiscoveryJob.read(session.get(Row,sys.argv[3]))
actual=DiscoveryParentRetirement._delete_catalog
def exited(retained):
 actual(retained)
 os._exit(69)
DiscoveryParentRetirement._delete_catalog=exited
retained=DiscoveryParentRetirement(factory,job,Path(sys.argv[2]))
retained.finish()
raise AssertionError("catalog exit point not exercised")
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(case.engine.url), str(case.root), job.workspace_id],
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 69, result.stderr[-1000:]
    assert row(case, job.workspace_id) is None
    assert finish(case, job).catalog_received
