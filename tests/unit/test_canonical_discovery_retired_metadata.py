from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata
from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata
from riverhog_core.journal_validation_retirement import RetiringJournalWorkspace
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
)
from riverhog_provenance import JournalReplayBinding

from tests.support.retirement_namespace import lease_retiring_journal_workspace
from tests.support.validation_workspace import leased_workspace
from tests.unit.test_canonical_discovery_metadata import case as case
from tests.unit.test_canonical_discovery_metadata import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_metadata import original as original
from tests.unit.test_canonical_discovery_metadata import owned
from tests.unit.test_canonical_discovery_metadata import qualified as qualified

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX retired Core custody")


def child_binding(job, number=1, width=1):
    return JournalReplayBinding(
        attempt_id=job.attempt_id,
        directory_id=f"{number:032x}",
        journal_id=f"urn:uuid:{number:08x}-1111-4111-8111-111111111111",
        source_bytes="9" * width,
        source_sha256="e" * 64,
        source_version="0",
        frame_count="0",
        catalog_sha256=job.catalog_sha256,
        require_profiles=False,
    )


def register_children(db, job, bindings, *, confirmed=True):
    for binding in bindings:
        db.execute(
            "INSERT INTO discovery_journal_children VALUES(?,?,?,?,?,?,?)",
            (
                job.original_build_id,
                job.admission_sha256,
                binding.journal_id,
                job.attempt_id,
                binding.directory_id,
                binding.to_json(),
                int(confirmed),
            ),
        ).close()


def prepare_retired(case, *, confirmed=True, count=2):
    with owned(case) as (live, path):
        live.open()
        bindings = [child_binding(live.job, index + 1) for index in range(count)]
        with live.transaction() as (db, _):
            register_children(db, live.job, bindings, confirmed=confirmed)
        with case.factory() as session, session.begin():
            job = jobs.retire_discovery_job(session, live.job)
        assert job is not None and job.storage_state == "confirmed"
        assert not live.verify_current()
    return job, path, bindings


@contextmanager
def retired(case, job):
    with lease_retiring_journal_workspace(case.root, job.workspace_id) as custody:
        assert custody is not None and custody.custody.verify_current()
        metadata = RetiredDiscoveryMetadata(case.factory, job, custody)
        try:
            yield metadata
        finally:
            metadata.close()


@pytest.mark.parametrize("confirmed", (False, True))
def test_retired_original_routing_reopens_without_live_worker_or_disposal(case, confirmed):
    job, path, bindings = prepare_retired(case, confirmed=confirmed)
    database = path / "discovery.sqlite3"
    before = database.read_bytes()
    with retired(case, job) as metadata:
        assert not metadata.verify_current()
        metadata.open()
        assert metadata.verify_current()
        first = metadata.child()
        assert first.binding == bindings[0] and first.confirmed is confirmed
        assert metadata.child() == first
        metadata.open()
        with leased_workspace(case.root, job.workspace_id, shared=True) as reader:
            assert reader is None
        with pytest.raises(StaleArchiveTask):
            DiscoveryMetadata(case.factory, job, case.admission, metadata.retirement.custody)
        assert database.read_bytes() == before
        with case.factory() as session:
            assert jobs.retire_discovery_job(session, job) == job
    assert not metadata.verify_current()
    with retired(case, job) as again:
        again.open()
        assert again.child() == first
    assert database.read_bytes() == before and (path / ".lease").exists()


@pytest.mark.parametrize(
    "damage", ("live", "unconfirmed", "later-phase", "path-only", "missing-namespace")
)
def test_retired_accessor_does_not_acquire_authority_from_labels(case, damage):
    job, path, _ = prepare_retired(case)
    with lease_retiring_journal_workspace(case.root, job.workspace_id) as custody:
        if damage == "live":
            job = replace(job, build_id=job.original_build_id)
        elif damage == "unconfirmed":
            job = replace(job, storage_state="unconfirmed")
        elif damage == "later-phase":
            job = replace(job, reclamation_phase="metadata")
        elif damage == "missing-namespace":
            custody = replace(custody, namespace=None)
        else:
            custody = RetiringJournalWorkspace(path, None, None)
        with pytest.raises(StaleArchiveTask):
            RetiredDiscoveryMetadata(case.factory, job, custody)


def test_retired_accessor_fences_exact_version_and_durable_selected_child(case):
    job, _, bindings = prepare_retired(case)
    with retired(case, job) as metadata:
        metadata.open()
        with case.factory() as session, session.begin():
            selected = jobs.advance_discovery_reclamation(
                session,
                job,
                "child_directory",
                journal_id=bindings[1].journal_id,
                directory_id=bindings[1].directory_id,
            )
        assert selected is not None
        assert not metadata.verify_current()
        with pytest.raises(StaleArchiveTask):
            metadata.child()
        metadata.close()
        with pytest.raises(StaleArchiveTask):
            metadata.open()
        fresh = RetiredDiscoveryMetadata(case.factory, selected, metadata.retirement)
        try:
            fresh.open()
            assert fresh.child().binding == bindings[1]
            fresh._db.execute(
                "DELETE FROM discovery_journal_children WHERE journal_id=?",
                (bindings[1].journal_id,),
            ).close()
            assert fresh.child() is None
            # An already-deleted routing row cannot erase the durable selected
            # namespace identity or promote physical/terminal completion.
            with case.factory() as session:
                assert jobs.retire_discovery_job(session, selected) == selected
            assert selected.reclamation_phase == "child_directory"
            assert selected.reclamation_directory_id == bindings[1].directory_id
        finally:
            fresh.close()


@pytest.mark.parametrize(
    "damage",
    (
        "format",
        "build",
        "attempt",
        "admission",
        "catalog",
        "policy",
        "schema",
        "worker",
        "missing",
        "mode",
        "link",
    ),
)
def test_retired_routing_faults_preserve_original_file_and_do_not_reinitialize(case, damage):
    job, path, _ = prepare_retired(case)
    file = path / "discovery.sqlite3"
    if damage == "missing":
        file.unlink()
    elif damage == "mode":
        file.chmod(0o644)
    elif damage == "link":
        os.link(file, path / "retained-alias")
    else:
        with sqlite3.connect(file) as db:
            if damage == "schema":
                db.execute("CREATE TABLE incompatible(value TEXT)").close()
            elif damage == "worker":
                db.execute("UPDATE discovery_archive_tasks SET worker_id=?", ("b" * 32,)).close()
            else:
                column = {
                    "format": "format",
                    "build": "build_id",
                    "attempt": "attempt_id",
                    "admission": "admission_sha256",
                    "catalog": "catalog_sha256",
                    "policy": "policy_sha256",
                }[damage]
                db.execute("UPDATE discovery_workspace SET " + column + "=?", ("changed",)).close()
    before = file.read_bytes() if file.exists() else None
    with retired(case, job) as metadata:
        with pytest.raises((LostJournalValidationWorkspace, FileNotFoundError)):
            metadata.open()
        assert not metadata.verify_current()
    assert (file.read_bytes() if file.exists() else None) == before


@pytest.mark.parametrize(
    "damage",
    ("worker", "directory", "journal", "attempt", "catalog", "profiles", "json", "disposition"),
)
def test_retired_child_binding_fault_is_explicit_and_preserves_routing(case, damage):
    job, path, bindings = prepare_retired(case)
    with sqlite3.connect(path / "discovery.sqlite3") as db:
        if damage in {"worker", "directory", "disposition"}:
            db.execute("PRAGMA ignore_check_constraints=ON").close()
            column, value = {
                "worker": ("worker_id", "b" * 32),
                "directory": ("directory_id", "b" * 32),
                "disposition": ("confirmed", 2),
            }[damage]
            db.execute(
                "UPDATE discovery_journal_children SET " + column + "=? WHERE journal_id=?",
                (value, bindings[0].journal_id),
            ).close()
        else:
            changed = {
                "journal": {"journal_id": bindings[1].journal_id},
                "attempt": {"attempt_id": "b" * 32},
                "catalog": {"catalog_sha256": "b" * 64},
                "profiles": {"require_profiles": True},
            }
            raw = (
                "not canonical binding"
                if damage == "json"
                else replace(bindings[0], **changed[damage]).to_json()
            )
            db.execute(
                "UPDATE discovery_journal_children SET binding_json=? WHERE journal_id=?",
                (raw, bindings[0].journal_id),
            ).close()
    before = (path / "discovery.sqlite3").read_bytes()
    with retired(case, job) as metadata:
        metadata.open()
        with pytest.raises(LostJournalValidationWorkspace):
            metadata.child()
    assert (path / "discovery.sqlite3").read_bytes() == before


@pytest.mark.parametrize("committed", (False, True))
def test_retired_routing_respects_catalog_rollback_and_lost_committed_phase_reply(case, committed):
    job, _, bindings = prepare_retired(case)
    with retired(case, job) as metadata:
        metadata.open()
        with pytest.raises(OSError, match="phase reply"):
            with case.factory() as session:
                with session.begin():
                    successor = jobs.advance_discovery_reclamation(
                        session,
                        job,
                        "child_directory",
                        journal_id=bindings[1].journal_id,
                        directory_id=bindings[1].directory_id,
                    )
                    assert successor is not None
                    if not committed:
                        raise OSError("phase reply rolled back")
                raise OSError("phase reply lost")
        assert metadata.verify_current() is (not committed)
        if committed:
            with pytest.raises(StaleArchiveTask):
                metadata.child()
        else:
            assert metadata.child().binding == bindings[0]


@pytest.mark.parametrize("lost_reply", (False, True))
def test_retired_uncertain_close_retains_partial_handle_and_exclusive_parent(case, lost_reply):
    job, _, _ = prepare_retired(case)
    with retired(case, job) as metadata:
        metadata.open()
        db = metadata._db

        class Close:
            attempted = False

            def close(self):
                if not self.attempted:
                    self.attempted = True
                    if lost_reply:
                        db.close()
                    raise OSError("retired Core close uncertain")
                db.close()

        partial = metadata._db = Close()
        with pytest.raises(OSError, match="close uncertain"):
            metadata.close()
        assert metadata._db is partial and not metadata.verify_current()
        with pytest.raises(StaleArchiveTask):
            metadata.child()
        with lease_retiring_journal_workspace(case.root, job.workspace_id) as busy:
            assert busy is None
        assert metadata.retirement.custody.verify_current()
        metadata.close()
        assert metadata._db is None and metadata._descriptor is None


def test_retired_partial_open_remains_owned_until_actual_close(case, monkeypatch):
    job, _, _ = prepare_retired(case)
    with retired(case, job) as metadata:

        def failed(*args, **kwargs):
            raise OSError("retired SQLite open failed")

        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", failed)
            with pytest.raises(OSError, match="SQLite open failed"):
                metadata.open()
        assert metadata._descriptor is not None and metadata._db is None
        with pytest.raises(LostJournalValidationWorkspace, match="unconfirmed"):
            metadata.open()
        with lease_retiring_journal_workspace(case.root, job.workspace_id) as busy:
            assert busy is None
        metadata.close()
        assert metadata._descriptor is None
