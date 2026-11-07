from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    lease_journal_validation_workspace,
)
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_result as result_owner
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_provenance import (
    JournalValidationCapacity,
    JournalValidationSession,
    validate_journal,
)
from riverhog_provenance.graph_certificates import JournalGraphCertificateBinding
from riverhog_provenance.replay_binding import JournalReplayBinding
from sqlalchemy import delete, event, select, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL, Journal
from tests.unit.test_upload_journal_certificates import ready
from tests.unit.test_upload_journal_validation_owner import finish, selected


@contextmanager
def sealed(tmp_path, *, raw=None):
    raw = source(journal_id=JOURNAL) if raw is None else raw
    with retained(tmp_path, raw) as (engine, factory):
        work, _ = ready(factory)
        root = tmp_path / "native"
        owner = UploadJournalValidationOwner(factory, root)
        try:
            outcome = finish(owner, work)
            assert outcome.failure is None
            assert source_owner._commit_upload_journal_validation(factory, work, outcome)
        finally:
            owner.close()
        yield engine, factory, root, selected(factory), raw


def open_result(session, root):
    return result_owner.open_upload_journal_result(
        session, root, 1, JOURNAL, catalog=source_owner.admission_provenance_catalog()
    )


def test_actual_sealed_result_reopens_inside_catalog_writer_without_replay_or_new_epoch(
    tmp_path, monkeypatch
):
    with sealed(tmp_path) as (engine, factory, root, job, raw):
        with validate_journal(raw, catalog=source_owner.admission_provenance_catalog()) as original:
            expected = original.materialize()
        before = (root / job.workspace_id / "snapshot.sqlite3").read_bytes()

        def forbidden(*_args, **_kwargs):
            pytest.fail("a sealed result reread source or replayed native validation")

        monkeypatch.setattr(source_owner, "_iter_upload_journal_chunks", forbidden)
        monkeypatch.setattr(source_owner, "validate_journal_chunks", forbidden)
        monkeypatch.setattr(JournalValidationSession, "advance", forbidden)
        statements = []

        def observe(_connection, _cursor, statement, _parameters, _context, _many):
            statements.append(statement)

        with factory() as session, session.begin():
            session.execute(
                update(Journal).where(Journal.collection_id == 1).values(state="sealed")
            )
            event.listen(engine, "before_cursor_execute", observe)
            try:
                with open_result(session, root) as summary:
                    assert summary.materialize() == expected
            finally:
                event.remove(engine, "before_cursor_execute", observe)
        assert statements and all(sql.lstrip().upper().startswith("SELECT ") for sql in statements)
        assert selected(factory) == job
        assert (root / job.workspace_id / "snapshot.sqlite3").read_bytes() == before
        assert engine.pool.checkedout() == 0


def test_sealed_result_does_not_autoflush_unrelated_pending_catalog_work(tmp_path):
    with sealed(tmp_path) as (_, factory, root, _, _):
        with factory() as session:
            session.execute(select(Journal.collection_id)).close()
            pending = Job(
                workspace_id="invalid pending caller fixture",
                attempt_id="not selected",
                source_binding_json="{}",
                certificate_binding_json="{}",
                version="0" * 64,
                state="opening",
            )
            session.add(pending)
            with open_result(session, root) as summary:
                assert summary.journal_id == JOURNAL
            assert pending in session.new
            session.rollback()


def test_independent_sealed_readers_can_reopen_while_an_evicted_borrow_remains_live(tmp_path):
    with sealed(tmp_path) as (_, factory, root, job, _):
        with factory() as session, session.begin():
            first = open_result(session, root)
            last = first.borrow()
            first.close()
            try:
                with open_result(session, root) as second:
                    assert second.anchor == last.anchor
                with lease_journal_validation_workspace(root, job.workspace_id) as busy:
                    assert busy is None
            finally:
                last.close()
            with lease_journal_validation_workspace(root, job.workspace_id) as free:
                assert free is not None


def test_busy_sealed_result_leaves_the_same_completed_job_pending_for_retry(tmp_path):
    with sealed(tmp_path) as (_, factory, root, job, _):
        with lease_journal_validation_workspace(root, job.workspace_id):
            with factory() as session, session.begin():
                with pytest.raises(JournalValidationCapacity, match="busy"):
                    open_result(session, root)
        assert selected(factory) == job
        with factory() as session, session.begin(), open_result(session, root) as summary:
            assert summary.journal_id == JOURNAL


@pytest.mark.parametrize(
    "fault",
    [
        "capture-version",
        "capture-attempt",
        "capture-directory",
        "terminal-anchor",
        "job-state",
        "job-worker",
        "job-progress",
        "job-certificate",
        "missing-job",
        "missing-database",
        "missing-coordinator",
    ],
)
def test_sealed_result_never_repairs_or_revalidates_changed_authority(tmp_path, fault):
    with sealed(tmp_path) as (_, factory, root, job, _):
        binding = JournalReplayBinding.from_json(job.source_binding_json)
        database = root / job.workspace_id / "snapshot.sqlite3"
        if fault == "missing-database":
            database.unlink()
        elif fault == "missing-coordinator":
            (root / ".coordinator").unlink()
        else:
            with factory() as session, session.begin():
                if fault == "missing-job":
                    session.execute(delete(Job).where(Job.workspace_id == job.workspace_id))
                elif fault.startswith("job-"):
                    values = {"state": "lost"}
                    if fault == "job-worker":
                        values = {"worker_epoch": job.worker_epoch + 1}
                    elif fault == "job-progress":
                        values = {
                            "progress_json": jobs.encode_progress(
                                replace(
                                    jobs.decode_progress(job.progress_json),
                                    checkpoint_sha256="0" * 64,
                                )
                            )
                        }
                    elif fault == "job-certificate":
                        values = {
                            "certificate_binding_json": replace(
                                JournalGraphCertificateBinding.from_json(
                                    job.certificate_binding_json
                                ),
                                manifest_sha256="e" * 64,
                            ).to_json()
                        }
                    session.execute(
                        update(Job).where(Job.workspace_id == job.workspace_id).values(**values)
                    )
                else:
                    values = {
                        "capture-version": {"capture_version": int(binding.source_version) + 2},
                        "capture-attempt": {"capture_attempt_id": "another-capture"},
                        "capture-directory": {"capture_directory_id": "another-directory"},
                        "terminal-anchor": {
                            "terminal_entry_id": "urn:uuid:"
                            + "0" * 8
                            + "-0000-0000-0000-"
                            + "0" * 12
                        },
                    }[fault]
                    session.execute(
                        update(Journal).where(Journal.collection_id == 1).values(**values)
                    )
        with factory() as session, session.begin():
            with pytest.raises(LostJournalValidationWorkspace):
                open_result(session, root)
        assert database.exists() is (fault != "missing-database")
        if fault == "missing-coordinator":
            assert not (root / ".coordinator").exists()


def test_changed_source_during_readonly_admission_releases_the_untransferred_lease(
    tmp_path, monkeypatch
):
    with sealed(tmp_path) as (_, factory, root, job, _):
        original = result_owner._SealedSource.verify_current
        calls = []

        def replaced(self):
            calls.append(1)
            if len(calls) == 2:
                self.session.execute(
                    update(Job).where(Job.workspace_id == job.workspace_id).values(version="e" * 64)
                )
            return original(self)

        monkeypatch.setattr(result_owner._SealedSource, "verify_current", replaced)
        with factory() as session, session.begin():
            with pytest.raises(LostJournalValidationWorkspace):
                open_result(session, root)
        assert len(calls) == 2
        with lease_journal_validation_workspace(root, job.workspace_id) as free:
            assert free is not None
