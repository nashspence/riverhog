from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from riverhog_core.services import upload_journal_result as owner
from riverhog_provenance import JournalValidationSession, open_retained_journal_result
from sqlalchemy import update

from tests.unit.test_upload_journal_result import JOURNAL, Job, Journal, sealed, source_owner


def prepare(session):
    return owner.prepare_upload_journal_result(
        session, 1, JOURNAL, catalog=source_owner.admission_provenance_catalog()
    )


def test_metadata_admission_detaches_original_binding_before_native_opening(tmp_path, monkeypatch):
    with sealed(tmp_path) as (engine, factory, root, job, raw):

        def forbidden(*args, **kwargs):
            pytest.fail("catalog-only admission opened Native storage or read source bytes")

        with monkeypatch.context() as patch:
            patch.setattr(owner, "open_retained_journal_result", forbidden)
            patch.setattr(owner, "lease_journal_validation_workspace", forbidden)
            patch.setattr(JournalValidationSession, "advance", forbidden)
            patch.setattr(source_owner, "_iter_upload_journal_chunks", forbidden)
            with factory() as session, session.begin():
                admitted = prepare(session)
        assert engine.pool.checkedout() == 0
        assert admitted.binding.to_json() == job.source_binding_json
        assert admitted.certificates.to_json() == job.certificate_binding_json
        assert admitted.confirmed.worker_epoch == job.worker_epoch
        assert admitted.confirmed.complete and admitted.binding.journal_id == JOURNAL
        with pytest.raises(FrozenInstanceError):
            admitted.job = None
        with pytest.raises(FrozenInstanceError):
            admitted.job.version = "changed"
        changed = admitted.anchor
        changed["through"]["entry_id"] = "another identity"
        assert admitted.anchor["through"]["entry_id"] != "another identity"

        class Source:
            binding = admitted.binding

            def verify_current(self):
                with factory() as session, session.begin():
                    return admitted.verify_current(session)

            def read_window(self, *args):
                forbidden()

            def read_exact(self, *args):
                forbidden()

        from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace

        # Native storage is admitted after the metadata transaction has ended.
        with lease_journal_validation_workspace(root, job.workspace_id, shared=True) as directory:
            assert directory is not None and engine.pool.checkedout() == 0
            openings = []
            with open_retained_journal_result(
                directory,
                Source(),
                confirmed=admitted.confirmed,
                certificates=admitted.certificates,
                catalog=source_owner.admission_provenance_catalog(),
                on_storage_open=openings.append,
            ) as result:
                assert result.anchor == admitted.anchor
                assert result.journal_sha256 == admitted.binding.source_sha256
                assert openings[0].closed and engine.pool.checkedout() == 0
        assert engine.pool.checkedout() == 0


@pytest.mark.parametrize("changed", ["source", "job", "missing"])
def test_detached_admission_checks_live_authority_in_a_new_short_transaction(tmp_path, changed):
    with sealed(tmp_path) as (_, factory, _root, job, _raw):
        with factory() as session, session.begin():
            admitted = prepare(session)
        with factory() as session, session.begin():
            assert admitted.verify_current(session)
            if changed == "source":
                session.execute(
                    update(Journal).where(Journal.collection_id == 1).values(capture_version=0)
                )
            elif changed == "job":
                session.execute(
                    update(Job).where(Job.workspace_id == job.workspace_id).values(version="e" * 64)
                )
            else:
                session.delete(session.get(Job, job.workspace_id))
        with factory() as session, session.begin():
            assert not admitted.verify_current(session)
        with factory() as session:
            assert not admitted.verify_current(session)


def test_catalog_only_admission_does_not_autoflush_unrelated_pending_rows(tmp_path):
    with sealed(tmp_path) as (_, factory, _root, _job, _raw):
        with factory() as session, session.begin():
            pending = Job(
                workspace_id="invalid pending caller fixture",
                attempt_id="not selected",
                source_binding_json="{}",
                certificate_binding_json="{}",
                version="0" * 64,
                state="opening",
            )
            session.add(pending)
            assert prepare(session).binding.journal_id == JOURNAL
            assert pending in session.new
            session.expunge(pending)


def test_prepare_requires_the_callers_current_catalog_transaction(tmp_path):
    with sealed(tmp_path) as (_, factory, _root, _job, _raw):
        with factory() as session:
            with pytest.raises(RuntimeError, match="transaction"):
                prepare(session)
