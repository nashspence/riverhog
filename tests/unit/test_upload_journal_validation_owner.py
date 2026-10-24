from __future__ import annotations

import hashlib
import subprocess
import sys

import pytest
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_provenance import JournalValidationBudget, validate_journal
from sqlalchemy import delete, select, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL, Journal
from tests.unit.test_upload_journal_certificates import ready


def selected(factory):
    with factory() as session:
        return jobs.ValidationJob.read(session.scalar(select(Job)))


def finish(owner, work):
    catalog = source_owner.admission_provenance_catalog()
    for _ in range(10000):
        step = owner.advance(work, catalog=catalog)
        if step.outcome is not None:
            return step.outcome
        assert step.progressed
    pytest.fail("small retained native validation failed to complete")


@pytest.mark.parametrize("malformed", [False, True])
def test_retained_owner_preserves_native_outcome_and_closes_catalog_readers(tmp_path, malformed):
    raw = source(journal_id=JOURNAL) + (b"malformed tail" if malformed else b"")
    failure = None
    try:
        with validate_journal(raw) as expected:
            expected_anchor = expected.anchor
    except ValueError as error:
        failure = str(error)
    with retained(tmp_path, raw) as (engine, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(factory, tmp_path / "native")
        try:
            outcome = finish(owner, work)
            assert outcome.failure == failure
            job = selected(factory)
            assert engine.pool.checkedout() == 0
            assert job.state == ("failed" if malformed else "complete")
            assert jobs.outcome(job) == outcome
            if failure is None:
                assert jobs.decode_progress(job.progress_json).complete
                assert outcome.terminal_entry_id == expected_anchor["through"]["entry_id"]
                assert outcome.terminal_json_sha256 == expected_anchor["through"]["json_sha256"]
            assert (
                owner.advance(work, catalog=source_owner.admission_provenance_catalog()).outcome
                == outcome
            )
            assert selected(factory).worker_epoch == 1
        finally:
            owner.close()


def test_capacity_pause_keeps_the_same_live_worker_and_unfinished_atom(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(factory, tmp_path / "native")
        try:
            assert not owner.advance(
                work,
                catalog=source_owner.admission_provenance_catalog(),
                budget=JournalValidationBudget(octets=1, source_octets=1),
            ).progressed
            assert finish(owner, work).failure is None
            assert selected(factory).worker_epoch == 1
        finally:
            owner.close()


def test_reopen_claims_a_new_epoch_and_never_recreates_lost_accepted_work(tmp_path):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(factory, root)
        owner.advance(work, catalog=source_owner.admission_provenance_catalog())
        job = selected(factory)
        owner.close()
        database = root / job.workspace_id / "snapshot.sqlite3"
        database.unlink()
        replacement = UploadJournalValidationOwner(factory, root)
        try:
            with pytest.raises(LostJournalValidationWorkspace):
                replacement.advance(work, catalog=source_owner.admission_provenance_catalog())
            assert selected(factory).state == "lost" and not database.exists()
            with pytest.raises(LostJournalValidationWorkspace):
                replacement.advance(work, catalog=source_owner.admission_provenance_catalog())
            assert not database.exists()
        finally:
            replacement.close()


def test_kernel_lease_rejects_a_second_owner_without_claiming_a_worker(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        root = tmp_path / "native"
        first = UploadJournalValidationOwner(factory, root)
        second = UploadJournalValidationOwner(factory, root)
        try:
            assert first.advance(
                work, catalog=source_owner.admission_provenance_catalog()
            ).progressed
            before = selected(factory)
            assert not second.advance(
                work, catalog=source_owner.admission_provenance_catalog()
            ).progressed
            assert selected(factory) == before
            first.close()
            assert second.advance(
                work, catalog=source_owner.admission_provenance_catalog()
            ).progressed
            assert selected(factory).worker_epoch == before.worker_epoch + 1
            assert finish(second, work).failure is None
        finally:
            first.close()
            second.close()


def test_real_worker_exit_adopts_committed_prefix_before_catalog_publication(tmp_path, monkeypatch):
    raw = source(journal_id=JOURNAL)
    root = tmp_path / "native"
    with retained(tmp_path, raw) as (engine, factory):
        work, _ = ready(factory)
        program = """import os,sys
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
factory=sessionmaker(create_catalog_engine(sys.argv[1]))
work=source_owner._read_upload_journal_validation(factory,1,sys.argv[3])
original=jobs.publish_progress
def publish(factory,work,job,progress,**kwargs):
 if progress is not None and progress.accepted_frames:
  os._exit(81)
 return original(factory,work,job,progress,**kwargs)
jobs.publish_progress=publish
owner=UploadJournalValidationOwner(factory,Path(sys.argv[2]))
for _ in range(10000):
 owner.advance(work,catalog=source_owner.admission_provenance_catalog())
raise AssertionError('confirmed prefix crash was not reached')
"""
        process = subprocess.run(
            [sys.executable, "-c", program, str(engine.url), str(root), JOURNAL],
            capture_output=True,
        )
        assert process.returncode == 81, process.stderr.decode()
        previous = selected(factory)
        assert jobs.decode_progress(previous.progress_json).accepted_frames == 0

        def forbidden(*_args, **_kwargs):
            pytest.fail("replacing a worker replayed an already accepted frame")

        monkeypatch.setattr(
            "riverhog_core.services.upload_journal_source.UploadJournalValidationSource.read_exact",
            forbidden,
        )
        replacement = UploadJournalValidationOwner(factory, root)
        try:
            outcome = finish(replacement, work)
            assert outcome.failure is None
            adopted = selected(factory)
            assert adopted.workspace_id == previous.workspace_id
            assert adopted.worker_epoch == previous.worker_epoch + 1
            assert jobs.decode_progress(adopted.progress_json).accepted_frames == 1
        finally:
            replacement.close()


def test_job_cleanup_pointer_survives_source_deletion_and_stale_owner_cannot_publish(tmp_path):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(factory, root)
        try:
            owner.advance(work, catalog=source_owner.admission_provenance_catalog())
            before = selected(factory)
            with factory() as session, session.begin():
                session.execute(delete(Journal))
            assert not owner.advance(
                work, catalog=source_owner.admission_provenance_catalog()
            ).progressed
            orphan = selected(factory)
            assert orphan.workspace_id == before.workspace_id
            assert orphan.collection_id is None and orphan.journal_id is None
            assert orphan.progress_json == before.progress_json
            assert (root / before.workspace_id / "snapshot.sqlite3").is_file()
        finally:
            owner.close()


@pytest.mark.parametrize("field", ["version", "worker_epoch", "source_binding_json"])
def test_stale_catalog_worker_cannot_publish_native_progress(tmp_path, field):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(factory, tmp_path / "native")
        try:
            owner.advance(work, catalog=source_owner.admission_provenance_catalog())
            before = selected(factory)
            replacement = {"version": "0" * 64, "worker_epoch": 99, "source_binding_json": "{}"}
            with factory() as session, session.begin():
                session.execute(update(Job).values({field: replacement[field]}))
            assert (
                jobs.publish_progress(
                    factory, work, before, jobs.decode_progress(before.progress_json)
                )
                is None
            )
            assert getattr(selected(factory), field) == replacement[field]
        finally:
            owner.close()


def test_changed_rank_manifest_cannot_advance_even_a_non_graph_unit(tmp_path):
    from riverhog_core.catalog_models import CollectionUploadProvenanceEpochScanRecord as Scan

    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(factory, tmp_path / "native")
        try:
            owner.advance(work, catalog=source_owner.admission_provenance_catalog())
            before = selected(factory)
            with factory() as session, session.begin():
                current = session.get(Scan, (1, JOURNAL))
                current.version += 1
            with pytest.raises(LostJournalValidationWorkspace):
                owner.advance(work, catalog=source_owner.admission_provenance_catalog())
            after = selected(factory)
            assert after.state == "lost" and after.progress_json == before.progress_json
            with factory() as session:
                assert session.get(Journal, (1, JOURNAL)).state == "validating"
        finally:
            owner.close()


def scheduler_for(factory):
    from riverhog_core.catalog_models import CollectionUploadRecord

    from tests.unit.test_journal_validation_handoff import service_for

    service = service_for(factory)
    with factory() as session, session.begin():
        upload = session.get(CollectionUploadRecord, 1)
        upload.planner_checkpoint_json = source_owner.incremental_volume_planner_checkpoint_bytes(
            source_owner.new_incremental_volume_planner(policy=service._config.volume_policy)
        ).decode()
    return service


def test_service_advances_the_same_native_worker_through_bounded_units(tmp_path):
    from tests.unit.test_journal_validation_handoff import terminal

    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        service = scheduler_for(factory)
        try:
            calls = 0
            while terminal(factory)["state"] == "validating":
                assert service.process_due_provenance_journal_validations(limit=1) == 1
                assert engine.pool.checkedout() == 0
                calls += 1
                assert calls < 1000
            assert calls > 1 and selected(factory).worker_epoch == 1
            with validate_journal(raw) as expected:
                assert terminal(factory)["anchor"] == expected.anchor
            assert service.process_due_provenance_journal_validations(limit=1) == 0
        finally:
            service.close()


def test_scheduler_progresses_two_exact_journals_without_reopening_their_atoms(tmp_path):
    from riverhog_core.catalog_models import CollectionUploadProvenanceJournalChunkRecord as Chunk

    from tests.unit.test_journal_validation_handoff import terminal

    other_id = "urn:uuid:22222222-2222-4222-8222-222222222222"
    raw = source(journal_id=JOURNAL)
    other = source(journal_id=other_id)
    with retained(tmp_path, raw) as (engine, factory):
        with factory() as session, session.begin():
            row = Journal(
                collection_id=1,
                journal_id=other_id,
                bytes=len(other),
                sha256=hashlib.sha256(other).hexdigest(),
                accepted_bytes=len(other),
                next_chunk_ordinal=1,
                content_hash_state="{}",
            )
            session.add(row)
            session.flush()
            session.add(
                Chunk(collection_id=1, journal_id=other_id, ordinal=0, byte_offset=0, content=other)
            )
            session.flush()
            source_owner._begin_upload_journal_capture(session, row)
        service = scheduler_for(factory)
        try:
            for _ in range(1000):
                with factory() as session:
                    states = list(session.scalars(select(Journal.state)))
                if states == ["sealed", "sealed"]:
                    break
                assert service.process_due_provenance_journal_validations(limit=1) == 1
                assert engine.pool.checkedout() == 0
            else:
                pytest.fail("independent journals stopped making bounded progress")
            with factory() as session:
                jobs_completed = list(session.scalars(select(Job)))
                assert len(jobs_completed) == 2
                assert {row.worker_epoch for row in jobs_completed} == {1}
                assert {row.state for row in jobs_completed} == {"complete"}
            with validate_journal(raw) as expected:
                assert terminal(factory)["anchor"] == expected.anchor
        finally:
            service.close()
