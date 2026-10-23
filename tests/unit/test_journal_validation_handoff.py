from __future__ import annotations

import subprocess
import sys
import threading

import pytest
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceFrameRecord as Frame,
)
from riverhog_core.catalog_models import (
    CollectionUploadRecord,
)
from riverhog_core.services import collection_uploads as owner
from riverhog_provenance import validate_journal
from sqlalchemy import delete, update

from tests.unit.test_journal_capture import advance, checkpoint, retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import EXTENT, JOURNAL, Chunk, Journal


def complete_capture(factory):
    while (work := owner._read_upload_journal_capture(factory, 1, JOURNAL)) is not None:
        assert owner._commit_upload_journal_capture(factory, work, advance(work))
    work = owner._read_upload_journal_validation(factory, 1, JOURNAL)
    assert work is not None
    return work


def service_for(factory):
    service = object.__new__(owner.SqlAlchemyCollectionUploadService)
    from riverhog_core.runtime_config import RuntimeConfig
    from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner

    service._session_factory = factory
    service._config = RuntimeConfig.for_testing(database_url=str(factory.kw["bind"].url))
    service._journal_validation_owner = UploadJournalValidationOwner(
        factory, service._config.journal_validation_workspace
    )
    service._journal_validation_after = None
    from riverhog_core.services.upload_journal_validation_retirement import (
        UploadJournalValidationRetirement,
    )

    service._journal_validation_retirement = UploadJournalValidationRetirement(
        factory, service._config.journal_validation_workspace
    )
    service._journal_validation_cleanup_turn = False
    return service


def native_outcome(factory, work):
    from contextlib import closing

    from riverhog_core.runtime_config import RuntimeConfig
    from riverhog_core.services import upload_journal_epochs as epochs
    from riverhog_core.services import upload_journal_rank as rank
    from riverhog_core.services import upload_journal_replay as replay
    from riverhog_core.services import upload_journal_topology as topology
    from riverhog_core.services import upload_journal_union as union
    from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner

    catalog = owner.admission_provenance_catalog()
    if not replay.verify_upload_journal_replay_binding(factory, work, catalog):
        raise owner._StaleUploadJournalValidation
    for stage in (
        epochs.advance_upload_journal_epoch_scan,
        topology.advance_upload_journal_topology_capture,
        union.advance_upload_journal_epoch_union,
        rank.advance_upload_journal_epoch_rank,
    ):
        for _ in range(10000):
            if stage(factory, work):
                break
        else:
            pytest.fail("small native preparation stopped making progress")
    assert replay.prepare_upload_journal_replay(factory, work, catalog=catalog)
    config = RuntimeConfig.for_testing(database_url=str(factory.kw["bind"].url))
    with closing(
        UploadJournalValidationOwner(factory, config.journal_validation_workspace)
    ) as current:
        for _ in range(10000):
            step = current.advance(work, catalog=catalog)
            if step.outcome is not None:
                return step.outcome
            assert step.progressed
    pytest.fail("small native replay stopped making progress")


def terminal(factory):
    with factory() as session:
        return owner._journal_payload(session.get(Journal, (1, JOURNAL)))


def record_state(factory):
    with factory() as session:
        record = session.get(Journal, (1, JOURNAL))
        return tuple(getattr(record, column.name) for column in Journal.__table__.columns)


def test_native_computation_reads_exact_captured_windows_after_readers_close(tmp_path, monkeypatch):
    from riverhog_core.services.upload_journal_source import UploadJournalValidationSource

    raw = source(2 * EXTENT + 37, journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        work = complete_capture(factory)
        windows, atoms = [], []
        read_window = UploadJournalValidationSource.read_window
        read_exact = UploadJournalValidationSource.read_exact

        def window(current, *args):
            result = read_window(current, *args)
            assert engine.pool.checkedout() == 0
            windows.append(result)
            return result

        def exact(current, *args):
            result = read_exact(current, *args)
            assert engine.pool.checkedout() == 0
            atoms.append(result)
            return result

        monkeypatch.setattr(UploadJournalValidationSource, "read_window", window)
        monkeypatch.setattr(UploadJournalValidationSource, "read_exact", exact)
        outcome = native_outcome(factory, work)
        assert b"".join(windows) == raw and max(map(len, windows)) <= 65536
        assert atoms == [raw[1:-1]]
        assert outcome.failure is None and engine.pool.checkedout() == 0
        assert terminal(factory)["state"] == "validating" and terminal(factory)["anchor"] is None
        assert owner._commit_upload_journal_validation(factory, work, outcome)
        with validate_journal(raw) as expected:
            assert terminal(factory)["anchor"] == expected.anchor


@pytest.mark.parametrize("failure", [False, True])
@pytest.mark.parametrize(
    "fence",
    [
        "capture_attempt_id",
        "capture_directory_id",
        "capture_version",
        "capture_algorithm",
        "capture_source_sha256",
        "capture_source_bytes",
        "capture_phase",
        "sha256",
        "bytes",
        "capture_byte_offset",
        "capture_frame_count",
        "state",
    ],
)
def test_stale_success_and_failure_cannot_publish_or_read_replacement(tmp_path, failure, fence):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (_, factory):
        work = complete_capture(factory)
        outcome = native_outcome(factory, work)
        if failure:
            outcome = owner._UploadJournalValidationOutcome("rejected old attempt")
        replacements = {
            "capture_attempt_id": "replacement-attempt",
            "capture_directory_id": "replacement-directory",
            "capture_version": work.version + 1,
            "capture_algorithm": "replacement-algorithm",
            "capture_source_sha256": "d" * 64,
            "capture_source_bytes": len(raw) + 1,
            "capture_phase": "scanning",
            "sha256": "d" * 64,
            "bytes": len(raw) + 1,
            "capture_byte_offset": len(raw) - 1,
            "capture_frame_count": work.cursor.frame_count + 1,
            "state": "accepting",
        }
        with factory() as session, session.begin():
            session.execute(update(Journal).values({fence: replacements[fence]}))
        before = record_state(factory), checkpoint(factory)
        with pytest.raises(owner._StaleUploadJournalValidation):
            from riverhog_core.services.upload_journal_source import UploadJournalValidationSource

            UploadJournalValidationSource(
                factory, work, catalog=owner.admission_provenance_catalog()
            ).read_window(0, 1)
        assert not owner._commit_upload_journal_validation(factory, work, outcome)
        assert (record_state(factory), checkpoint(factory)) == before


@pytest.mark.parametrize("failure", [False, True])
def test_duplicate_terminal_publication_is_fenced(tmp_path, failure):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work = complete_capture(factory)
        outcome = (
            owner._UploadJournalValidationOutcome("native rejection")
            if failure
            else native_outcome(factory, work)
        )
        assert owner._commit_upload_journal_validation(factory, work, outcome)
        published = terminal(factory), checkpoint(factory)
        assert checkpoint(factory)[0] == work.version + 1
        assert terminal(factory)["state"] == ("failed" if failure else "sealed")
        assert not owner._commit_upload_journal_validation(factory, work, outcome)
        assert (terminal(factory), checkpoint(factory)) == published


@pytest.mark.parametrize("replace_attempt", [False, True])
def test_unrelated_catalog_work_progresses_during_native_computation(
    tmp_path, monkeypatch, replace_attempt
):
    from riverhog_provenance import JournalValidationSession

    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        complete_capture(factory)
        paused, resume = threading.Event(), threading.Event()
        native = JournalValidationSession.advance
        failures = []
        service = service_for(factory)

        def waiting(current, *args, **kwargs):
            assert engine.pool.checkedout() == 0
            paused.set()
            if not resume.wait(30):
                raise TimeoutError("native computation was not resumed")
            return native(current, *args, **kwargs)

        monkeypatch.setattr(JournalValidationSession, "advance", waiting)

        def worker():
            try:
                for _ in range(10000):
                    service._advance_provenance_journal_validation(1, JOURNAL)
                    if replace_attempt and paused.is_set():
                        return
                    if terminal(factory)["state"] == "sealed":
                        return
                raise AssertionError("small native service failed to complete")
            except BaseException as exc:
                failures.append(exc)

        thread = threading.Thread(target=worker)
        thread.start()
        try:
            assert paused.wait(10)
            assert engine.pool.checkedout() == 0
            with factory() as session, session.begin():
                session.execute(
                    update(CollectionUploadRecord).values(last_activity_at="2026-01-02T00:00:00Z")
                )
                if replace_attempt:
                    session.execute(update(Journal).values(capture_attempt_id="replacement"))
            with factory() as session:
                assert (
                    session.get(CollectionUploadRecord, 1).last_activity_at
                    == "2026-01-02T00:00:00Z"
                )
        finally:
            resume.set()
            thread.join(30)
        try:
            assert not thread.is_alive() and not failures
            assert engine.pool.checkedout() == 0
            assert terminal(factory)["state"] == ("validating" if replace_attempt else "sealed")
            if replace_attempt:
                assert terminal(factory)["anchor"] is None
                for _ in range(10000):
                    service._advance_provenance_journal_validation(1, JOURNAL)
                    assert engine.pool.checkedout() == 0
                    if terminal(factory)["state"] == "sealed":
                        break
                    assert terminal(factory)["state"] == "validating"
                    assert terminal(factory)["anchor"] is None
                else:
                    pytest.fail("replacement validation stopped making bounded progress")
        finally:
            service.close()


def test_captured_source_survives_original_transport_chunk_removal(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        work = complete_capture(factory)
        with factory() as session, session.begin():
            session.execute(delete(Chunk))
        outcome = native_outcome(factory, work)
        assert outcome.failure is None and engine.pool.checkedout() == 0
        assert owner._commit_upload_journal_validation(factory, work, outcome)
        assert terminal(factory)["state"] == "sealed" and terminal(factory)["anchor"] is not None


def test_lost_capture_manifest_link_before_publication_fails_closed(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work = complete_capture(factory)
        outcome = native_outcome(factory, work)
        with factory() as session, session.begin():
            session.execute(delete(Frame))
        before = record_state(factory)
        with pytest.raises(RuntimeError, match="missing frame position"):
            owner._commit_upload_journal_validation(factory, work, outcome)
        assert record_state(factory) == before
        assert terminal(factory)["state"] == "validating"
        assert terminal(factory)["anchor"] is None


@pytest.mark.parametrize("commit", [False, True])
def test_process_exit_preserves_only_committed_terminal_metadata(tmp_path, commit):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        complete_capture(factory)
        before = terminal(factory), checkpoint(factory)
        code = """
import os, sys
from contextlib import contextmanager
from riverhog_core.catalog_db import make_session_factory
from riverhog_core.services import collection_uploads as owner
factory = make_session_factory(sys.argv[1])
work = owner._read_upload_journal_validation(factory, 1, sys.argv[2])
from tests.unit.test_journal_validation_handoff import native_outcome
outcome = native_outcome(factory, work)
assert outcome.failure is None
original = owner.session_scope
@contextmanager
def interrupted(factory):
    with original(factory) as session:
        yield session
        os._exit(73)
if sys.argv[3] == 'False':
    owner.session_scope = interrupted
assert owner._commit_upload_journal_validation(factory, work, outcome)
os._exit(73)
"""
        result = subprocess.run(
            [sys.executable, "-c", code, str(engine.url), JOURNAL, str(commit)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 73, result.stderr
        engine.dispose()
        if commit:
            assert terminal(factory)["state"] == "sealed"
            assert checkpoint(factory)[0] == before[1][0] + 1
            with validate_journal(raw) as expected:
                assert terminal(factory)["anchor"] == expected.anchor
        else:
            assert (terminal(factory), checkpoint(factory)) == before
            service_for(factory)._advance_provenance_journal_validation(1, JOURNAL)
            assert terminal(factory)["state"] == "sealed"
