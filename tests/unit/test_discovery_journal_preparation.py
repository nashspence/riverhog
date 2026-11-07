from __future__ import annotations

import pytest
from riverhog_core import canonical_discovery_journal_preparation as preparation
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import JournalValidationBudget, JournalValidationSession

from tests.unit.test_discovery_journal_owner import prepared, quantum


def ready(work, framing):
    while (provider := work.prepare_journal(framing)) is None:
        pass
    return provider


def test_actual_preparation_and_native_quanta_stay_warm_across_work_and_takeover(
    tmp_path,
    monkeypatch,
):
    actual, calls = JournalValidationSession.advance, []

    def advance(self, budget=None):
        calls.append(budget)
        return actual(self, budget)

    with prepared(tmp_path / "source") as case:
        monkeypatch.setattr(JournalValidationSession, "advance", advance)
        previous, provider, inode, native = None, None, None, None
        pending = 0
        while previous is None:
            with quantum(case) as (work, framing):
                before = len(calls)
                result = work.advance_journal(framing, budget=JournalValidationBudget(rows=8))
                assert len(calls) - before in {0, 1}
                if result is None:
                    pending += 1
                    assert work._slot.journal is None
                else:
                    previous = result
                    native, provider = work._slot.journal.session, work._slot.certificates
                    directory = work._slot.journal.directory
                    inode = (directory / "snapshot.sqlite3").stat().st_ino
                    epoch = work.metadata.job.worker_epoch
                    assert not result.complete
        assert pending > 4
        with quantum(case) as (work, framing):
            result = work.advance_journal(framing, budget=JournalValidationBudget(rows=8))
            assert work._slot.journal.session is native
            assert work._slot.certificates is provider
            assert result.accepted_frames >= previous.accepted_frames
            previous = result
        case.pool.close()
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with quantum(case) as (work, framing):
            result = work.advance_journal(framing, budget=JournalValidationBudget(rows=8))
            assert work.metadata.job.worker_epoch == epoch + 1
            assert work._slot.certificates.binding == provider.binding
            assert work._slot.journal.session is not native
            assert (directory / "snapshot.sqlite3").stat().st_ino == inode
            assert result.accepted_frames >= previous.accepted_frames
        while not result.complete:
            with quantum(case) as (work, framing):
                before = len(calls)
                result = work.advance_journal(framing, budget=JournalValidationBudget(rows=8))
                assert len(calls) == before + 1
        with quantum(case) as (work, framing):
            assert result.accepted_sha256 == work.journal_source(framing).binding.source_sha256
            with work._slot.journal.session.result() as summary:
                assert summary.journal_sha256 == result.accepted_sha256


def test_busy_native_custody_keeps_completed_preparation_and_retries_same_binding(tmp_path):
    with prepared(tmp_path / "source") as case:
        with quantum(case) as (work, framing):
            provider = ready(work, framing)
            child_root = work.metadata.custody.directory / "journals"
            identity = provider.binding.source.directory_id
            with lease_journal_validation_workspace(
                child_root, identity, create=True, creation_current=provider.verify_current
            ) as held:
                assert held is not None
                assert work.advance_journal(framing, budget=JournalValidationBudget(rows=8)) is None
                assert work._slot.certificates is provider
                assert work._slot.journal is None
                with work.metadata.read() as (db, key):
                    assert preparation.discovery_journal_preparation_progress(
                        db, key, work._slot.preparation
                    ).complete
            assert work.advance_journal(framing, budget=JournalValidationBudget(rows=8)) is not None
            assert work._slot.journal.certificates is provider


def test_cached_completed_evidence_does_not_skip_lost_preparation_controls(tmp_path):
    from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask

    with prepared(tmp_path / "source") as case:
        with quantum(case) as (work, framing):
            provider = ready(work, framing)
            assert provider.verify_current()
            with work.metadata.transaction() as (db, _):
                db.execute("DELETE FROM discovery_journal_preparation").close()
            with pytest.raises(StaleArchiveTask):
                work.advance_journal(framing, budget=JournalValidationBudget(rows=8))
            assert work._slot.journal is None
