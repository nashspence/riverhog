from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from threading import Event

import pytest
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_provenance import JournalValidationBudget
from riverhog_provenance._entry_preparation import EntryPreparation

from tests.unit.test_discovery_journal_owner import prepared, quantum
from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_certificates import ready
from tests.unit.test_upload_journal_validation_owner import selected

QUERY = (
    "WITH RECURSIVE work(n) AS (VALUES(0) UNION ALL "
    "SELECT n+1 FROM work WHERE n<4096) SELECT sum(n) FROM work"
)


def suspended_query(monkeypatch):
    entered, release = Event(), Event()
    original = EntryPreparation._admission
    children = []

    def admission(child):
        children.append(child)
        entered.set()
        assert release.wait(timeout=15)
        with closing(child.db.execute(QUERY)) as rows:
            rows.fetchone()
        return original(child)

    monkeypatch.setattr(EntryPreparation, "_admission", admission)
    return entered, release, children


def test_upload_shutdown_interrupts_actual_native_query_and_receives_original_slot(
    tmp_path, monkeypatch, journal_constructions
):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(
            factory, tmp_path / "native", construction_factory=journal_constructions.retain
        )
        catalog = source_owner.admission_provenance_catalog()
        try:
            for _ in range(32):
                assert owner.advance(work, catalog=catalog).progressed
                slot = next(iter(owner._slots.values()))
                native = slot.session
                if native.progress().phase == "entry-admission":
                    break
            else:
                pytest.fail("small actual upload did not reach Native preparation")
            assert slot.stopping is owner._stopping
            before = selected(factory)
            entered, release, children = suspended_query(monkeypatch)
            with ThreadPoolExecutor(max_workers=2) as executor:
                running = executor.submit(owner.advance, work, catalog=catalog)
                try:
                    assert entered.wait(timeout=15)
                    assert engine.pool.checkedout() == 0
                    closing_owner = executor.submit(owner.close)
                    assert slot.stopping.wait(timeout=15)
                finally:
                    release.set()
                with pytest.raises(InterruptedError, match="Native journal validation is stopping"):
                    running.result(timeout=15)
                closing_owner.result(timeout=15)
            assert len(children) == 1 and children[0].epoch is native._ledger.epoch
            assert native.storage_received and owner.active_keys() == ()
            assert selected(factory) == before
            assert engine.pool.checkedout() == 0
        finally:
            owner.close()


def test_discovery_shutdown_interrupts_actual_native_query_and_receives_original_slot(
    tmp_path, monkeypatch, journal_constructions
):
    with prepared(tmp_path / "source", journal_constructions=journal_constructions) as case:
        for _ in range(256):
            with quantum(case) as (work, framing):
                progress = work.advance_journal(framing, budget=JournalValidationBudget(rows=8))
                if progress is not None and progress.phase == "entry-admission":
                    slot = work._slot
                    native = slot.journal.session
                    break
        else:
            pytest.fail("small actual discovery did not reach Native preparation")
        assert slot.stopping is case.pool._stopping
        entered, release, children = suspended_query(monkeypatch)

        def advance():
            with quantum(case) as (work, framing):
                return work.advance_journal(framing, budget=JournalValidationBudget(rows=8))

        with ThreadPoolExecutor(max_workers=2) as executor:
            running = executor.submit(advance)
            try:
                assert entered.wait(timeout=15)
                assert case.engine.pool.checkedout() == 0
                closing_owner = executor.submit(case.pool.close)
                assert slot.stopping.wait(timeout=15)
            finally:
                release.set()
            with pytest.raises(InterruptedError, match="Native journal validation is stopping"):
                running.result(timeout=15)
            closing_owner.result(timeout=15)
        assert len(children) == 1 and children[0].epoch is native._ledger.epoch
        assert native.storage_received and case.pool.active_build_ids() == ()
        assert case.engine.pool.checkedout() == 0
