from __future__ import annotations

from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_core import canonical_discovery_endpoint_index as index
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
)
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import JournalValidationBudget

from tests.unit.test_canonical_discovery_endpoint_index import before_edge, drive
from tests.unit.test_canonical_discovery_journal_extraction import run
from tests.unit.test_discovery_journal_owner import prepared, quantum


def test_endpoint_cursor_survives_actual_metadata_and_native_worker_takeover(tmp_path):
    with prepared(tmp_path / "source") as case:
        terminal = None
        while terminal is None or not terminal.complete:
            with quantum(case) as (application, framing):
                terminal = application.advance_journal(
                    framing, budget=JournalValidationBudget(rows=64)
                )
        with quantum(case) as (application, framing):
            metadata = application.metadata
            checked = application.advance_journal(framing, budget=JournalValidationBudget(rows=64))
            assert checked is not None and checked.complete
            native = application._slot.journal.session
            with native.result() as summary:
                with QualifiedJournalSource(summary, case.admission.identity) as source:
                    anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
                    work = index.EndpointIndexWork(source, anchor)
                    key = JournalTaskKey(
                        metadata.key.build_id, source.source_key, metadata.key.worker_id
                    )
                    with metadata.read() as (db, _):
                        run(JournalFactExtraction(db, key, source), 128)
                    with metadata.transaction() as (db, _):
                        index.begin_endpoint_index(db, key, work)
                    with metadata.read() as (db, _):
                        local = SimpleNamespace(db=db, key=key, work=work)
                        before = before_edge(local)
                        page = index.prepare_endpoint_index(db, key, work, octet_budget=4096)
                    with metadata.transaction() as (db, _):
                        saved = index.apply_endpoint_index(db, key, work, page)
                    assert saved.endpoints == before.endpoints + 1
                    directory, epoch = metadata.custody.directory, metadata.job.worker_epoch
                    inode = (directory / "discovery.sqlite3").stat().st_ino
                    stable_key = metadata.key
        case.pool.close()
        with pytest.raises(StaleArchiveTask):
            with metadata.transaction():
                pytest.fail("old metadata admitted a replaced worker")
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with quantum(case) as (application, framing):
            replacement = application.metadata
            assert replacement.job.worker_epoch == epoch + 1
            assert replacement.key == stable_key
            assert (directory / "discovery.sqlite3").stat().st_ino == inode
            checked = application.advance_journal(framing, budget=JournalValidationBudget(rows=64))
            assert checked is not None and checked.complete
            native = application._slot.journal.session
            with native.result() as summary:
                with QualifiedJournalSource(summary, case.admission.identity) as source:
                    current = index.EndpointIndexWork(source, anchor)
                    with replacement.transaction() as (db, _):
                        assert index.begin_endpoint_index(db, key, current) == saved
                        with pytest.raises(StaleJournalTask, match="exact detached"):
                            index.apply_endpoint_index(db, key, current, page)
                    with replacement.read() as (db, _):
                        local = SimpleNamespace(db=db, key=key, work=current)
                        remaining = drive(local)
                        completed = index.endpoint_index_progress(db, key, current)
                    assert completed.complete and completed.version > saved.version
                    assert remaining[0].expected == saved
