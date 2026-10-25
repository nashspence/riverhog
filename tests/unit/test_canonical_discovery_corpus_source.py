from __future__ import annotations

import sqlite3
from contextlib import closing
from copy import copy
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_selection as selection
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_corpus_source import DiscoveryCorpusSource
from riverhog_core.canonical_discovery_history_work import ArchiveHistoryWork
from riverhog_provenance import IncompleteSourceError, RetainedCorpusValidator

from tests.unit.test_canonical_discovery_metadata import case as case
from tests.unit.test_canonical_discovery_metadata import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_metadata import original as original
from tests.unit.test_canonical_discovery_metadata import owned
from tests.unit.test_canonical_discovery_metadata import qualified as qualified


def seed(metadata, qualified):
    """Copy already qualified original work into this fixture's actual owned attempt."""
    initial, values = qualified
    _, admission, reader, manifest, _, anchors = values
    assert admission.identity == metadata.admission.identity
    with closing(sqlite3.connect(initial)) as original, metadata.transaction() as (db, key):
        tables = original.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'discovery_%'"
        ).fetchall()
        for (table,) in tables:
            if table == "discovery_work_format":
                continue
            columns = [row[1] for row in original.execute(f"PRAGMA table_info({table})")]
            db.execute(f"DELETE FROM {table}")
            for row in original.execute(f"SELECT * FROM {table}"):
                values = list(row)
                for column, value in (("build_id", key.build_id), ("worker_id", key.worker_id)):
                    if column in columns:
                        values[columns.index(column)] = value
                db.execute(f"INSERT INTO {table} VALUES({','.join('?' for _ in values)})", values)
    work = selection.CorpusSelectionWork(ArchiveHistoryWork(metadata.admission, reader), manifest)
    with metadata.transaction() as (db, key):
        selection.begin_corpus_selection(db, key, work)
    while True:
        with metadata.read() as (db, key):
            progress = selection.corpus_selection_progress(db, key, work)
            if progress.phase == "complete":
                frozen = selection.frozen_corpus_selection(db, key, work)
                break
            prepared = selection.prepare_corpus_selection(db, key, work)
        with metadata.transaction() as (db, key):
            selection.apply_corpus_selection(db, key, work, prepared)
    pinned = jobs.pin_discovery_corpus(metadata.factory, metadata.job, frozen)
    assert pinned is not None
    metadata.job = pinned
    return work, anchors


def test_native_receives_full_original_sources_and_exact_clipped_selections(
    case, qualified, original
):
    with owned(case) as (metadata, path):
        metadata.open()
        work, anchors = seed(metadata, qualified)
        source = DiscoveryCorpusSource(metadata, work)
        assert source.binding == metadata.job.binding
        assert source.binding.source_count == 1 and source.binding.selection_count == 3
        assert source.verify_current()
        assert source.admit_source(anchors[-1])
        assert not source.admit_source(anchors[0])
        assert all(source.admit_selection(anchor) for anchor in anchors)
        assert not source.admit_selection(replace(anchors[0], prefix_bytes=1))
        with RetainedCorpusValidator.open_retained(
            path, source, worker_epoch=metadata.job.worker_epoch, create=True, catalog=case.catalog
        ) as native:
            while not native.advance_source(original[1], frame_budget=1).complete:
                pass
            for anchor in anchors:
                native.select_source(anchor)
            while not native.advance_semantics(row_budget=1).complete:
                pass
            while not native.advance_dependencies(row_budget=1).complete:
                pass
            for kind in ("derivation", "specialization"):
                while not native.advance_order(kind, row_budget=1).complete:
                    pass
        assert source.verify_current()


@pytest.mark.parametrize("damage", ["worker", "binding", "admission", "freeze", "file", "closed"])
def test_source_rechecks_actual_owner_and_frozen_inputs(case, qualified, damage):
    with owned(case) as (metadata, path):
        metadata.open()
        work, anchors = seed(metadata, qualified)
        source = DiscoveryCorpusSource(metadata, work)
        assert source.verify_current()
        if damage == "worker":
            assert jobs.claim_discovery_worker(case.factory, metadata.job) is not None
        elif damage == "binding":
            metadata.job = replace(metadata.job, source_manifest_sha256="e" * 64)
        elif damage == "admission":
            work.history.admission = copy(case.admission)
        elif damage == "freeze":
            with metadata.transaction() as (db, _):
                db.execute("UPDATE discovery_corpus_selection SET selection_sha256=?", ("f" * 64,))
        elif damage == "file":
            (path / "discovery.sqlite3").unlink()
        else:
            metadata.close()
        assert not source.verify_current()
        with pytest.raises((StaleArchiveTask, IncompleteSourceError)):
            source.admit_selection(anchors[0])
