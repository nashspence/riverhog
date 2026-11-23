from __future__ import annotations

import hashlib
import sqlite3

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_provenance import IncompleteSourceError
from test_corpus_delivery_support import delivery_source
from test_retained_corpus_workspace import OwnedCorpusSource, directory, ingest, open_workspace


@pytest.mark.parametrize("operation", ["historical_assertion", "selected_delivery_support"])
def test_retained_point_lookup_has_no_checkpoint_write(
    journal_constructions, tmp_path, graph, who, catalog, operation
):
    path = directory(tmp_path)
    with delivery_source(graph, who, catalog) as (summary, arguments):
        source = OwnedCorpusSource(summary, catalog)
        with open_workspace(
            path, source, create=True, catalog=catalog, journal_constructions=journal_constructions
        ) as workspace:
            ingest(workspace, summary, source)
            if operation == "historical_assertion":
                args = (HistoryJournalAnchor.from_mapping(summary.anchor), graph["states"][0]["id"])
                kwargs = {"byte_budget": 65536}
            else:
                args, kwargs = (), {**arguments, "metadata_byte_budget": 65536}
            expected = getattr(workspace._corpus, operation)(*args, **kwargs)
            before = workspace._load()
            changes = workspace._db.total_changes
            digest = hashlib.sha256((path / "corpus.sqlite3").read_bytes()).digest()
            source.calls.clear()
            writes = []

            def authorize(action, _table, _column, _database, _trigger):
                if action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE):
                    writes.append(action)
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            workspace._db.set_authorizer(authorize)
            try:
                for _ in range(2):
                    assert getattr(workspace, operation)(*args, **kwargs) == expected
            finally:
                workspace._db.set_authorizer(None)
            assert writes == []
            assert source.calls == ["current"] * 4
            assert workspace._load() == before
            assert workspace._db.total_changes == changes
            assert hashlib.sha256((path / "corpus.sqlite3").read_bytes()).digest() == digest
            assert not workspace._db.in_transaction


@pytest.mark.parametrize("operation", ["historical_assertion", "selected_delivery_support"])
def test_point_lookup_rechecks_current_source_before_returning(
    journal_constructions, tmp_path, graph, who, catalog, monkeypatch, operation
):
    path = directory(tmp_path)
    with delivery_source(graph, who, catalog) as (summary, arguments):
        source = OwnedCorpusSource(summary, catalog)
        with open_workspace(
            path, source, create=True, catalog=catalog, journal_constructions=journal_constructions
        ) as workspace:
            ingest(workspace, summary, source)
            if operation == "historical_assertion":
                args = (HistoryJournalAnchor.from_mapping(summary.anchor), graph["states"][0]["id"])
                kwargs = {"byte_budget": 65536}
            else:
                args, kwargs = (), {**arguments, "metadata_byte_budget": 65536}
            actual = getattr(workspace._corpus, operation)
            before = workspace._load()
            changes = workspace._db.total_changes
            returned = []

            def stale(*args, **kwargs):
                result = actual(*args, **kwargs)
                returned.append(result)
                source.current = False
                return result

            with monkeypatch.context() as patch:
                patch.setattr(workspace._corpus, operation, stale)
                with pytest.raises(IncompleteSourceError, match="no longer current"):
                    getattr(workspace, operation)(*args, **kwargs)
            assert len(returned) == 1
            assert workspace._load() == before
            assert workspace._db.total_changes == changes
            assert not workspace._db.in_transaction
            source.current = True
            assert getattr(workspace, operation)(*args, **kwargs) == returned[0]


@pytest.mark.parametrize("operation", ["historical_assertion", "selected_delivery_support"])
@pytest.mark.parametrize("field", ["worker_epoch", "version", "checkpoint", "binding"])
def test_point_lookup_rejects_metadata_change_before_receiving(
    journal_constructions, tmp_path, graph, who, catalog, monkeypatch, operation, field
):
    from riverhog_provenance import StaleCorpusWorkspace

    path = directory(tmp_path)
    with delivery_source(graph, who, catalog) as (summary, arguments):
        source = OwnedCorpusSource(summary, catalog)
        with open_workspace(
            path, source, create=True, catalog=catalog, journal_constructions=journal_constructions
        ) as workspace:
            ingest(workspace, summary, source)
            if operation == "historical_assertion":
                args = (HistoryJournalAnchor.from_mapping(summary.anchor), graph["states"][0]["id"])
                kwargs = {"byte_budget": 65536}
            else:
                args, kwargs = (), {**arguments, "metadata_byte_budget": 65536}
            before = workspace._load()
            changed = []
            actual = source.verify_current

            def current():
                if changed:
                    value = {
                        "worker_epoch": "999",
                        "version": str(before[1] + 1),
                        "checkpoint": b"{}",
                        "binding": b"foreign",
                    }[field]
                    workspace._db.execute(
                        "UPDATE corpus_workspace SET " + field + "=? WHERE singleton=1", (value,)
                    ).close()
                changed.append(field)
                return actual()

            with monkeypatch.context() as patch:
                patch.setattr(source, "verify_current", current)
                with pytest.raises(StaleCorpusWorkspace, match="checkpoint changed"):
                    getattr(workspace, operation)(*args, **kwargs)
            assert len(changed) == 2
            assert not workspace._db.in_transaction
            assert workspace._load() == before
