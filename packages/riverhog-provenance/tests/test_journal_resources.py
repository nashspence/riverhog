from __future__ import annotations

import hashlib
import sqlite3
from io import BytesIO
from pathlib import Path

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor, MemberHistoryStore
from riverhog_provenance import (
    MemberHistoryClosure,
    ProvenanceValidationError,
    append_assertions,
    append_checkpoint,
    assertion,
    create_journal,
    journal_store,
    reference,
    validate_journal,
    validate_journal_set,
)
from riverhog_provenance import journal as owner


class ObservedCursor(sqlite3.Cursor):
    closed = False

    def close(self):
        self.closed = True
        return super().close()


class ObservedConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cursors = []

    def execute(self, sql, parameters=()):
        cursor = self.cursor(factory=ObservedCursor)
        self.cursors.append((sql, cursor))
        return cursor.execute(sql, parameters)


@pytest.fixture
def storage(journal, monkeypatch, tmp_path):
    directories = []
    connections = []
    connect = journal_store.sqlite3.connect
    mkdtemp = journal_store.mkdtemp

    def directory(**kwargs):
        path = Path(mkdtemp(dir=tmp_path, **kwargs))
        directories.append(path)
        return str(path)

    def connection(path, **kwargs):
        if str(path).endswith("snapshot.sqlite3"):
            actual = connect(path, factory=ObservedConnection, **kwargs)
            connections.append(actual)
            return actual
        return connect(path, **kwargs)

    monkeypatch.setattr(journal_store, "mkdtemp", directory)
    monkeypatch.setattr(journal_store.sqlite3, "connect", connection)
    return directories, connections


def test_independent_leases_preserve_exact_snapshot_until_last_close(journal, catalog, storage):
    directories, connections = storage
    summary = validate_journal(journal, catalog=catalog)
    first = summary.borrow()
    second = first.borrow()
    expected = summary.materialize()
    summary.close()
    summary.close()
    with pytest.raises(RuntimeError, match="closed"):
        summary.borrow()
    assert first.materialize() == second.materialize() == expected
    first.close()
    assert second.journal_sha256 == hashlib.sha256(journal).hexdigest()
    assert second.anchor == expected["journal"] and directories[0].exists()
    second.close()
    assert not directories[0].exists()
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")


@pytest.mark.parametrize(
    "view", ["frames", "slice", "objects", "graph", "origins", "retired", "findings"]
)
def test_closed_views_fail_while_another_owner_is_live(journal, catalog, storage, view):
    summary = validate_journal(journal, catalog=catalog)
    selected = {
        "frames": summary.frames,
        "slice": summary.frames[:1],
        "objects": summary.graph_validation.objects,
        "graph": summary.graph_validation.view,
        "origins": summary.assertion_entries,
        "retired": summary.retracted_assertion_ids,
        "findings": summary.findings,
    }[view]
    with summary.borrow() as borrower:
        summary.close()
        with pytest.raises(RuntimeError, match="closed"):
            len(selected)
        assert borrower.tail.reference == borrower.anchor["through"]
    assert not any(path.exists() for path in storage[0])


@pytest.mark.parametrize("view", ["frames", "objects", "origins", "graph"])
def test_early_iterator_close_releases_cursor_without_closing_owner(
    journal, catalog, storage, view
):
    with validate_journal(journal, catalog=catalog) as summary:
        connection = storage[1][0]
        values = {
            "frames": summary.frames,
            "objects": summary.graph_validation.objects,
            "origins": summary.assertion_entries,
            "graph": summary.graph_validation.view,
        }[view]
        before = len(connection.cursors)
        iterator = iter(values)
        next(iterator)
        opened = connection.cursors[before:]
        assert opened and not opened[0][1].closed
        iterator.close()
        assert all(cursor.closed for _, cursor in opened)
        assert summary.journal_sha256 == hashlib.sha256(journal).hexdigest()
        assert summary.tail.reference == summary.anchor["through"]
    assert not storage[0][0].exists()


def test_context_failure_and_native_validation_failure_release_storage(journal, catalog, storage):
    with pytest.raises(LookupError):
        with validate_journal(journal, catalog=catalog):
            raise LookupError("caller stopped")
    with pytest.raises(ProvenanceValidationError):
        validate_journal(journal + b"bad tail", catalog=catalog)
    assert len(storage[0]) == 2 and not any(path.exists() for path in storage[0])


def test_native_append_helpers_release_projections_on_success_and_error(
    journal, who, catalog, storage
):
    appended = append_checkpoint(
        journal, recorded_by_agent_id=who, purpose="urn:test:resource-scope", catalog=catalog
    )
    checkpointed = append_checkpoint(
        appended, recorded_by_agent_id=who, purpose="urn:test:resource-scope", catalog=catalog
    )
    with validate_journal(checkpointed, catalog=catalog) as summary:
        assert summary.journal_sha256 == hashlib.sha256(checkpointed).hexdigest()
        assert len(summary.frames) == 3
    with pytest.raises(ValueError, match="schema validation"):
        append_assertions(journal, {"bad": []}, recorded_by_agent_id=who, catalog=catalog)
    assert not any(path.exists() for path in storage[0])


def test_batch_generator_does_not_hold_a_snapshot_across_yield(journal, who, catalog, storage):
    with validate_journal(journal, catalog=catalog) as summary:
        artifact_id = summary.graph["artifacts"][0]["id"]
    row = assertion(
        "extension",
        who,
        subject=reference(artifact_id, "artifact"),
        property="urn:test:resource",
        value={"type": "text", "value": "batch"},
    )
    iterator = owner._assertion_batch_entries(
        journal,
        ({"extensions": [row]},),
        recorded_by_agent_id=who,
        catalog=catalog,
        recorded_at=None,
        entry_ids=None,
    )
    assert next(iterator) == journal
    assert not any(path.exists() for path in storage[0])
    iterator.close()


def test_set_failure_closes_previously_validated_journals(journal, catalog, storage):
    with pytest.raises(ProvenanceValidationError):
        validate_journal_set((journal, b"bad"), catalog=catalog)
    assert len(storage[0]) == 2 and not any(path.exists() for path in storage[0])


def test_set_owns_only_longest_exact_prefix_and_closes_all(journal, who, catalog, storage):
    appended = append_checkpoint(
        journal, recorded_by_agent_id=who, purpose="urn:test:resource-scope", catalog=catalog
    )
    with validate_journal_set((journal, appended, journal), catalog=catalog) as result:
        assert len(result.journals) == 1 and len(result.journals[0].frames) == 2
        assert sum(path.exists() for path in storage[0]) == 1
    assert not any(path.exists() for path in storage[0])


def test_history_cache_eviction_and_owner_exit_preserve_live_borrow(graph, who, catalog, storage):
    sources = {}
    anchors = []
    for _ in range(33):
        raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(raw, catalog=catalog) as summary:
            anchors.append(HistoryJournalAnchor.from_mapping(summary.anchor))
            sources[summary.journal_id] = raw
    with MemberHistoryClosure(
        MemberHistoryStore(lambda path: ()),
        lambda identity, end: (sources[identity][:end],),
        member_role="urn:test:member",
        catalog=catalog,
    ) as closure:
        first = closure.summary_at(anchors[0])
        for anchor in anchors[1:]:
            with closure.summary_at(anchor) as current:
                assert current.anchor == anchor.to_mapping()
        assert len(closure._summaries) == 32
        assert first.anchor == anchors[0].to_mapping()
        assert sum(path.exists() for path in storage[0]) == 33
    assert first.anchor == anchors[0].to_mapping()
    assert sum(path.exists() for path in storage[0]) == 1
    first.close()
    assert not any(path.exists() for path in storage[0])


def test_spool_rewind_failure_releases_the_validated_result(journal, who, catalog, storage):
    class Spool(BytesIO):
        def seek(self, offset, whence=0):
            if len(storage[0]) > 1 and offset == 0 and whence == 0:
                raise OSError("final rewind failed")
            return super().seek(offset, whence)

    with pytest.raises(OSError, match="final rewind"):
        owner.write_assertion_batches(
            Spool(),
            journal,
            (),
            recorded_by_agent_id=who,
            catalog=catalog,
        )
    assert not any(path.exists() for path in storage[0])


def test_parallel_borrowed_readers_preserve_exact_validated_snapshot(
    journal, catalog, parallel_snapshot_reads
):
    with validate_journal(journal, catalog=catalog) as summary:
        parallel_snapshot_reads(summary)
