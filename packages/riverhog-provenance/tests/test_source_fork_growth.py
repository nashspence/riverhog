from __future__ import annotations

import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import replace

import pytest
import riverhog_canonical_json as codec
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import create_journal, validate_journal
from riverhog_provenance import journal as owner
from riverhog_provenance.journal_store import InvalidJournalWorkspace
from test_journal_result import completed, history
from test_source_anchor_growth import octets, record


def forked(journal, graph, who, catalog, *, policy_uri="urn:test:fork"):
    with validate_journal(history(journal, 17, catalog), catalog=catalog) as parent:
        anchor = parent.anchor_at(0)
        assert anchor != parent.anchor
    raw = create_journal(
        graph,
        recorded_by_agent_id=who,
        catalog=catalog,
        forked_from=anchor,
        policy_uri=policy_uri,
    )
    return raw, anchor


def test_fork_preserves_the_recorded_original_prefix_and_detaches_results(
    journal, graph, who, catalog
):
    raw, anchor = forked(journal, graph, who, catalog)
    with validate_journal(history(raw, 17, catalog), catalog=catalog) as current:
        assert current.fork_anchor() == anchor
        detached = current.fork_anchor()
        detached["through"]["entry_id"] = "changed caller view"
        assert current.fork_anchor() == anchor
        assert current.fork_anchor() != current.anchor


def test_no_fork_is_explicit_and_requires_an_open_owned_projection(journal, catalog):
    with validate_journal(journal, catalog=catalog) as current:
        assert current.fork_anchor() is None
        with pytest.raises(RuntimeError, match="owned"):
            replace(current).fork_anchor()
        independent = current.borrow()
    with independent:
        assert independent.fork_anchor() is None
    with pytest.raises(RuntimeError, match="closed"):
        independent.fork_anchor()


def test_fork_metadata_retained_reopen_uses_the_same_original_atom(
    tmp_path, journal, graph, who, catalog
):
    from riverhog_provenance import open_retained_journal_result

    raw, anchor = forked(journal, graph, who, catalog)
    path, source, confirmed, _, _ = completed(tmp_path, raw, catalog)
    with open_retained_journal_result(path, source, confirmed=confirmed, catalog=catalog) as first:
        independent = first.borrow()
        assert first.fork_anchor() == anchor
    with independent:
        assert independent.fork_anchor() == anchor
    with open_retained_journal_result(
        path, source, confirmed=confirmed, catalog=catalog
    ) as reopened:
        assert reopened.fork_anchor() == anchor


def test_missing_initial_atom_fails_closed(journal, catalog, monkeypatch):
    with validate_journal(journal, catalog=catalog) as current:
        actual = current._store.db

        class MissingInitial:
            def execute(self, query, parameters):
                assert query == "SELECT value FROM frames WHERE sequence = ?"
                assert parameters == ("0",)
                return actual.execute(query, ("missing",))

        with monkeypatch.context() as patch:
            patch.setattr(current._store._storage, "connection", MissingInitial())
            with pytest.raises(InvalidJournalWorkspace, match="initialization is missing"):
                current.fork_anchor()


class Cursor:
    def __init__(self, actual, recorder):
        self.actual, self.recorder, self.closed = actual, recorder, False

    def fetchone(self):
        row = self.actual.fetchone()
        if row is not None:
            self.recorder.metrics["returned_rows"] += 1
            self.recorder.metrics["source_read_octets"] += sum(map(octets, row))
        return row

    def close(self):
        self.actual.close()
        self.closed = True
        self.recorder.metrics["closed_cursors"] += 1


class Observer:
    def __init__(self, actual):
        self.actual, self.metrics, self.cursors = actual, defaultdict(int), []

    def execute(self, query, parameters=()):
        assert query == "SELECT value FROM frames WHERE sequence = ?"
        assert parameters == ("0",)
        self.metrics["sql_statements"] += 1
        self.metrics["bound_parameter_octets"] += sum(map(octets, parameters))
        cursor = Cursor(self.actual.execute(query, parameters), self)
        self.cursors.append(cursor)
        return cursor

    @contextmanager
    def observe(self, current, monkeypatch):
        parse, encode = codec.parse_identity_json, codec.canonical_json_bytes

        def decoded(raw):
            assert all(cursor.closed for cursor in self.cursors)
            self.metrics["json_decodes"] += 1
            self.metrics["decoded_octets"] += len(raw)
            return parse(raw)

        def encoded(value):
            self.metrics["json_encodes"] += 1
            raw = encode(value)
            self.metrics["encoded_octets"] += len(raw)
            return raw

        def stepped():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def forbidden(*args, **kwargs):
            pytest.fail("fork extraction traversed or replayed journal source")

        self.actual.set_progress_handler(stepped, 1)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(current._store._storage, "connection", self)
                patch.setattr(codec, "parse_identity_json", decoded)
                patch.setattr(codec, "canonical_json_bytes", encoded)
                patch.setattr(owner.JournalSummary, "anchor_at", forbidden)
                patch.setattr(owner, "iter_journal_frames", forbidden)
                patch.setattr(owner, "validate_journal_chunks", forbidden)
                patch.setattr(sqlite3, "connect", forbidden)
                yield
        finally:
            self.actual.set_progress_handler(None, 0)
            assert all(cursor.closed for cursor in self.cursors)
            assert not self.actual.in_transaction


def measure(current, monkeypatch):
    size = current._store.db.execute(
        "SELECT length(value) FROM frames WHERE sequence='0'"
    ).fetchone()[0]
    dimensions = {"retained_frames": len(current.frames), "initial_octets": size}
    observer = Observer(current._store.db)
    with observer.observe(current, monkeypatch):
        result = current.fork_anchor()
    return result, record(
        growth_binding(owner.JournalSummary.fork_anchor), dimensions, observer.metrics
    )


@pytest.mark.growth_witness(
    "native-frozen-source-fork-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal.JournalSummary.fork_anchor",
)
def test_actual_fork_selection_charges_one_initial_atom_across_source_regimes(
    journal, graph, who, catalog, monkeypatch, record_property
):
    records = []
    raw, anchor = forked(journal, graph, who, catalog)
    for count in (1, 17, 129, 1025):
        for source, expected in ((journal, None), (raw, anchor)):
            with validate_journal(history(source, count, catalog), catalog=catalog) as current:
                for _ in range(2):
                    result, sample = measure(current, monkeypatch)
                    assert result == expected
                    records.append(sample)
    for padding in (1, 4097, 65537):
        wide, expected = forked(
            journal, graph, who, catalog, policy_uri="urn:test:fork:" + "x" * padding
        )
        with validate_journal(wide, catalog=catalog) as current:
            result, sample = measure(current, monkeypatch)
            assert result == expected
            records.append(sample)
    record_property("growth_samples", records)
