from __future__ import annotations

import copy
import sqlite3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from threading import Barrier

import pytest
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import (
    ProvenanceValidationError,
    append_assertion_batches,
    append_correction,
    assertion,
    assertion_reference,
    external_reference,
    new_id,
    reference,
    validate_journal,
)
from riverhog_provenance import journal as owner
from test_journal_result import completed, history
from test_source_anchor_growth import Cursor, octets, record
from test_source_anchor_growth import measure as measure_anchor


def selected(current):
    return external_reference(current, current.states[0]["id"])


def test_exact_documentary_dependency_anchor(journal, catalog):
    with validate_journal(journal, catalog=catalog) as current:
        dependency = selected(current)
        assert current.reference_anchor(dependency) == current.anchor
        dependency["entry"]["entry_id"] = new_id()
        with pytest.raises(ProvenanceValidationError, match="exact assertion"):
            current.reference_anchor(dependency)


@pytest.mark.parametrize(
    "field",
    [
        "journal_id",
        "entry_id",
        "sequence",
        "json_sha256",
        "assertion_id",
        "object_id",
        "object_type",
    ],
)
def test_dependency_anchor_rejects_every_changed_coordinate(journal, catalog, field):
    with validate_journal(journal, catalog=catalog) as current:
        dependency = copy.deepcopy(selected(current))
        if field in {"entry_id", "sequence", "json_sha256"}:
            dependency["entry"][field] = {
                "entry_id": new_id(),
                "sequence": "1",
                "json_sha256": "0" * 64,
            }[field]
        else:
            dependency[field] = "artifact" if field == "object_type" else new_id()
        with pytest.raises(ProvenanceValidationError):
            current.reference_anchor(dependency)


def test_documentary_dependency_retains_retracted_original_without_effective_attribution(
    journal, who, catalog
):
    with validate_journal(journal, catalog=catalog) as initial:
        artifact = initial.graph["artifacts"][0]["id"]
    fact = assertion(
        "extension",
        who,
        subject=reference(artifact, "artifact"),
        property="urn:test:documentary-dependency",
        value={"type": "text", "value": "original"},
    )
    before = append_assertion_batches(
        journal, ({"extensions": [fact]},), recorded_by_agent_id=who, catalog=catalog
    )
    with validate_journal(before, catalog=catalog) as original:
        dependency = external_reference(original, fact["id"])
        anchor = original.anchor
        target = assertion_reference(original, fact["assertion_id"])
    raw = append_correction(
        before, (target,), reason="withdrawn", recorded_by_agent_id=who, catalog=catalog
    )
    with validate_journal(raw, catalog=catalog) as current:
        assert fact["assertion_id"] in current.retracted_assertion_ids
        assert current.graph_validation.objects.get(fact["id"]) is None
        assert current.reference_anchor(dependency) == anchor
        assert anchor != current.anchor


def test_dependency_anchor_retained_reopen_and_independent_close(tmp_path, journal, catalog):
    from riverhog_provenance import open_retained_journal_result

    path, source, confirmed, certificates, expected = completed(tmp_path, journal, catalog)
    with open_retained_journal_result(path, source, confirmed=confirmed, catalog=catalog) as first:
        dependency = selected(first)
        independent = first.borrow()
        anchor = first.reference_anchor(dependency)
    with independent:
        assert independent.reference_anchor(dependency) == anchor
    with pytest.raises(RuntimeError, match="closed"):
        independent.reference_anchor(dependency)


def test_reference_headers_match_authenticated_dependency_resolution(journal, catalog):
    from riverhog_archive_contracts import MemberHistoryStore
    from riverhog_provenance import MemberHistoryClosure

    reads = octets_read = 0

    def authenticated_read(_journal_id, _end):
        nonlocal reads, octets_read
        reads += 1
        octets_read += len(journal)
        yield journal

    with (
        validate_journal(journal, catalog=catalog) as current,
        MemberHistoryClosure(
            MemberHistoryStore(lambda _path: ()),
            authenticated_read,
            member_role="member",
            catalog=catalog,
        ) as closure,
    ):
        dependencies = [external_reference(current, row["id"]) for row in current.states]
        for dependency in dependencies * 2:
            assert (
                current.reference_anchor(dependency)
                == closure._reference_anchor(dependency).to_mapping()
            )
        assert reads == 2 * len(dependencies)
        assert octets_read == reads * len(journal)


def test_reference_headers_share_exact_parallel_reader_leases(journal, catalog):
    with validate_journal(journal, catalog=catalog) as current:
        dependency = selected(current)
        expected = current.reference_anchor(dependency)
        barrier = Barrier(8)

        def read(_):
            with current.borrow() as reader:
                barrier.wait(timeout=30)
                for _ in range(8):
                    assert reader.reference_anchor(dependency) == expected

        with ThreadPoolExecutor(max_workers=8) as readers:
            list(readers.map(read, range(8)))
        with pytest.raises(RuntimeError, match="owned"):
            replace(current).reference_anchor(dependency)


class Observer:
    def __init__(self, actual):
        self.actual = actual
        self.metrics = defaultdict(int)
        self.cursors = []

    def execute(self, query, parameters=()):
        self.metrics["sql_statements"] += 1
        self.metrics["bound_parameter_octets"] += sum(map(octets, parameters))
        cursor = Cursor(self.actual.execute(query, parameters), self)
        self.cursors.append(cursor)
        return cursor

    @contextmanager
    def observe(self, current, monkeypatch):
        def stepped():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def authorized(action, table, column, _database, _trigger):
            allowed = {
                "assertions": {"assertion_id", "sequence", "object_id", "type"},
                "frames": {"sequence", "reference", "prefix_state"},
            }
            if action == sqlite3.SQLITE_READ and column not in allowed.get(table, set()):
                self.metrics["source_read_octets"] += 1
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.actual.set_authorizer(authorized)
        self.actual.set_progress_handler(stepped, 1)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(current._store._storage, "connection", self)
                yield
        finally:
            self.actual.set_authorizer(None)
            self.actual.set_progress_handler(None, 0)
            assert all(cursor.closed for cursor in self.cursors)
            assert not self.actual.in_transaction


def measure(current, dependency, monkeypatch):
    actual = current._store.db
    row = actual.execute(
        "SELECT sequence,object_id,type FROM assertions WHERE assertion_id=?",
        (dependency["assertion_id"],),
    ).fetchone()
    dimensions = {
        "indexed_assertions": actual.execute("SELECT count(*) FROM assertions").fetchone()[0],
        "assertion_key_octets": octets(dependency["assertion_id"]),
        "metadata_octets": sum(map(octets, row)),
    }
    observer, children = Observer(actual), []

    def anchor(summary, sequence):
        assert all(cursor.closed for cursor in observer.cursors)
        observer.metrics["native_anchor_calls"] += 1
        summary._store._storage.connection = actual
        try:
            result, child = measure_anchor(summary, sequence, monkeypatch)
            children.append(child)
            return result
        finally:
            summary._store._storage.connection = observer

    with observer.observe(current, monkeypatch):
        with monkeypatch.context() as patch:
            patch.setattr(owner.JournalSummary, "anchor_at", anchor)
            result = current.reference_anchor(dependency)
    parent = record(
        growth_binding(owner.JournalSummary.reference_anchor),
        dimensions,
        observer.metrics,
        {"anchor": children},
    )
    return result, parent


@pytest.mark.growth_witness(
    "native-frozen-reference-header-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal.JournalSummary.reference_anchor",
)
def test_reference_headers_visit_exact_metadata_without_prefix_or_assertion_replay(
    journal, who, catalog, monkeypatch, record_property
):
    records = []
    for count in (1, 17, 129):
        with validate_journal(journal, catalog=catalog) as initial:
            artifact = initial.graph["artifacts"][0]["id"]
        rows = [
            assertion(
                "extension",
                who,
                subject=reference(artifact, "artifact"),
                property="urn:test:shared-dependency",
                value={"type": "text", "value": "x" * 128},
            )
            for _ in range(count)
        ]
        raw = append_assertion_batches(
            journal, ({"extensions": rows},), recorded_by_agent_id=who, catalog=catalog
        )
        with validate_journal(raw, catalog=catalog) as current:
            expected = current.anchor
            for row in rows:
                dependency = external_reference(current, row["id"])
                for _ in range(2):
                    result, sample = measure(current, dependency, monkeypatch)
                    assert result == expected
                    records.append(sample)
                    result["through"]["entry_id"] = new_id()
                    assert current.reference_anchor(dependency) == expected
    for count in (17, 129, 1025):
        raw = history(journal, count, catalog)
        with validate_journal(raw, catalog=catalog) as current:
            dependency = selected(current)
            result, sample = measure(current, dependency, monkeypatch)
            assert result == current.anchor_at(0)
            records.append(sample)
    record_property("growth_samples", records)
