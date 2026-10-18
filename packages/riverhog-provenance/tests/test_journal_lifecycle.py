from __future__ import annotations

import copy
import hashlib
import sqlite3
from contextlib import closing

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    ProvenanceValidationError,
    append_assertions,
    append_correction,
    assertion,
    assertion_reference,
    create_journal,
    journal_store,
    new_id,
    reference,
    validate_graph,
    validate_journal,
)
from riverhog_provenance.graph import time_ns


def lifecycle_graph(graph, who):
    graph = copy.deepcopy(graph)
    activity = copy.deepcopy(graph["activities"][0])
    activity.update(id=new_id(), assertion_id=new_id(), kind="creation")
    activity.pop("started_at", None)
    activity.pop("ended_at", None)
    graph["activities"].append(activity)
    state = graph["states"][0]["id"]

    def event(kind, at=None):
        fields = {"at": at} if at is not None else {}
        if kind == "usage":
            fields["role"] = "urn:test:usage-role"
        return assertion(
            kind, who, activity_id=activity["id"], state=reference(state, "state"), **fields
        )

    return graph, state, event


@pytest.mark.parametrize(
    "generated,used,ended,message",
    [
        (None, None, None, None),
        ("2020-01-01T00:00:00Z", None, None, None),
        (None, None, "1960-01-01T00:00:00Z", None),
        (None, "1960-01-01T00:00:00Z", None, None),
        ("0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999999Z", None, None),
        (
            "2020-01-01T00:00:00.000000001Z",
            "2020-01-01T00:00:00.000000001Z",
            "2020-01-01T00:00:00.000000001Z",
            None,
        ),
        (
            "2020-01-01T00:00:00.000000002Z",
            "2020-01-01T00:00:00.000000001Z",
            None,
            "before known generation",
        ),
        (
            None,
            "2020-01-01T00:00:00.000000002Z",
            "2020-01-01T00:00:00.000000001Z",
            "after known invalidation",
        ),
        ("2021-01-01T00:00:00Z", None, "2020-01-01T00:00:00Z", "before known generation"),
    ],
)
def test_exact_temporal_inequalities_match_native_graph(
    graph, who, catalog, generated, used, ended, message
):
    graph, state, event = lifecycle_graph(graph, who)
    graph["relations"] = [
        event("generation", generated),
        event("usage", used),
        event("invalidation", ended),
    ]
    if message:
        with pytest.raises(ProvenanceValidationError, match=message):
            validate_graph(graph, catalog=catalog)
        with pytest.raises(ProvenanceValidationError, match=message):
            create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    else:
        validate_graph(graph, catalog=catalog)
        raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(raw, catalog=catalog) as summary:
            assert summary.journal_sha256 == hashlib.sha256(raw).hexdigest()
            assert {row["assertion_id"] for row in summary.graph["relations"]} == {
                row["assertion_id"] for row in graph["relations"]
            }


def test_shared_state_uses_indexed_extrema_and_late_bounds_recheck_prior_usage(
    graph, who, catalog, monkeypatch
):
    graph, state, event = lifecycle_graph(graph, who)
    graph["relations"] = [event("usage", "2020-01-01T00:00:00Z") for _ in range(129)]
    calls = []
    check = journal_store.JournalStore._lifecycle_bounds_hold

    def observed(self, identity, **bounds):
        calls.append(identity)
        return check(self, identity, **bounds)

    monkeypatch.setattr(journal_store.JournalStore, "_lifecycle_bounds_hold", observed)
    raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    calls.clear()
    with validate_journal(raw, catalog=catalog) as summary:
        assert len(summary.graph["relations"]) == 129
    assert calls == [state] * 129
    for kind, at, message in [
        ("generation", "2021-01-01T00:00:00Z", "before known generation"),
        ("invalidation", "2019-01-01T00:00:00Z", "after known invalidation"),
    ]:
        with pytest.raises(ProvenanceValidationError, match=message):
            append_assertions(
                raw, {"relations": [event(kind, at)]}, recorded_by_agent_id=who, catalog=catalog
            )


def test_corrections_remove_withdrawn_extremes_before_late_bounds(graph, who, catalog):
    graph, state, event = lifecycle_graph(graph, who)
    early, remaining, late = [
        event("usage", f"{year}-01-01T00:00:00Z") for year in (2010, 2020, 2030)
    ]
    graph["relations"] = [early, remaining, late]
    raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    with validate_journal(raw, catalog=catalog) as summary:
        targets = [assertion_reference(summary, row["assertion_id"]) for row in (early, late)]
    corrected = append_correction(
        raw, targets, reason="withdraw dated usages", recorded_by_agent_id=who, catalog=catalog
    )
    bounded = append_assertions(
        corrected,
        {
            "relations": [
                event("generation", "2015-01-01T00:00:00Z"),
                event("invalidation", "2025-01-01T00:00:00Z"),
            ]
        },
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    with validate_journal(bounded, catalog=catalog) as summary:
        assert {early["assertion_id"], late["assertion_id"]} <= summary.retracted_assertion_ids
        assert len(summary.graph["relations"]) == 3
        assert bounded.startswith(raw)
        assert summary.journal_sha256 == hashlib.sha256(bounded).hexdigest()


def test_multiple_violations_retain_native_diagnostic_scan_order(graph, who, catalog, monkeypatch):
    graph, state, event = lifecycle_graph(graph, who)
    graph["relations"] = [
        event("usage", "2030-01-01T00:00:00Z"),
        event("generation", "2022-01-01T00:00:00Z"),
        event("invalidation", "2020-01-01T00:00:00Z"),
    ]
    with pytest.raises(ProvenanceValidationError) as actual:
        create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    monkeypatch.setattr(journal_store.JournalStore, "_lifecycle_bounds_hold", lambda *a, **k: False)
    with pytest.raises(ProvenanceValidationError) as native_scan:
        create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    assert str(actual.value) == str(native_scan.value)


@pytest.mark.parametrize(
    "fault,message",
    [
        ("duplicate", "conflicting known generation"),
        ("activity", "event precedes activity start"),
        ("calendar", "invalid calendar instant"),
    ],
)
def test_native_uniqueness_activity_and_calendar_checks_precede_extrema(
    graph, who, catalog, fault, message
):
    graph, state, event = lifecycle_graph(graph, who)
    if fault == "duplicate":
        graph["relations"] = [
            event("generation", "2020-01-01T00:00:00Z"),
            event("generation", "2021-01-01T00:00:00Z"),
        ]
    elif fault == "activity":
        graph["activities"][-1]["started_at"] = "2021-01-01T00:00:00Z"
        graph["relations"] = [event("usage", "2020-01-01T00:00:00Z")]
    else:
        graph["relations"] = [event("usage", "2020-02-30T00:00:00Z")]
    with pytest.raises(ProvenanceValidationError, match=message):
        create_journal(graph, recorded_by_agent_id=who, catalog=catalog)


class ObservedCursor(sqlite3.Cursor):
    def fetchone(self):
        row = super().fetchone()
        if self.recording and row is not None:
            self.counts["returned_rows"] += 1
            self.counts["returned_time_octets"] += sum(
                len(v.encode()) for v in row if v is not None
            )
        return row

    def close(self):
        self.closed = True
        return super().close()


class ObservedConnection(sqlite3.Connection):
    def execute(self, sql, parameters=()):
        cursor = self.cursor(factory=ObservedCursor)
        cursor.recording = getattr(self, "recording", False)
        cursor.closed = False
        if cursor.recording:
            cursor.counts = self.counts
            self.statements.append((sql, parameters, cursor))
        return cursor.execute(sql, parameters)


@pytest.mark.growth_witness(
    "riverhog-lifecycle-extrema-check-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_store.JournalStore._lifecycle_bounds_hold",
)
def test_actual_indexed_extrema_work(monkeypatch, record_property):
    connect = journal_store.sqlite3.connect

    def observed_connect(*args, **kwargs):
        return connect(*args, factory=ObservedConnection, **kwargs)

    monkeypatch.setattr(journal_store.sqlite3, "connect", observed_connect)
    binding = growth_binding(journal_store.JournalStore._lifecycle_bounds_hold)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    low, high = "0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999999Z"
    for count, missing, generated, ended, expected in [
        (0, False, low, high, True),
        (1, False, low, high, True),
        (17, False, low, high, True),
        (129, False, low, high, True),
        (1025, False, low, high, True),
        (1025, True, low, high, True),
        (17, False, high, None, False),
        (17, False, None, low, False),
    ]:
        current = journal_store.JournalStore()
        try:
            keys = [f"urn:uuid:00000000-0000-4000-8000-{n:012x}" for n in range(count)]
            current.db.executemany(
                "INSERT INTO lifecycle_extrema VALUES (?, ?, ?)",
                ((key, str(time_ns(low)), str(time_ns(high))) for key in keys),
            )
            db = current.db
            db.counts = {"returned_rows": 0, "returned_time_octets": 0}
            db.statements = []
            instructions = [0]

            def progress(counter=instructions):
                counter[0] += 1
                return 0

            def authorize(action, table, column, database, trigger):
                return (
                    sqlite3.SQLITE_DENY
                    if action == sqlite3.SQLITE_READ and table == "assertions"
                    else sqlite3.SQLITE_OK
                )

            db.set_progress_handler(progress, 1)
            db.set_authorizer(authorize)
            db.recording = True
            try:
                target = keys[-1] if keys and not missing else new_id()
                assert (
                    current._lifecycle_bounds_hold(
                        target, generated_at=generated, invalidated_at=ended
                    )
                    is expected
                )
            finally:
                db.recording = False
                db.set_authorizer(None)
                db.set_progress_handler(None, 0)
            assert len(db.statements) == 1 and db.statements[0][2].closed
            assert db.counts["returned_rows"] == int(bool(keys) and not missing)
            sql, parameters, _ = db.statements[0]
            with closing(db.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as rows:
                plan = " ".join(row[3] for row in rows)
            assert "SEARCH lifecycle_extrema USING INDEX" in plan
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {"indexed_states": count},
                "metrics": {
                    "sql_statements": len(db.statements),
                    **db.counts,
                    "sqlite_vm_instructions": instructions[0],
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
        finally:
            current.close()
    record_property("growth_samples", samples)
