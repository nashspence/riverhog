from __future__ import annotations

import hashlib
import inspect
import sqlite3
import sys
from contextlib import closing, contextmanager
from copy import deepcopy
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import _entry_replay as replay_owner
from riverhog_provenance import assertion, encode_entry, parse_journal, validate_journal
from riverhog_provenance._changed_assertions import ChangedAssertionCapacity, read_changed_assertion
from riverhog_provenance._entry_replay import EntryReplay, ReplayBudget
from riverhog_provenance._observation_coverage import ObservationCoverage
from riverhog_provenance._semantic_program import Nodes
from riverhog_provenance.journal_store import JournalStore
from test_entry_replay import owned_summary
from test_entry_replay_growth import REGIMES, ReplayConnection, finish_entry, observe_replay, owner
from test_entry_replay_growth import admission_histories as admission_histories
from test_entry_replay_publication_growth import restore
from test_generic_semantics import code_tree
from test_semantic_edge import WorkCursor, observed_work
from test_semantic_owner import snapshot

ROW_CASES = (
    *((regime, "extensions", 0) for regime in REGIMES if regime[3] != "checkpoint"),
    (REGIMES[0], "descriptions", 0),
    *((REGIMES[0], "contexts", count) for count in (1, 17, 129, 1025)),
)
_KEY = "SELECT assertion_id FROM changes WHERE assertion_id > ? ORDER BY assertion_id LIMIT 1"
_LENGTH = "SELECT length(value) FROM assertions WHERE assertion_id = ? AND active = 1"
_PAYLOAD = "SELECT value FROM assertions WHERE assertion_id = ? AND active = 1"


class RowCursor(WorkCursor):
    def record(self, row):
        result = super().record(row)
        if row is not None:
            if self.sql == _KEY:
                self.connection.selected_identity = row[0]
            elif self.sql == _LENGTH:
                assert self.parameters == (self.connection.selected_identity,)
                self.connection.selected_length = row[0]
            elif self.sql == _PAYLOAD:
                assert type(row[0]) is bytes
                size = len(row[0])
                self.connection.metrics["returned_metadata_octets"] -= size
                self.connection.metrics["returned_atom_octets"] += size
        return result


class RowConnection(ReplayConnection):
    selected_identity = None
    selected_length = None
    byte_budget = None
    active_sql = None
    active_parameters = ()

    def execute(self, sql, parameters=()):
        self.active_sql, self.active_parameters = sql, parameters
        if self.recording and sql in {_KEY, _LENGTH, _PAYLOAD}:
            if sql == _PAYLOAD:
                assert parameters == (self.selected_identity,)
                assert self.selected_length is not None
                assert self.byte_budget is not None and self.selected_length <= self.byte_budget
            cursor = self.cursor(factory=RowCursor)
            cursor.sql, cursor.parameters = sql, parameters
            self.statements.append((sql, parameters, cursor))
            return cursor.execute(sql, parameters)
        return super().execute(sql, parameters)

    @contextmanager
    def observe(self):
        with super().observe() as metrics:
            metrics.update(returned_atom_octets=0, historical_payload_octets=0)
            yield metrics

    def set_authorizer(self, callback):
        if callback is None:
            return sqlite3.Connection.set_authorizer(self, None)

        def authorize(action, table, column, database, trigger):
            if action == sqlite3.SQLITE_READ and column == "value":
                if table == "journal_validation_frame":
                    return sqlite3.SQLITE_DENY
                if table == "assertions":
                    if self.active_sql not in {_LENGTH, _PAYLOAD}:
                        return sqlite3.SQLITE_DENY
                    if self.active_parameters != (self.selected_identity,):
                        return sqlite3.SQLITE_DENY
                    if self.active_sql == _PAYLOAD and (
                        self.selected_length is None
                        or self.byte_budget is None
                        or self.selected_length > self.byte_budget
                    ):
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK
            return callback(action, table, column, database, trigger)

        return sqlite3.Connection.set_authorizer(self, authorize)


def node_inputs(value):
    pending, items = [value], 0
    while pending:
        current = pending.pop()
        items += 1
        if isinstance(current, dict):
            pending.extend(current.values())
        elif isinstance(current, list):
            pending.extend(current)
    observation = value.get("type") == "observation"
    rows = value.get("coverage", []) if observation else []
    return {
        "atom_input_items": items,
        "coverage_calls": int(observation),
        "coverage_rows": len(rows),
        "coverage_key_octets": sum(
            len(row["profile_id"].encode()) + len(row["category"].encode()) for row in rows
        ),
    }


@contextmanager
def row_fixture(build, specification, catalog, *, cold):
    regime, category, metadata_count = specification
    case = dict(build(regime))
    candidate = case["candidate"]
    if metadata_count:
        document = deepcopy(candidate.document)
        row = assertion(
            "context",
            case["who"],
            kind="opaque",
            metadata=[
                {
                    "profile_id": "urn:test:profile",
                    "category": "urn:test:category",
                    "name": {"kind": "text", "text": f"field-{n}"},
                    "source": {
                        "interface": "urn:test:interface",
                        "field": {"kind": "text", "text": "field"},
                    },
                    "status": "captured",
                    "sensitivity": "public",
                    "value": {"type": "text", "value": "current metadata"},
                }
                for n in range(metadata_count)
            ],
        )
        document["body"]["assertions"].setdefault("contexts", []).append(row)
        selected = row["assertion_id"]
        candidate = parse_journal(encode_entry(document, catalog=catalog))[0]
        case["raw"] = case["raw"][: -len(case["candidate"].encoded)] + candidate.encoded
        case["candidate"] = candidate
    else:
        selected = candidate.document["body"]["assertions"][category][0]["assertion_id"]
    with closing(JournalStore()) as store:
        current = restore(store, case, catalog)
        current.admit(candidate)
        for _ in range(100000):
            before = current.ledger.load()
            if (
                before.checkpoint.phase == "generic-admission"
                and before.checkpoint.row_key == selected
            ):
                break
            current.advance(ReplayBudget(rows=64))
        else:
            pytest.fail("the finite native row fixture did not reach its selected admission")
        cached = current._current
        row = cached.row
        with closing(
            store.db.execute("SELECT value FROM assertions WHERE assertion_id=?", (selected,))
        ) as cursor:
            raw = cursor.fetchone()[0]
        assert raw == canonical_json_bytes(row) == cached.atom.encoded
        original = snapshot(store.db)
        dimensions = {
            "indexed_assertions": len(original["assertions"]),
            "changed_identities": len(original["changes"]),
            "atom_octets": len(raw),
            "cold_row": int(cold),
            **node_inputs(row),
        }
        case.update(
            before=before,
            cached=cached,
            row=row,
            row_raw=raw,
            original=original,
            dimensions=dimensions,
        )
        path = store._storage.directory / "snapshot.sqlite3"
        store.db.close()
        store._storage.connection = sqlite3.connect(path, factory=RowConnection)
        store.db.execute("PRAGMA synchronous=FULL").close()
        store.db.execute("PRAGMA cache_size=-64").close()
        current = owner(store, catalog, case["source_binding"], case["epoch"])
        current._current = replace(cached, child_kind=None, child_id=None, child=None)
        if cold:
            current._current.row_key, current._current.row_sha256 = None, None
            current._current.row, current._current.atom = None, None
        current._start(before, ReplayBudget())
        yield current, case


@contextmanager
def row_observer(current, case):
    db = current.db
    node_operation, coverage_operation = Nodes.__init__, ObservationCoverage.__init__
    allowed = {
        node_operation: code_tree(inspect.unwrap(node_operation).__code__)
        | code_tree(inspect.unwrap(coverage_operation).__code__),
        coverage_operation: code_tree(inspect.unwrap(coverage_operation).__code__),
    }
    active = []

    def trace(frame, event, _argument):
        if event == "call":
            if not any(frame.f_code in tracker["allowed"] for tracker in active):
                return None
            frame.f_trace_opcodes = True
            return trace
        if event == "opcode":
            for tracker in active:
                if frame.f_code in tracker["allowed"]:
                    tracker["events"] += 1
        return trace

    @contextmanager
    def kernel(operation, args):
        if operation not in allowed:
            yield None
            return
        tracker = {"allowed": allowed[operation], "events": 0}
        outer = not active
        previous = sys.gettrace()
        observed_frame = sys._getframe()
        previous_opcodes = observed_frame.f_trace_opcodes
        if outer:
            observed_frame.f_trace_opcodes = True
            sys.settrace(trace)
        active.append(tracker)

        def complete(_result):
            assert tracker["events"] > 0
            if operation is node_operation:
                return {
                    "indexed_containers": len(args[0].values),
                    "python_opcode_events": tracker["events"],
                }
            return {
                "indexed_rows": len(args[0].statuses),
                "retained_key_octets": sum(
                    len(profile.encode()) + len(category.encode())
                    for profile, category in args[0].statuses
                ),
                "python_opcode_events": tracker["events"],
            }

        try:
            yield complete
        finally:
            assert active.pop() is tracker
            if outer:
                sys.settrace(previous)
                observed_frame.f_trace_opcodes = previous_opcodes

    def inputs(parent, operation, args, kwargs):
        if operation is read_changed_assertion:
            assert args[0] is db
            assert kwargs["after"] == case["before"].checkpoint.row_after
            db.byte_budget = kwargs["byte_budget"]
            return {
                item.name: parent[item.name]
                for item in growth_binding(operation).declaration.dimensions
            }
        if operation is node_operation:
            return {
                item.name: parent["atom_input_items" if item.name == "input_items" else item.name]
                for item in growth_binding(operation).declaration.dimensions
            }
        assert operation is coverage_operation
        return {"input_rows": parent["coverage_rows"], "key_octets": parent["coverage_key_octets"]}

    with observe_replay(
        db,
        current.catalog,
        case["dimensions"],
        root_operation=EntryReplay._row,
        input_dimensions=inputs,
        operation_owners={
            read_changed_assertion: replay_owner,
            node_operation: Nodes,
            coverage_operation: ObservationCoverage,
        },
        kernel_observer=kernel,
    ) as observed:
        yield observed
    assert not active


def row_samples(catalog, build, *, samples, cases=ROW_CASES):
    binding = growth_binding(EntryReplay._row)
    projection = project_bindings((binding,))
    for specification in cases:
        for cold in (False, True):
            with row_fixture(build, specification, catalog, cold=cold) as (current, case):
                with row_observer(current, case) as (observed, complete):
                    with current.db.observe():
                        result = current._row(case["before"].checkpoint, current._current)
                primitive = observed_work(current.db, {key: 0 for key in current.db.metrics}, 0, 0)
                primitive.update(
                    changed_rows=current.db.metrics["changed_rows"],
                    indexed_containers=sum(
                        child["metrics"]["indexed_containers"]
                        for child in observed["delegates"]["index"]
                    ),
                    index_python_opcode_events=sum(
                        child["metrics"]["python_opcode_events"]
                        for child in observed["delegates"]["index"]
                    ),
                )
                sample = complete(observed, primitive)
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
                assert result == case["row"] and result is current._current.row
                assert (result is case["row"]) is (not cold)
                assert current._current.atom.encoded == case["row_raw"]
                assert not current.db.in_transaction and snapshot(current.db) == case["original"]
                finish_entry(current)
                with validate_journal(case["raw"], catalog=catalog) as expected:
                    with owned_summary(current.store, current.prefix.finish()) as actual:
                        assert actual.materialize() == expected.materialize()
                        assert actual.journal_sha256 == hashlib.sha256(case["raw"]).hexdigest()
    assert len(samples) == 2 * len(cases)


@pytest.mark.growth_witness(
    "riverhog-entry-replay-row-restoration-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_replay.EntryReplay._row",
)
def test_actual_cold_and_warm_selected_row_restoration(
    catalog, admission_histories, record_property
):
    samples = []
    record_property("growth_samples", samples)
    row_samples(catalog, admission_histories, samples=samples)


def test_row_capacity_rejects_before_the_selected_atom_is_fetched(catalog, admission_histories):
    with row_fixture(admission_histories, ROW_CASES[1], catalog, cold=True) as (current, case):
        budget = ReplayBudget(octets=current._spent + len(case["row_raw"]) - 1)
        current._start(case["before"], budget)
        with row_observer(current, case):
            with current.db.observe():
                with pytest.raises(ChangedAssertionCapacity):
                    current._row(case["before"].checkpoint, current._current)
        assert current.db.metrics["returned_atom_octets"] == 0 and current._current.row is None
        assert snapshot(current.db) == case["original"]


def test_changed_selected_atom_is_rejected_without_rewriting_state(catalog, admission_histories):
    with row_fixture(admission_histories, ROW_CASES[0], catalog, cold=True) as (current, case):
        raw = case["row_raw"]
        current.db.execute(
            "UPDATE assertions SET value=? WHERE assertion_id=?",
            (bytes([raw[0] ^ 1]) + raw[1:], case["before"].checkpoint.row_key),
        ).close()
        current.db.commit()
        before = snapshot(current.db)
        with row_observer(current, case):
            with current.db.observe():
                with pytest.raises(RuntimeError, match="retained assertion atom differs"):
                    current._row(case["before"].checkpoint, current._current)
        assert current.db.metrics["returned_atom_octets"] == len(raw)
        assert current._current.row is None and current._current.atom is None
        assert snapshot(current.db) == before and not current.db.in_transaction
