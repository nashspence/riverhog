"""Actual whole entry-replay work on native checkpoints and fresh bounded pagers."""

from __future__ import annotations

import hashlib
import importlib
import json
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import _entry_replay as replay_owner
from riverhog_provenance import validate_journal
from riverhog_provenance._entry_replay import EntryReplay, ReplayBudget
from riverhog_provenance._observation_coverage import ObservationCoverage
from riverhog_provenance._semantic_program import Nodes
from riverhog_provenance._session_checkpoint import ValidationLedger
from riverhog_provenance.journal_store import JournalStore
from test_child_admission_growth import octets
from test_entry_replay import owned_summary
from test_entry_replay_child_growth import (
    _STAGING_IDENTITY_LINK,
    FAMILIES,
    TRAJECTORIES,
    ChildConnection,
    ChildCursor,
    dimensions_for,
    node_kernels,
    trajectories,
)
from test_entry_replay_graph_growth import graph_histories as graph_histories
from test_entry_replay_graph_growth import graph_inputs, initialize_graph_cache
from test_entry_replay_growth import REGIMES, observe_replay, owner
from test_entry_replay_growth import admission_histories as admission_histories
from test_entry_replay_publication_growth import PUBLICATION_CASES, publication_fixture, restore
from test_entry_replay_restoration_growth import _METADATA, _PAYLOAD
from test_entry_replay_transitions_growth import (
    ROW_TRANSITIONS,
    transition_cases,
    transition_inputs,
)
from test_generic_semantics import semantic_child_input_dimensions
from test_semantic_edge import observed_work
from test_semantic_owner import snapshot


class AdvanceCursor(ChildCursor):
    def record(self, row):
        result = super().record(row)
        if row is not None:
            if self.sql == _METADATA:
                self.connection.frame_metadata = row
            elif self.sql == _PAYLOAD:
                size = len(row[0])
                self.connection.metrics["returned_metadata_octets"] -= size
                self.connection.metrics["source_payload_octets"] += size
        return result


class AdvanceConnection(ChildConnection):
    frame_metadata = None
    frame_admitted = None
    graph_key_only = False

    @contextmanager
    def observe(self):
        with super().observe() as metrics:
            metrics["admitted_graph_octets"] = 0
            yield metrics

    def execute(self, sql, parameters=()):
        if self.recording and sql in {_METADATA, _PAYLOAD}:
            self.active_sql, self.active_parameters = sql, parameters
            if sql == _PAYLOAD:
                assert self.frame_metadata is not None
                assert self.frame_admitted == self.frame_metadata[2]
            cursor = self.cursor(factory=AdvanceCursor)
            cursor.sql, cursor.parameters = sql, parameters
            self.statements.append((sql, parameters, cursor))
            return cursor.execute(sql, parameters)
        return super().execute(sql, parameters)

    def set_authorizer(self, callback):
        if callback is None:
            return sqlite3.Connection.set_authorizer(self, None)

        def authorize(action, table, column, database, trigger):
            if (
                action == sqlite3.SQLITE_READ
                and self.graph_key_only
                and (
                    (table == "dag_levels" and column == "level")
                    or (table == "dag_undo" and column == "old_value")
                )
            ):
                return sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_READ and column == "value":
                sql = self.active_sql
                if table == "frames":
                    return sqlite3.SQLITE_DENY
                if table == "journal_validation_frame":
                    return (
                        sqlite3.SQLITE_OK
                        if (sql == _METADATA or sql == _PAYLOAD and self.frame_admitted is not None)
                        else sqlite3.SQLITE_DENY
                    )
                if table == "identities":
                    if sql == _STAGING_IDENTITY_LINK:
                        return sqlite3.SQLITE_OK
                    return (
                        sqlite3.SQLITE_OK
                        if sql.startswith(
                            (
                                "SELECT domain,length(value)",
                                "SELECT domain,typeof(value),length(value)",
                                "SELECT value=?",
                            )
                        )
                        else sqlite3.SQLITE_DENY
                    )
                if table == "assertions":
                    if sql.startswith("SELECT length(value)"):
                        return sqlite3.SQLITE_OK
                    if sql == "SELECT value FROM assertions WHERE assertion_id = ? AND active = 1":
                        return (
                            sqlite3.SQLITE_OK
                            if (
                                self.selected_length is not None
                                and self.byte_budget is not None
                                and self.selected_length <= self.byte_budget
                                and self.active_parameters == (self.selected_identity,)
                            )
                            else sqlite3.SQLITE_DENY
                        )
                    if self.family == "finish" and sql.startswith("SELECT value "):
                        return sqlite3.SQLITE_OK
                    if (
                        self.family == "targets"
                        and sql.startswith("SELECT t.position")
                        and "length(" in sql
                    ):
                        return sqlite3.SQLITE_OK
                    return sqlite3.SQLITE_DENY
            return callback(action, table, column, database, trigger)

        return sqlite3.Connection.set_authorizer(self, authorize)


@contextmanager
def advance_observer(
    current,
    dimensions,
    *,
    root_operation=None,
    extra_owners=None,
    operation_aliases=None,
    kernel_hook=None,
    primitive_hook=None,
    failed_operations=(),
):
    db = current.db
    root_operation = EntryReplay.advance if root_operation is None else root_operation
    binding = growth_binding(root_operation)
    coordinators = [
        growth_binding(operation)
        for operation in (
            root_operation,
            EntryReplay.advance,
            EntryReplay._child_step,
            EntryReplay._graph_step,
            EntryReplay._admit_row,
            EntryReplay._lifecycle,
        )
    ]
    dimension_sets = [
        (item, {value.name for value in item.declaration.dimensions}) for item in coordinators
    ]
    selected, row, node, coverage = (
        replay_owner.read_changed_assertion,
        EntryReplay._row,
        Nodes.__init__,
        ObservationCoverage.__init__,
    )
    frame_operation = EntryReplay._entry
    child_operation = EntryReplay._child_step
    graph_operation = EntryReplay._graph_step
    graph_application = replay_owner.OnlineDag._apply
    ledger_publication = ValidationLedger._publish
    operations, pending = set(), [binding]
    while pending:
        for _tag, operation in pending.pop().delegates:
            if operation not in operations:
                operations.add(operation)
                pending.append(growth_binding(operation))
    owners = {}
    for operation in operations:
        if operation is selected:
            owners[operation] = replay_owner
        elif "." in operation.__qualname__:
            module = importlib.import_module(operation.__module__)
            owners[operation] = getattr(module, operation.__qualname__.split(".")[0])

    if extra_owners is not None:
        owners.update(extra_owners)

    def tagged(parent, operation):
        for parent_binding, keys in dimension_sets:
            if keys != set(parent):
                continue
            tags = [tag for tag, child in parent_binding.delegates if child is operation]
            assert tags
            if len(tags) == 1:
                return tags[0]
            assert operation is row and set(tags) == {"row_constructor", "row_completion"}
            return (
                "row_constructor"
                if parent["row_constructor_calls"] and db.row_invocations == 0
                else "row_completion"
            )
        return None

    def inputs(parent, operation, _args, kwargs):
        tag = tagged(parent, operation)
        declaration = growth_binding(operation).declaration
        if tag is not None:
            result = {item.name: parent[tag + "_" + item.name] for item in declaration.dimensions}
            if operation is row:
                db.row_invocations += 1
            if operation.__name__ == "prepare" and "byte_budget" in result:
                assert kwargs["byte_budget"] == result["byte_budget"]
        elif operation is selected:
            result = {item.name: parent[item.name] for item in declaration.dimensions}
        elif operation is node:
            result = {
                item.name: parent["atom_input_items" if item.name == "input_items" else item.name]
                for item in declaration.dimensions
            }
        elif operation is coverage:
            result = {
                "input_rows": parent["coverage_rows"],
                "key_octets": parent["coverage_key_octets"],
            }
        elif operation.__qualname__.startswith(("ValidationLedger.", "JournalPrefix.")):
            result = {}
            for item in declaration.dimensions:
                ledger_rows = (
                    operation.__qualname__.startswith("ValidationLedger.")
                    and item.name == "retained_rows"
                )
                result[item.name] = parent["checkpoint_retained_rows" if ledger_rows else item.name]
                if ledger_rows and operation is ledger_publication and "padding_blocks" in parent:
                    result[item.name] += 1
        else:
            result = semantic_child_input_dimensions(parent, operation)
        if operation is selected:
            db.byte_budget = kwargs["byte_budget"]
        return result

    def selector(parent, operation, _args, _kwargs, tags):
        assert operation is row and set(tags) == {"row_constructor", "row_completion"}
        tag = (
            "row_constructor"
            if parent["dimensions"]["row_constructor_calls"]
            and not parent["delegates"]["row_constructor"]
            else "row_completion"
        )
        return tag

    index_owners = {
        EntryReplay.advance,
        EntryReplay._row,
        EntryReplay._child_step,
        EntryReplay._admit_row,
        EntryReplay._lifecycle,
    }
    marks = {}
    db.all_node_containers = db.all_node_opcodes = 0

    def primitive(operation, _args, _result, work):
        if operation is node:
            db.all_node_containers += work["indexed_containers"]
            db.all_node_opcodes += work["python_opcode_events"]
        if operation in index_owners:
            containers, opcodes = marks.pop(operation)
            work.update(
                indexed_containers=db.all_node_containers - containers,
                index_python_opcode_events=db.all_node_opcodes - opcodes,
            )
        if any(
            bound.metric == "metadata_octets"
            for bound in growth_binding(operation).declaration.bounds
        ):
            work["metadata_octets"] = work["returned_metadata_octets"]
        work.setdefault("historical_payload_octets", 0)
        return work if primitive_hook is None else primitive_hook(operation, _args, _result, work)

    real_claim = current._claim
    active = []

    def admitted(octets):
        if db.recording and frame_operation in active:
            assert db.frame_metadata is not None and octets == db.frame_metadata[2]
            assert db.frame_admitted is None
            db.frame_admitted = octets
        if db.recording and graph_operation in active:
            db.metrics["admitted_graph_octets"] += octets
        return real_claim(octets)

    db.row_invocations = db.node_containers = db.node_opcodes = 0
    with node_kernels() as kernel:

        @contextmanager
        def context(operation, args):
            previous = db.family
            previous_key_only = db.graph_key_only
            if operation is graph_operation:
                db.graph_key_only = True
            if operation in index_owners:
                assert operation not in marks
                marks[operation] = (db.all_node_containers, db.all_node_opcodes)
            selected_family = operation.__qualname__.split(".")[0]
            db.family = next(
                (family for family, cls in FAMILIES.items() if cls.__name__ == selected_family),
                previous,
            )
            if operation is child_operation:
                db.row_invocations = 0
            active.append(operation)
            try:
                with kernel(operation, args) as callback:
                    if kernel_hook is None:
                        yield callback
                    else:
                        with kernel_hook(operation, args) as extra:
                            assert callback is None or extra is None
                            yield callback if extra is None else extra
            finally:
                assert active.pop() is operation
                db.family = previous
                db.graph_key_only = previous_key_only

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(current, "_claim", admitted)
            with observe_replay(
                db,
                current.catalog,
                dimensions,
                root_operation=root_operation,
                input_dimensions=inputs,
                operation_owners=owners,
                operation_aliases=operation_aliases,
                kernel_observer=context,
                failed_operations=failed_operations,
                delegate_selector=selector,
                validation_policy=lambda _schema, _value: None,
                primitive_adapter=primitive,
                inclusive_operation=lambda parent, operation: (
                    parent.declaration.id
                    == "riverhog-provenance.sqlite-online-graph-retirement-application.growth/v1"
                    and operation is graph_application
                ),
            ) as observed:
                yield observed


def advance_cases(root, catalog, build, graphs, *, early=False):
    child_specs = tuple(
        TRAJECTORIES[i] for i in ((0, 3, 4, 5, 6) if early else range(len(TRAJECTORIES)))
    )
    row_specs = tuple(
        ROW_TRANSITIONS[i] for i in ((0, 4, 5, 6, 7) if early else range(len(ROW_TRANSITIONS)))
    )
    for regime in sorted(
        {regime for regime, _ in (*child_specs, *row_specs)}, key=lambda value: value[0]
    ):
        build(regime)
    children = root / "children"
    children.mkdir()
    rows = root / "rows"
    rows.mkdir()
    result = trajectories(children, catalog, build, child_specs)
    result.extend(transition_cases(rows, catalog, build, row_specs))
    result.extend(graphs((0 if early else 17, 1, 2, 64)))
    for ordinal, specification in enumerate(
        tuple(PUBLICATION_CASES[i] for i in ((0, 9, 10) if early else (0, 8, 9, 10, 11, 12, 14)))
    ):
        with publication_fixture(build, specification, catalog) as (store, case):
            path = root / f"publication-{ordinal}.sqlite3"
            with closing(sqlite3.connect(path)) as saved:
                store.db.backup(saved)
            result.append({**case, "path": path})
    # Genuine pre-admission and recording checkpoints supplement the phase-specific histories.
    for ordinal, regime in enumerate((REGIMES[0], REGIMES[10], REGIMES[12])):
        case = dict(build(regime))
        with closing(JournalStore()) as store:
            current = restore(store, case, catalog)
            before = current.ledger.load()
            path = root / f"idle-{ordinal}.sqlite3"
            with closing(sqlite3.connect(path)) as saved:
                store.db.backup(saved)
            result.append({**case, "path": path, "before": before, "cached": None})
            current.admit(case["candidate"])
            for _ in range(100000):
                before = current.ledger.load()
                if before.checkpoint.phase == "recording":
                    path = root / f"recording-{ordinal}.sqlite3"
                    with closing(sqlite3.connect(path)) as saved:
                        store.db.backup(saved)
                    result.append(
                        {
                            **case,
                            "path": path,
                            "before": before,
                            "cached": replace(
                                current._current, child_kind=None, child_id=None, child=None
                            ),
                        }
                    )
                if before.checkpoint.phase == "frame":
                    break
                current.advance(ReplayBudget(rows=16))
            else:
                pytest.fail("finite whole-entry recording trajectory did not complete")
            with validate_journal(case["raw"], catalog=catalog) as expected:
                with owned_summary(store, current.prefix.finish()) as actual:
                    assert actual.materialize() == expected.materialize()
                    assert actual.journal_sha256 == hashlib.sha256(case["raw"]).hexdigest()
    return result


def advance_inputs(current, case, budget, *, cold):
    before = case["before"]
    state = before.checkpoint
    original = snapshot(current.db)
    width = 16384 + 2 * octets((before, original["journal_validation"], original["journal_prefix"]))
    ledger = {
        "retained_rows": sum(
            len(original[name]) for name in ("journal_validation", "journal_prefix", "frames")
        ),
        "metadata_row_octets": width,
    }
    active = state.phase not in {"frame", "terminal", "done"}
    cached = current._current
    current._start(before, budget)
    if active and cold:
        current._claim(len(cached.frame.json_bytes))
    selected_tag, selected_values = None, {}
    if active:
        if state.phase.startswith(("derivation-", "specialization-")):
            selected_tag = "graph"
            selected_values, _ = graph_inputs(current, case, budget)
        elif state.phase in {"row-admit", "lifecycle"}:
            selected_tag = "row_admission" if state.phase == "row-admit" else "lifecycle"
            _operation, selected_values, _ = transition_inputs(current, case, cold=cold)
        elif state.phase == "recording":
            selected_tag = "recording"
            selected_values = {"indexed_assertions": len(original["assertions"])}
        elif state.phase == "publish":
            selected_tag = "entry_publication"
            selected_values = case["dimensions"]
        else:
            selected_tag = "child"
            selected_values, _ = dimensions_for(
                current, case, cold_child=cold, cold_row=cold, budget=budget
            )
    entry_values = (
        {}
        if not active
        else {
            "retained_frames": state.frame_ordinal,
            "frame_octets": len(cached.frame.encoded),
            "initial_tail_octets": cached.prepared.before.byte_count % 64,
            "source_blocks": (cached.prepared.before.byte_count % 64 + len(cached.frame.encoded))
            // 64,
            "cold_cache": int(cold),
            "quarantine_octets": len(cached.frame.json_bytes),
            "metadata_row_octets": width,
        }
    )
    values = {"metadata_row_octets": width}
    for tag, operation in growth_binding(EntryReplay.advance).delegates:
        called = (
            tag == "load"
            or tag == "entry"
            and active
            or tag == selected_tag
            or tag in {"fence", "publication"}
            and selected_tag == "recording"
        )
        child_values = (
            ledger
            if tag in {"load", "fence", "publication"}
            else entry_values
            if tag == "entry"
            else selected_values
            if tag == selected_tag
            else {}
        )
        values[tag + "_calls"] = int(called)
        for item in growth_binding(operation).declaration.dimensions:
            values[tag + "_" + item.name] = child_values.get(item.name, 0)
    return values, original


@contextmanager
def advance_fixture(case, catalog, budget, *, cold, measured=False):
    with closing(JournalStore()) as store:
        with closing(sqlite3.connect(case["path"])) as seed:
            seed.backup(store.db)
        initialize_graph_cache(store)
        options = {} if "anchor" not in case else {"expected_anchor": case["anchor"]}
        current = owner(store, catalog, case["source_binding"], case["epoch"], **options)
        state = case["before"].checkpoint
        active = state.phase not in {"frame", "terminal", "done"}
        if active:
            current._start(case["before"], budget)
            current._current = (
                replace(case["cached"]) if case.get("cached") is not None else current._entry(state)
            )
        cached = current._current
        if not measured:
            yield current
            return
        path = store._storage.directory / "snapshot.sqlite3"
        store.db.close()
        store._storage.connection = sqlite3.connect(path, factory=AdvanceConnection)
        store.db.execute("PRAGMA synchronous=FULL").close()
        store.db.execute("PRAGMA cache_size=-64").close()
        current = owner(store, catalog, case["source_binding"], case["epoch"], **options)
        current._current = None if cold or cached is None else replace(cached)
        if not cold and active and "family" in case and not state.phase.endswith("-admission"):
            current._start(case["before"], budget)
            current._child(state, current._current, state.phase, state.child_id)
        yield current


def advance_samples(root, catalog, build, graphs, *, samples, early=False, checkpoints=None):
    projection = project_bindings((growth_binding(EntryReplay.advance),))
    cases = (
        advance_cases(root, catalog, build, graphs, early=early)
        if checkpoints is None
        else checkpoints
    )
    for ordinal, case in enumerate(cases):
        state = case["before"].checkpoint
        rows = (1, 16, 64) if ordinal % 7 == 0 and not early else (16,)
        for quantum in rows:
            budget = ReplayBudget(rows=quantum)
            for cold in (False,) if state.phase in {"frame", "terminal", "done"} else (False, True):
                with advance_fixture(case, catalog, budget, cold=cold) as setup:
                    dimensions, original = advance_inputs(setup, case, budget, cold=cold)
                with advance_fixture(case, catalog, budget, cold=cold, measured=True) as current:
                    # Same native cold/warm cache state on a separate uninstrumented pager.
                    with advance_fixture(case, catalog, budget, cold=cold, measured=True) as oracle:
                        with pytest.MonkeyPatch.context() as patch:
                            patch.setattr(
                                replay_owner, "uuid4", lambda: SimpleNamespace(hex="f" * 32)
                            )
                            expected = oracle.advance(budget)
                        expected_rows = snapshot(oracle.db)
                        expected_runs = dict(oracle.store._dag_runs)
                    with pytest.MonkeyPatch.context() as patch:
                        patch.setattr(replay_owner, "uuid4", lambda: SimpleNamespace(hex="f" * 32))
                        with advance_observer(current, dimensions) as (observed, complete):
                            with current.db.observe():
                                result = current.advance(budget)
                    work = observed_work(current.db, {key: 0 for key in current.db.metrics}, 0, 0)
                    work.update(
                        changed_rows=current.db.metrics["changed_rows"],
                        indexed_containers=current.db.all_node_containers,
                        index_python_opcode_events=current.db.all_node_opcodes,
                    )
                    sample = complete(observed, work)
                    validate_execution_samples(
                        projection, growth_binding(EntryReplay.advance).declaration.id, [sample]
                    )
                    samples.append(sample)
                    if len(samples) == 1 or len(samples) % 32 == 0:
                        (root / "whole-advance-progress.json").write_text(
                            json.dumps(
                                {
                                    "samples": len(samples),
                                    "phase": state.phase,
                                    "case_index": ordinal,
                                    "cases": len(cases),
                                    "cold": cold,
                                }
                            )
                            + "\n"
                        )
                    assert result == expected == current.ledger.load()
                    assert snapshot(current.db) == expected_rows and not current.db.in_transaction
                    assert current.store._dag_runs == expected_runs
                    assert result.checkpoint.version == state.version + int(
                        state.phase not in {"frame", "terminal", "done"}
                    )
                    for tag, children in sample["delegates"].items():
                        assert len(children) == dimensions[tag + "_calls"]
                    if state.phase != "publish":
                        assert original["journal_prefix"] == expected_rows["journal_prefix"]
    assert samples


@pytest.mark.growth_witness(
    "riverhog-whole-entry-replay-advance-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_replay.EntryReplay.advance",
)
def test_actual_whole_entry_advance(
    tmp_path, catalog, admission_histories, graph_histories, record_property
):
    samples = []
    record_property("growth_samples", samples)
    advance_samples(tmp_path, catalog, admission_histories, graph_histories, samples=samples)
