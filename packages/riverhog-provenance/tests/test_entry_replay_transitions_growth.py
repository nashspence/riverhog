from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing, contextmanager
from copy import deepcopy
from dataclasses import replace

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import _entry_replay as replay_owner
from riverhog_provenance import (
    append_correction,
    assertion_reference,
    encode_entry,
    new_id,
    parse_journal,
    validate_journal,
)
from riverhog_provenance._entry_replay import EntryReplay, ReplayBudget
from riverhog_provenance._observation_coverage import ObservationCoverage
from riverhog_provenance._semantic_program import Nodes
from riverhog_provenance._session_checkpoint import ValidationLedger
from riverhog_provenance.journal_store import JournalStore
from test_child_admission_growth import octets
from test_entry_replay import owned_summary
from test_entry_replay_child_growth import ChildConnection, node_kernels
from test_entry_replay_growth import REGIMES, observe_replay, owner
from test_entry_replay_growth import admission_histories as admission_histories
from test_entry_replay_publication_growth import restore
from test_entry_replay_row_growth import node_inputs
from test_journal_lifecycle import lifecycle_graph
from test_semantic_edge import observed_work
from test_semantic_owner import snapshot

ROW_TRANSITIONS = (
    (REGIMES[0], "plain"),
    (REGIMES[3], "plain"),
    (REGIMES[5], "plain"),
    (REGIMES[6], "plain"),
    (REGIMES[8], "plain"),
    (REGIMES[12], "plain"),
    (REGIMES[2], "correction"),
    (REGIMES[0], "lifecycle"),
)


def transition_cases(root, catalog, build, specifications=ROW_TRANSITIONS):
    for regime in sorted({regime for regime, _ in specifications}, key=lambda value: value[0]):
        build(regime)
    result = []
    for ordinal, (regime, variant) in enumerate(specifications):
        case = dict(build(regime))
        if variant == "correction":
            raw = case["raw"][: -len(case["candidate"].encoded)]
            with validate_journal(raw, catalog=catalog) as initial:
                value = deepcopy(initial.graph["artifacts"][0])
                target = assertion_reference(initial, value["assertion_id"])
            value["assertion_id"] = new_id()
            case["raw"] = append_correction(
                raw,
                [target],
                reason="Replace documentary assertion.",
                recorded_by_agent_id=case["who"],
                assertions={"artifacts": [value]},
                catalog=catalog,
            )
            case["candidate"] = parse_journal(case["raw"])[-1]
        elif variant == "lifecycle":
            document = deepcopy(case["candidate"].document)
            graph, _state, event = lifecycle_graph(document["body"]["assertions"], case["who"])
            graph["relations"] = [
                event("usage"),
                event("usage", "9999-12-30T23:59:59.999999999Z"),
                event("usage", "9999-12-30T23:59:59.999999998Z"),
                event("invalidation", "9999-12-31T23:59:59.999999999Z"),
            ]
            document["body"]["assertions"] = graph
            case["raw"] = encode_entry(document, catalog=catalog)
            case["candidate"] = parse_journal(case["raw"])[0]
        with closing(JournalStore()) as store:
            current = restore(store, case, catalog)
            current.admit(case["candidate"])
            seen = set()
            for _ in range(100000):
                before = current.ledger.load()
                state = before.checkpoint
                if state.phase == "frame":
                    break
                if state.phase in {"row-admit", "lifecycle"}:
                    if state.phase == "row-admit":
                        selected = replay_owner.read_changed_assertion(
                            store.db, after=state.row_after, byte_budget=ReplayBudget().octets
                        )
                        row = (
                            None
                            if selected is None or selected.raw is None
                            else json.loads(selected.raw)
                        )
                        status = (
                            "empty" if selected is None else "inactive" if row is None else "active"
                        )
                    else:
                        row = current._row(state, current._current)
                        status = "restored"
                    update = (
                        row is not None and row["type"] in {"usage", "invalidation"} and "at" in row
                    )
                    prior = (
                        None
                        if not update
                        else store.db.execute(
                            "SELECT earliest,latest_usage FROM lifecycle_extrema WHERE state_id=?",
                            (row["state"]["object_id"],),
                        ).fetchone()
                    )
                    key = (
                        state.phase,
                        status,
                        None if row is None else row["type"],
                        bool(update),
                        prior is not None,
                    )
                    if key not in seen:
                        seen.add(key)
                        path = root / f"row-trajectory-{ordinal}-{len(seen)}.sqlite3"
                        with closing(sqlite3.connect(path)) as saved:
                            store.db.backup(saved)
                        result.append(
                            {
                                **case,
                                "path": path,
                                "before": before,
                                "key": key,
                                "row": row,
                                "cached": replace(
                                    current._current, child_kind=None, child_id=None, child=None
                                ),
                            }
                        )
                current.advance(ReplayBudget(rows=16))
            else:
                pytest.fail("finite native row transition trajectory did not complete")
            with validate_journal(case["raw"], catalog=catalog) as expected:
                with owned_summary(store, current.prefix.finish()) as actual:
                    assert actual.materialize() == expected.materialize()
                    assert actual.journal_sha256 == hashlib.sha256(case["raw"]).hexdigest()
    return result


def transition_inputs(current, case, *, cold):
    original = snapshot(current.db)
    before, row = case["before"], case["row"]
    admission = before.checkpoint.phase == "row-admit"
    operation = EntryReplay._admit_row if admission else EntryReplay._lifecycle
    width = 16384 + 2 * octets((before, original["journal_validation"], original["journal_prefix"]))
    row_values = {
        "indexed_assertions": len(original["assertions"]),
        "changed_identities": len(original["changes"]),
        "atom_octets": 0 if row is None else len(replay_owner.GenericAtom.admit(row).encoded),
    }
    node = {item.name: 0 for item in growth_binding(Nodes.__init__).declaration.dimensions}
    if row is not None:
        node.update(node_inputs(row))
        # node_inputs names the row wrapper's input-slot dimension explicitly.
        node["input_items"] = node.pop("atom_input_items")
    restoration = {
        **row_values,
        **{key: value for key, value in node.items() if key != "input_items"},
        "atom_input_items": node["input_items"],
        "cold_row": int(cold),
    }
    values = {
        "retained_rows": sum(len(rows) for rows in original.values()),
        "metadata_row_octets": width,
    }
    if not admission:
        values["lifecycle_update"] = int(row["type"] in {"usage", "invalidation"} and "at" in row)
    for tag, child in growth_binding(operation).delegates:
        active = tag != "index" or row is not None
        child_values = (
            row_values
            if tag == "admission"
            else node
            if tag == "index"
            else restoration
            if tag == "restoration"
            else {
                "retained_rows": sum(
                    len(original[name])
                    for name in ("journal_validation", "journal_prefix", "frames")
                ),
                "metadata_row_octets": width,
            }
        )
        values[tag + "_calls"] = int(active)
        for item in growth_binding(child).declaration.dimensions:
            values[tag + "_" + item.name] = child_values[item.name]
    return operation, values, original


@contextmanager
def transition_observer(current, operation, dimensions):
    db = current.db
    binding = growth_binding(operation)
    selected, node, coverage, row_operation = (
        replay_owner.read_changed_assertion,
        Nodes.__init__,
        ObservationCoverage.__init__,
        EntryReplay._row,
    )
    root_operations = {child for _, child in binding.delegates}
    owners = {
        selected: replay_owner,
        node: Nodes,
        coverage: ObservationCoverage,
        row_operation: EntryReplay,
        ValidationLedger._fence: ValidationLedger,
        ValidationLedger._publish: ValidationLedger,
    }

    def inputs(parent, child, _args, kwargs):
        if child in root_operations:
            tag = next(tag for tag, value in binding.delegates if value is child)
            result = {
                item.name: dimensions[tag + "_" + item.name]
                for item in growth_binding(child).declaration.dimensions
            }
        elif child is selected:
            result = {
                item.name: parent[item.name]
                for item in growth_binding(child).declaration.dimensions
            }
        elif child is node:
            result = {
                item.name: parent["atom_input_items" if item.name == "input_items" else item.name]
                for item in growth_binding(child).declaration.dimensions
            }
        else:
            assert child is coverage
            result = {
                "input_rows": parent["coverage_rows"],
                "key_octets": parent["coverage_key_octets"],
            }
        if child is selected:
            db.byte_budget = kwargs["byte_budget"]
        return result

    def primitive(child, _args, _result, work):
        if child is node:
            db.node_containers += work["indexed_containers"]
            db.node_opcodes += work["python_opcode_events"]
        if child is row_operation:
            work.update(
                indexed_containers=db.node_containers, index_python_opcode_events=db.node_opcodes
            )
            db.node_containers = db.node_opcodes = 0
        work.setdefault("historical_payload_octets", 0)
        return work

    db.node_containers = db.node_opcodes = 0
    with node_kernels() as kernel:
        with observe_replay(
            db,
            current.catalog,
            dimensions,
            root_operation=operation,
            input_dimensions=inputs,
            operation_owners=owners,
            kernel_observer=kernel,
            primitive_adapter=primitive,
        ) as observed:
            yield observed


def transition_samples(
    root, catalog, build, *, samples, specifications=ROW_TRANSITIONS, checkpoints=None, phase=None
):
    cases = (
        transition_cases(root, catalog, build, specifications)
        if checkpoints is None
        else checkpoints
    )
    cases = [case for case in cases if phase is None or case["before"].checkpoint.phase == phase]
    for case in cases:
        admission = case["before"].checkpoint.phase == "row-admit"
        for cold in (False,) if admission else (False, True):
            with closing(JournalStore()) as store:
                with closing(sqlite3.connect(case["path"])) as seed:
                    seed.backup(store.db)
                current = owner(store, catalog, case["source_binding"], case["epoch"])
                current._current = replace(case["cached"])
                current._start(case["before"], ReplayBudget())
                operation, dimensions, original = transition_inputs(current, case, cold=cold)
                with closing(JournalStore()) as expected_store:
                    store.db.backup(expected_store.db)
                    expected_owner = owner(
                        expected_store, catalog, case["source_binding"], case["epoch"]
                    )
                    expected_owner._current = replace(case["cached"])
                    expected_owner._start(case["before"], ReplayBudget())
                    expected = operation(expected_owner, case["before"], expected_owner._current)
                    expected_rows = snapshot(expected_store.db)
                path = store._storage.directory / "snapshot.sqlite3"
                store.db.close()
                store._storage.connection = sqlite3.connect(path, factory=ChildConnection)
                store.db.execute("PRAGMA synchronous=FULL").close()
                store.db.execute("PRAGMA cache_size=-64").close()
                current = owner(store, catalog, case["source_binding"], case["epoch"])
                current._current = replace(case["cached"])
                if cold:
                    current._current.row_key = current._current.row_sha256 = None
                    current._current.row = current._current.atom = None
                current._start(case["before"], ReplayBudget())
                with transition_observer(current, operation, dimensions) as (observed, complete):
                    with current.db.observe():
                        result = operation(current, case["before"], current._current)
                work = observed_work(current.db, {key: 0 for key in current.db.metrics}, 0, 0)
                work.update(
                    changed_rows=current.db.metrics["changed_rows"],
                    indexed_containers=sum(
                        child["metrics"]["indexed_containers"]
                        for tag in ("index", "restoration")
                        for child in observed["delegates"].get(tag, [])
                    ),
                    index_python_opcode_events=sum(
                        child["metrics"][
                            "python_opcode_events"
                            if tag == "index"
                            else "index_python_opcode_events"
                        ]
                        for tag in ("index", "restoration")
                        for child in observed["delegates"].get(tag, [])
                    ),
                )
                sample = complete(observed, work)
                binding = growth_binding(operation)
                validate_execution_samples(
                    project_bindings((binding,)), binding.declaration.id, [sample]
                )
                samples.append(sample)
                assert result == expected == current.ledger.load()
                assert snapshot(current.db) == expected_rows and not current.db.in_transaction
                assert snapshot(current.db)["journal_prefix"] == original["journal_prefix"]
                for tag, children in sample["delegates"].items():
                    assert len(children) == dimensions[tag + "_calls"]
                if not admission:
                    assert (
                        current._current.row_key,
                        current._current.row_sha256,
                        current._current.row,
                        current._current.atom,
                    ) == (None, None, None, None)
    assert samples


@pytest.fixture(scope="module")
def row_transition_checkpoints(tmp_path_factory, catalog, admission_histories):
    root = tmp_path_factory.mktemp("native-row-transitions")
    return transition_cases(root, catalog, admission_histories)


@pytest.mark.growth_witness(
    "riverhog-entry-replay-row-transitions-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_replay.EntryReplay._admit_row",
)
def test_actual_selected_row_admission(catalog, row_transition_checkpoints, record_property):
    samples = []
    record_property("growth_samples", samples)
    transition_samples(
        None,
        catalog,
        None,
        samples=samples,
        checkpoints=row_transition_checkpoints,
        phase="row-admit",
    )


@pytest.mark.growth_witness(
    "riverhog-entry-replay-lifecycle-transition-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_replay.EntryReplay._lifecycle",
)
def test_actual_lifecycle_transition(catalog, row_transition_checkpoints, record_property):
    samples = []
    record_property("growth_samples", samples)
    transition_samples(
        None,
        catalog,
        None,
        samples=samples,
        checkpoints=row_transition_checkpoints,
        phase="lifecycle",
    )


@pytest.mark.parametrize("phase", ["row-admit", "lifecycle"])
def test_parent_failure_rolls_back_row_transition_and_native_retry(
    tmp_path, catalog, admission_histories, monkeypatch, phase
):
    cases = transition_cases(tmp_path, catalog, admission_histories, (ROW_TRANSITIONS[-1],))
    case = next(
        case
        for case in cases
        if case["before"].checkpoint.phase == phase
        and case["row"] is not None
        and case["row"]["type"] == "usage"
        and "at" in case["row"]
    )
    with closing(JournalStore()) as store:
        with closing(sqlite3.connect(case["path"])) as seed:
            seed.backup(store.db)
        current = owner(store, catalog, case["source_binding"], case["epoch"])
        current._current = replace(case["cached"])
        current._start(case["before"], ReplayBudget())
        operation = EntryReplay._admit_row if phase == "row-admit" else EntryReplay._lifecycle
        original = snapshot(store.db)
        publication = current.ledger._publish

        def fail_after_write(*args, **kwargs):
            publication(*args, **kwargs)
            raise RuntimeError("injected parent publication failure")

        with monkeypatch.context() as patch:
            patch.setattr(current.ledger, "_publish", fail_after_write)
            with pytest.raises(RuntimeError, match="injected parent"):
                operation(current, case["before"], current._current)
        assert snapshot(store.db) == original and not store.db.in_transaction
        assert current.ledger.load() == case["before"]
        result = operation(current, case["before"], current._current)
        assert result.checkpoint.version == case["before"].checkpoint.version + 1
        assert result == current.ledger.load()
        if phase == "lifecycle":
            assert store.db.execute("SELECT count(*) FROM lifecycle_extrema").fetchone()[0] == 1
            assert current._current.row is None and current._current.atom is None
