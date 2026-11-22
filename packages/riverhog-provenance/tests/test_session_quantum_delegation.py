"""Whole-invocation Native growth with heterogeneous actual child arguments.

Every measured row is one public advance with one original WorkQuantum. A
separate one-unit trajectory supplies exact per-call inputs and semantic oracles;
it never supplies maxima or costs to the parent envelope.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from contextlib import nullcontext
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import _entry_replay as replay_owner
from riverhog_provenance import _online_dag as graph_owner
from riverhog_provenance import journal_session as session_owner
from riverhog_provenance._checkpoint_manifest import PREFIX_BINDING_OCTETS_MAX
from riverhog_provenance._generic_semantics import _HEADER_OCTETS as CHILD_HEADER_OCTETS
from riverhog_provenance._journal_prefix import _CHECKPOINT_CHARACTERS
from riverhog_provenance._native_work import WorkAmount, WorkQuantum
from riverhog_provenance._session_checkpoint import _HEADER_CHARACTERS
from riverhog_provenance._session_input import _HEADER_OCTETS as INPUT_HEADER_OCTETS
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
    JournalValidationWorkBudget,
)
from test_child_admission_growth import octets
from test_entry_replay_graph_growth import graph_histories as graph_histories
from test_entry_replay_growth import admission_histories as admission_histories
from test_journal_session_growth import (
    progress_dimensions,
    session_cases,
    session_dimensions,
    session_fixture,
    session_observer,
)
from test_semantic_edge import observed_work
from test_semantic_owner import snapshot

from scripts.validation_numeric import numeric_policy_identity
from tests.support.growth_batch import SampleBatch

ADVANCE = JournalValidationSession.advance
ORIGINS = (
    "stream-empty-pull",
    "stream-pending-atom",
    "retained-window",
    "retained-short-window",
    "retained-synchronize",
    "wide-control-entry-admission",
    "terminal-done",
    "staging-admission",
    "staging-plan",
    "row-admission",
    "generic-admission",
    "history-generic-plan",
    "lifecycle",
    "finish-plan",
    "correction-targets",
    "correction-retire",
    "correction-graph-replace",
    "correction-graph-retire",
    "correction-seed",
    "long-history-terminal",
    "large-history-publication",
    "large-current-publication",
    "sha-tail-55",
    "sha-tail-56",
)


def select_origin(index, before, key):
    phase, selected = key[:2]
    frame = before.checkpoint.frame_ordinal
    working = selected is not None and selected[0] is False
    candidate = key[-1]
    selectors = (
        index == 0 and phase == "frame" and not selected[0] and selected[-1] == "empty",
        index == 0 and phase == "frame" and selected[0],
        index == 1 and phase == "frame" and not selected[0] and selected[3],
        index == 2 and phase == "frame" and not selected[0] and not selected[3],
        index == 1 and phase == "frame" and selected[1],
        index == 7 and phase == "entry-admission",
        index == 1 and phase == "done",
        index == 0 and phase == "staging-admission",
        index == 1 and phase == "staging" and working,
        index == 0 and phase == "row-admit",
        index == 1 and phase == "generic-admission",
        index == 3 and phase == "generic" and working and candidate,
        index == 0 and phase == "lifecycle",
        index == 1 and phase == "finish" and working,
        index == 10 and phase == "targets" and working,
        index == 10 and phase == "retire" and working,
        index == 10 and phase == "derivation-replace",
        index == 10 and phase == "derivation-retire",
        index == 10 and phase == "seed" and working,
        index == 4 and phase == "terminal",
        index == 5 and phase == "publish" and frame == 0,
        index == 6 and phase == "publish" and candidate,
        index == 8 and phase == "terminal",
        index == 9 and phase == "terminal",
    )
    return next((name for name, selected in zip(ORIGINS, selectors, strict=True) if selected), None)


@pytest.fixture(scope="module")
def quantum_origins(
    tmp_path_factory, catalog, admission_histories, graph_histories, module_journal_constructions
):
    root = tmp_path_factory.mktemp("native-quantum-origins")
    captured = set()

    def select(index, before, key):
        name = select_origin(index, before, key)
        if name in captured:
            return None
        if name is not None:
            captured.add(name)
        return name

    started = time.monotonic_ns()
    cases = session_cases(
        root,
        catalog,
        admission_histories,
        graph_histories,
        journal_constructions=module_journal_constructions,
        capture_candidate=True,
        select=select,
    )
    footprint = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
    evidence = Path(os.environ.get("RIVERHOG_NATIVE_DELEGATION_EVIDENCE", str(root)))
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "fixture.json").write_text(
        json.dumps(
            {
                "build_elapsed_ns": time.monotonic_ns() - started,
                "retained_fixture_bytes": footprint,
                "origins": [
                    {
                        "origin": row["origin"],
                        "regime": row["regime"],
                        "phase": row["before"].checkpoint.phase,
                        "frame_ordinal": row["before"].checkpoint.frame_ordinal,
                        "source_bytes": len(row["raw"]),
                        "snapshot_bytes": row["path"].stat().st_size,
                    }
                    for row in cases
                ],
            },
            indent=2,
        )
        + "\n"
    )
    assert captured == set(ORIGINS), (
        "missing independently selected origins",
        set(ORIGINS) - captured,
    )
    return {row["origin"]: row for row in cases}, evidence


def parent_envelopes(session, budget):
    """Only original state plus admitted allowances; no future vector maxima.

    Native journal_* controls have at most 13 fixed occupied rows. Existing header and
    binding limits bound their fixed shape; original widths plus admitted scalar
    increments preserve warm controls across decimal-width boundaries. No cold
    decoder cap is imposed on live state. Every exact child primitive check and
    independent aggregate row/count ceiling remains in force.
    """
    original = snapshot(session._store.db)
    retained = sum(map(len, original.values()))
    header = tuple(
        rows
        for name, rows in original.items()
        if name.startswith("journal_") and name != "journal_validation_frame"
    )
    old_controls = octets(
        (
            header,
            session._ledger.binding,
            session._source_binding,
            session._ledger.load(),
            getattr(session._input, "state", None),
        )
    )
    # Graph scalar magnitudes are independent of row count. Sum only the two
    # current fixed graph controls; retain a maximum old level/undo width.
    graph_controls = octets(original["dag_runs"]) + max(
        [0, *(octets(row) for name in ("dag_levels", "dag_undo") for row in original[name])]
    )
    atom = max(
        [
            budget.octets,
            session._ledger.load().checkpoint.frame_octets,
            *(octets(row) for name in ("assertions", "maps", "findings") for row in original[name]),
        ]
    )
    rows = retained + budget.work.rows + 256 * budget.work.transitions
    scalar_growth = (
        1
        + (
            1
            + budget.work.rows
            + budget.work.octets
            + budget.work.source_octets
            + budget.work.transitions
        ).bit_length()
    )
    # Four octets per character is conservative for existing character limits.
    # 8192 covers finite algorithm/UUID/digest binding fields and JSON punctuation.
    controls = (
        2 * PREFIX_BINDING_OCTETS_MAX
        + 4
        * (_HEADER_CHARACTERS + INPUT_HEADER_OCTETS + _CHECKPOINT_CHARACTERS + CHILD_HEADER_OCTETS)
        + 8192
    )
    # <32 occupied copies:13 journal controls,2 graph runs, before/input/bindings.
    # Each fixed control has <64 scalar/token slots. No historical payload sum
    # or observed later child maximum enters the parent metadata envelope.
    width = 16384 + 2 * 32 * (old_controls + graph_controls + controls + 64 * scalar_growth)

    def envelope(name, unit):
        if name.endswith("_calls"):
            return 2 if name.endswith("digest_calls") else 1
        if name.endswith(("padding_blocks",)):
            return 2
        if name.endswith("source_blocks"):
            return (atom + 63) // 64
        if name.endswith("block_octets"):
            return 64
        if name.endswith("initial_tail_octets"):
            return 63
        if name.endswith(("row_budget", "step_budget", "generic_schema_steps")):
            return budget.rows
        if name.endswith("frame_budget"):
            return 1
        if name.endswith("byte_budget"):
            return budget.octets
        if name.endswith(("metadata_row_octets", "control_octets")):
            return width
        if not unit:
            return 1
        if name.endswith(
            ("retained_rows", "retained_frames", "indexed_assertions", "changed_identities")
        ):
            return rows
        if unit == (("octets", 1),):
            return atom + width if name.endswith("coverage_key_octets") else atom
        if unit == (("count", 1),) and name.endswith(("input_items", "coverage_rows")):
            # JSON containers/value slots and coverage tuples each consume at
            # least one encoded octet of their original admitted atom. Existing
            # indexed coverage contributes at most the retained row population.
            return rows + atom
        raise AssertionError(("unmapped source-owned envelope dimension", name, unit))

    result = {
        "retained_rows": retained,
        "metadata_row_octets": width,
        "transition_limit": budget.work.transitions,
        "work_rows": budget.work.rows,
        "work_octets": budget.work.octets,
        "work_source_octets": budget.work.source_octets,
    }
    for tag, operation in growth_binding(ADVANCE).delegates:
        for dimension in growth_binding(operation).declaration.dimensions:
            result[tag + "_" + dimension.name] = envelope(dimension.name, dimension.unit)
    return result


class OriginalIdentifiers:
    """Repeat the original fresh-ID sequence on the independent oracle only."""

    def __init__(self, old_rows):
        self.excluded = {
            match.group()
            for rows in old_rows.values()
            for row in rows
            for value in row
            if isinstance(value, (str, bytes))
            for match in re.finditer(
                r"(?<![0-9a-f])[0-9a-f]{32}(?![0-9a-f])",
                value if isinstance(value, str) else value.decode("utf-8", errors="ignore"),
            )
        }
        self.values = []
        self.next_value = 0

    def __call__(self):
        while True:
            self.next_value += 1
            value = f"{self.next_value:032x}"
            assert len(value) == 32
            if value not in self.excluded:
                break
        assert value not in self.values
        self.values.append(value)
        return SimpleNamespace(hex=value)


def exact_plan(session, budget):
    """Predict exact calls, proving every original reservation fits shared remaining work."""
    unit = replace(budget, work=replace(budget.work, transitions=1))
    limits = WorkAmount(
        budget.work.rows, budget.work.octets, budget.work.source_octets, budget.work.transitions
    )
    total, plan, owned = WorkAmount(), [], []
    original_reserve = WorkQuantum.reserve

    def quantum(selected):
        value = WorkQuantum(selected)
        owned.append(value)
        return value

    def reserve(current, amount):
        reservation = original_reserve(current, amount)
        if current is not owned[-1] or not (total + current.consumed + current.held).fits(limits):
            reservation.close()
            raise AssertionError(
                "one-unit oracle admission would exceed the original shared quantum"
            )
        return reservation

    for _ in range(budget.work.transitions):
        values, before = session_dimensions(session, unit, ADVANCE, cold=False)
        values["progress_calls"] = 0
        plan.append((before, values))
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(session_owner, "WorkQuantum", quantum)
            patch.setattr(WorkQuantum, "reserve", reserve)
            result = session.advance(unit)
        owned[-1].require_received()
        total += owned[-1].consumed
        assert total.fits(limits)
        if result.complete:
            break
    return (
        plan,
        result,
        snapshot(session._store.db),
        progress_dimensions(session, session._ledger.load()),
        total,
    )


class ExactInputs:
    def __init__(self, plan, progress):
        self.plan, self.progress = plan, progress
        self.position = -1
        self.calls = None
        self.original_work = None
        self.boundaries = []
        self.pending_load = None
        self.load_operation = dict(growth_binding(ADVANCE).delegates)["load"]
        self.padding_operation = dict(growth_binding(ADVANCE).delegates)["padding"]
        self.root_keys = {item.name for item in growth_binding(ADVANCE).declaration.dimensions}

    def enter(self, session, budget, original):
        if self.calls is not None:
            assert not any(self.calls.values()), ("missing original calls", self.calls)
        self.position += 1
        assert self.position < len(self.plan), "executor exceeded independent transition plan"
        before, values = self.plan[self.position]
        received = session._ledger._control.state
        assert received == before or received is None and self.position == 0
        epoch = session._ledger.epoch
        assert epoch.work is not None and epoch._execution_active
        if self.original_work is None:
            self.original_work = epoch.work
        assert epoch.work is self.original_work
        assert not any(epoch.work.held.coordinates)
        assert epoch.work.consumed.transitions == self.position + 1
        self.boundaries.append((before.checkpoint.phase, before.checkpoint.version))
        self.calls = {tag: values[tag + "_calls"] for tag, _ in growth_binding(ADVANCE).delegates}
        # This is a per-child-call selector, not a primitive counter. Actual
        # container/opcode/row totals remain cumulative across the whole call.
        session._store.db.row_invocations = 0
        return original(session, budget)

    def inputs(self, parent, operation, _args, kwargs):
        if set(parent) != self.root_keys:
            return None
        tag = next(tag for tag, owner in growth_binding(ADVANCE).delegates if owner is operation)
        if tag == "progress":
            assert self.calls is not None and not any(self.calls.values())
            return dict(self.progress)
        assert self.calls is not None and self.calls[tag] > 0, ("unexpected original call", tag)
        self.calls[tag] -= 1
        if tag == "load":
            assert self.pending_load is None
            self.pending_load = self.plan[self.position][0]
        dimensions = self.plan[self.position][1]
        result = {
            item.name: dimensions[tag + "_" + item.name]
            for item in growth_binding(operation).declaration.dimensions
        }
        if operation.__name__ == "prepare" and "byte_budget" in result:
            assert kwargs["byte_budget"] == result["byte_budget"]
        return result

    def completed(self, operation, _args, result, _work):
        # SHA's maintained kernel reads its exact actual 64-byte argument
        # directly, bypassing the generic input hook. At terminal/done these
        # are the direct finish calls; progress padding starts after this ledger
        # is exhausted and remains nested under its own progress sample.
        if operation is self.padding_operation and self.calls is not None and self.calls["padding"]:
            assert self.plan[self.position][0].checkpoint.phase in {"terminal", "done"}
            self.calls["padding"] -= 1
        if operation is self.load_operation and self.pending_load is not None:
            assert result == self.pending_load
            self.pending_load = None


def actual_work(session):
    db = session._store.db
    work = observed_work(db, {key: 0 for key in db.metrics}, 0, 0)
    work.update(
        changed_rows=db.metrics["changed_rows"],
        indexed_containers=db.all_node_containers,
        index_python_opcode_events=db.all_node_opcodes,
    )
    return work


@pytest.mark.growth_witness(
    "riverhog-native-session-whole-quantum-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.advance",
)
@pytest.mark.parametrize("transitions", (1, 4, 16))
@pytest.mark.parametrize("origin", ORIGINS)
def test_whole_native_quantum(
    quantum_origins,
    catalog,
    journal_constructions,
    numeric_policy,
    monkeypatch,
    origin,
    transitions,
    record_property,
):
    cases, evidence = quantum_origins
    budget = JournalValidationBudget(work=JournalValidationWorkBudget(transitions=transitions))
    assert budget.rows == 64 and budget.octets == JournalValidationBudget().octets
    sample, row = quantum_sample(
        cases[origin],
        budget,
        catalog=catalog,
        journal_constructions=journal_constructions,
        numeric_policy=numeric_policy,
        monkeypatch=monkeypatch,
        origin=origin,
    )
    record_property("growth_samples", [sample])
    (evidence / f"{ORIGINS.index(origin):02d}-{transitions:02d}.json").write_text(
        json.dumps(row, indent=2) + "\n"
    )
    record_property("native_origin", origin)
    record_property("native_transitions", str(transitions))
    record_property("native_numeric_consumed", str(row["numeric_usage"]["consumed"]))


def quantum_sample(
    case,
    budget,
    *,
    catalog,
    journal_constructions,
    numeric_policy,
    monkeypatch,
    origin,
    batch=None,
):
    """Check one whole quantum against independent inputs, admission, SQL and semantic oracles."""
    transitions = budget.work.transitions
    started = time.monotonic_ns()
    with session_fixture(
        case, catalog, cold=False, journal_constructions=journal_constructions
    ) as setup:
        initial_rows = snapshot(setup._store.db)
        oracle_identifiers = OriginalIdentifiers(initial_rows)
        monkeypatch.setattr(replay_owner, "uuid4", oracle_identifiers)
        monkeypatch.setattr(graph_owner, "uuid4", oracle_identifiers)
        dimensions = parent_envelopes(setup, budget)
        plan, expected, expected_rows, progress, planned_work = exact_plan(setup, budget)
    for name in (
        "source_verifications",
        "input_publications",
        "input_atom_octets",
        "source_windows",
        "source_atoms",
        "caller_pulls",
        "source_window_octets",
        "source_atom_octets",
        "caller_pull_octets",
        "caller_slice_octets",
    ):
        dimensions[name] = sum(values[name] for _, values in plan)
    for tag, _ in growth_binding(ADVANCE).delegates:
        dimensions[tag + "_calls"] = (
            1 if tag == "progress" else sum(values[tag + "_calls"] for _, values in plan)
        )
    assert dimensions["progress_calls"] == 1
    for tag, _ in growth_binding(ADVANCE).delegates:
        assert dimensions[tag + "_calls"] <= (
            1 if tag == "progress" else (4 if tag == "padding" else 1) * len(plan)
        )
    actual_identifiers = OriginalIdentifiers(initial_rows)
    assert actual_identifiers.excluded == oracle_identifiers.excluded
    monkeypatch.setattr(replay_owner, "uuid4", actual_identifiers)
    monkeypatch.setattr(graph_owner, "uuid4", actual_identifiers)
    inputs = ExactInputs(plan, progress)
    original = JournalValidationSession._advance_unit
    monkeypatch.setattr(
        JournalValidationSession,
        "_advance_unit",
        lambda current, selected: inputs.enter(current, selected, original),
    )
    with session_fixture(
        case, catalog, cold=False, measured=True, journal_constructions=journal_constructions
    ) as current:
        with (
            SampleBatch((ADVANCE,), limit=1, numeric_policy=numeric_policy)
            if batch is None
            else nullcontext(batch)
        ) as batch:
            with session_observer(
                current,
                dimensions,
                ADVANCE,
                batch=batch,
                input_dimensions_hook=inputs.inputs,
                completed_call_hook=inputs.completed,
            ) as (observed, complete):
                measured_start = time.monotonic_ns()
                with current._store.db.observe():
                    result = current.advance(budget)
                measured_ns = time.monotonic_ns() - measured_start
            sample = complete(observed, actual_work(current))
            batch.receive(ADVANCE, sample)
        assert inputs.position + 1 == len(plan) and inputs.pending_load is None
        assert result == expected
        assert snapshot(current._store.db) == expected_rows
        assert (
            result.accepted_sha256
            == hashlib.sha256(case["raw"][: result.accepted_octets]).hexdigest()
        )
        assert current._ledger.epoch.quiescent
        assert inputs.original_work is not None and inputs.original_work.held.coordinates == (
            0,
            0,
            0,
            0,
        )
        assert inputs.original_work.consumed == planned_work
        assert actual_identifiers.values == oracle_identifiers.values
        assert len(actual_identifiers.values) == len(set(actual_identifiers.values))
        assert not set(actual_identifiers.values) & actual_identifiers.excluded
        if result.complete:
            independent = current.result()
            current.close()
            try:
                assert independent.journal_bytes == len(case["raw"])
                assert independent.journal_sha256 == hashlib.sha256(case["raw"]).hexdigest()
            finally:
                independent.close()
    projection = project_bindings((growth_binding(ADVANCE),), numeric_policy=numeric_policy)
    usage = validate_execution_samples(
        projection, growth_binding(ADVANCE).declaration.id, [sample], numeric_policy=numeric_policy
    )
    negative_bounds = []
    if origin == "long-history-terminal" and transitions == 16:
        # Keep every actual child/nested sample intact. Independently excessive
        # parent counters still fail even when all child records are unchanged.
        for metric, excess in (
            ("aggregate_returned_rows", budget.work.rows + 256 * transitions + 1),
            ("quantum_transitions", transitions + 1),
            ("calls_progress", 2),
        ):
            invalid = {**sample, "metrics": {**sample["metrics"], metric: excess}}
            with pytest.raises(ValueError, match="actual primitive observations violate"):
                validate_execution_samples(
                    projection,
                    growth_binding(ADVANCE).declaration.id,
                    [invalid],
                    numeric_policy=numeric_policy,
                )
            negative_bounds.append(metric)
    row = {
        "origin": origin,
        "transitions": transitions,
        "per_unit_rows": budget.rows,
        "source_sha256": hashlib.sha256(case["raw"]).hexdigest(),
        "regime": case["regime"],
        "actual_units": len(plan),
        "original_identifier_sequence": actual_identifiers.values,
        "identifier_sequence_matches_oracle": True,
        "measured_advance_ns": measured_ns,
        "setup_observation_validation_ns": time.monotonic_ns() - started,
        "boundaries": inputs.boundaries,
        "numeric_policy": numeric_policy_identity(numeric_policy),
        "numeric_usage": asdict(usage),
        "rejected_parent_counter_faults": negative_bounds,
        "sample": sample,
        "original_received": True,
        "semantic_oracle_equal": True,
    }
    return sample, row
