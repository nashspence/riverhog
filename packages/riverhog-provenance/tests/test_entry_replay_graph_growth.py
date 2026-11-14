from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing, contextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import _entry_replay as replay_owner
from riverhog_provenance import (
    assertion,
    encode_entry,
    new_id,
    parse_journal,
    reference,
    validate_journal,
)
from riverhog_provenance import journal as journal_owner
from riverhog_provenance._entry_replay import EntryReplay, ReplayBudget
from riverhog_provenance._online_dag import _RETIRE, OnlineDag
from riverhog_provenance._session_checkpoint import StaleValidationCheckpoint, ValidationLedger
from riverhog_provenance.journal_store import JournalStore
from test_child_admission_growth import octets
from test_entry_replay import owned_summary
from test_entry_replay_child_growth import ChildConnection
from test_entry_replay_growth import admission_histories as admission_histories
from test_entry_replay_growth import observe_replay, owner
from test_entry_replay_publication_growth import restore
from test_generic_semantics import identity
from test_online_dag import graph_dimensions
from test_semantic_edge import observed_work
from test_semantic_owner import snapshot

GRAPH_CASES = (
    *((count, 1, 2, 64) for count in (0, 1, 17, 64, 129, 1025)),
    (17, 65537, 2, 64),
    (0, 1, 2**200, 16384),
)


class GraphConnection(ChildConnection):
    def set_authorizer(self, callback):
        if callback is None:
            return super().set_authorizer(None)

        def authorize(action, table, column, database, trigger):
            if action == sqlite3.SQLITE_READ and (
                (table == "dag_levels" and column == "level")
                or (table == "dag_undo" and column == "old_value")
            ):
                return sqlite3.SQLITE_DENY
            return callback(action, table, column, database, trigger)

        return super().set_authorizer(authorize)

    @contextmanager
    def observe(self):
        with super().observe() as metrics:
            metrics["admitted_graph_octets"] = 0
            yield metrics


def initialize_graph_cache(store):
    with closing(store.db.execute("SELECT kind,run_id FROM dag_runs")) as cursor:
        store._dag_runs = dict(cursor.fetchall())


@pytest.fixture(scope="session")
def graph_histories(tmp_path_factory, catalog, admission_histories):
    root = tmp_path_factory.mktemp("entry-graph-histories")
    saved = {}

    def build(specification):
        if specification in saved:
            return saved[specification]
        count, payload, epoch, width = specification
        case = dict(admission_histories((0, 1, payload, "init", epoch, width)))
        document = deepcopy(case["candidate"].document)
        values = document["body"]["assertions"]
        first = values["states"][0]
        states = [first]
        for number in range(count):
            value = deepcopy(first)
            value["id"], value["assertion_id"] = identity(10000 + number), identity(20000 + number)
            states.append(value)
        values["states"] = states
        relations = values.setdefault("relations", []) if count else []
        # Large retirement runs keep the same key cardinality without requiring
        # a long dependency chain to construct their accepted native graph.
        pairs = (
            ((first, value) for value in states[1:])
            if count >= 64
            else zip(states, states[1:], strict=False)
        )
        for number, (left, right) in enumerate(pairs):
            relations.extend(
                [
                    assertion(
                        "derivation",
                        case["who"],
                        assertion_id=identity(100000 + 2 * number),
                        used_state=reference(left["id"], "state"),
                        generated_state=reference(right["id"], "state"),
                        kind="copy",
                    ),
                    assertion(
                        "specialization",
                        case["who"],
                        assertion_id=identity(100001 + 2 * number),
                        specific=reference(left["id"], "state"),
                        general=reference(right["id"], "state"),
                    ),
                ]
            )
        initial_raw = encode_entry(document, catalog=catalog)
        initial_frame = parse_journal(initial_raw)[0]
        replacement = deepcopy(values["artifacts"][0])
        target = {"entry": initial_frame.reference, "assertion_id": replacement["assertion_id"]}
        replacement["assertion_id"] = new_id()
        correction = journal_owner._entry(
            journal_id=document["journal_id"],
            recorder_id=case["who"],
            kind="correction",
            body={
                "reason": "Replace documentary assertion.",
                "retracts": [target],
                "assertions": {"artifacts": [replacement]},
            },
            sequence=1,
            previous=initial_frame.reference,
        )
        correction_raw = encode_entry(correction, catalog=catalog)
        raw = initial_raw + correction_raw
        unit_limit = 1_000_000

        def progress(stage, units, state=None):
            value = {"edge_count": count, "stage": stage, "units": units}
            if state is not None:
                value.update(phase=state.checkpoint.phase, version=state.checkpoint.version)
            (root / "native-trajectory-progress.json").write_text(json.dumps(value) + "\n")

        snapshots = []
        with closing(JournalStore()) as store:
            current = restore(store, case, catalog)
            before = current.admit(initial_frame)
            progress("initial", 0, before)
            for units in range(unit_limit):
                if before.checkpoint.phase == "frame":
                    break
                changed = current.advance(ReplayBudget(rows=64))
                assert changed.checkpoint.version == before.checkpoint.version + 1
                before = changed
                if units % 4096 == 0:
                    progress("initial", units + 1, before)
            else:
                pytest.fail("finite native initial graph trajectory did not complete")
            assert before == current.ledger.load()
            before = current.admit(parse_journal(correction_raw)[0])
            progress("correction", 0, before)
            seen = set()
            for units in range(unit_limit):
                phase = before.checkpoint.phase
                if phase == "frame":
                    break
                if phase in {
                    kind + "-" + stage
                    for kind in ("derivation", "specialization")
                    for stage in ("replace", "retire")
                }:
                    kind, stage = phase.split("-", 1)
                    retired = store.db.execute(
                        "SELECT run_id,phase FROM dag_retired WHERE kind=? ORDER BY run_id LIMIT 1",
                        (kind,),
                    ).fetchone()
                    retained = 0
                    if retired is not None:
                        table = _RETIRE[retired[1]][0]
                        retained = store.db.execute(
                            "SELECT count(*) FROM " + table + " WHERE kind=? AND run_id=?",
                            (kind, retired[0]),
                        ).fetchone()[0]
                    bucket = (
                        "empty"
                        if not retained
                        else "one"
                        if retained == 1
                        else "short"
                        if retained < 64
                        else "exact"
                        if retained == 64
                        else "bulk"
                    )
                    key = kind, stage, None if retired is None else retired[1], bucket
                    if key not in seen:
                        seen.add(key)
                        path = root / f"graph-{len(saved)}-checkpoint-{len(seen)}.sqlite3"
                        with closing(sqlite3.connect(path)) as db:
                            store.db.backup(db)
                        snapshots.append(
                            {
                                **case,
                                "path": path,
                                "before": before,
                                "key": key,
                                "specification": specification,
                                "raw": raw,
                            }
                        )
                changed = current.advance(ReplayBudget(rows=16))
                assert changed.checkpoint.version == before.checkpoint.version + 1
                before = changed
                if units % 4096 == 0:
                    progress("correction", units + 1, before)
            else:
                pytest.fail("finite native correction graph trajectory did not complete")
            assert before == current.ledger.load()
            progress("public-validation", units, before)
            with validate_journal(raw, catalog=catalog) as expected:
                with owned_summary(store, current.prefix.finish()) as actual:
                    assert actual.materialize() == expected.materialize()
                    assert actual.journal_sha256 == hashlib.sha256(raw).hexdigest()
            progress("complete", units, before)
        saved[specification] = snapshots
        return snapshots

    return build


def graph_inputs(current, case, budget):
    before = case["before"]
    original = snapshot(current.db)
    kind, stage = before.checkpoint.phase.split("-", 1)
    run = current.db.execute("SELECT run_id FROM dag_runs WHERE kind=?", (kind,)).fetchone()[0]
    graph = OnlineDag(current.db, kind, run)
    state = graph._load()
    retired = current.db.execute(
        "SELECT run_id,phase FROM dag_retired WHERE kind=? ORDER BY run_id LIMIT 1", (kind,)
    ).fetchone()
    replacement = stage == "replace"
    batch = not replacement and retired is not None
    width = 16384 + 2 * octets(
        (before, state.work.encoded(), str(state.version + 1), graph.run_id, "f" * 32)
    )
    dimensions = {
        "retained_rows": sum(len(rows) for rows in original.values()),
        "metadata_row_octets": width,
        "retirement_byte_budget": 0 if replacement else current._remaining(),
        "digest_calls": 1 + int(batch),
    }
    for tag, operation in growth_binding(EntryReplay._graph_step).delegates:
        if tag in {"fence", "publication"}:
            active = True
            inputs = {
                "retained_rows": sum(
                    len(original[name])
                    for name in ("journal_validation", "journal_prefix", "frames")
                ),
                "metadata_row_octets": width,
            }
        else:
            active = (
                replacement
                if tag.startswith("replacement_")
                else not replacement
                if tag == "retirement_plan"
                else batch
            )
            inputs = graph_dimensions(
                graph, 1 if replacement else budget.rows, identities=("f" * 32,)
            )
        dimensions[tag + "_calls"] = int(active)
        for item in growth_binding(operation).declaration.dimensions:
            dimensions[tag + "_" + item.name] = inputs[item.name]
    return dimensions, original


@contextmanager
def graph_observer(current, dimensions):
    db = current.db
    binding = growth_binding(EntryReplay._graph_step)
    operations = dict(binding.delegates)
    owners = {
        operation: ValidationLedger
        if operation.__qualname__.startswith("ValidationLedger.")
        else OnlineDag
        for operation in operations.values()
    }

    def inputs(_parent, operation, args, kwargs):
        tag = next(tag for tag, child in binding.delegates if child is operation)
        result = {
            item.name: dimensions[tag + "_" + item.name]
            for item in growth_binding(operation).declaration.dimensions
        }
        if tag == "retirement_plan":
            assert (
                args[1] == result["row_budget"]
                and kwargs["byte_budget"] == dimensions["retirement_byte_budget"]
            )
        return result

    claim = current._claim

    def admitted(octets):
        if db.recording:
            db.metrics["admitted_graph_octets"] += octets
        return claim(octets)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(current, "_claim", admitted)
        with observe_replay(
            db,
            current.catalog,
            dimensions,
            root_operation=EntryReplay._graph_step,
            input_dimensions=inputs,
            operation_owners=owners,
        ) as observed:
            yield observed


@contextmanager
def graph_fixture(case, catalog, *, rows, tight=False, measured=False):
    with closing(JournalStore()) as store:
        with closing(sqlite3.connect(case["path"])) as db:
            db.backup(store.db)
        if measured:
            path = store._storage.directory / "snapshot.sqlite3"
            store.db.close()
            store._storage.connection = sqlite3.connect(path, factory=GraphConnection)
            store.db.execute("PRAGMA synchronous=FULL").close()
            store.db.execute("PRAGMA cache_size=-64").close()
        initialize_graph_cache(store)
        current = owner(store, catalog, case["source_binding"], case["epoch"])
        budget = ReplayBudget(rows=rows)
        current._start(case["before"], budget)
        if tight:
            budget = replace(budget, octets=current._spent + 16384)
            current._start(case["before"], budget)
        yield current, budget


def graph_samples(catalog, build, *, samples, specifications=GRAPH_CASES):
    binding = growth_binding(EntryReplay._graph_step)
    projection = project_bindings((binding,))
    for specification in specifications:
        for case in build(specification):
            kind, stage, _retired, bucket = case["key"]
            budgets = (
                (1,)
                if stage == "replace"
                else (1, 16, 64)
                if bucket in {"bulk", "exact"}
                else (16,)
            )
            variants = [(rows, False) for rows in budgets]
            if stage == "retire" and bucket == "bulk" and specification[0] == 129:
                variants.append((64, True))
            for rows, tight in variants:
                with graph_fixture(case, catalog, rows=rows, tight=tight) as (oracle, _budget):
                    with pytest.MonkeyPatch.context() as patch:
                        patch.setattr(replay_owner, "uuid4", lambda: SimpleNamespace(hex="f" * 32))
                        expected = oracle._graph_step(case["before"])
                    expected_rows, expected_runs = snapshot(oracle.db), dict(oracle.store._dag_runs)
                with graph_fixture(case, catalog, rows=rows, tight=tight) as (setup, budget):
                    dimensions, original = graph_inputs(setup, case, budget)
                with graph_fixture(case, catalog, rows=rows, tight=tight, measured=True) as (
                    current,
                    _budget,
                ):
                    with pytest.MonkeyPatch.context() as patch:
                        patch.setattr(replay_owner, "uuid4", lambda: SimpleNamespace(hex="f" * 32))
                        with graph_observer(current, dimensions) as (observed, complete):
                            with current.db.observe():
                                result = current._graph_step(case["before"])
                    primitive = observed_work(
                        current.db, {key: 0 for key in current.db.metrics}, 0, 0
                    )
                    primitive["changed_rows"] = current.db.metrics["changed_rows"]
                    sample = complete(observed, primitive)
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
                    assert result == expected == current.ledger.load()
                    assert not current.db.in_transaction and snapshot(current.db) == expected_rows
                    assert current.store._dag_runs == expected_runs
                    assert original["journal_prefix"] == expected_rows["journal_prefix"]
                    for tag, children in sample["delegates"].items():
                        assert len(children) == dimensions[tag + "_calls"]
    assert samples


@pytest.mark.growth_witness(
    "riverhog-entry-replay-graph-step-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_replay.EntryReplay._graph_step",
)
def test_actual_cold_graph_replacement_and_retirement(catalog, graph_histories, record_property):
    samples = []
    record_property("growth_samples", samples)
    graph_samples(catalog, graph_histories, samples=samples)


@pytest.mark.parametrize("stage", ("replace", "retire"))
def test_parent_publication_failure_rolls_back_graph_and_cached_run(
    catalog, graph_histories, monkeypatch, stage
):
    case = next(case for case in graph_histories(GRAPH_CASES[1]) if case["key"][1] == stage)
    with graph_fixture(case, catalog, rows=1) as (current, _budget):
        before, cached = snapshot(current.db), dict(current.store._dag_runs)
        publication = current.ledger._publish

        def fail(*args, **kwargs):
            publication(*args, **kwargs)
            raise StaleValidationCheckpoint("replaced after graph mutation")

        with monkeypatch.context() as patch:
            patch.setattr(current.ledger, "_publish", fail)
            with pytest.raises(StaleValidationCheckpoint, match="replaced after graph mutation"):
                current._graph_step(case["before"])
        assert not current.db.in_transaction and snapshot(current.db) == before
        assert current.store._dag_runs == cached
        current._graph_step(case["before"])
        assert current.ledger.load().checkpoint.version == case["before"].checkpoint.version + 1
