from __future__ import annotations

import hashlib
import json
import random
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from uuid import UUID

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance._online_dag import OnlineDag, StaleDagWork, install

KINDS = ("derivation", "specialization")
PHASES = (
    "initialize",
    "back-pop",
    "back-edges",
    "clear-back",
    "promote",
    "clear-incoming",
    "forward-pop",
    "forward-edges",
    "add",
    "undo",
    "clean-undo",
    "clean-seen",
    "clean-queue",
)
TABLES = {
    "dag_levels": "vertex, level",
    "dag_edges": "source, target",
    "dag_incoming": "target, source",
    "dag_seen": "operation_id, vertex",
    "dag_queue": "operation_id, direction, vertex",
    "dag_undo": "operation_id, relation, first_key, second_key, old_value",
}


def vertex(index):
    return UUID(int=index + 1).urn


def connect(path):
    db = sqlite3.connect(path, check_same_thread=False)
    db.execute("PRAGMA synchronous = FULL").close()
    return db


@contextmanager
def workspace(path=":memory:", *, kind="derivation", run="fixture-run"):
    db = connect(path)
    install(db)
    dag = OnlineDag(db, kind, run)
    dag.bind()
    try:
        yield dag
    finally:
        db.close()


def rows(dag, table, *, run=None):
    columns = TABLES[table]
    return dag.db.execute(
        f"SELECT {columns} FROM {table} WHERE kind = ? AND run_id = ? ORDER BY {columns}",
        (dag.kind, dag.run_id if run is None else run),
    ).fetchall()


def snapshot(dag, *, working=False):
    names = TABLES if working else ("dag_levels", "dag_edges", "dag_incoming")
    return {name: rows(dag, name) for name in names}


def complete(dag, budget=3):
    for _ in range(100_000):
        progress = dag.progress()
        if progress.complete:
            return dag.result(progress.operation_id)
        dag.advance(budget)
    pytest.fail("finite fixture stopped making graph progress")


def insert(dag, source, target, *, operation=None, budget=3):
    operation = operation or f"{source}-{target}"
    dag.start(operation, source, target)
    return complete(dag, budget)


def reaches(edges, source, target):
    pending, seen = [source], set()
    while pending:
        current = pending.pop()
        if current == target:
            return True
        if current not in seen:
            seen.add(current)
            pending.extend(end for start, end in edges if start == current)
    return False


def invariants(dag, expected):
    state = snapshot(dag)
    assert set(state["dag_edges"]) == expected
    levels = {key: int(value) for key, value in state["dag_levels"]}
    assert all(levels[start] <= levels[end] for start, end in expected)
    assert set(state["dag_incoming"]) == {
        (end, start) for start, end in expected if levels[start] == levels[end]
    }
    assert all(not rows(dag, name) for name in ("dag_seen", "dag_queue", "dag_undo"))


def rich_graph(dag, *, cycle=False, degree=17):
    source, target = vertex(2000), vertex(3000)
    expected = set()
    for index in range(degree):
        for edge in (
            (vertex(1000 + index), source),
            (target, vertex(4000 + index)),
        ):
            assert insert(dag, *edge) == "accepted"
            expected.add(edge)
    if cycle:
        edge = (vertex(4000), source)
        assert insert(dag, *edge) == "accepted"
        expected.add(edge)
    return source, target, expected


def until_phase(dag, phase, *, budget=3):
    for _ in range(100_000):
        progress = dag.progress()
        if progress.phase == phase:
            return progress
        assert not progress.complete, (phase, progress)
        dag.advance(budget)
    pytest.fail("finite fixture never reached its required graph phase")


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("budget", [1, 17, 64])
def test_online_parity_duplicates_rejection_and_horizontal_invariant(kind, budget):
    for seed in range(6):
        randomizer = random.Random(seed)
        with workspace(kind=kind) as dag:
            accepted = set()
            for index in range(64):
                source, target = (vertex(randomizer.randrange(12)) for _ in range(2))
                expected = source != target and not reaches(accepted, target, source)
                before = snapshot(dag)
                outcome = insert(dag, source, target, operation=f"operation-{index}", budget=budget)
                assert (outcome == "accepted") == expected
                if expected:
                    accepted.add((source, target))
                else:
                    assert snapshot(dag) == before
                    assert outcome == ("self" if source == target else "cycle")
                invariants(dag, accepted)


@pytest.mark.parametrize("kind", KINDS)
def test_high_degree_forward_and_incoming_work_keep_explicit_cursors(kind):
    with workspace(kind=kind) as dag:
        source, target, expected = rich_graph(dag, degree=129)
        dag.start("wide-operation", source, target)
        before = snapshot(dag)
        forward_cursors = []
        cleared = []
        for _ in range(100_000):
            current = dag._load().work
            if current.phase == "complete":
                break
            if current.phase == "clear-incoming":
                cleared.append(len(rows(dag, "dag_incoming")))
            if current.phase == "forward-edges" and current.active == target:
                forward_cursors.append(current.after)
            progress = dag.advance(7)
            assert progress.adjacency_visits <= 7
        else:
            pytest.fail("wide graph operation did not complete")
        assert dag.result("wide-operation") == "accepted"
        assert len(forward_cursors) > 17
        assert [value for value in forward_cursors if value] == sorted(
            value for value in forward_cursors if value
        )
        assert cleared and snapshot(dag) != before
        invariants(dag, expected | {(source, target)})


@pytest.mark.parametrize("kind", KINDS)
def test_late_cycle_is_quarantined_until_bounded_undo_restores_every_before_image(kind):
    with workspace(kind=kind) as dag:
        source, target, expected = rich_graph(dag, cycle=True, degree=129)
        before = snapshot(dag)
        dag.start("late-cycle", source, target)
        until_phase(dag, "undo", budget=7)
        assert snapshot(dag) != before
        assert rows(dag, "dag_undo")
        with pytest.raises(RuntimeError, match="quarantined"):
            dag.result("late-cycle")
        with pytest.raises(RuntimeError, match="quarantined"):
            dag.start("unrelated", vertex(8000), vertex(8001))
        while dag.progress().phase == "undo":
            before_count = len(rows(dag, "dag_undo"))
            dag.advance(7)
            assert 0 <= before_count - len(rows(dag, "dag_undo")) <= 7
        assert complete(dag, 7) == "cycle"
        assert snapshot(dag) == before
        invariants(dag, expected)
        assert insert(dag, vertex(8000), vertex(8001)) == "accepted"


_RESTART = """
import os, sqlite3, sys
from riverhog_provenance._online_dag import OnlineDag
path, kind, run, seam = sys.argv[1:]
db = sqlite3.connect(path)
db.execute("PRAGMA synchronous = FULL")
dag = OnlineDag(db, kind, run)
dag.bind()
if seam == "before":
    dag._prepare(3)
    os._exit(83)
dag.advance(3)
os._exit(84)
"""


@pytest.mark.parametrize("phase", PHASES)
@pytest.mark.parametrize("seam", ["before", "after"])
def test_process_exit_at_each_transition_reopens_the_same_actual_continuation(
    tmp_path, phase, seam
):
    path = tmp_path / "owned.sqlite3"
    with workspace(path) as dag:
        source, target, expected = rich_graph(dag, cycle=phase == "undo")
        initial = snapshot(dag)
        dag.start("restart-operation", source, target)
        until_phase(dag, phase)
        version = dag.progress().version
        dag.db.close()
        result = subprocess.run(
            [sys.executable, "-c", _RESTART, str(path), dag.kind, dag.run_id, seam],
            check=False,
        )
        assert result.returncode == (83 if seam == "before" else 84)
        dag.db = connect(path)
        reopened = OnlineDag(dag.db, dag.kind, dag.run_id)
        reopened.bind()
        assert reopened.progress().version == version + (seam == "after")
        assert reopened.progress().operation_id == "restart-operation"
        assert complete(reopened) == ("cycle" if phase == "undo" else "accepted")
        if phase == "undo":
            assert snapshot(reopened) == initial
            invariants(reopened, expected)
        else:
            invariants(reopened, expected | {(source, target)})


_CRASH_WRITE = """
import os, sqlite3, sys
from riverhog_provenance._online_dag import OnlineDag
class Crash(sqlite3.Connection):
    def execute(self, sql, parameters=()):
        cursor = super().execute(sql, parameters)
        if sql.startswith("UPDATE dag_levels SET"):
            os._exit(85)
        return cursor
db = sqlite3.connect(sys.argv[1], factory=Crash)
db.execute("PRAGMA synchronous = FULL")
dag = OnlineDag(db, "derivation", "fixture-run")
dag.advance(3)
raise AssertionError("the armed write did not execute")
"""


def test_process_exit_inside_a_unit_rolls_back_graph_header_and_before_image(tmp_path):
    path = tmp_path / "owned.sqlite3"
    with workspace(path) as dag:
        source, target, expected = rich_graph(dag)
        dag.start("interrupted-write", source, target)
        until_phase(dag, "promote")
        before, version = snapshot(dag, working=True), dag.progress().version
        dag.db.close()
        result = subprocess.run([sys.executable, "-c", _CRASH_WRITE, str(path)], check=False)
        assert result.returncode == 85
        dag.db = connect(path)
        reopened = OnlineDag(dag.db, dag.kind, dag.run_id)
        assert reopened.progress().version == version
        assert snapshot(reopened, working=True) == before
        assert complete(reopened) == "accepted"
        invariants(reopened, expected | {(source, target)})


def test_late_write_failure_rolls_back_header_and_undo_then_resumes():
    with workspace() as dag:
        source, target, expected = rich_graph(dag)
        dag.start("write-failure", source, target)
        until_phase(dag, "promote")
        before, version = snapshot(dag, working=True), dag.progress().version

        def authorize(action, table, _column, _database, _trigger):
            if action == sqlite3.SQLITE_UPDATE and table == "dag_levels":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        dag.db.set_authorizer(authorize)
        try:
            with pytest.raises(sqlite3.DatabaseError):
                dag.advance()
        finally:
            dag.db.set_authorizer(None)
        assert not dag.db.in_transaction
        assert dag.progress().version == version
        assert snapshot(dag, working=True) == before
        assert complete(dag) == "accepted"
        invariants(dag, expected | {(source, target)})


def test_competing_workers_commit_one_bounded_unit(tmp_path):
    path = tmp_path / "owned.sqlite3"
    with workspace(path) as first:
        first.start("contended", vertex(1), vertex(2))
        second_db = connect(path)
        try:
            second = OnlineDag(second_db, first.kind, first.run_id)
            plans = first._prepare(3), second._prepare(3)
            version = first.progress().version
            ready = threading.Barrier(2)

            def commit(dag, plan):
                ready.wait(timeout=10)
                return dag._commit(plan)

            with ThreadPoolExecutor(max_workers=2) as executor:
                a = executor.submit(commit, first, plans[0])
                b = executor.submit(commit, second, plans[1])
                results = a.result(), b.result()
            assert sum(result.stale for result in results) == 1
            assert first.progress().version == version + 1
            assert complete(first) == "accepted"
            invariants(first, {(vertex(1), vertex(2))})
        finally:
            second_db.close()


def test_replacement_fences_old_workers_and_reclaims_only_retired_rows():
    with workspace() as dag:
        source, target, _expected = rich_graph(dag, degree=17)
        other = OnlineDag(dag.db, "specialization", "other-graph")
        other.bind()
        assert insert(other, target, source) == "accepted"
        other_before = snapshot(other)
        dag.start("old-operation", source, target)
        dag.advance()
        old_plan = dag._prepare(3)
        replacement = dag.replace_run("replacement-run")
        with pytest.raises(StaleDagWork):
            dag._commit(old_plan)
        with pytest.raises(StaleDagWork):
            dag.advance()
        with pytest.raises(StaleDagWork):
            dag.retire()
        with pytest.raises(RuntimeError, match="already retained"):
            replacement.replace_run(dag.run_id)
        assert insert(replacement, vertex(8000), vertex(8001)) == "accepted"
        current = snapshot(replacement)
        while True:
            before = sum(len(rows(replacement, table, run=dag.run_id)) for table in TABLES)
            retired = replacement.retire(3)
            after = sum(len(rows(replacement, table, run=dag.run_id)) for table in TABLES)
            assert 0 <= before - after <= 3
            assert snapshot(replacement) == current
            assert snapshot(other) == other_before
            if not retired:
                break
        assert not any(rows(replacement, table, run=dag.run_id) for table in TABLES)


def test_owning_level_fuel_and_checkpoint_domains_are_not_sqlite_integer_widths():
    with workspace() as dag:
        large = 2**200
        source, target = vertex(1), vertex(2)
        dag.db.execute("UPDATE dag_runs SET version = ? WHERE kind = ?", (str(large), dag.kind))
        dag.db.execute(
            "INSERT INTO dag_levels VALUES (?, ?, ?, ?)",
            (dag.kind, dag.run_id, source, str(large)),
        )
        dag.db.commit()
        dag.start("wide-scalars", source, target)
        assert complete(dag, 1) == "accepted"
        assert dag.progress().version > large
        assert dict(rows(dag, "dag_levels")) == {source: str(large), target: str(large)}
        invariants(dag, {(source, target)})


@pytest.mark.parametrize(
    "change",
    [
        {"phase": "unknown"},
        {"fuel": "-1"},
        {"level": "01"},
        {"a": 1},
        {"source": []},
        {"operation_id": ""},
        {"unexpected": "field"},
    ],
)
def test_invalid_retained_checkpoint_never_advances(change):
    with workspace() as dag:
        dag.start("valid-operation", vertex(1), vertex(2))
        row = dag.db.execute("SELECT work FROM dag_runs WHERE kind = ?", (dag.kind,)).fetchone()
        work = json.loads(row[0])
        work.update(change)
        raw = json.dumps(work, sort_keys=True, separators=(",", ":"))
        dag.db.execute("UPDATE dag_runs SET work = ? WHERE kind = ?", (raw, dag.kind))
        dag.db.commit()
        before = snapshot(dag, working=True)
        with pytest.raises(RuntimeError, match="checkpoint"):
            dag.advance()
        assert snapshot(dag, working=True) == before


@pytest.mark.parametrize("budget", [0, -1, 65, True, 1.5])
def test_invalid_work_budgets_do_not_mutate_owned_state(budget):
    with workspace() as dag:
        dag.start("operation", vertex(1), vertex(2))
        before = dag.progress(), snapshot(dag, working=True)
        with pytest.raises(ValueError, match="budget"):
            dag.advance(budget)
        assert (dag.progress(), snapshot(dag, working=True)) == before


class GraphCursor(sqlite3.Cursor):
    closed = False
    observation = None

    def fetchall(self):
        result = super().fetchall()
        if self.observation is not None:
            owner, sql = self.observation
            owner.metrics["returned_rows"] += len(result)
            owner.metrics["returned_metadata_octets"] += sum(
                len(value if isinstance(value, bytes) else str(value).encode())
                for row in result
                for value in row
                if value is not None
            )
            if "FROM dag_edges AS edges" in sql or "FROM dag_incoming AS incoming" in sql:
                owner.metrics["adjacency_rows"] += len(result)
        return result

    def close(self):
        self.closed = True
        return super().close()


class GraphConnection(sqlite3.Connection):
    recording = False

    def execute(self, sql, parameters=()):
        cursor = self.cursor(factory=GraphCursor)
        if self.recording:
            cursor.observation = (self, sql)
            self.statements.append((sql, parameters, cursor))
            octets = sum(
                len(value if isinstance(value, bytes) else str(value).encode())
                for value in parameters
                if value is not None
            )
            self.metrics["bound_parameter_octets"] += octets
            if sql.startswith(("INSERT", "UPDATE", "DELETE")):
                self.metrics["written_value_octets"] += octets
        return cursor.execute(sql, parameters)

    @contextmanager
    def observe(self):
        was_in_transaction = self.in_transaction
        self.metrics = {
            "sql_statements": 0,
            "returned_rows": 0,
            "returned_metadata_octets": 0,
            "adjacency_rows": 0,
            "source_payload_octets": 0,
            "bound_parameter_octets": 0,
            "written_value_octets": 0,
            "sqlite_vm_instructions": 0,
        }
        self.statements, self.denied = [], []
        changes = self.total_changes

        def trace(_sql):
            self.metrics["sql_statements"] += 1

        def tick():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def authorize(action, table, column, _database, _trigger):
            if action == sqlite3.SQLITE_READ and not table.startswith("dag_"):
                self.denied.append((table, column))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.set_trace_callback(trace)
        self.set_progress_handler(tick, 1)
        self.set_authorizer(authorize)
        self.recording = True
        try:
            yield self.metrics
        finally:
            self.recording = False
            self.set_trace_callback(None)
            self.set_progress_handler(None, 0)
            self.set_authorizer(None)
            self.metrics["changed_rows"] = self.total_changes - changes
        assert self.in_transaction == was_in_transaction and not self.denied
        assert all(cursor.closed for _, _, cursor in self.statements)
        for sql, parameters, _cursor in self.statements:
            if sql.startswith(("SELECT", "UPDATE", "DELETE")):
                with closing(self.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as cursor:
                    plan = " ".join(row[3] for row in cursor.fetchall())
                assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan


def graph_dimensions(dag, budget, *, identities=()):
    retained = sum(
        dag.db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in (*TABLES, "dag_runs", "dag_retired", "dag_algorithm")
    )
    state = dag._load()
    scalars = [state.version, state.version + 1]
    scalars.extend(int(getattr(state.work, name)) + 1 for name in ("a", "b", "fuel", "level"))
    scalars.extend(int(level) + 1 for _, level in rows(dag, "dag_levels"))
    scalar_width = max(len(str(value).encode()) for value in scalars)
    identity_width = max(
        len(canonical_json_bytes(value))
        for value in (dag.kind, dag.run_id, state.work.operation_id or "", *identities)
    )
    return {
        "retained_rows": retained,
        "row_budget": budget,
        "metadata_row_octets": 1024 + 8 * scalar_width + 4 * identity_width,
    }


@contextmanager
def cold_graph(dag, path):
    # A fresh pager must seek the retained indexes; no warmed connection is measured.
    with closing(sqlite3.connect(path)) as stored:
        dag.db.backup(stored)
    with closing(sqlite3.connect(path, factory=GraphConnection)) as db:
        db.execute("PRAGMA synchronous = FULL").close()
        yield OnlineDag(db, dag.kind, dag.run_id)


def seeded_growth_graph(dag, count, *, cycle):
    source, target = vertex(100_000), vertex(200_000)
    edges = set()
    for index in range(count):
        edges.update(
            (
                (vertex(index), source),
                (target, vertex(300_000 + index)),
                (vertex(400_000 + index), target),
            )
        )
    edges.add((vertex(500_000), vertex(0)))
    if cycle:
        edges.add((vertex(300_000), source))
    # All levels are one in this valid DAG. Inserting target's outgoing arcs
    # before its incoming arcs produces this same state through the algorithm.
    dag.db.executemany(
        "INSERT INTO dag_levels VALUES (?, ?, ?, '1')",
        ((dag.kind, dag.run_id, key) for key in sorted({v for edge in edges for v in edge})),
    ).close()
    dag.db.executemany(
        "INSERT INTO dag_edges VALUES (?, ?, ?, ?)",
        ((dag.kind, dag.run_id, start, end) for start, end in sorted(edges)),
    ).close()
    dag.db.executemany(
        "INSERT INTO dag_incoming VALUES (?, ?, ?, ?)",
        ((dag.kind, dag.run_id, end, start) for start, end in sorted(edges)),
    ).close()
    dag.db.commit()
    invariants(dag, edges)
    return source, target, edges


def graph_sample(method, dimensions, metrics):
    binding = growth_binding(method)
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": {bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds},
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


def graph_transition_samples(tmp_path, measured_operation, *, samples):
    for phase in PHASES:
        for count, budget in [(1, 1), (17, 16), (129, 16), (1025, 64)]:
            identity = 'owned-µ"\\' * (17 if count == 1025 else 1)
            with workspace(run=identity) as dag:
                source, target, expected = seeded_growth_graph(dag, count, cycle=phase == "undo")
                initial = snapshot(dag)
                operation = "operation-" + identity
                dag.start(operation, source, target)
                until_phase(dag, phase, budget=budget)
                if count == 1025:
                    dag.db.execute("UPDATE dag_runs SET version = ?", (str(2**200),)).close()
                    dag.db.commit()
                with cold_graph(dag, tmp_path / f"{phase}-{count}.sqlite3") as actual:
                    dimensions = graph_dimensions(actual, budget)
                    if measured_operation == "_prepare":
                        before = actual.db.total_changes
                        with actual.db.observe() as metrics:
                            plan = actual._prepare(budget)
                        assert not actual.db.in_transaction and actual.db.total_changes == before
                        assert actual._load() == plan.before
                        progress = actual._commit(plan)
                    elif measured_operation == "_apply":
                        plan = actual._prepare(budget)
                        unchanged = tuple(actual.db.iterdump())
                        actual.db.execute("BEGIN IMMEDIATE").close()
                        try:
                            with actual.db.observe() as metrics:
                                assert actual._apply(plan)
                            assert actual.db.in_transaction
                        finally:
                            actual.db.rollback()
                        assert tuple(actual.db.iterdump()) == unchanged
                        progress = actual._commit(plan)
                    else:
                        assert measured_operation == "advance"
                        with actual.db.observe() as metrics:
                            progress = actual.advance(budget)
                    assert metrics["adjacency_rows"] <= budget
                    if measured_operation == "_apply":
                        assert metrics["adjacency_rows"] == 0
                    else:
                        assert progress.adjacency_visits <= metrics["adjacency_rows"]
                    samples.append(
                        graph_sample(getattr(OnlineDag, measured_operation), dimensions, metrics)
                    )
                    # Only the measured cold unit requires the file-backed FULL pager.
                    # Drain its exact committed output in memory; crash recovery is
                    # exercised independently at every phase and inside a transaction.
                    with closing(sqlite3.connect(":memory:")) as suffix_db:
                        actual.db.backup(suffix_db)
                        suffix = OnlineDag(suffix_db, actual.kind, actual.run_id)
                        outcome = complete(suffix, budget)
                        if phase == "undo":
                            assert outcome == "cycle" and snapshot(suffix) == initial
                        else:
                            assert outcome == "accepted"
                            expected.add((source, target))
                        invariants(suffix, expected)


@pytest.mark.growth_witness(
    "riverhog-online-graph-transition-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._online_dag.OnlineDag.advance",
)
def test_actual_online_graph_transition_work(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    graph_transition_samples(tmp_path, "advance", samples=samples)
    assert len(samples) == 52


@pytest.mark.growth_witness(
    "riverhog-online-graph-transition-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._online_dag.OnlineDag._prepare",
)
def test_actual_detached_online_graph_transition_plan(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    graph_transition_samples(tmp_path, "_prepare", samples=samples)
    assert len(samples) == 52


@pytest.mark.growth_witness(
    "riverhog-online-graph-transition-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._online_dag.OnlineDag._apply",
)
def test_actual_owner_transaction_online_graph_transition_application(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    graph_transition_samples(tmp_path, "_apply", samples=samples)
    assert len(samples) == 52


def retired_rows(table, count):
    for index in range(count):
        first, second = vertex(index), vertex(600_000 + index)
        if table == "dag_levels":
            yield first, "1"
        elif table in ("dag_edges", "dag_incoming"):
            yield first, second
        elif table == "dag_seen":
            yield "old-operation", first
        elif table == "dag_queue":
            yield "old-operation", "back", first
        else:
            yield "old-operation", "incoming", first, second, "1"


def graph_retirement_samples(tmp_path, measured_operation, *, samples):
    for phase, table in enumerate(TABLES):
        regimes = [
            (count, budget, admission)
            for count, budget in [(1, 1), (17, 16), (129, 16), (1025, 64)]
            for admission in ([None] if measured_operation == "retire" else [None, 8192])
        ]
        for count, budget, admission in regimes:
            with workspace() as dag:
                other = OnlineDag(dag.db, "specialization", "other-kind")
                other.bind()
                scopes = [
                    (dag.kind, dag.run_id),
                    (dag.kind, "retired"),
                    (dag.kind, "neighbor"),
                    (other.kind, other.run_id),
                ]
                for kind, run in scopes:
                    payload = list(retired_rows(table, count))
                    placeholders = ", ".join("?" for _ in range(2 + len(payload[0])))
                    dag.db.executemany(
                        f"INSERT INTO {table} VALUES ({placeholders})",
                        ((kind, run, *row) for row in payload),
                    ).close()
                dag.db.execute(
                    "INSERT INTO dag_retired VALUES (?, 'retired', ?)", (dag.kind, phase)
                ).close()
                dag.db.commit()
                with cold_graph(
                    dag, tmp_path / f"retire-{phase}-{count}-{admission}.sqlite3"
                ) as actual:
                    dimensions = graph_dimensions(actual, budget)
                    if measured_operation == "_prepare_retirement":
                        before = actual.db.total_changes
                        with actual.db.observe() as metrics:
                            batch = actual._prepare_retirement(budget, byte_budget=admission)
                        assert batch is not None
                        assert not actual.db.in_transaction and actual.db.total_changes == before
                        actual._commit(batch.plan)
                    elif measured_operation == "_apply_retirement":
                        batch = actual._prepare_retirement(budget, byte_budget=admission)
                        assert batch is not None
                        unchanged = tuple(actual.db.iterdump())
                        actual.db.execute("BEGIN IMMEDIATE").close()
                        try:
                            with actual.db.observe() as metrics:
                                actual._apply_retirement(batch)
                            assert actual.db.in_transaction
                        finally:
                            actual.db.rollback()
                        assert tuple(actual.db.iterdump()) == unchanged
                        actual._commit(batch.plan)
                    else:
                        assert measured_operation == "retire"
                        batch = None
                        with actual.db.observe() as metrics:
                            assert actual.retire(budget)
                    deleted = (
                        min(count, budget)
                        if batch is None
                        else sum(
                            statement.startswith(f"DELETE FROM {table} WHERE")
                            for statement, _parameters in batch.plan.writes
                        )
                    )
                    assert 0 < deleted <= min(count, budget)
                    if batch is not None and admission is not None:
                        assert 0 < batch.octets <= admission
                    assert metrics["adjacency_rows"] == 0
                    assert metrics["changed_rows"] <= budget + 2
                    samples.append(
                        graph_sample(getattr(OnlineDag, measured_operation), dimensions, metrics)
                    )
                    for kind, run in scopes:
                        remaining = actual.db.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE kind = ? AND run_id = ?",
                            (kind, run),
                        ).fetchone()[0]
                        assert remaining == count - (deleted if run == "retired" else 0)


@pytest.mark.growth_witness(
    "riverhog-online-graph-retirement-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._online_dag.OnlineDag.retire",
)
def test_actual_online_graph_retirement_work(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    graph_retirement_samples(tmp_path, "retire", samples=samples)
    assert len(samples) == 24


@pytest.mark.growth_witness(
    "riverhog-online-graph-retirement-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._online_dag.OnlineDag._prepare_retirement",
)
def test_actual_detached_online_graph_retirement_plan(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    graph_retirement_samples(tmp_path, "_prepare_retirement", samples=samples)
    assert len(samples) == 48


@pytest.mark.growth_witness(
    "riverhog-online-graph-retirement-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._online_dag.OnlineDag._apply_retirement",
)
def test_actual_owner_transaction_online_graph_retirement_application(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    graph_retirement_samples(tmp_path, "_apply_retirement", samples=samples)
    assert len(samples) == 48


@pytest.mark.parametrize("count", [17, 129, 1025])
def test_prepend_stream_does_not_repeat_reachability_over_the_accepted_prefix(
    count, record_property
):
    with closing(sqlite3.connect(":memory:", factory=GraphConnection)) as db:
        install(db)
        dag = OnlineDag(db, "derivation", "prepend")
        dag.bind()
        edges = set()
        visits = instructions = statements = 0
        for index in range(count):
            edge = vertex(index + 1), vertex(index)
            with db.observe() as metrics:
                dag.add(*edge)
            edges.add(edge)
            visits += metrics["adjacency_rows"]
            instructions += metrics["sqlite_vm_instructions"]
            statements += metrics["sql_statements"]
        assert visits == 0 and statements <= 100 * count
        invariants(dag, edges)
        initial = snapshot(dag)
        dag.start("terminal-cycle", vertex(0), vertex(count))
        with db.observe() as rejected:
            assert complete(dag) == "cycle"
        assert snapshot(dag) == initial
        assert rejected["adjacency_rows"] <= 3 * count
        record_property(
            "online_graph_stream_observation",
            {
                "accepted_edges": count,
                "accepted_adjacency_rows": visits,
                "accepted_sql_statements": statements,
                "accepted_sqlite_vm_instructions": instructions,
                "rejected_adjacency_rows": rejected["adjacency_rows"],
            },
        )


@pytest.mark.parametrize("family", ["sparse", "shared", "dense"])
@pytest.mark.parametrize("count", [17, 33, 65])
def test_mixed_order_stream_accounts_for_accepted_work_and_repeated_rejections(
    family, count, record_property
):
    if family == "sparse":
        expected = {(vertex(i), vertex(i + 1)) for i in range(count - 1)}
        expected.update((vertex(i), vertex(i + 2)) for i in range(count - 2))
    elif family == "shared":
        expected = {(vertex(i), vertex(count - 1)) for i in range(count - 1)}
        expected.update((vertex(0), vertex(i)) for i in range(1, count - 1))
    else:
        expected = {(vertex(i), vertex(j)) for i in range(count) for j in range(i + 1, count)}
    stream = sorted(expected)
    random.Random(317).shuffle(stream)
    with closing(sqlite3.connect(":memory:", factory=GraphConnection)) as db:
        install(db)
        dag = OnlineDag(db, "specialization", "mixed-stream")
        dag.bind()
        with db.observe() as accepted:
            for index, (source, target) in enumerate(stream):
                progress = dag.start(str(index), source, target)
                while not progress.complete:
                    progress = dag.advance(1)
                assert dag.result(str(index)) == "accepted"
        invariants(dag, expected)
        initial = snapshot(dag)
        rejections = []
        for repetition in range(3):
            with db.observe() as rejected:
                progress = dag.start(f"rejected-{repetition}", vertex(count - 1), vertex(0))
                while not progress.complete:
                    progress = dag.advance(1)
                assert dag.result(f"rejected-{repetition}") == "cycle"
            assert snapshot(dag) == initial
            assert rejected["adjacency_rows"] <= 2 * len(expected)
            rejections.append(dict(rejected))
        record_property(
            "online_graph_stream_observation",
            {
                "family": family,
                "vertices": count,
                "accepted_edges": len(expected),
                "accepted_work": dict(accepted),
                "rejected_work": rejections,
            },
        )


def graph_start_samples(tmp_path, measured_operation, *, samples):
    for count in (1, 17, 129, 1025):
        for mode in ("new", "self", "duplicate", "repeat"):
            identity = 'owned-µ"\\' * (17 if count == 1025 else 1)
            operation = "start-" + identity
            with workspace(run=identity) as dag:
                source, target, expected = seeded_growth_graph(dag, count, cycle=False)
                if mode == "self":
                    target = source
                elif mode == "duplicate":
                    source, target = vertex(0), source
                elif mode == "repeat":
                    dag.start(operation, source, target)
                with cold_graph(dag, tmp_path / f"start-{mode}-{count}.sqlite3") as actual:
                    dimensions = graph_dimensions(actual, 1, identities=(operation, source, target))
                    unchanged = tuple(actual.db.iterdump())
                    if measured_operation == "_prepare_start":
                        changes = actual.db.total_changes
                        with actual.db.observe() as metrics:
                            plan = actual._prepare_start(operation, source, target)
                        assert not actual.db.in_transaction and actual.db.total_changes == changes
                        assert actual._load() == plan.before
                    else:
                        assert measured_operation == "_apply"
                        plan = actual._prepare_start(operation, source, target)
                        actual.db.execute("BEGIN IMMEDIATE").close()
                        try:
                            with actual.db.observe() as metrics:
                                assert actual._apply(plan)
                            assert actual.db.in_transaction
                        finally:
                            actual.db.rollback()
                    assert tuple(actual.db.iterdump()) == unchanged
                    assert metrics["adjacency_rows"] == 0
                    samples.append(
                        graph_sample(getattr(OnlineDag, measured_operation), dimensions, metrics)
                    )
                    actual.start(operation, source, target)
                    with closing(sqlite3.connect(":memory:")) as suffix_db:
                        actual.db.backup(suffix_db)
                        suffix = OnlineDag(suffix_db, actual.kind, actual.run_id)
                        outcome = complete(suffix, 64)
                        if mode == "self":
                            assert outcome == "self"
                        else:
                            assert outcome == "accepted"
                            expected.add((source, target))
                        invariants(suffix, expected)


@pytest.mark.growth_witness(
    "riverhog-online-graph-start-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._online_dag.OnlineDag._prepare_start",
)
def test_actual_detached_online_graph_start_plan(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    graph_start_samples(tmp_path, "_prepare_start", samples=samples)
    assert len(samples) == 16


@pytest.mark.growth_witness(
    "riverhog-online-graph-start-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._online_dag.OnlineDag._apply",
)
def test_actual_owner_transaction_online_graph_start_application(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    graph_start_samples(tmp_path, "_apply", samples=samples)
    assert len(samples) == 16


def graph_replacement_samples(tmp_path, measured_operation, *, samples):
    for kind in KINDS:
        for pending in (False, True):
            for count in (0, 1, 17, 129, 1025):
                identity = 'owned-µ"\\' * (17 if count == 1025 else 1)
                next_run = "replacement-" + identity
                with workspace(kind=kind, run=identity) as dag:
                    source, target, _expected = seeded_growth_graph(dag, count, cycle=False)
                    other_kind = "specialization" if kind == "derivation" else "derivation"
                    other = OnlineDag(dag.db, other_kind, "other-run")
                    other.bind()
                    assert insert(other, vertex(8000), vertex(8001)) == "accepted"
                    other_before = snapshot(other, working=True)
                    if pending:
                        dag.start("pending-" + identity, source, target)
                        dag.advance(1)
                    old_before = snapshot(dag, working=True)
                    with cold_graph(
                        dag, tmp_path / f"replacement-{kind}-{pending}-{count}.sqlite3"
                    ) as actual:
                        dimensions = graph_dimensions(actual, 1, identities=(next_run,))
                        unchanged = tuple(actual.db.iterdump())
                        if measured_operation == "_prepare_replacement":
                            changes = actual.db.total_changes
                            with actual.db.observe() as metrics:
                                batch = actual._prepare_replacement(next_run)
                            assert (
                                not actual.db.in_transaction and actual.db.total_changes == changes
                            )
                            assert actual._load() == batch.before
                        else:
                            assert measured_operation == "_apply_replacement"
                            batch = actual._prepare_replacement(next_run)
                            actual.db.execute("BEGIN IMMEDIATE").close()
                            try:
                                with actual.db.observe() as metrics:
                                    prepared = actual._apply_replacement(batch)
                                assert actual.db.in_transaction and prepared.run_id == next_run
                                assert prepared.db is actual.db
                            finally:
                                actual.db.rollback()
                        assert tuple(actual.db.iterdump()) == unchanged
                        assert metrics["adjacency_rows"] == 0
                        samples.append(
                            graph_sample(
                                getattr(OnlineDag, measured_operation), dimensions, metrics
                            )
                        )
                        replacement = actual.replace_run(next_run)
                        with pytest.raises(StaleDagWork):
                            actual.advance(1)
                        assert snapshot(actual, working=True) == old_before
                        assert not any(snapshot(replacement, working=True).values())
                        foreign = OnlineDag(actual.db, other_kind, "other-run")
                        assert snapshot(foreign, working=True) == other_before
                        assert insert(replacement, vertex(8002), vertex(8003)) == "accepted"
                        invariants(replacement, {(vertex(8002), vertex(8003))})


@pytest.mark.growth_witness(
    "riverhog-online-graph-replacement-plan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._online_dag.OnlineDag._prepare_replacement",
)
def test_actual_detached_online_graph_replacement_plan(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    graph_replacement_samples(tmp_path, "_prepare_replacement", samples=samples)
    assert len(samples) == 20


@pytest.mark.growth_witness(
    "riverhog-online-graph-replacement-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._online_dag.OnlineDag._apply_replacement",
)
def test_actual_owner_transaction_online_graph_replacement_application(tmp_path, record_property):
    samples = []
    record_property("growth_samples", samples)
    graph_replacement_samples(tmp_path, "_apply_replacement", samples=samples)
    assert len(samples) == 20
