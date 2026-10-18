from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import subprocess
import sys
from contextlib import closing, contextmanager
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    append_assertions,
    assertion,
    new_id,
    reference,
    validate_journal,
)
from riverhog_provenance._journal_prefix import PrefixState
from riverhog_provenance._online_dag import OnlineDag
from riverhog_provenance._semantic_edge import (
    EdgeState,
    SemanticEdge,
    StaleSemanticEdge,
)
from riverhog_provenance._semantic_source import SemanticSource
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.journal import JournalFrame
from riverhog_provenance.journal_store import JournalStore
from sha256_continuation import CheckpointSHA256


def identity(ordinal):
    return f"urn:uuid:00000000-0000-4000-8000-{ordinal:012x}"


def connect(path, **options):
    db = sqlite3.connect(path, **options)
    db.execute("PRAGMA synchronous=FULL").close()
    db.execute("PRAGMA cache_size=-64").close()
    return db


def descriptor(**options):
    return SemanticSource(
        **{
            "sha256": "a" * 64,
            "sequence": "1",
            "journal_id": identity(2),
            "catalog_sha256": "b" * 64,
            "require_profiles": False,
            **options,
        }
    )


def create(path, kind="derivation"):
    store = JournalStore()
    try:
        with closing(connect(path)) as db:
            store.db.backup(db)
    finally:
        store.close()
    prior = JournalFrame(canonical_json_bytes({"id": identity(1), "sequence": "0"}))
    prefix = PrefixState(
        1,
        len(prior.encoded),
        identity(2),
        canonical_json_bytes(prior.reference).decode(),
        CheckpointSHA256(prior.encoded).export_state(),
        False,
    )
    with closing(connect(path)) as db:
        db.execute(
            "INSERT INTO journal_prefix VALUES (1,?,?,?)",
            ("prefix-attempt", b"exact-source-and-modes", prefix.to_json()),
        )
        db.execute(
            "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
            (identity(10), identity(11), "relations", kind, None, None, "1", b"{}", 1),
        )
        db.execute("INSERT INTO changes VALUES (?)", (identity(10),))
        db.commit()


def work(db, **options):
    kind = options.get("kind", "derivation")
    run = db.execute("SELECT run_id FROM dag_runs WHERE kind=?", (kind,)).fetchone()[0]
    return SemanticEdge(
        db,
        **{
            "kind": kind,
            "run_id": run,
            "attempt_id": "semantic-attempt",
            "source_binding": descriptor(),
            "assertion_id": identity(10),
            "object_id": identity(11),
            "source": identity(2000),
            "target": identity(3000),
            **options,
        },
    )


def rich(db, *, degree=17, cycle=False):
    current = work(db)
    dag = current.checker
    edges = set()
    for ordinal in range(degree):
        for edge in (
            (identity(1000 + ordinal), current.source),
            (current.target, identity(4000 + ordinal)),
        ):
            dag.add(*edge)
            edges.add(edge)
    if cycle:
        edge = (identity(4000), current.source)
        dag.add(*edge)
        edges.add(edge)
    return edges


def content(db):
    return {
        table: db.execute("SELECT * FROM " + table + " ORDER BY rowid").fetchall()
        for table in (
            "journal_prefix",
            "journal_semantic_edges",
            "dag_runs",
            "dag_levels",
            "dag_edges",
            "dag_incoming",
            "dag_seen",
            "dag_queue",
            "dag_undo",
        )
    }


def finish(current, budget=3):
    for _ in range(100000):
        if current.advance(row_budget=budget).done:
            return
    pytest.fail("finite semantic graph work stopped progressing")


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
@pytest.mark.parametrize("cycle", [False, True])
def test_reopen_every_unit_preserves_graph_progress_and_exact_prefix(tmp_path, kind, cycle):
    path = tmp_path / "work.sqlite3"
    create(path, kind)
    with closing(connect(path)) as db:
        # Both graph kinds use the same native topology transitions.
        current = work(db, kind=kind)
        dag = current.checker
        expected = set()
        for ordinal in range(17):
            for edge in (
                (identity(1000 + ordinal), current.source),
                (current.target, identity(4000 + ordinal)),
            ):
                dag.add(*edge)
                expected.add(edge)
        if cycle:
            dag.add(identity(4000), current.source)
            expected.add((identity(4000), current.source))
        current.initialize()
        prefix = content(db)["journal_prefix"]
    phases = set()
    for _ in range(100000):
        with closing(connect(path)) as db:
            current = work(db, kind=kind)
            before = current.load()
            phases.add(current.checker.progress().phase)
            after = current.advance(row_budget=1)
            assert after.version == before.version + 1
            assert content(db)["journal_prefix"] == prefix
            if after.done:
                if cycle:
                    with pytest.raises(ProvenanceValidationError, match="graph contains a cycle"):
                        current.result()
                else:
                    current.result()
                    expected.add((current.source, current.target))
                observed = set(
                    db.execute(
                        "SELECT source,target FROM dag_edges WHERE kind=?", (kind,)
                    ).fetchall()
                )
                assert observed == expected
                assert not any(
                    db.execute("SELECT 1 FROM " + name).fetchall()
                    for name in ("dag_seen", "dag_queue", "dag_undo")
                )
                assert current.advance().done
                break
    else:
        pytest.fail("reopened work did not finish")
    assert {"initialize", "back-pop"} <= phases
    if cycle:
        assert "undo" in phases
    else:
        assert {"add", "clean-undo"} <= phases


@pytest.mark.parametrize(
    "field",
    [
        "attempt_id",
        "sha256",
        "catalog_sha256",
        "require_profiles",
        "sequence",
        "journal_id",
        "source",
        "target",
        "object_id",
    ],
)
def test_restoration_requires_exact_source_inputs(tmp_path, field):
    path = tmp_path / "work.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db)
        current.initialize()
        current.advance()
        before = content(db)
        options = {}
        if field in {"sha256", "catalog_sha256", "require_profiles", "sequence", "journal_id"}:
            value = {
                "sha256": "c" * 64,
                "catalog_sha256": "c" * 64,
                "require_profiles": True,
                "sequence": "2",
                "journal_id": identity(3),
            }[field]
            options["source_binding"] = replace(descriptor(), **{field: value})
        else:
            options[field] = "another-attempt" if field == "attempt_id" else identity(9999)
        with pytest.raises(StaleSemanticEdge):
            work(db, **options).load()
        assert content(db) == before


@pytest.mark.parametrize("field", ["attempt_id", "binding", "checkpoint"])
def test_prefix_replacement_fences_prepared_graph_writes(tmp_path, field):
    path = tmp_path / "work.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db)
        current.initialize()
        current.advance()
        batch = current.prepare(row_budget=1)
        old = db.execute("SELECT " + field + " FROM journal_prefix").fetchone()[0]
        value = old + b"-changed" if isinstance(old, bytes) else old + "-changed"
        if field == "checkpoint":
            value = replace(PrefixState.from_json(old), matched_anchor=True).to_json()
        db.execute("UPDATE journal_prefix SET " + field + "=?", (value,))
        db.commit()
        before = content(db)
        with pytest.raises(StaleSemanticEdge):
            current.commit(batch)
        assert content(db) == before


def test_competing_proposals_and_finished_workers_cannot_advance_replacements(tmp_path):
    path = tmp_path / "work.sqlite3"
    create(path)
    with closing(connect(path)) as one, closing(connect(path)) as two:
        first, second = work(one), work(two)
        first.initialize()
        a, b = first.prepare(), second.prepare()
        first.commit(a)
        before = content(one)
        with pytest.raises(StaleSemanticEdge):
            second.commit(b)
        assert content(one) == before
        finish(first)
        old = first.prepare()
        work(two, attempt_id="replacement").initialize()
        with pytest.raises(StaleSemanticEdge):
            first.commit(old)


def test_graph_statement_failure_rolls_back_graph_and_semantic_checkpoints(tmp_path):
    path = tmp_path / "work.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db)
        current.initialize()
        current.advance()
        batch = current.prepare()
        before = content(db)
        db.execute(
            "CREATE TEMP TRIGGER reject_mutation BEFORE INSERT ON dag_levels "
            "BEGIN SELECT RAISE(ABORT,'injected graph write failure'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="injected graph write failure"):
            current.commit(batch)
        assert not db.in_transaction
        assert content(db) == before
        db.execute("DROP TRIGGER reject_mutation")
        current.commit(batch)
        finish(current)
        current.result()


@pytest.mark.parametrize("completed", [False, True])
def test_initialization_never_resets_an_existing_attempt(tmp_path, completed):
    path = tmp_path / "work.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db)
        current.initialize()
        if completed:
            finish(current)
        before = content(db)
        with pytest.raises(StaleSemanticEdge):
            current.initialize()
        if not completed:
            with pytest.raises(StaleSemanticEdge):
                work(db, attempt_id="replacement").initialize()
        assert content(db) == before


@pytest.mark.parametrize("field", ["active", "type", "object_id", "missing"])
def test_missing_or_changed_semantic_subject_is_never_reset(tmp_path, field):
    path = tmp_path / "work.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db)
        current.initialize()
        batch = current.prepare()
        if field == "missing":
            db.execute("DELETE FROM changes")
        else:
            db.execute(
                "UPDATE assertions SET " + field + "=?",
                ({"active": 0, "type": "agent", "object_id": identity(999)}[field],),
            )
        db.commit()
        before = content(db)
        with pytest.raises(StaleSemanticEdge):
            current.commit(batch)
        assert content(db) == before


def test_native_wrapper_drives_fenced_units_without_the_whole_edge_driver(
    journal, who, catalog, monkeypatch
):
    with validate_journal(journal, catalog=catalog) as summary:
        old = summary.graph["artifacts"][0]
        new = copy.deepcopy(old)
        new["id"], new["assertion_id"] = new_id(), new_id()
        relation = assertion(
            "specialization",
            who,
            specific=reference(new["id"], "artifact"),
            general=reference(old["id"], "artifact"),
        )
    seen = []
    original = SemanticEdge.advance

    def advance(self, **options):
        prefix = self.db.execute("SELECT * FROM journal_prefix").fetchall()
        state = original(self, row_budget=1)
        assert self.db.execute("SELECT * FROM journal_prefix").fetchall() == prefix
        assert not self.db.in_transaction
        seen.append(state.phase)
        return state

    def unbounded(*args, **options):
        pytest.fail("native semantic insertion used the whole edge driver")

    monkeypatch.setattr(SemanticEdge, "advance", advance)
    monkeypatch.setattr(OnlineDag, "add", unbounded)
    raw = append_assertions(
        journal,
        {"artifacts": [new], "relations": [relation]},
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    with validate_journal(raw, catalog=catalog) as summary:
        assert summary.journal_sha256 == hashlib.sha256(raw).hexdigest()
    assert {"graph", "done"} <= set(seen)


def position(current, phase, *, budget=1):
    if phase == "start":
        return
    for _ in range(100000):
        state = current.load()
        if phase == "done" and state.done:
            return
        if state.phase == "graph" and current.checker.progress().phase == phase:
            return
        assert not state.done
        current.advance(row_budget=budget)
    pytest.fail("finite fixture never reached its required semantic graph phase")


CRASH_PROGRAM = r"""
import os,sqlite3,sys
from riverhog_provenance._semantic_edge import SemanticEdge
from riverhog_provenance._semantic_source import SemanticSource
class CrashConnection(sqlite3.Connection):
    armed=False
    def execute(self,sql,parameters=()):
        result=super().execute(sql,parameters)
        header=sys.argv[2]=='header' and sql.startswith('UPDATE journal_semantic_edges')
        graph=sys.argv[2]=='graph' and sql.startswith('UPDATE dag_runs')
        mutation=sys.argv[2]=='mutation' and sql.startswith(
            ('INSERT INTO dag_levels','INSERT INTO dag_seen','DELETE FROM dag_undo'))
        if self.armed and (header or graph or mutation): os._exit(73)
        return result
    def commit(self):
        if self.armed and sys.argv[2]=='before-commit': os._exit(73)
        super().commit()
        if self.armed and sys.argv[2]=='after-commit': os._exit(73)
db=sqlite3.connect(sys.argv[1],factory=CrashConnection)
db.execute('PRAGMA synchronous=FULL').close()
run=db.execute("SELECT run_id FROM dag_runs WHERE kind='derivation'").fetchone()[0]
def identity(n): return f'urn:uuid:00000000-0000-4000-8000-{n:012x}'
current=SemanticEdge(db,kind='derivation',run_id=run,attempt_id='semantic-attempt',
    source_binding=SemanticSource('a'*64,'1',identity(2),'b'*64,False),
    assertion_id=identity(10),object_id=identity(11),source=identity(2000),target=identity(3000))
batch=current.prepare(row_budget=1)
if sys.argv[2]=='prepared': os._exit(73)
db.armed=True
current.commit(batch)
raise SystemExit(99)
"""


@pytest.mark.parametrize(
    "phase,seam",
    [
        (phase, seam)
        for phase in ("start", "initialize", "add", "undo", "clean-undo", "complete")
        for seam in ("prepared", "header", "before-commit", "after-commit")
    ]
    + [(phase, "graph") for phase in ("start", "initialize", "add", "undo", "clean-undo")]
    + [("initialize", "mutation"), ("undo", "mutation")],
)
def test_real_process_death_keeps_graph_and_semantic_progress_atomic(tmp_path, phase, seam):
    path = tmp_path / "crash.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        rich(db, cycle=phase == "undo")
        current = work(db)
        current.initialize()
        position(current, phase)
        before = content(db)
        with closing(connect(tmp_path / "expected.sqlite3")) as expected:
            db.backup(expected)
            proposed = work(expected)
            proposed.advance(row_budget=1)
            after = content(expected)
    result = subprocess.run(
        [sys.executable, "-c", CRASH_PROGRAM, str(path), seam],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 73, result.stderr
    with closing(connect(path)) as db:
        assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert content(db) == (after if seam == "after-commit" else before)
        resumed = work(db)
        finish(resumed)
        if phase == "undo":
            with pytest.raises(ProvenanceValidationError, match="graph contains a cycle"):
                resumed.result()
        else:
            resumed.result()
        assert content(db)["journal_prefix"] == before["journal_prefix"]


@pytest.mark.parametrize("damage", ["missing", "algorithm", "phase", "version", "graph"])
def test_corrupt_or_missing_continuation_fails_without_reset(tmp_path, damage):
    path = tmp_path / "work.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db)
        current.initialize()
        current.advance()
        if damage == "missing":
            db.execute("DELETE FROM journal_semantic_edges")
        elif damage == "algorithm":
            db.execute("UPDATE journal_semantic_edges SET algorithm='unknown'")
        elif damage == "graph":
            db.execute(
                "UPDATE dag_runs SET work=? WHERE kind='derivation'",
                (replace(current.checker._load().work, operation_id="another").encoded(),),
            )
        else:
            state = json.loads(current.load().to_json())
            state[damage] = "unknown" if damage == "phase" else "01"
            db.execute(
                "UPDATE journal_semantic_edges SET checkpoint=?",
                (canonical_json_bytes(state).decode(),),
            )
        db.commit()
        before = content(db)
        with pytest.raises(RuntimeError):
            current.load()
        assert content(db) == before


def test_semantic_versions_remain_decimal_text(tmp_path):
    path = tmp_path / "work.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        current = work(db)
        current.initialize()
        large = 2**200
        db.execute(
            "UPDATE journal_semantic_edges SET checkpoint=?", (EdgeState(version=large).to_json(),)
        )
        db.commit()
        assert current.advance().version == large + 1


class WorkCursor(sqlite3.Cursor):
    closed = False

    def record(self, row):
        if row is not None:
            metrics = self.connection.metrics
            metrics["returned_rows"] += 1
            metrics["returned_metadata_octets"] += sum(
                len(value) if isinstance(value, bytes) else len(str(value).encode())
                for value in row
                if value is not None
            )
            if "FROM dag_edges AS edges" in self.sql or "FROM dag_incoming AS incoming" in self.sql:
                metrics["adjacency_rows"] += 1
        return row

    def fetchone(self):
        return self.record(super().fetchone())

    def fetchall(self):
        return [self.record(row) for row in super().fetchall()]

    def close(self):
        self.closed = True
        return super().close()


class WorkConnection(sqlite3.Connection):
    recording = False

    def execute(self, sql, parameters=()):
        if not self.recording:
            return super().execute(sql, parameters)
        cursor = self.cursor(factory=WorkCursor)
        cursor.sql = sql
        self.statements.append((sql, parameters, cursor))
        return cursor.execute(sql, parameters)

    @contextmanager
    def observe(self):
        self.metrics = {
            name: 0
            for name in (
                "sql_statements",
                "returned_rows",
                "changed_rows",
                "returned_metadata_octets",
                "adjacency_rows",
                "source_payload_octets",
                "sqlite_vm_instructions",
            )
        }
        self.statements = []
        before = self.total_changes

        def trace(_sql):
            self.metrics["sql_statements"] += 1

        def tick():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def authorize(action, table, column, _database, _trigger):
            if (
                action == sqlite3.SQLITE_READ
                and table in {"assertions", "frames", "identities"}
                and column == "value"
            ):
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
            self.metrics["changed_rows"] = self.total_changes - before
        assert not self.in_transaction
        assert all(cursor.closed for _sql, _parameters, cursor in self.statements)
        for sql, parameters, _cursor in self.statements:
            if sql.startswith(("SELECT", "UPDATE", "DELETE")):
                with closing(self.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as cursor:
                    plan = " ".join(row[3] for row in cursor.fetchall())
                assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan, (
                    sql,
                    plan,
                )
                if sql.startswith("SELECT a.object_id,a.type,a.active FROM changes"):
                    assert "COVERING INDEX semantic_subject" in plan, plan


def seeded(db, count, cycle):
    current = work(db)
    dag = current.checker
    edges = set()
    for ordinal in range(count):
        edges.update(
            (
                (identity(100000 + ordinal), current.source),
                (current.target, identity(200000 + ordinal)),
                (identity(300000 + ordinal), current.target),
            )
        )
    edges.add((identity(400000), identity(100000)))
    if cycle:
        edges.add((identity(200000), current.source))
    db.executemany(
        "INSERT INTO dag_levels VALUES (?, ?, ?, '1')",
        ((dag.kind, dag.run_id, key) for key in sorted({v for edge in edges for v in edge})),
    ).close()
    db.executemany(
        "INSERT INTO dag_edges VALUES (?, ?, ?, ?)",
        ((dag.kind, dag.run_id, a, b) for a, b in sorted(edges)),
    ).close()
    db.executemany(
        "INSERT INTO dag_incoming VALUES (?, ?, ?, ?)",
        ((dag.kind, dag.run_id, b, a) for a, b in sorted(edges)),
    ).close()
    if count in {17, 1025}:
        value = assertion(
            "derivation",
            identity(12),
            used_state=reference(current.source, "state"),
            generated_state=reference(current.target, "state"),
            kind="transformation",
        )
        value["assertion_id"], value["id"] = current.assertion_id, current.object_id
        value["evidence"] *= 1025 if count == 17 else 16385
        payload = canonical_json_bytes(value)
        assert 65536 < len(payload) <= PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
        db.execute(
            "UPDATE assertions SET value=? WHERE assertion_id=?", (payload, current.assertion_id)
        ).close()
    if count == 1025:
        db.execute("INSERT INTO dag_retired VALUES ('derivation','old-run',0)")
        db.executemany(
            "INSERT INTO dag_levels VALUES ('derivation','old-run',?,'1')",
            ((identity(500000 + n),) for n in range(4097)),
        ).close()
    db.commit()
    return edges


@pytest.mark.growth_witness(
    "riverhog-semantic-edge-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._semantic_edge.SemanticEdge.advance",
)
def test_actual_cold_source_fenced_graph_units(tmp_path, record_property):
    binding = growth_binding(SemanticEdge.advance)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    phases = (
        "start",
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
        "complete",
        "done",
    )
    for phase in phases:
        for count, budget in ((1, 1), (15, 16), (16, 16), (17, 16), (129, 16), (1025, 64)):
            path = tmp_path / f"{phase}-{count}.sqlite3"
            create(path)
            # Prepare the unmeasured prefix in memory; the admitted unit below
            # restores its exact committed state into a fresh file-backed pager.
            with closing(sqlite3.connect(":memory:")) as db:
                with closing(connect(path)) as stored:
                    stored.backup(db)
                seeded(db, count, phase == "undo")
                current = work(db)
                current.initialize()
                position(current, phase, budget=budget)
                retained = sum(
                    db.execute("SELECT count(*) FROM " + table).fetchone()[0]
                    for table in (
                        "journal_prefix",
                        "journal_semantic_edges",
                        "dag_runs",
                        "dag_levels",
                        "dag_edges",
                        "dag_incoming",
                        "dag_seen",
                        "dag_queue",
                        "dag_undo",
                        "assertions",
                        "changes",
                        "dag_retired",
                    )
                )
                prefix = db.execute("SELECT binding,checkpoint FROM journal_prefix").fetchone()
                width = max(
                    len(str(value[0])) for value in db.execute("SELECT level FROM dag_levels")
                )
                envelope = 1024 + len(current.binding) + sum(len(v) for v in prefix) + 8 * width
                with closing(connect(path)) as stored:
                    db.backup(stored)
            with closing(connect(path, factory=WorkConnection)) as db:
                current = work(db)
                with db.observe() as metrics:
                    current.advance(row_budget=budget)
                assert metrics["adjacency_rows"] <= budget
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": digest,
                    "dimensions": {
                        "retained_rows": retained,
                        "row_budget": budget,
                        "metadata_row_octets": envelope,
                    },
                    "metrics": dict(metrics),
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
                # Validate the suffix without including its work in the measured unit.
                with closing(sqlite3.connect(":memory:")) as suffix_db:
                    db.backup(suffix_db)
                    suffix = work(suffix_db)
                    finish(suffix, budget=64)
                    if phase == "undo":
                        with pytest.raises(
                            ProvenanceValidationError, match="graph contains a cycle"
                        ):
                            suffix.result()
                    else:
                        suffix.result()
    assert len(samples) == 96
    record_property("growth_samples", samples)
