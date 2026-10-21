from __future__ import annotations

import copy
import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest
import test_assertion_staging as staging
import test_row_semantics as rows
import test_semantic_edge as edges
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance._generic_semantics import GenericSemanticsCapacity, StaleGenericSemantics
from riverhog_provenance._online_dag import OnlineDag
from riverhog_provenance._relation_projection import read_relation_metadata
from riverhog_provenance._semantic_edge import EdgeAdmission, EdgeBatch, StaleSemanticEdge
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.journal import JournalFrame
from test_generic_semantics import connect, finish, identity, require_reclaimed, snapshot, work


def subject(kind, mode="valid"):
    value = rows.subject(kind)
    if kind == "derivation":
        value.update(activity_id=identity(35), usage_id=identity(40), generation_id=identity(41))
    if mode == "unknown-process":
        for key in ("activity_id", "usage_id", "generation_id"):
            value.pop(key, None)
    elif mode == "used-before-generated":
        value["used_state"]["object_id"] = identity(998)
        value["generated_state"]["object_id"] = identity(999)
    elif mode == "generated-before-activity":
        value["generated_state"]["object_id"] = identity(999)
        value["activity_id"] = identity(997)
    elif mode in {"self", "self-before-activity"}:
        value["generated_state" if kind == "derivation" else "general"] = copy.deepcopy(
            value["used_state" if kind == "derivation" else "specific"]
        )
        if mode == "self-before-activity":
            value["activity_id"] = identity(997)
    elif mode in {"cycle-before-activity", "activity-before-usage"}:
        value["activity_id"] = identity(997)
        value["usage_id"] = identity(998)
    elif mode == "usage-before-generation":
        value["usage_id"] = identity(998)
        value["generation_id"] = identity(999)
    elif mode == "generation-before-usage-mismatch":
        value["generation_id"] = identity(999)
    return value


def prepare(path, row, *, degree=1, cycle=False, mode="valid", history=0):
    rows.prepare(path, row, retained=history)
    with closing(connect(path)) as db:
        for kind, n, state in (("usage", 40, 33), ("generation", 41, 34)):
            value = {
                "id": identity(n),
                "assertion_id": identity(n + 100),
                "type": kind,
                "evidence": [],
                "activity_id": identity(35),
                "state": rows.reference("state", state),
            }
            if kind == "usage" and mode in {"usage-mismatch", "generation-before-usage-mismatch"}:
                value["activity_id"] = identity(997)
            if kind == "generation" and mode == "generation-mismatch":
                value["state"] = rows.reference("state", 33)
            db.execute(
                "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    value["assertion_id"],
                    value["id"],
                    "relations",
                    kind,
                    None,
                    None,
                    "0",
                    canonical_json_bytes(value),
                    1,
                ),
            ).close()
            db.execute(
                "INSERT INTO relation_projection VALUES (?,?,?)",
                (value["assertion_id"], value["activity_id"], value["state"]["object_id"]),
            ).close()
        db.commit()
        kind = row["type"]
        run = db.execute("SELECT run_id FROM dag_runs WHERE kind=?", (kind,)).fetchone()[0]
        dag = OnlineDag(db, kind, run)
        left, right = (
            ("used_state", "generated_state") if kind == "derivation" else ("specific", "general")
        )
        source, target = row[left]["object_id"], row[right]["object_id"]
        expected = set()
        for n in range(degree):
            for arc in ((identity(1000 + n), source), (target, identity(2000 + n))):
                dag.add(*arc)
                expected.add(arc)
        if cycle:
            arc = (identity(2000), source)
            dag.add(*arc)
            expected.add(arc)
        return expected


def owned(db):
    return {"parent": snapshot(db), "child": edges.content(db)}


def to_child(current):
    for _ in range(100):
        state = current.load()
        if state.phase == "edge":
            return state
        current.advance(step_budget=1)
    pytest.fail("native references did not reach their graph child")


def close_reopen(path, row, catalog, *, budget=1, error=None, initialize=True):
    child_ids = set()
    with closing(connect(path)) as db:
        if initialize:
            work(db, row, catalog, operation="row").initialize()
        prefix = db.execute("SELECT * FROM journal_prefix").fetchall()
    for _ in range(10000):
        with closing(connect(path)) as db:
            current = work(db, row, catalog, operation="row")
            state = current.advance(step_budget=budget)
            assert not db.in_transaction
            assert db.execute("SELECT * FROM journal_prefix").fetchall() == prefix
            if state.phase == "edge":
                child_ids.add(
                    db.execute(
                        "SELECT attempt_id FROM journal_semantic_edges WHERE kind=?",
                        (row["type"],),
                    ).fetchone()[0]
                )
            if state.done:
                before = owned(db)
                for _ in range(2):
                    if error is None:
                        current.result()
                    else:
                        with pytest.raises(ProvenanceValidationError, match=error):
                            current.result()
                    assert owned(db) == before
                require_reclaimed(db)
                assert current.advance().done
                assert len(child_ids) == 1
                return set(
                    db.execute(
                        "SELECT source,target FROM dag_edges WHERE kind=?", (row["type"],)
                    ).fetchall()
                )
    pytest.fail("finite parent/child work did not finish")


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
@pytest.mark.parametrize("degree", [15, 16, 17])
@pytest.mark.parametrize("budget", [1, 64])
@pytest.mark.parametrize("cycle", [False, True])
def test_parent_and_child_reopen_every_unit_without_restarting_graph(
    tmp_path, catalog, kind, degree, budget, cycle
):
    row = subject(kind)
    path = tmp_path / "reopen.sqlite3"
    expected = prepare(path, row, degree=degree, cycle=cycle)
    actual = close_reopen(
        path, row, catalog, budget=budget, error="graph contains a cycle" if cycle else None
    )
    if not cycle:
        left, right = (
            ("used_state", "generated_state") if kind == "derivation" else ("specific", "general")
        )
        expected.add((row[left]["object_id"], row[right]["object_id"]))
    assert actual == expected


@pytest.mark.parametrize("budget", [1, 64])
@pytest.mark.parametrize(
    "mode,error,accepted_edge",
    [
        ("unknown-process", None, True),
        ("self-before-activity", "cannot relate an entity to itself", False),
        ("cycle-before-activity", "graph contains a cycle", False),
        ("activity-before-usage", "unresolved local identity: " + identity(997), True),
        ("usage-before-generation", "unresolved local identity: " + identity(998), True),
        ("generation-before-usage-mismatch", "unresolved local identity: " + identity(999), True),
        ("usage-mismatch", "qualified derivation does not match usage/generation", True),
        ("generation-mismatch", "qualified derivation does not match usage/generation", True),
    ],
)
def test_graph_completion_precedes_later_native_derivation_diagnostics(
    tmp_path, catalog, budget, mode, error, accepted_edge
):
    row = subject("derivation", mode)
    path = tmp_path / "ordered.sqlite3"
    expected = prepare(path, row, mode=mode, cycle=mode == "cycle-before-activity")
    actual = close_reopen(path, row, catalog, budget=budget, error=error)
    if accepted_edge:
        expected.add((row["used_state"]["object_id"], row["generated_state"]["object_id"]))
    assert actual == expected


@pytest.mark.parametrize("field", ["attempt_id", "binding", "checkpoint"])
def test_stale_parent_cannot_admit_or_advance_child(tmp_path, catalog, field):
    row = subject("derivation")
    path = tmp_path / "stale.sqlite3"
    prepare(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        current.advance(step_budget=1)
        current.advance(step_budget=1)
        for _ in range(2):
            batch = current.prepare(step_budget=1)
            if isinstance(batch.edge, EdgeAdmission):
                break
            current.commit(batch)
        assert isinstance(batch.edge, EdgeAdmission)
        value = (
            "replacement"
            if field == "attempt_id"
            else b"replacement"
            if field == "binding"
            else "{}"
        )
        db.execute("UPDATE journal_generic_semantics SET " + field + "=?", (value,)).close()
        db.commit()
        before = owned(db)
        with pytest.raises((StaleGenericSemantics, RuntimeError)):
            current.commit(batch)
        assert owned(db) == before


@pytest.mark.parametrize("target", ["parent", "child", "graph"])
def test_statement_failure_rolls_back_parent_child_and_graph_together(tmp_path, catalog, target):
    row = subject("specialization")
    path = tmp_path / "rollback.sqlite3"
    prepare(path, row, degree=17)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        to_child(current)
        table = {
            "parent": "journal_generic_semantics",
            "child": "journal_semantic_edges",
            "graph": "dag_runs",
        }[target]
        db.execute(
            "CREATE TRIGGER fail_unit AFTER UPDATE ON "
            + table
            + " BEGIN SELECT RAISE(ABORT,'injected atomic unit failure'); END;"
        ).close()
        db.commit()
        before = owned(db)
        with pytest.raises(sqlite3.IntegrityError, match="injected atomic unit"):
            current.advance(step_budget=1)
        assert not db.in_transaction and owned(db) == before
        db.execute("DROP TRIGGER fail_unit").close()
        db.commit()
        assert current.advance(step_budget=1).phase == "edge"


def test_child_admission_and_parent_intent_are_one_transaction(tmp_path, catalog):
    row = subject("derivation")
    path = tmp_path / "admit.sqlite3"
    prepare(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        while True:
            proposal = current.prepare(step_budget=1)
            if isinstance(proposal.edge, EdgeAdmission):
                break
            current.commit(proposal)
        db.execute(
            "CREATE TRIGGER reject_admission AFTER INSERT ON journal_semantic_edges "
            "BEGIN SELECT RAISE(ABORT,'injected child admission failure'); END;"
        ).close()
        db.commit()
        before = owned(db)
        with pytest.raises(sqlite3.IntegrityError, match="child admission"):
            current.commit(proposal)
        assert owned(db) == before
        db.execute("DROP TRIGGER reject_admission").close()
        db.commit()
        assert current.commit(proposal).phase == "edge"
        assert db.execute("SELECT count(*) FROM journal_semantic_edges").fetchone() == (1,)


def test_byte_suspension_cannot_publish_graph_or_parent_progress(tmp_path, catalog):
    row = subject("derivation")
    path = tmp_path / "capacity.sqlite3"
    prepare(path, row, degree=17)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        to_child(current)
        before = owned(db)
        with pytest.raises(GenericSemanticsCapacity):
            current.advance(step_budget=64, byte_budget=16384)
        assert not db.in_transaction and owned(db) == before
        assert current.advance(step_budget=64, byte_budget=4000000).phase == "edge"


@pytest.mark.parametrize("kind", ["usage", "generation"])
def test_relation_lookup_uses_raw_fixed_correlates_without_payloads(tmp_path, kind):
    row = subject("derivation")
    path = tmp_path / "metadata.sqlite3"
    prepare(path, row, history=4097)
    with closing(connect(path, factory=edges.WorkConnection)) as db:
        db.execute(
            "UPDATE assertions SET value=? WHERE object_id=? AND active=1",
            (b"unread historical payload" * 4096, identity(40 if kind == "usage" else 41)),
        ).close()
        db.commit()
        with db.observe() as observed:
            actual = read_relation_metadata(db, identity(40 if kind == "usage" else 41), kind)
        assert actual == (identity(35), identity(33 if kind == "usage" else 34))
        assert observed["source_payload_octets"] == 0
        assert observed["sql_statements"] == 2 and observed["changed_rows"] == 0


@pytest.mark.parametrize(
    "field", ["attempt_id", "binding", "checkpoint", "missing", "graph-version"]
)
def test_changed_child_or_graph_fences_parent_progress(tmp_path, catalog, field):
    row = subject("derivation")
    path = tmp_path / "child-stale.sqlite3"
    prepare(path, row, degree=17)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        to_child(current)
        proposal = current.prepare(step_budget=1)
        assert isinstance(proposal.edge, EdgeBatch)
        if field == "missing":
            db.execute("DELETE FROM journal_semantic_edges").close()
        elif field == "graph-version":
            db.execute("UPDATE dag_runs SET version='9999' WHERE kind='derivation'").close()
        else:
            value = (
                b"replacement"
                if field == "binding"
                else "replacement"
                if field == "attempt_id"
                else "{}"
            )
            db.execute("UPDATE journal_semantic_edges SET " + field + "=?", (value,)).close()
        db.commit()
        before = owned(db)
        with pytest.raises((StaleSemanticEdge, RuntimeError)):
            current.commit(proposal)
        assert owned(db) == before and not db.in_transaction


def test_foreign_child_proposal_rolls_back_its_parent_checkpoint(tmp_path, catalog):
    row = subject("derivation")
    path = tmp_path / "foreign.sqlite3"
    prepare(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        to_child(current)
        proposal = current.prepare(step_budget=1)
        assert isinstance(proposal.edge, EdgeBatch)
        foreign = replace(proposal, edge=replace(proposal.edge, attempt_id="foreign-worker"))
        before = owned(db)
        with pytest.raises(StaleSemanticEdge):
            current.commit(foreign)
        assert owned(db) == before and not db.in_transaction
        current.commit(proposal)


@pytest.mark.parametrize("damage", ["accepted", "self", "missing", "attempt"])
def test_graph_rejection_result_authenticates_retained_child_without_replay(
    tmp_path, catalog, damage
):
    row = subject("derivation")
    path = tmp_path / "rejected.sqlite3"
    prepare(path, row, cycle=True)
    close_reopen(path, row, catalog, error="graph contains a cycle")
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        if damage in {"accepted", "self"}:
            value = json.loads(
                db.execute("SELECT work FROM dag_runs WHERE kind='derivation'").fetchone()[0]
            )
            value["outcome"] = damage
            db.execute(
                "UPDATE dag_runs SET work=? WHERE kind='derivation'",
                (canonical_json_bytes(value).decode(),),
            ).close()
        elif damage == "missing":
            db.execute("DELETE FROM journal_semantic_edges").close()
        else:
            db.execute("UPDATE journal_semantic_edges SET attempt_id='replacement'").close()
        db.commit()
        before = owned(db)
        with pytest.raises(RuntimeError):
            current.result()
        assert owned(db) == before and not db.in_transaction


@pytest.mark.parametrize(
    "mode,missing",
    [
        ("used-before-generated", 998),
        ("generated-before-activity", 999),
    ],
)
def test_native_reference_failure_precedes_graph_admission(tmp_path, catalog, mode, missing):
    row = subject("derivation", mode)
    path = tmp_path / "early.sqlite3"
    prepare(path, row)
    with closing(connect(path)) as db:
        before = edges.content(db)
        current = work(db, row, catalog, operation="row")
        current.initialize()
        finish(current, budget=1)
        with pytest.raises(
            ProvenanceValidationError, match="unresolved local identity: " + identity(missing)
        ):
            current.result()
        assert edges.content(db) == before
        assert db.execute("SELECT 1 FROM journal_semantic_edges").fetchone() is None


@pytest.mark.parametrize("kind", ["usage", "generation"])
@pytest.mark.parametrize(
    "damage",
    [
        "missing-object",
        "retired",
        "wrong-type",
        "missing-projection",
        "null",
        "numeric",
        "overlong",
        "uuid",
    ],
)
def test_relation_metadata_rejects_missing_changed_or_corrupt_state(tmp_path, kind, damage):
    row = subject("derivation")
    path = tmp_path / "corrupt-relation.sqlite3"
    prepare(path, row)
    target = 40 if kind == "usage" else 41
    assertion_id = identity(target + 100)
    with closing(connect(path)) as db:
        expected = RuntimeError
        message = "staged relation"
        if damage == "missing-object":
            db.execute("DELETE FROM assertions WHERE assertion_id=?", (assertion_id,)).close()
            expected = ProvenanceValidationError
            message = "unresolved local identity"
        elif damage == "retired":
            db.execute(
                "UPDATE assertions SET active=0 WHERE assertion_id=?", (assertion_id,)
            ).close()
            expected = ProvenanceValidationError
            message = "unresolved local identity"
        elif damage == "wrong-type":
            db.execute(
                "UPDATE assertions SET type='agent' WHERE assertion_id=?", (assertion_id,)
            ).close()
            db.execute(
                "DELETE FROM relation_projection WHERE assertion_id=?", (assertion_id,)
            ).close()
            expected = ProvenanceValidationError
            message = "wrong reference type"
        elif damage == "missing-projection":
            db.execute(
                "DELETE FROM relation_projection WHERE assertion_id=?", (assertion_id,)
            ).close()
        else:
            field = "activity_id" if kind == "usage" else "state_id"
            if damage == "null":
                # SQLite enforces the stored shape before readers can observe it.
                with pytest.raises(sqlite3.IntegrityError):
                    db.execute(
                        "UPDATE relation_projection SET " + field + "=NULL WHERE assertion_id=?",
                        (assertion_id,),
                    ).close()
                db.rollback()
                return
            value = {"numeric": 123, "overlong": "x" * 20000, "uuid": "x" * 45}[damage]
            db.execute(
                "UPDATE relation_projection SET " + field + "=? WHERE assertion_id=?",
                (value, assertion_id),
            ).close()
        db.commit()
        before = db.total_changes
        with pytest.raises(expected, match=message):
            read_relation_metadata(db, identity(target), kind)
        assert db.total_changes == before and not db.in_transaction


def raw_relations():
    values = []
    for kind, n, state in (("usage", 40, 33), ("generation", 41, 34)):
        values.append(
            {
                "id": identity(n),
                "assertion_id": identity(n + 100),
                "type": kind,
                "evidence": [],
                "activity_id": identity(997),
                "state": rows.reference("state", state),
            }
        )
    return JournalFrame(
        canonical_json_bytes(
            {
                "journal_id": identity(2),
                "sequence": "1",
                "body": {"assertions": {"relations": values}},
            }
        )
    )


@pytest.mark.parametrize("budget", [1, 15, 16, 17, 64])
def test_raw_relation_staging_reopens_without_early_semantic_reference_checks(tmp_path, budget):
    path = tmp_path / "raw-relations.sqlite3"
    staging.create(path)
    frame = raw_relations()
    with closing(staging.connect(path)) as db:
        staging.work(db, frame).initialize()
        prefix = db.execute("SELECT * FROM journal_prefix").fetchall()
    for _ in range(4):
        with closing(staging.connect(path)) as db:
            current = staging.work(db, frame)
            state = current.advance(row_budget=budget, byte_budget=4000000)
            assert db.execute("SELECT * FROM journal_prefix").fetchall() == prefix
            assert not db.in_transaction
            if state.done:
                assert read_relation_metadata(db, identity(40), "usage") == (
                    identity(997),
                    identity(33),
                )
                assert read_relation_metadata(db, identity(41), "generation") == (
                    identity(997),
                    identity(34),
                )
                assert (
                    db.execute(
                        "SELECT 1 FROM assertions WHERE object_id=?", (identity(997),)
                    ).fetchone()
                    is None
                )
                break
    else:
        pytest.fail("raw relation staging did not finish")


def test_raw_relation_and_staging_checkpoint_roll_back_together(tmp_path):
    path = tmp_path / "raw-atomic.sqlite3"
    staging.create(path)
    frame = raw_relations()
    with closing(staging.connect(path)) as db:
        current = staging.work(db, frame)
        current.initialize()
        tables = (
            "journal_assertion_staging",
            "identities",
            "assertions",
            "changes",
            "relation_projection",
        )

        def content():
            return {
                table: db.execute("SELECT * FROM " + table + " ORDER BY rowid").fetchall()
                for table in tables
            }

        db.execute(
            "CREATE TRIGGER reject_raw AFTER INSERT ON relation_projection "
            "BEGIN SELECT RAISE(ABORT,'injected relation staging failure'); END;"
        ).close()
        db.commit()
        before = content()
        with pytest.raises(sqlite3.IntegrityError, match="relation staging"):
            current.advance(row_budget=1, byte_budget=4000000)
        assert not db.in_transaction and content() == before
        db.execute("DROP TRIGGER reject_raw").close()
        db.commit()
        assert current.advance(row_budget=1, byte_budget=4000000).position == 1
        db.execute("UPDATE relation_projection SET activity_id=?", (identity(998),)).close()
        db.commit()
        with pytest.raises(RuntimeError, match="staged relation metadata"):
            current.load()


GRAPH_CRASH_PROGRAM = r"""
import json,os,sqlite3,sys
from pathlib import Path
sys.path.insert(0,sys.argv[2])
from test_generic_semantics import connect,work
from riverhog_provenance_contracts import ContractCatalog
class CrashConnection(sqlite3.Connection):
    active=False
    def commit(self):
        super().commit()
        if self.active:os._exit(73)
path,_,row_path,seam,budget=sys.argv[1:]
db=connect(path,factory=CrashConnection)
db.create_function('hard_exit',0,lambda:os._exit(73))
triggers={
 'parent-header':('UPDATE','journal_generic_semantics'),
 'child-admission':('INSERT','journal_semantic_edges'),
 'child-header':('UPDATE','journal_semantic_edges'),
 'graph-header':('UPDATE','dag_runs'),
 'frame':('UPDATE','generic_semantic_frames'),
}
if seam in triggers:
    event,table=triggers[seam]
    db.execute('CREATE TRIGGER hard_crash AFTER '+event+' ON '+table+
               ' BEGIN SELECT hard_exit(); END;').close()
sqlite3.Connection.commit(db)
if seam=='before-commit':
    def trace(sql):
        if sql.strip().upper()=='COMMIT':os._exit(73)
    db.set_trace_callback(trace)
if seam=='after-commit':db.active=True
current=work(db,json.loads(Path(row_path).read_text()),ContractCatalog(),operation='row')
current.advance(step_budget=int(budget))
raise SystemExit(99)
"""


def graph_position(current, phase, *, budget=1):
    for _ in range(10000):
        state = current.load()
        if state.phase == "edge":
            child = json.loads(
                current.db.execute(
                    "SELECT checkpoint FROM journal_semantic_edges WHERE kind=?",
                    (current.kind,),
                ).fetchone()[0]
            )
            graph = json.loads(
                current.db.execute(
                    "SELECT work FROM dag_runs WHERE kind=?",
                    (current.kind,),
                ).fetchone()[0]
            )
            actual = (
                "start"
                if child["phase"] == "start"
                else "child-done"
                if child["phase"] == "done"
                else graph["phase"]
            )
            if actual == phase:
                return
        assert not state.done, (phase, state.phase)
        current.advance(step_budget=budget)
    pytest.fail("graph child did not reach " + phase)


@pytest.mark.parametrize("kind", ["derivation", "specialization"])
@pytest.mark.parametrize(
    "phase,seam",
    [
        ("admission", "parent-header"),
        ("admission", "child-admission"),
        ("references", "frame"),
        ("admission", "before-commit"),
        ("admission", "after-commit"),
        ("back-pop", "parent-header"),
        ("back-pop", "child-header"),
        ("back-pop", "graph-header"),
        ("back-pop", "before-commit"),
        ("back-pop", "after-commit"),
        ("undo", "parent-header"),
        ("undo", "child-header"),
        ("undo", "graph-header"),
        ("undo", "before-commit"),
        ("undo", "after-commit"),
    ],
)
def test_real_process_death_preserves_parent_child_graph_atomicity(
    tmp_path, catalog, kind, phase, seam
):
    row = subject(kind)
    path = tmp_path / "hard-exit.sqlite3"
    expected = prepare(path, row, degree=3, cycle=phase == "undo")
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        if phase == "admission":
            current.advance(step_budget=1)
            current.advance(step_budget=1)
            assert isinstance(current.prepare(step_budget=1).edge, EdgeAdmission)
        elif phase != "references":
            graph_position(current, phase)
        before = owned(db)
    row_path = tmp_path / "row.json"
    row_path.write_bytes(canonical_json_bytes(row))
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            GRAPH_CRASH_PROGRAM,
            str(path),
            str(Path(__file__).parent),
            str(row_path),
            seam,
            "64" if phase == "references" else "1",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 73, result.stderr[-1000:]
    with closing(connect(path)) as db:
        after = owned(db)
        if seam == "after-commit":
            assert after != before
        else:
            assert after == before
        db.execute("DROP TRIGGER IF EXISTS hard_crash").close()
        db.commit()
    actual = close_reopen(
        path,
        row,
        catalog,
        error="graph contains a cycle" if phase == "undo" else None,
        initialize=False,
    )
    if phase != "undo":
        left, right = (
            ("used_state", "generated_state") if kind == "derivation" else ("specific", "general")
        )
        expected.add((row[left]["object_id"], row[right]["object_id"]))
    assert actual == expected


REGIMES = ((1, 1), (15, 16), (16, 16), (17, 16), (129, 16), (1025, 64))


def seed_graph(db, row, count, cycle):
    kind = row["type"]
    run = db.execute("SELECT run_id FROM dag_runs WHERE kind=?", (kind,)).fetchone()[0]
    left, right = (
        ("used_state", "generated_state") if kind == "derivation" else ("specific", "general")
    )
    source, target = row[left]["object_id"], row[right]["object_id"]
    arcs = {
        arc
        for n in range(count)
        for arc in (
            (identity(100000 + n), source),
            (target, identity(200000 + n)),
            (identity(300000 + n), target),
        )
    }
    arcs.add((identity(400000), identity(100000)))
    if cycle:
        arcs.add((identity(200000), source))
    db.executemany(
        "INSERT INTO dag_levels VALUES (?,?,?,'1')",
        ((kind, run, v) for v in sorted({v for arc in arcs for v in arc})),
    ).close()
    db.executemany(
        "INSERT INTO dag_edges VALUES (?,?,?,?)",
        ((kind, run, a, b) for a, b in sorted(arcs)),
    ).close()
    db.executemany(
        "INSERT INTO dag_incoming VALUES (?,?,?,?)",
        ((kind, run, b, a) for a, b in sorted(arcs)),
    ).close()
    if count == 1025:
        db.execute("INSERT INTO dag_retired VALUES (?, 'old-run', 0)", (kind,)).close()
        db.executemany(
            "INSERT INTO dag_levels VALUES (?,'old-run',?,'1')",
            ((kind, identity(500000 + n)) for n in range(4097)),
        ).close()
    db.commit()
    return arcs


def position_parent(current, phase, *, budget=1):
    if phase in {"done", "rejected"}:
        finish(current, budget=64)
        assert current.load().phase == phase
        return
    if phase not in {"admit", "resumed"}:
        graph_position(current, phase, budget=budget)
        return
    for _ in range(10000):
        state = current.load()
        if state.phase == "walk":
            frame = current.db.execute(
                "SELECT value FROM generic_semantic_frames WHERE attempt_id=? AND frame_id=?",
                (current.attempt_id, state.top),
            ).fetchone()
            value = json.loads(frame[0])
            if value["operation"] == "row" and value["stage"] == ("2" if phase == "admit" else "3"):
                return
        assert not state.done, (phase, state.phase)
        current.advance(step_budget=1 if phase == "admit" else budget)
    pytest.fail("parent did not reach " + phase)


def semantic_sample(binding, digest, retained, budget, envelope, metrics, byte_budget):
    return {
        "declaration": binding.declaration.id,
        "declaration_sha256": digest,
        "dimensions": {
            "retained_rows": retained,
            "step_budget": budget,
            "byte_budget": byte_budget,
            "metadata_row_octets": envelope,
        },
        "metrics": dict(metrics),
        "delegates": {},
    }


def envelope(current, prefix):
    return (
        8192
        + len(current.binding)
        + sum(len(v) if isinstance(v, bytes) else len(str(v).encode()) for v in prefix)
    )


def growth_samples(tmp_path, catalog):
    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance._generic_semantics import GenericSemantics
    from test_generic_semantics import CommonWorkConnection, binding_projection, workspace_rows

    binding, projection, digest = binding_projection(GenericSemantics.advance)
    samples = []
    phases = (
        "admit",
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
        "child-done",
        "resumed",
        "done",
        "rejected",
    )
    for kind in ("derivation", "specialization"):
        for phase in phases:
            for count, budget in REGIMES:
                rejected = phase in {"undo", "rejected"}
                row = subject(kind)
                if count in {17, 1025}:
                    row["evidence"] = [
                        {"asserted_by_agent_id": identity(20), "basis": "attestation"}
                    ] * (1025 if count == 17 else 8193)
                path = tmp_path / f"edge-parent-{kind}-{phase}-{count}.sqlite3"
                prepare(path, row, degree=0, history=4097 if count in {17, 1025} else count)
                with closing(sqlite3.connect(":memory:")) as db:
                    with closing(connect(path)) as original:
                        original.backup(db)
                    expected = seed_graph(db, row, count, rejected)
                    current = work(db, row, catalog, operation="row")
                    current.initialize()
                    position_parent(current, phase, budget=budget)
                    retained = workspace_rows(db)
                    prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                    width = envelope(current, prefix)
                    with closing(connect(path)) as destination:
                        db.backup(destination)
                with closing(connect(path, factory=CommonWorkConnection)) as db:
                    current = work(db, row, catalog, operation="row")
                    byte_budget = 16 * 1024 * 1024
                    with db.observe() as metrics:
                        current.advance(step_budget=budget, byte_budget=byte_budget)
                    sample = semantic_sample(
                        binding, digest, retained, budget, width, metrics, byte_budget
                    )
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
                    assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
                    with closing(sqlite3.connect(":memory:")) as suffix_db:
                        db.backup(suffix_db)
                        suffix = work(suffix_db, row, catalog, operation="row")
                        finish(suffix, budget=64)
                        if rejected:
                            with pytest.raises(
                                ProvenanceValidationError, match="graph contains a cycle"
                            ):
                                suffix.result()
                        else:
                            suffix.result()
                            left, right = (
                                ("used_state", "generated_state")
                                if kind == "derivation"
                                else ("specific", "general")
                            )
                            expected.add((row[left]["object_id"], row[right]["object_id"]))
                        actual = set(
                            suffix_db.execute(
                                "SELECT source,target FROM dag_edges WHERE kind=?", (kind,)
                            ).fetchall()
                        )
                        assert actual == expected
                        require_reclaimed(suffix_db)
    assert len(samples) == 240
    return samples


def terminal_growth_samples(tmp_path, catalog):
    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance._generic_semantics import _RESULT_BYTES, GenericSemantics
    from test_generic_semantics import CommonWorkConnection, binding_projection, workspace_rows

    binding, projection, digest = binding_projection(GenericSemantics.result)
    samples = []
    for kind in ("derivation", "specialization"):
        for mode in ("self", "cycle"):
            for count, _budget in REGIMES:
                row = subject(kind, "self" if mode == "self" else "valid")
                path = tmp_path / f"edge-terminal-{kind}-{mode}-{count}.sqlite3"
                prepare(path, row, degree=0, history=4097 if count in {17, 1025} else count)
                with closing(sqlite3.connect(":memory:")) as db:
                    with closing(connect(path)) as original:
                        original.backup(db)
                    if mode == "cycle":
                        seed_graph(db, row, count, True)
                    current = work(db, row, catalog, operation="row")
                    current.initialize()
                    finish(current, budget=64)
                    assert current.load().phase == "rejected"
                    retained = workspace_rows(db)
                    prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                    width = envelope(current, prefix)
                    with closing(connect(path)) as destination:
                        db.backup(destination)
                with closing(connect(path, factory=CommonWorkConnection)) as db:
                    current = work(db, row, catalog, operation="row")
                    before = owned(db)
                    message = (
                        "cannot relate an entity to itself"
                        if mode == "self"
                        else "graph contains a cycle"
                    )
                    with db.observe() as metrics:
                        with pytest.raises(ProvenanceValidationError, match=message):
                            current.result()
                    assert owned(db) == before
                    sample = semantic_sample(
                        binding, digest, retained, 1, width, metrics, _RESULT_BYTES
                    )
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
    assert len(samples) == 24
    return samples


def staging_growth_samples(tmp_path):
    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance._assertion_staging import AssertionStaging
    from test_generic_semantics import binding_projection, workspace_rows

    binding, projection, digest = binding_projection(AssertionStaging.advance)
    samples = []
    for kind in ("usage", "generation"):
        for phase in ("first", "last", "done"):
            for count, budget in REGIMES:
                values = [
                    {
                        "id": identity(100000 + n),
                        "assertion_id": identity(200000 + n),
                        "type": kind,
                        "evidence": [],
                        "activity_id": identity(997),
                        "state": rows.reference("state", 33),
                    }
                    for n in range(count)
                ]
                raw = JournalFrame(
                    canonical_json_bytes(
                        {
                            "journal_id": identity(2),
                            "sequence": "1",
                            "body": {"assertions": {"relations": values}},
                        }
                    )
                )
                path = tmp_path / f"edge-staging-{kind}-{phase}-{count}.sqlite3"
                staging.create(path)
                with closing(sqlite3.connect(":memory:")) as db:
                    with closing(staging.connect(path)) as original:
                        original.backup(db)
                    current = staging.work(db, raw)
                    current.initialize()
                    stop = (
                        count
                        if phase == "done"
                        else max(0, count - budget)
                        if phase == "last"
                        else 0
                    )
                    while current.load().position < stop:
                        current.advance(
                            row_budget=min(64, stop - current.load().position), byte_budget=4000000
                        )
                    retained = workspace_rows(db)
                    prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                    with closing(staging.connect(path)) as destination:
                        db.backup(destination)
                with closing(staging.connect(path, factory=staging.WorkConnection)) as db:
                    current = staging.work(db, raw)
                    with db.observe() as metrics:
                        current.advance(row_budget=budget, byte_budget=65536)
                    sample = {
                        "declaration": binding.declaration.id,
                        "declaration_sha256": digest,
                        "dimensions": {
                            "retained_rows": retained,
                            "row_budget": budget,
                            "byte_budget": 65536,
                            "max_atom_octets": max(
                                staging.proposed_octets("relations", v) for v in values
                            ),
                        },
                        "metrics": {
                            b.metric: metrics[b.metric] for b in binding.declaration.bounds
                        },
                        "delegates": {},
                    }
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
                    assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
    assert len(samples) == 36
    return samples


@pytest.mark.growth_witness(
    "riverhog-relation-metadata-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._relation_projection.read_relation_metadata",
)
def test_actual_cold_relation_metadata_work(tmp_path, record_property):
    from riverhog_growth_contracts import validate_execution_samples
    from test_entry_preparation import WorkConnection
    from test_generic_semantics import binding_projection

    binding, projection, digest = binding_projection(read_relation_metadata)
    samples = []
    for kind in ("usage", "generation"):
        for mode in ("valid", "missing", "wrong", "retired", "missing-projection"):
            for history in (0, 1, 15, 16, 17, 129, 4097):
                path = tmp_path / f"relation-metadata-{kind}-{mode}-{history}.sqlite3"
                prepare(path, subject("derivation"), degree=0, history=history)
                target = identity(40 if kind == "usage" else 41)
                assertion_id = identity(140 if kind == "usage" else 141)
                with closing(connect(path)) as db:
                    db.execute(
                        "UPDATE assertions SET value=? WHERE assertion_id=?",
                        (
                            b"unread documentary payload" * 16384,
                            assertion_id,
                        ),
                    ).close()
                    if mode == "missing":
                        db.execute(
                            "DELETE FROM assertions WHERE assertion_id=?", (assertion_id,)
                        ).close()
                    elif mode == "retired":
                        db.execute(
                            "UPDATE assertions SET active=0 WHERE assertion_id=?", (assertion_id,)
                        ).close()
                    elif mode == "wrong":
                        db.execute(
                            "UPDATE assertions SET type='agent' WHERE assertion_id=?",
                            (assertion_id,),
                        ).close()
                    elif mode == "missing-projection":
                        db.execute(
                            "DELETE FROM relation_projection WHERE assertion_id=?", (assertion_id,)
                        ).close()
                    db.commit()
                with closing(connect(path, factory=WorkConnection)) as db:
                    indexed = db.execute("SELECT count(*) FROM assertions").fetchone()[0]
                    with db.observe() as metrics:
                        if mode == "valid":
                            assert read_relation_metadata(db, target, kind) == (
                                identity(35),
                                identity(33 if kind == "usage" else 34),
                            )
                        else:
                            error = (
                                RuntimeError
                                if mode == "missing-projection"
                                else ProvenanceValidationError
                            )
                            message = (
                                "missing staged relation"
                                if mode == "missing-projection"
                                else "wrong reference type"
                                if mode == "wrong"
                                else "unresolved local identity"
                            )
                            with pytest.raises(error, match=message):
                                read_relation_metadata(db, target, kind)
                    sample = {
                        "declaration": binding.declaration.id,
                        "declaration_sha256": digest,
                        "dimensions": {"indexed_assertions": indexed},
                        "metrics": dict(metrics),
                        "delegates": {},
                    }
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
    assert len(samples) == 70
    record_property("growth_samples", samples)
