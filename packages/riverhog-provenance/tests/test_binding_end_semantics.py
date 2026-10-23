from __future__ import annotations

import copy
import hashlib
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
import test_row_semantics as rows
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import _generic_semantics as semantics
from riverhog_provenance import (
    assertion,
    create_journal,
    new_id,
    reference,
    validate_graph,
    validate_journal,
)
from riverhog_provenance._assertion_staging import AssertionStagingCapacity
from riverhog_provenance._binding_projection import read_binding_time
from riverhog_provenance._generic_semantics import GenericSemantics, StaleGenericSemantics
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.graph import _Validator
from riverhog_provenance.journal import JournalFrame
from test_assertion_staging import connect, contents, create, identity, proposed_octets, work
from test_entry_preparation import WorkConnection
from test_generic_semantics import (
    CRASH_PROGRAM,
    CommonWorkConnection,
    binding_projection,
    finish,
    measure_semantic_operation,
    outputs,
    position,
    require_reclaimed,
    snapshot,
    workspace_rows,
)
from test_generic_semantics import (
    work as semantic_work,
)

MODES = (
    "unbounded",
    "instant",
    "interval",
    "end-only",
    "raw-calendar",
    "missing",
    "wrong",
    "retired",
    "reasserted",
    "missing-projection",
)


def binding(ordinal=1, *, scope=None, payload=0):
    return {
        "assertion_id": identity(100 + ordinal),
        "id": identity(1000000 + ordinal),
        "type": "locator_binding",
        "evidence": [],
        "temporal_scope": scope or {"kind": "unknown", "reason": "unavailable"},
        "locator": {"kind": "opaque", "value": {"kind": "text", "text": "x" * payload}},
    }


def raw_point(row):
    scope = row["temporal_scope"]
    return scope.get("at", scope.get("start"))


def insert(db, row, *, active=1, projection=True):
    db.execute(
        "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
        (
            row["assertion_id"],
            row["id"],
            "locator_bindings",
            row["type"],
            None,
            None,
            "0",
            canonical_json_bytes(row),
            active,
        ),
    ).close()
    if row["type"] == "locator_binding" and projection:
        db.execute(
            "INSERT INTO binding_projection VALUES (?,?)",
            (row["assertion_id"], raw_point(row)),
        ).close()


def prepare(path, mode, *, history=0, payload=0):
    create(path)
    current = binding(payload=payload)
    if mode in {"instant", "reasserted"}:
        current["temporal_scope"] = {"kind": "instant", "at": "0001-01-01T00:00:00Z"}
    elif mode == "interval":
        current["temporal_scope"] = {
            "kind": "interval",
            "start": "9999-12-31T23:59:59.999999999Z",
        }
    elif mode == "end-only":
        current["temporal_scope"] = {"kind": "interval", "end": "2026-01-01T00:00:00Z"}
    elif mode == "raw-calendar":
        current["temporal_scope"] = {"kind": "instant", "at": "2026-02-30T00:00:00Z"}
    elif mode == "wrong":
        current["type"] = "agent"
    with closing(connect(path)) as db:
        prior = binding(999, scope={"kind": "instant", "at": "2020-01-01T00:00:00Z"})
        prior["id"] = current["id"]
        insert(db, prior, active=0)
        if mode != "missing":
            insert(
                db,
                current,
                active=0 if mode == "retired" else 1,
                projection=mode != "missing-projection",
            )
        for n in range(history):
            insert(db, binding(10000 + n, payload=262144 if n == 0 else 0), active=n % 2)
        db.commit()
    return current


def check(db, row, mode):
    if mode in {"missing", "retired"}:
        with pytest.raises(ProvenanceValidationError, match="unresolved local identity"):
            read_binding_time(db, row["id"])
    elif mode == "wrong":
        with pytest.raises(ProvenanceValidationError, match="wrong reference type"):
            read_binding_time(db, row["id"])
    elif mode == "missing-projection":
        with pytest.raises(RuntimeError, match="missing staged locator-binding metadata"):
            read_binding_time(db, row["id"])
    else:
        assert read_binding_time(db, row["id"]) == raw_point(row)


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("query_only", [False, True])
def test_binding_time_reads_effective_raw_fields_without_payload_or_writes(
    tmp_path,
    mode,
    query_only,
):
    path = tmp_path / "binding.sqlite3"
    row = prepare(path, mode, history=17, payload=1024 * 1024)
    original = copy.deepcopy(row)
    with closing(connect(path, factory=WorkConnection)) as db:
        before = contents(db)
        if query_only:
            db.execute("PRAGMA query_only=ON").close()
        with db.observe() as metrics:
            check(db, row, mode)
        assert metrics["sql_statements"] == (1 if mode in {"missing", "retired", "wrong"} else 2)
        assert metrics["changed_rows"] == metrics["historical_payload_octets"] == 0
        assert metrics["returned_metadata_octets"] <= 128
        assert contents(db) == before and not db.in_transaction
    assert row == original


@pytest.mark.parametrize("value", ["x" * 1048576, b"x" * 1048576, "\N{SNOWMAN}"])
def test_invalid_projection_is_rejected_after_a_bounded_metadata_read(tmp_path, value):
    path = tmp_path / "corrupt.sqlite3"
    row = prepare(path, "instant")
    with closing(connect(path, factory=WorkConnection)) as db:
        db.execute(
            "UPDATE binding_projection SET bound_at=? WHERE assertion_id=?",
            (value, row["assertion_id"]),
        ).close()
        db.commit()
        with db.observe() as metrics:
            with pytest.raises(RuntimeError, match="invalid staged locator-binding time"):
                read_binding_time(db, row["id"])
        assert metrics["returned_metadata_octets"] <= 128
        assert metrics["historical_payload_octets"] == metrics["changed_rows"] == 0


def source():
    return JournalFrame(
        canonical_json_bytes(
            {
                "journal_id": identity(2),
                "sequence": "1",
                "body": {
                    "assertions": {
                        "locator_bindings": [
                            binding(1, scope={"kind": "instant", "at": "2026-02-30T00:00:00Z"}),
                            binding(2, scope={"kind": "interval", "end": "2026-01-01T00:00:00Z"}),
                        ],
                    }
                },
            }
        )
    )


def test_staging_copies_raw_binding_times_atomically_and_admits_their_bytes(tmp_path):
    path = tmp_path / "stage.sqlite3"
    create(path)
    frame = source()
    first = frame.document["body"]["assertions"]["locator_bindings"][0]
    budget = proposed_octets("locator_bindings", first)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        before = contents(db)
        with pytest.raises(AssertionStagingCapacity):
            current.advance(row_budget=1, byte_budget=budget - 1)
        assert contents(db) == before
        current.advance(row_budget=1, byte_budget=budget)
        assert read_binding_time(db, first["id"]) == "2026-02-30T00:00:00Z"
        assert contents(db)["journal_prefix"] == before["journal_prefix"]
    with closing(connect(path)) as db:
        assert work(db, frame).advance(row_budget=1).done
        assert db.execute("SELECT * FROM binding_projection ORDER BY assertion_id").fetchall() == [
            (identity(101), "2026-02-30T00:00:00Z"),
            (identity(102), None),
        ]


STAGING_CRASH = r"""
import os,sqlite3,sys
from pathlib import Path
from test_binding_end_semantics import connect,work,JournalFrame
seam=sys.argv[3]
class DeathConnection(sqlite3.Connection):
    def execute(self,sql,parameters=()):
        if sql.startswith('INSERT INTO binding_projection') and seam=='projection-before':
            os._exit(73)
        result=super().execute(sql,parameters)
        if sql.startswith('INSERT INTO binding_projection') and seam=='projection-after':
            os._exit(73)
        return result
    def commit(self):
        if seam=='before-commit':os._exit(73)
        super().commit()
        if seam=='after-commit':os._exit(73)
db=connect(sys.argv[1],factory=DeathConnection)
work(db,JournalFrame(Path(sys.argv[2]).read_bytes())).advance(row_budget=1)
raise SystemExit(99)
"""


@pytest.mark.parametrize(
    "seam", ["projection-before", "projection-after", "before-commit", "after-commit"]
)
def test_process_death_keeps_binding_assertion_projection_and_cursor_atomic(tmp_path, seam):
    path, expected_path = tmp_path / "stage.sqlite3", tmp_path / "expected.sqlite3"
    create(path)
    frame = source()
    source_path = tmp_path / "source.json"
    source_path.write_bytes(frame.json_bytes)
    with closing(connect(path)) as db:
        work(db, frame).initialize()
        before = contents(db)
        with closing(connect(expected_path)) as target:
            db.backup(target)
    with closing(connect(expected_path)) as db:
        work(db, frame).advance(row_budget=1)
        expected = contents(db)
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys;sys.path.insert(0,sys.argv.pop(4));" + STAGING_CRASH,
            str(path),
            str(source_path),
            seam,
            str(Path(__file__).parent),
        ],
        capture_output=True,
        text=True,
    )
    assert child.returncode == 73, child.stderr
    with closing(connect(path)) as db:
        assert db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert contents(db) == (expected if seam == "after-commit" else before)
        assert work(db, frame).load().position == int(seam == "after-commit")
        while not work(db, frame).advance(row_budget=1).done:
            pass
        assert len(contents(db)["binding_projection"]) == 2


@pytest.mark.parametrize("fault", ["valid", "missing", "wrong", "before", "calendar", "duplicate"])
def test_public_binding_end_preserves_native_result_and_error_order(
    graph,
    who,
    catalog,
    monkeypatch,
    fault,
):
    context = assertion("context", who, kind="repository")
    target = assertion(
        "locator_binding",
        who,
        target=reference(graph["states"][0]["id"], "state"),
        context_id=context["id"],
        locator={"kind": "repository_id", "identifier": {"kind": "text", "text": "x" * 4096}},
        temporal_scope={"kind": "instant", "at": "2026-01-01T00:00:00Z"},
    )
    end = assertion("locator_binding_end", who, binding_id=target["id"], at="2026-02-01T00:00:00Z")
    target["assertion_id"], end["assertion_id"] = identity(70001), identity(70002)
    graph["contexts"], graph["locator_bindings"] = [context], [target, end]
    messages = {
        "missing": "unresolved local identity",
        "wrong": "wrong reference type",
        "before": "binding end precedes the known binding",
        "calendar": "invalid calendar",
        "duplicate": "duplicate effective end",
    }
    if fault == "missing":
        end["binding_id"] = new_id()
    elif fault == "wrong":
        end["binding_id"] = context["id"]
    elif fault == "before":
        end["at"] = "2025-12-31T00:00:00Z"
    elif fault == "calendar":
        end["at"] = "2026-02-30T00:00:00Z"
    elif fault == "duplicate":
        later = assertion(
            "locator_binding_end", who, binding_id=target["id"], at="2025-12-31T00:00:00Z"
        )
        later["assertion_id"] = identity(70003)
        graph["locator_bindings"].append(later)
    calls = []
    original = semantics.read_binding_time

    def observed(db, identity):
        calls.append(identity)
        return original(db, identity)

    monkeypatch.setattr(semantics, "read_binding_time", observed)
    if fault == "valid":
        validate_graph(graph, catalog=catalog)
        raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(raw, catalog=catalog) as summary:
            assert summary.journal_sha256 == hashlib.sha256(raw).hexdigest()
            assert {row["id"] for row in summary.graph["locator_bindings"]} == {
                target["id"],
                end["id"],
            }
        assert calls
    else:
        with pytest.raises(ProvenanceValidationError, match=messages[fault]) as memory:
            validate_graph(graph, catalog=catalog)
        with pytest.raises(ProvenanceValidationError, match=messages[fault]) as stored:
            create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        assert str(stored.value) == str(memory.value)


@pytest.mark.parametrize(
    "fault,message",
    [
        ("valid", None),
        ("equal", None),
        ("nanosecond-equal", None),
        ("unbounded-calendar", None),
        ("missing", "unresolved local identity"),
        ("wrong", "wrong reference type"),
        ("duplicate-calendar", "duplicate effective end"),
        ("calendar", "invalid calendar instant"),
        ("before", "binding end precedes"),
        ("nanosecond-before", "binding end precedes"),
    ],
)
def test_binding_end_native_order_and_boundary_cases(tmp_path, catalog, fault, message):
    row = rows.subject("locator_binding_end", rejected=fault == "before")
    referents = rows.references()
    target = next(value for value in referents if value["type"] == "locator_binding")
    if fault == "missing":
        row["binding_id"] = identity(999)
    elif fault == "wrong":
        row["binding_id"] = identity(35)
    elif fault == "equal":
        row["at"] = target["temporal_scope"]["at"]
    elif fault.startswith("nanosecond"):
        target["temporal_scope"]["at"] = "2026-01-01T00:00:00.000000002Z"
        row["at"] = (
            "2026-01-01T00:00:00.000000001Z"
            if fault.endswith("before")
            else "2026-01-01T00:00:00.000000002Z"
        )
    elif fault in {"calendar", "duplicate-calendar", "unbounded-calendar"}:
        row["at"] = "2026-02-30T00:00:00Z"
        if fault == "unbounded-calendar":
            target["temporal_scope"] = {"kind": "unknown", "reason": "unavailable"}
    memory = _Validator(
        {"artifacts": [*referents, row]},
        catalog=catalog,
        journal_id=identity(2),
        require_profiles=False,
    )
    path = tmp_path / "order.sqlite3"
    rows.prepare(path, row, end_history=129)
    with closing(connect(path)) as db:
        db.execute(
            "UPDATE binding_projection SET bound_at=? WHERE assertion_id=?",
            (raw_point(target), target["assertion_id"]),
        ).close()
        if fault == "duplicate-calendar":
            memory.binding_ends.add(row["binding_id"])
            db.execute(
                "INSERT INTO maps VALUES ('ends',?,?)",
                (b"j" + canonical_json_bytes(row["binding_id"]), b"null"),
            ).close()
        db.commit()
        before = outputs(db)
        prefix = snapshot(db)["journal_prefix"]
        current = semantic_work(db, row, catalog, operation="row")
        current.initialize()
        finish(current, budget=1)
        if message:
            with pytest.raises(ProvenanceValidationError, match=message) as native:
                memory.row_semantics(row)
            with pytest.raises(ProvenanceValidationError, match=message) as stored:
                current.result()
            assert str(native.value) == str(stored.value)
            assert outputs(db) == before
        else:
            memory.row_semantics(row)
            current.result()
            assert outputs(db) == sorted(
                before + [("ends", b"j" + canonical_json_bytes(row["binding_id"]), b"null")]
            )
        assert snapshot(db)["journal_prefix"] == prefix
        require_reclaimed(db)


def test_late_time_rejection_undoes_only_its_new_binding_end(tmp_path, catalog):
    row = rows.subject("locator_binding_end", rejected=True)
    path = tmp_path / "undo.sqlite3"
    rows.prepare(path, row, end_history=4097)
    with closing(connect(path)) as db:
        before = outputs(db)
        prefix = snapshot(db)["journal_prefix"]
        current = semantic_work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "2"))
        assert len(outputs(db)) == len(before) + 1
        assert db.execute("SELECT 1 FROM generic_semantic_outputs WHERE name='ends'").fetchone()
        finish(current, budget=1)
        with pytest.raises(ProvenanceValidationError, match="binding end precedes"):
            current.result()
        assert outputs(db) == before and snapshot(db)["journal_prefix"] == prefix
        require_reclaimed(db)


def test_stale_binding_end_proposal_cannot_replace_output_or_progress(tmp_path, catalog):
    row = rows.subject("locator_binding_end")
    path = tmp_path / "stale.sqlite3"
    rows.prepare(path, row)
    with closing(connect(path)) as db:
        current = semantic_work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "1"))
        batch = current.prepare(step_budget=1)
        current.advance(step_budget=1)
        before = snapshot(db)
        with pytest.raises(StaleGenericSemantics):
            current.commit(batch)
        assert snapshot(db) == before
        finish(current, budget=1)
        current.result()
        require_reclaimed(db)


def test_reopen_after_binding_end_output_does_not_repeat_duplicate_detection(tmp_path, catalog):
    row = rows.subject("locator_binding_end")
    path = tmp_path / "resume.sqlite3"
    rows.prepare(path, row, end_history=4097)
    with closing(connect(path)) as db:
        current = semantic_work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "2"))
        before = outputs(db)
    with closing(connect(path)) as db:
        current = semantic_work(db, row, catalog, operation="row")
        with pytest.raises(StaleGenericSemantics):
            current.initialize()
        statements = []
        db.set_trace_callback(statements.append)
        current.advance(step_budget=1)
        db.set_trace_callback(None)
        assert not any(
            "FROM maps" in sql or sql.startswith("SELECT object_id,type") for sql in statements
        )
        assert outputs(db) == before
        finish(current, budget=1)
        current.result()
        require_reclaimed(db)


@pytest.mark.parametrize(
    "seam",
    [
        "header",
        "frame-update",
        "output-insert",
        "undo-insert",
        "before-commit",
        "after-commit",
        "rollback-output",
        "rollback-marker",
        "output-retirement",
    ],
)
def test_process_death_in_binding_end_commit_or_undo(tmp_path, catalog, seam):
    rejected = seam.startswith("rollback")
    row = rows.subject("locator_binding_end", rejected=rejected)
    path, input_path = tmp_path / "kill.sqlite3", tmp_path / "row.json"
    rows.prepare(path, row, retained=17, end_history=129)
    input_path.write_bytes(canonical_json_bytes(row))
    with closing(connect(path)) as db:
        prior = outputs(db)
        current = semantic_work(db, row, catalog, operation="row")
        current.initialize()
        position(
            current,
            "reject-outputs"
            if rejected
            else "outputs"
            if seam == "output-retirement"
            else ("row", "1"),
        )
        before = snapshot(db)
    old = "current=work(db,json.loads(Path(input_path).read_text()),ContractCatalog())"
    assert CRASH_PROGRAM.count(old) == 1
    program = CRASH_PROGRAM.replace(old, old[:-1] + ',operation="row")')
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            program,
            str(path),
            str(Path(__file__).parent),
            str(input_path),
            seam,
        ],
        capture_output=True,
        text=True,
    )
    assert child.returncode == 73, child.stderr
    with closing(connect(path)) as db:
        db.execute("DROP TRIGGER IF EXISTS hard_crash").close()
        db.commit()
        assert db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        after = snapshot(db)
        assert after["journal_prefix"] == before["journal_prefix"]
        assert (after != before) == (seam == "after-commit")
        current = semantic_work(db, row, catalog, operation="row")
        finish(current, budget=1)
        if rejected:
            with pytest.raises(ProvenanceValidationError, match="binding end precedes"):
                current.result()
            assert outputs(db) == prior
        else:
            current.result()
            assert outputs(db) == sorted(prior + rows.native(row, catalog))
        require_reclaimed(db)


@pytest.mark.parametrize("history", [0, 1, 15, 16, 17, 129, 4097])
def test_cold_binding_end_step_has_independent_retained_set_history(tmp_path, catalog, history):
    row = rows.subject("locator_binding_end")
    path = tmp_path / "cold.sqlite3"
    rows.prepare(path, row, retained=17 if history == 0 else 1, end_history=history)
    with closing(connect(path)) as db:
        db.execute(
            "UPDATE assertions SET value=? WHERE object_id=?",
            (b"unread-binding-payload" * 65536, identity(36)),
        ).close()
        db.commit()
        current = semantic_work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "1"))
    binding, projection, _ = binding_projection(GenericSemantics.advance)
    with closing(connect(path, factory=CommonWorkConnection)) as db:
        current = semantic_work(db, row, catalog, operation="row")
        retained = workspace_rows(db)
        before = snapshot(db)["journal_prefix"]
        byte_budget = 16 * 1024 * 1024
        sample = measure_semantic_operation(
            current,
            measured_operation="advance",
            step_budget=1,
            byte_budget=byte_budget,
            retained_rows=retained,
            metadata_row_octets=16384,
        )
        metrics = sample["metrics"]
        validate_execution_samples(projection, binding.declaration.id, [sample])
        assert metrics["source_payload_octets"] == metrics["adjacency_rows"] == 0
        assert snapshot(db)["journal_prefix"] == before
        finish(current, budget=1)
        current.result()
        assert len(outputs(db)) == history + 1
        require_reclaimed(db)


@pytest.mark.growth_witness(
    "riverhog-binding-time-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._binding_projection.read_binding_time",
)
def test_actual_cold_binding_time_reads(tmp_path, record_property):
    binding = growth_binding(read_binding_time)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for ordinal, (history, mode) in enumerate(
        (count, mode) for count in (0, 1, 15, 16, 17, 129, 1025, 4097) for mode in MODES
    ):
        path = tmp_path / f"binding-{ordinal}.sqlite3"
        row = prepare(path, mode, history=history, payload=1048576 if history in (17, 4097) else 0)
        with closing(connect(path, factory=WorkConnection)) as db:
            with closing(db.execute("SELECT count(*) FROM assertions")) as cursor:
                retained = cursor.fetchone()[0]
            with db.observe() as metrics:
                check(db, row, mode)
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {"indexed_assertions": retained},
                "metrics": dict(metrics),
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
    assert len(samples) == 80
    record_property("growth_samples", samples)
