from __future__ import annotations

import copy
import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import validate_execution_samples
from riverhog_provenance import _generic_semantics as semantics
from riverhog_provenance import assertion, create_journal, validate_graph, validate_journal
from riverhog_provenance._generic_semantics import (
    GenericSemantics,
    GenericSemanticsCapacity,
    StaleGenericSemantics,
)
from riverhog_provenance._semantic_program import MemoryStack, Nodes, Program
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.graph import _Validator
from test_generic_semantics import (
    CRASH_PROGRAM,
    CommonWorkConnection,
    agent,
    binding_projection,
    connect,
    create,
    finish,
    identity,
    metadata,
    outputs,
    position,
    require_reclaimed,
    snapshot,
    subject,
    work,
    workspace_rows,
)


def content(count=17, *, rejected=False):
    digests = [{"algorithm": "sha-256", "value": "a" * 64}]
    digests.extend(
        {"algorithm": "other", "algorithm_uri": f"urn:test:digest:{n}", "value": str(n)}
        for n in range(1, count)
    )
    if rejected:
        duplicate = dict(digests[min(1, len(digests) - 1)])
        duplicate["value"] = "b" * 64
        digests.append(duplicate)
    return {"size_bytes": "0", "digests": digests}


def row(count=17, *, containers=1, rejected=False, aliased=False):
    value = subject(0)
    shared = {"type": "digest", "value": content(count)}
    value["metadata"] = [metadata(9000)]
    for n in range(containers):
        typed = shared if aliased else copy.deepcopy(shared)
        if rejected and n == containers - 1:
            typed = {"type": "digest", "value": content(count, rejected=True)}
        value["metadata"].append(metadata(n, typed))
    return value


def native(value, catalog):
    validator = _Validator(
        {"artifacts": [value, agent()]},
        catalog=catalog,
        journal_id=identity(2),
        require_profiles=False,
    )
    validator.generic(value)
    return sorted(
        (name, b"b" + key, canonical_json_bytes(item))
        for name, values in (("externals", validator.externals), ("profiles", validator.profiles))
        for key, item in values.items()
    )


def frame(current):
    state = current.load()
    raw = current.db.execute(
        "SELECT value FROM generic_semantic_frames WHERE attempt_id=? AND frame_id=?",
        (current.attempt_id, state.top),
    ).fetchone()[0]
    return json.loads(raw)


def typed_digest(current):
    for _ in range(10000):
        value = frame(current)
        if value["operation"] == "typed":
            if current.atom.nodes.values[int(value["node"])]["type"] == "digest":
                return
        current.advance(step_budget=1)
    raise AssertionError("fixture did not reach its typed digest")


def content_cursor(current, cursor):
    position(current, ("content", "0"))
    while int(frame(current)["cursor"]) < cursor:
        current.advance(step_budget=min(64, cursor - int(frame(current)["cursor"])))
    assert int(frame(current)["cursor"]) == cursor


@pytest.mark.parametrize("budget", [1, 15, 16, 17, 64])
@pytest.mark.parametrize("containers", [1, 3])
@pytest.mark.parametrize("rejected", [False, True])
def test_reopen_content_units_preserves_scoped_native_results(
    tmp_path, catalog, budget, containers, rejected
):
    value = row(containers=containers, rejected=rejected)
    if rejected:
        with pytest.raises(ProvenanceValidationError, match="duplicate digest algorithm"):
            native(value, catalog)
        expected = []
    else:
        expected = native(value, catalog)
    path = tmp_path / "content.sqlite3"
    create(path, value)
    with closing(connect(path)) as db:
        work(db, value, catalog).initialize()
        prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
    for _ in range(10000):
        with closing(connect(path)) as db:
            current = work(db, value, catalog)
            state = current.advance(step_budget=budget)
            assert not db.in_transaction
            assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
            if state.done:
                if rejected:
                    for _ in range(2):
                        with pytest.raises(
                            ProvenanceValidationError, match="duplicate digest algorithm"
                        ):
                            current.result()
                else:
                    current.result()
                assert outputs(db) == expected
                require_reclaimed(db)
                break
    else:
        raise AssertionError("content fixture did not finish")


@pytest.mark.parametrize("mode", ["valid", "duplicate", "aliased"])
def test_public_typed_digest_results_match_native_and_stored_graphs(graph, who, catalog, mode):
    values = row(containers=2, rejected=mode == "duplicate", aliased=mode == "aliased")
    records = values["metadata"][1:]
    for item in records:
        item.update(status="digest_only", sensitivity="public")
    context = assertion("context", who, kind="opaque", metadata=records)
    graph.setdefault("contexts", []).append(context)
    if mode == "duplicate":
        with pytest.raises(ProvenanceValidationError, match="duplicate digest algorithm") as memory:
            validate_graph(graph, catalog=catalog)
        with pytest.raises(ProvenanceValidationError, match="duplicate digest algorithm") as stored:
            create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        assert str(memory.value) == str(stored.value)
    else:
        validate_graph(graph, catalog=catalog)
        raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(raw, catalog=catalog) as summary:
            stored_context = next(
                item for item in summary.graph["contexts"] if item["id"] == context["id"]
            )
            assert stored_context["metadata"] == records


def test_one_content_step_does_not_drain_the_digest_collection(tmp_path, catalog, monkeypatch):
    value = row(129)
    path = tmp_path / "one.sqlite3"
    create(path, value)
    with closing(connect(path)) as db:
        current = work(db, value, catalog)
        current.initialize()
        typed_digest(current)

        def forbidden(*_arguments):
            raise AssertionError("typed digests require their checkpointed child")

        monkeypatch.setattr(semantics._ProjectedValidator, "content", forbidden)
        current.advance(step_budget=1)
        assert frame(current)["operation"] == "content"
        assert frame(current)["cursor"] == "0"
        before = db.execute("SELECT count(*) FROM generic_semantic_keys").fetchone()[0]
        current.advance(step_budget=1)
        assert frame(current)["cursor"] == "1"
        assert db.execute("SELECT count(*) FROM generic_semantic_keys").fetchone()[0] == before + 1
        finish(current)
        current.result()
        require_reclaimed(db)


def test_one_content_step_has_constant_python_control_work(catalog):
    counts = []
    for count in (1, 15, 16, 17, 129, 1025, 4097):
        descriptor = content(count)
        validator = _Validator({}, catalog=catalog, journal_id=None, require_profiles=False)
        stack = MemoryStack("content")
        program = Program(validator, Nodes(descriptor), stack, set())
        events = []

        def trace(current, event, _argument, events=events):
            if current.f_code is not Program.step.__code__:
                return None
            current.f_trace_opcodes = True
            if event == "opcode":
                events.append(event)
            return trace

        previous = sys.gettrace()
        observed_frame = sys._getframe()
        previous_opcodes = observed_frame.f_trace_opcodes
        observed_frame.f_trace_opcodes = True
        try:
            sys.settrace(trace)
            program.step()
        finally:
            sys.settrace(previous)
            observed_frame.f_trace_opcodes = previous_opcodes
        counts.append(len(events))
        assert stack.peek().cursor == 1
    assert min(counts) > 0 and max(counts) <= min(counts) + 256


def test_content_control_measurement_works_in_a_fresh_process():
    program = (
        "import sys;sys.path.insert(0,sys.argv[1]);"
        "from test_content_semantics import "
        "test_one_content_step_has_constant_python_control_work;"
        "from riverhog_provenance_contracts import ContractCatalog;"
        "test_one_content_step_has_constant_python_control_work(ContractCatalog())"
    )
    child = subprocess.run(
        [sys.executable, "-c", program, str(Path(__file__).parent)],
        capture_output=True,
        text=True,
    )
    assert child.returncode == 0, child.stderr


def test_wide_digest_key_admission_stale_proposal_and_reopen(tmp_path, catalog):
    value = row(2)
    value["metadata"][1]["value"]["value"]["digests"][1]["algorithm_uri"] += "x" * 80000
    path = tmp_path / "wide.sqlite3"
    create(path, value)
    with closing(connect(path)) as db:
        current = work(db, value, catalog)
        current.initialize()
        content_cursor(current, 1)
        before = snapshot(db)
        with pytest.raises(GenericSemanticsCapacity):
            current.advance(step_budget=1, byte_budget=16384)
        assert snapshot(db) == before
        proposal = current.prepare(step_budget=1)
        current.advance(step_budget=1)
        after = snapshot(db)
        with pytest.raises(StaleGenericSemantics):
            current.commit(proposal)
        assert snapshot(db) == after
    with closing(connect(path)) as db:
        current = work(db, value, catalog)
        finish(current)
        current.result()
        assert outputs(db) == native(value, catalog)
        require_reclaimed(db)


@pytest.mark.parametrize(
    "change", [{"cursor": "999"}, {"stage": "2"}, {"node": "0"}, {"node": "999999"}]
)
def test_content_frame_corruption_fails_before_mutation(tmp_path, catalog, change):
    value = row()
    path = tmp_path / "corrupt.sqlite3"
    create(path, value)
    with closing(connect(path)) as db:
        current = work(db, value, catalog)
        current.initialize()
        position(current, ("content", "0"))
        retained = {**frame(current), **change}
        db.execute(
            "UPDATE generic_semantic_frames SET value=? WHERE attempt_id=? AND frame_id=?",
            (canonical_json_bytes(retained).decode(), current.attempt_id, current.load().top),
        ).close()
        db.commit()
        before = snapshot(db)
        with pytest.raises(RuntimeError, match="corrupt generic semantic frame"):
            current.advance(step_budget=1)
        assert snapshot(db) == before


def test_corrupt_retained_content_rejection_does_not_replay_a_different_entry(tmp_path, catalog):
    value = row(rejected=True)
    path = tmp_path / "rejection.sqlite3"
    create(path, value)
    with closing(connect(path)) as db:
        current = work(db, value, catalog)
        current.initialize()
        finish(current)
        state = current.load()
        failure = json.loads(state.failure)
        failure["cursor"] = str(len(value["metadata"][1]["value"]["value"]["digests"]))
        checkpoint = json.loads(state.to_json())
        checkpoint["failure"] = canonical_json_bytes(failure).decode()
        db.execute(
            "UPDATE journal_generic_semantics SET checkpoint=?",
            (canonical_json_bytes(checkpoint).decode(),),
        ).close()
        db.commit()
        before = snapshot(db)
        with pytest.raises(RuntimeError, match="corrupt retained content rejection"):
            current.result()
        assert snapshot(db) == before


@pytest.mark.parametrize(
    "seam",
    [
        "frame-insert",
        "frame-update",
        "header",
        "metadata-key",
        "before-commit",
        "after-commit",
        "frame-delete",
        "key-cleanup",
        "reject-frames",
        "reject-keys",
        "rollback-output",
        "rollback-marker",
        "output-retirement",
    ],
)
def test_process_death_preserves_content_progress_keys_and_output_undo(tmp_path, catalog, seam):
    rejected = seam.startswith("reject") or seam.startswith("rollback")
    value = row(rejected=rejected)
    path, expected_path = tmp_path / "kill.sqlite3", tmp_path / "expected.sqlite3"
    input_path = tmp_path / "row.json"
    input_path.write_bytes(canonical_json_bytes(value))
    create(path, value)
    with closing(connect(path)) as db:
        current = work(db, value, catalog)
        current.initialize()
        if seam == "frame-insert":
            typed_digest(current)
        else:
            target = (
                seam
                if seam in {"reject-frames", "reject-keys"}
                else "reject-outputs"
                if seam.startswith("rollback")
                else "outputs"
                if seam == "output-retirement"
                else "keys"
                if seam == "key-cleanup"
                else ("content", "1")
                if seam == "frame-delete"
                else ("content", "0")
            )
            position(current, target)
        before = snapshot(db)
        with closing(connect(expected_path)) as target:
            db.backup(target)
    with closing(connect(expected_path)) as db:
        work(db, value, catalog).advance(step_budget=1)
        after = snapshot(db)
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            CRASH_PROGRAM,
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
        assert db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert snapshot(db) == (after if seam == "after-commit" else before)
        db.execute("DROP TRIGGER IF EXISTS hard_crash").close()
        db.commit()
        current = work(db, value, catalog)
        finish(current)
        if rejected:
            with pytest.raises(ProvenanceValidationError, match="duplicate digest algorithm"):
                current.result()
            assert outputs(db) == []
        else:
            current.result()
            assert outputs(db) == native(value, catalog)
        require_reclaimed(db)


def growth_samples(tmp_path, catalog):
    binding, projection, digest = binding_projection(GenericSemantics.advance)
    samples = []
    phases = (
        "content-first",
        "content-middle",
        "content-end",
        "content-pop",
        "duplicate",
        "keys",
        "reject-frames",
        "reject-keys",
        "rejected",
    )
    for phase in phases:
        for count, budget in ((1, 1), (15, 16), (16, 16), (17, 16), (129, 16), (1025, 64)):
            rejected = phase.startswith("reject") or phase == "duplicate"
            value = row(count, rejected=rejected)
            path = tmp_path / f"content-{phase}-{count}.sqlite3"
            create(path, value)
            with closing(sqlite3.connect(":memory:")) as db:
                with closing(connect(path)) as original:
                    original.backup(db)
                if count in (17, 1025):
                    db.executemany(
                        "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            (
                                identity(100000 + n),
                                identity(20),
                                "agents",
                                "agent",
                                None,
                                None,
                                "0",
                                b"unread-history",
                                0,
                            )
                            for n in range(4097)
                        ),
                    ).close()
                    db.execute(
                        "UPDATE assertions SET value=? WHERE assertion_id=?",
                        (b"unread-active-payload" * 32768, identity(21)),
                    ).close()
                    db.commit()
                current = work(db, value, catalog)
                current.initialize()
                if phase == "rejected":
                    finish(current, budget=64)
                elif phase in {"keys", "reject-frames", "reject-keys"}:
                    position(current, phase)
                elif phase == "content-pop":
                    position(current, ("content", "1"))
                else:
                    cursor = (
                        count // 2
                        if phase == "content-middle"
                        else count
                        if phase in {"content-end", "duplicate"}
                        else 0
                    )
                    content_cursor(current, cursor)
                retained = workspace_rows(db)
                prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                envelope = (
                    8192
                    + len(current.binding)
                    + sum(
                        len(item) if isinstance(item, bytes) else len(str(item).encode())
                        for item in prefix
                    )
                )
                with closing(connect(path)) as target:
                    db.backup(target)
            with closing(connect(path, factory=CommonWorkConnection)) as db:
                current = work(db, value, catalog)
                byte_budget = 16 * 1024 * 1024
                with db.observe() as metrics:
                    current.advance(step_budget=budget, byte_budget=byte_budget)
                sample = {
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
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
                assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
                with closing(sqlite3.connect(":memory:")) as suffix_db:
                    db.backup(suffix_db)
                    suffix = work(suffix_db, value, catalog)
                    finish(suffix, budget=64)
                    if rejected:
                        with pytest.raises(
                            ProvenanceValidationError, match="duplicate digest algorithm"
                        ):
                            suffix.result()
                        assert outputs(suffix_db) == []
                    else:
                        suffix.result()
                        assert outputs(suffix_db) == native(value, catalog)
                    require_reclaimed(suffix_db)
    assert len(samples) == 54
    return samples
