from __future__ import annotations

import copy
import json
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
import test_row_semantics as rows
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import validate_execution_samples
from riverhog_provenance import (
    assertion,
    create_journal,
    reference,
    validate_graph,
    validate_journal,
)
from riverhog_provenance._description_projection import read_description_metadata
from riverhog_provenance._generic_semantics import GenericSemantics
from riverhog_provenance._index_key import encode_index_key
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.graph import PROFILE, _Validator
from test_assertion_staging import (
    contents,
    proposed_octets,
)
from test_assertion_staging import (
    create as create_staging,
)
from test_assertion_staging import (
    work as stage_work,
)
from test_content_semantics import content
from test_entry_preparation import WorkConnection
from test_generic_semantics import (
    CRASH_PROGRAM,
    CommonWorkConnection,
    binding_projection,
    connect,
    finish,
    identity,
    outputs,
    position,
    require_reclaimed,
    snapshot,
    work,
    workspace_rows,
)

CASES = (
    "valid",
    "external-known",
    "external-missing",
    "state-unresolved",
    "state-wrong",
    "state-incompatible",
    "context-wrong",
    "context-mismatch",
    "content-before-coverage",
    "coverage-before-profile",
    "profile-local-opaque",
    "profile-external-known",
    "profile-untyped-occurrence",
    "context-untyped-occurrence",
    "external-own-journal",
)
ERRORS = {
    "state-unresolved": "unresolved local identity",
    "state-wrong": "wrong reference type: context",
    "state-incompatible": "reference declares an incompatible object type",
    "context-wrong": "wrong reference type: agent",
    "context-mismatch": "subject context differs from occurrence context",
    "content-before-coverage": "duplicate digest algorithm",
    "coverage-before-profile": "duplicate profile-qualified coverage category",
    "profile-local-opaque": "filesystem profile requires a filesystem occurrence",
    "profile-external-known": "filesystem profile requires a filesystem occurrence",
    "context-untyped-occurrence": "wrong reference type: agent",
    "external-own-journal": "an external reference cannot point into its own journal",
}


def subject(count=17, mode="valid"):
    row = rows.subject("reported_description", count)
    row["content"] = content(count)
    if mode.startswith("external-") or mode in {
        "coverage-before-profile",
        "profile-external-known",
    }:
        row["state"] = rows.reference(
            "state", 999 if mode == "external-missing" else 33, external=True
        )
    if mode == "external-own-journal":
        row["state"]["journal_id"] = identity(2)
    if mode == "state-unresolved":
        row["state"]["object_id"] = identity(999)
    elif mode == "state-wrong":
        row["state"]["object_id"] = identity(31)
    elif mode == "state-incompatible":
        row["state"]["object_type"] = "artifact"
    elif mode == "context-wrong":
        row["source_context_id"] = identity(20)
    if mode in {"content-before-coverage", "coverage-before-profile"}:
        row["coverage"].append(dict(row["coverage"][0]))
    if mode == "content-before-coverage":
        row["content"] = content(count, rejected=True)
    if mode.startswith("profile-") or mode in {
        "coverage-before-profile",
        "context-untyped-occurrence",
    }:
        row["profiles"].append(
            {"profile": {"schema_id": PROFILE + "/profiles/filesystem-observation.schema.json"}}
        )
    if mode == "profile-untyped-occurrence":
        del row["source_context_id"]
    return row


def referents(mode="valid"):
    values = copy.deepcopy(rows.references())
    occurrence = next(v for v in values if v["type"] == "occurrence")
    if mode == "context-mismatch":
        occurrence["source_context_id"] = identity(999)
    elif mode == "profile-local-opaque":
        occurrence["kind"] = "opaque"
    elif mode in {"profile-untyped-occurrence", "context-untyped-occurrence"}:
        occurrence["type"] = "agent"
    return values


def prepare(path, row, mode="valid", *, history=0):
    rows.prepare(path, row, retained=history)
    with closing(connect(path)) as db:
        for v in referents(mode):
            db.execute(
                "UPDATE assertions SET type=?,value=? WHERE assertion_id=?",
                (v["type"], canonical_json_bytes(v), v["assertion_id"]),
            ).close()
            db.execute(
                "INSERT INTO description_projection VALUES (?,?,?,?) "
                "ON CONFLICT(assertion_id) DO UPDATE SET occurrence_id=excluded.occurrence_id, "
                "kind=excluded.kind,source_context_id=excluded.source_context_id",
                (
                    v["assertion_id"],
                    v.get("occurrence_id"),
                    v.get("kind"),
                    v.get("source_context_id"),
                ),
            ).close()
        db.commit()


def native(row, catalog, mode="valid"):
    v = _Validator(
        {"artifacts": [*referents(mode), row]},
        catalog=catalog,
        journal_id=identity(2),
        require_profiles=False,
    )
    v.row_semantics(row)
    return sorted(
        (name, encode_index_key(key), canonical_json_bytes(value))
        for name, values in (("externals", v.externals), ("profiles", v.profiles))
        for key, value in values.items()
    )


@pytest.mark.parametrize("mode", CASES)
@pytest.mark.parametrize("budget", [1, 15, 16, 17, 64])
def test_description_reopen_preserves_native_diagnostic_order_and_outputs(
    tmp_path, catalog, mode, budget
):
    row = subject(mode=mode)
    if mode in ERRORS:
        with pytest.raises(ProvenanceValidationError, match=ERRORS[mode]) as expected:
            native(row, catalog, mode)
        expected_output = []
    else:
        expected_output = native(row, catalog, mode)
    path = tmp_path / "description.sqlite3"
    prepare(path, row, mode, history=129)
    with closing(connect(path)) as db:
        work(db, row, catalog, operation="row").initialize()
        prefix = snapshot(db)["journal_prefix"]
    for _ in range(10000):
        with closing(connect(path)) as db:
            current = work(db, row, catalog, operation="row")
            state = current.advance(step_budget=budget)
            assert not db.in_transaction and snapshot(db)["journal_prefix"] == prefix
            if state.done:
                if mode in ERRORS:
                    for _ in range(2):
                        with pytest.raises(type(expected.value)) as actual:
                            current.result()
                        assert str(actual.value) == str(expected.value)
                else:
                    current.result()
                assert outputs(db) == expected_output
                require_reclaimed(db)
                break
    else:
        pytest.fail("description continuation did not finish")


@pytest.mark.parametrize("mode", ["valid", "duplicate-content", "duplicate-coverage"])
def test_public_description_native_and_journal_agree(graph, who, catalog, mode):
    coverage = [
        {"profile_id": "urn:test:profile", "category": "urn:test:category", "status": "complete"}
    ]
    if mode == "duplicate-coverage":
        coverage.append(
            {**coverage[0], "status": "partial", "reason": "incomplete imported evidence"}
        )
    value = assertion(
        "reported_description",
        who,
        state=reference(graph["states"][0]["id"], "state"),
        content=content(17, rejected=mode == "duplicate-content"),
        coverage=coverage,
    )
    graph["descriptions"].append(value)
    if mode == "valid":
        validate_graph(graph, catalog=catalog)
        raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(raw, catalog=catalog) as summary:
            stored = next(row for row in summary.graph["descriptions"] if row["id"] == value["id"])
            assert stored["content"] == value["content"]
            assert stored["coverage"] == coverage
    else:
        message = (
            "duplicate digest algorithm"
            if mode == "duplicate-content"
            else "duplicate profile-qualified coverage category"
        )
        with pytest.raises(ProvenanceValidationError, match=message):
            validate_graph(graph, catalog=catalog)
        with pytest.raises(ProvenanceValidationError, match=message):
            create_journal(graph, recorded_by_agent_id=who, catalog=catalog)


@pytest.mark.parametrize("field", ["occurrence_id", "kind", "source_context_id"])
@pytest.mark.parametrize(
    "value",
    ["x" * 131072, b"x" * 131072, "é", "", b"\x00"],
    ids=("oversized-text", "oversized-blob", "non-ascii", "empty-text", "nul-blob"),
)
def test_description_corruption_is_rejected_after_bounded_metadata_read(tmp_path, field, value):
    path = tmp_path / "corrupt.sqlite3"
    create_staging(path)
    v = rows.references()[0]
    with closing(connect(path, factory=WorkConnection)) as db:
        insert(db, v)
        db.execute("UPDATE description_projection SET " + field + "=?", (value,)).close()
        db.commit()
        before = contents(db)
        with db.observe() as metrics:
            with pytest.raises(RuntimeError, match="invalid staged description"):
                read_description_metadata(db, v["id"])
        assert metrics["returned_metadata_octets"] <= 256
        assert metrics["historical_payload_octets"] == metrics["changed_rows"] == 0
        assert contents(db) == before


def insert(db, row, *, active=1, projection=True):
    db.execute(
        "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
        (
            row["assertion_id"],
            row["id"],
            "artifacts",
            row["type"],
            None,
            None,
            "0",
            canonical_json_bytes(row),
            active,
        ),
    ).close()
    if projection:
        db.execute(
            "INSERT INTO description_projection VALUES (?,?,?,?)",
            (
                row["assertion_id"],
                row.get("occurrence_id"),
                row.get("kind"),
                row.get("source_context_id"),
            ),
        ).close()


@pytest.mark.parametrize("mode", ["valid", "missing", "wrong", "retired", "missing-projection"])
@pytest.mark.parametrize("query_only", [False, True])
def test_description_reads_close_cursors_and_skip_historical_payloads(tmp_path, mode, query_only):
    path = tmp_path / "read.sqlite3"
    create_staging(path)
    row = next(v for v in referents() if v["type"] == "state")
    if mode == "wrong":
        row["type"] = "agent"
    row["note"] = "x" * 1048576
    with closing(connect(path, factory=WorkConnection)) as db:
        prior = {**row, "assertion_id": identity(999)}
        insert(db, prior, active=0)
        if mode != "missing":
            insert(db, row, active=int(mode != "retired"), projection=mode != "missing-projection")
        db.commit()
        if query_only:
            db.execute("PRAGMA query_only=ON").close()
        with db.observe() as metrics:
            check_metadata(db, row, "state", mode)
        assert metrics["sql_statements"] == (1 if mode in {"missing", "wrong", "retired"} else 2)
        assert metrics["changed_rows"] == metrics["historical_payload_octets"] == 0
        assert metrics["returned_metadata_octets"] <= 256 and not db.in_transaction


def check_metadata(db, row, kind, mode):
    if mode in {"missing", "retired"}:
        with pytest.raises(ProvenanceValidationError, match="unresolved local identity"):
            read_description_metadata(db, row["id"], kind)
    elif mode == "wrong":
        with pytest.raises(ProvenanceValidationError, match="wrong reference type"):
            read_description_metadata(db, row["id"], kind)
    elif mode == "missing-projection":
        with pytest.raises(RuntimeError, match="missing staged description"):
            read_description_metadata(db, row["id"], kind)
    else:
        actual = read_description_metadata(db, row["id"], kind)
        assert actual == {
            k: v
            for k, v in row.items()
            if k in {"id", "type", "occurrence_id", "kind", "source_context_id"}
        }


@pytest.mark.parametrize("kind", ["state", "occurrence", "agent"])
def test_description_projection_is_admitted_and_committed_with_staging(tmp_path, kind):
    path = tmp_path / "stage.sqlite3"
    create_staging(path)
    row = next(v for v in referents() if v["type"] == kind)
    category = {"state": "states", "occurrence": "occurrences", "agent": "agents"}[kind]
    from riverhog_provenance.journal import JournalFrame

    source = JournalFrame(
        canonical_json_bytes(
            {"journal_id": identity(2), "sequence": "1", "body": {"assertions": {category: [row]}}}
        )
    )
    budget = proposed_octets(category, row)
    from riverhog_provenance._assertion_staging import AssertionStagingCapacity

    with closing(connect(path)) as db:
        current = stage_work(db, source)
        current.initialize()
        before = contents(db)
        with pytest.raises(AssertionStagingCapacity):
            current.advance(row_budget=1, byte_budget=budget - 1)
        assert contents(db) == before
        assert current.advance(row_budget=1, byte_budget=budget).done
        assert db.execute("SELECT * FROM description_projection").fetchall() == [
            (
                row["assertion_id"],
                row.get("occurrence_id"),
                row.get("kind"),
                row.get("source_context_id"),
            )
        ]


@pytest.mark.parametrize(
    "seam",
    [
        "header",
        "frame-insert",
        "frame-update",
        "frame-delete",
        "metadata-key",
        "before-commit",
        "after-commit",
        "key-cleanup",
        "reject-frames",
        "reject-keys",
        "rollback-output",
        "rollback-marker",
        "output-retirement",
    ],
)
def test_process_death_preserves_description_keys_outputs_and_progress(tmp_path, catalog, seam):
    rejected = seam.startswith("reject") or seam.startswith("rollback")
    mode = "coverage-before-profile" if rejected else "external-known"
    row = subject(mode=mode)
    path, expected_path = tmp_path / "kill.sqlite3", tmp_path / "expected.sqlite3"
    input_path = tmp_path / "row.json"
    input_path.write_bytes(canonical_json_bytes(row))
    prepare(path, row, mode)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        target = (
            seam
            if seam in {"reject-frames", "reject-keys"}
            else "reject-outputs"
            if seam.startswith("rollback")
            else "outputs"
            if seam == "output-retirement"
            else "keys"
            if seam == "key-cleanup"
            else ("row", "3")
            if seam == "frame-insert"
            else ("content", "1")
            if seam == "frame-delete"
            else ("row", "4")
        )
        position(current, target)
        before = snapshot(db)
        with closing(connect(expected_path)) as other:
            db.backup(other)
    with closing(connect(expected_path)) as db:
        work(db, row, catalog, operation="row").advance(step_budget=1)
        after = snapshot(db)
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
        assert db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert snapshot(db) == (after if seam == "after-commit" else before)
        db.execute("DROP TRIGGER IF EXISTS hard_crash").close()
        db.commit()
        current = work(db, row, catalog, operation="row")
        finish(current)
        if rejected:
            with pytest.raises(ProvenanceValidationError, match=ERRORS[mode]):
                current.result()
            assert outputs(db) == []
        else:
            current.result()
            assert outputs(db) == native(row, catalog, mode)
        require_reclaimed(db)


def growth_samples(tmp_path, catalog, *, measured_operation="advance"):
    from test_generic_semantics import measure_semantic_operation

    binding, projection, digest = binding_projection(getattr(GenericSemantics, measured_operation))
    samples = []
    for phase in (
        "context",
        "coverage-first",
        "coverage-middle",
        "coverage-end",
        "profile",
        "reject-keys",
        "rejected",
    ):
        for count, budget in ((1, 1), (15, 16), (16, 16), (17, 16), (129, 16), (1025, 64)):
            rejected = phase.startswith("reject")
            mode = "coverage-before-profile" if rejected else "valid"
            row = subject(count, mode)
            if phase == "profile":
                row["profiles"] = [
                    {
                        "profile": {
                            "schema_id": PROFILE + "/profiles/filesystem-observation.schema.json"
                        },
                        "data": {"object_kind": "regular_file"},
                    }
                    for _ in range(count)
                ]
            path = tmp_path / f"description-{phase}-{count}.sqlite3"
            prepare(path, row, mode, history=4097 if count in (17, 1025) else count)
            with closing(connect(path)) as db:
                current = work(db, row, catalog, operation="row")
                current.initialize()
                if phase == "rejected":
                    finish(current, budget=64)
                elif phase == "reject-keys":
                    position(current, "reject-keys")
                else:
                    stage = 2 if phase == "context" else 5 if phase == "profile" else 4
                    position(current, ("row", str(stage)))
                    stop = (
                        count // 2
                        if phase == "coverage-middle"
                        else count
                        if phase == "coverage-end"
                        else 0
                    )
                    for _ in range(stop):
                        current.advance(step_budget=1)
                retained = workspace_rows(db)
                prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                envelope = (
                    8192
                    + len(current.binding)
                    + sum(len(v) if isinstance(v, bytes) else len(str(v).encode()) for v in prefix)
                )
            with closing(connect(path, factory=CommonWorkConnection)) as db:
                current = work(db, row, catalog, operation="row")
                sample = measure_semantic_operation(
                    current,
                    measured_operation=measured_operation,
                    step_budget=budget,
                    byte_budget=131072,
                    retained_rows=retained,
                    metadata_row_octets=envelope,
                )
                metrics = sample["metrics"]
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
                assert metrics["source_payload_octets"] == metrics["adjacency_rows"] == 0
                assert snapshot(db)["journal_prefix"] == [tuple(prefix)]
            with closing(connect(path)) as db:
                current = work(db, row, catalog, operation="row")
                finish(current, budget=64)
                if rejected:
                    with pytest.raises(ProvenanceValidationError, match=ERRORS[mode]):
                        current.result()
                    assert outputs(db) == []
                else:
                    current.result()
                    assert outputs(db) == native(row, catalog)
                require_reclaimed(db)
    assert len(samples) == 42
    return samples


@pytest.mark.growth_witness(
    "riverhog-description-metadata-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._description_projection.read_description_metadata",
)
def test_actual_cold_description_metadata_work(tmp_path, record_property):
    binding, projection, digest = binding_projection(read_description_metadata)
    samples = []
    for kind in ("state", "occurrence", "agent"):
        for mode in ("valid", "missing", "wrong", "retired", "missing-projection"):
            for history in (0, 1, 15, 16, 17, 129, 4097):
                path = tmp_path / f"metadata-{kind}-{mode}-{history}.sqlite3"
                create_staging(path)
                row = next(v for v in referents() if v["type"] == kind)
                if mode == "wrong":
                    row["type"] = "context" if kind != "context" else "agent"
                row["note"] = "x" * 262144
                with closing(connect(path)) as db:
                    if mode != "missing":
                        insert(
                            db,
                            row,
                            active=int(mode != "retired"),
                            projection=mode != "missing-projection",
                        )
                    for n in range(history):
                        v = {
                            **row,
                            "id": identity(1000000 + n),
                            "assertion_id": identity(2000000 + n),
                            "note": "retained",
                        }
                        insert(db, v, active=n % 2)
                    db.commit()
                with closing(connect(path, factory=WorkConnection)) as db:
                    indexed = db.execute("SELECT count(*) FROM assertions").fetchone()[0]
                    with db.observe() as metrics:
                        check_metadata(db, row, kind, mode)
                    sample = {
                        "declaration": binding.declaration.id,
                        "declaration_sha256": digest,
                        "dimensions": {"indexed_assertions": indexed},
                        "metrics": dict(metrics),
                        "delegates": {},
                    }
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
    assert len(samples) == 105
    record_property("growth_samples", samples)


@pytest.mark.parametrize(
    "change",
    [{"stage": "7"}, {"stage": "4", "cursor": "18"}, {"stage": "5", "cursor": "18"}, {"node": "1"}],
)
def test_corrupt_description_frame_refuses_without_replay(tmp_path, catalog, change):
    row = subject()
    path = tmp_path / "cursor.sqlite3"
    prepare(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        value = {"operation": "row", "node": "0", "stage": "0", "cursor": "0", **change}
        db.execute(
            "UPDATE generic_semantic_frames SET value=?", (canonical_json_bytes(value).decode(),)
        ).close()
        db.commit()
        before = snapshot(db)
        with pytest.raises(RuntimeError, match="corrupt generic semantic frame"):
            current.advance()
        assert snapshot(db) == before


def test_corrupt_retained_coverage_failure_cannot_be_reinterpreted(tmp_path, catalog):
    row = subject(mode="coverage-before-profile")
    path = tmp_path / "failure.sqlite3"
    prepare(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        finish(current)
        state = json.loads(
            db.execute("SELECT checkpoint FROM journal_generic_semantics").fetchone()[0]
        )
        failed = json.loads(state["failure"])
        failed["cursor"] = str(len(row["coverage"]))
        state["failure"] = canonical_json_bytes(failed).decode()
        db.execute(
            "UPDATE journal_generic_semantics SET checkpoint=?",
            (canonical_json_bytes(state).decode(),),
        ).close()
        db.commit()
        before = snapshot(db)
        with pytest.raises(RuntimeError, match="corrupt retained coverage rejection"):
            current.result()
        assert snapshot(db) == before


def test_wide_coverage_key_admission_stale_proposal_and_reopen(tmp_path, catalog):
    from riverhog_provenance._generic_semantics import (
        GenericSemanticsCapacity,
        StaleGenericSemantics,
    )

    row = subject(1)
    row["coverage"][0]["category"] = "urn:test:" + "x" * 80000
    path = tmp_path / "wide.sqlite3"
    prepare(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "4"))
        before = snapshot(db)
        with pytest.raises(GenericSemanticsCapacity):
            current.prepare(step_budget=1, byte_budget=65536)
        assert snapshot(db) == before
        proposal = current.prepare(step_budget=1, byte_budget=200000)
        current.advance(step_budget=1, byte_budget=200000)
        after = snapshot(db)
        with pytest.raises(StaleGenericSemantics):
            current.commit(proposal)
        assert snapshot(db) == after
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        finish(current)
        current.result()
        require_reclaimed(db)


@pytest.mark.parametrize("stage", [4, 5])
def test_one_description_collection_step_has_constant_python_control_work(catalog, stage):
    from riverhog_provenance._semantic_program import Frame, MemoryStack, Nodes, Program
    from test_generic_semantics import code_tree

    allowed = (
        code_tree(Program.step.__code__)
        | code_tree(Program._row.__code__)
        | code_tree(Program.coverage_key.__code__)
    )
    counts = []
    for count in (1, 15, 16, 17, 129, 1025, 4097):
        row = subject(count)
        nodes = Nodes(row)
        stack = MemoryStack("row")
        stack.frames = [Frame("row", 0, stage)]
        validator = _Validator(
            {"artifacts": [*referents(), row]},
            catalog=catalog,
            journal_id=identity(2),
            require_profiles=False,
        )
        program = Program(validator, nodes, stack, set())
        events = [0]

        def trace(frame, event, _argument, events=events):
            if event == "call":
                if frame.f_code not in allowed:
                    return None
                frame.f_trace_opcodes = True
                return trace
            if event == "opcode":
                events[0] += 1
            return trace

        previous = sys.gettrace()
        caller = sys._getframe()
        previous_opcodes = caller.f_trace_opcodes
        caller.f_trace_opcodes = True
        try:
            sys.settrace(trace)
            program.step()
        finally:
            sys.settrace(previous)
            caller.f_trace_opcodes = previous_opcodes
        counts.append(events[0])
    assert min(counts) > 0 and max(counts) <= min(counts) + 256


def test_description_control_measurement_works_in_a_fresh_process():
    program = """import sys
sys.path.insert(0,sys.argv[1])
from riverhog_provenance_contracts import ContractCatalog
from test_description_semantics import (
    test_one_description_collection_step_has_constant_python_control_work as check,
)
for stage in (4,5):check(ContractCatalog(),stage)
"""
    result = subprocess.run(
        [sys.executable, "-c", program, str(Path(__file__).parent)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
