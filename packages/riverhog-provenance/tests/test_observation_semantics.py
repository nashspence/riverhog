from __future__ import annotations

import copy
from contextlib import closing

import pytest
import test_row_semantics as rows
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance._index_key import encode_index_key
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.graph import _Validator
from test_generic_semantics import (
    connect,
    finish,
    identity,
    outputs,
    require_reclaimed,
    snapshot,
    work,
)

CASES = (
    "valid",
    "partial",
    "unreadable-partial",
    "metadata-withheld",
    "evidence-empty",
    "metadata-unreadable-contradiction",
    "snapshot-reference-before-capability",
    "external",
    "unknown-extent",
    "capture-missing",
    "capture-type",
    "capture-kind",
    "capture-failed",
    "target-conflict",
    "primary-missing",
    "incomplete-outcome",
    "evidence-missing",
    "evidence-last",
    "snapshot-capability",
    "metadata-capability",
    "metadata-missing",
    "metadata-unreadable",
    "metadata-contradiction",
    "coverage-before-profile",
    "profile-before-capture",
    "content-before-coverage",
)
ERRORS = {
    "evidence-empty": "observation lacks attributed direct measurement evidence",
    "metadata-unreadable-contradiction": "unreadable metadata requires partial/failed coverage",
    "snapshot-reference-before-capability": "wrong reference type: artifact",
    "external": "direct observation requires a local bounded state",
    "unknown-extent": "direct observation requires a local bounded state",
    "capture-missing": "unresolved local identity",
    "capture-type": "wrong reference type: context",
    "capture-kind": "observation capture is not an observation activity",
    "capture-failed": "a failed capture cannot establish complete primary fixity",
    "target-conflict": "one capture cannot observe distinct states",
    "primary-missing": "direct observation requires complete primary-content coverage",
    "incomplete-outcome": "incomplete coverage requires partial capture outcome",
    "evidence-missing": "observation lacks attributed direct measurement evidence",
    "snapshot-capability": "snapshot claim requires stable-version capability",
    "metadata-capability": "native evidence contradicts source capability declaration",
    "metadata-missing": "native metadata has no declared coverage category",
    "metadata-unreadable": "unreadable metadata requires partial/failed coverage",
    "metadata-contradiction": "metadata contradicts its declared coverage",
    "coverage-before-profile": "duplicate profile-qualified coverage category",
    "profile-before-capture": "filesystem profile requires a filesystem occurrence",
    "content-before-coverage": "duplicate digest algorithm",
}


def subject(count=17, mode="valid"):
    value = rows.subject("observation", count)
    value["metadata"] = [
        {"profile_id": "urn:test:profile", "category": f"urn:test:coverage:{n}", "status": "value"}
        for n in range(count)
    ]
    if mode == "external":
        value["state"] = rows.reference("state", 33, external=True)
    elif mode == "capture-missing":
        value["capture_id"] = identity(999)
    elif mode == "capture-type":
        value["capture_id"] = identity(31)
    elif mode == "primary-missing":
        value["coverage"].pop()
    elif mode == "incomplete-outcome":
        value["coverage"][0]["status"] = "partial"
    elif mode == "evidence-missing":
        value["evidence"] = [{"basis": "reported", "asserted_by_agent_id": identity(20)}]
    elif mode == "evidence-last":
        value["evidence"] = [
            {"basis": "direct_measurement", "asserted_by_agent_id": identity(21)}
            for _ in range(count)
        ] + value["evidence"]
    elif mode == "snapshot-capability":
        value["consistency"]["level"] = "immutable_snapshot"
    elif mode == "metadata-capability":
        value["capabilities"]["native_metadata"] = False
    elif mode == "metadata-missing":
        value["metadata"][-1]["category"] = "urn:test:missing"
    elif mode == "metadata-unreadable":
        value["metadata"][-1]["status"] = "unreadable"
    elif mode == "metadata-contradiction":
        value["coverage"][count - 1]["status"] = "not_exposed"
    elif mode in {"coverage-before-profile", "profile-before-capture", "content-before-coverage"}:
        value["profiles"] = [
            {
                "profile": {
                    "schema_id": rows.PROFILE + "/profiles/filesystem-observation.schema.json",
                }
            }
        ]
        value["capture_id"] = identity(999)
        if mode != "profile-before-capture":
            value["coverage"].append(copy.deepcopy(value["coverage"][0]))
        if mode == "content-before-coverage":
            value["content"]["digests"].append(copy.deepcopy(value["content"]["digests"][0]))
    if mode in {"partial", "unreadable-partial"}:
        value["coverage"][0]["status"] = "partial"
        if mode == "unreadable-partial":
            value["metadata"][0]["status"] = "unreadable"
    elif mode == "metadata-withheld":
        value["coverage"][0]["status"] = "withheld"
    elif mode == "evidence-empty":
        value["evidence"] = []
    elif mode == "metadata-unreadable-contradiction":
        value["coverage"][0]["status"] = "not_requested"
        value["metadata"][0]["status"] = "unreadable"
    elif mode == "snapshot-reference-before-capability":
        value["consistency"].update(
            level="immutable_snapshot", snapshot_identifier=snapshot_identifier()
        )
        value["consistency"]["snapshot_identifier"]["authority_id"] = identity(30)
    return value


def referents(mode="valid"):
    values = rows.references()
    if mode == "unknown-extent":
        next(v for v in values if v["id"] == identity(33))["extent"]["kind"] = "unknown"
    if mode in {"capture-kind", "capture-failed"}:
        capture = next(v for v in values if v["id"] == identity(38))
        capture["kind" if mode == "capture-kind" else "outcome"] = (
            "creation" if mode == "capture-kind" else "failed"
        )
    if mode in {"profile-before-capture", "coverage-before-profile", "content-before-coverage"}:
        next(v for v in values if v["type"] == "occurrence")["kind"] = "opaque"
    if mode in {"partial", "unreadable-partial"}:
        next(v for v in values if v["id"] == identity(38))["outcome"] = "partial"
    return values


def native(value, catalog, mode="valid"):
    current = _Validator(
        {"artifacts": [*referents(mode), value]},
        catalog=catalog,
        journal_id=identity(2),
        require_profiles=False,
    )
    if mode == "target-conflict":
        current.capture_targets[identity(38)] = identity(34)
    current.row_semantics(value)
    return sorted(
        (name, encode_index_key(key), canonical_json_bytes(item))
        for name, mapping in (
            ("captures", current.capture_targets),
            ("externals", current.externals),
            ("profiles", current.profiles),
        )
        for key, item in mapping.items()
    )


def prepare(path, value, mode="valid", *, history=129):
    rows.prepare(path, value, retained=history)
    with closing(connect(path)) as db:
        for source in referents(mode):
            db.execute(
                "UPDATE assertions SET type=?,value=? WHERE assertion_id=?",
                (
                    source["type"],
                    b"unread-historical-payload" * 8192,
                    source["assertion_id"],
                ),
            ).close()
            db.execute(
                "UPDATE description_projection SET occurrence_id=?,kind=?,source_context_id=? "
                "WHERE assertion_id=?",
                (
                    source.get("occurrence_id"),
                    source.get("kind"),
                    source.get("source_context_id"),
                    source["assertion_id"],
                ),
            ).close()
            if source["type"] in {"state", "activity"}:
                db.execute(
                    "UPDATE observation_projection SET extent_kind=?,outcome=? "
                    "WHERE assertion_id=?",
                    (
                        source.get("extent", {}).get("kind"),
                        source.get("outcome"),
                        source["assertion_id"],
                    ),
                ).close()
        if mode == "target-conflict":
            db.execute(
                "INSERT INTO maps VALUES ('captures',?,?)",
                (
                    encode_index_key(identity(38)),
                    canonical_json_bytes(identity(34)),
                ),
            ).close()
        db.commit()


@pytest.mark.parametrize("mode", CASES)
@pytest.mark.parametrize("budget", [1, 15, 16, 17, 64])
def test_reopen_observation_preserves_native_errors_and_reclaims_rejection(
    tmp_path, catalog, mode, budget
):
    value = subject(mode=mode)
    if mode in ERRORS:
        with pytest.raises(ProvenanceValidationError, match=ERRORS[mode]) as expected:
            native(value, catalog, mode)
        expected_output = (
            [("captures", encode_index_key(identity(38)), canonical_json_bytes(identity(34)))]
            if mode == "target-conflict"
            else []
        )
    else:
        expected_output = native(value, catalog, mode)
    path = tmp_path / "observation.sqlite3"
    prepare(path, value, mode)
    with closing(connect(path)) as db:
        work(db, value, catalog, operation="row").initialize()
        prefix = snapshot(db)["journal_prefix"]
    for _ in range(10000):
        with closing(connect(path)) as db:
            current = work(db, value, catalog, operation="row")
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
                require_reclaimed(db)
                assert outputs(db) == expected_output
                break
    else:
        pytest.fail("observation continuation did not finish")


def snapshot_identifier():
    return {
        "scheme": "urn:test:snapshot",
        "scope": "context",
        "authority_id": identity(20),
        "value": {"kind": "text", "text": "stable snapshot"},
    }


@pytest.mark.parametrize(
    "seam",
    [
        "header",
        "frame-insert",
        "frame-update",
        "frame-delete",
        "output-insert",
        "undo-insert",
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
def test_actual_process_death_preserves_observation_capture_outputs_and_undo(
    tmp_path, catalog, seam
):
    import subprocess
    import sys
    from pathlib import Path

    from test_generic_semantics import CRASH_PROGRAM, position

    rejected = seam.startswith("reject") or seam.startswith("rollback")
    mode = "metadata-unreadable" if rejected else "valid"
    value = subject(mode=mode)
    value["consistency"]["snapshot_identifier"] = snapshot_identifier()
    path, expected_path = tmp_path / "kill.sqlite3", tmp_path / "expected.sqlite3"
    input_path = tmp_path / "row.json"
    input_path.write_bytes(canonical_json_bytes(value))
    prepare(path, value, mode)
    with closing(connect(path)) as db:
        current = work(db, value, catalog, operation="row")
        current.initialize()
        target = (
            seam
            if seam in {"reject-frames", "reject-keys"}
            else "reject-outputs"
            if seam.startswith("rollback")
            else "keys"
            if seam == "key-cleanup"
            else "outputs"
            if seam == "output-retirement"
            else ("row", "9")
            if seam in {"output-insert", "undo-insert"}
            else ("row", "13")
            if seam == "frame-insert"
            else ("identifier", "2")
            if seam == "frame-delete"
            else ("row", "16")
        )
        position(current, target)
        before = snapshot(db)
        with closing(connect(expected_path)) as other:
            db.backup(other)
    with closing(connect(expected_path)) as db:
        work(db, value, catalog, operation="row").advance(step_budget=1)
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
        current = work(db, value, catalog, operation="row")
        finish(current)
        if rejected:
            with pytest.raises(ProvenanceValidationError, match=ERRORS[mode]):
                current.result()
            assert outputs(db) == []
        else:
            current.result()
            assert outputs(db) == native(value, catalog)
        require_reclaimed(db)


@pytest.mark.parametrize(
    "change",
    [
        {"stage": "18"},
        {"stage": "12", "cursor": "2"},
        {"stage": "16", "cursor": "18"},
        {"stage": "11", "cursor": "1"},
        {"node": "1"},
    ],
)
def test_corrupt_observation_control_frames_fail_without_modifying_progress(
    tmp_path, catalog, change
):
    path = tmp_path / "corrupt.sqlite3"
    value = subject()
    prepare(path, value)
    with closing(connect(path)) as db:
        current = work(db, value, catalog, operation="row")
        current.initialize()
        frame = {"operation": "row", "node": "0", "stage": "0", "cursor": "0", **change}
        db.execute(
            "UPDATE generic_semantic_frames SET value=?", (canonical_json_bytes(frame).decode(),)
        ).close()
        db.commit()
        before = snapshot(db)
        with pytest.raises(RuntimeError, match="corrupt generic semantic frame"):
            current.advance()
        assert snapshot(db) == before


@pytest.mark.parametrize("stage,cursor", [(4, 19), (12, 2), (16, 18)])
def test_rejection_cannot_reconstruct_a_failure_beyond_its_source_collection(
    tmp_path, catalog, stage, cursor
):
    import json
    from dataclasses import replace

    path = tmp_path / "failure.sqlite3"
    value = subject(mode="metadata-unreadable")
    prepare(path, value)
    with closing(connect(path)) as db:
        current = work(db, value, catalog, operation="row")
        current.initialize()
        finish(current)
        state = current.load()
        frame = json.loads(state.failure)
        frame.update(stage=str(stage), cursor=str(cursor))
        db.execute(
            "UPDATE journal_generic_semantics SET checkpoint=?",
            (replace(state, failure=canonical_json_bytes(frame).decode()).to_json(),),
        ).close()
        db.commit()
        before = snapshot(db)
        with pytest.raises(RuntimeError, match="corrupt generic semantic frame"):
            current.result()
        assert snapshot(db) == before


def test_wide_observation_correlation_requires_admission_and_survives_stale_worker(
    tmp_path, catalog
):
    from riverhog_provenance._generic_semantics import (
        GenericSemanticsCapacity,
        StaleGenericSemantics,
    )
    from test_generic_semantics import position

    value = subject(1)
    wide = "urn:test:" + "é" * 40000
    value["coverage"][0]["category"] = wide
    value["metadata"][0]["category"] = wide
    path = tmp_path / "wide.sqlite3"
    prepare(path, value)
    with closing(connect(path)) as db:
        current = work(db, value, catalog, operation="row")
        current.initialize()
        position(current, ("row", "16"))
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
        finish(current)
        current.result()
        require_reclaimed(db)


def memory_copy(path):
    """Keep fixture positioning separate from each measured cold disk unit."""
    db = connect(":memory:")
    with closing(connect(path)) as source:
        source.backup(db)
    return db


def growth_samples(tmp_path, catalog, *, measured_operation="advance"):
    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance._generic_semantics import GenericSemantics
    from test_generic_semantics import (
        CommonWorkConnection,
        binding_projection,
        measure_semantic_operation,
        position,
        workspace_rows,
    )

    binding, projection, digest = binding_projection(getattr(GenericSemantics, measured_operation))
    phases = {
        "state": 6,
        "capture": 7,
        "outcome": 8,
        "target": 9,
        "primary": 10,
        "incomplete": 11,
        "evidence": 12,
        "evidence-last": 12,
        "snapshot": 13,
        "capability": 14,
        "metadata": 16,
        "metadata-last": 16,
        "reject-keys": None,
        "reject-outputs": None,
        "rejected": None,
    }
    samples = []
    for phase, stage in phases.items():
        for count, budget in ((1, 1), (15, 16), (16, 16), (17, 16), (129, 16), (1025, 64)):
            rejected = phase.startswith("reject")
            mode = (
                "metadata-unreadable"
                if rejected
                else "evidence-last"
                if phase == "evidence-last"
                else "valid"
            )
            value = subject(count, mode)
            value["consistency"]["snapshot_identifier"] = snapshot_identifier()
            if phase == "incomplete":
                value["coverage"][0]["status"] = "partial"
            path = tmp_path / f"observation-{phase}-{count}.sqlite3"
            prepare(path, value, mode, history=4097 if count in (17, 1025) else count)
            with closing(memory_copy(path)) as db:
                if phase == "incomplete":
                    db.execute(
                        "UPDATE observation_projection SET outcome='partial' WHERE assertion_id=?",
                        (identity(138),),
                    ).close()
                    db.commit()
                for n in range(4097 if count == 129 else count):
                    db.execute(
                        "INSERT INTO maps VALUES ('captures',?,?)",
                        (
                            encode_index_key(identity(1000000 + n)),
                            canonical_json_bytes(identity(34)),
                        ),
                    ).close()
                db.commit()
                current = work(db, value, catalog, operation="row")
                current.initialize()
                if phase == "rejected":
                    finish(current, budget=64)
                elif phase in {"reject-keys", "reject-outputs"}:
                    position(current, phase)
                else:
                    position(current, ("row", str(stage)))
                    for _ in range(
                        count
                        if phase == "evidence-last"
                        else count - 1
                        if phase == "metadata-last"
                        else 0
                    ):
                        current.advance(step_budget=1)
                retained = workspace_rows(db)
                prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                envelope = (
                    8192
                    + len(current.binding)
                    + sum(len(v) if isinstance(v, bytes) else len(str(v).encode()) for v in prefix)
                )
                history_outputs = [
                    row for row in outputs(db) if row[1] != encode_index_key(identity(38))
                ]
                with closing(connect(path)) as destination:
                    db.backup(destination)
            with closing(connect(path, factory=CommonWorkConnection)) as db:
                current = work(db, value, catalog, operation="row")
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
                assert metrics["source_payload_octets"] == metrics["adjacency_rows"] == 0
                samples.append(sample)
            with closing(memory_copy(path)) as db:
                current = work(db, value, catalog, operation="row")
                finish(current, budget=64)
                if rejected:
                    with pytest.raises(ProvenanceValidationError, match=ERRORS[mode]):
                        current.result()
                    assert outputs(db) == history_outputs
                else:
                    current.result()
                require_reclaimed(db)
    assert len(samples) == 90
    return samples


def measured_call(function, allowed):
    import sys

    events = 0

    def trace(frame, event, _argument):
        nonlocal events
        if event == "call":
            if frame.f_code not in allowed:
                return None
            frame.f_trace_opcodes = True
            return trace
        if event == "opcode":
            events += 1
        return trace

    previous, caller = sys.gettrace(), sys._getframe()
    previous_opcodes = caller.f_trace_opcodes
    caller.f_trace_opcodes = True
    try:
        sys.settrace(trace)
        value = function()
    finally:
        sys.settrace(previous)
        caller.f_trace_opcodes = previous_opcodes
    assert events > 0
    return value, events


@pytest.mark.growth_witness(
    "riverhog-observation-coverage-index-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._observation_coverage.ObservationCoverage.__init__",
)
def test_actual_observation_coverage_index_is_linear_in_rows_and_key_bytes(record_property):
    import inspect

    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance._observation_coverage import ObservationCoverage
    from test_generic_semantics import binding_projection, code_tree

    binding, projection, digest = binding_projection(ObservationCoverage.__init__)
    allowed = code_tree(inspect.unwrap(ObservationCoverage.__init__).__code__)
    samples = []
    for count, mode in [(n, "ordinary") for n in (1, 15, 16, 17, 129, 1025, 4097)] + [
        (17, "wide"),
        (17, "duplicate"),
    ]:
        value = subject(count)
        if mode == "wide":
            value["coverage"][0]["category"] = "urn:test:" + "é" * 40000
        elif mode == "duplicate":
            value["coverage"].append({**value["coverage"][0], "status": "partial"})
        index, events = measured_call(lambda value=value: ObservationCoverage(value), allowed)
        assert index.primary_complete
        assert index.incomplete is (mode == "duplicate")
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": digest,
            "dimensions": {
                "input_rows": len(value["coverage"]),
                "key_octets": sum(
                    len(item[key].encode())
                    for item in value["coverage"]
                    for key in ("profile_id", "category")
                ),
            },
            "metrics": {
                "indexed_rows": len(index.statuses),
                "retained_key_octets": sum(
                    len(item.encode()) for pair in index.statuses for item in pair
                ),
                "python_opcode_events": events,
            },
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)
    record_property("growth_samples", samples)


@pytest.mark.parametrize("stage", [10, 11, 12, 16])
def test_one_observation_step_does_not_rescan_coverage_or_metadata(catalog, stage):
    from riverhog_provenance._semantic_program import Frame, MemoryStack, Nodes, Program
    from test_generic_semantics import code_tree

    allowed = (
        code_tree(Program.step.__code__)
        | code_tree(Program._row.__code__)
        | code_tree(Program._observation.__code__)
    )
    counts = []
    for count in (1, 15, 16, 17, 129, 1025, 4097):
        value = subject(count)
        nodes = Nodes(value)
        current = _Validator(
            {"artifacts": [*referents(), value]},
            catalog=catalog,
            journal_id=identity(2),
            require_profiles=False,
        )
        # Raw native membership preparation has separate ownership from the interpreter step.
        current._observation_observer(value["capture_id"], identity(20))
        stack = MemoryStack("row")
        stack.frames = [Frame("row", 0, stage)]
        program = Program(current, nodes, stack, set())
        _value, events = measured_call(program.step, allowed)
        counts.append(events)
    assert max(counts) <= min(counts) + 256


def test_observation_control_counts_work_in_a_fresh_process():
    import subprocess
    import sys
    from pathlib import Path

    program = """import sys
sys.path.insert(0,sys.argv[1])
from riverhog_provenance_contracts import ContractCatalog
from test_observation_semantics import (
 test_one_observation_step_does_not_rescan_coverage_or_metadata as check
)
for stage in (10,11,12,16):
 check(ContractCatalog(),stage)
"""
    completed = subprocess.run(
        [sys.executable, "-c", program, str(Path(__file__).parent)], capture_output=True, text=True
    )
    assert completed.returncode == 0, completed.stderr


def test_ordinary_advance_and_rejected_result_reuse_the_admitted_coverage_index(
    tmp_path, catalog, monkeypatch
):
    from riverhog_provenance._observation_coverage import ObservationCoverage
    from test_generic_semantics import position

    path = tmp_path / "index-owner.sqlite3"
    value = subject(129, "metadata-unreadable")
    prepare(path, value)
    with closing(connect(path)) as db:
        current = work(db, value, catalog, operation="row")
        current.initialize()
        position(current, ("row", "16"))

        def forbidden(*_args, **_kwargs):
            raise AssertionError("the admitted coverage index was rebuilt during continuation")

        monkeypatch.setattr(ObservationCoverage, "__init__", forbidden)
        finish(current, budget=1)
        for _ in range(2):
            with pytest.raises(ProvenanceValidationError, match=ERRORS["metadata-unreadable"]):
                current.result()
        require_reclaimed(db)
        assert outputs(db) == []


@pytest.mark.parametrize("mode", ["valid", "partial", "unreadable", "target-conflict"])
def test_public_graph_and_journal_preserve_observation_correlations(graph, who, catalog, mode):
    from riverhog_provenance import (
        create_journal,
        new_id,
        reference,
        validate_graph,
        validate_journal,
    )
    from test_generic_semantics import metadata

    value = next(v for v in graph["descriptions"] if v["type"] == "observation")
    native_metadata = metadata(1, {"type": "integer", "value": "1"})
    native_metadata.update(status="captured", sensitivity="public")
    value["metadata"] = [native_metadata]
    value["capabilities"]["native_metadata"] = True
    value["coverage"].append(
        {
            "profile_id": native_metadata["profile_id"],
            "category": native_metadata["category"],
            "status": "complete",
        }
    )
    if mode in {"partial", "unreadable"}:
        native_metadata.pop("value")
        native_metadata.update(status="unreadable", reason="source did not return the field")
        if mode == "partial":
            value["coverage"][-1].update(status="partial", reason="one native field is unreadable")
            next(v for v in graph["activities"] if v["id"] == value["capture_id"])["outcome"] = (
                "partial"
            )
    elif mode == "target-conflict":
        state = copy.deepcopy(
            next(v for v in graph["states"] if v["id"] == value["state"]["object_id"])
        )
        state.update(id=new_id(), assertion_id=new_id())
        graph["states"].append(state)
        second = copy.deepcopy(value)
        second.update(id=new_id(), assertion_id=new_id(), state=reference(state["id"], "state"))
        graph["descriptions"].append(second)
    if mode in {"unreadable", "target-conflict"}:
        message = (
            "unreadable metadata requires partial/failed coverage"
            if mode == "unreadable"
            else "one capture cannot observe distinct states"
        )
        with pytest.raises(ProvenanceValidationError, match=message):
            validate_graph(graph, catalog=catalog)
        with pytest.raises(ProvenanceValidationError, match=message):
            create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    else:
        validate_graph(graph, catalog=catalog)
        raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(raw, catalog=catalog) as summary:
            stored = next(v for v in summary.graph["descriptions"] if v["id"] == value["id"])
            assert stored["coverage"] == value["coverage"]
            assert stored["metadata"] == value["metadata"]


@pytest.mark.parametrize("damage", ["different-frame", "different-error"])
def test_rejection_reconstruction_uses_source_facts_rather_than_the_desired_error(
    tmp_path, catalog, damage
):
    import json
    from dataclasses import replace

    path = tmp_path / "authentic-rejection.sqlite3"
    value = subject(mode="metadata-unreadable")
    prepare(path, value)
    with closing(connect(path)) as db:
        current = work(db, value, catalog, operation="row")
        current.initialize()
        finish(current)
        state = current.load()
        if damage == "different-frame":
            frame = json.loads(state.failure)
            frame["cursor"] = "0"
            state = replace(state, failure=canonical_json_bytes(frame).decode())
        else:
            state = replace(state, error_sha256="0" * 64)
        db.execute("UPDATE journal_generic_semantics SET checkpoint=?", (state.to_json(),)).close()
        db.commit()
        before = snapshot(db)
        with pytest.raises(RuntimeError, match="retained semantic rejection"):
            current.result()
        assert snapshot(db) == before


@pytest.mark.parametrize(
    "value",
    [
        b'"' + b"x" * 131072 + b'"',
        b"null",
        b'"not-a-uuid"',
        canonical_json_bytes(identity(34)).decode(),
    ],
    ids=["oversized-json", "json-null", "non-uuid-json", "text-storage"],
)
def test_capture_target_corruption_is_bounded_and_cannot_publish_progress(tmp_path, catalog, value):
    from test_generic_semantics import CommonWorkConnection, position

    path = tmp_path / "capture-map.sqlite3"
    row = subject()
    prepare(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "9"))
        db.execute(
            "INSERT INTO maps VALUES ('captures',?,?)", (encode_index_key(identity(38)), value)
        ).close()
        db.commit()
    with closing(connect(path, factory=CommonWorkConnection)) as db:
        current = work(db, row, catalog, operation="row")
        before = snapshot(db)
        with db.observe() as metrics:
            with pytest.raises(RuntimeError, match="invalid retained capture target"):
                current.advance(step_budget=1)
        assert snapshot(db) == before
        assert metrics["changed_rows"] == metrics["source_payload_octets"] == 0
        assert metrics["returned_metadata_octets"] < 8192


@pytest.mark.growth_witness(
    "riverhog-generic-semantic-result-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generic_semantics.GenericSemantics.result",
)
def test_actual_cold_terminal_result_is_read_only_and_reconstructs_native_failures(
    tmp_path, catalog, record_property
):
    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance._generic_semantics import _RESULT_BYTES, GenericSemantics
    from test_generic_semantics import (
        CommonWorkConnection,
        binding_projection,
        measure_semantic_result,
        semantic_terminal_dimensions,
        workspace_rows,
    )
    from test_generic_semantics import subject as common_subject

    binding, projection, digest = binding_projection(GenericSemantics.result)
    samples = []
    for mode in (
        "metadata-unreadable",
        "target-conflict",
        "coverage-before-profile",
        "evidence-empty",
        "capture-kind",
        "snapshot-reference-before-capability",
        "content-before-coverage",
        "generic-metadata",
    ):
        for count in (1, 15, 16, 17, 129, 1025):
            operation = "generic" if mode == "generic-metadata" else "row"
            value = common_subject(count) if operation == "generic" else subject(count, mode)
            if operation == "generic":
                value["metadata"].append(copy.deepcopy(value["metadata"][0]))
            path = tmp_path / f"terminal-{mode}-{count}.sqlite3"
            prepare(path, value, mode, history=4097 if count in (17, 1025) else count)
            with closing(memory_copy(path)) as db:
                current = work(db, value, catalog, operation=operation)
                current.initialize()
                finish(current, budget=64)
                assert current.load().phase == "rejected"
                require_reclaimed(db)
                terminal_dimensions = semantic_terminal_dimensions(current)
                retained = workspace_rows(db)
                prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                envelope = (
                    8192
                    + len(current.binding)
                    + sum(len(v) if isinstance(v, bytes) else len(str(v).encode()) for v in prefix)
                )
                before = snapshot(db)
                with closing(connect(path)) as destination:
                    db.backup(destination)
            with closing(connect(path, factory=CommonWorkConnection)) as db:
                current = work(db, value, catalog, operation=operation)
                message = (
                    "duplicate source-qualified native metadata row"
                    if operation == "generic"
                    else ERRORS[mode]
                )
                sample = measure_semantic_result(
                    current,
                    {
                        "retained_rows": retained,
                        "step_budget": 1,
                        "byte_budget": _RESULT_BYTES,
                        "metadata_row_octets": envelope,
                        **terminal_dimensions,
                    },
                    expected_exception=ProvenanceValidationError,
                    pattern=message,
                )
                metrics = sample["metrics"]
                assert snapshot(db) == before
                assert metrics["changed_rows"] == metrics["source_payload_octets"] == 0
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
    from test_edge_row_semantics import terminal_growth_samples

    samples.extend(terminal_growth_samples(tmp_path, catalog))
    from test_comparison_semantics import terminal_growth_samples as comparison_terminal_samples

    samples.extend(comparison_terminal_samples(tmp_path, catalog))
    from test_locator_semantics import terminal_growth_samples as locator_terminal_samples

    samples.extend(locator_terminal_samples(tmp_path, catalog))
    assert len(samples) == 306
    record_property("growth_samples", samples)
