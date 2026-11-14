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
from riverhog_provenance._generic_semantics import GenericSemantics, StaleGenericSemantics
from riverhog_provenance.errors import ProvenanceValidationError
from test_generic_semantics import (
    CRASH_PROGRAM,
    CommonWorkConnection,
    binding_projection,
    connect,
    finish,
    identity,
    measure_semantic_operation,
    outputs,
    position,
    require_reclaimed,
    snapshot,
    work,
    workspace_rows,
)

KINDS = ("usage", "generation", "invalidation")
FAULTS = (
    "valid",
    "external",
    "declared-type",
    "missing-state",
    "wrong-state",
    "own-journal",
    "missing-activity",
    "wrong-activity",
    "state-first",
    "before",
    "after",
    "calendar",
)


def metadata(row):
    return {key: row[key] for key in ("activity_id", "at") if key in row}


def stored_event(db, row, *, value=None):
    db.execute(
        "INSERT INTO maps VALUES (?,?,?)",
        (
            row["type"],
            b"j" + canonical_json_bytes(row["state"]["object_id"]),
            canonical_json_bytes(metadata(row) if value is None else value),
        ),
    ).close()
    db.commit()


def apply_fault(row, fault):
    if fault == "external":
        row["state"] = rows.reference("state", 333, external=True)
    elif fault == "declared-type":
        row["state"]["object_type"] = "agent"
    elif fault == "missing-state":
        row["state"]["object_id"] = identity(999)
    elif fault == "wrong-state":
        row["state"]["object_id"] = identity(31)
    elif fault == "own-journal":
        row["state"] = rows.reference("state", 333, external=True)
        row["state"]["journal_id"] = identity(2)
    elif fault == "missing-activity":
        row["activity_id"] = identity(999)
    elif fault == "wrong-activity":
        row["activity_id"] = identity(31)
    elif fault == "state-first":
        row["state"]["object_id"] = identity(31)
        row["activity_id"] = identity(999)
    elif fault == "before":
        row["at"] = "2025-12-31T00:00:00Z"
    elif fault == "after":
        row["at"] = "2026-01-03T00:00:00Z"
    elif fault == "calendar":
        row["at"] = "2026-02-30T00:00:00Z"


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("fault", FAULTS)
def test_lifecycle_reopen_preserves_native_order_and_rejects_without_outputs(
    tmp_path,
    catalog,
    kind,
    fault,
):
    row = rows.subject(kind)
    apply_fault(row, fault)
    error = None
    try:
        expected = rows.native(row, catalog)
    except ProvenanceValidationError as exc:
        error, expected = exc, []
    if fault not in {"valid", "external"}:
        assert error is not None
    if fault == "state-first":
        assert str(error).startswith("wrong reference type: context")
    path = tmp_path / "lifecycle.sqlite3"
    rows.prepare(path, row, retained=129, aggregate_history=17)
    with closing(connect(path)) as db:
        before_outputs = outputs(db)
        before_prefix = snapshot(db)["journal_prefix"]
        work(db, row, catalog, operation="row").initialize()
    for _ in range(100):
        with closing(connect(path)) as db:
            current = work(db, row, catalog, operation="row")
            state = current.advance(step_budget=1)
            assert not db.in_transaction
            assert snapshot(db)["journal_prefix"] == before_prefix
            if state.done:
                if error is None:
                    current.result()
                    assert outputs(db) == sorted(before_outputs + expected)
                else:
                    with pytest.raises(type(error)) as actual:
                        current.result()
                    assert str(actual.value) == str(error)
                    assert outputs(db) == before_outputs
                require_reclaimed(db)
                break
    else:
        pytest.fail("lifecycle continuation did not finish")


def test_observation_activity_rejects_generation_before_uniqueness_and_time(
    tmp_path,
    catalog,
    monkeypatch,
):
    references = rows.references()
    activity = next(value for value in references if value["type"] == "activity")
    activity["kind"] = "observation"

    def selected_references(*, comparison=False):
        assert not comparison
        return references

    monkeypatch.setattr(rows, "references", selected_references)
    row = rows.subject("generation", rejected=True)
    path = tmp_path / "capture.sqlite3"
    rows.prepare(path, row)
    with pytest.raises(ProvenanceValidationError, match="capture does not generate"):
        rows.native(row, catalog)
    with closing(connect(path)) as db:
        prior = copy.deepcopy(row)
        prior["activity_id"] = identity(999)
        stored_event(db, prior)
        before = outputs(db)
        current = work(db, row, catalog, operation="row")
        current.initialize()
        finish(current, budget=1)
        with pytest.raises(ProvenanceValidationError, match="capture does not generate"):
            current.result()
        assert outputs(db) == before
        require_reclaimed(db)


@pytest.mark.parametrize("kind", ["generation", "invalidation"])
@pytest.mark.parametrize("mode", ["same", "undated", "raw-time", "activity", "time-before"])
def test_compatible_duplicate_events_and_conflicts_preserve_existing_aggregate(
    tmp_path,
    catalog,
    kind,
    mode,
):
    row = rows.subject(kind)
    if mode == "undated":
        del row["at"]
    prior = copy.deepcopy(row)
    if mode == "raw-time":
        prior["at"] = "2026-01-01T12:00:00.000Z"
    elif mode == "activity":
        prior["activity_id"] = identity(999)
    elif mode == "time-before":
        row["at"] = "2025-12-31T00:00:00Z"
    path = tmp_path / "duplicate.sqlite3"
    rows.prepare(path, row)
    with closing(connect(path)) as db:
        stored_event(db, prior)
        before = outputs(db)
        current = work(db, row, catalog, operation="row")
        current.initialize()
        finish(current, budget=1)
        if mode in {"same", "undated"}:
            current.result()
        else:
            with pytest.raises(ProvenanceValidationError, match=f"conflicting known {kind}"):
                current.result()
        assert outputs(db) == before
        require_reclaimed(db)


@pytest.mark.parametrize("kind", ["generation", "invalidation"])
@pytest.mark.parametrize("preexisting_external", [False, True])
def test_late_time_rejection_rolls_back_new_events_and_preserves_old_external_output(
    tmp_path,
    catalog,
    kind,
    preexisting_external,
):
    row = rows.subject(kind, rejected=True)
    row["state"] = rows.reference("state", 333, external=True)
    path = tmp_path / "undo.sqlite3"
    rows.prepare(path, row)
    with closing(connect(path)) as db:
        if preexisting_external:
            value = canonical_json_bytes(row["state"])
            db.execute("INSERT INTO maps VALUES ('externals',?,?)", (b"b" + value, value)).close()
            db.commit()
        before = outputs(db)
        prefix = snapshot(db)["journal_prefix"]
        current = work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "3"))
        assert any(name == kind for name, _, _ in outputs(db))
        assert db.execute("SELECT 1 FROM generic_semantic_outputs WHERE name=?", (kind,)).fetchone()
        finish(current, budget=1)
        with pytest.raises(ProvenanceValidationError, match="event precedes activity start"):
            current.result()
        assert outputs(db) == before
        assert snapshot(db)["journal_prefix"] == prefix
        require_reclaimed(db)


@pytest.mark.parametrize("kind", ["generation", "invalidation"])
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
def test_actual_process_death_in_lifecycle_aggregate_commit_or_undo(tmp_path, catalog, kind, seam):
    rejected = seam.startswith("rollback")
    row = rows.subject(kind, rejected=rejected)
    path, input_path = tmp_path / "kill.sqlite3", tmp_path / "row.json"
    input_path.write_bytes(canonical_json_bytes(row))
    rows.prepare(path, row, retained=17)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        position(
            current,
            (
                "reject-outputs"
                if rejected
                else "outputs"
                if seam == "output-retirement"
                else ("row", "2")
            ),
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
        current = work(db, row, catalog, operation="row")
        finish(current, budget=1)
        if rejected:
            with pytest.raises(ProvenanceValidationError, match="event precedes activity start"):
                current.result()
            assert outputs(db) == []
        else:
            current.result()
            assert outputs(db) == rows.native(row, catalog)
        require_reclaimed(db)


@pytest.mark.parametrize("kind", ["generation", "invalidation"])
def test_stale_aggregate_proposal_cannot_replace_newer_output_or_progress(tmp_path, catalog, kind):
    row = rows.subject(kind)
    path = tmp_path / "stale.sqlite3"
    rows.prepare(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "2"))
        batch = current.prepare(step_budget=1)
        current.advance(step_budget=1)
        before = snapshot(db)
        with pytest.raises(StaleGenericSemantics):
            current.commit(batch)
        assert snapshot(db) == before
        assert outputs(db) == rows.native(row, catalog)


@pytest.mark.parametrize("kind", ["generation", "invalidation"])
def test_ordinary_reopen_loads_after_aggregate_without_repeating_earlier_reads(
    tmp_path,
    catalog,
    kind,
):
    row = rows.subject(kind)
    path = tmp_path / "resume.sqlite3"
    rows.prepare(path, row, aggregate_history=4097)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "3"))
        before = snapshot(db)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        assert current.load().phase == "walk"
        with pytest.raises(StaleGenericSemantics):
            current.initialize()
        statements = []
        db.set_trace_callback(statements.append)
        current.advance(step_budget=1)
        db.set_trace_callback(None)
        assert not any("FROM maps" in sql for sql in statements)
        assert not any(sql.startswith("SELECT object_id,type") for sql in statements)
        assert outputs(db) == sorted(before["maps"])


@pytest.mark.parametrize("kind", ["generation", "invalidation"])
@pytest.mark.parametrize("damage", ["oversized", "extra", "uuid", "timestamp", "encoding"])
def test_corrupt_retained_event_fails_without_reset_or_large_payload_read(
    tmp_path,
    catalog,
    kind,
    damage,
):
    row = rows.subject(kind)
    path = tmp_path / "corrupt.sqlite3"
    rows.prepare(path, row)
    value = metadata(row)
    if damage == "extra":
        value["extra"] = 0
    elif damage == "uuid":
        value["activity_id"] = "untrusted"
    elif damage == "timestamp":
        value["at"] = "x" * 31
    encoded = (
        canonical_json_bytes({"activity_id": identity(35), "extra": "x" * 262144})
        if damage == "oversized"
        else canonical_json_bytes(value)
    )
    if damage == "encoding":
        encoded = json.dumps(value).encode()
    with closing(connect(path)) as db:
        db.execute(
            "INSERT INTO maps VALUES (?,?,?)",
            (
                kind,
                b"j" + canonical_json_bytes(row["state"]["object_id"]),
                encoded,
            ),
        ).close()
        db.commit()
        current = work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "2"))
        before = snapshot(db)
        statements = []
        db.set_trace_callback(statements.append)
        with pytest.raises(RuntimeError, match="lifecycle metadata"):
            current.advance(step_budget=1)
        db.set_trace_callback(None)
        assert snapshot(db) == before and not db.in_transaction
        if damage == "oversized":
            assert not any(sql.startswith("SELECT value FROM maps") for sql in statements)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("history", [0, 1, 15, 16, 17, 129, 4097])
def test_cold_lifecycle_unit_uses_metadata_with_independent_aggregate_history(
    tmp_path,
    catalog,
    kind,
    history,
):
    row = rows.subject(kind)
    path = tmp_path / "cold.sqlite3"
    rows.prepare(path, row, retained=17 if history == 0 else 1, aggregate_history=history)
    with closing(connect(path)) as db:
        # The referents' documentary bodies remain unread even at large sizes.
        db.execute(
            "UPDATE assertions SET value=? WHERE object_id IN (?,?)",
            (
                b"unread-retained-source" * 65536,
                identity(33),
                identity(35),
            ),
        ).close()
        db.commit()
        current = work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "2"))
        retained = workspace_rows(db)
        envelope = 8192 + len(current.binding)
        prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
        envelope += sum(len(v) if isinstance(v, bytes) else len(str(v).encode()) for v in prefix)
    binding, projection, _ = binding_projection(GenericSemantics.advance)
    with closing(connect(path, factory=CommonWorkConnection)) as db:
        current = work(db, row, catalog, operation="row")
        sample = measure_semantic_operation(
            current,
            measured_operation="advance",
            step_budget=1,
            byte_budget=65536,
            retained_rows=retained,
            metadata_row_octets=envelope,
        )
        metrics = sample["metrics"]
        validate_execution_samples(projection, binding.declaration.id, [sample])
        assert metrics["source_payload_octets"] == metrics["adjacency_rows"] == 0
