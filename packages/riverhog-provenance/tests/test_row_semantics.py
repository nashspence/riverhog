from __future__ import annotations

import copy
import hashlib
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import validate_execution_samples
from riverhog_provenance._generic_semantics import GenericSemantics, StaleGenericSemantics
from riverhog_provenance._semantic_program import ROW_TYPES
from riverhog_provenance.errors import ProvenanceValidationError, UnresolvedContractError
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
    outputs,
    position,
    require_reclaimed,
    snapshot,
    work,
)


def reference(kind, ordinal, *, external=False):
    value = {
        "scope": "external" if external else "local",
        "object_id": identity(ordinal),
        "object_type": kind,
    }
    if external:
        value["journal_id"] = identity(99)
    return value


def unknown_profile(ordinal):
    return {
        "profile": {
            "contract_id": "urn:test:unavailable",
            "contract_sha256": "b" * 64,
            "schema_id": f"urn:test:row-profile:{ordinal}",
        },
        "data": {"ordinal": str(ordinal)},
    }


def subject(kind, count=3, *, rejected=False):
    row = {"id": identity(11), "assertion_id": identity(10), "type": kind, "evidence": []}
    if kind == "occurrence":
        row.update(artifact_id=identity(30), source_context_id=identity(31))
    elif kind == "state":
        row.update(
            occurrence_id=identity(32),
            extent={
                "kind": "segment",
                "start_boundary": {
                    "type": "reference",
                    "value": reference("artifact", 130, external=True),
                },
                "end_boundary": {"type": "json", "value": unknown_profile(1)},
            },
        )
    elif kind == "activity":
        row.update(
            started_at="2026-01-01T00:00:00Z",
            ended_at="2026-01-02T00:00:00Z",
            associations=[{"agent_id": identity(20)} for _ in range(count)],
            contexts=[{"context_id": identity(31)} for _ in range(count)],
            details=[unknown_profile(n) for n in range(count)],
            configuration=unknown_profile(count + 10),
        )
        row["configuration_sha256"] = hashlib.sha256(
            canonical_json_bytes(row["configuration"]["data"])
        ).hexdigest()
        if rejected:
            row["configuration_sha256"] = "0" * 64
    elif kind in ("usage", "generation", "invalidation"):
        row.update(
            state=reference("state", 33),
            activity_id=identity(35),
            at="2025-12-31T00:00:00Z" if rejected else "2026-01-01T12:00:00Z",
        )
    elif kind == "locator_binding_end":
        row.update(
            binding_id=identity(36),
            at="2025-12-31T00:00:00Z" if rejected else "2026-01-01T12:00:00Z",
        )
    elif kind == "continuity":
        row.update(earlier_state=reference("state", 33), later_state=reference("state", 34))
    elif kind == "custody_assertion":
        row.update(
            target=reference("artifact", 130, external=True),
            custodian_agent_id=identity(20),
            context_id=identity(31),
        )
    elif kind == "availability_assertion":
        row.update(
            occurrence=reference("occurrence", 132, external=True),
            context_id=identity(31),
            activity_id=identity(35),
        )
    elif kind == "journal_subject":
        row.update(artifact=reference("artifact", 130, external=True), journal_id=identity(2))
    elif kind == "extension":
        row.update(
            subject=reference("artifact", 130, external=True),
            value={"type": "json", "value": unknown_profile(1)},
        )
    else:
        raise AssertionError(kind)
    return row


def references():
    values = [agent()] + [
        {"id": identity(n), "assertion_id": identity(n + 100), "type": kind, "evidence": []}
        for kind, n in (
            ("artifact", 30),
            ("context", 31),
            ("occurrence", 32),
            ("state", 33),
            ("state", 34),
            ("activity", 35),
            ("locator_binding", 36),
        )
    ]
    for value in values:
        if value["type"] == "activity":
            value.update(
                kind="creation", started_at="2026-01-01T00:00:00Z", ended_at="2026-01-02T00:00:00Z"
            )
        elif value["type"] == "locator_binding":
            value["temporal_scope"] = {"kind": "instant", "at": "2026-01-01T00:00:00Z"}
    return values


def prepare(path, row, *, retained=0, aggregate_history=0, end_history=0):
    create(path, row)
    with closing(connect(path)) as db:
        for value in references()[1:]:
            db.execute(
                "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    value["assertion_id"],
                    value["id"],
                    "artifacts",
                    value["type"],
                    None,
                    None,
                    "1",
                    canonical_json_bytes(value),
                    1,
                ),
            ).close()
            if value["type"] == "activity":
                db.execute(
                    "INSERT INTO activity_projection VALUES (?,?,?,?)",
                    (
                        value["assertion_id"],
                        value["kind"],
                        value.get("started_at"),
                        value.get("ended_at"),
                    ),
                ).close()
            elif value["type"] == "locator_binding":
                db.execute(
                    "INSERT INTO binding_projection VALUES (?,?)",
                    (value["assertion_id"], value["temporal_scope"]["at"]),
                ).close()
        for n in range(aggregate_history):
            db.execute(
                "INSERT INTO maps VALUES (?,?,?)",
                (
                    "generation" if n % 2 else "invalidation",
                    b"j" + canonical_json_bytes(identity(10000 + n)),
                    canonical_json_bytes({"activity_id": identity(35)}),
                ),
            ).close()
        for n in range(end_history):
            db.execute(
                "INSERT INTO maps VALUES ('ends',?,?)",
                (b"j" + canonical_json_bytes(identity(20000 + n)), b"null"),
            ).close()
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
                    b"unread-historical-payload",
                    0,
                )
                for n in range(retained)
            ),
        ).close()
        db.execute(
            "UPDATE assertions SET value=? WHERE assertion_id=?",
            (b"unread-active-payload" * 32768, identity(21)),
        ).close()
        db.commit()


def native(row, catalog):
    current = _Validator(
        {"artifacts": [*references(), row]},
        catalog=catalog,
        journal_id=identity(2),
        require_profiles=False,
    )
    current.row_semantics(row)
    return (
        sorted(
            (name, key, canonical_json_bytes(value))
            for name, values in (("externals", current.externals), ("profiles", current.profiles))
            for key, value in values.items()
        )
        + sorted(
            (
                name,
                b"j" + canonical_json_bytes(key),
                canonical_json_bytes(
                    {field: value[field] for field in ("activity_id", "at") if field in value}
                ),
            )
            for name, values in current.lifecycle.items()
            for key, value in values.items()
        )
        + sorted(
            ("ends", b"j" + canonical_json_bytes(key), b"null") for key in current.binding_ends
        )
    )


@pytest.mark.parametrize("kind", sorted(ROW_TYPES))
@pytest.mark.parametrize("budget", [1, 15, 16, 17, 64])
def test_reopen_each_row_unit_preserves_native_outputs(tmp_path, catalog, kind, budget):
    row = subject(kind, 17)
    path = tmp_path / "row.sqlite3"
    prepare(path, row, retained=129)
    with closing(connect(path)) as db:
        work(db, row, catalog, operation="row").initialize()
        prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
    for _ in range(10000):
        with closing(connect(path)) as db:
            current = work(db, row, catalog, operation="row")
            state = current.advance(step_budget=budget)
            assert not db.in_transaction
            assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
            if state.done:
                current.result()
                require_reclaimed(db)
                assert outputs(db) == native(row, catalog)
                break
    else:
        pytest.fail("row continuation did not finish")


@pytest.mark.parametrize(
    "kind,change,message",
    [
        (
            "occurrence",
            {"artifact_id": identity(31), "source_context_id": identity(999)},
            "wrong reference type: context",
        ),
        ("state", {"occurrence_id": identity(31)}, "wrong reference type: context"),
        (
            "activity",
            {"started_at": "2026-02-30T00:00:00Z", "associations": [{"agent_id": identity(999)}]},
            "calendar",
        ),
        (
            "activity",
            {"ended_at": "2025-12-31T00:00:00Z", "associations": [{"agent_id": identity(999)}]},
            "ends before",
        ),
        (
            "activity",
            {
                "associations": [{"agent_id": identity(31)}],
                "contexts": [{"context_id": identity(999)}],
            },
            "wrong reference type: context",
        ),
        ("activity", {"contexts": [{"context_id": identity(20)}]}, "wrong reference type: agent"),
        ("activity", {"configuration_sha256": "0" * 64}, "configuration digest mismatch"),
        ("continuity", {"later_state": reference("state", 33)}, "distinct state"),
        (
            "custody_assertion",
            {"custodian_agent_id": identity(31)},
            "wrong reference type: context",
        ),
        (
            "availability_assertion",
            {"context_id": identity(20), "activity_id": identity(999)},
            "wrong reference type: agent",
        ),
        ("journal_subject", {"journal_id": identity(99)}, "another journal"),
        (
            "extension",
            {"value": {"type": "timestamp", "value": "2026-02-30T00:00:00Z"}},
            "calendar",
        ),
    ],
)
def test_row_failure_order_and_bounded_output_rollback(tmp_path, catalog, kind, change, message):
    row = subject(kind)
    row.update(copy.deepcopy(change))
    with pytest.raises(ProvenanceValidationError, match=message) as expected:
        native(row, catalog)
    path = tmp_path / "row.sqlite3"
    prepare(path, row)
    with closing(connect(path)) as db:
        before = snapshot(db)["journal_prefix"]
        current = work(db, row, catalog, operation="row")
        current.initialize()
        finish(current, budget=1)
        with pytest.raises(type(expected.value)) as actual:
            current.result()
        assert str(actual.value) == str(expected.value)
        require_reclaimed(db)
        assert outputs(db) == []
        assert snapshot(db)["journal_prefix"] == before


def test_root_operation_is_bound_and_unknown_codec_is_not_rebuilt(tmp_path, catalog):
    row = subject("activity")
    path = tmp_path / "row.sqlite3"
    prepare(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        before = snapshot(db)
        with pytest.raises(StaleGenericSemantics):
            work(db, row, catalog).advance()
        assert snapshot(db) == before
        db.execute("UPDATE journal_generic_semantics SET algorithm='unrecognized'").close()
        db.commit()
        with pytest.raises(RuntimeError, match="algorithm"):
            current.advance()


@pytest.mark.parametrize("change", [{"stage": "9"}, {"stage": "3", "cursor": "4"}, {"node": "1"}])
def test_corrupt_row_cursor_refuses_without_replay(tmp_path, catalog, change):
    row = subject("activity")
    path = tmp_path / "row.sqlite3"
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
        with pytest.raises(RuntimeError, match="corrupt"):
            current.advance()
        assert snapshot(db) == before


def test_required_activity_profile_precedes_late_digest_failure(tmp_path, catalog):
    row = subject("activity", rejected=True)
    path = tmp_path / "row.sqlite3"
    prepare(path, row)
    from test_generic_semantics import source

    with closing(connect(path)) as db:
        current = work(
            db, row, catalog, operation="row", source_binding=source(catalog, require_profiles=True)
        )
        current.initialize()
        finish(current, budget=1)
        with pytest.raises(UnresolvedContractError, match="unavailable"):
            current.result()
        require_reclaimed(db)


@pytest.mark.parametrize(
    "seam",
    [
        "init-header",
        "init-frame",
        "init-before-commit",
        "init-after-commit",
        "frame-insert",
        "frame-update",
        "frame-delete",
        "output-insert",
        "undo-insert",
        "before-commit",
        "after-commit",
        "reject-frames",
        "rollback-output",
        "rollback-marker",
        "output-retirement",
    ],
)
def test_actual_process_death_in_row_suboperations(tmp_path, catalog, seam):
    rejected = seam.startswith("reject-") or seam.startswith("rollback-")
    row = subject("activity", rejected=rejected)
    path, input_path = tmp_path / "row.sqlite3", tmp_path / "row.json"
    input_path.write_bytes(canonical_json_bytes(row))
    prepare(path, row)
    with closing(connect(path)) as db:
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
        current = work(db, row, catalog, operation="row")
        if not seam.startswith("init-"):
            current.initialize()
            position(
                current,
                {
                    "frame-insert": ("row", "3"),
                    "frame-update": ("row", "3"),
                    "frame-delete": ("association", "1"),
                    "output-insert": ("profile", "0"),
                    "undo-insert": ("profile", "0"),
                    "before-commit": ("row", "4"),
                    "after-commit": ("row", "4"),
                    "reject-frames": "reject-frames",
                    "rollback-output": "reject-outputs",
                    "rollback-marker": "reject-outputs",
                    "output-retirement": "outputs",
                }[seam],
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
        after = snapshot(db)
        assert after["journal_prefix"] == before["journal_prefix"]
        assert (after != before) == (seam in {"after-commit", "init-after-commit"})
        current = work(db, row, catalog, operation="row")
        if seam.startswith("init-") and seam != "init-after-commit":
            assert not after["journal_generic_semantics"]
            current.initialize()
        finish(current, budget=1)
        if rejected:
            with pytest.raises(ProvenanceValidationError, match="configuration digest mismatch"):
                current.result()
            assert outputs(db) == []
        else:
            current.result()
            assert outputs(db) == native(row, catalog)
        require_reclaimed(db)


def growth_samples(tmp_path, catalog):
    binding, projection, digest = binding_projection(GenericSemantics.advance)
    samples = []
    cases = [(kind, 0, False) for kind in sorted(ROW_TYPES)]
    cases += [("activity", stage, False) for stage in (3, 4, 5, 6, 7)]
    cases += [("activity", 7, True)]
    cases += [
        (kind, stage, False)
        for kind in ("usage", "generation", "invalidation")
        for stage in (1, 2, 3)
    ]
    cases += [(kind, 3, True) for kind in ("usage", "generation", "invalidation")]
    cases += [("locator_binding_end", stage, False) for stage in (1, 2)]
    cases += [("locator_binding_end", 2, True)]
    for case, (kind, stage, rejected) in enumerate(cases):
        for count, budget in ((1, 1), (15, 16), (16, 16), (17, 16), (129, 16), (1025, 64)):
            row = subject(kind, count, rejected=rejected)
            path = tmp_path / f"row-{case}-{count}.sqlite3"
            retained_history = 4097 if count in (17, 1025) else count
            aggregate_history = (
                (4097 if count == 129 else count if count in (15, 16, 17) else 0)
                if kind in ("generation", "invalidation") and stage == 2
                else 0
            )
            end_history = (
                (4097 if count == 129 else count if count in (15, 16, 17) else 0)
                if kind == "locator_binding_end" and stage == 1
                else 0
            )
            prepare(
                path,
                row,
                retained=retained_history,
                aggregate_history=aggregate_history,
                end_history=end_history,
            )
            with closing(sqlite3.connect(":memory:")) as db:
                with closing(connect(path)) as original:
                    original.backup(db)
                history_outputs = outputs(db)
                current = work(db, row, catalog, operation="row")
                current.initialize()
                if stage:
                    position(current, ("row", str(stage)))
                tables = (
                    "assertions",
                    "changes",
                    "maps",
                    "activity_projection",
                    "binding_projection",
                    "journal_prefix",
                    "journal_generic_semantics",
                    "generic_semantic_frames",
                    "generic_semantic_keys",
                    "generic_semantic_outputs",
                )
                retained = sum(
                    db.execute("SELECT count(*) FROM " + name).fetchone()[0] for name in tables
                )
                prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                envelope = (
                    8192
                    + len(current.binding)
                    + sum(
                        len(value) if isinstance(value, bytes) else len(str(value).encode())
                        for value in prefix
                    )
                )
                with closing(connect(path)) as destination:
                    db.backup(destination)
            with closing(connect(path, factory=CommonWorkConnection)) as db:
                current = work(db, row, catalog, operation="row")
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
                    suffix = work(suffix_db, row, catalog, operation="row")
                    finish(suffix, budget=64)
                    if rejected:
                        message = (
                            "event precedes activity start"
                            if kind in ("usage", "generation", "invalidation")
                            else "binding end precedes the known binding"
                            if kind == "locator_binding_end"
                            else "configuration digest mismatch"
                        )
                        with pytest.raises(ProvenanceValidationError, match=message):
                            suffix.result()
                        assert outputs(suffix_db) == history_outputs
                    else:
                        suffix.result()
                        assert outputs(suffix_db) == sorted(history_outputs + native(row, catalog))
                    require_reclaimed(suffix_db)
    assert len(samples) == 198
    return samples


def test_missing_activity_associations_keeps_the_native_required_lookup(tmp_path, catalog):
    row = subject("activity")
    del row["associations"]
    with pytest.raises(KeyError, match="associations"):
        native(row, catalog)
    path = tmp_path / "row.sqlite3"
    prepare(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        before = snapshot(db)
        with pytest.raises(KeyError, match="associations"):
            current.advance()
        assert snapshot(db) == before
        assert not db.in_transaction
