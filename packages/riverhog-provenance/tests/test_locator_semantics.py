from __future__ import annotations

import copy
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
import riverhog_provenance.graph as graph
import test_row_semantics as rows
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance._delivery_projection import read_locator_observation
from riverhog_provenance.errors import ProvenanceValidationError
from test_generic_semantics import (
    connect,
    finish,
    identity,
    outputs,
    require_reclaimed,
    snapshot,
    source,
    work,
)

MODES = (
    "observed",
    "unobserved",
    "occurrence",
    "filesystem",
    "object",
    "repository",
    "database",
    "wrong-filesystem-context",
    "wrong-object-context",
    "wrong-repository-context",
    "wrong-database-context",
    "missing-target",
    "wrong-target",
    "external-target",
    "self-external",
    "missing-context",
    "wrong-context",
    "missing-observation",
    "wrong-observation",
    "missing-state",
    "wrong-state",
    "different-subject",
    "unknown-address",
    "unknown-address-missing-capture",
    "missing-capture",
    "wrong-capture",
    "before-capture",
    "after-capture",
    "interval-missing-capture",
    "external-before-missing-state",
)


def subject(mode="observed"):
    row = rows.subject("locator_binding")
    row["observation_id"] = identity(37)
    if mode == "unobserved":
        row.pop("observation_id")
    elif mode == "occurrence":
        row["target"] = rows.reference("occurrence", 32)
    elif mode == "missing-target":
        row["target"] = rows.reference("state", 998)
    elif mode == "wrong-target":
        row["target"] = rows.reference("state", 20)
    elif mode in {"external-target", "self-external", "external-before-missing-state"}:
        row["target"] = rows.reference("state", 33, external=True)
        if mode == "self-external":
            row["target"]["journal_id"] = identity(2)
    elif mode == "different-subject":
        row["target"] = rows.reference("state", 34)
    elif mode in {"missing-context", "wrong-context"}:
        row["context_id"] = identity(998 if mode == "missing-context" else 20)
    elif mode in {"missing-observation", "wrong-observation"}:
        row["observation_id"] = identity(998 if mode == "missing-observation" else 20)
    elif mode in {"before-capture", "after-capture"}:
        row["temporal_scope"]["at"] = (
            "2025-12-31T00:00:00Z" if mode == "before-capture" else "2026-01-03T00:00:00Z"
        )
    elif mode == "interval-missing-capture":
        row["temporal_scope"] = {"kind": "interval", "start": "2026-01-01T12:00:00Z"}
    naming = (
        mode.removeprefix("wrong-").removesuffix("-context") if mode.startswith("wrong-") else mode
    )
    if naming == "filesystem":
        row["locator"] = {"kind": "filesystem_path", "name": {"kind": "text", "text": "item"}}
    elif naming == "object":
        row["locator"] = {
            "kind": "object_key",
            "key": {"kind": "text", "text": "item"},
            "version": {"kind": "text", "text": "v1"},
        }
    elif naming == "repository":
        row["locator"] = {
            "kind": "repository_id",
            "identifier": {"kind": "text", "text": "item"},
            "version": {"kind": "text", "text": "v1"},
        }
    elif naming == "database":
        row["locator"] = {"kind": "database_selector", "selector": rows.unknown_profile(1)}
    return row


def referents(mode="observed"):
    values = copy.deepcopy(rows.references())
    for value in values:
        if value["type"] == "context" and mode in {
            "filesystem",
            "object",
            "repository",
            "database",
        }:
            value["kind"] = {
                "filesystem": "filesystem_namespace",
                "object": "object_service",
                "repository": "repository",
                "database": "database",
            }[mode]
        if value["type"] == "observation":
            value.update(capture_id=identity(38), address_status="known")
            if mode in {"missing-state", "external-before-missing-state"}:
                value["state"] = rows.reference("state", 998)
            elif mode == "wrong-state":
                value["state"] = rows.reference("state", 20)
            if mode in {"unknown-address", "unknown-address-missing-capture"}:
                value["address_status"] = "unknown"
            if mode in {
                "missing-capture",
                "interval-missing-capture",
                "unknown-address-missing-capture",
            }:
                value["capture_id"] = identity(998)
            elif mode == "wrong-capture":
                value["capture_id"] = identity(20)
    return values


def prepare(path, row, mode="observed", *, history=0):
    rows.prepare(path, row, retained=history)
    with closing(connect(path)) as db:
        for value in referents(mode)[1:]:
            db.execute(
                "UPDATE assertions SET value=? WHERE assertion_id=?",
                (canonical_json_bytes(value), value["assertion_id"]),
            ).close()
            if value["type"] == "context":
                db.execute(
                    "UPDATE delivery_projection SET value=? WHERE assertion_id=?",
                    (value["kind"], value["assertion_id"]),
                ).close()
            elif value["type"] == "observation":
                db.execute(
                    "UPDATE delivery_projection SET value=?,capture_id=?,address_status=? "
                    "WHERE assertion_id=?",
                    (
                        value["state"]["object_id"],
                        value["capture_id"],
                        value["address_status"],
                        value["assertion_id"],
                    ),
                ).close()
        db.commit()


def owned(db):
    return {
        "semantic": snapshot(db),
        "findings": db.execute("SELECT key,value FROM findings ORDER BY key").fetchall(),
    }


def native(row, catalog, mode="observed", *, strict=False):
    return graph._Validator(
        {"artifacts": [*referents(mode), row]},
        catalog=catalog,
        journal_id=identity(2),
        require_profiles=strict,
    )


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("budget", [1, 64])
def test_reopened_locator_preserves_native_order_and_outputs(tmp_path, catalog, mode, budget):
    row = subject(mode)
    validator = native(row, catalog, mode)
    expected = None
    try:
        validator.row_semantics(row)
    except ProvenanceValidationError as error:
        expected = type(error), str(error)
    path = tmp_path / "locator.sqlite3"
    prepare(path, row, mode)
    with closing(connect(path)) as db:
        work(db, row, catalog, operation="row").initialize()
        prefix = db.execute("SELECT * FROM journal_prefix").fetchall()
    for _ in range(100):
        with closing(connect(path)) as db:
            current = work(db, row, catalog, operation="row")
            state = current.advance(step_budget=budget)
            assert not db.in_transaction
            assert db.execute("SELECT * FROM journal_prefix").fetchall() == prefix
            if not state.done:
                continue
            before = owned(db)
            if expected is not None:
                with pytest.raises(expected[0]) as caught:
                    current.result()
                assert str(caught.value) == expected[1]
                assert outputs(db) == []
            else:
                current.result()
                expected_outputs = sorted(
                    (name, b"b" + key, canonical_json_bytes(value))
                    for name, mapping in (
                        ("externals", validator.externals),
                        ("profiles", validator.profiles),
                    )
                    for key, value in mapping.items()
                )
                assert outputs(db) == expected_outputs
            assert owned(db) == before
            require_reclaimed(db)
            break
    else:
        pytest.fail("locator did not finish")


@pytest.mark.parametrize("strict", [False, True])
def test_locator_selector_keeps_profile_requirement_and_error_type(tmp_path, catalog, strict):
    row = subject("database")
    validator = native(row, catalog, "database", strict=strict)
    path = tmp_path / "profile.sqlite3"
    prepare(path, row, "database")
    with closing(connect(path)) as db:
        current = work(
            db,
            row,
            catalog,
            operation="row",
            source_binding=source(catalog, require_profiles=strict),
        )
        current.initialize()
        finish(current, budget=1)
        if strict:
            with pytest.raises(ProvenanceValidationError) as expected:
                validator.row_semantics(row)
            with pytest.raises(type(expected.value)) as actual:
                current.result()
            assert str(actual.value) == str(expected.value)
        else:
            validator.row_semantics(row)
            current.result()
        require_reclaimed(db)


@pytest.mark.parametrize(
    "column,value",
    [
        ("value", "x" * 65537),
        ("capture_id", "x" * 65537),
        ("address_status", "x" * 65537),
        ("capture_id", b"not-text"),
        ("address_status", None),
        ("value", identity(33).upper()),
    ],
)
def test_locator_raw_metadata_corruption_is_bounded(tmp_path, column, value):
    from test_entry_preparation import WorkConnection

    path = tmp_path / "raw.sqlite3"
    prepare(path, subject())
    with closing(connect(path)) as db:
        db.execute(
            "UPDATE delivery_projection SET " + column + "=? WHERE assertion_id=?",
            (value, identity(137)),
        ).close()
        db.execute(
            "UPDATE assertions SET value=? WHERE assertion_id=?", (b"unread" * 65537, identity(137))
        ).close()
        db.commit()
    with closing(connect(path, factory=WorkConnection)) as db:
        with db.observe() as metrics:
            with pytest.raises(RuntimeError, match="invalid staged locator"):
                read_locator_observation(db, identity(37))
        assert metrics["sql_statements"] == 2
        assert metrics["returned_metadata_octets"] <= 256
        assert metrics["changed_rows"] == metrics["historical_payload_octets"] == 0


@pytest.mark.growth_witness(
    "riverhog-locator-observation-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._delivery_projection.read_locator_observation",
)
def test_actual_cold_locator_observation_metadata_work(tmp_path, record_property):
    from riverhog_growth_contracts import validate_execution_samples
    from test_entry_preparation import WorkConnection
    from test_generic_semantics import binding_projection

    binding, projection, digest = binding_projection(read_locator_observation)
    samples = []
    statuses = ("known", "not_observed", "not_exposed", "withheld", "unknown")
    for mode in (*statuses, "missing", "wrong", "retired", "missing-projection"):
        for history in (0, 1, 15, 16, 17, 129, 4097):
            path = tmp_path / f"locator-metadata-{mode}-{history}.sqlite3"
            prepare(path, subject(), history=history)
            with closing(connect(path)) as db:
                db.execute(
                    "UPDATE assertions SET object_id=?,type='observation' WHERE active=0",
                    (identity(37),),
                ).close()
                db.execute(
                    "UPDATE assertions SET value=? WHERE assertion_id=?",
                    (b"unread observation payload" * 16384, identity(137)),
                ).close()
                if mode in statuses:
                    db.execute(
                        "UPDATE delivery_projection SET address_status=? WHERE assertion_id=?",
                        (mode, identity(137)),
                    ).close()
                elif mode == "missing":
                    db.execute(
                        "DELETE FROM assertions WHERE assertion_id=?", (identity(137),)
                    ).close()
                elif mode == "wrong":
                    db.execute(
                        "UPDATE assertions SET type='agent' WHERE assertion_id=?", (identity(137),)
                    ).close()
                elif mode == "retired":
                    db.execute(
                        "UPDATE assertions SET active=0 WHERE assertion_id=?", (identity(137),)
                    ).close()
                else:
                    db.execute(
                        "DELETE FROM delivery_projection WHERE assertion_id=?", (identity(137),)
                    ).close()
                db.commit()
            with closing(connect(path, factory=WorkConnection)) as db:
                indexed = db.execute("SELECT count(*) FROM assertions").fetchone()[0]
                prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                with db.observe() as metrics:
                    if mode in statuses:
                        assert read_locator_observation(db, identity(37)) == {
                            "id": identity(37),
                            "type": "observation",
                            "state": {"object_id": identity(33)},
                            "capture_id": identity(38),
                            "address_status": mode,
                        }
                    else:
                        error = (
                            RuntimeError
                            if mode == "missing-projection"
                            else ProvenanceValidationError
                        )
                        message = (
                            "missing staged locator observation"
                            if mode == "missing-projection"
                            else "wrong reference type"
                            if mode == "wrong"
                            else "unresolved local identity"
                        )
                        with pytest.raises(error, match=message):
                            read_locator_observation(db, identity(37))
                assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
                for sql, parameters, _cursor in db.statements:
                    if "semantic_reference_type" in sql:
                        plan = " ".join(
                            v[3]
                            for v in db.execute("EXPLAIN QUERY PLAN " + sql, parameters).fetchall()
                        )
                        assert "COVERING INDEX semantic_reference_type" in plan
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": digest,
                    "dimensions": {"indexed_assertions": indexed},
                    "metrics": dict(metrics),
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
    assert len(samples) == 63
    record_property("growth_samples", samples)


@pytest.mark.parametrize("damage", ["delete", "state", "capture", "address"])
def test_staging_reload_authenticates_raw_locator_metadata(tmp_path, damage):
    import test_assertion_staging as staging
    from riverhog_provenance import BoundedSourceObserver, BytesSource

    value = next(
        v
        for v in BoundedSourceObserver()
        .observe(BytesSource(b"raw locator"))
        .graph_fragment()["descriptions"]
        if v["type"] == "observation"
    )
    path = tmp_path / "staging-link.sqlite3"
    staging.create(path)
    raw = staging.frame([value])
    with closing(staging.connect(path)) as db:
        current = staging.work(db, raw)
        current.initialize()
        assert current.advance(row_budget=1).done
        current.load()
        if damage == "delete":
            db.execute(
                "DELETE FROM delivery_projection WHERE assertion_id=?", (value["assertion_id"],)
            ).close()
        else:
            column = {"state": "value", "capture": "capture_id", "address": "address_status"}[
                damage
            ]
            changed = (
                "unknown"
                if damage == "address" and value["address_status"] != "unknown"
                else "known"
                if damage == "address"
                else identity(999)
            )
            db.execute(
                "UPDATE delivery_projection SET " + column + "=? WHERE assertion_id=?",
                (changed, value["assertion_id"]),
            ).close()
        db.commit()
        with pytest.raises(RuntimeError, match="missing or changed staged delivery"):
            staging.work(db, raw).load()


LOCATOR_CRASH_PROGRAM = r"""
import json,os,sqlite3,sys
from pathlib import Path
sys.path.insert(0,sys.argv[2])
from riverhog_provenance_contracts import ContractCatalog
from test_generic_semantics import connect,work
class CrashConnection(sqlite3.Connection):
    active=False
    def commit(self):
        super().commit()
        if self.active:os._exit(73)
path,_,source,seam,budget=sys.argv[1:]
db=connect(path,factory=CrashConnection)
db.create_function('hard_exit',0,lambda:os._exit(73))
if seam in {'parent','output','undo-owner'}:
    event,table={'parent':('UPDATE','journal_generic_semantics'),
                 'output':('INSERT','maps'),
                 'undo-owner':('INSERT','generic_semantic_outputs')}[seam]
    db.execute('CREATE TRIGGER hard_crash AFTER '+event+' ON '+table+
               ' BEGIN SELECT hard_exit(); END;').close()
sqlite3.Connection.commit(db)
if seam=='before-commit':
    def trace(sql):
        if sql.strip().upper()=='COMMIT':os._exit(73)
    db.set_trace_callback(trace)
if seam=='after-commit':db.active=True
current=work(db,json.loads(Path(source).read_text()),ContractCatalog(),operation='row')
current.advance(step_budget=int(budget))
raise SystemExit(99)
"""


@pytest.mark.parametrize("kind", ["reference", "profile"])
@pytest.mark.parametrize("budget", [1, 64])
@pytest.mark.parametrize(
    "seam", ["parent", "output", "undo-owner", "before-commit", "after-commit"]
)
def test_locator_process_death_preserves_parent_and_output(tmp_path, catalog, kind, budget, seam):
    from test_generic_semantics import position

    mode = "unobserved" if kind == "reference" else "database"
    row = subject(mode)
    row.pop("observation_id", None)
    if kind == "reference":
        row["target"] = rows.reference("state", 33, external=True)
    path = tmp_path / "hard-exit.sqlite3"
    prepare(path, row, mode)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        if kind == "profile":
            position(current, ("profile", "0"))
        before = owned(db)
        with closing(sqlite3.connect(":memory:")) as expected_db:
            db.backup(expected_db)
            work(expected_db, row, catalog, operation="row").advance(step_budget=budget)
            expected = owned(expected_db)
    source_path = tmp_path / "source.json"
    source_path.write_bytes(canonical_json_bytes(row))
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            LOCATOR_CRASH_PROGRAM,
            str(path),
            str(Path(__file__).parent),
            str(source_path),
            seam,
            str(budget),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert process.returncode == 73, process.stderr[-1000:]
    with closing(connect(path)) as db:
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert owned(db) == (expected if seam == "after-commit" else before)
        db.execute("DROP TRIGGER IF EXISTS hard_crash").close()
        db.commit()
        current = work(db, row, catalog, operation="row")
        finish(current, budget=64)
        current.result()
        validator = native(row, catalog, mode)
        validator.row_semantics(row)
        assert outputs(db) == sorted(
            (name, b"b" + key, canonical_json_bytes(value))
            for name, mapping in (
                ("externals", validator.externals),
                ("profiles", validator.profiles),
            )
            for key, value in mapping.items()
        )
        assert (
            db.execute("SELECT * FROM journal_prefix").fetchall()
            == before["semantic"]["journal_prefix"]
        )
        require_reclaimed(db)


REGIMES = ((1, 1), (15, 16), (16, 16), (17, 16), (129, 16), (1025, 64))
VALID_MODES = (
    "observed",
    "unobserved",
    "occurrence",
    "filesystem",
    "object",
    "repository",
    "database",
)
FAILURE_STAGES = {
    "wrong-filesystem-context": 1,
    "wrong-object-context": 1,
    "wrong-repository-context": 1,
    "wrong-database-context": 1,
    "missing-target": 0,
    "wrong-target": 0,
    "external-target": 6,
    "self-external": 0,
    "missing-context": 1,
    "wrong-context": 1,
    "missing-observation": 5,
    "wrong-observation": 5,
    "missing-state": 6,
    "wrong-state": 6,
    "different-subject": 6,
    "unknown-address": 7,
    "unknown-address-missing-capture": 7,
    "missing-capture": 8,
    "wrong-capture": 8,
    "before-capture": 8,
    "after-capture": 8,
    "external-before-missing-state": 6,
}


def native_outcome(row, catalog, mode):
    validator = native(row, catalog, mode)
    error = None
    try:
        validator.row_semantics(row)
    except ProvenanceValidationError as failure:
        error = type(failure), str(failure)
    expected_outputs = sorted(
        (name, b"b" + key, canonical_json_bytes(value))
        for name, mapping in (("externals", validator.externals), ("profiles", validator.profiles))
        for key, value in mapping.items()
    )
    return error, expected_outputs


def assert_native_result(current, db, expected):
    error, expected_outputs = expected
    if error is None:
        current.result()
        assert outputs(db) == expected_outputs
    else:
        with pytest.raises(error[0]) as actual:
            current.result()
        assert str(actual.value) == error[1]
        assert outputs(db) == []
    assert db.execute("SELECT value FROM findings").fetchall() == []
    require_reclaimed(db)


def growth_subject(mode, count):
    row = subject(mode)
    row["evidence"] = [{"asserted_by_agent_id": identity(20), "basis": "attestation"}] * count
    if count in {17, 1025}:
        for key in ("name", "key", "identifier", "value", "version"):
            item = row["locator"].get(key)
            if isinstance(item, dict) and item.get("kind") == "text":
                item["text"] = "x" * 65537
        if row["locator"]["kind"] == "database_selector":
            row["locator"]["selector"]["data"]["documentary"] = "x" * 65537
    return row


def prepare_growth(path, row, mode, count):
    prepare(path, row, mode, history=4097 if count in {17, 1025} else count)
    if count in {17, 1025}:
        with closing(connect(path)) as db:
            for ordinal in (31, 33, 37, 38):
                db.execute(
                    "UPDATE assertions SET value=? WHERE assertion_id=?",
                    (b"unread historical documentary payload" * 4096, identity(ordinal + 100)),
                ).close()
            db.commit()


def growth_samples(tmp_path, catalog):
    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance._generic_semantics import GenericSemantics
    from test_edge_row_semantics import envelope, semantic_sample
    from test_generic_semantics import (
        CommonWorkConnection,
        binding_projection,
        position,
        workspace_rows,
    )

    binding, projection, digest = binding_projection(GenericSemantics.advance)
    cases = [(mode, ("row", str(stage))) for mode in VALID_MODES for stage in range(10)]
    cases += [(mode, ("row", str(stage))) for mode, stage in FAILURE_STAGES.items()]
    cases += [("database", ("profile", "0")), ("interval-missing-capture", ("row", "8"))]
    samples = []
    for ordinal, (mode, target) in enumerate(cases):
        for count, budget in REGIMES:
            row = growth_subject(mode, count)
            expected = native_outcome(row, catalog, mode)
            path = tmp_path / f"locator-step-{ordinal}-{count}.sqlite3"
            prepare_growth(path, row, mode, count)
            with closing(sqlite3.connect(":memory:")) as db:
                with closing(connect(path)) as original:
                    original.backup(db)
                current = work(db, row, catalog, operation="row")
                current.initialize()
                position(current, target)
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
                    assert_native_result(suffix, suffix_db, expected)
    assert len(samples) == 564
    return samples


def terminal_growth_samples(tmp_path, catalog):
    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance._generic_semantics import _RESULT_BYTES, GenericSemantics
    from test_edge_row_semantics import envelope, semantic_sample
    from test_generic_semantics import CommonWorkConnection, binding_projection, workspace_rows

    binding, projection, digest = binding_projection(GenericSemantics.result)
    samples = []
    for mode in MODES:
        for count, _budget in REGIMES:
            row = growth_subject(mode, count)
            expected = native_outcome(row, catalog, mode)
            path = tmp_path / f"locator-result-{mode}-{count}.sqlite3"
            prepare_growth(path, row, mode, count)
            with closing(sqlite3.connect(":memory:")) as db:
                with closing(connect(path)) as original:
                    original.backup(db)
                current = work(db, row, catalog, operation="row")
                current.initialize()
                finish(current, budget=64)
                assert_native_result(current, db, expected)
                retained = workspace_rows(db)
                prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                width = envelope(current, prefix)
                with closing(connect(path)) as destination:
                    db.backup(destination)
            with closing(connect(path, factory=CommonWorkConnection)) as db:
                current = work(db, row, catalog, operation="row")
                before = owned(db)
                with db.observe() as metrics:
                    if expected[0] is None:
                        current.result()
                    else:
                        with pytest.raises(expected[0][0]) as actual:
                            current.result()
                        assert str(actual.value) == expected[0][1]
                assert owned(db) == before
                assert metrics["changed_rows"] == metrics["source_payload_octets"] == 0
                sample = semantic_sample(
                    binding, digest, retained, 1, width, metrics, _RESULT_BYTES
                )
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
    assert len(samples) == 180
    return samples
