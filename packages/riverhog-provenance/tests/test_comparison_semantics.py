from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
import riverhog_provenance.graph as graph
import test_row_semantics as rows
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance._fixity_projection import read_fixity_metadata
from riverhog_provenance._generic_semantics import StaleGenericSemantics
from riverhog_provenance.errors import ProvenanceValidationError
from test_entry_preparation import WorkConnection
from test_generic_semantics import (
    connect,
    finish,
    identity,
    outputs,
    require_reclaimed,
    snapshot,
    work,
)


def subject(mode="matching"):
    value = {
        "id": identity(11),
        "assertion_id": identity(10),
        "type": "content_comparison",
        "evidence": [],
        "left_description": rows.reference("reported_description", 40),
        "right_description": rows.reference("reported_description", 41),
        "result": "matching_fixity",
    }
    if mode in {"indeterminate", "absent-indeterminate", "external-indeterminate"}:
        value["result"] = "indeterminate"
    elif mode in {"different", "contradictory-different"}:
        value["result"] = "different"
    if mode in {"external", "external-indeterminate", "external-before-wrong", "self-external"}:
        value["left_description"] = rows.reference("reported_description", 40, external=True)
    if mode == "self-external":
        value["left_description"]["journal_id"] = identity(2)
    if mode in {"missing-left", "missing-right"}:
        value["left_description" if mode.endswith("left") else "right_description"]["object_id"] = (
            identity(998)
        )
    elif mode in {"wrong-left", "wrong-right", "external-before-wrong"}:
        value["left_description" if mode == "wrong-left" else "right_description"]["object_id"] = (
            identity(35)
        )
    elif mode == "declared-before-missing":
        value["left_description"] = rows.reference("state", 998)
    return value


def descriptions(mode="matching", kinds=("reported_description", "reported_description")):
    result = []
    for index, kind in enumerate(kinds):
        value = {
            "id": identity(40 + index),
            "assertion_id": identity(140 + index),
            "type": kind,
            "state": rows.reference("state", 33),
            "evidence": [],
            "content": {
                "size_bytes": "0",
                "digests": [{"algorithm": "sha-256", "value": "a" * 64}],
            },
        }
        if index and mode in {"different", "contradictory-matching", "indeterminate"}:
            value["content"]["digests"][0]["value"] = "b" * 64
        if mode in {"absent", "absent-indeterminate"} and index:
            value.pop("content")
        result.append(value)
    return result


def prepare(path, row, values, *, history=0):
    rows.prepare(path, row, retained=history)
    with closing(connect(path)) as db:
        for value in values:
            db.execute(
                "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    value["assertion_id"],
                    value["id"],
                    "descriptions",
                    value["type"],
                    value["state"]["object_id"],
                    None,
                    "0",
                    canonical_json_bytes(value),
                    1,
                ),
            ).close()
            content = value.get("content")
            db.execute(
                "INSERT INTO fixity_projection VALUES (?,?,?)",
                (
                    value["assertion_id"],
                    None if content is None else content["size_bytes"],
                    None
                    if content is None
                    else next(
                        v["value"] for v in content["digests"] if v["algorithm"] == "sha-256"
                    ),
                ),
            ).close()
        db.commit()


def owned(db):
    return {
        "semantic": snapshot(db),
        "findings": db.execute("SELECT key,value FROM findings ORDER BY key").fetchall(),
    }


def warning(row):
    return f"{row['id']}: comparison depends on unavailable external content evidence"


MODES = (
    "matching",
    "different",
    "indeterminate",
    "contradictory-matching",
    "contradictory-different",
    "absent",
    "absent-indeterminate",
    "external",
    "external-indeterminate",
    "missing-left",
    "missing-right",
    "wrong-left",
    "wrong-right",
    "external-before-wrong",
    "self-external",
    "declared-before-missing",
)


@pytest.mark.parametrize("budget", [1, 64])
@pytest.mark.parametrize("mode", MODES)
def test_reopen_preserves_native_diagnostics_outputs_and_findings(tmp_path, catalog, mode, budget):
    row, values = subject(mode), descriptions(mode)
    native = graph._Validator(
        {"artifacts": [*rows.references(), *values, row]},
        catalog=catalog,
        journal_id=identity(2),
        require_profiles=False,
    )
    expected = None
    try:
        native.row_semantics(row)
    except ProvenanceValidationError as error:
        expected = type(error), str(error)
    path = tmp_path / "comparison.sqlite3"
    prepare(path, row, values)
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
            if expected is None:
                current.result()
                assert db.execute("SELECT value FROM findings ORDER BY key").fetchall() == [
                    (v,) for v in sorted(native.findings)
                ]
                assert outputs(db) == sorted(
                    ("externals", b"b" + key, canonical_json_bytes(value))
                    for key, value in native.externals.items()
                )
            else:
                with pytest.raises(expected[0]) as caught:
                    current.result()
                assert str(caught.value) == expected[1]
                assert outputs(db) == []
                assert db.execute("SELECT 1 FROM findings").fetchone() is None
            assert owned(db) == before
            require_reclaimed(db)
            assert current.advance().done
            break
    else:
        pytest.fail("comparison did not finish")


@pytest.mark.parametrize("kind", ["observation", "reported_description"])
@pytest.mark.parametrize("field", ["size", "digest"])
def test_raw_fixity_reader_never_loads_large_documentary_payloads(tmp_path, kind, field):
    row, values = subject(), descriptions(kinds=(kind, "reported_description"))
    if field == "size":
        values[0]["content"]["size_bytes"] = "9223372036854775807"
    path = tmp_path / "metadata.sqlite3"
    prepare(path, row, values, history=4097)
    with closing(connect(path)) as db:
        db.execute(
            "UPDATE assertions SET value=? WHERE assertion_id=?",
            (b"unread payload" * 65536, identity(140)),
        ).close()
        db.commit()
    with closing(connect(path, factory=WorkConnection)) as db:
        with db.observe() as metrics:
            result = read_fixity_metadata(db, identity(40))
        assert result["content"] == values[0]["content"]
        assert metrics["historical_payload_octets"] == metrics["changed_rows"] == 0
        assert metrics["sql_statements"] == 2 and metrics["returned_rows"] == 2


@pytest.mark.parametrize(
    "table", ["journal_generic_semantics", "findings", "generic_semantic_outputs"]
)
def test_finding_and_semantic_checkpoint_roll_back_as_one_unit(tmp_path, catalog, table):
    row = subject("external")
    path = tmp_path / "atomic.sqlite3"
    prepare(path, row, descriptions())
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        current.advance(step_budget=1)
        current.advance(step_budget=1)
        event = "UPDATE" if table == "journal_generic_semantics" else "INSERT"
        db.execute(
            "CREATE TRIGGER fail_finding AFTER "
            + event
            + " ON "
            + table
            + " BEGIN SELECT RAISE(ABORT,'injected finding failure'); END;"
        ).close()
        db.commit()
        before = owned(db)
        with pytest.raises(sqlite3.IntegrityError, match="injected finding failure"):
            current.advance(step_budget=1)
        assert not db.in_transaction and owned(db) == before
        db.execute("DROP TRIGGER fail_finding").close()
        db.commit()
        finish(current)
        current.result()
        assert db.execute("SELECT value FROM findings").fetchall() == [(warning(row),)]
        require_reclaimed(db)


@pytest.mark.parametrize("existing", ["same", "changed"])
def test_existing_finding_is_preserved_and_inconsistent_identity_is_rejected(
    tmp_path, catalog, existing
):
    row = subject("external")
    path = tmp_path / "existing.sqlite3"
    prepare(path, row, descriptions())
    with closing(connect(path)) as db:
        message = warning(row)
        db.execute(
            "INSERT INTO findings VALUES (?,?)",
            (message, message if existing == "same" else "changed"),
        ).close()
        db.commit()
        current = work(db, row, catalog, operation="row")
        current.initialize()
        current.advance(step_budget=1)
        current.advance(step_budget=1)
        before = owned(db)
        if existing == "changed":
            with pytest.raises(RuntimeError, match="retained semantic finding"):
                current.advance(step_budget=1)
            assert owned(db) == before
        else:
            finish(current)
            current.result()
            assert db.execute("SELECT key,value FROM findings").fetchall() == [(message, message)]
            require_reclaimed(db)


@pytest.mark.parametrize(
    "field,value",
    [
        ("size", "01"),
        ("size", "9223372036854775808"),
        ("size", "x" * 100000),
        ("size", b"0"),
        ("size", None),
        ("sha256", "X" * 64),
        ("sha256", "a" * 65),
        ("sha256", b"a" * 64),
        ("sha256", None),
    ],
    ids=(
        "size-noncanonical",
        "size-large-integer",
        "size-oversized",
        "size-blob",
        "size-null",
        "sha-uppercase",
        "sha-oversized",
        "sha-blob",
        "sha-null",
    ),
)
def test_raw_fixity_corruption_fails_closed_after_bounded_reads(tmp_path, field, value):
    path = tmp_path / "corrupt.sqlite3"
    prepare(path, subject(), descriptions())
    with closing(connect(path)) as db:
        db.execute(
            "UPDATE fixity_projection SET " + field + "=? WHERE assertion_id=?",
            (value, identity(140)),
        ).close()
        db.commit()
    with closing(connect(path, factory=WorkConnection)) as db:
        with db.observe() as metrics:
            with pytest.raises(RuntimeError, match="staged description fixity"):
                read_fixity_metadata(db, identity(40))
        assert metrics["returned_metadata_octets"] <= 256
        assert metrics["changed_rows"] == 0


@pytest.mark.parametrize("damage", ["attempt", "prefix", "binding"])
def test_stale_finding_proposal_cannot_publish_warning_or_progress(tmp_path, catalog, damage):
    row = subject("external")
    path = tmp_path / "stale.sqlite3"
    prepare(path, row, descriptions())
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        current.advance(step_budget=1)
        current.advance(step_budget=1)
        batch = current.prepare(step_budget=1)
        if damage == "prefix":
            db.execute("UPDATE journal_prefix SET attempt_id='replacement'").close()
        elif damage == "attempt":
            db.execute("UPDATE journal_generic_semantics SET attempt_id='replacement'").close()
        else:
            db.execute("UPDATE journal_generic_semantics SET binding=X'00'").close()
        db.commit()
        before = owned(db)
        with pytest.raises(StaleGenericSemantics):
            current.commit(batch)
        assert owned(db) == before


@pytest.mark.parametrize("count", [15, 16, 17])
@pytest.mark.parametrize("budget", [1, 64])
@pytest.mark.parametrize("absent", [False, True])
def test_raw_fixity_staging_reopens_without_early_description_semantics(
    tmp_path, count, budget, absent
):
    import test_assertion_staging as staging
    from riverhog_provenance.journal import JournalFrame

    values = descriptions("absent" if absent else "matching")
    values[0]["content"]["digests"] = [
        {"algorithm": "sha-512", "value": f"{n:0128x}"} for n in range(count)
    ] + values[0]["content"]["digests"]
    raw = JournalFrame(
        canonical_json_bytes(
            {
                "journal_id": identity(2),
                "sequence": "1",
                "body": {"assertions": {"descriptions": values}},
            }
        )
    )
    path = tmp_path / "raw-fixity.sqlite3"
    staging.create(path)
    with closing(staging.connect(path)) as db:
        staging.work(db, raw).initialize()
        prefix = db.execute("SELECT * FROM journal_prefix").fetchall()
    for _ in range(3):
        with closing(staging.connect(path)) as db:
            current = staging.work(db, raw)
            state = current.advance(row_budget=budget, byte_budget=4000000)
            assert not db.in_transaction
            assert db.execute("SELECT * FROM journal_prefix").fetchall() == prefix
            if state.done:
                assert read_fixity_metadata(db, identity(40))["content"]["digests"] == [
                    {"algorithm": "sha-256", "value": "a" * 64},
                ]
                second = read_fixity_metadata(db, identity(41))
                assert ("content" not in second) == absent
                break
    else:
        pytest.fail("raw fixity staging did not finish")


def test_fixity_directory_is_prepared_once_outside_staging_units(tmp_path, monkeypatch):
    import test_assertion_staging as staging
    from riverhog_provenance import _assertion_staging as implementation
    from riverhog_provenance.journal import JournalFrame

    values = descriptions()
    raw = JournalFrame(
        canonical_json_bytes(
            {
                "journal_id": identity(2),
                "sequence": "1",
                "body": {"assertions": {"descriptions": values}},
            }
        )
    )
    path = tmp_path / "directory.sqlite3"
    staging.create(path)
    with closing(staging.connect(path)) as db:
        current = staging.work(db, raw)
        current.initialize()

        def forbid(_source):
            raise AssertionError("staging unit rescanned its admitted digest array")

        monkeypatch.setattr(implementation, "_raw_fixity", forbid)
        assert not current.advance(row_budget=1).done
        assert current.advance(row_budget=1).done
        assert current.load().done
        assert current.advance(row_budget=1).done


@pytest.mark.parametrize("damage", ["size", "sha256", "missing"])
def test_staging_cursor_rejects_missing_or_changed_raw_fixity(tmp_path, damage):
    import test_assertion_staging as staging
    from riverhog_provenance.journal import JournalFrame

    raw = JournalFrame(
        canonical_json_bytes(
            {
                "journal_id": identity(2),
                "sequence": "1",
                "body": {"assertions": {"descriptions": descriptions()}},
            }
        )
    )
    path = tmp_path / "raw-corrupt.sqlite3"
    staging.create(path)
    with closing(staging.connect(path)) as db:
        current = staging.work(db, raw)
        current.initialize()
        assert current.advance(row_budget=1).position == 1
        if damage == "missing":
            db.execute("DELETE FROM fixity_projection").close()
        else:
            db.execute(
                "UPDATE fixity_projection SET " + damage + "=?",
                ("1" if damage == "size" else "b" * 64,),
            ).close()
        db.commit()
        with pytest.raises(RuntimeError, match="staged description fixity"):
            current.load()


@pytest.mark.parametrize("budget", [1, 64])
def test_rejected_shared_unit_undoes_new_findings_and_preserves_earlier_findings(
    tmp_path, catalog, monkeypatch, budget
):
    from riverhog_provenance._generic_semantics import _ProjectedValidator
    from test_generic_semantics import create, metadata
    from test_generic_semantics import subject as common_subject

    row = common_subject(3)
    row["metadata"].append(metadata(99, {"type": "timestamp", "value": "2026-02-30T00:00:00Z"}))
    path = tmp_path / "finding-undo.sqlite3"
    create(path, row)
    reference = _ProjectedValidator.reference

    def notice(validator, *args, **kwargs):
        result = reference(validator, *args, **kwargs)
        validator.findings.append("injected semantic notice")
        return result

    monkeypatch.setattr(_ProjectedValidator, "reference", notice)
    with closing(connect(path)) as db:
        db.execute("INSERT INTO findings VALUES ('retained','retained')").close()
        db.commit()
        current = work(db, row, catalog)
        current.initialize()
        phases = finish(current, budget=budget)
        assert "reject-outputs" in phases
        with pytest.raises(ProvenanceValidationError, match="calendar"):
            current.result()
        assert db.execute("SELECT key,value FROM findings ORDER BY key").fetchall() == [
            ("retained", "retained")
        ]
        require_reclaimed(db)


FINDING_CRASH_PROGRAM = r"""
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
 'parent':('UPDATE','journal_generic_semantics'),
 'finding':('INSERT','findings'),
 'undo-owner':('INSERT','generic_semantic_outputs'),
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


@pytest.mark.parametrize("budget", [1, 64])
@pytest.mark.parametrize(
    "seam", ["parent", "finding", "undo-owner", "before-commit", "after-commit"]
)
def test_process_death_commits_warning_owner_and_progress_together(tmp_path, catalog, budget, seam):
    import subprocess
    import sys
    from pathlib import Path

    row = subject("external")
    path = tmp_path / "crash.sqlite3"
    prepare(path, row, descriptions())
    with closing(connect(path)) as db:
        current = work(db, row, catalog, operation="row")
        current.initialize()
        current.advance(step_budget=1)
        current.advance(step_budget=1)
        before = owned(db)
    row_path = tmp_path / "row.json"
    row_path.write_bytes(canonical_json_bytes(row))
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            FINDING_CRASH_PROGRAM,
            str(path),
            str(Path(__file__).parent),
            str(row_path),
            seam,
            str(budget),
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
            assert db.execute("SELECT value FROM findings").fetchall() == [(warning(row),)]
        else:
            assert after == before
        db.execute("DROP TRIGGER IF EXISTS hard_crash").close()
        db.commit()
        current = work(db, row, catalog, operation="row")
        finish(current)
        current.result()
        assert db.execute("SELECT value FROM findings").fetchall() == [(warning(row),)]
        require_reclaimed(db)


def test_observation_cannot_lose_its_raw_fixity(tmp_path):
    path = tmp_path / "missing-observation.sqlite3"
    prepare(path, subject(), descriptions(kinds=("observation", "reported_description")))
    with closing(connect(path)) as db:
        db.execute(
            "UPDATE fixity_projection SET size=NULL,sha256=NULL WHERE assertion_id=?",
            (identity(140),),
        ).close()
        db.commit()
        with pytest.raises(RuntimeError, match="staged observation fixity"):
            read_fixity_metadata(db, identity(40))


REGIMES = ((1, 1), (15, 16), (16, 16), (17, 16), (129, 16), (1025, 64))
COMPARISON_ERRORS = {
    "missing-left": "unresolved local identity",
    "missing-right": "unresolved local identity",
    "wrong-left": "wrong reference type",
    "wrong-right": "wrong reference type",
    "absent": "content comparison has no cited complete fixity evidence",
    "contradictory-matching": "matching-fixity comparison contradicts cited evidence",
    "contradictory-different": "different-content comparison contradicts cited SHA-256 evidence",
}


def assert_comparison_result(current, db, row, mode):
    if mode in COMPARISON_ERRORS:
        with pytest.raises(ProvenanceValidationError, match=COMPARISON_ERRORS[mode]):
            current.result()
        assert outputs(db) == []
        assert db.execute("SELECT value FROM findings").fetchall() == []
    else:
        current.result()
        assert db.execute("SELECT value FROM findings").fetchall() == (
            [(warning(row),)] if mode == "external" else []
        )
        expected = []
        if mode == "external":
            ref = row["left_description"]
            expected = [("externals", b"b" + canonical_json_bytes(ref), canonical_json_bytes(ref))]
        assert outputs(db) == expected
    require_reclaimed(db)


def measured_descriptions(mode, count):
    values = descriptions(mode)
    if count in {17, 1025}:
        values[0]["note"] = "x" * 65537
    return values


def growth_samples(tmp_path, catalog, *, measured_operation="advance"):
    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance._generic_semantics import GenericSemantics
    from test_edge_row_semantics import envelope
    from test_generic_semantics import (
        CommonWorkConnection,
        binding_projection,
        measure_semantic_operation,
        position,
        workspace_rows,
    )

    binding, projection, digest = binding_projection(getattr(GenericSemantics, measured_operation))
    samples = []
    for stage in (0, 1, 2):
        for mode in (
            "matching",
            "different",
            "indeterminate",
            "contradictory-matching",
            "absent",
            "external",
        ):
            for count, budget in REGIMES:
                row = subject(mode)
                row["evidence"] = [
                    {"asserted_by_agent_id": identity(20), "basis": "attestation"}
                ] * count
                path = tmp_path / f"comparison-step-{stage}-{mode}-{count}.sqlite3"
                prepare(
                    path,
                    row,
                    measured_descriptions(mode, count),
                    history=4097 if count in {17, 1025} else count,
                )
                with closing(sqlite3.connect(":memory:")) as db:
                    with closing(connect(path)) as original:
                        original.backup(db)
                    current = work(db, row, catalog, operation="row")
                    current.initialize()
                    position(current, ("row", str(stage)))
                    retained = workspace_rows(db)
                    prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                    width = envelope(current, prefix)
                    with closing(connect(path)) as destination:
                        db.backup(destination)
                with closing(connect(path, factory=CommonWorkConnection)) as db:
                    current = work(db, row, catalog, operation="row")
                    byte_budget = 16 * 1024 * 1024
                    sample = measure_semantic_operation(
                        current,
                        measured_operation=measured_operation,
                        step_budget=budget,
                        byte_budget=byte_budget,
                        retained_rows=retained,
                        metadata_row_octets=width,
                    )
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
                    assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
                    with closing(sqlite3.connect(":memory:")) as suffix_db:
                        db.backup(suffix_db)
                        suffix = work(suffix_db, row, catalog, operation="row")
                        finish(suffix, budget=64)
                        assert_comparison_result(suffix, suffix_db, row, mode)
    samples.extend(
        finding_undo_growth_samples(tmp_path, catalog, measured_operation=measured_operation)
    )
    assert len(samples) == 114
    return samples


def finding_undo_growth_samples(tmp_path, catalog, *, measured_operation="advance"):
    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance._generic_semantics import GenericSemantics, _ProjectedValidator
    from test_edge_row_semantics import envelope
    from test_generic_semantics import (
        CommonWorkConnection,
        binding_projection,
        create,
        measure_semantic_operation,
        metadata,
        position,
        workspace_rows,
    )
    from test_generic_semantics import subject as common_subject

    binding, projection, digest = binding_projection(getattr(GenericSemantics, measured_operation))
    samples = []
    for count, budget in REGIMES:
        row = common_subject(count)
        row["metadata"].append(
            metadata(count + 1, {"type": "timestamp", "value": "2026-02-30T00:00:00Z"})
        )
        path = tmp_path / f"finding-undo-work-{count}.sqlite3"
        create(path, row)
        notice_text = "injected semantic notice" + ("x" * 65537 if count in {17, 1025} else "")
        reference = _ProjectedValidator.reference

        def notice(validator, *args, _reference=reference, _notice_text=notice_text, **kwargs):
            result = _reference(validator, *args, **kwargs)
            validator.findings.append(_notice_text)
            return result

        with closing(sqlite3.connect(":memory:")) as db:
            with closing(connect(path)) as original:
                original.backup(db)
            db.execute("INSERT INTO findings VALUES ('retained','retained')").close()
            db.commit()
            current = work(db, row, catalog)
            current.initialize()
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(_ProjectedValidator, "reference", notice)
                position(current, "reject-outputs")
            while db.execute(
                "SELECT name FROM generic_semantic_outputs WHERE attempt_id=? ORDER BY id LIMIT 1",
                (current.attempt_id,),
            ).fetchone() != ("finding",):
                assert not current.load().done
                current.advance(step_budget=1)
            assert db.execute("SELECT 1 FROM findings WHERE key=?", (notice_text,)).fetchone() == (
                1,
            )
            retained = workspace_rows(db)
            prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
            width = envelope(current, prefix)
            with closing(connect(path)) as destination:
                db.backup(destination)
        with closing(connect(path, factory=CommonWorkConnection)) as db:
            current = work(db, row, catalog)
            byte_budget = 16 * 1024 * 1024
            sample = measure_semantic_operation(
                current,
                measured_operation=measured_operation,
                step_budget=budget,
                byte_budget=byte_budget,
                retained_rows=retained,
                metadata_row_octets=width,
            )
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
            assert (
                db.execute("SELECT 1 FROM findings WHERE key=?", (notice_text,)).fetchone() is None
            )
            assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
            with closing(sqlite3.connect(":memory:")) as suffix_db:
                db.backup(suffix_db)
                suffix = work(suffix_db, row, catalog)
                finish(suffix, budget=64)
                with pytest.raises(ProvenanceValidationError, match="calendar"):
                    suffix.result()
                assert outputs(suffix_db) == []
                assert suffix_db.execute("SELECT key,value FROM findings").fetchall() == [
                    ("retained", "retained")
                ]
                require_reclaimed(suffix_db)
    assert len(samples) == 6
    return samples


def terminal_growth_samples(tmp_path, catalog):
    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance._generic_semantics import _RESULT_BYTES, GenericSemantics
    from test_edge_row_semantics import envelope
    from test_generic_semantics import (
        CommonWorkConnection,
        binding_projection,
        measure_semantic_result,
        semantic_terminal_dimensions,
        workspace_rows,
    )

    binding, projection, digest = binding_projection(GenericSemantics.result)
    samples = []
    for mode in (*COMPARISON_ERRORS, "external", "absent-indeterminate"):
        for count, _budget in REGIMES:
            row = subject(mode)
            path = tmp_path / f"comparison-terminal-{mode}-{count}.sqlite3"
            prepare(
                path,
                row,
                measured_descriptions(mode, count),
                history=4097 if count in {17, 1025} else count,
            )
            with closing(sqlite3.connect(":memory:")) as db:
                with closing(connect(path)) as original:
                    original.backup(db)
                current = work(db, row, catalog, operation="row")
                current.initialize()
                finish(current, budget=64)
                assert_comparison_result(current, db, row, mode)
                terminal_dimensions = semantic_terminal_dimensions(current)
                retained = workspace_rows(db)
                prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                width = envelope(current, prefix)
                before = owned(db)
                with closing(connect(path)) as destination:
                    db.backup(destination)
            with closing(connect(path, factory=CommonWorkConnection)) as db:
                current = work(db, row, catalog, operation="row")
                sample = measure_semantic_result(
                    current,
                    {
                        "retained_rows": retained,
                        "step_budget": 1,
                        "byte_budget": _RESULT_BYTES,
                        "metadata_row_octets": width,
                        **terminal_dimensions,
                    },
                    expected_exception=ProvenanceValidationError
                    if mode in COMPARISON_ERRORS
                    else None,
                    pattern=COMPARISON_ERRORS.get(mode),
                )
                assert owned(db) == before
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
    assert len(samples) == 54
    return samples


def staging_growth_samples(tmp_path):
    import test_assertion_staging as staging
    from riverhog_growth_contracts import validate_execution_samples
    from riverhog_provenance import BoundedSourceObserver, BytesSource
    from riverhog_provenance._assertion_staging import AssertionStaging
    from riverhog_provenance.journal import JournalFrame, _identity_signature
    from test_generic_semantics import binding_projection, workspace_rows

    observation_template = next(
        value
        for value in BoundedSourceObserver()
        .observe(BytesSource(b"fixity witness"))
        .graph_fragment()["descriptions"]
        if value["type"] == "observation"
    )
    binding, projection, digest = binding_projection(AssertionStaging.advance)
    samples = []
    for kind in ("observation", "reported_description", "absent-reported"):
        for phase in ("first", "last", "done"):
            for count, budget in REGIMES:
                template = descriptions(
                    "absent" if kind == "absent-reported" else "matching",
                    (
                        "reported_description",
                        "observation" if kind == "observation" else "reported_description",
                    ),
                )[1]
                if kind == "observation":
                    template = observation_template
                values = [
                    {**template, "id": identity(100000 + n), "assertion_id": identity(200000 + n)}
                    for n in range(count)
                ]
                if count in {17, 1025}:
                    values[0]["note"] = "x" * 65537
                raw = JournalFrame(
                    canonical_json_bytes(
                        {
                            "journal_id": identity(2),
                            "sequence": "1",
                            "body": {"assertions": {"descriptions": values}},
                        }
                    )
                )
                path = tmp_path / f"fixity-staging-{kind}-{phase}-{count}.sqlite3"
                staging.create(path)
                with closing(sqlite3.connect(":memory:")) as db:
                    with closing(staging.connect(path)) as original:
                        original.backup(db)
                    for n in range(4097 if count in {17, 1025} else count):
                        prior = staging.row(10000000 + n)
                        db.execute(
                            "INSERT INTO identities VALUES (?, 'referent', ?)",
                            (prior["id"], _identity_signature(prior)),
                        ).close()
                        db.execute(
                            "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,0)",
                            (
                                prior["assertion_id"],
                                prior["id"],
                                "agents",
                                "agent",
                                None,
                                None,
                                "0",
                                canonical_json_bytes(prior),
                            ),
                        ).close()
                    db.commit()
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
                    byte_budget = 4000000
                    with db.observe() as metrics:
                        current.advance(row_budget=budget, byte_budget=byte_budget)
                    sample = {
                        "declaration": binding.declaration.id,
                        "declaration_sha256": digest,
                        "dimensions": {
                            "retained_rows": retained,
                            "row_budget": budget,
                            "byte_budget": byte_budget,
                            "max_atom_octets": max(
                                staging.proposed_octets("descriptions", v) for v in values
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
                    with closing(sqlite3.connect(":memory:")) as suffix_db:
                        db.backup(suffix_db)
                        suffix = staging.work(suffix_db, raw)
                        while not suffix.advance(row_budget=64, byte_budget=4000000).done:
                            pass
                        expected = [
                            (v["assertion_id"], None, None)
                            if kind == "absent-reported"
                            else (
                                v["assertion_id"],
                                v["content"]["size_bytes"],
                                v["content"]["digests"][0]["value"],
                            )
                            for v in values
                        ]
                        assert (
                            suffix_db.execute(
                                "SELECT * FROM fixity_projection ORDER BY assertion_id"
                            ).fetchall()
                            == expected
                        )
    assert len(samples) == 54
    return samples


@pytest.mark.growth_witness(
    "riverhog-description-fixity-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._fixity_projection.read_fixity_metadata",
)
def test_actual_cold_description_fixity_metadata_work(tmp_path, record_property):
    from riverhog_growth_contracts import validate_execution_samples
    from test_generic_semantics import binding_projection

    binding, projection, digest = binding_projection(read_fixity_metadata)
    samples = []
    for kind in ("observation", "reported_description"):
        modes = ("valid", "missing", "wrong", "retired", "missing-projection")
        if kind == "reported_description":
            modes += ("absent",)
        for mode in modes:
            for history in (0, 1, 15, 16, 17, 129, 4097):
                path = tmp_path / f"fixity-metadata-{kind}-{mode}-{history}.sqlite3"
                values = descriptions(kinds=(kind, "reported_description"))
                if mode == "absent":
                    values[0].pop("content")
                else:
                    values[0]["content"]["size_bytes"] = "9223372036854775807"
                prepare(path, subject(), values, history=history)
                target, assertion_id = identity(40), identity(140)
                with closing(connect(path)) as db:
                    db.execute(
                        "UPDATE assertions SET object_id=?,type=? WHERE active=0", (target, kind)
                    ).close()
                    db.execute(
                        "UPDATE assertions SET value=? WHERE assertion_id=?",
                        (b"unread documentary payload" * 16384, assertion_id),
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
                            "DELETE FROM fixity_projection WHERE assertion_id=?", (assertion_id,)
                        ).close()
                    db.commit()
                with closing(connect(path, factory=WorkConnection)) as db:
                    indexed = db.execute("SELECT count(*) FROM assertions").fetchone()[0]
                    prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
                    with db.observe() as metrics:
                        if mode in {"valid", "absent"}:
                            expected = {"id": target, "type": kind}
                            if mode == "valid":
                                expected["content"] = values[0]["content"]
                            assert read_fixity_metadata(db, target) == expected
                        else:
                            error = (
                                RuntimeError
                                if mode == "missing-projection"
                                else ProvenanceValidationError
                            )
                            message = (
                                "missing staged description fixity"
                                if mode == "missing-projection"
                                else "wrong reference type"
                                if mode == "wrong"
                                else "unresolved local identity"
                            )
                            with pytest.raises(error, match=message):
                                read_fixity_metadata(db, target)
                    assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
                    for sql, parameters, _cursor in db.statements:
                        if "semantic_reference_type" in sql:
                            plan = " ".join(
                                v[3]
                                for v in db.execute(
                                    "EXPLAIN QUERY PLAN " + sql, parameters
                                ).fetchall()
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
    assert len(samples) == 77
    record_property("growth_samples", samples)
