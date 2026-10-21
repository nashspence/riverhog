from __future__ import annotations

import copy
import hashlib
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest
import test_row_semantics as rows
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    growth_binding,
    project_bindings,
    validate_execution_samples,
)
from riverhog_provenance import _generic_semantics as semantics
from riverhog_provenance import (
    assertion,
    create_journal,
    external_reference,
    new_id,
    reference,
    validate_graph,
    validate_journal,
)
from riverhog_provenance._assertion_staging import AssertionStagingCapacity
from riverhog_provenance._delivery_projection import read_delivery_metadata
from riverhog_provenance._generic_semantics import (
    GenericSemanticsCapacity,
    StaleGenericSemantics,
)
from riverhog_provenance._index_key import encode_index_key
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.graph import _name_identity, _Validator
from riverhog_provenance.journal import JournalFrame
from riverhog_provenance.journal_store import _Map, _Set
from test_assertion_staging import (
    connect,
    contents,
    create,
    identity,
    proposed_octets,
    work,
)
from test_entry_preparation import WorkConnection
from test_generic_semantics import (
    CRASH_PROGRAM,
    CommonWorkConnection,
    binding_projection,
    finish,
    outputs,
    position,
    require_reclaimed,
    snapshot,
    workspace_rows,
)
from test_generic_semantics import work as semantic_work

KINDS = ("context", "observation")
MODES = (
    "valid",
    "max-context",
    "alternate",
    "missing",
    "wrong",
    "retired",
    "reasserted",
    "missing-projection",
)


def referent(kind, ordinal=1, *, payload=0):
    row = {
        "assertion_id": identity(100 + ordinal),
        "id": identity(1000000 + ordinal),
        "type": kind,
        "evidence": [],
    }
    if kind == "context":
        row.update(kind="delivery", label="x" * (payload or 1))
    else:
        row.update(state=rows.reference("state", 33), note="x" * (payload or 1))
    return row


def field(row):
    return row["kind"] if row["type"] == "context" else row["state"]["object_id"]


def insert(db, row, *, active=1, projection=True):
    db.execute(
        "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
        (
            row["assertion_id"],
            row["id"],
            "contexts" if row["type"] == "context" else "descriptions",
            row["type"],
            None,
            None,
            "0",
            canonical_json_bytes(row),
            active,
        ),
    ).close()
    if row["type"] in KINDS and projection:
        db.execute(
            "INSERT INTO delivery_projection(assertion_id,value) VALUES (?,?)",
            (row["assertion_id"], field(row)),
        ).close()


def prepare(path, kind, mode, *, history=0, payload=0):
    create(path)
    row = referent(kind, payload=payload)
    if mode in {"alternate", "max-context"}:
        if kind == "context":
            row["kind"] = "opaque" if mode == "alternate" else "execution_environment"
        else:
            row["state"]["object_id"] = identity(34 if mode == "alternate" else 999999999)
    if mode == "wrong":
        row["type"] = "agent"
    with closing(connect(path)) as db:
        prior = referent(kind, 999)
        prior["id"] = row["id"]
        insert(db, prior, active=0)
        if mode != "missing":
            insert(
                db,
                row,
                active=int(mode != "retired"),
                projection=mode != "missing-projection",
            )
        for n in range(history):
            insert(
                db,
                referent(kind, 10000 + n, payload=262144 if n == 0 else 0),
                active=n % 2,
            )
        db.commit()
    return row


def check(db, row, kind, mode):
    if mode in {"missing", "retired"}:
        with pytest.raises(ProvenanceValidationError, match="unresolved local identity"):
            read_delivery_metadata(db, row["id"], kind)
    elif mode == "wrong":
        with pytest.raises(ProvenanceValidationError, match="wrong reference type"):
            read_delivery_metadata(db, row["id"], kind)
    elif mode == "missing-projection":
        with pytest.raises(RuntimeError, match="missing staged delivery reference metadata"):
            read_delivery_metadata(db, row["id"], kind)
    else:
        assert read_delivery_metadata(db, row["id"], kind) == field(row)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("query_only", [False, True])
def test_effective_delivery_metadata_reads_are_closed_and_payload_free(
    tmp_path, kind, mode, query_only
):
    path = tmp_path / "metadata.sqlite3"
    row = prepare(path, kind, mode, history=17, payload=1048576)
    with closing(connect(path, factory=WorkConnection)) as db:
        before = contents(db)
        if query_only:
            db.execute("PRAGMA query_only=ON").close()
        with db.observe() as metrics:
            check(db, row, kind, mode)
        assert metrics["sql_statements"] == (1 if mode in {"missing", "retired", "wrong"} else 2)
        assert metrics["changed_rows"] == metrics["historical_payload_octets"] == 0
        assert metrics["returned_metadata_octets"] <= 128
        assert contents(db) == before and not db.in_transaction


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("value", ["x" * 131072, b"x" * 131072, "é", "", b"\x00"])
def test_corrupt_delivery_metadata_rejects_after_bounded_scalar_read(tmp_path, kind, value):
    path = tmp_path / "corrupt.sqlite3"
    row = prepare(path, kind, "valid")
    with closing(connect(path, factory=WorkConnection)) as db:
        db.execute(
            "UPDATE delivery_projection SET value=? WHERE assertion_id=?",
            (value, row["assertion_id"]),
        ).close()
        db.commit()
        with db.observe() as metrics:
            with pytest.raises(RuntimeError, match="invalid staged delivery reference"):
                read_delivery_metadata(db, row["id"], kind)
        assert metrics["returned_metadata_octets"] <= 128
        assert metrics["changed_rows"] == metrics["historical_payload_octets"] == 0


def test_observation_target_is_a_canonical_uuid_and_other_projection_types_are_refused(
    tmp_path,
):
    path = tmp_path / "identity.sqlite3"
    row = prepare(path, "observation", "valid")
    with closing(connect(path, factory=WorkConnection)) as db:
        db.execute("UPDATE delivery_projection SET value='not-a-uuid'").close()
        db.commit()
        with pytest.raises(RuntimeError, match="invalid staged delivery reference"):
            read_delivery_metadata(db, row["id"], "observation")
        with db.observe() as metrics:
            with pytest.raises(ValueError, match="requires a context or observation"):
                read_delivery_metadata(db, row["id"], "agent")
        assert metrics["sql_statements"] == 0


def source(kind):
    category = "contexts" if kind == "context" else "descriptions"
    return JournalFrame(
        canonical_json_bytes(
            {
                "journal_id": identity(2),
                "sequence": "1",
                "body": {"assertions": {category: [referent(kind, 1), referent(kind, 2)]}},
            }
        )
    )


@pytest.mark.parametrize("kind", KINDS)
def test_delivery_projection_staging_admits_all_bytes_before_any_write(tmp_path, kind):
    path = tmp_path / "stage.sqlite3"
    create(path)
    frame = source(kind)
    category = "contexts" if kind == "context" else "descriptions"
    first = frame.document["body"]["assertions"][category][0]
    budget = proposed_octets(category, first)
    with closing(connect(path)) as db:
        current = work(db, frame)
        current.initialize()
        before = contents(db)
        with pytest.raises(AssertionStagingCapacity):
            current.advance(row_budget=1, byte_budget=budget - 1)
        assert contents(db) == before
        current.advance(row_budget=1, byte_budget=budget)
        assert read_delivery_metadata(db, first["id"], kind) == field(first)
        assert contents(db)["journal_prefix"] == before["journal_prefix"]
    with closing(connect(path)) as db:
        assert work(db, frame).advance(row_budget=1).done
        assert len(contents(db)["delivery_projection"]) == 2


@pytest.mark.parametrize(
    "fault",
    [
        "valid",
        "external",
        "context",
        "missing",
        "wrong",
        "target",
        "duplicate",
        "separate",
    ],
)
def test_public_delivery_rows_preserve_native_and_stored_results(
    graph, who, journal, catalog, monkeypatch, fault
):
    context = assertion("context", who, kind="delivery")
    item = assertion(
        "delivery_association",
        who,
        delivery_context_id=context["id"],
        slot={"kind": "text", "text": "../opaque-slot"},
        role="urn:test:payload",
        state=reference(graph["states"][0]["id"], "state"),
        verification_observation_id=next(
            value["id"] for value in graph["descriptions"] if value["type"] == "observation"
        ),
    )
    graph.setdefault("contexts", []).append(context)
    graph["delivery_associations"] = [item]
    messages = {
        "context": "requires a delivery context",
        "missing": "unresolved local identity",
        "wrong": "wrong reference type",
        "target": "verification target differ",
        "duplicate": "duplicate delivery slot",
    }
    if fault == "context":
        context["kind"] = "repository"
    elif fault == "missing":
        item["delivery_context_id"] = new_id()
    elif fault == "wrong":
        item["verification_observation_id"] = context["id"]
    elif fault == "target":
        other = copy.deepcopy(graph["states"][0])
        other.update(id=new_id(), assertion_id=new_id())
        graph["states"].append(other)
        item["state"]["object_id"] = other["id"]
    elif fault == "external":
        with validate_journal(journal, catalog=catalog) as foreign:
            item["state"] = external_reference(foreign, item["state"]["object_id"])
    elif fault in {"duplicate", "separate"}:
        other = copy.deepcopy(item)
        other.update(id=new_id(), assertion_id=new_id())
        other["role"] = "urn:test:other-payload"
        graph["delivery_associations"].append(other)
        if fault == "separate":
            second = assertion("context", who, kind="delivery")
            graph["contexts"].append(second)
            other["delivery_context_id"] = second["id"]
    calls = []
    original = semantics.read_delivery_metadata

    def observed(db, identity, kind):
        calls.append((identity, kind))
        return original(db, identity, kind)

    monkeypatch.setattr(semantics, "read_delivery_metadata", observed)
    if fault in messages:
        with pytest.raises(ProvenanceValidationError, match=messages[fault]) as memory:
            validate_graph(graph, catalog=catalog)
        with pytest.raises(ProvenanceValidationError, match=messages[fault]) as stored:
            create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        assert str(memory.value) == str(stored.value)
    else:
        validate_graph(graph, catalog=catalog)
        raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(raw, catalog=catalog) as summary:
            assert summary.journal_sha256 == hashlib.sha256(raw).hexdigest()
            assert len(summary.graph["delivery_associations"]) == len(
                graph["delivery_associations"]
            )
            assert len(summary.graph_validation.external_references) >= int(fault == "external")
        assert calls


def slot_key(row):
    return row["delivery_context_id"], canonical_json_bytes(_name_identity(row["slot"]))


@pytest.mark.parametrize(
    "fault,message",
    [
        ("valid", None),
        ("external", None),
        ("context-before-slot", "requires a delivery context"),
        ("slot-before-state", "invalid base64"),
        ("state-before-observation", "incompatible object type"),
        ("missing", "unresolved local identity"),
        ("wrong", "wrong reference type"),
        ("target-before-duplicate", "verification target differ"),
        ("duplicate", "duplicate delivery slot"),
    ],
)
def test_delivery_row_error_order_and_output_rollback(
    tmp_path, catalog, monkeypatch, fault, message
):
    row = rows.subject("delivery_association", rejected=fault == "target-before-duplicate")
    referents = rows.references()
    if fault == "context-before-slot":
        next(v for v in referents if v["type"] == "context")["kind"] = "repository"
    if fault in {"context-before-slot", "slot-before-state"}:
        row["slot"] = {
            "kind": "bytes",
            "encoding": "opaque",
            "bytes": {"data": "?", "byte_length": "0"},
        }
        row["state"]["object_id"] = identity(999)
    elif fault == "state-before-observation":
        row["state"]["object_type"] = "artifact"
        row["verification_observation_id"] = identity(999)
    elif fault == "missing":
        row["verification_observation_id"] = identity(999)
    elif fault == "wrong":
        row["verification_observation_id"] = identity(31)
    elif fault == "external":
        row["state"] = rows.reference("state", 33, external=True)

    def selected_references(*, comparison=False):
        assert not comparison
        return referents

    monkeypatch.setattr(rows, "references", selected_references)
    native = _Validator(
        {"artifacts": [*referents, row]},
        catalog=catalog,
        journal_id=identity(2),
        require_profiles=False,
    )
    duplicate = fault in {"target-before-duplicate", "duplicate"}
    if duplicate:
        native.delivery_slots.add(slot_key(row))
    if message:
        with pytest.raises(ProvenanceValidationError, match=message) as expected:
            native.row_semantics(row)
    else:
        native.row_semantics(row)
    path = tmp_path / "row.sqlite3"
    rows.prepare(path, row, slot_history=129)
    with closing(connect(path)) as db:
        if duplicate:
            db.execute(
                "INSERT INTO maps VALUES ('slots',?,?)",
                (encode_index_key(slot_key(row)), b"null"),
            ).close()
            db.commit()
        before = outputs(db)
        current = semantic_work(db, row, catalog, operation="row")
        current.initialize()
        finish(current, budget=1)
        if message:
            with pytest.raises(ProvenanceValidationError, match=message) as actual:
                current.result()
            assert str(actual.value) == str(expected.value)
            assert outputs(db) == before
        else:
            current.result()
            assert outputs(db) == sorted(before + rows.native(row, catalog))
        require_reclaimed(db)


def test_variable_slot_admission_stale_proposals_and_reopen_preserve_uniqueness(tmp_path, catalog):
    row = rows.subject("delivery_association")
    row["slot"]["text"] = "x" * 80000
    path = tmp_path / "large.sqlite3"
    rows.prepare(path, row, slot_history=17)
    with closing(connect(path)) as db:
        current = semantic_work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "4"))
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
        current = semantic_work(db, row, catalog, operation="row")
        statements = []
        db.set_trace_callback(statements.append)
        finish(current, budget=1)
        current.result()
        db.set_trace_callback(None)
        assert not any("SELECT 1 FROM maps WHERE name='slots'" in sql for sql in statements)
        assert slot_key(row) in _Set(SimpleNamespace(db=db), "slots")
        require_reclaimed(db)


def test_common_outputs_use_native_map_identity_codec(tmp_path, catalog):
    row = rows.subject("extension")
    path = tmp_path / "codec.sqlite3"
    rows.prepare(path, row)
    with closing(connect(path)) as db:
        current = semantic_work(db, row, catalog, operation="row")
        current.initialize()
        finish(current, budget=1)
        current.result()
        store = SimpleNamespace(db=db)
        assert _Map(store, "externals")[canonical_json_bytes(row["subject"])] == row["subject"]
        profile = row["value"]["value"]["profile"]
        assert _Map(store, "profiles")[canonical_json_bytes(profile)] == profile
        assert outputs(db) == sorted(rows.native(row, catalog))


@pytest.mark.growth_witness(
    "riverhog-delivery-metadata-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._delivery_projection.read_delivery_metadata",
)
def test_actual_cold_delivery_metadata_reads(tmp_path, record_property):
    binding = growth_binding(read_delivery_metadata)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    cases = (
        (kind, history, mode)
        for kind in KINDS
        for history in (0, 1, 15, 16, 17, 129, 1025, 4097)
        for mode in MODES
    )
    for ordinal, (kind, history, mode) in enumerate(cases):
        path = tmp_path / f"cold-{ordinal}.sqlite3"
        row = prepare(
            path,
            kind,
            mode,
            history=history,
            payload=1048576 if history in (17, 4097) else 0,
        )
        with closing(connect(path, factory=WorkConnection)) as db:
            retained = db.execute("SELECT count(*) FROM assertions").fetchone()[0]
            with db.observe() as metrics:
                check(db, row, kind, mode)
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {"indexed_assertions": retained},
                "metrics": dict(metrics),
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
    assert len(samples) == 128
    record_property("growth_samples", samples)


STAGING_CRASH = r"""
import os,sqlite3,sys
from pathlib import Path
from test_delivery_semantics import connect,work,JournalFrame
seam=sys.argv[3]
class DeathConnection(sqlite3.Connection):
    def execute(self,sql,parameters=()):
        if sql.startswith('INSERT INTO delivery_projection') and seam=='projection-before':
            os._exit(73)
        result=super().execute(sql,parameters)
        if sql.startswith('INSERT INTO delivery_projection') and seam=='projection-after':
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


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    "seam", ["projection-before", "projection-after", "before-commit", "after-commit"]
)
def test_process_death_keeps_delivery_projection_assertion_and_cursor_atomic(tmp_path, kind, seam):
    path, expected_path = tmp_path / "stage.sqlite3", tmp_path / "expected.sqlite3"
    create(path)
    frame = source(kind)
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
        assert len(contents(db)["delivery_projection"]) == 2


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
def test_process_death_in_delivery_slot_commit_or_row_output_undo(tmp_path, catalog, seam):
    rejected = seam.startswith("rollback")
    row = rows.subject("delivery_association", rejected=rejected)
    if rejected:
        row["state"] = rows.reference("state", 34, external=True)
    path, input_path = tmp_path / "kill.sqlite3", tmp_path / "row.json"
    rows.prepare(path, row, retained=17, slot_history=129)
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
            else ("row", "4"),
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
            with pytest.raises(ProvenanceValidationError, match="verification target differ"):
                current.result()
            assert outputs(db) == prior
        else:
            current.result()
            assert outputs(db) == sorted(prior + rows.native(row, catalog))
        require_reclaimed(db)


@pytest.mark.parametrize("history", [0, 1, 15, 16, 17, 129, 4097])
def test_cold_delivery_uniqueness_uses_only_its_indexed_slot(tmp_path, catalog, history):
    row = rows.subject("delivery_association")
    path = tmp_path / "slot.sqlite3"
    rows.prepare(path, row, retained=17 if history == 0 else 1, slot_history=history)
    with closing(connect(path)) as db:
        db.execute(
            "UPDATE assertions SET value=? WHERE object_id IN (?,?)",
            (b"unread-projected-payload" * 65536, identity(31), identity(37)),
        ).close()
        db.commit()
        current = semantic_work(db, row, catalog, operation="row")
        current.initialize()
        position(current, ("row", "4"))
        retained = workspace_rows(db)
        envelope = 8192 + len(current.binding)
        prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
        envelope += sum(len(v) if isinstance(v, bytes) else len(str(v).encode()) for v in prefix)
    binding, projection, digest = binding_projection(semantics.GenericSemantics.advance)
    with closing(connect(path, factory=CommonWorkConnection)) as db:
        current = semantic_work(db, row, catalog, operation="row")
        with db.observe() as metrics:
            current.advance(step_budget=1, byte_budget=65536)
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": digest,
            "dimensions": {
                "retained_rows": retained,
                "step_budget": 1,
                "byte_budget": 65536,
                "metadata_row_octets": envelope,
            },
            "metrics": dict(metrics),
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        assert metrics["source_payload_octets"] == metrics["adjacency_rows"] == 0
        finish(current, budget=1)
        current.result()
        assert slot_key(row) in _Set(SimpleNamespace(db=db), "slots")
        require_reclaimed(db)
