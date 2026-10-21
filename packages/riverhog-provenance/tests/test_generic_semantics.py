from __future__ import annotations

import copy
import hashlib
import inspect
import json
import sqlite3
import subprocess
import sys
from contextlib import closing, contextmanager
from dataclasses import replace
from pathlib import Path
from types import CodeType

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance._generic_semantics import (
    GenericAtom,
    GenericSemantics,
    GenericSemanticsCapacity,
    StaleGenericSemantics,
)
from riverhog_provenance._journal_prefix import PrefixState
from riverhog_provenance._semantic_program import Nodes
from riverhog_provenance._semantic_source import SemanticSource
from riverhog_provenance.errors import ProvenanceValidationError, UnresolvedContractError
from riverhog_provenance.graph import _Validator
from riverhog_provenance.journal import JournalFrame
from riverhog_provenance.journal_store import JournalStore
from riverhog_provenance_contracts import PROFILE, profile_reference
from sha256_continuation import CheckpointSHA256
from test_semantic_edge import WorkConnection


def identity(ordinal):
    return f"urn:uuid:00000000-0000-4000-8000-{ordinal:012x}"


def external(ordinal):
    return {
        "scope": "external",
        "journal_id": identity(99),
        "object_id": identity(1000 + ordinal),
        "object_type": "artifact",
    }


def metadata(ordinal, value=None):
    return {
        "profile_id": "urn:test:profile",
        "category": "urn:test:category",
        "name": {"kind": "text", "text": f"field-{ordinal}"},
        "source": {"interface": "urn:test:interface", "field": {"kind": "text", "text": "field"}},
        "status": "present",
        "value": {"type": "reference", "value": external(ordinal)} if value is None else value,
    }


def subject(count=3):
    return {
        "id": identity(11),
        "assertion_id": identity(10),
        "type": "artifact",
        "evidence": [{"asserted_by_agent_id": identity(20), "basis": "attestation"}],
        "metadata": [metadata(n) for n in range(count)],
    }


def agent():
    return {
        "id": identity(20),
        "assertion_id": identity(21),
        "type": "agent",
        "kind": "software",
        "name": "validator",
        "evidence": [],
    }


def connect(path, **options):
    db = sqlite3.connect(path, **options)
    db.execute("PRAGMA synchronous=FULL").close()
    db.execute("PRAGMA cache_size=-64").close()
    return db


def create(path, row):
    store = JournalStore()
    try:
        with closing(connect(path)) as db:
            store.db.backup(db)
    finally:
        store.close()
    prior = JournalFrame(canonical_json_bytes({"id": identity(1), "sequence": "0"}))
    prefix = PrefixState(
        1,
        len(prior.encoded),
        identity(2),
        canonical_json_bytes(prior.reference).decode(),
        CheckpointSHA256(prior.encoded).export_state(),
        False,
    )
    with closing(connect(path)) as db:
        db.execute(
            "INSERT INTO journal_prefix VALUES (1,?,?,?)",
            ("prefix-attempt", b"exact-source-and-modes", prefix.to_json()),
        ).close()
        for value, category in ((agent(), "agents"), (row, "artifacts")):
            db.execute(
                "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    value["assertion_id"],
                    value["id"],
                    category,
                    value["type"],
                    None,
                    None,
                    "1",
                    canonical_json_bytes(value),
                    1,
                ),
            ).close()
        db.execute("INSERT INTO changes VALUES (?)", (row["assertion_id"],)).close()
        db.commit()


def source(catalog, **options):
    return SemanticSource(
        **{
            "sha256": "a" * 64,
            "sequence": "1",
            "journal_id": identity(2),
            "catalog_sha256": catalog.checkpoint_sha256(),
            "require_profiles": False,
            **options,
        }
    )


def work(db, row, catalog, **options):
    return GenericSemantics(
        db,
        **{
            "attempt_id": "common-attempt",
            "source_binding": source(catalog),
            "atom": GenericAtom.admit(row),
            "catalog": catalog,
            **options,
        },
    )


def finish(current, *, budget=16):
    phases = set()
    for _ in range(10000):
        state = current.advance(step_budget=budget)
        phases.add(state.phase)
        assert not current.db.in_transaction
        if state.done:
            return phases
    raise AssertionError("common semantic fixture did not finish")


def outputs(db):
    return db.execute("SELECT name,key,value FROM maps ORDER BY name,key").fetchall()


def require_reclaimed(db):
    for table in ("generic_semantic_frames", "generic_semantic_keys", "generic_semantic_outputs"):
        assert db.execute("SELECT 1 FROM " + table + " LIMIT 1").fetchone() is None


@pytest.mark.parametrize("budget", [1, 15, 16, 17, 64])
def test_reopen_each_committed_common_unit_preserves_native_outputs(tmp_path, catalog, budget):
    row = subject(17)
    path = tmp_path / "common.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog)
        current.initialize()
        prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
    for _ in range(10000):
        with closing(connect(path)) as db:
            current = work(db, row, catalog)
            state = current.advance(step_budget=budget)
            assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
            assert not db.in_transaction
            if state.done:
                current.result()
                require_reclaimed(db)
                result = outputs(db)
                break
    else:
        pytest.fail("restored common fixture did not finish")
    native = _Validator(
        {"agents": [agent()], "artifacts": [row]},
        catalog=catalog,
        journal_id=identity(2),
        require_profiles=False,
    )
    native.generic(row)
    assert result == [
        ("externals", b"b" + key, canonical_json_bytes(value))
        for key, value in sorted(native.externals.items())
    ]


def test_late_rejection_undoes_only_new_outputs_and_reclaims_state(tmp_path, catalog):
    row = subject(3)
    row["profiles"] = [
        {
            "profile": {
                "contract_id": "urn:test:unavailable",
                "contract_sha256": "b" * 64,
                "schema_id": "urn:test:schema",
            },
            "data": {},
        }
    ]
    row["metadata"].append(metadata(9, {"type": "timestamp", "value": "2026-02-30T00:00:00Z"}))
    path = tmp_path / "common.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        key = canonical_json_bytes(external(0))
        db.execute("INSERT INTO maps VALUES (?,?,?)", ("externals", b"b" + key, key)).close()
        db.commit()
        before = outputs(db)
        prefix = db.execute("SELECT * FROM journal_prefix").fetchone()
        current = work(db, row, catalog)
        current.initialize()
        phases = finish(current, budget=1)
        assert {"reject-frames", "reject-keys", "reject-outputs", "rejected"} <= phases
        with pytest.raises(ProvenanceValidationError, match="calendar"):
            current.result()
        assert outputs(db) == before
        assert db.execute("SELECT * FROM journal_prefix").fetchone() == prefix
        require_reclaimed(db)
        with pytest.raises(StaleGenericSemantics, match="already bound"):
            work(db, row, catalog, attempt_id="replacement").initialize()


def test_duplicate_metadata_precedes_invalid_later_value(tmp_path, catalog):
    row = subject(1)
    duplicate = copy.deepcopy(row["metadata"][0])
    duplicate["value"] = {"type": "timestamp", "value": "2026-02-30T00:00:00Z"}
    row["metadata"].append(duplicate)
    path = tmp_path / "common.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog)
        current.initialize()
        finish(current, budget=1)
        with pytest.raises(ProvenanceValidationError, match="duplicate source-qualified"):
            current.result()
        require_reclaimed(db)
        assert outputs(db) == []


def test_unavailable_profile_keeps_its_native_error_type(tmp_path, catalog):
    row = subject(0)
    row["profiles"] = [
        {
            "profile": {
                "contract_id": "urn:test:unavailable",
                "contract_sha256": "b" * 64,
                "schema_id": "urn:test:schema",
            },
            "data": {},
        }
    ]
    path = tmp_path / "common.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog, source_binding=source(catalog, require_profiles=True))
        current.initialize()
        finish(current, budget=1)
        with pytest.raises(UnresolvedContractError):
            current.result()
        require_reclaimed(db)


def test_stale_proposal_does_not_mutate_current_worker(tmp_path, catalog):
    row = subject()
    path = tmp_path / "common.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog)
        current.initialize()
        proposal = current.prepare(step_budget=1)
        current.advance(step_budget=1)
        state = current.load()
        changes = db.total_changes
        with pytest.raises(StaleGenericSemantics, match="checkpoint changed"):
            current.commit(proposal)
        assert current.load() == state
        assert db.total_changes == changes


def test_admitted_tree_is_detached_from_the_callers_row(tmp_path, catalog):
    row = subject()
    atom = GenericAtom.admit(row)
    path = tmp_path / "common.sqlite3"
    create(path, row)
    row["metadata"][0]["value"] = {"type": "timestamp", "value": "2026-02-30T00:00:00Z"}
    with closing(connect(path)) as db:
        current = work(db, row, catalog, atom=atom)
        current.initialize()
        finish(current, budget=1)
        current.result()
        assert len(outputs(db)) == 3


def test_restoration_loads_without_implicit_initialization(tmp_path, catalog):
    row = subject()
    path = tmp_path / "common.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog)
        current.initialize()
        current.advance(step_budget=1)
        before = current.load()
        changes = db.total_changes
        with pytest.raises(StaleGenericSemantics, match="already bound"):
            current.initialize()
        with pytest.raises(StaleGenericSemantics, match="already bound"):
            work(db, row, catalog, attempt_id="replacement").initialize()
        assert work(db, row, catalog).load() == before
        assert db.total_changes == changes


@pytest.mark.parametrize("change", ["header", "missing-frame", "node", "cursor"])
def test_corrupt_or_missing_cursor_is_not_rebuilt(tmp_path, catalog, change):
    row = subject()
    path = tmp_path / "common.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog)
        current.initialize()
        if change == "header":
            value = json.loads(current.load().to_json())
            value["top"] = None
            db.execute(
                "UPDATE journal_generic_semantics SET checkpoint=?",
                (canonical_json_bytes(value).decode(),),
            ).close()
        elif change == "missing-frame":
            db.execute("DELETE FROM generic_semantic_frames").close()
        else:
            value = json.loads(
                db.execute("SELECT value FROM generic_semantic_frames").fetchone()[0]
            )
            value[change] = "1000000" if change == "node" else "2"
            db.execute(
                "UPDATE generic_semantic_frames SET value=?",
                (canonical_json_bytes(value).decode(),),
            ).close()
        db.commit()
        changes = db.total_changes
        with pytest.raises(RuntimeError, match="corrupt|missing"):
            current.advance()
        assert db.total_changes == changes


def test_completed_worker_is_fenced_after_a_new_explicit_operation(tmp_path, catalog):
    row = subject()
    path = tmp_path / "common.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog)
        current.initialize()
        finish(current)
        current.result()
        proposal = current.prepare()
        replacement = work(db, row, catalog, attempt_id="replacement")
        replacement.initialize()
        before = replacement.load()
        changes = db.total_changes
        with pytest.raises(StaleGenericSemantics, match="finished"):
            current.commit(proposal)
        assert replacement.load() == before and db.total_changes == changes


@pytest.mark.parametrize("change", ["source", "mode", "attempt", "row", "prefix", "subject"])
def test_replaced_binding_refuses_without_mutation(tmp_path, catalog, change):
    row = subject()
    path = tmp_path / "common.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog)
        current.initialize()
        options = {}
        if change == "source":
            options["source_binding"] = replace(source(catalog), sha256="c" * 64)
        elif change == "mode":
            options["source_binding"] = replace(source(catalog), require_profiles=True)
        elif change == "attempt":
            options["attempt_id"] = "replacement"
        elif change == "row":
            row = copy.deepcopy(row)
            row["metadata"].pop()
        elif change == "prefix":
            db.execute("UPDATE journal_prefix SET binding=?", (b"replacement",)).close()
            db.commit()
        else:
            db.execute(
                "UPDATE assertions SET active=0 WHERE assertion_id=?", (row["assertion_id"],)
            ).close()
            db.commit()
        changes = db.total_changes
        with pytest.raises(StaleGenericSemantics):
            work(db, row, catalog, **options).advance()
        assert db.total_changes == changes and not db.in_transaction


def test_control_and_reference_units_never_read_assertion_payloads(tmp_path, catalog):
    row = subject(0)
    row["identifiers"] = [{"value": {"kind": "text", "text": "key"}, "authority_id": identity(20)}]
    path = tmp_path / "common.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        db.execute(
            "UPDATE assertions SET value=? WHERE object_id=?",
            (b"unreadable" * 100000, identity(20)),
        ).close()
        db.commit()
        current = work(db, row, catalog)
        current.initialize()

        def authorize(action, table, column, *_):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and table == "assertions" and column == "value"
                else sqlite3.SQLITE_OK
            )

        db.set_authorizer(authorize)
        finish(current, budget=1)
        current.result()
        db.set_authorizer(None)


def test_byte_admission_does_not_silently_skip_a_large_key(tmp_path, catalog):
    row = subject(1)
    row["metadata"][0]["name"]["text"] = "x" * 80000
    path = tmp_path / "common.sqlite3"
    create(path, row)
    with closing(connect(path)) as db:
        current = work(db, row, catalog)
        current.initialize()
        with pytest.raises(GenericSemanticsCapacity):
            for _ in range(100):
                current.advance(step_budget=1, byte_budget=16384)
        before = current.load()
        changes = db.total_changes
        with pytest.raises(GenericSemanticsCapacity):
            current.advance(step_budget=1, byte_budget=16384)
        assert current.load() == before and db.total_changes == changes
        finish(current)
        current.result()
        assert len(outputs(db)) == 1


def position(current, target):
    for _ in range(16 * (len(current.atom.nodes.values) + 1)):
        state = current.load()
        if target in {"keys", "outputs", "reject-frames", "reject-keys", "reject-outputs"}:
            if state.phase == target:
                return
        elif state.phase == "walk":
            raw = current.db.execute(
                "SELECT value FROM generic_semantic_frames WHERE attempt_id=? AND frame_id=?",
                (current.attempt_id, state.top),
            ).fetchone()[0]
            frame = json.loads(raw)
            if target == "unresolved-profile" and (frame["operation"], frame["stage"]) == (
                "profile",
                "0",
            ):
                value = current.atom.nodes.values[int(frame["node"])]
                if value["profile"]["contract_id"] == "urn:test:unavailable":
                    return
            if (frame["operation"], frame["stage"]) == target:
                return
        if state.done:
            break
        current.advance(step_budget=1)
    raise AssertionError("fixture did not reach its selected crash seam")


def workspace_rows(db):
    with closing(
        db.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    ) as cursor:
        names = [row[0] for row in cursor]
    total = 0
    for name in names:
        with closing(
            db.execute('SELECT count(*) FROM "' + name.replace('"', '""') + '"')
        ) as cursor:
            total += cursor.fetchone()[0]
    return total


def snapshot(db):
    return {
        table: db.execute("SELECT * FROM " + table + " ORDER BY 1,2").fetchall()
        for table in (
            "journal_prefix",
            "journal_generic_semantics",
            "generic_semantic_frames",
            "generic_semantic_keys",
            "generic_semantic_outputs",
            "maps",
        )
    }


CRASH_PROGRAM = r"""
import json,os,sqlite3,sys
from pathlib import Path
sys.path.insert(0,sys.argv[2])
from test_generic_semantics import connect,work
from riverhog_provenance_contracts import ContractCatalog

class CrashConnection(sqlite3.Connection):
    active=False
    def commit(self):
        super().commit()
        if self.active:
            os._exit(73)

path,_,input_path,seam=sys.argv[1:]
db=connect(path,factory=CrashConnection)
db.create_function('hard_exit',0,lambda:os._exit(73))
triggers={
 'init-header':('INSERT','journal_generic_semantics'),
 'init-frame':('INSERT','generic_semantic_frames'),
 'header':('UPDATE','journal_generic_semantics'),
 'frame-insert':('INSERT','generic_semantic_frames'),
 'frame-update':('UPDATE','generic_semantic_frames'),
 'frame-delete':('DELETE','generic_semantic_frames'),
 'metadata-key':('INSERT','generic_semantic_keys'),
 'output-insert':('INSERT','maps'),
 'undo-insert':('INSERT','generic_semantic_outputs'),
 'key-cleanup':('DELETE','generic_semantic_keys'),
 'output-retirement':('DELETE','generic_semantic_outputs'),
 'reject-frames':('DELETE','generic_semantic_frames'),
 'reject-keys':('DELETE','generic_semantic_keys'),
 'rollback-output':('DELETE','maps'),
 'rollback-marker':('DELETE','generic_semantic_outputs'),
}
if seam in triggers:
    event,table=triggers[seam]
    db.execute('CREATE TRIGGER hard_crash AFTER '+event+' ON '+table+
               ' BEGIN SELECT hard_exit(); END;').close()
sqlite3.Connection.commit(db)
if seam in {'before-commit','init-before-commit'}:
    def trace(sql):
        if sql.strip().upper()=='COMMIT':
            os._exit(73)
    db.set_trace_callback(trace)
if seam in {'after-commit','init-after-commit'}:
    db.active=True
current=work(db,json.loads(Path(input_path).read_text()),ContractCatalog())
if seam.startswith('init-'):
    current.initialize()
else:
    current.advance(step_budget=1)
raise SystemExit(99)
"""


@pytest.mark.parametrize(
    "seam",
    [
        "init-header",
        "init-frame",
        "init-before-commit",
        "init-after-commit",
        "header",
        "frame-insert",
        "frame-update",
        "frame-delete",
        "metadata-key",
        "output-insert",
        "undo-insert",
        "before-commit",
        "after-commit",
        "key-cleanup",
        "output-retirement",
        "reject-frames",
        "reject-keys",
        "rollback-output",
        "rollback-marker",
    ],
)
def test_real_process_death_preserves_a_common_unit_or_its_commit(tmp_path, catalog, seam):
    rejected = seam.startswith("reject-") or seam.startswith("rollback-")
    row = subject(3)
    if rejected:
        row["metadata"].append(metadata(9, {"type": "timestamp", "value": "2026-02-30T00:00:00Z"}))
    path = tmp_path / "common.sqlite3"
    input_path = tmp_path / "admitted-row.json"
    input_path.write_bytes(canonical_json_bytes(row))
    create(path, row)
    with closing(connect(path)) as db:
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
        current = work(db, row, catalog)
        if not seam.startswith("init-"):
            current.initialize()
            target = {
                "header": ("generic", "0"),
                "frame-insert": ("generic", "0"),
                "frame-update": ("generic", "0"),
                "frame-delete": ("evidence", "3"),
                "metadata-key": ("generic", "3"),
                "output-insert": ("typed", "0"),
                "undo-insert": ("typed", "0"),
                "before-commit": ("typed", "0"),
                "after-commit": ("typed", "0"),
                "key-cleanup": "keys",
                "output-retirement": "outputs",
                "reject-frames": "reject-frames",
                "reject-keys": "reject-keys",
                "rollback-output": "reject-outputs",
                "rollback-marker": "reject-outputs",
            }[seam]
            position(current, target)
        before = snapshot(db)
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
        db.execute("DROP TRIGGER IF EXISTS hard_crash").close()
        db.commit()
        after = snapshot(db)
        assert after["journal_prefix"] == before["journal_prefix"]
        if seam not in {"after-commit", "init-after-commit"}:
            assert after == before
        else:
            assert after != before
        current = work(db, row, catalog)
        if seam.startswith("init-") and seam != "init-after-commit":
            # No admission transaction committed. This is an explicit first
            # initialization, rather than a reset of a retained continuation.
            assert not after["journal_generic_semantics"]
            current.initialize()
        finish(current, budget=1)
        if rejected:
            with pytest.raises(ProvenanceValidationError, match="calendar"):
                current.result()
            assert outputs(db) == []
        else:
            current.result()
            assert len(outputs(db)) == 3
        require_reclaimed(db)
        assert snapshot(db)["journal_prefix"] == before["journal_prefix"]


def rich_subject(count, rejected=False):
    row = subject(count)
    identifier = {
        "scheme": "urn:test:identifier",
        "scope": "context",
        "authority_id": identity(20),
        "value": {"kind": "text", "text": "key"},
    }
    row["identifiers"] = [
        dict(identifier, value={"kind": "text", "text": f"key-{n}"}) for n in range(count)
    ]
    field_source = {"interface": "urn:test:interface", "field": {"kind": "text", "text": "field"}}
    data = {
        "object_kind": "regular_file",
        "native_identifiers": [identifier],
        "principals": [
            {
                "kind": "user",
                "identifier": identifier,
                "source": field_source,
                "display_name": {"kind": "text", "text": "Example"},
            }
        ],
        "timestamps": [
            {
                "kind": "created",
                "value_status": "exact",
                "value": "2026-01-01T00:00:00Z",
                "resolution_ns": "1",
                "source": field_source,
                "raw": {
                    "value": {"type": "integer", "value": "1"},
                    "unit_uri": "urn:test:unit",
                    "epoch": "1970-01-01T00:00:00Z",
                },
            }
        ],
        "posix_mode": {"octal": "0600", "source": field_source},
    }
    row["profiles"] = [
        {
            "profile": profile_reference(PROFILE + "/profiles/filesystem-observation.schema.json"),
            "data": data,
        }
    ]
    row["profiles"].extend(
        {
            "profile": {
                "contract_id": "urn:test:unavailable",
                "contract_sha256": "b" * 64,
                "schema_id": f"urn:test:schema:{n}",
            },
            "data": {},
        }
        for n in range(count)
    )
    if count:
        row["metadata"][0]["interpretations"] = [
            {"asserted_by_agent_id": identity(20), "value": {"type": "integer", "value": "1"}}
        ]
    if rejected:
        row["metadata"].append(
            metadata(count + 10, {"type": "timestamp", "value": "2026-02-30T00:00:00Z"})
        )
    return row


class CommonWorkConnection(WorkConnection):
    @contextmanager
    def observe(self):
        with super().observe() as metrics:
            yield metrics
        metrics["bound_parameter_octets"] = sum(
            len(value) if isinstance(value, bytes) else len(str(value).encode())
            for _sql, parameters, _cursor in self.statements
            for value in parameters
            if value is not None
        )
        for sql, parameters, _cursor in self.statements:
            if "FROM assertions INDEXED BY semantic_reference_type" in sql:
                with closing(self.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as cursor:
                    plan = " ".join(row[3] for row in cursor.fetchall())
                assert "COVERING INDEX semantic_reference_type" in plan, plan


def binding_projection(operation):
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    return binding, projection, digest


@pytest.mark.growth_witness(
    "riverhog-generic-semantics-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generic_semantics.GenericSemantics.advance",
)
def test_actual_cold_common_semantic_units(tmp_path, catalog, record_property):
    binding, projection, digest = binding_projection(GenericSemantics.advance)
    phases = (
        "generic",
        "metadata-key",
        "unresolved-profile",
        "evidence",
        "identifier",
        "source",
        "profile",
        "filesystem",
        "principal",
        "stamp",
        "native",
        "interpretation",
        "typed",
        "keys",
        "outputs",
        "reject-frames",
        "reject-keys",
        "reject-outputs",
        "done",
        "rejected",
    )
    samples = []
    for phase in phases:
        regimes = [(1, 1), (15, 16), (16, 16), (17, 16), (129, 16), (1025, 64)]
        if phase in {"keys", "outputs", "done"}:
            regimes.append((0, 16))
        for count, budget in regimes:
            rejected = phase.startswith("reject")
            row = rich_subject(count, rejected) if count else subject(0)
            if count == 17:
                row["metadata"][0]["name"]["text"] = "k" * 65536
            path = tmp_path / f"{phase}-{count}.sqlite3"
            create(path, row)
            with closing(sqlite3.connect(":memory:")) as db:
                with closing(connect(path)) as original:
                    original.backup(db)
                # Independently retained inactive history shares the referenced
                # object identity. Active type lookup must not walk that history.
                if count in {17, 1025}:
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
                            for n in range(4097)
                        ),
                    ).close()
                    db.execute(
                        "UPDATE assertions SET value=? WHERE assertion_id=?",
                        (b"unread-active-payload" * 32768, identity(21)),
                    ).close()
                    db.commit()
                current = work(db, row, catalog)
                current.initialize()
                if phase in {"done", "rejected"}:
                    finish(current, budget=64)
                elif phase in {"keys", "outputs", "reject-frames", "reject-keys", "reject-outputs"}:
                    position(current, phase)
                elif phase == "metadata-key":
                    position(current, ("generic", "3"))
                elif phase == "unresolved-profile":
                    position(current, phase)
                else:
                    position(current, (phase, "0"))
                retained = workspace_rows(db)
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
                current = work(db, row, catalog)
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
                # Verify the exact suffix independently of the measured cold unit.
                with closing(sqlite3.connect(":memory:")) as suffix_db:
                    db.backup(suffix_db)
                    suffix = work(suffix_db, row, catalog)
                    finish(suffix, budget=64)
                    if rejected:
                        with pytest.raises(ProvenanceValidationError, match="calendar"):
                            suffix.result()
                        assert outputs(suffix_db) == []
                    else:
                        suffix.result()
                    require_reclaimed(suffix_db)
    from test_row_semantics import growth_samples as row_growth_samples

    samples.extend(row_growth_samples(tmp_path, catalog))
    from test_content_semantics import growth_samples as content_growth_samples

    samples.extend(content_growth_samples(tmp_path, catalog))
    from test_description_semantics import growth_samples as description_growth_samples

    samples.extend(description_growth_samples(tmp_path, catalog))
    from test_observation_semantics import growth_samples as observation_growth_samples

    samples.extend(observation_growth_samples(tmp_path, catalog))
    from test_edge_row_semantics import growth_samples as edge_growth_samples

    samples.extend(edge_growth_samples(tmp_path, catalog))
    from test_comparison_semantics import growth_samples as comparison_growth_samples

    samples.extend(comparison_growth_samples(tmp_path, catalog))
    from test_locator_semantics import growth_samples as locator_growth_samples

    samples.extend(locator_growth_samples(tmp_path, catalog))
    assert len(samples) == 1497
    record_property("growth_samples", samples)


def code_tree(code):
    result = {code}
    for constant in code.co_consts:
        if isinstance(constant, CodeType):
            result.update(code_tree(constant))
    return result


@pytest.mark.growth_witness(
    "riverhog-semantic-atom-index-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._semantic_program.Nodes.__init__",
)
def test_actual_semantic_atom_index_work(record_property):
    binding, projection, digest = binding_projection(Nodes.__init__)
    allowed = code_tree(inspect.unwrap(Nodes.__init__).__code__)
    samples = []
    for count, padding in [
        (0, 0),
        (1, 0),
        (15, 0),
        (16, 0),
        (17, 0),
        (129, 0),
        (1025, 0),
        (1, 1024 * 1024),
        (17, 65536),
    ]:
        value = {"members": [{"ordinal": n, "tags": [n, n + 1]} for n in range(count)]}
        if padding:
            value["opaque"] = "x" * padding
        pending, expected, items = [value], {}, 0
        while pending:
            current = pending.pop()
            items += 1
            if isinstance(current, dict):
                expected[id(current)] = current
                pending.extend(current.values())
            elif isinstance(current, list):
                expected[id(current)] = current
                pending.extend(current)
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

        previous = sys.gettrace()
        observed_frame = sys._getframe()
        previous_opcodes = observed_frame.f_trace_opcodes
        observed_frame.f_trace_opcodes = True
        try:
            sys.settrace(trace)
            nodes = Nodes(value)
        finally:
            sys.settrace(previous)
            observed_frame.f_trace_opcodes = previous_opcodes
        assert events > 0
        assert set(nodes.identities) == set(expected)
        assert all(
            nodes.values[nodes.identity(container)] is container for container in expected.values()
        )
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": digest,
            "dimensions": {"input_items": items},
            "metrics": {"indexed_containers": len(nodes.values), "python_opcode_events": events},
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)
    assert len(samples) == 9
    record_property("growth_samples", samples)
