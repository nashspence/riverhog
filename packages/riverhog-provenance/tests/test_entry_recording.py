from __future__ import annotations

import copy
import hashlib
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import encode_entry, parse_journal, validate_journal
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.journal_store import JournalStore
from test_assertion_staging import connect, create, identity
from test_entry_preparation import WorkConnection, owner

MODES = (
    "agent-only",
    "with-context",
    "agent-missing",
    "agent-wrong",
    "agent-inactive",
    "context-missing",
    "context-wrong",
    "context-inactive",
    "both-wrong",
)


def entry(mode="with-context"):
    result = {"recorded_by_agent_id": identity(100)}
    if mode != "agent-only":
        result["recording_context_id"] = identity(200)
    return result


def insert(db, assertion, object_id, kind, *, active=1, payload=0):
    body = canonical_json_bytes(
        {
            "assertion_id": identity(assertion),
            "id": identity(object_id),
            "type": kind,
            "kind": "software" if kind == "agent" else "delivery",
            "name": "x" * payload,
        }
    )
    db.execute(
        "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
        (
            identity(assertion),
            identity(object_id),
            "agents" if kind == "agent" else "contexts",
            kind,
            None,
            None,
            "0",
            body,
            active,
        ),
    ).close()


def prepare(path, mode, history=0, payload=0):
    create(path)
    with closing(connect(path)) as db:
        # Older wrong-type rows share the identities but are not effective.
        insert(db, 900000, 100, "context", active=0)
        insert(db, 900001, 200, "agent", active=0)
        if mode != "agent-missing":
            insert(
                db,
                1000,
                100,
                "context" if mode in {"agent-wrong", "both-wrong"} else "agent",
                active=0 if mode == "agent-inactive" else 1,
                payload=payload,
            )
        if mode != "context-missing":
            insert(
                db,
                2000,
                200,
                "agent" if mode in {"context-wrong", "both-wrong"} else "context",
                active=0 if mode == "context-inactive" else 1,
                payload=payload,
            )
        for ordinal in range(history):
            insert(
                db,
                1000000 + ordinal,
                2000000 + ordinal,
                "agent",
                active=ordinal % 2,
                payload=262144 if ordinal == 0 else 0,
            )
        db.commit()


def expected_error(mode):
    if mode in {"agent-missing", "agent-wrong", "agent-inactive", "both-wrong"}:
        return "entry recorder must resolve to an effective agent"
    if mode in {"context-missing", "context-wrong", "context-inactive"}:
        return "recording context does not resolve"
    return None


def check(db, document, mode):
    message = expected_error(mode)
    if message is None:
        assert owner(db)._check_entry_recording(document) is None
    else:
        with pytest.raises(ProvenanceValidationError, match=message):
            owner(db)._check_entry_recording(document)


def snapshot(db):
    result = {}
    for table in ("assertions", "frames", "journal_prefix"):
        with closing(db.execute("SELECT * FROM " + table + " ORDER BY 1")) as cursor:
            result[table] = cursor.fetchall()
    return result


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("query_only", [False, True])
def test_recording_uses_only_effective_types_and_preserves_read_only_state(
    tmp_path, mode, query_only
):
    path = tmp_path / "recording.sqlite3"
    prepare(path, mode, history=17, payload=1024 * 1024)
    document = entry(mode)
    before_document = copy.deepcopy(document)
    with closing(connect(path, factory=WorkConnection)) as db:
        before = snapshot(db)
        if query_only:
            db.execute("PRAGMA query_only=ON").close()
        with db.observe() as metrics:
            check(db, document, mode)
        assert metrics["sql_statements"] == (
            1 if mode == "agent-only" or mode.startswith("agent-") or mode == "both-wrong" else 2
        )
        assert metrics["changed_rows"] == metrics["historical_payload_octets"] == 0
        assert metrics["returned_metadata_octets"] <= 2
        assert snapshot(db) == before
        assert not db.in_transaction
    assert document == before_document


@pytest.mark.parametrize("wrong_agent", [False, True])
def test_public_journal_routes_recording_after_assertion_validation(
    journal, catalog, monkeypatch, wrong_agent
):
    document = parse_journal(journal)[0].document
    document["recording_context_id"] = identity(999999)
    if wrong_agent:
        document["recorded_by_agent_id"] = identity(999998)
    original = JournalStore._check_entry_recording
    seen = []

    def recording(store, value):
        seen.append(value["id"])
        return original(store, value)

    monkeypatch.setattr(JournalStore, "_check_entry_recording", recording)
    message = "entry recorder must resolve" if wrong_agent else "recording context does not resolve"
    with pytest.raises(ProvenanceValidationError, match=message):
        validate_journal(encode_entry(document, catalog=catalog), catalog=catalog)
    assert seen == [document["id"]]


def test_closed_summary_refuses_recording_lookup():
    store = JournalStore()
    store.close()
    with pytest.raises(RuntimeError, match="closed"):
        store._check_entry_recording(entry())


CRASH_PROGRAM = r"""
import os,sqlite3,sys
from test_entry_recording import connect,entry,owner
seam=sys.argv[2]
class DeathCursor(sqlite3.Cursor):
    def fetchone(self):
        result=super().fetchone()
        if seam=='after-'+str(self.ordinal):os._exit(73)
        return result
class DeathConnection(sqlite3.Connection):
    ordinal=0
    def execute(self,sql,parameters=()):
        if not sql.startswith('SELECT type = ?'):
            return super().execute(sql,parameters)
        self.ordinal+=1
        if seam=='before-'+str(self.ordinal):os._exit(73)
        cursor=self.cursor(factory=DeathCursor)
        cursor.ordinal=self.ordinal
        return cursor.execute(sql,parameters)
db=connect(sys.argv[1],factory=DeathConnection)
owner(db)._check_entry_recording(entry())
raise SystemExit(99)
"""


@pytest.mark.parametrize("seam", ["before-1", "after-1", "before-2", "after-2"])
def test_actual_process_death_replays_only_the_fixed_read_unit(tmp_path, seam):
    path = tmp_path / "recording.sqlite3"
    prepare(path, "with-context", history=17)
    with closing(connect(path)) as db:
        before = snapshot(db)
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys;sys.path.insert(0,sys.argv.pop(3));" + CRASH_PROGRAM,
            str(path),
            seam,
            str(Path(__file__).parent),
        ],
        capture_output=True,
        text=True,
    )
    assert child.returncode == 73, child.stderr
    with closing(connect(path)) as db:
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert snapshot(db) == before
        assert owner(db)._check_entry_recording(entry()) is None
        assert snapshot(db) == before


@pytest.mark.growth_witness(
    "riverhog-entry-recording-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_store.JournalStore._check_entry_recording",
)
def test_actual_cold_recording_checks(tmp_path, record_property):
    binding = growth_binding(JournalStore._check_entry_recording)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for ordinal, (history, mode) in enumerate(
        (n, mode) for n in (0, 1, 15, 16, 17, 129, 1025, 4097) for mode in MODES
    ):
        path = tmp_path / f"recording-{ordinal}.sqlite3"
        prepare(path, mode, history=history, payload=1024 * 1024 if history in (17, 4097) else 0)
        with closing(connect(path, factory=WorkConnection)) as db:
            with closing(db.execute("SELECT count(*) FROM assertions")) as cursor:
                retained = cursor.fetchone()[0]
            with db.observe() as metrics:
                check(db, entry(mode), mode)
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {"indexed_assertions": retained},
                "metrics": dict(metrics),
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
    assert len(samples) == 72
    record_property("growth_samples", samples)
