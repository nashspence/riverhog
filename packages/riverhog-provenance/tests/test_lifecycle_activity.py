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
from riverhog_provenance import create_journal, journal_store, validate_graph, validate_journal
from riverhog_provenance._activity_projection import read_activity
from riverhog_provenance._assertion_staging import AssertionStagingCapacity
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.journal import JournalFrame
from riverhog_provenance.journal_store import _StoredValidator
from test_assertion_staging import connect, contents, create, identity, proposed_octets, work
from test_entry_preparation import WorkConnection
from test_journal_lifecycle import lifecycle_graph

MODES = (
    "bare",
    "started",
    "ended",
    "both",
    "raw-calendar",
    "missing",
    "wrong",
    "retired",
    "reasserted",
    "missing-projection",
)


def activity(ordinal=1, *, kind="creation", payload=0, **fields):
    return {
        "assertion_id": identity(100 + ordinal),
        "id": identity(1000000 + ordinal),
        "type": "activity",
        "kind": kind,
        "evidence": [],
        "name": "x" * payload,
        **fields,
    }


def source():
    return JournalFrame(
        canonical_json_bytes(
            {
                "journal_id": identity(2),
                "sequence": "1",
                "body": {
                    "assertions": {
                        "activities": [
                            activity(1, started_at="0001-01-01T00:00:00Z"),
                            activity(
                                2, kind="materialization", ended_at="9999-12-31T23:59:59.999999999Z"
                            ),
                        ]
                    }
                },
            }
        )
    )


def insert(db, row, *, active=1, projection=True):
    db.execute(
        "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,?)",
        (
            row["assertion_id"],
            row["id"],
            "activities",
            row["type"],
            None,
            None,
            "0",
            canonical_json_bytes(row),
            active,
        ),
    ).close()
    if row["type"] == "activity" and projection:
        db.execute(
            "INSERT INTO activity_projection VALUES (?,?,?,?)",
            (
                row["assertion_id"],
                row["kind"],
                row.get("started_at"),
                row.get("ended_at"),
            ),
        ).close()


def prepare(path, mode, *, history=0, payload=0):
    create(path)
    current = activity(payload=payload)
    if mode in {"started", "both", "reasserted"}:
        current["started_at"] = "0001-01-01T00:00:00Z"
    if mode in {"ended", "both", "reasserted"}:
        current["ended_at"] = "9999-12-31T23:59:59.999999999Z"
    if mode == "raw-calendar":
        current["started_at"] = "2020-02-30T00:00:00Z"
    if mode == "wrong":
        current["type"], current["kind"] = "agent", "software"
    with closing(connect(path)) as db:
        # An old assertion at the same referent must never provide current fields.
        prior = activity(999, kind="observation", started_at="2026-01-01T00:00:00Z")
        prior["id"] = current["id"]
        insert(db, prior, active=0)
        if mode != "missing":
            insert(
                db,
                current,
                active=0 if mode == "retired" else 1,
                projection=mode != "missing-projection",
            )
        for n in range(history):
            insert(
                db,
                activity(10000 + n, kind="metadata_update", payload=262144 if n == 0 else 0),
                active=n % 2,
            )
        db.commit()
    return current


def check(db, row, mode):
    if mode in {"missing", "retired"}:
        with pytest.raises(ProvenanceValidationError, match="unresolved local identity"):
            read_activity(db, row["id"])
    elif mode == "wrong":
        with pytest.raises(ProvenanceValidationError, match="wrong reference type"):
            read_activity(db, row["id"])
    elif mode == "missing-projection":
        with pytest.raises(RuntimeError, match="missing staged activity metadata"):
            read_activity(db, row["id"])
    else:
        assert read_activity(db, row["id"]) == {
            key: row[key] for key in ("id", "type", "kind", "started_at", "ended_at") if key in row
        }


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("query_only", [False, True])
def test_activity_reads_effective_raw_metadata_without_payload_or_writes(
    tmp_path, mode, query_only
):
    path = tmp_path / "activity.sqlite3"
    row = prepare(path, mode, history=17, payload=1024 * 1024)
    original = copy.deepcopy(row)
    with closing(connect(path, factory=WorkConnection)) as db:
        before = contents(db)
        if query_only:
            db.execute("PRAGMA query_only=ON").close()
        with db.observe() as metrics:
            check(db, row, mode)
        assert metrics["sql_statements"] == (1 if mode in {"missing", "retired", "wrong"} else 2)
        assert metrics["changed_rows"] == metrics["historical_payload_octets"] == 0
        assert contents(db) == before and not db.in_transaction
    assert row == original


@pytest.mark.parametrize(
    "kind,fault",
    [
        ("usage", "valid"),
        ("generation", "valid"),
        ("invalidation", "valid"),
        ("generation", "capture"),
        ("generation", "conflict"),
        ("usage", "before"),
        ("invalidation", "after"),
        ("usage", "state-first"),
        ("usage", "missing-activity"),
    ],
)
def test_public_lifecycle_uses_projection_and_preserves_native_error_order(
    graph,
    who,
    catalog,
    monkeypatch,
    kind,
    fault,
):
    graph, _, event = lifecycle_graph(graph, who)
    target = graph["activities"][-1]
    target.update(started_at="2010-01-01T00:00:00Z", ended_at="2030-01-01T00:00:00Z")
    row = event(kind, "2020-01-01T00:00:00Z")
    row.update(assertion_id=identity(700001), id=identity(700002))
    graph["relations"] = [row]
    if fault == "capture":
        row["activity_id"] = graph["activities"][0]["id"]
    elif fault == "conflict":
        # Uniqueness precedes the second event's activity interval rejection.
        second = event(kind, "2000-01-01T00:00:00Z")
        second.update(assertion_id=identity(700003), id=identity(700004))
        graph["relations"].append(second)
    elif fault == "before":
        row["at"] = "2000-01-01T00:00:00Z"
    elif fault == "after":
        row["at"] = "2040-01-01T00:00:00Z"
    elif fault == "state-first":
        row["state"]["object_type"] = "agent"
        row["activity_id"] = identity(999)
    elif fault == "missing-activity":
        row["activity_id"] = identity(999)
    calls = []
    original = journal_store.read_activity

    def observed(db, identity):
        calls.append(identity)
        return original(db, identity)

    monkeypatch.setattr(journal_store, "read_activity", observed)
    if fault == "valid":
        validate_graph(graph, catalog=catalog)
        raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        with validate_journal(raw, catalog=catalog) as summary:
            assert summary.journal_sha256 == hashlib.sha256(raw).hexdigest()
            assert summary.graph["relations"] == graph["relations"]
    else:
        with pytest.raises(ProvenanceValidationError) as memory:
            validate_graph(graph, catalog=catalog)
        with pytest.raises(ProvenanceValidationError) as stored:
            create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
        assert str(stored.value) == str(memory.value)
    if fault == "state-first":
        assert calls == []
    else:
        assert calls and row["activity_id"] in calls


def test_closed_snapshot_refuses_lifecycle_activity_lookup():
    store = journal_store.JournalStore()
    stored = object.__new__(_StoredValidator)
    stored.store = store
    store.close()
    with pytest.raises(RuntimeError, match="closed"):
        stored._lifecycle_activity(identity(1))


def test_staging_reopen_copies_raw_fields_and_keeps_prefix_private(tmp_path):
    path = tmp_path / "stage.sqlite3"
    create(path)
    current_source = source()
    with closing(connect(path)) as db:
        work(db, current_source).initialize()
        prefix = contents(db)["journal_prefix"]
    for _ in range(2):
        with closing(connect(path)) as db:
            current = work(db, current_source)
            state = current.advance(row_budget=1)
            assert contents(db)["journal_prefix"] == prefix
            assert not db.in_transaction
    assert state.done
    with closing(connect(path)) as db:
        assert db.execute("SELECT * FROM activity_projection ORDER BY assertion_id").fetchall() == [
            (identity(101), "creation", "0001-01-01T00:00:00Z", None),
            (identity(102), "materialization", None, "9999-12-31T23:59:59.999999999Z"),
        ]
        check(db, activity(1, started_at="0001-01-01T00:00:00Z"), "started")


def test_projection_bytes_are_admitted_before_assertion_or_cursor_writes(tmp_path):
    path = tmp_path / "capacity.sqlite3"
    create(path)
    current_source = source()
    first = current_source.document["body"]["assertions"]["activities"][0]
    admission = proposed_octets("activities", first)
    with closing(connect(path)) as db:
        current = work(db, current_source)
        current.initialize()
        before = contents(db)
        with pytest.raises(AssertionStagingCapacity):
            current.advance(row_budget=1, byte_budget=admission - 1)
        assert contents(db) == before
        current.advance(row_budget=1, byte_budget=admission)
        assert len(contents(db)["activity_projection"]) == 1


CRASH_PROGRAM = r"""
import os,sqlite3,sys
from pathlib import Path
from test_lifecycle_activity import connect,work,JournalFrame
seam=sys.argv[3]
class DeathConnection(sqlite3.Connection):
    def execute(self,sql,parameters=()):
        if sql.startswith('INSERT INTO activity_projection') and seam=='projection-before':
            os._exit(73)
        result=super().execute(sql,parameters)
        if sql.startswith('INSERT INTO activity_projection') and seam=='projection-after':
            os._exit(73)
        return result
    def commit(self):
        if seam=='before-commit':os._exit(73)
        super().commit()
        if seam=='after-commit':os._exit(73)
db=connect(sys.argv[1],factory=DeathConnection)
current=work(db,JournalFrame(Path(sys.argv[2]).read_bytes()))
current.advance(row_budget=1)
raise SystemExit(99)
"""


@pytest.mark.parametrize(
    "seam",
    [
        "projection-before",
        "projection-after",
        "before-commit",
        "after-commit",
    ],
)
def test_actual_process_death_keeps_activity_assertion_and_progress_atomic(tmp_path, seam):
    path = tmp_path / "stage.sqlite3"
    expected_path = tmp_path / "expected.sqlite3"
    create(path)
    current_source = source()
    source_path = tmp_path / "source.json"
    source_path.write_bytes(current_source.json_bytes)
    with closing(connect(path)) as db:
        work(db, current_source).initialize()
        before = contents(db)
        with closing(connect(expected_path)) as expected_db:
            db.backup(expected_db)
    with closing(connect(expected_path)) as db:
        work(db, current_source).advance(row_budget=1)
        expected = contents(db)
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys;sys.path.insert(0,sys.argv.pop(4));" + CRASH_PROGRAM,
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
        current = work(db, current_source)
        while not current.advance(row_budget=1).done:
            pass
        assert contents(db)["journal_prefix"] == before["journal_prefix"]
        assert len(contents(db)["activity_projection"]) == 2


@pytest.mark.growth_witness(
    "riverhog-lifecycle-activity-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._activity_projection.read_activity",
)
def test_actual_cold_activity_metadata_reads(tmp_path, record_property):
    binding = growth_binding(read_activity)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for ordinal, (history, mode) in enumerate(
        (n, mode) for n in (0, 1, 15, 16, 17, 129, 1025, 4097) for mode in MODES
    ):
        path = tmp_path / f"activity-{ordinal}.sqlite3"
        row = prepare(
            path, mode, history=history, payload=1024 * 1024 if history in (17, 4097) else 0
        )
        with closing(connect(path, factory=WorkConnection)) as db:
            with closing(db.execute("SELECT count(*) FROM assertions")) as cursor:
                retained = cursor.fetchone()[0]
            with db.observe() as metrics:
                check(db, row, mode)
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {"indexed_assertions": retained},
                "metrics": dict(metrics),
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
    assert len(samples) == 80
    record_property("growth_samples", samples)
