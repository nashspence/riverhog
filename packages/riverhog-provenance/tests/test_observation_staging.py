from __future__ import annotations

import subprocess
import sys
from contextlib import closing
from dataclasses import replace

import pytest
import test_assertion_staging as staging
import test_row_semantics as rows
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import validate_execution_samples
from riverhog_provenance._assertion_staging import AssertionStaging, AssertionStagingCapacity
from riverhog_provenance._observation_projection import (
    _read,
    has_observer,
    read_observation_metadata,
)
from riverhog_provenance.errors import ProvenanceValidationError
from riverhog_provenance.journal import JournalFrame
from test_entry_preparation import WorkConnection
from test_generic_semantics import binding_projection, identity, workspace_rows


def activity(count=17):
    value = next(v for v in rows.references() if v["id"] == identity(38))
    value["associations"] = [
        {"agent_id": identity(50000 + n), "role": "urn:test:observer"} for n in range(count)
    ]
    return value


def source(count=17, *, with_state=False):
    if not with_state:
        return staging.frame([activity(count)])
    state = next(v for v in rows.references() if v["id"] == identity(33))
    return JournalFrame(
        canonical_json_bytes(
            {
                "journal_id": identity(2),
                "sequence": "1",
                "body": {
                    "assertions": {
                        "states": [state],
                        "activities": [activity(count)],
                    }
                },
            }
        )
    )


def finish(current, *, budget=16):
    for _ in range(10000):
        state = current.advance(row_budget=budget, byte_budget=4000000)
        if state.done:
            return state
    pytest.fail("raw observation staging did not finish")


@pytest.mark.parametrize("budget", [1, 15, 16, 17, 64])
@pytest.mark.parametrize("count", [1, 15, 16, 17, 129])
def test_raw_membership_reopens_at_each_checkpoint_without_early_activity_semantics(
    tmp_path, count, budget
):
    path = tmp_path / "observers.sqlite3"
    staging.create(path)
    raw = source(count)
    with closing(staging.connect(path)) as db:
        current = staging.work(db, raw)
        current.initialize()
        prefix = db.execute("SELECT * FROM journal_prefix").fetchall()
        first = current.advance(row_budget=budget, byte_budget=4000000)
        assert first.phase == "observers" and first.position == 1
        assert db.execute("SELECT observers_complete FROM observation_projection").fetchall() == [
            (0,)
        ]
        assert not db.execute("SELECT 1 FROM observation_observers").fetchall()
        with pytest.raises(RuntimeError, match="incomplete staged observation"):
            read_observation_metadata(db, activity()["id"], "activity")
    for _ in range(count + 2):
        with closing(staging.connect(path)) as db:
            current = staging.work(db, raw)
            before = current.load()
            state = current.advance(row_budget=budget)
            assert db.execute("SELECT * FROM journal_prefix").fetchall() == prefix
            assert not db.in_transaction
            actual = db.execute(
                "SELECT agent_id FROM observation_observers ORDER BY agent_id"
            ).fetchall()
            consumed = count if state.done else state.observer_cursor
            assert actual == [(identity(50000 + n),) for n in range(consumed)]
            assert consumed - before.observer_cursor <= budget
            if state.done:
                assert (
                    read_observation_metadata(db, activity()["id"], "activity")["outcome"]
                    == "success"
                )
                assert has_observer(db, activity()["id"], identity(50000 + count - 1))
                assert not has_observer(db, activity()["id"], identity(90000))
                break
    else:
        pytest.fail("raw observer membership did not finish")


def test_raw_membership_keeps_repeated_agents_and_forward_unresolved_references_private(tmp_path):
    path = tmp_path / "raw.sqlite3"
    staging.create(path)
    value = activity(3)
    value["associations"][-1]["agent_id"] = value["associations"][0]["agent_id"]
    value["started_at"] = "2026-02-30T00:00:00Z"
    raw = staging.frame([value])
    with closing(staging.connect(path)) as db:
        current = staging.work(db, raw)
        current.initialize()
        finish(current, budget=1)
        assert db.execute("SELECT count(*) FROM observation_observers").fetchone() == (2,)
        assert has_observer(db, value["id"], identity(50000))
        assert db.execute("SELECT 1 FROM assertions WHERE type='agent'").fetchone() is None
        assert db.execute("SELECT started_at FROM activity_projection").fetchone() == (
            value["started_at"],
        )


@pytest.mark.parametrize("final", [False, True])
def test_membership_byte_admission_includes_completion_write(tmp_path, final):
    path = tmp_path / "capacity.sqlite3"
    staging.create(path)
    raw = source(2)
    with closing(staging.connect(path)) as db:
        current = staging.work(db, raw)
        current.initialize()
        current.advance(row_budget=1, byte_budget=4000000)
        if final:
            current.advance(row_budget=1)
        budget = 90 + (46 if final else 0)
        before = staging.contents(db)
        with pytest.raises(AssertionStagingCapacity):
            current.advance(row_budget=1, byte_budget=budget - 1)
        assert staging.contents(db) == before
        state = current.advance(row_budget=1, byte_budget=budget)
        assert state.done is final


@pytest.mark.parametrize(
    "damage", ["projection", "outcome", "completion", "member", "cursor", "phase"]
)
def test_staging_reopen_rejects_missing_or_changed_raw_observer_links(tmp_path, damage):
    path = tmp_path / "corrupt.sqlite3"
    staging.create(path)
    raw = source(17)
    with closing(staging.connect(path)) as db:
        current = staging.work(db, raw)
        current.initialize()
        current.advance(row_budget=1, byte_budget=4000000)
        state = current.advance(row_budget=1)
        if damage == "projection":
            db.execute("DELETE FROM observation_projection").close()
        elif damage == "outcome":
            db.execute("UPDATE observation_projection SET outcome='failed'").close()
        elif damage == "completion":
            db.execute("UPDATE observation_projection SET observers_complete=1").close()
        elif damage == "member":
            db.execute("DELETE FROM observation_observers").close()
        else:
            state = replace(
                state,
                observer_cursor=18 if damage == "cursor" else 1,
                phase="stage" if damage == "phase" else "observers",
            )
            db.execute(
                "UPDATE journal_assertion_staging SET checkpoint=?", (state.to_json(),)
            ).close()
        db.commit()
        before = staging.contents(db)
        with pytest.raises(RuntimeError):
            current.advance()
        assert staging.contents(db) == before


@pytest.mark.parametrize("field", ["extent_kind", "outcome", "kind"])
@pytest.mark.parametrize(
    "value",
    ["x" * 131072, b"x" * 131072, "é", "", b"\x00"],
    ids=["oversized-text", "oversized-blob", "non-ascii", "empty", "nul"],
)
def test_observation_projection_rejects_corruption_after_bounded_reads(tmp_path, field, value):
    path = tmp_path / "projection-corruption.sqlite3"
    staging.create(path)
    raw = source(1)
    with closing(staging.connect(path, factory=WorkConnection)) as db:
        current = staging.work(db, raw)
        current.initialize()
        finish(current)
        table = "description_projection" if field == "kind" else "observation_projection"
        db.execute("UPDATE " + table + " SET " + field + "=?", (value,)).close()
        db.commit()
        before = staging.contents(db)
        with db.observe() as metrics:
            with pytest.raises(RuntimeError, match="invalid staged observation"):
                read_observation_metadata(db, activity()["id"], "activity")
        assert metrics["returned_metadata_octets"] <= 384
        assert metrics["changed_rows"] == metrics["historical_payload_octets"] == 0
        assert staging.contents(db) == before


@pytest.mark.parametrize(
    "mode", ["present", "absent", "retired", "wrong", "missing-projection", "incomplete"]
)
@pytest.mark.parametrize("query_only", [False, True])
def test_observation_projection_reads_close_cursors_and_use_effective_assertion(
    tmp_path, mode, query_only
):
    path = tmp_path / "projection.sqlite3"
    staging.create(path)
    raw = source(17)
    with closing(staging.connect(path, factory=WorkConnection)) as db:
        current = staging.work(db, raw)
        current.initialize()
        finish(current)
        db.execute(
            "UPDATE assertions SET value=?", (b"unread-capture-associations" * 65536,)
        ).close()
        if mode == "retired":
            db.execute("UPDATE assertions SET active=0").close()
        elif mode == "wrong":
            db.execute("UPDATE assertions SET type='context'").close()
        elif mode == "missing-projection":
            db.execute("DELETE FROM observation_projection").close()
        elif mode == "incomplete":
            db.execute("UPDATE observation_projection SET observers_complete=0").close()
        db.commit()
        if query_only:
            db.execute("PRAGMA query_only=ON").close()
        with db.observe() as metrics:
            if mode in {"retired", "wrong"}:
                with pytest.raises(ProvenanceValidationError):
                    has_observer(db, activity()["id"], identity(50000))
            elif mode in {"missing-projection", "incomplete"}:
                with pytest.raises(RuntimeError):
                    has_observer(db, activity()["id"], identity(50000))
            else:
                assert has_observer(
                    db, activity()["id"], identity(50000 if mode == "present" else 90000)
                ) is (mode == "present")
        assert metrics["sql_statements"] <= 3
        assert metrics["returned_metadata_octets"] <= 384
        assert metrics["historical_payload_octets"] == metrics["changed_rows"] == 0


def growth_samples(tmp_path):
    binding, projection, digest = binding_projection(AssertionStaging.advance)
    samples = []
    for count, budget in ((1, 1), (15, 16), (16, 16), (17, 16), (129, 16), (4097, 64)):
        for phase in ("first", "last", "done"):
            path = tmp_path / f"staging-observers-{count}-{phase}.sqlite3"
            staging.create(path)
            raw = source(count)
            with closing(staging.connect(path)) as db:
                current = staging.work(db, raw)
                current.initialize()
                current.advance(row_budget=1, byte_budget=4000000)
                if phase == "last":
                    while current.load().observer_cursor + budget < count:
                        current.advance(
                            row_budget=min(64, count - current.load().observer_cursor - budget)
                        )
                elif phase == "done":
                    finish(current, budget=64)
                retained = workspace_rows(db)
            with closing(staging.connect(path, factory=staging.WorkConnection)) as db:
                current = staging.work(db, raw)
                with db.observe() as metrics:
                    current.advance(row_budget=budget, byte_budget=65536)
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": digest,
                    "dimensions": {
                        "retained_rows": retained,
                        "row_budget": budget,
                        "byte_budget": 65536,
                        "max_atom_octets": 136,
                    },
                    "metrics": {
                        bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds
                    },
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                assert metrics["changed_rows"] <= budget + 2
                assert metrics["historical_payload_fetched_octets"] == 0
                samples.append(sample)
    return samples


@pytest.mark.growth_witness(
    "riverhog-observation-metadata-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._observation_projection._read",
)
def test_actual_cold_observation_metadata_and_membership_growth(tmp_path, record_property):
    binding, projection, digest = binding_projection(_read)
    samples = []
    for count in (1, 15, 16, 17, 129, 1025, 4097):
        path = tmp_path / f"projection-{count}.sqlite3"
        staging.create(path)
        with closing(staging.connect(path)) as db:
            current = staging.work(db, source(count, with_state=True))
            current.initialize()
            finish(current, budget=64)
            db.execute(
                "UPDATE assertions SET value=?", (b"unread-historical-payload" * 65536,)
            ).close()
            db.commit()
            retained = workspace_rows(db)
        for operation in ("fields", "state", "present", "absent"):
            with closing(staging.connect(path, factory=WorkConnection)) as db:
                with db.observe() as metrics:
                    if operation == "state":
                        assert read_observation_metadata(db, identity(33), "state")["extent"] == {
                            "kind": "whole"
                        }
                    elif operation == "fields":
                        assert (
                            read_observation_metadata(db, activity()["id"], "activity")["kind"]
                            == "observation"
                        )
                    else:
                        assert has_observer(
                            db,
                            activity()["id"],
                            identity(50000 + count - 1 if operation == "present" else 90000),
                        ) is (operation == "present")
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": digest,
                    "dimensions": {"indexed_rows": retained},
                    "metrics": dict(metrics),
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                assert metrics["historical_payload_octets"] == 0
                samples.append(sample)
    record_property("growth_samples", samples)


STAGING_CRASH = r"""
import os,sqlite3,sys
from riverhog_provenance._assertion_staging import AssertionStaging
from riverhog_provenance.journal import JournalFrame
class Connection(sqlite3.Connection):
    active=False
    def execute(self,sql,parameters=()):
        result=super().execute(sql,parameters)
        prefixes={'header':'UPDATE journal_assertion_staging',
                  'member':'INSERT INTO observation_observers',
                  'complete':'UPDATE observation_projection'}
        if self.active and sql.startswith(prefixes.get(sys.argv[3],'impossible')):
            os._exit(73)
        return result
    def commit(self):
        if self.active and sys.argv[3]=='before-commit': os._exit(73)
        super().commit()
        if self.active and sys.argv[3]=='after-commit': os._exit(73)
db=sqlite3.connect(sys.argv[1],factory=Connection)
db.execute('PRAGMA synchronous=FULL').close()
current=AssertionStaging(db,attempt_id='owned-staging-attempt',
    frame=JournalFrame(open(sys.argv[2],'rb').read()),catalog_sha256='a'*64,require_profiles=False)
batch=current.prepare(row_budget=1)
db.active=True
current.commit(batch)
raise SystemExit(99)
"""


@pytest.mark.parametrize(
    "final,seam",
    [
        (final, seam)
        for final in (False, True)
        for seam in ("header", "member", "before-commit", "after-commit")
    ]
    + [(True, "complete")],
)
def test_real_process_death_atomically_preserves_membership_completion_and_cursor(
    tmp_path, final, seam
):
    path, expected_path = tmp_path / "kill.sqlite3", tmp_path / "expected.sqlite3"
    raw = source(3)
    raw_path = tmp_path / "source.json"
    raw_path.write_bytes(raw.json_bytes)
    staging.create(path)
    with closing(staging.connect(path)) as db:
        current = staging.work(db, raw)
        current.initialize()
        current.advance(row_budget=1, byte_budget=4000000)
        if final:
            current.advance(row_budget=2)
        before = staging.contents(db)
        with closing(staging.connect(expected_path)) as other:
            db.backup(other)
    with closing(staging.connect(expected_path)) as db:
        staging.work(db, raw).advance(row_budget=1)
        expected = staging.contents(db)
    child = subprocess.run(
        [sys.executable, "-c", STAGING_CRASH, str(path), str(raw_path), seam],
        capture_output=True,
        text=True,
    )
    assert child.returncode == 73, child.stderr
    with closing(staging.connect(path)) as db:
        assert db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert staging.contents(db) == (expected if seam == "after-commit" else before)
        current = staging.work(db, raw)
        finish(current, budget=1)
        assert db.execute("SELECT count(*) FROM observation_observers").fetchone() == (3,)
        assert has_observer(db, activity()["id"], identity(50002))


def test_stale_observer_proposal_cannot_write_after_a_newer_checkpoint(tmp_path):
    from riverhog_provenance._assertion_staging import StaleAssertionStaging

    path = tmp_path / "stale.sqlite3"
    staging.create(path)
    raw = source(3)
    with closing(staging.connect(path)) as db:
        current = staging.work(db, raw)
        current.initialize()
        current.advance(row_budget=1, byte_budget=4000000)
        proposal = current.prepare(row_budget=2)
        current.advance(row_budget=1)
        before = staging.contents(db)
        with pytest.raises(StaleAssertionStaging):
            current.commit(proposal)
        assert staging.contents(db) == before
