from __future__ import annotations

from contextlib import closing

import pytest
import test_assertion_staging as staging
import test_correction_projection as correction
import test_correction_targets as targets
import test_entry_preparation as preparation
import test_semantic_finish as finish
from test_generic_semantics import agent, connect
from test_semantic_owner import apply_owned, assert_uncommitted_hidden, owner_table, snapshot

FAMILIES = ("entry", "targets", "retire", "seed", "staging", "finish")
ADMISSION_BYTES = 16 * 1024 * 1024


def create_case(path, family):
    if family == "targets":
        source = targets.create(path, count=3)
        return lambda db: targets.work(db, source)
    if family in {"retire", "seed"}:
        correction.create(path)
        with closing(connect(path)) as db:
            if family == "retire":
                correction.fill(db, "maps", 3)
            else:
                correction.assertions(db, 3, 2)
            db.commit()
        return lambda db: correction.work(db, family)
    if family == "entry":
        staging.create(path)
        source = preparation.frame()
        return lambda db: preparation.work(db, source)
    if family == "staging":
        staging.create(path)
        source = staging.frame([agent()])
        return lambda db: staging.work(db, source)
    assert family == "finish"
    finish.create(path)
    return finish.work


def complete(current):
    for _ in range(100):
        if current.load().done:
            return
        current.commit(current.prepare(row_budget=1, byte_budget=ADMISSION_BYTES))
    raise AssertionError("the small child fixture did not finish")


@pytest.mark.parametrize("family", FAMILIES)
@pytest.mark.parametrize("admission", [False, True])
def test_each_child_shares_the_owner_transaction(tmp_path, family, admission):
    path = tmp_path / "owner.sqlite3"
    factory = create_case(path, family)
    with closing(connect(path)) as db:
        owner_table(db)
        current = factory(db)
        if admission:
            batch = current.prepare_admission()
        else:
            current.initialize()
            batch = current.prepare(row_budget=1, byte_budget=ADMISSION_BYTES)
        before = snapshot(db)
        db.execute("BEGIN IMMEDIATE").close()
        state = (current._admit if admission else current._apply)(batch)
        assert db.in_transaction
        assert_uncommitted_hidden(path, before)
        db.rollback()
        assert snapshot(db) == before
        assert apply_owned(current, batch, admission=admission) == state
        assert current.load() == state
        with closing(connect(path)) as reader:
            assert snapshot(reader) == snapshot(db)
        assert db.execute("SELECT version,checkpoint FROM semantic_owner").fetchone() == (
            "1",
            state.to_json(),
        )


@pytest.mark.parametrize("family", FAMILIES)
@pytest.mark.parametrize("admission", [False, True])
def test_replaced_owner_cannot_publish_a_child(tmp_path, family, admission):
    path = tmp_path / "owner.sqlite3"
    factory = create_case(path, family)
    with closing(connect(path)) as db:
        owner_table(db)
        current = factory(db)
        if admission:
            batch = current.prepare_admission()
        else:
            current.initialize()
            batch = current.prepare(row_budget=1, byte_budget=ADMISSION_BYTES)
        db.execute("UPDATE semantic_owner SET epoch='3'").close()
        db.commit()
        before = snapshot(db)
        with pytest.raises(RuntimeError, match="owner was replaced"):
            apply_owned(current, batch, admission=admission)
        assert not db.in_transaction and snapshot(db) == before


@pytest.mark.parametrize("family", FAMILIES)
def test_finished_child_can_share_a_parent_checkpoint(tmp_path, family):
    path = tmp_path / "owner.sqlite3"
    factory = create_case(path, family)
    with closing(connect(path)) as db:
        owner_table(db)
        current = factory(db)
        current.initialize()
        complete(current)
        batch = current.prepare(row_budget=1, byte_budget=ADMISSION_BYTES)
        before = snapshot(db)
        apply_owned(current, batch)
        after = snapshot(db)
        assert all(after[key] == rows for key, rows in before.items() if key != "semantic_owner")
        assert after["semantic_owner"] != before["semantic_owner"]


@pytest.mark.parametrize("family", [name for name in FAMILIES if name != "entry"])
def test_finished_synchronous_child_retains_its_read_only_transaction_boundary(tmp_path, family):
    path = tmp_path / "owner.sqlite3"
    factory = create_case(path, family)
    with closing(connect(path)) as db:
        current = factory(db)
        current.initialize()
        complete(current)
        batch = current.prepare(row_budget=1, byte_budget=ADMISSION_BYTES)
        statements = []
        db.execute("PRAGMA query_only=ON").close()
        db.set_trace_callback(statements.append)
        assert current.commit(batch).done
        db.set_trace_callback(None)
        assert not db.in_transaction
        assert all(
            not sql.strip().upper().startswith(("BEGIN", "COMMIT", "ROLLBACK"))
            for sql in statements
        )
