from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_journal_cleanup as cleanup
from riverhog_core.services import upload_journal_epochs as epochs
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from sqlalchemy import event, func, insert, select, update

from tests.unit.test_journal_capture import Frame, retained
from tests.unit.test_journal_epoch_union import uid
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_journal_validation_handoff import complete_capture, terminal
from tests.unit.test_staged_journal_range import JOURNAL, Journal

Retirement = cleanup.Retirement


def prepare(factory):
    work = complete_capture(factory)
    epochs._begin_scan(factory, work)
    return work


def replace_attempt(factory, work, attempt):
    with factory() as session, session.begin():
        session.execute(update(Journal).values(capture_attempt_id=attempt))
    current = replace(work, attempt_id=attempt)
    epochs._begin_scan(factory, current)
    return current


def rows(phase, attempt, count, directory="retained-directory"):
    for n in range(count):
        row = {"collection_id": 1, "journal_id": JOURNAL, "attempt_id": attempt}
        if phase in {0, 1, 2, 3, 4}:
            row.update(epoch=n if phase in {1, 4} else 0, kind="derivation")
        if phase == 0:
            row.update(vertex_id=uid(n), remaining_degree=0, rank=n, ready=False)
        elif phase == 1:
            row.update(
                vertex_count=0, arc_count=0, settled_vertices=0, visited_arcs=0, state="acyclic"
            )
        elif phase == 2:
            row.update(source_id=uid(n), target_id=uid(n + 1))
        elif phase == 3:
            row.update(vertex_id=uid(n), indegree=0)
        elif phase == 4:
            row.update(vertex_count=0, arc_count=0, state="complete")
        elif phase == 5:
            row.update(
                assertion_id=uid(n),
                frame_ordinal=0,
                item_ordinal=n,
                kind="derivation",
                source_id=uid(n),
                target_id=uid(n + 1),
                active=bool(n % 2),
            )
        else:
            row.update(
                frame_ordinal=n,
                array_kind="relations",
                item_ordinal=0,
                directory_id=directory,
                epoch=0,
                byte_offset=0,
                json_bytes=1,
                json_sha256="0" * 64,
                assertion_id=uid(n),
                kind="derivation",
                source_id=uid(n),
                target_id=uid(n + 1),
            )
        yield row


def populate(
    factory, attempt, count, *, phases=(0, 1, 2, 3, 4, 5, 6), directory="retained-directory"
):
    with factory() as session, session.begin():
        for phase in phases:
            session.execute(
                insert(cleanup._TABLES[phase]), list(rows(phase, attempt, count, directory))
            )


def counts(factory, attempt):
    with factory() as session:
        return tuple(
            session.scalar(
                select(func.count()).select_from(table).where(table.c.attempt_id == attempt)
            )
            for table in cleanup._TABLES
        )


def queued(factory):
    with factory() as session:
        return tuple(
            tuple(row)
            for row in session.execute(
                select(
                    Retirement.retired_attempt_id,
                    Retirement.directory_id,
                    Retirement.version,
                    Retirement.phase,
                    Retirement.algorithm,
                ).order_by(Retirement.retired_attempt_id)
            )
        )


def finish(factory, work):
    for calls in range(5000):
        if cleanup.advance_retired_upload_journal_preparation(factory, work):
            return calls
        assert terminal(factory)["state"] == "validating" and terminal(factory)["anchor"] is None
    pytest.fail("retired preparation stopped progressing")


def test_replacement_atomically_queues_and_reclaims_only_its_private_attempt(tmp_path, monkeypatch):
    raw = source(17, journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        original = prepare(factory)
        populate(factory, original.attempt_id, 1025, directory=original.directory_id)
        current = replace_attempt(factory, original, uid(900000))
        populate(factory, current.attempt_id, 17)
        populate(factory, uid(900001), 3)
        assert queued(factory) == (
            (original.attempt_id, original.directory_id, 0, 0, cleanup.RETIREMENT_ALGORITHM),
        )
        assert epochs._begin_scan(factory, current).next_frame == 0
        assert len(queued(factory)) == 1
        before_source = owner._read_upload_journal_validation(factory, 1, JOURNAL)
        deleted = []
        commit = cleanup._commit

        def observed(factory, work):
            result = commit(factory, work)
            assert engine.pool.checkedout() == 0
            deleted.append((work.phase, result))
            return result

        monkeypatch.setattr(cleanup, "_commit", observed)
        assert finish(factory, current) == 17 * 7
        assert all(0 <= count <= 64 for _phase, count in deleted)
        assert sum(count for _phase, count in deleted) == 1025 * 7
        assert counts(factory, original.attempt_id) == (0,) * 7
        assert counts(factory, current.attempt_id) == (17,) * 7
        assert counts(factory, uid(900001)) == (3,) * 7
        assert not queued(factory)
        assert owner._read_upload_journal_validation(factory, 1, JOURNAL) == before_source
        from riverhog_core.services.upload_journal_source import UploadJournalValidationSource

        source_reader = UploadJournalValidationSource(
            factory, current, catalog=owner.admission_provenance_catalog()
        )
        pieces, offset = [], 0
        while offset < len(raw):
            piece = source_reader.read_window(offset, min(65536, len(raw) - offset))
            assert piece
            pieces.append(piece)
            offset += len(piece)
        assert b"".join(pieces) == raw
        with factory() as session:
            assert (
                session.scalar(select(func.count()).select_from(Frame))
                == current.cursor.frame_count
            )
        assert epochs.advance_upload_journal_epoch_scan(factory, current)
        assert terminal(factory)["anchor"] is None


def test_epoch_coordinator_yields_after_one_retirement_unit_before_new_preparation(
    tmp_path, monkeypatch
):
    with retained(tmp_path, source(1, journal_id=JOURNAL)) as (engine, factory):
        old = prepare(factory)
        populate(factory, old.attempt_id, 129, phases=(0,))
        current = replace_attempt(factory, old, uid(900000))
        compute = epochs._compute_scan

        def forbidden(*_args):
            pytest.fail("new epoch work ran while retirement remains pending")

        monkeypatch.setattr(epochs, "_compute_scan", forbidden)
        assert not epochs.advance_upload_journal_epoch_scan(factory, current)
        assert counts(factory, old.attempt_id)[0] == 65
        assert epochs._begin_scan(factory, current).next_frame == 0
        finish(factory, current)
        monkeypatch.setattr(epochs, "_compute_scan", compute)
        assert epochs.advance_upload_journal_epoch_scan(factory, current)
        assert engine.pool.checkedout() == 0


@pytest.mark.parametrize(
    "changed", ["capture_attempt_id", "capture_directory_id", "capture_version", "sha256"]
)
def test_lost_worker_cannot_retire_any_rows_or_advance_the_checkpoint(tmp_path, changed):
    with retained(tmp_path, source(1, journal_id=JOURNAL)) as (_, factory):
        old = prepare(factory)
        populate(factory, old.attempt_id, 129)
        current = replace_attempt(factory, old, uid(900000))
        valid, work = cleanup._begin(factory, current)
        assert valid and work is not None
        before = queued(factory), counts(factory, old.attempt_id)
        with factory() as session, session.begin():
            session.execute(
                update(Journal).values({changed: 99 if changed == "capture_version" else "f" * 64})
            )
        assert cleanup._commit(factory, work) is None
        assert cleanup._begin(factory, current) == (False, None)
        assert not cleanup.advance_retired_upload_journal_preparation(factory, current)
        assert (queued(factory), counts(factory, old.attempt_id)) == before


def test_duplicate_worker_and_another_replacement_preserve_exact_attempt_custody(tmp_path):
    with retained(tmp_path, source(1, journal_id=JOURNAL)) as (_, factory):
        old = prepare(factory)
        populate(factory, old.attempt_id, 129)
        middle = replace_attempt(factory, old, uid(900000))
        populate(factory, middle.attempt_id, 17)
        _, work = cleanup._begin(factory, middle)
        assert cleanup._commit(factory, work) == 64
        before = queued(factory), counts(factory, old.attempt_id)
        assert cleanup._commit(factory, work) is None
        assert (queued(factory), counts(factory, old.attempt_id)) == before
        _, stale = cleanup._begin(factory, middle)
        current = replace_attempt(factory, middle, uid(900001))
        populate(factory, current.attempt_id, 3)
        assert len(queued(factory)) == 2
        before = (
            queued(factory),
            counts(factory, old.attempt_id),
            counts(factory, middle.attempt_id),
        )
        assert cleanup._commit(factory, stale) is None
        assert (
            queued(factory),
            counts(factory, old.attempt_id),
            counts(factory, middle.attempt_id),
        ) == before
        finish(factory, current)
        assert counts(factory, old.attempt_id) == counts(factory, middle.attempt_id) == (0,) * 7
        assert counts(factory, current.attempt_id) == (3,) * 7


@pytest.mark.parametrize("phase", [0, 6])
def test_late_checkpoint_failure_rolls_back_the_bounded_deletion(tmp_path, phase):
    with retained(tmp_path, source(1, journal_id=JOURNAL)) as (engine, factory):
        old = prepare(factory)
        populate(factory, old.attempt_id, 3, phases=(phase,))
        current = replace_attempt(factory, old, uid(900000))
        with factory() as session, session.begin():
            session.execute(update(Retirement).values(phase=phase))
        _, work = cleanup._begin(factory, current)
        before = queued(factory), counts(factory, old.attempt_id)

        def fail(_conn, _cursor, statement, _params, _context, _many):
            if statement.startswith(
                (
                    "UPDATE collection_upload_provenance_retired_preparations SET phase=",
                    "DELETE FROM collection_upload_provenance_retired_preparations",
                )
            ):
                raise RuntimeError("injected retirement publication failure")

        event.listen(engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="injected"):
                cleanup._commit(factory, work)
        finally:
            event.remove(engine, "before_cursor_execute", fail)
        assert (queued(factory), counts(factory, old.attempt_id)) == before
        finish(factory, current)


@pytest.mark.parametrize("committed", [False, True])
def test_process_death_reopens_only_atomically_committed_retirement(tmp_path, committed):
    with retained(tmp_path, source(1, journal_id=JOURNAL)) as (engine, factory):
        old = prepare(factory)
        populate(factory, old.attempt_id, 129, phases=(0,))
        current = replace_attempt(factory, old, uid(900000))
        before = queued(factory), counts(factory, old.attempt_id)
        code = """import os,sys
from sqlalchemy import create_engine,event
from sqlalchemy.orm import sessionmaker
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_journal_cleanup as cleanup
engine=create_engine(sys.argv[1]); factory=sessionmaker(engine)
source=owner._read_upload_journal_validation(factory,1,sys.argv[2])
valid,work=cleanup._begin(factory,source)
assert valid and work is not None
if sys.argv[3]=='before':
    event.listen(engine,'commit',lambda _connection: os._exit(73))
assert cleanup._commit(factory,work)==64
os._exit(73)
"""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                code,
                str(engine.url),
                JOURNAL,
                "after" if committed else "before",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 73, result.stderr
        engine.dispose()
        if committed:
            assert counts(factory, old.attempt_id)[0] == 65
            assert queued(factory)[0][2:4] == (1, 0)
        else:
            assert (queued(factory), counts(factory, old.attempt_id)) == before
        finish(factory, current)
        assert counts(factory, old.attempt_id) == (0,) * 7 and not queued(factory)


@pytest.mark.parametrize("fault", ["algorithm", "current-attempt", "directory"])
def test_untrusted_retirement_binding_fails_closed(tmp_path, fault):
    with retained(tmp_path, source(1, journal_id=JOURNAL)) as (_, factory):
        old = prepare(factory)
        populate(factory, old.attempt_id, 3)
        current = replace_attempt(factory, old, uid(900000))
        with factory() as session, session.begin():
            values = (
                {"algorithm": "unrecognized"}
                if fault == "algorithm"
                else (
                    {"retired_attempt_id": current.attempt_id}
                    if fault == "current-attempt"
                    else {"directory_id": ""}
                )
            )
            session.execute(update(Retirement).values(**values))
        before = queued(factory), counts(factory, old.attempt_id)
        with pytest.raises(RuntimeError, match="checkpoint"):
            cleanup._begin(factory, current)
        assert (queued(factory), counts(factory, old.attempt_id)) == before


def test_retirement_checkpoint_preserves_owning_large_version_domain(tmp_path):
    with retained(tmp_path, source(1, journal_id=JOURNAL)) as (_, factory):
        old = prepare(factory)
        current = replace_attempt(factory, old, uid(900000))
        with factory() as session, session.begin():
            session.execute(update(Retirement).values(version=2**200))
        _, work = cleanup._begin(factory, current)
        assert cleanup._commit(factory, work) == 0
        assert queued(factory)[0][2] == 2**200 + 1


class ObservedDeletion:
    def __init__(self, rows, counts):
        self.rows, self.counts = rows, counts

    def __enter__(self):
        self.rows.__enter__()
        return self

    def __iter__(self):
        for row in self.rows:
            self.counts["deleted_rows"] += 1
            yield row

    def __exit__(self, *args):
        try:
            return self.rows.__exit__(*args)
        finally:
            self.counts["closed"] = self.rows.closed


@pytest.mark.growth_witness(
    "riverhog-retired-journal-preparation-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_cleanup._delete_batch",
)
def test_actual_indexed_retired_preparation_deletion_work(tmp_path, monkeypatch, record_property):
    binding = growth_binding(cleanup._delete_batch)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for phase in range(7):
        for count, budget in [(1, 1), (17, 16), (129, 16), (1025, 64)]:
            with retained(tmp_path / f"{phase}-{count}", source(1, journal_id=JOURNAL)) as (
                engine,
                factory,
            ):
                source_work = prepare(factory)
                target, neighbor = uid(999998), uid(999997)
                for attempt in (source_work.attempt_id, neighbor, target):
                    populate(factory, attempt, count, phases=(phase,))
                work = cleanup._Work(source_work, target, "retained-directory", 0, phase)
                with engine.connect() as connection:
                    native = connection.connection.driver_connection
                    assert isinstance(native, sqlite3.Connection)
                statements, instructions, payload_reads = [], [0], []
                counts_out = {"deleted_rows": 0, "closed": False}

                def observe(_conn, _cursor, statement, params, _context, _many, output=statements):
                    output.append((statement, params))

                def tick(counter=instructions):
                    counter[0] += 1
                    return 0

                def authorize(action, table, column, _database, _trigger, reads=payload_reads):
                    if action == sqlite3.SQLITE_READ and column in {
                        "content",
                        "topology_cursor_json",
                        "union_cursor_json",
                        "rank_cursor_json",
                        "replay_binding_json",
                    }:
                        reads.append((table, column))
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                with factory() as session, session.begin():
                    execute = session.execute

                    def observed_execute(*args, base=execute, observed=counts_out, **kwargs):
                        return ObservedDeletion(base(*args, **kwargs), observed)

                    event.listen(engine, "before_cursor_execute", observe)
                    native.set_authorizer(authorize)
                    native.set_progress_handler(tick, 1)
                    try:
                        with monkeypatch.context() as patch:
                            patch.setattr(session, "execute", observed_execute)
                            actual = cleanup._delete_batch(session, work, row_budget=budget)
                    finally:
                        native.set_progress_handler(None, 0)
                        native.set_authorizer(None)
                        event.remove(engine, "before_cursor_execute", observe)
                assert actual == min(count, budget) == counts_out["deleted_rows"]
                assert counts_out["closed"] and not payload_reads and engine.pool.checkedout() == 0
                table = cleanup._TABLES[phase]
                for statement, params in statements:
                    with engine.connect() as connection:
                        with connection.exec_driver_sql(
                            "EXPLAIN QUERY PLAN " + statement, params
                        ) as plan_rows:
                            plan = " ".join(row[3] for row in plan_rows)
                    assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan
                with factory() as session:
                    remaining = {
                        attempt: session.scalar(
                            select(func.count())
                            .select_from(table)
                            .where(table.c.attempt_id == attempt)
                        )
                        for attempt in (source_work.attempt_id, neighbor, target)
                    }
                assert remaining == {
                    source_work.attempt_id: count,
                    neighbor: count,
                    target: count - actual,
                }
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": digest,
                    "dimensions": {"retained_rows": 3 * count, "row_budget": budget},
                    "metrics": {
                        "sql_statements": len(statements),
                        "deleted_rows": actual,
                        "source_payload_octets": 0,
                        "sqlite_vm_instructions": instructions[0],
                    },
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
    record_property("growth_samples", samples)
