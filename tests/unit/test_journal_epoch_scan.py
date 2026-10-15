from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
from contextlib import contextmanager

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_models import CollectionUploadProvenanceCaptureSegmentRecord as Segment
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_journal_epochs as epochs
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import parse_journal, validate_journal
from sqlalchemy import delete, event, select, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_journal_validation_handoff import complete_capture, service_for, terminal
from tests.unit.test_staged_journal_range import JOURNAL, Journal

Frame, Scan = epochs.Frame, epochs.Scan


def transcript(kinds, *, octets=0):
    return b"".join(
        b"\x1e" + canonical_json_bytes({"entry_kind": kind, "notes": "x" * octets}) + b"\n"
        for kind in kinds
    )


def progress(factory):
    with factory() as session:
        row = session.get(Scan, (1, JOURNAL))
        return (
            None
            if row is None
            else (row.version, row.next_frame, row.byte_offset, row.epoch, row.state)
        )


def facts(factory):
    with factory() as session:
        return [
            tuple(row)
            for row in session.execute(
                select(
                    Frame.ordinal,
                    Frame.epoch_ordinal,
                    Frame.epoch_entry_kind,
                    Frame.epoch_json_sha256,
                )
                .where(Frame.epoch_attempt_id.is_not(None))
                .order_by(Frame.ordinal)
            )
        ]


def finish(factory, work):
    while not epochs.advance_upload_journal_epoch_scan(factory, work):
        assert terminal(factory)["state"] == "validating"
        assert terminal(factory)["anchor"] is None


def test_real_correction_boundaries_cross_bounded_batches_without_accepting_semantics(tmp_path):
    kinds = ["journal_init"] + [
        "correction" if n in {1, 63, 64, 65, 128} else "assertion" for n in range(1, 130)
    ]
    raw = transcript(kinds)
    with retained(tmp_path, raw) as (_, factory):
        work = complete_capture(factory)
        before = 0
        calls = 0
        while True:
            ready = epochs.advance_upload_journal_epoch_scan(factory, work)
            checkpoint = progress(factory)
            assert 0 < checkpoint[1] - before <= epochs._FRAME_BUDGET
            before = checkpoint[1]
            calls += 1
            assert terminal(factory)["state"] == "validating"
            assert terminal(factory)["anchor"] is None
            if ready:
                break
        assert calls == 3 and checkpoint[1:3] == (len(kinds), len(raw))
        epoch = 0
        frames = parse_journal(raw)
        for (ordinal, actual, kind, digest), frame in zip(facts(factory), frames, strict=True):
            epoch += int(ordinal > 0 and kind == "correction")
            assert actual == epoch and kind == kinds[ordinal]
            assert digest == hashlib.sha256(frame.json_bytes).hexdigest()
        assert checkpoint[-1] == "complete"


def test_one_entry_bounded_atom_can_cross_page_and_batch_byte_boundaries(tmp_path):
    raw = transcript(["journal_init", "correction", "assertion"], octets=2 * 65536 + 37)
    with retained(tmp_path, raw) as (engine, factory):
        work = complete_capture(factory)
        for ordinal in range(3):
            ready = epochs.advance_upload_journal_epoch_scan(factory, work)
            assert progress(factory)[1] == ordinal + 1
            assert ready is (ordinal == 2)
            assert engine.pool.checkedout() == 0
        assert [row[1] for row in facts(factory)] == [0, 1, 1]


def test_decoding_and_hashing_run_after_catalog_readers_close(tmp_path, monkeypatch):
    raw = source(2 * 65536, journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        work = complete_capture(factory)
        classify = epochs.classify_epoch_boundary
        calls = []

        def detached(*args, **kwargs):
            assert engine.pool.checkedout() == 0
            calls.append(True)
            return classify(*args, **kwargs)

        monkeypatch.setattr(epochs, "classify_epoch_boundary", detached)
        assert epochs.advance_upload_journal_epoch_scan(factory, work)
        assert calls == [True] and engine.pool.checkedout() == 0
        assert terminal(factory)["anchor"] is None


def test_a_detached_batch_reuses_only_its_current_exact_source_page(tmp_path, monkeypatch):
    with retained(tmp_path, transcript(["journal_init"] * 129)) as (_, factory):
        source_work = complete_capture(factory)
        work = epochs._begin_scan(factory, source_work)
        read = owner._read_upload_journal_capture_segment
        loaded = []

        def observed(*args, **kwargs):
            loaded.append(kwargs["ordinal"])
            return read(*args, **kwargs)

        monkeypatch.setattr(owner, "_read_upload_journal_capture_segment", observed)
        advance = epochs._compute_scan(factory, work)
        assert advance.next_frame == 64
        assert loaded == [0]
        with factory() as session, session.begin():
            session.execute(update(Journal).values(capture_attempt_id="replacement"))
        assert not epochs._commit_scan(factory, work, advance)
        assert not facts(factory)


def test_native_replay_remains_the_only_acceptance_authority(tmp_path, monkeypatch):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        complete_capture(factory)
        native = owner._compute_upload_journal_validation
        calls = []

        def observed(*args):
            assert engine.pool.checkedout() == 0
            assert progress(factory)[-1] == "complete"
            assert terminal(factory)["anchor"] is None
            calls.append(True)
            return native(*args)

        monkeypatch.setattr(owner, "_compute_upload_journal_validation", observed)
        service_for(factory)._advance_provenance_journal_validation(1, JOURNAL)
        assert calls == [True]
        with validate_journal(raw) as expected:
            assert terminal(factory)["anchor"] == expected.anchor


@pytest.mark.parametrize("suffix", [b"\x1e{}\n", b"\x1e[1]\n", b"\x1e", b"x"])
def test_late_speculative_failure_never_outranks_an_earlier_native_prefix_error(tmp_path, suffix):
    document = parse_journal(source(journal_id=JOURNAL))[0].document
    document["sequence"] = "7"
    raw = b"\x1e" + canonical_json_bytes(document) + b"\n" + suffix
    with pytest.raises(ValueError) as expected:
        validate_journal(raw)
    with retained(tmp_path, raw) as (_, factory):
        work = complete_capture(factory)
        finish(factory, work)
        assert progress(factory)[-1] == "deferred"
        outcome = owner._compute_upload_journal_validation(factory, work)
        assert outcome.failure == str(expected.value)
        assert terminal(factory)["anchor"] is None


@pytest.mark.parametrize(
    "changed", ["capture_attempt_id", "capture_directory_id", "capture_version", "sha256"]
)
def test_stale_epoch_worker_cannot_publish_source_or_attempt_progress(tmp_path, changed):
    with retained(tmp_path, transcript(["journal_init", "correction"])) as (_, factory):
        source_work = complete_capture(factory)
        work = epochs._begin_scan(factory, source_work)
        advance = epochs._compute_scan(factory, work)
        before = progress(factory), facts(factory)
        value = 99 if changed == "capture_version" else "f" * 64
        with factory() as session, session.begin():
            session.execute(update(Journal).values({changed: value}))
        assert not epochs._commit_scan(factory, work, advance)
        assert (progress(factory), facts(factory)) == before


def test_two_computations_cannot_publish_the_same_epoch_batch_twice(tmp_path):
    with retained(tmp_path, transcript(["journal_init"] * 129)) as (_, factory):
        source_work = complete_capture(factory)
        work = epochs._begin_scan(factory, source_work)
        advance = epochs._compute_scan(factory, work)
        assert epochs._commit_scan(factory, work, advance)
        before = progress(factory), facts(factory)
        assert not epochs._commit_scan(factory, work, advance)
        assert (progress(factory), facts(factory)) == before
        resumed = epochs._begin_scan(factory, source_work)
        assert resumed.next_frame == 64


def test_partial_output_failure_rolls_back_the_cursor_and_all_frame_facts(tmp_path):
    with retained(tmp_path, transcript(["journal_init", "correction", "assertion"])) as (
        engine,
        factory,
    ):
        source_work = complete_capture(factory)
        work = epochs._begin_scan(factory, source_work)
        advance = epochs._compute_scan(factory, work)
        before = progress(factory), facts(factory)
        writes = []

        def fail(_connection, _cursor, statement, _parameters, _context, _many):
            if statement.startswith("UPDATE collection_upload_provenance_frames SET"):
                writes.append(True)
                if len(writes) == 2:
                    raise RuntimeError("second frame write interrupted")

        event.listen(engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="second frame write"):
                epochs._commit_scan(factory, work, advance)
        finally:
            event.remove(engine, "before_cursor_execute", fail)
        assert len(writes) == 2 and (progress(factory), facts(factory)) == before
        assert engine.pool.checkedout() == 0


@pytest.mark.parametrize("fault", ["page", "digest", "frame", "offset", "algorithm", "last_fact"])
def test_resume_fails_closed_on_missing_or_mismatched_retained_work(tmp_path, fault):
    raw = transcript(["journal_init"] * 129)
    with retained(tmp_path, raw) as (_, factory):
        source_work = complete_capture(factory)
        assert not epochs.advance_upload_journal_epoch_scan(factory, source_work)
        with factory() as session, session.begin():
            if fault == "page":
                session.execute(delete(Segment))
            elif fault == "digest":
                session.execute(update(Segment).values(sha256="0" * 64))
            elif fault == "frame":
                session.execute(delete(Frame).where(Frame.ordinal == 64))
            elif fault == "offset":
                session.execute(update(Scan).values(byte_offset=1))
            elif fault == "algorithm":
                session.execute(update(Scan).values(algorithm="untrusted-algorithm"))
            else:
                session.execute(
                    update(Frame).where(Frame.ordinal == 63).values(epoch_attempt_id="replacement")
                )
        before = progress(factory), facts(factory)
        with pytest.raises(RuntimeError):
            epochs.advance_upload_journal_epoch_scan(factory, source_work)
        assert (progress(factory), facts(factory)) == before
        assert terminal(factory)["anchor"] is None


@pytest.mark.parametrize("commit", [False, True])
def test_process_death_preserves_only_atomically_committed_epoch_progress(tmp_path, commit):
    with retained(tmp_path, transcript(["journal_init"] * 129)) as (engine, factory):
        source_work = complete_capture(factory)
        epochs._begin_scan(factory, source_work)
        before = progress(factory), facts(factory)
        code = """import os,sys
from sqlalchemy import create_engine,event
from sqlalchemy.orm import sessionmaker
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_journal_epochs as epochs
engine=create_engine(sys.argv[1])
factory=sessionmaker(engine)
source=owner._read_upload_journal_validation(factory,1,sys.argv[2])
work=epochs._begin_scan(factory,source)
advance=epochs._compute_scan(factory,work)
if sys.argv[3]=='before':
    event.listen(engine,'commit',lambda _connection: os._exit(73))
assert epochs._commit_scan(factory,work,advance)
os._exit(73)
"""
        completed = subprocess.run(
            [sys.executable, "-c", code, str(engine.url), JOURNAL, "after" if commit else "before"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert completed.returncode == 73, completed.stderr
        engine.dispose()
        if commit:
            assert progress(factory)[1] == 64 and len(facts(factory)) == 64
        else:
            assert (progress(factory), facts(factory)) == before
        resumed = epochs._begin_scan(factory, source_work)
        assert resumed.next_frame == (64 if commit else 0)
        finish(factory, source_work)
        assert len(facts(factory)) == 129 and terminal(factory)["anchor"] is None


class ObservedWindow:
    def __init__(self, rows, counts):
        self.rows, self.counts = rows, counts

    def __enter__(self):
        self.rows.__enter__()
        return self

    def __iter__(self):
        for row in self.rows:
            self.counts["returned_positions"] += 1
            yield row

    def __exit__(self, *args):
        try:
            return self.rows.__exit__(*args)
        finally:
            self.counts["closed"] = self.rows.closed


@pytest.mark.growth_witness(
    "riverhog-journal-epoch-frame-window-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_epochs._read_epoch_frames",
)
def test_actual_fenced_epoch_frame_window_work(tmp_path, monkeypatch, record_property):
    binding = growth_binding(epochs._read_epoch_frames)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for count, requested in [
        (0, 1),
        (1, 1),
        (17, 1),
        (129, 1),
        (1025, 1),
        (17, 16),
        (129, 16),
        (1025, 64),
    ]:
        raw = transcript(["assertion"] * count) if count else b"x"
        with retained(tmp_path / f"{count}-{requested}", raw) as (engine, factory):
            source_work = complete_capture(factory)
            work = epochs._begin_scan(factory, source_work)
            counts = {"returned_positions": 0, "closed": False}
            snapshot = epochs.read_snapshot

            @contextmanager
            def observed_snapshot(factory, base=snapshot, observed=counts):
                with base(factory) as session:
                    execute = session.execute

                    def observed_execute(*args, **kwargs):
                        return ObservedWindow(execute(*args, **kwargs), observed)

                    monkeypatch.setattr(session, "execute", observed_execute)
                    yield session

            with engine.connect() as connection:
                native = connection.connection.driver_connection
                assert isinstance(native, sqlite3.Connection)
            statements, instructions, payload_reads = [], [0], []

            def sql(
                _connection, _cursor, statement, parameters, _context, _many, output=statements
            ):
                output.append((statement, parameters))

            def tick(counter=instructions):
                counter[0] += 1
                return 0

            def authorize(action, table, column, _database, _trigger, reads=payload_reads):
                if action == sqlite3.SQLITE_READ and column == "content":
                    reads.append((table, column))
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            event.listen(engine, "before_cursor_execute", sql)
            native.set_authorizer(authorize)
            native.set_progress_handler(tick, 1)
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(epochs, "read_snapshot", observed_snapshot)
                    actual = epochs._read_epoch_frames(factory, work, frame_budget=requested)
            finally:
                native.set_progress_handler(None, 0)
                native.set_authorizer(None)
                event.remove(engine, "before_cursor_execute", sql)
            assert len(actual) == min(count, requested) == counts["returned_positions"]
            assert counts["closed"] and not payload_reads and engine.pool.checkedout() == 0
            for statement, parameters in statements:
                with engine.connect() as connection:
                    result = connection.exec_driver_sql(
                        "EXPLAIN QUERY PLAN " + statement, parameters
                    )
                    try:
                        plan = " ".join(row[3] for row in result)
                    finally:
                        result.close()
                assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {"retained_frames": count, "requested_frames": requested},
                "metrics": {
                    "sql_statements": len(statements),
                    "returned_positions": counts["returned_positions"],
                    "source_payload_octets": 0,
                    "sqlite_vm_instructions": instructions[0],
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
    record_property("growth_samples", samples)
