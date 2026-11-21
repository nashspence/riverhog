from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing

import pytest
from riverhog_provenance._journal_prefix import JournalPrefix, install
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_frame import NativeFrame
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum
from riverhog_provenance.journal import parse_journal
from sha256_continuation import CheckpointSHA256
from sqlite_admission import SqliteProgressConnection


def owner(db, journal, catalog):
    install(db)
    epoch = EpochContext(db)
    prefix = JournalPrefix(
        db,
        attempt_id="owned-attempt",
        source_binding="owned-source",
        catalog=catalog,
        require_profiles=True,
        epoch=epoch,
    )
    prefix.initialize()
    frame = NativeFrame(parse_journal(journal)[0].json_bytes, epoch)
    assert frame.document and frame.sha256
    return epoch, prefix, frame


def quantum(octets):
    return WorkQuantum(WorkAmount(rows=4, octets=octets, source_octets=1, transitions=1))


def observe_source_hash(monkeypatch):
    original, seen = CheckpointSHA256.update_bytes, []

    def update(current, raw):
        # Continuation construction also submits empty initialization inputs.
        if raw:
            seen.append(raw)
        return original(current, raw)

    monkeypatch.setattr(CheckpointSHA256, "update_bytes", update)
    return seen


def test_prefix_accounts_framed_allocation_and_source_hash_as_separate_actual_passes(
    journal, catalog, monkeypatch
):
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, prefix, frame = owner(db, journal, catalog)
        expected = frame.encoded
        seen = observe_source_hash(monkeypatch)
        before, changes = prefix.load(), db.total_changes
        work = quantum(2 * len(expected))
        with epoch.execution(None, work=work):
            prepared = prefix.prepare(frame, catalog)
        assert seen == [expected] and prepared.frame is frame
        digest = CheckpointSHA256.from_state(prepared.sha256_state)
        assert digest.total_bytes == len(expected)
        assert digest.hexdigest() == hashlib.sha256(expected).hexdigest()
        assert work.consumed == WorkAmount(rows=1, octets=2 * len(expected))
        assert work.held == WorkAmount() and epoch.quiescent
        assert prefix.load() == before and db.total_changes == changes
        work.require_received()


@pytest.mark.parametrize("admitted_passes", [0, 1])
def test_prefix_capacity_refuses_before_the_unadmitted_pass_and_retries_the_original(
    journal, catalog, monkeypatch, admitted_passes
):
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, prefix, frame = owner(db, journal, catalog)
        expected = frame.encoded
        seen = observe_source_hash(monkeypatch)
        before, changes = prefix.load(), db.total_changes
        work = quantum((admitted_passes + 1) * len(expected) - 1)
        statements = []
        db.set_trace_callback(statements.append)
        try:
            with pytest.raises(NativeWorkCapacity):
                with epoch.execution(None, work=work):
                    prefix.prepare(frame, catalog)
        finally:
            db.set_trace_callback(None)
        assert seen == [] and not any(sql.startswith("BEGIN") for sql in statements)
        assert work.consumed == WorkAmount(rows=1, octets=admitted_passes * len(expected))
        assert work.held == WorkAmount() and epoch.quiescent
        assert prefix.load() == before and db.total_changes == changes
        retry = quantum(2 * len(expected))
        with epoch.execution(None, work=retry):
            prepared = prefix.prepare(frame, catalog)
        assert seen == [expected] and prepared.frame is frame
        assert retry.consumed == WorkAmount(rows=1, octets=2 * len(expected))
        work.require_received()
        retry.require_received()


def test_repeated_prefix_preparation_does_not_replenish_the_same_original_quantum(
    journal, catalog, monkeypatch
):
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, prefix, frame = owner(db, journal, catalog)
        expected = frame.encoded
        seen = observe_source_hash(monkeypatch)
        before = prefix.load()
        work = quantum(3 * len(expected))
        with epoch.execution(None, work=work):
            first = prefix.prepare(frame, catalog)
            with pytest.raises(NativeWorkCapacity):
                prefix.prepare(frame, catalog)
        assert first.frame is frame and seen == [expected]
        assert work.consumed == WorkAmount(rows=2, octets=3 * len(expected))
        assert work.held == WorkAmount() and epoch.quiescent and prefix.load() == before
        work.require_received()


def test_failed_original_source_hash_keeps_attempted_input_cost_without_publishing(
    journal, catalog, monkeypatch
):
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, prefix, frame = owner(db, journal, catalog)
        expected = frame.encoded
        before, changes = prefix.load(), db.total_changes
        failure, seen = OSError("original source hash failed"), []
        original = CheckpointSHA256.update_bytes

        def fail(current, raw):
            if raw:
                seen.append(raw)
                raise failure
            return original(current, raw)

        monkeypatch.setattr(CheckpointSHA256, "update_bytes", fail)
        work = quantum(2 * len(expected))
        with pytest.raises(OSError) as caught:
            with epoch.execution(None, work=work):
                prefix.prepare(frame, catalog)
        assert caught.value is failure and seen == [expected]
        assert work.consumed == WorkAmount(rows=1, octets=2 * len(expected))
        assert work.held == WorkAmount() and epoch.quiescent
        assert prefix.load() == before and db.total_changes == changes
        work.require_received()
