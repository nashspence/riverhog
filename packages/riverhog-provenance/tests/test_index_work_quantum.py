from __future__ import annotations

import sqlite3
from contextlib import closing
from copy import deepcopy

import pytest
from riverhog_provenance._changed_assertions import read_changed_assertion
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum
from riverhog_provenance._observation_coverage import ObservationCoverage
from riverhog_provenance._semantic_program import Nodes
from riverhog_provenance.constants import PRIMARY_CONTENT_CATEGORY, PROVENANCE_PROFILE
from riverhog_provenance.journal_session import JournalValidationWorkBudget
from sqlite_admission import SqliteProgressConnection
from test_native_atom import session as session


def quantum(rows, *, octets=1):
    return WorkQuantum(WorkAmount(rows=rows, octets=octets, source_octets=1, transitions=1))


def tree():
    return {"first": [{}, {}], "second": {"x": "scalar", "y": "opaque" * 8192}}


def test_actual_container_and_value_slot_visits_have_no_scalar_byte_copy_or_source_credit():
    document = tree()
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, work = EpochContext(db), quantum(11)
        with epoch.execution(None, work=work):
            index = Nodes(document, epoch=epoch)
        # Five containers and six value slots; the opaque scalar is retained.
        assert len(index.values) == 5
        assert index.values[0] is document and index.values[-1] is document["second"]
        assert work.consumed == WorkAmount(rows=11) and work.held == WorkAmount()
        work.require_received()


def test_index_refusal_preserves_visited_prefix_and_same_original_retry():
    document, before = tree(), deepcopy(tree())
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, small = EpochContext(db), quantum(5)
        with pytest.raises(NativeWorkCapacity):
            with epoch.execution(None, work=small):
                Nodes(document, epoch=epoch)
        # Root plus its two value slots were admitted; its first list was not.
        assert small.consumed == WorkAmount(rows=3) and small.held == WorkAmount()
        assert document == before and epoch.quiescent
        retry = quantum(11)
        with epoch.execution(None, work=retry):
            assert len(Nodes(document, epoch=epoch).values) == 5
        assert retry.consumed == WorkAmount(rows=11) and retry.held == WorkAmount()
        small.require_received()
        retry.require_received()


def test_known_container_revisit_charges_its_point_without_repeating_child_slots():
    shared = {"flag": True}
    document = {"first": shared, "second": shared}
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, work = EpochContext(db), quantum(6)
        with epoch.execution(None, work=work):
            index = Nodes(document, epoch=epoch)
        assert index.values == [document, shared]
        # Root with two slots, first child with one slot, then one known point.
        assert work.consumed == WorkAmount(rows=6) and work.held == WorkAmount()
        work.require_received()


def test_two_index_constructions_do_not_replenish_the_original_quantum():
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, work = EpochContext(db), quantum(21)
        with epoch.execution(None, work=work):
            assert len(Nodes(tree(), epoch=epoch).values) == 5
            with pytest.raises(NativeWorkCapacity):
                Nodes(tree(), epoch=epoch)
        assert work.consumed == WorkAmount(rows=19) and work.held == WorkAmount()
        assert epoch.quiescent
        work.require_received()


def test_coverage_counts_duplicate_source_rows_separately_from_distinct_index_keys():
    first = {
        "profile_id": PROVENANCE_PROFILE,
        "category": PRIMARY_CONTENT_CATEGORY,
        "status": "complete",
    }
    duplicate = dict(first, status="partial")
    last = {"profile_id": "urn:test:other", "category": "urn:test:other", "status": "failed"}
    document = {"coverage": [first, duplicate, last]}
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, small = EpochContext(db), quantum(2)
        with pytest.raises(NativeWorkCapacity):
            with epoch.execution(None, work=small):
                ObservationCoverage(document, epoch=epoch)
        assert small.consumed == WorkAmount(rows=2) and small.held == WorkAmount()
        retry = quantum(3)
        with epoch.execution(None, work=retry):
            index = ObservationCoverage(document, epoch=epoch)
        assert len(index.statuses) == 2 and index.status(first) == "complete"
        assert index.primary_complete and index.incomplete
        assert retry.consumed == WorkAmount(rows=3) and retry.held == WorkAmount()
        small.require_received()
        retry.require_received()


def test_python_index_loop_carries_the_original_memory_check_and_cause():
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, work, calls = EpochContext(db), quantum(11), []
        cause = InterruptedError("original Native index stopped")

        def check():
            calls.append(None)
            if len(calls) == 3:
                raise cause

        with pytest.raises(InterruptedError) as caught:
            with epoch.execution(check, work=work):
                Nodes(tree(), epoch=epoch)
        assert caught.value is cause and len(calls) == 3
        assert work.consumed == WorkAmount(rows=3) and work.held == WorkAmount()
        assert epoch.quiescent and epoch.work is None
        work.require_received()


def test_python_index_loop_rejects_a_changed_non_none_check_result():
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch, work, calls = EpochContext(db), quantum(11), []

        def check():
            calls.append(None)
            return None if len(calls) == 1 else "invalid check result"

        with pytest.raises(TypeError, match="must return None"):
            with epoch.execution(check, work=work):
                Nodes(tree(), epoch=epoch)
        assert len(calls) == 2 and work.consumed == work.held == WorkAmount()
        assert epoch.quiescent
        work.require_received()


def test_actual_native_index_row_refusal_leaves_no_partial_row_cache_or_parent_write(session):
    replay, epoch = session._replay, session._ledger.epoch
    before, current = session._ledger.load(), replay._current
    selected = read_changed_assertion(
        replay.db, after=before.checkpoint.row_after, byte_budget=1024 * 1024
    )
    from json import loads

    document = loads(selected.raw)
    assert any(isinstance(value, (dict, list)) for value in document.values())
    # Admit the original three SQL points and root container/slots, then refuse a child.
    rows = 3 + 1 + len(document)
    work = quantum(rows, octets=3 * len(selected.raw))
    changes = replay.db.total_changes
    with pytest.raises(NativeWorkCapacity):
        with epoch.execution(None, work=work):
            replay._admit_row(before, current)
    assert work.consumed == WorkAmount(rows=rows, octets=3 * len(selected.raw))
    assert work.held == WorkAmount() and epoch.quiescent
    assert current.row_key is current.row_sha256 is current.row is current.atom is None
    assert session._ledger.load() == before and replay.db.total_changes == changes
    retry = quantum(JournalValidationWorkBudget().rows, octets=3 * len(selected.raw))
    with epoch.execution(None, work=retry):
        after = replay._admit_row(before, current)
    assert after.checkpoint.phase == "generic-admission"
    assert current.atom.nodes.values[0] is current.row and current.atom._epoch is epoch
    assert retry.consumed.rows > rows and retry.consumed.octets == 3 * len(selected.raw)
    assert retry.consumed.source_octets == 0 and retry.held == WorkAmount()
    work.require_received()
    retry.require_received()
