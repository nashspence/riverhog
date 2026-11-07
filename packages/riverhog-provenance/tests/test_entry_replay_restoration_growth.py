from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing, contextmanager

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import validate_journal
from riverhog_provenance._entry_replay import EntryReplay, EntryReplayCapacity, ReplayBudget
from riverhog_provenance.journal_store import JournalStore
from test_child_admission_growth import octets
from test_entry_replay import owned_summary
from test_entry_replay_growth import REGIMES, ReplayConnection, finish_entry, observe_replay, owner
from test_entry_replay_growth import admission_histories as admission_histories
from test_entry_replay_publication_growth import restore
from test_semantic_edge import WorkCursor, observed_work
from test_semantic_owner import snapshot

_METADATA = "SELECT ordinal,sha256,length(value) FROM journal_validation_frame WHERE singleton=1"
_PAYLOAD = "SELECT value FROM journal_validation_frame WHERE singleton=1"


class CurrentCursor(WorkCursor):
    def record(self, row):
        result = super().record(row)
        if row is not None:
            if self.sql == _METADATA:
                self.connection.observed_metadata = row
            elif self.sql == _PAYLOAD:
                assert type(row[0]) is bytes
                size = len(row[0])
                self.connection.metrics["returned_metadata_octets"] -= size
                self.connection.metrics["source_payload_octets"] += size
        return result


class RestorationConnection(ReplayConnection):
    admitted_octets = None
    observed_metadata = None
    active_sql = None

    def execute(self, sql, parameters=()):
        self.active_sql = sql
        if self.recording and sql in {_METADATA, _PAYLOAD}:
            if sql == _PAYLOAD:
                assert self.observed_metadata is not None
                assert self.admitted_octets == self.observed_metadata[2]
            cursor = self.cursor(factory=CurrentCursor)
            cursor.sql = sql
            self.statements.append((sql, parameters, cursor))
            return cursor.execute(sql, parameters)
        return super().execute(sql, parameters)

    def set_authorizer(self, callback):
        if callback is None:
            return sqlite3.Connection.set_authorizer(self, None)

        def authorize(action, table, column, database, trigger):
            if (
                action == sqlite3.SQLITE_READ
                and table == "journal_validation_frame"
                and column == "value"
            ):
                if self.active_sql not in {_METADATA, _PAYLOAD}:
                    return sqlite3.SQLITE_DENY
                if self.active_sql == _PAYLOAD and self.admitted_octets is None:
                    return sqlite3.SQLITE_DENY
            return callback(action, table, column, database, trigger)

        return sqlite3.Connection.set_authorizer(self, authorize)


@contextmanager
def restoration_fixture(build, regime, catalog, *, cold):
    case = dict(build(regime))
    with closing(JournalStore()) as store:
        current = restore(store, case, catalog)
        current.admit(case["candidate"])
        before = current.ledger.load()
        cached = current._current
        original = snapshot(store.db)
        width = 8192 + 2 * octets(
            (before, cached.prepared.before, cached.source, current.prefix.binding)
        )
        state = case["state"]
        case.update(
            before=before,
            cached=cached,
            original=original,
            dimensions={
                "retained_frames": state.frame_count,
                "frame_octets": len(case["candidate"].encoded),
                "initial_tail_octets": state.byte_count % 64,
                "source_blocks": (state.byte_count % 64 + len(case["candidate"].encoded)) // 64,
                "cold_cache": int(cold),
                "quarantine_octets": len(case["candidate"].json_bytes),
                "metadata_row_octets": width,
            },
        )
        path = store._storage.directory / "snapshot.sqlite3"
        store.db.close()
        store._storage.connection = sqlite3.connect(path, factory=RestorationConnection)
        store.db.execute("PRAGMA synchronous=FULL").close()
        store.db.execute("PRAGMA cache_size=-64").close()
        current = owner(store, catalog, case["source_binding"], case["epoch"])
        current._current = None if cold else cached
        current._start(before, ReplayBudget())
        real_claim = current._claim

        def claim(size):
            real_claim(size)
            if store.db.recording:
                assert store.db.observed_metadata is not None
                assert size == store.db.observed_metadata[2]
                assert store.db.admitted_octets is None
                store.db.admitted_octets = size

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(current, "_claim", claim)
            yield current, case


def restoration_samples(catalog, build, *, samples, regimes=REGIMES):
    binding = growth_binding(EntryReplay._entry)
    projection = project_bindings((binding,))
    for regime in regimes:
        for cold in (False, True):
            with restoration_fixture(build, regime, catalog, cold=cold) as (current, case):
                db = current.db
                with observe_replay(
                    db, catalog, case["dimensions"], root_operation=EntryReplay._entry
                ) as (observed, complete):
                    with db.observe():
                        result = current._entry(case["before"].checkpoint)
                primitive = observed_work(db, {key: 0 for key in db.metrics}, 0, 0)
                primitive["changed_rows"] = db.metrics["changed_rows"]
                sample = complete(observed, primitive)
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
                assert result is current._current
                assert (result.frame, result.prepared, result.document, result.source) == (
                    case["cached"].frame,
                    case["cached"].prepared,
                    case["cached"].document,
                    case["cached"].source,
                )
                assert (result is case["cached"]) is (not cold)
                assert snapshot(db) == case["original"] and not db.in_transaction
                finish_entry(current)
                with validate_journal(case["raw"], catalog=catalog) as expected:
                    with owned_summary(current.store, current.prefix.finish()) as actual:
                        assert actual.materialize() == expected.materialize()
                        assert actual.journal_sha256 == hashlib.sha256(case["raw"]).hexdigest()
                        assert actual.journal_bytes == len(case["raw"])
    assert len(samples) == 2 * len(regimes)


@pytest.mark.growth_witness(
    "riverhog-entry-replay-restoration-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_replay.EntryReplay._entry",
)
def test_actual_cold_and_warm_current_frame_restoration(
    catalog, admission_histories, record_property
):
    samples = []
    record_property("growth_samples", samples)
    restoration_samples(catalog, admission_histories, samples=samples)


def test_capacity_rejects_before_current_payload_is_read(catalog, admission_histories):
    with restoration_fixture(admission_histories, REGIMES[1], catalog, cold=True) as (
        current,
        case,
    ):
        before = case["before"]
        budget = ReplayBudget(octets=current._spent + len(case["candidate"].json_bytes) - 1)
        current._start(before, budget)
        with current.db.observe():
            with pytest.raises(EntryReplayCapacity):
                current._entry(before.checkpoint)
        assert current.db.metrics["source_payload_octets"] == 0
        assert current.db.admitted_octets is None and current._current is None
        assert snapshot(current.db) == case["original"]


def test_changed_current_payload_is_rejected_without_rewriting_state(catalog, admission_histories):
    with restoration_fixture(admission_histories, REGIMES[0], catalog, cold=True) as (
        current,
        case,
    ):
        raw = case["candidate"].json_bytes
        changed = bytes([raw[0] ^ 1]) + raw[1:]
        current.db.execute(
            "UPDATE journal_validation_frame SET value=? WHERE singleton=1", (changed,)
        ).close()
        current.db.commit()
        before = snapshot(current.db)
        with current.db.observe():
            with pytest.raises(RuntimeError, match="missing or changed"):
                current._entry(case["before"].checkpoint)
        assert current.db.metrics["source_payload_octets"] == len(raw)
        assert current._current is None and not current.db.in_transaction
        assert snapshot(current.db) == before
