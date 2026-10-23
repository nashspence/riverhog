from __future__ import annotations

import hashlib
import sqlite3
from copy import deepcopy

import pytest
from riverhog_provenance import (
    JournalFrame,
    JournalSummary,
    JournalValidationCapacity,
    ProvenanceValidationError,
    append_checkpoint,
    append_correction,
    assertion_reference,
    iter_journal_frames,
    new_id,
    validate_journal,
)
from riverhog_provenance._entry_replay import EntryReplay, EntryReplayCapacity, ReplayBudget
from riverhog_provenance._entry_replay import install as install_replay
from riverhog_provenance._journal_prefix import JournalPrefix
from riverhog_provenance._online_dag import OnlineDag
from riverhog_provenance._session_checkpoint import ValidationLedger
from riverhog_provenance._session_checkpoint import install as install_parent
from riverhog_provenance.journal_store import JournalStore

ATTEMPT = "a" * 32


def make(store, catalog, **options):
    prefix = JournalPrefix(
        store.db,
        attempt_id=ATTEMPT,
        source_binding="exact-test-source",
        catalog=catalog,
        require_profiles=True,
        **options,
    )
    parent = ValidationLedger(store.db, attempt_id=ATTEMPT, binding=prefix.binding, worker_epoch=2)
    return EntryReplay(store, prefix=prefix, ledger=parent, catalog=catalog)


def initialize(store, catalog, **options):
    install_parent(store.db)
    install_replay(store.db)
    result = make(store, catalog, **options)
    result.prefix.initialize()
    result.ledger.initialize()
    return result


def owned_summary(store, state):
    store.frame_count = state.frame_count
    store.freeze()
    summary = JournalSummary(
        state.journal_id,
        store.frames,
        store.graph_validation,
        store.retired,
        state.hexdigest,
        state.byte_count,
        store.origins,
    )
    object.__setattr__(summary, "_store", store)
    return summary


def replay(raw, catalog, *, budget=1, reopen=False, **options):
    store = JournalStore()
    try:
        current = initialize(store, catalog, **options)
        phases = set()
        steps = 0
        for frame in iter_journal_frames((raw,)):
            current.admit(frame, ReplayBudget(rows=budget))
            while current.ledger.load().checkpoint.phase != "frame":
                if reopen:
                    path = store._storage.directory / "snapshot.sqlite3"
                    store.db.close()
                    store._storage.connection = sqlite3.connect(path, check_same_thread=False)
                    store.db.execute("PRAGMA synchronous=FULL").close()
                    current = make(store, catalog, **options)
                phases.add(current.ledger.load().checkpoint.phase)
                current.advance(ReplayBudget(rows=budget))
                steps += 1
                assert steps < 100000
        state = current.prefix.finish()
        assert current.ledger.load().checkpoint.frame_ordinal == state.frame_count
        assert store.db.execute("SELECT 1 FROM journal_validation_frame LIMIT 1").fetchone() is None
        return owned_summary(store, state), phases, steps
    except BaseException:
        store.close()
        raise


@pytest.mark.parametrize("budget", [1, 16, 64])
def test_checkpointed_entry_replay_preserves_native_summary(journal, catalog, budget):
    with validate_journal(journal, catalog=catalog) as expected:
        actual, phases, steps = replay(journal, catalog, budget=budget)
        with actual:
            assert actual.materialize() == expected.materialize()
            assert actual.journal_sha256 == hashlib.sha256(journal).hexdigest()
            assert actual.journal_bytes == len(journal)
        assert {
            "entry-admission",
            "staging",
            "generic",
            "row",
            "lifecycle",
            "finish",
            "recording",
            "publish",
        } <= phases
        assert steps > 20


def test_connection_replacement_reconstructs_only_inflight_atoms(journal, catalog):
    actual, phases, _ = replay(journal, catalog, reopen=True)
    with actual, validate_journal(journal, catalog=catalog) as expected:
        assert actual.materialize() == expected.materialize()
    assert "lifecycle" in phases and "publish" in phases


@pytest.mark.parametrize("reopen", [False, True])
def test_real_correction_retires_and_reseeds_the_native_view(journal, catalog, who, reopen):
    with validate_journal(journal, catalog=catalog) as initial:
        original = initial.graph["artifacts"][0]
        replacement = deepcopy(original)
        replacement["assertion_id"] = new_id()
        target = assertion_reference(initial, original["assertion_id"])
    raw = append_correction(
        journal,
        [target],
        reason="Retain the artifact while replacing its documentary assertion.",
        recorded_by_agent_id=who,
        assertions={"artifacts": [replacement]},
        catalog=catalog,
    )
    actual, phases, _ = replay(raw, catalog, reopen=reopen)
    with actual, validate_journal(raw, catalog=catalog) as expected:
        assert actual.materialize() == expected.materialize()
        assert original["assertion_id"] in actual.retracted_assertion_ids
    assert {
        "targets",
        "retire",
        "derivation-replace",
        "derivation-retire",
        "specialization-replace",
        "specialization-retire",
        "seed",
    } <= phases


@pytest.mark.parametrize("budget", [512, 4096, 16384])
@pytest.mark.parametrize(
    "phase,table,keys",
    [
        (0, "dag_levels", ("vertex",)),
        (1, "dag_edges", ("source", "target")),
        (2, "dag_incoming", ("target", "source")),
        (3, "dag_seen", ("operation_id", "vertex")),
        (4, "dag_queue", ("operation_id", "direction", "vertex")),
        (5, "dag_undo", ("operation_id", "relation", "first_key", "second_key")),
    ],
)
def test_graph_retirement_admits_actual_keys_and_write_plan_before_transaction(
    budget, phase, table, keys
):
    store = JournalStore()
    try:
        graph = OnlineDag(store.db, "derivation", store._dag_runs["derivation"])
        old = "b" * 32
        rows = []
        for index in range(97):
            identity = f"urn:uuid:{index:08x}-0000-4000-8000-000000000000"
            values = {
                "vertex": identity,
                "source": identity,
                "target": identity,
                "operation_id": f"{index:032x}",
                "direction": "back",
                "relation": "edge",
                "first_key": identity,
                "second_key": identity,
            }
            rows.append(("derivation", old, *(values[key] for key in keys)))
        extra = ", level" if table == "dag_levels" else ", old_value" if table == "dag_undo" else ""
        if extra:
            rows = [
                (*row, "7" if table == "dag_levels" else "unread payload" * 1024) for row in rows
            ]
        columns = "kind, run_id, " + ", ".join(keys) + extra
        placeholders = ",".join("?" for _ in rows[0])
        store.db.executemany(f"INSERT INTO {table} ({columns}) VALUES({placeholders})", rows)
        store.db.execute("INSERT INTO dag_retired VALUES(?,?,?)", ("derivation", old, phase))
        store.db.commit()
        statements = []
        store.db.set_trace_callback(statements.append)
        changed = store.db.total_changes
        with pytest.raises(JournalValidationCapacity):
            graph._prepare_retirement(64, byte_budget=1)
        assert store.db.total_changes == changed and not store.db.in_transaction
        assert not any(sql.startswith("SELECT " + ", ".join(keys) + " FROM") for sql in statements)
        store.db.set_trace_callback(None)

        def authorize(action, selected, column, *_rest):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ
                and selected == table
                and column in {"level", "old_value"}
                else sqlite3.SQLITE_OK
            )

        store.db.set_authorizer(authorize)
        batch = graph._prepare_retirement(64, byte_budget=budget)
        store.db.set_authorizer(None)
        assert batch is not None and 0 < batch.octets <= budget
        assert store.db.total_changes == changed and not store.db.in_transaction
        store.db.execute("BEGIN IMMEDIATE")
        graph._apply_retirement(batch)
        store.db.commit()
        remaining = store.db.execute(
            f"SELECT count(*) FROM {table} WHERE kind=? AND run_id=?", ("derivation", old)
        ).fetchone()[0]
        deleted = 97 - remaining
        assert 0 < deleted <= 64
        assert store.db.total_changes - changed == deleted + 1
        assert graph._load().version == batch.after.version
    finally:
        store.close()


def test_parent_charges_each_prepared_graph_retirement_before_publishing(
    journal, catalog, who, monkeypatch
):
    with validate_journal(journal, catalog=catalog) as initial:
        replacement = deepcopy(initial.graph["artifacts"][0])
        target = assertion_reference(initial, replacement["assertion_id"])
    replacement["assertion_id"] = new_id()
    raw = append_correction(
        journal,
        [target],
        reason="Replace the documentary assertion.",
        recorded_by_agent_id=who,
        assertions={"artifacts": [replacement]},
        catalog=catalog,
    )
    prepared, charged = [], []
    original_prepare, original_claim = OnlineDag._prepare_retirement, EntryReplay._claim

    def prepare(self, budget=64, *, byte_budget=None):
        assert byte_budget is not None
        batch = original_prepare(self, budget, byte_budget=byte_budget)
        if batch is not None:
            assert batch.octets <= byte_budget
            prepared.append(batch.octets)
        return batch

    def claim(self, octets):
        if self.ledger.load().checkpoint.phase.endswith("-retire"):
            charged.append(octets)
        return original_claim(self, octets)

    monkeypatch.setattr(OnlineDag, "_prepare_retirement", prepare)
    monkeypatch.setattr(EntryReplay, "_claim", claim)
    actual, _, _ = replay(raw, catalog, budget=64)
    with actual:
        assert actual.journal_sha256 == hashlib.sha256(raw).hexdigest()
    assert prepared and prepared == charged


def test_accepted_frames_are_never_read_to_continue_the_next_entry(journal, catalog, who):
    raw = append_checkpoint(
        journal, recorded_by_agent_id=who, purpose="urn:test:replay", catalog=catalog
    )
    store = JournalStore()
    try:
        current = initialize(store, catalog)

        def authorize(action, table, column, *_rest):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and table == "frames" and column == "value"
                else sqlite3.SQLITE_OK
            )

        store.db.set_authorizer(authorize)
        try:
            for frame in iter_journal_frames((raw,)):
                current.admit(frame)
                while current.ledger.load().checkpoint.phase != "frame":
                    current.advance(ReplayBudget(rows=1))
            assert current.prefix.finish().byte_count == len(raw)
        finally:
            store.db.set_authorizer(None)
    finally:
        store.close()


def test_current_frame_is_hashed_a_fixed_number_of_times(journal, catalog, monkeypatch):
    original = JournalFrame.sha256.fget
    counts = []

    def digest(frame):
        counts[-1] += 1
        return original(frame)

    monkeypatch.setattr(JournalFrame, "sha256", property(digest))
    for budget in (1, 64):
        counts.append(0)
        actual, _, _ = replay(journal, catalog, budget=budget)
        actual.close()
    assert counts[0] == counts[1]
    assert counts[0] < 20


def test_capacity_refuses_before_the_owned_frame_is_published(journal, catalog):
    store = JournalStore()
    try:
        current = initialize(store, catalog)
        before = current.ledger.load()
        frame = next(iter_journal_frames((journal,)))
        with pytest.raises(EntryReplayCapacity):
            current.admit(frame, ReplayBudget(rows=1, octets=1))
        assert current.ledger.load() == before and not store.db.in_transaction
        assert store.db.execute("SELECT 1 FROM journal_validation_frame LIMIT 1").fetchone() is None
    finally:
        store.close()


def test_semantic_failure_precedes_a_later_framing_failure(journal, catalog, monkeypatch):
    def reject(_self, _document):
        raise ProvenanceValidationError("earlier recorder failure")

    monkeypatch.setattr(JournalStore, "_check_entry_recording", reject)
    for run in (
        lambda raw: validate_journal(raw, catalog=catalog),
        lambda raw: replay(raw, catalog),
    ):
        with pytest.raises(ProvenanceValidationError, match="earlier recorder failure"):
            run(journal + b"later invalid RFC7464 octets")
