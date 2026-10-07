from __future__ import annotations

import threading
import time

from sqlalchemy import event
from stove0_core.metadata_steps import MetadataSteps, WorkScan
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_core.scheduler import Stove0Scheduler
from stove0_core.work_state import ClaimBinding, WorkRecord
from stove0_protocol import CollectionRootIdentityRef, RecipeIdentityRef, WorkIdentity, WorkPayload


def identity(index, *, records=()):
    return WorkIdentity.seal(
        WorkPayload(
            recipe=RecipeIdentityRef(id="metadata-fixture/v1", revision="1", sha256="a" * 64),
            inputs=(
                CollectionRootIdentityRef(
                    collection_id=str(index),
                    archive_root_sha256=f"{index:064x}",
                    artifact_set_identity="b" * 64,
                ),
            ),
            effective_intent={"records": list(records)},
        )
    )


def test_large_preparation_preserves_scheduler_fairness_and_independent_claim_maintenance(tmp_path):
    state = SqlAlchemyStateStore(f"sqlite+pysqlite:///{tmp_path / 'state.db'}")
    records = tuple("x" * 600 for _ in range(8192))
    held = state.create(
        WorkRecord(
            work=identity(1, records=records),
            phase="claimed",
            claim=ClaimBinding(claim_id="held", fence=1),
        )
    )
    healthy = state.create(
        WorkRecord(
            work=identity(2), phase="claimed", claim=ClaimBinding(claim_id="healthy", fence=1)
        )
    )
    entered, release = threading.Event(), threading.Event()
    starts, renewals, scanned = [], [], []
    main_thread = threading.get_ident()

    @event.listens_for(state.engine, "before_cursor_execute")
    def statements(_connection, _cursor, statement, _parameters, _context, _many):
        if threading.get_ident() == main_thread and statement.startswith("SELECT"):
            scanned.append(statement)

    class Coordinator:
        def step(self, work_id):
            record = state.load(work_id)
            starts.append(work_id)
            if work_id == held.work_id:
                assert len(record.work.effective_intent["records"]) == len(records)
                entered.set()
                assert release.wait(10)
            return state.compare_and_swap(
                work_id,
                expected_revision=record.revision,
                replacement=record.model_copy(update={"revision": record.revision + 1}),
            )

        def maintain(self, work_id):
            renewals.append(work_id)
            return state.load(work_id)

    steps = MetadataSteps(wait_seconds=0.01)
    scheduler = Stove0Scheduler(coordinator=Coordinator(), state=state, metadata_steps=steps)
    try:
        started = time.monotonic()
        scheduler.advance(role="controller", limit=10)
        assert time.monotonic() - started < 0.5
        assert entered.wait(3)
        deadline = time.monotonic() + 5
        while state.load_work_scan(healthy.work_id).revision == 1:
            assert time.monotonic() < deadline
            scheduler.advance(role="controller", limit=10)
            time.sleep(0.01)
        while held.work_id not in renewals:
            assert time.monotonic() < deadline
            scheduler.advance(role="controller", limit=10)
        assert starts.count(held.work_id) == 1
        assert scanned and all("document_json" not in statement for statement in scanned)
        release.set()
        deadline = time.monotonic() + 5
        while state.load_work_scan(held.work_id).revision == 1:
            assert time.monotonic() < deadline
            scheduler.advance(role="controller", limit=10)
            time.sleep(0.01)
        assert starts.count(held.work_id) == 1
        assert state.load(held.work_id).work == held.work
    finally:
        release.set()
        steps.close()
        state.engine.dispose()


def test_scheduler_scan_is_a_bounded_projection_without_parsing_whole_work(tmp_path, monkeypatch):
    import stove0_core.persistence as persistence

    state = SqlAlchemyStateStore(f"sqlite+pysqlite:///{tmp_path / 'state.db'}")
    record = state.create(WorkRecord(work=identity(1)))

    def decoding_is_for_the_preparation_worker(_raw):
        raise AssertionError("scheduler decoded a whole work document")

    monkeypatch.setattr(persistence, "_decode_work_record", decoding_is_for_the_preparation_worker)
    try:
        records, cursor = state.scan_work(phases=("eligible",), after_work_id="", limit=1)
        assert records == [WorkScan(record.work_id, record.phase, record.revision)]
        assert cursor == record.work_id
        assert state.load_work_scan(record.work_id) == records[0]
    finally:
        state.engine.dispose()
