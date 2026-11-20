from __future__ import annotations

import threading
from datetime import timedelta
from functools import partial

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_finalizer_registry as registry_module
from riverhog_core.services import physical_execution as execution_module
from riverhog_core.services.archive_finalization_pipeline import (
    ArchiveFinalizationPipeline,
    ArchiveFinalizationStagePlan,
)

from tests.unit.test_archive_finalizer_registry import (
    Finalizer,
    _key,
    _receive,
    _scope,
    _submit,
)
from tests.unit.test_archive_finalizer_registry import owned as owned
from tests.unit.test_archive_upload_maintenance import _join
from tests.unit.test_archive_write_admission import _case, _selected
from tests.unit.test_collection_finalization_plan import _metadata_case

_RETRY = timedelta(seconds=10)


def _maintain(registry, **values):
    # Resource-only fixture: production maintenance owns the catalog interlock.
    return registry.process_ready(
        **values,
        retire=lambda key, target: registry.retire_idle(key, target_incarnation=target),
    )


def _failed_original(registry, pool, key=None):
    original = Finalizer()
    original.pending = True

    def fail(owner):
        owner.calls.append("original")
        raise OSError("original continuation is unavailable")

    assert _submit(registry, key, create=lambda: original, advance=fail) == "submitted"
    return original


def _failed_receipts(registry, pool):
    for receipt in _receive(registry, pool):
        with pytest.raises(OSError):
            receipt.result()


def test_original_resumption_obeys_the_existing_maintenance_cadence(owned, monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(registry_module, "monotonic", lambda: clock[0])
    registry, pool = owned.make()
    original = _failed_original(registry, pool)
    _failed_receipts(registry, pool)
    assert _maintain(registry, limit=1, retry_after=_RETRY) == 1
    _failed_receipts(registry, pool)
    assert original.calls == ["original", "original"]
    assert _maintain(registry, limit=1, retry_after=_RETRY) == 0
    clock[0] += 10
    assert _maintain(registry, limit=1, retry_after=_RETRY) == 1
    _failed_receipts(registry, pool)
    assert registry._contexts[1].owner is original
    assert original.calls == ["original"] * 3 and original.has_pending


def test_bounded_visits_rotate_past_idle_unknown_and_locked_originals(owned):
    registry, pool = owned.make(retained_constructions=4, retained_constructions_per_target=4)
    assert _submit(registry, _key(1)) == "submitted"
    assert _receive(registry, pool)[0].result().progressed

    def failed_constructor():
        raise OSError("original constructor has no confirmed receiver")

    assert _submit(registry, _key(2), create=failed_constructor) == "submitted"
    _failed_receipts(registry, pool)
    locked = _failed_original(registry, pool, _key(3))
    _failed_receipts(registry, pool)
    ready = _failed_original(registry, pool, _key(4))
    _failed_receipts(registry, pool)
    with registry._contexts[3].lock:
        assert _maintain(registry, limit=1, retry_after=_RETRY) == 1
        assert 1 not in registry._contexts
        assert _maintain(registry, limit=1, retry_after=_RETRY) == 0
        assert _maintain(registry, limit=1, retry_after=_RETRY) == 0
        assert _maintain(registry, limit=1, retry_after=_RETRY) == 1
    _failed_receipts(registry, pool)
    assert locked.calls == ["original"] and ready.calls == ["original", "original"]
    assert registry._contexts[2].construction_failure is not None


def test_published_future_cannot_release_physical_capacity_for_an_original_retry(
    owned, monkeypatch
):
    returned, release = threading.Event(), owned.event()

    class HeldExit(threading.Thread):
        def run(self):
            super().run()
            returned.set()
            assert release.wait(10)

    monkeypatch.setattr(execution_module, "Thread", HeldExit)
    registry, pool = owned.make()
    original = _failed_original(registry, pool)
    assert returned.wait(5) and pool._slots[_key(1)].future.done()
    slot = pool._slots[_key(1)]

    def forbidden(*args, **kwargs):
        pytest.fail("resumption cannot wait for an original worker")

    with monkeypatch.context() as patch:
        patch.setattr(slot.worker, "join", forbidden)
        assert _maintain(registry, limit=1, retry_after=_RETRY) == 0
        assert registry.collect(limit=1) == ()
    assert original.calls == ["original"]
    release.set()
    _failed_receipts(registry, pool)
    assert _maintain(registry, limit=1, retry_after=_RETRY) == 1
    _failed_receipts(registry, pool)


def test_fenced_originals_cannot_be_resumed_as_forward_work(tmp_path, owned):
    with _case(tmp_path) as case:
        key, target = _scope(case)
        registry, pool = owned.make(case.factory)
        original = Finalizer()
        original.pending = True

        def failed(owner):
            owner.calls.append("original")
            raise OSError("original reply is unavailable")

        assert (
            _submit(registry, key, target=target, create=lambda: original, advance=failed)
            == "submitted"
        )
        _failed_receipts(registry, pool)
        operation = _selected(case)
        assert (
            registry.fence_terminal(key, target_incarnation=target, operation_id=operation)
            == "fenced"
        )
        assert _maintain(registry, limit=1, retry_after=_RETRY) == 0
        assert registry._contexts[key.collection_id].owner is original
        assert original.calls == ["original"] and original.has_pending


@pytest.mark.parametrize("limit,seconds", [(0, 1), (-1, 1), (True, 1), (1, 0), (1, -1)])
def test_resumption_requires_positive_visit_and_cadence_limits(owned, limit, seconds):
    registry, _ = owned.make()
    with pytest.raises(ValueError, match="positive"):
        _maintain(registry, limit=limit, retry_after=timedelta(seconds=seconds))


def test_service_maintenance_resumes_a_failed_read_with_its_original_pipeline_and_claim(
    tmp_path, monkeypatch
):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        service = case.service
        constructed, selections = [], []
        original = Finalizer()

        def selected():
            selections.append("original")
            if len(selections) == 1:
                raise OSError("read-only workflow selection failed")
            return lambda: original

        def create(chosen):
            constructed.append(chosen)
            return ArchiveFinalizationPipeline(
                case.factory,
                collection_id=case.collection_id,
                stages=(ArchiveFinalizationStagePlan(selected),),
            )

        monkeypatch.setattr(service, "_finalization_owner", create)
        assert service._submit_finalization(claim) == "submitted"
        _join(service)
        assert service._archive_finalizers._contexts[case.collection_id].owner.has_pending
        with session_scope(case.factory) as session:
            assert session.get(Upload, case.collection_id).archive_claim_id == claim.claim_id
        assert service.process_due_archive_uploads() == 2
        _join(service)
        assert service.process_due_archive_uploads() == 2
        assert constructed == [claim] and selections == ["original", "original"]
        assert original.calls == ["original"]
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert upload.archive_claim_id is None
            assert upload.archive_phase == "finalization_queued"
        assert service.process_due_archive_uploads() == 0


def test_service_maintenance_receives_original_feedback_after_a_new_claim_was_selected(
    tmp_path, monkeypatch
):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        service = case.service
        actual, received = service._requeue_finalization_step, []

        def lost(chosen):
            received.append(chosen)
            actual(chosen)
            if len(received) == 1:
                raise OSError("original workflow feedback reply lost")

        monkeypatch.setattr(service, "_requeue_finalization_step", lost)
        assert service._submit_finalization(claim) == "submitted"
        _join(service)
        replacement = service._claim_due_finalization()
        assert replacement is not None and replacement.claim_id != claim.claim_id

        def forbidden(*args, **kwargs):
            pytest.fail("resuming feedback cannot select a fresh pipeline")

        monkeypatch.setattr(service, "_finalization_owner", forbidden)
        assert service.process_due_archive_uploads() == 2
        _join(service)
        assert service.process_due_archive_uploads() == 2
        assert received == [claim, claim]
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert upload.archive_claim_id == replacement.claim_id
            assert upload.archive_phase == "finalizing"
        assert service.process_due_archive_uploads() == 0


def test_idle_receiving_returns_retention_capacity_without_selecting_new_work(owned):
    registry, pool = owned.make(retained_constructions=1, retained_constructions_per_target=1)
    assert _submit(registry, _key(1)) == "submitted"
    assert _receive(registry, pool)[0].result().progressed
    assert _submit(registry, _key(2)) == "retention-capacity"
    assert _maintain(registry, limit=1, retry_after=_RETRY) == 1
    assert not registry._contexts
    assert _submit(registry, _key(2)) == "submitted"
    assert _receive(registry, pool)[0].result().progressed


def test_maintenance_cannot_retire_a_healthy_result_before_actual_worker_exit(owned, monkeypatch):
    returned, release = threading.Event(), owned.event()

    class HeldExit(threading.Thread):
        def run(self):
            super().run()
            returned.set()
            assert release.wait(10)

    monkeypatch.setattr(execution_module, "Thread", HeldExit)
    registry, pool = owned.make(retained_constructions=1, retained_constructions_per_target=1)
    assert _submit(registry, _key(1)) == "submitted"
    assert returned.wait(5) and pool._slots[_key(1)].future.done()
    assert _maintain(registry, limit=1, retry_after=_RETRY) == 0
    assert registry.collect(limit=1) == ()
    assert _submit(registry, _key(2)) == "retention-capacity"
    release.set()
    assert _receive(registry, pool)[0].result().progressed
    assert _maintain(registry, limit=1, retry_after=_RETRY) == 1
    assert _submit(registry, _key(2)) == "submitted"
    assert _receive(registry, pool)[0].result().progressed


def test_racing_foreign_physical_work_keeps_the_original_pending_owner(owned, monkeypatch):
    from riverhog_core.services.physical_execution import PhysicalExecutionOccupied

    registry, pool = owned.make()
    original = _failed_original(registry, pool)
    _failed_receipts(registry, pool)
    actual, foreign = pool.try_submit, object()
    raced = []

    def submit(key, **arguments):
        if not raced:
            raced.append(key)
            assert (
                actual(
                    key,
                    target_incarnation="target-a",
                    terminal=False,
                    work=lambda: "foreign",
                    owner=foreign,
                )
                == "submitted"
            )
        with pytest.raises(PhysicalExecutionOccupied):
            actual(key, **arguments)
        raise PhysicalExecutionOccupied("another local owner acquired the slot")

    monkeypatch.setattr(pool, "try_submit", submit)
    assert _maintain(registry, limit=1, retry_after=_RETRY) == 0
    assert registry._contexts[1].owner is original and original.has_pending
    for slot in tuple(pool._slots.values()):
        slot.worker.join(timeout=10)
        assert not slot.worker.is_alive()
    assert registry.collect(limit=1) == ()
    (receipt,) = pool.collect(limit=1, owner=foreign)
    assert receipt.owner is foreign and receipt.result() == "foreign"
    assert original.calls == ["original"]


def test_retirement_visit_work_is_independent_of_other_retained_contexts(owned, record_property):
    class Idle(Finalizer):
        def __init__(self, probes):
            super().__init__()
            self.probes = probes

        @property
        def has_pending(self):
            self.probes.append("original")
            return False

    counts = []
    for size in (8, 32, 128):
        registry, pool = owned.make(
            retained_constructions=size, retained_constructions_per_target=size
        )
        probes = []
        for identity in range(1, size + 1):
            assert _submit(registry, _key(identity), create=partial(Idle, probes)) == "submitted"
            assert _receive(registry, pool)[0].result().progressed
        probes.clear()
        assert _maintain(registry, limit=3, retry_after=_RETRY) == 3
        assert len(registry._contexts) == size - 3
        counts.append(len(probes))
    assert counts == [6, 6, 6]
    record_property("retained_originals", [8, 32, 128])
    record_property("bounded_idle_owner_observations", counts)
