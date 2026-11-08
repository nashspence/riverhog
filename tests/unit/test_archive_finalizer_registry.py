from __future__ import annotations

import hashlib
import json
import threading
from types import SimpleNamespace

import pytest
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.services import archive_upload_execution as execution_module
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.archive_finalizer_registry import ArchiveFinalizerRegistry
from riverhog_core.services.archive_upload_execution import (
    ArchiveExecutionOccupied,
    ArchiveUploadExecutionKey,
    ArchiveUploadExecutionOwner,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_write_admission import _case, _selected


class Finalizer:
    def __init__(self):
        self.pending = False
        self.calls = []
        self.drains = []

    @property
    def has_pending(self):
        return self.pending

    def advance(self):
        self.calls.append("original")
        return True

    def drain_terminal(self, operation_id):
        self.drains.append(operation_id)
        return "closed"


def _key(number=1):
    return ArchiveUploadExecutionKey(number, "a" * 64)


def _operation(case):
    return _selected(
        SimpleNamespace(owner=ArchiveWriteAdmission(case.factory), collection_id=case.collection_id)
    )


def _scope(case):
    with session_scope(case.factory) as session:
        control = session.get(ArchiveWriteControlRecord, case.collection_id)
        value = json.loads(control.construction_json)
        return (
            ArchiveUploadExecutionKey(
                case.collection_id, hashlib.sha256(control.construction_json.encode()).hexdigest()
            ),
            value["archive_incarnation_id"],
        )


@pytest.fixture
def owned():
    registries, releases = [], []

    def make(factory=None, *, retention=None, execution=None, **values):
        tuning = ArchiveExecutionTuning(**values)
        pool = execution or ArchiveUploadExecutionOwner(tuning)
        registry = ArchiveFinalizerRegistry(
            factory, pool, retention or ArchiveConstructionRetention(tuning)
        )
        registries.append(registry)
        return registry, pool

    def event():
        release = threading.Event()
        releases.append(release)
        return release

    yield SimpleNamespace(make=make, event=event)
    for event in releases:
        event.set()
    for registry in registries:
        registry.close()


def _submit(
    registry, key=None, *, target="target-a", selection="original", create=Finalizer, advance=None
):
    return registry.submit(
        key or _key(),
        target_incarnation=target,
        selection=selection,
        create=create,
        advance=advance or (lambda owner: owner.advance()),
    )


def _receive(registry, pool):
    for slot in tuple(pool._slots.values()):
        slot.worker.join(timeout=10)
        assert not slot.worker.is_alive()
    return registry.collect(limit=100)


def test_pending_original_retention_is_independent_of_received_physical_capacity(owned):
    registry, pool = owned.make(retained_constructions=2, retained_constructions_per_target=1)
    owner = Finalizer()

    def fail(original):
        original.pending = True
        original.calls.append("failed original")
        raise OSError("synthetic original acknowledgement")

    assert _submit(registry, create=lambda: owner, advance=fail) == "submitted"
    (result,) = _receive(registry, pool)
    with pytest.raises(OSError, match="original acknowledgement"):
        result.result()
    assert pool.status(_key()) == "untracked"
    assert not registry.retire_idle(_key(), target_incarnation="target-a")
    assert _submit(registry, _key(2)) == "target-retention-capacity"
    assert _submit(registry, _key(2), target="target-b") == "submitted"
    _receive(registry, pool)
    assert _submit(registry, _key(3), target="target-c") == "retention-capacity"
    assert registry._contexts[1].owner is owner and owner.pending


def test_pending_original_resumes_its_saved_selection_before_new_factory_or_arguments(owned):
    registry, pool = owned.make()
    original = Finalizer()
    attempts = 0

    def old_step(owner):
        nonlocal attempts
        attempts += 1
        owner.pending = attempts == 1
        if owner.pending:
            raise OSError("first acknowledgement")
        return True

    assert _submit(registry, create=lambda: original, advance=old_step) == "submitted"
    (result,) = _receive(registry, pool)
    with pytest.raises(OSError):
        result.result()
    assert (
        _submit(
            registry,
            selection="different selected input",
            create=lambda: pytest.fail("cannot reconstruct pending original"),
            advance=lambda owner: pytest.fail("cannot replace original arguments"),
        )
        == "submitted"
    )
    (result,) = _receive(registry, pool)
    turn = result.result()
    assert turn.selection == "original" and turn.progressed
    assert attempts == 2 and registry._contexts[1].owner is original
    assert registry.retire_idle(_key(), target_incarnation="target-a")


def test_idle_owner_cannot_retire_between_future_publication_and_actual_join(owned, monkeypatch):
    returned = threading.Event()
    release = owned.event()

    class HeldExit(threading.Thread):
        def run(self):
            super().run()
            returned.set()
            assert release.wait(10)

    monkeypatch.setattr(execution_module, "Thread", HeldExit)
    registry, pool = owned.make(retained_constructions=1)
    assert _submit(registry) == "submitted"
    assert returned.wait(5)
    assert pool._slots[1].future.done()
    assert not registry.retire_idle(_key(), target_incarnation="target-a")
    assert registry.collect(limit=1) == ()
    assert _submit(registry, _key(2)) == "retention-capacity"
    release.set()
    (result,) = _receive(registry, pool)
    assert result.result().progressed
    assert registry.retire_idle(_key(), target_incarnation="target-a")
    assert _submit(registry, _key(2)) == "submitted"


def test_constructor_is_tracked_before_execution_without_stalling_an_independent_target(owned):
    registry, pool = owned.make()
    entered = threading.Event()
    release = owned.event()

    def construct():
        entered.set()
        assert release.wait(10)
        return Finalizer()

    assert _submit(registry, create=construct) == "submitted"
    assert entered.wait(5)
    assert registry._contexts[1].owner is None
    assert _submit(registry, _key(2), target="target-b") == "submitted"
    pool._slots[2].worker.join(timeout=5)
    assert not pool._slots[2].worker.is_alive()
    (result,) = registry.collect(limit=2)
    assert result.key == _key(2) and result.result().progressed
    release.set()


def test_a_racing_foreign_execution_cannot_be_adopted_as_this_registry_work(owned, monkeypatch):
    registry, pool = owned.make()
    actual = pool.try_submit
    raced = False

    def submit(key, **arguments):
        nonlocal raced
        if not raced:
            raced = True
            assert (
                actual(key, target_incarnation="target-a", terminal=False, work=lambda: "foreign")
                == "submitted"
            )
        return actual(key, **arguments)

    monkeypatch.setattr(pool, "try_submit", submit)
    with pytest.raises(ArchiveExecutionOccupied):
        _submit(registry, create=lambda: pytest.fail("must not run foreign constructor"))
    assert registry._contexts == {}
    assert _receive(registry, pool) == ()
    assert pool.status(_key()) == "awaiting-join"
    (result,) = pool.collect(limit=1, owner=None)
    assert result.owner is None and result.result() == "foreign"


def test_failed_constructor_retains_its_reserved_unknown_context(owned):
    registry, pool = owned.make(retained_constructions=1)

    def construct():
        raise OSError("constructor acknowledgement")

    assert _submit(registry, create=construct) == "submitted"
    (result,) = _receive(registry, pool)
    with pytest.raises(OSError, match="constructor acknowledgement"):
        result.result()
    assert not registry.retire_idle(_key(), target_incarnation="target-a")
    assert _submit(registry, _key(2)) == "retention-capacity"


def test_terminal_fence_precedes_constructor_return_and_never_disposes_inputs(tmp_path, owned):
    with _case(tmp_path) as case:
        key, target = _scope(case)
        registry, pool = owned.make(case.factory)
        entered, release = threading.Event(), owned.event()
        original = Finalizer()

        def construct():
            entered.set()
            assert release.wait(10)
            return original

        assert _submit(registry, key, target=target, create=construct) == "submitted"
        assert entered.wait(5)
        operation = _operation(case)
        assert (
            registry.fence_terminal(key, target_incarnation=target, operation_id=operation)
            == "fenced"
        )
        with pytest.raises(Conflict, match="fenced"):
            _submit(registry, key, target=target)
        assert (
            registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
            == "busy"
        )
        release.set()
        (result,) = _receive(registry, pool)
        with pytest.raises(Conflict, match="fenced during construction"):
            result.result()
        assert original.calls == []
        assert registry._contexts[key.collection_id].owner is original
        assert (
            registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
            == "submitted"
        )
        (result,) = _receive(registry, pool)
        assert result.result() == "closed" and original.drains == [operation]
        assert (
            registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
            == "drained"
        )
        assert not registry.retire_idle(key, target_incarnation=target)


def test_invalid_terminal_request_cannot_install_a_local_forward_fence(tmp_path, owned):
    with _case(tmp_path) as case:
        key, target = _scope(case)
        registry, pool = owned.make(case.factory)
        assert _submit(registry, key, target=target) == "submitted"
        _receive(registry, pool)
        with pytest.raises(Conflict):
            registry.fence_terminal(key, target_incarnation=target, operation_id="f" * 64)
        assert registry._contexts[key.collection_id].terminal_id is None


def test_original_terminal_retry_does_not_repeat_catalog_selection(tmp_path, owned, monkeypatch):
    from riverhog_core.services import archive_finalizer_registry as registry_module

    with _case(tmp_path) as case:
        key, target = _scope(case)
        registry, pool = owned.make(case.factory)
        original = Finalizer()
        assert _submit(registry, key, target=target, create=lambda: original) == "submitted"
        _receive(registry, pool)
        operation = _operation(case)
        assert (
            registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
            == "submitted"
        )
        _receive(registry, pool)
        monkeypatch.setattr(
            registry_module,
            "inspect_terminal_write_records",
            lambda *args: pytest.fail("must replay original local fence"),
        )
        assert (
            registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
            == "drained"
        )
        with pytest.raises(Conflict, match="another terminal"):
            registry.submit_terminal(key, target_incarnation=target, operation_id="b" * 64)


def test_shutdown_joins_workers_without_clearing_pending_originals(owned):
    registry, pool = owned.make()
    original = Finalizer()
    original.pending = True
    # The first call constructs this pending owner; its continuation is retained.
    assert _submit(registry, create=lambda: original) == "submitted"
    _receive(registry, pool)
    registry.close()
    assert registry._contexts[1].owner is original and original.pending
    assert _submit(registry, _key(2)) == "closed"
    assert not registry.retire_idle(_key(), target_incarnation="target-a")


@pytest.mark.parametrize("mode", ["direct", "http"])
@pytest.mark.parametrize("committed", [False, True])
def test_actual_description_original_survives_worker_receiving_before_terminal_drain(
    tmp_path, monkeypatch, owned, mode, committed
):
    from tests.unit.test_archive_initial_description_checkpoint import _description_source

    with _description_source(tmp_path, monkeypatch, mode=mode) as fixture:
        _actual_description_terminal(fixture, monkeypatch, owned, committed)


def test_unknown_constructor_cannot_be_recreated_by_another_forward_submission(owned):
    registry, pool = owned.make()

    def unknown():
        raise OSError("original constructor acknowledgement")

    assert _submit(registry, create=unknown) == "submitted"
    (first,) = _receive(registry, pool)
    with pytest.raises(OSError):
        first.result()
    assert (
        _submit(
            registry,
            selection="replacement",
            create=lambda: pytest.fail("unknown constructor must not be recreated"),
        )
        == "submitted"
    )
    (retry,) = _receive(registry, pool)
    with pytest.raises(Conflict, match="constructor outcome is unresolved"):
        retry.result()
    assert registry._contexts[1].selection == "original"


def test_unknown_constructor_blocks_terminal_owner_closure_and_retirement(tmp_path, owned):
    with _case(tmp_path) as case:
        key, target = _scope(case)
        registry, pool = owned.make(case.factory)

        def unknown():
            raise OSError("original constructor acknowledgement")

        assert _submit(registry, key, target=target, create=unknown) == "submitted"
        (original,) = _receive(registry, pool)
        with pytest.raises(OSError):
            original.result()
        operation = _operation(case)
        assert (
            registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
            == "submitted"
        )
        (result,) = _receive(registry, pool)
        assert result.result() == "pending"
        assert not registry._contexts[key.collection_id].drained
        assert not registry.retire_idle(key, target_incarnation=target)


def _actual_description_terminal(fixture, monkeypatch, owned, committed):
    from tests.unit.test_archive_initial_description_checkpoint import _snapshot
    from tests.unit.test_archive_initial_description_finalizer import _owner
    from tests.unit.test_archive_provenance_journal_writer import _lose_ack

    key, target = _scope(fixture.case)
    registry, pool = owned.make(fixture.case.factory)
    before = _snapshot(fixture)
    created = []

    def construct():
        value = _owner(fixture)
        created.append(value)
        return value

    with _lose_ack(monkeypatch, "report", committed):
        assert _submit(registry, key, target=target, create=construct) == "submitted"
        (result,) = _receive(registry, pool)
        with pytest.raises(OSError):
            result.result()
    original = created[0]
    actor = original._actor
    assert original.has_pending and actor._response_json is not None
    assert pool.status(key) == "untracked"
    assert not registry.retire_idle(key, target_incarnation=target)
    operation = _operation(fixture.case)
    assert (
        registry.submit_terminal(key, target_incarnation=target, operation_id=operation)
        == "submitted"
    )
    (result,) = _receive(registry, pool)
    assert result.result() == "closed"
    assert registry._contexts[key.collection_id].owner is original
    assert original._actor is actor and original.has_pending
    assert _snapshot(fixture) == before
    assert not registry.retire_idle(key, target_incarnation=target)


def test_finalizer_receiving_cannot_retire_another_work_kinds_terminal_outcome(owned):
    registry, pool = owned.make()
    terminal_owner = object()
    terminal_result = object()
    assert (
        pool.try_submit(
            _key(2),
            target_incarnation="target-b",
            terminal=True,
            owner=terminal_owner,
            work=lambda: terminal_result,
        )
        == "submitted"
    )
    assert _submit(registry) == "submitted"
    (result,) = _receive(registry, pool)
    assert result.key == _key() and result.owner is registry
    assert result.result().progressed
    assert pool.status(_key(2)) == "awaiting-join"
    assert pool._counts[True] == 1
    assert registry.collect(limit=2) == ()
    (result,) = pool.collect(limit=2, owner=terminal_owner)
    assert result.owner is terminal_owner and result.terminal
    assert result.result() is terminal_result and pool._counts[True] == 0


def test_independent_registries_share_global_and_target_retention_without_releasing_unknowns(owned):
    tuning = ArchiveExecutionTuning(
        retained_constructions=2,
        retained_constructions_per_target=1,
    )
    retention = ArchiveConstructionRetention(tuning)
    execution = ArchiveUploadExecutionOwner(tuning)
    first, _ = owned.make(retention=retention, execution=execution)
    second, _ = owned.make(retention=retention, execution=execution)
    original = Finalizer()
    original.pending = True
    assert _submit(first, _key(), create=lambda: original) == "submitted"
    _receive(first, execution)
    assert (
        _submit(
            second, _key(2), create=lambda: pytest.fail("target grant must precede construction")
        )
        == "target-retention-capacity"
    )
    assert _submit(second, _key(2), target="target-b") == "submitted"
    _receive(second, execution)
    assert (
        _submit(
            first,
            _key(3),
            target="target-c",
            create=lambda: pytest.fail("global grant must precede construction"),
        )
        == "retention-capacity"
    )
    assert not first.retire_idle(_key(), target_incarnation="target-a")
    assert second.retire_idle(_key(2), target_incarnation="target-b")
    assert _submit(second, _key(3), target="target-c") == "submitted"
    _receive(second, execution)
    assert first._contexts[1].owner is original and original.has_pending


def test_same_construction_in_another_registry_does_not_retire_an_earlier_original(owned):
    tuning = ArchiveExecutionTuning(retained_constructions=2)
    retention = ArchiveConstructionRetention(tuning)
    execution = ArchiveUploadExecutionOwner(tuning)
    first, _ = owned.make(retention=retention, execution=execution)
    second, _ = owned.make(retention=retention, execution=execution)
    original = Finalizer()
    original.pending = True
    assert _submit(first, create=lambda: original) == "submitted"
    _receive(first, execution)
    assert _submit(second) == "submitted"
    _receive(second, execution)
    assert second.retire_idle(_key(), target_incarnation="target-a")
    assert not first.retire_idle(_key(), target_incarnation="target-a")
    assert first._contexts[1].owner is original
    assert _submit(second, _key(2), target="target-b") == "submitted"
    _receive(second, execution)
    assert _submit(second, _key(3), target="target-c") == "retention-capacity"


def test_busy_retention_owner_defers_idle_retirement_without_losing_its_context(owned):
    tuning = ArchiveExecutionTuning(retained_constructions=1)
    retention = ArchiveConstructionRetention(tuning)
    registry, execution = owned.make(retention=retention)
    assert _submit(registry) == "submitted"
    _receive(registry, execution)
    with retention._lock:
        assert not registry.retire_idle(_key(), target_incarnation="target-a")
        assert _submit(registry, _key(2)) == "busy"
    assert _submit(registry, _key(2)) == "retention-capacity"
    assert registry.retire_idle(_key(), target_incarnation="target-a")
    assert _submit(registry, _key(2)) == "submitted"


def test_unstarted_capacity_refusal_can_retire_after_shared_retention_contention(
    owned, monkeypatch
):
    tuning = ArchiveExecutionTuning(forward_workers=1, retained_constructions=1)
    retention = ArchiveConstructionRetention(tuning)
    registry, execution = owned.make(retention=retention, forward_workers=1)
    release = owned.event()
    assert (
        execution.try_submit(
            _key(9), target_incarnation="target-a", terminal=False, work=lambda: release.wait(10)
        )
        == "submitted"
    )
    actual = execution.try_submit

    def refuse(*args, **kwargs):
        outcome = actual(*args, **kwargs)
        assert outcome == "capacity"
        retention._lock.acquire()
        return outcome

    monkeypatch.setattr(execution, "try_submit", refuse)
    try:
        assert (
            _submit(registry, create=lambda: pytest.fail("refused constructor cannot start"))
            == "capacity"
        )
        assert registry._contexts[1].owner is None and registry._contexts[1].selection is None
        assert not registry.retire_idle(_key(), target_incarnation="target-a")
    finally:
        retention._lock.release()
    assert registry.retire_idle(_key(), target_incarnation="target-a")
    release.set()


def test_missing_original_finalizer_receiver_cannot_become_terminal_drainage(tmp_path, owned):
    with _case(tmp_path) as case:
        operation = _operation(case)
        key, target = _scope(case)
        registry, pool = owned.make(case.factory)
        assert _submit(registry, key, target=target, create=lambda: None) == "submitted"
        (failed,) = _receive(registry, pool)
        with pytest.raises(ValueError, match="no receiver"):
            failed.result()
        assert (
            registry.submit_terminal(
                key,
                target_incarnation=target,
                operation_id=operation,
            )
            == "submitted"
        )
        (pending,) = _receive(registry, pool)
        assert pending.result() == "pending"
        assert (
            registry.fence_terminal(
                key,
                target_incarnation=target,
                operation_id=operation,
            )
            == "fenced"
        )


def test_original_only_resume_cannot_install_an_untracked_constructor(owned):
    registry, pool = owned.make()
    with pytest.raises(Conflict, match="original archive finalizer is unavailable"):
        registry.resume(_key(), target_incarnation="target-a")
    assert registry._contexts == {} and pool._slots == {}
    assert registry._retention._reservations == {}


@pytest.mark.parametrize("pending", (False, True))
def test_original_only_resume_keeps_exact_original_and_never_calls_its_factory(owned, pending):
    registry, pool = owned.make()
    original, created = Finalizer(), []
    original.pending = pending

    def create():
        created.append(original)
        return original

    assert _submit(registry, create=create) == "submitted"
    (first,) = _receive(registry, pool)
    assert first.result().progressed
    context = registry._contexts[1]
    assert registry.resume(_key(), target_incarnation="target-a") == "submitted"
    (resumed,) = _receive(registry, pool)
    if pending:
        assert resumed.result().selection == "original"
    else:
        with pytest.raises(Conflict, match="original archive finalizer is unavailable"):
            resumed.result()
    assert context.owner is original and created == [original]
    assert original.calls == ["original"] * (2 if pending else 1)


def test_original_only_resume_retains_unknown_constructor_without_reconstruction(owned):
    registry, pool = owned.make()
    calls = []

    def create():
        calls.append("constructor")
        raise OSError("injected constructor acknowledgement")

    assert _submit(registry, create=create) == "submitted"
    (failed,) = _receive(registry, pool)
    with pytest.raises(OSError, match="constructor acknowledgement"):
        failed.result()
    context = registry._contexts[1]
    failure = context.construction_failure
    assert registry.resume(_key(), target_incarnation="target-a") == "submitted"
    (resumed,) = _receive(registry, pool)
    with pytest.raises(Conflict, match="constructor outcome is unresolved"):
        resumed.result()
    assert context.construction_failure is failure and calls == ["constructor"]
    assert not registry.retire_idle(_key(), target_incarnation="target-a")
