from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from riverhog_core.services import archive_upload_terminal_registry as registry_module
from riverhog_core.services import physical_execution as physical
from riverhog_core.services.physical_execution import PhysicalExecutionKey
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_upload_maintenance import _join
from tests.unit.test_archive_upload_terminal_result import _state
from tests.unit.test_archive_upload_terminal_retirement import _retained
from tests.unit.test_archive_upload_terminal_turn import _drained, _turn_case

RETRY = timedelta(seconds=1)


def _registry(case, monkeypatch, *, create=None):
    registry = case.service._archive_terminals
    actual = create or registry._create
    actors, calls = [], []

    def make(collection_id, operation_id):
        assert threading.current_thread().name == "physical-execution"
        assert registry._retention._counts[True] == 1
        calls.append((collection_id, operation_id))
        actor = actual(collection_id, operation_id)
        actors.append(actor)
        return actor

    monkeypatch.setattr(registry, "_create", make)
    actual_submit = registry._execution.try_submit

    def submit_after_lock(key, **options):
        if options.get("owner") is not registry:
            return actual_submit(key, **options)
        submitted = threading.Event()
        work = options["work"]

        def after_submission():
            assert submitted.wait(10)
            return work()

        # Isolate retirement work after submission releases its status-probe mutex.
        options["work"] = after_submission
        try:
            return actual_submit(key, **options)
        finally:
            submitted.set()

    monkeypatch.setattr(registry._execution, "try_submit", submit_after_lock)
    clock = [100.0]
    monkeypatch.setattr(registry_module, "monotonic", lambda: clock[0])
    key = PhysicalExecutionKey(case.collection_id, case.operation)
    return registry, key, clock, calls, actors


def _submit(registry, key, target, *, work_limit=1):
    return registry.submit(key, target_incarnation=target, work_limit=work_limit, retry_after=RETRY)


def _receive(case, registry):
    _join(case.service)
    return registry.collect(limit=1)


def _bounded_original(case, monkeypatch):
    _drained(case)
    registry, key, clock, calls, actors = _registry(case, monkeypatch)
    for remaining in range(len(case.registries) - 1, -1, -1):
        if not calls:
            assert _submit(registry, key, case.incarnation) == 1
        else:
            clock[0] += 2
            assert registry.resume_ready(limit=1, work_limit=1, retry_after=RETRY) == 1
        assert _receive(case, registry) == 1
        context = registry._contexts[key]
        assert context.progress.retired_originals == 1
        assert context.progress.originals_retired is (remaining == 0)
        assert _retained(case) == remaining + 1
        assert context.original is context.actor is actors[0]
        assert context.reservation.terminal
        assert case.service._archive_retention._counts == {False: remaining, True: 1}
        assert _state(case, case.operation)[3] is None
    assert len(actors) == len(calls) == 1
    assert not case.service._archive_execution._slots
    # Positive local retirement is an intermediate result. The global actor
    # keeps the original operation and its current guard charged.
    assert registry.collect(limit=1) == 0 and key in registry._contexts
    return registry, key, clock, calls, actors


def test_registry_uses_one_original_factory_for_bounded_turns_and_keeps_terminal_charge(
    tmp_path, monkeypatch
):
    with _turn_case(tmp_path, originals=3) as case:
        _bounded_original(case, monkeypatch)


def _unknown_constructor(case, monkeypatch, error):
    def fail(*args):
        raise error("original constructor uncertain")

    registry, key, clock, calls, actors = _registry(case, monkeypatch, create=fail)
    assert _submit(registry, key, case.incarnation) == 1
    assert _receive(case, registry) == 1
    context = registry._contexts[key]
    assert context.entered and context.actor is None and context.received
    for _ in range(3):
        clock[0] += 2
        assert registry.resume_ready(limit=1, work_limit=1, retry_after=RETRY) == 0
        assert _submit(registry, key, case.incarnation) == 0
        assert registry.collect(limit=1) == 0
    assert len(calls) == 1 and actors == []
    assert case.service._archive_retention._counts[True] == 1
    assert _state(case, case.operation)[0] == "draining"


@pytest.mark.parametrize("error", (OSError, KeyboardInterrupt, SystemExit))
def test_unknown_constructor_is_received_but_never_recreated_or_released(
    tmp_path, monkeypatch, error
):
    with _turn_case(tmp_path, originals=0) as case:
        _unknown_constructor(case, monkeypatch, error)


@pytest.mark.parametrize("mismatch", ("key", "target", "type"))
def test_wrong_constructor_original_stays_charged_without_becoming_an_actor(
    tmp_path, monkeypatch, mismatch
):
    with _turn_case(tmp_path, originals=0) as case:
        make = case.service._terminal_turn

        def wrong(*args):
            actor = make(*args)
            if mismatch == "type":
                return object()
            if mismatch == "key":
                actor._key = PhysicalExecutionKey(case.collection_id, "f" * 64)
            else:
                actor._target = "another-target"
            return actor

        registry, key, clock, calls, actors = _registry(case, monkeypatch, create=wrong)
        assert _submit(registry, key, case.incarnation) == 1
        assert _receive(case, registry) == 1
        context = registry._contexts[key]
        assert context.original is actors[0] and context.actor is None
        assert case.service._archive_retention._counts[True] == 1
        clock[0] += 2
        assert registry.resume_ready(limit=1, work_limit=1, retry_after=RETRY) == 0
        assert len(calls) == 1 and _state(case, case.operation)[0] == "draining"


def _joined_receiving(case, monkeypatch, *, lost_start=False):
    _drained(case)
    registry, key, clock, calls, actors = _registry(case, monkeypatch)
    actual = physical._run
    published, release = threading.Event(), threading.Event()

    def held(work, future):
        actual(work, future)
        published.set()
        assert release.wait(15)

    monkeypatch.setattr(physical, "_run", held)
    submit = registry._execution.try_submit

    def lose(*args, **kwargs):
        result = submit(*args, **kwargs)
        if lost_start and result == "submitted":
            raise OSError("worker start acknowledgement lost")
        return result

    monkeypatch.setattr(registry._execution, "try_submit", lose)
    try:
        if lost_start:
            with pytest.raises(OSError, match="start acknowledgement"):
                _submit(registry, key, case.incarnation)
        else:
            assert _submit(registry, key, case.incarnation) == 1
        assert published.wait(10)
        context = registry._contexts[key]
        assert context.actor is actors[0] and not context.received and context.progress is None
        assert _state(case, case.operation)[0] == "cleanup-ready"
        assert case.service._archive_retention._counts == {False: 0, True: 1}
        slot = registry._execution._slots[key]

        def forbidden(*args, **kwargs):
            pytest.fail("coordinator cannot join a live terminal worker")

        with monkeypatch.context() as patch:
            patch.setattr(slot.worker, "join", forbidden)
            with ThreadPoolExecutor(max_workers=1) as pool:
                assert pool.submit(registry.collect, limit=1).result(timeout=2) == 0
                assert (
                    pool.submit(
                        registry.resume_ready, limit=1, work_limit=1, retry_after=RETRY
                    ).result(timeout=2)
                    == 0
                )
            assert slot.worker.is_alive()
            assert case.service._archive_retention._counts[True] == 1
    finally:
        release.set()
    assert _receive(case, registry) == 1
    assert context.received and context.progress.originals_retired
    assert context.actor is actors[0] and len(calls) == 1
    assert not registry._execution._slots
    return registry, key, clock, calls, actors


@pytest.mark.parametrize("lost_start", (False, True))
def test_successful_quantum_and_lost_start_reply_require_actual_join_and_original_receiving(
    tmp_path, monkeypatch, lost_start
):
    with _turn_case(tmp_path) as case:
        _joined_receiving(case, monkeypatch, lost_start=lost_start)


def test_positive_thread_nonentry_releases_unused_terminal_reservation(tmp_path, monkeypatch):
    with _turn_case(tmp_path, originals=0) as case:
        registry, key, clock, calls, actors = _registry(case, monkeypatch)

        def fail(worker):
            raise OSError("thread never started")

        with monkeypatch.context() as patch:
            patch.setattr(physical.Thread, "start", fail)
            with pytest.raises(OSError, match="never started"):
                _submit(registry, key, case.incarnation)
        assert not calls and not actors and not registry._execution._slots
        assert registry.collect(limit=1) == 1 and not registry._contexts
        assert not case.service._archive_retention._reservations
        assert _submit(registry, key, case.incarnation) == 1
        assert _receive(case, registry) == 1


@pytest.mark.parametrize("lane", ("forward", "terminal"))
def test_constructor_admission_uses_reserved_terminal_retention(tmp_path, monkeypatch, lane):
    with _turn_case(tmp_path, originals=0) as case:
        registry, key, clock, calls, actors = _registry(case, monkeypatch)
        retention, held = registry._retention, []
        owner = object()
        while True:
            reservation = retention.try_retain(
                PhysicalExecutionKey(case.collection_id, "e" * 64),
                target_incarnation=case.incarnation,
                owner=owner,
                terminal=lane == "terminal",
            )
            if isinstance(reservation, str):
                break
            held.append(reservation)
        try:
            assert _submit(registry, key, case.incarnation) == int(lane == "forward")
            if lane == "terminal":
                assert not calls and not registry._contexts
            else:
                assert _receive(case, registry) == 1 and len(calls) == 1
        finally:
            for reservation in held:
                assert retention.retire(reservation, owner=owner)


def test_retained_scope_and_nonblocking_registry_contention(tmp_path, monkeypatch):
    with _turn_case(tmp_path, originals=0) as case:
        registry, key, clock, calls, actors = _registry(case, monkeypatch)
        assert _submit(registry, key, case.incarnation) == 1
        assert _receive(case, registry) == 1
        with pytest.raises(Conflict, match="original target changed"):
            _submit(registry, key, "different-target")
        with registry._lock, ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(registry.collect, limit=1).result(timeout=2) == 0
            assert (
                pool.submit(registry.resume_ready, limit=1, work_limit=1, retry_after=RETRY).result(
                    timeout=2
                )
                == 0
            )
            assert pool.submit(_submit, registry, key, case.incarnation).result(timeout=2) == 0
        clock[0] += 2
        registry.close()
        assert _submit(registry, key, case.incarnation) == 0
        assert registry.resume_ready(limit=1, work_limit=1, retry_after=RETRY) == 0
        assert len(calls) == 1 and registry._contexts[key].actor is actors[0]


def _resume_start_failure(case, monkeypatch):
    _drained(case)
    registry, key, clock, calls, actors = _registry(case, monkeypatch)
    assert _submit(registry, key, case.incarnation) == 1
    assert _receive(case, registry) == 1
    context = registry._contexts[key]
    original = context.actor
    clock[0] += 2

    def fail(worker):
        raise OSError("resumed thread never started")

    with monkeypatch.context() as patch:
        patch.setattr(physical.Thread, "start", fail)
        with pytest.raises(OSError, match="resumed thread"):
            _submit(registry, key, case.incarnation)
    assert context.received and context.actor is original
    assert registry.collect(limit=1) == 0
    assert case.service._archive_retention._counts[True] == 1
    assert _submit(registry, key, case.incarnation) == 1
    assert _receive(case, registry) == 1
    assert context.actor is original and len(calls) == len(actors) == 1


def test_later_thread_nonentry_preserves_the_received_original_and_can_resume(
    tmp_path, monkeypatch
):
    with _turn_case(tmp_path) as case:
        _resume_start_failure(case, monkeypatch)


@pytest.mark.parametrize("occupied", ("scope", "target", "global"))
def test_actual_shared_physical_refusal_never_enters_an_uncharged_constructor(
    tmp_path, monkeypatch, occupied
):
    from dataclasses import replace

    with _turn_case(tmp_path, originals=0) as case:
        registry, key, clock, calls, actors = _registry(case, monkeypatch)
        execution = registry._execution
        monkeypatch.setattr(execution, "_tuning", replace(execution._tuning, terminal_workers=1))
        foreign = object()
        release, entered = threading.Event(), threading.Event()
        other_key = (
            key if occupied == "scope" else PhysicalExecutionKey(case.collection_id, "e" * 64)
        )
        target = "another-target" if occupied == "global" else case.incarnation

        def held():
            entered.set()
            assert release.wait(15)

        try:
            assert (
                execution.try_submit(
                    other_key,
                    target_incarnation=target,
                    terminal=True,
                    work=held,
                    owner=foreign,
                )
                == "submitted"
            )
            assert entered.wait(5)
            assert _submit(registry, key, case.incarnation) == 0
            assert calls == actors == []
            assert registry.collect(limit=1) == 1
            assert not registry._contexts and not registry._retention._reservations
            assert execution.status(other_key) == "running"
        finally:
            release.set()
            _join(case.service)
            (received,) = execution.collect(limit=1, owner=foreign)
            received.result()
        assert _submit(registry, key, case.incarnation) == 1
        assert _receive(case, registry) == 1 and len(calls) == 1


def test_foreign_scope_occupation_preserves_a_received_actor_for_later_resume(
    tmp_path, monkeypatch
):
    _original_scope_occupation(tmp_path, monkeypatch)


def _original_scope_occupation(tmp_path, monkeypatch):
    with _turn_case(tmp_path, originals=0) as case:
        registry, key, clock, calls, actors = _registry(case, monkeypatch)
        assert _submit(registry, key, case.incarnation) == 1
        assert _receive(case, registry) == 1
        original = registry._contexts[key].actor
        foreign, release, entered = object(), threading.Event(), threading.Event()

        def held():
            entered.set()
            assert release.wait(15)

        try:
            assert (
                registry._execution.try_submit(
                    key,
                    target_incarnation=case.incarnation,
                    terminal=True,
                    work=held,
                    owner=foreign,
                )
                == "submitted"
            )
            assert entered.wait(5)
            clock[0] += 2
            assert _submit(registry, key, case.incarnation) == 0
            assert registry._contexts[key].received
            assert registry.collect(limit=1) == 0
        finally:
            release.set()
            _join(case.service)
            (received,) = registry._execution.collect(limit=1, owner=foreign)
            received.result()
        assert _submit(registry, key, case.incarnation) == 1
        assert _receive(case, registry) == 1
        assert registry._contexts[key].actor is original and len(calls) == 1
