from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services import archive_upload_execution as source
from riverhog_core.services.archive_upload_execution import (
    ArchiveExecutionOccupied,
    ArchiveUploadExecutionKey,
    ArchiveUploadExecutionOwner,
)
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_protocol.errors import Conflict


def _key(number=1, digest="a" * 64):
    return ArchiveUploadExecutionKey(number, digest)


@pytest.fixture
def owned():
    owners = []
    releases = []

    def make(**values):
        owner = ArchiveUploadExecutionOwner(ArchiveExecutionTuning(**values))
        owners.append(owner)
        return owner

    def event():
        release = threading.Event()
        releases.append(release)
        return release

    yield SimpleNamespace(make=make, event=event)
    for release in releases:
        release.set()
    for owner in owners:
        owner.close()


def _submit(owner, number, work, *, target="target-a", terminal=False):
    return owner.try_submit(_key(number), target_incarnation=target, terminal=terminal, work=work)


def _blocked(entered, release, value=None):
    entered.set()
    assert release.wait(10), "fixture failed to release its original worker"
    return value


def _joined(owner):
    for slot in tuple(owner._slots.values()):
        slot.worker.join(timeout=10)
        assert not slot.worker.is_alive()


def test_forward_exhaustion_preserves_target_isolation_and_terminal_headroom(owned):
    owner = owned.make(
        forward_workers=2,
        terminal_workers=1,
        forward_workers_per_target=1,
        terminal_workers_per_target=1,
    )
    release = owned.event()
    entered = [threading.Event() for _ in range(3)]
    assert _submit(owner, 1, lambda: _blocked(entered[0], release)) == "submitted"
    assert entered[0].wait(5)
    assert _submit(owner, 2, lambda: pytest.fail("must defer same target")) == "target-capacity"
    assert (
        _submit(owner, 2, lambda: _blocked(entered[1], release), target="target-b") == "submitted"
    )
    assert entered[1].wait(5)
    assert (
        _submit(owner, 3, lambda: pytest.fail("must defer full forward lane"), target="target-c")
        == "capacity"
    )
    assert _submit(owner, 3, lambda: _blocked(entered[2], release), terminal=True) == "submitted"
    assert entered[2].wait(5)
    assert (
        _submit(
            owner,
            4,
            lambda: pytest.fail("must defer full terminal lane"),
            terminal=True,
            target="target-b",
        )
        == "capacity"
    )
    release.set()
    _joined(owner)
    assert {result.key.collection_id for result in owner.collect(owner=None, limit=3)} == {1, 2, 3}


def test_terminal_target_limit_leaves_independent_terminal_work_available(owned):
    owner = owned.make(terminal_workers=2, terminal_workers_per_target=1)
    release = owned.event()
    entered = threading.Event()
    assert _submit(owner, 1, lambda: _blocked(entered, release), terminal=True) == "submitted"
    assert entered.wait(5)
    assert _submit(owner, 2, lambda: pytest.fail("must defer"), terminal=True) == "target-capacity"
    assert _submit(owner, 2, lambda: 42, terminal=True, target="target-b") == "submitted"
    release.set()
    _joined(owner)
    results = owner.collect(owner=None, limit=2)
    assert [result.result() for result in results] == [None, 42]


def test_duplicate_submission_keeps_original_callback_and_unreceived_result(owned):
    owner = owned.make()
    release = owned.event()
    entered = threading.Event()
    original = object()
    assert _submit(owner, 1, lambda: _blocked(entered, release, original)) == "submitted"
    assert entered.wait(5)
    assert _submit(owner, 1, lambda: pytest.fail("duplicate callback")) == "retained"
    release.set()
    _joined(owner)
    assert _submit(owner, 1, lambda: pytest.fail("lost original result")) == "retained"
    (result,) = owner.collect(owner=None, limit=1)
    assert result.key == _key()
    assert result.target_incarnation == "target-a"
    assert result.terminal is False
    assert result.result() is result.result() is original
    assert owner.collect(owner=None, limit=1) == ()
    assert _submit(owner, 1, lambda: "next turn") == "submitted"


@pytest.mark.parametrize("changed", ["construction", "target", "lane"])
def test_retained_turn_cannot_be_reinterpreted_as_another_construction_or_lane(owned, changed):
    owner = owned.make()
    assert _submit(owner, 1, lambda: None) == "submitted"
    arguments = dict(key=_key(), target_incarnation="target-a", terminal=False, work=lambda: None)
    if changed == "construction":
        arguments["key"] = _key(digest="b" * 64)
    elif changed == "target":
        arguments["target_incarnation"] = "target-b"
    else:
        arguments["terminal"] = True
    with pytest.raises(Conflict, match="another original turn"):
        owner.try_submit(**arguments)


def test_published_future_is_not_physical_worker_join_or_free_capacity(owned, monkeypatch):
    returned = threading.Event()
    release = owned.event()
    actual_thread = threading.Thread

    class RetainedThread(actual_thread):
        def run(self):
            super().run()
            returned.set()
            assert release.wait(10), "fixture failed to release worker exit"

    monkeypatch.setattr(source, "Thread", RetainedThread)
    owner = owned.make(forward_workers=1)
    assert not owner.has_unreceived_work()
    with owner._lock:
        assert owner.has_unreceived_work()
    original = object()
    assert _submit(owner, 1, lambda: original) == "submitted"
    assert returned.wait(5)
    slot = owner._slots[1]
    assert slot.future.done() and slot.future.result() is original
    assert slot.worker.is_alive() and owner.has_unreceived_work()
    assert owner.collect(owner=None, limit=1) == ()
    assert _submit(owner, 2, lambda: pytest.fail("physical capacity still held")) == "capacity"
    assert _submit(owner, 1, lambda: pytest.fail("original outcome still held")) == "retained"
    release.set()
    _joined(owner)
    assert owner.has_unreceived_work()
    joins = []
    actual_join = slot.worker.join

    def observe_join(timeout=None):
        joins.append(timeout)
        actual_join(timeout)

    monkeypatch.setattr(slot.worker, "join", observe_join)
    (result,) = owner.collect(owner=None, limit=1)
    assert joins == [0]
    assert result.result() is original and not owner.has_unreceived_work()
    assert _submit(owner, 2, lambda: None) == "submitted"


def test_failed_worker_preserves_the_exact_original_exception_for_receiving(owned):
    owner = owned.make()
    failure = SystemExit("synthetic original execution failure")

    def fail():
        raise failure

    assert _submit(owner, 1, fail) == "submitted"
    _joined(owner)
    (completion,) = owner.collect(owner=None, limit=1)
    with pytest.raises(SystemExit) as observed:
        completion.result()
    assert observed.value is failure
    assert owner.collect(owner=None, limit=1) == ()


def test_collect_has_bounded_fair_progress_past_a_stuck_worker(owned):
    owner = owned.make()
    release = owned.event()
    entered = threading.Event()
    assert _submit(owner, 1, lambda: _blocked(entered, release)) == "submitted"
    assert entered.wait(5)
    assert _submit(owner, 2, lambda: "independent") == "submitted"
    owner._slots[2].worker.join(timeout=5)
    assert not owner._slots[2].worker.is_alive()
    assert owner.collect(owner=None, limit=1) == ()
    (completion,) = owner.collect(owner=None, limit=1)
    assert completion.key.collection_id == 2 and completion.result() == "independent"
    assert owner.collect(owner=None, limit=1) == ()


def test_poll_and_submission_defer_under_registry_contention_without_waiting(owned):
    owner = owned.make()
    with owner._lock:
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(_submit, owner, 1, lambda: None)
            assert result.result(timeout=5) == "busy"
            assert executor.submit(owner.collect, owner=None, limit=1).result(timeout=5) == ()


@pytest.mark.parametrize("after_start", [False, True])
def test_startup_failure_releases_only_a_worker_that_never_started(owned, monkeypatch, after_start):
    actual_thread = threading.Thread
    release = owned.event()
    entered = threading.Event()

    class FailedStart(actual_thread):
        def start(self):
            if after_start:
                super().start()
            raise RuntimeError("synthetic startup acknowledgement failure")

    monkeypatch.setattr(source, "Thread", FailedStart)
    owner = owned.make(forward_workers=1)
    with pytest.raises(RuntimeError, match="startup acknowledgement"):
        _submit(owner, 1, lambda: _blocked(entered, release, "original"))
    monkeypatch.setattr(source, "Thread", actual_thread)
    if after_start:
        assert entered.wait(5)
        assert (
            _submit(owner, 1, lambda: pytest.fail("cannot replace original worker")) == "retained"
        )
        assert _submit(owner, 2, lambda: pytest.fail("physical capacity charged")) == "capacity"
        release.set()
        _joined(owner)
        (completion,) = owner.collect(owner=None, limit=1)
        assert completion.result() == "original"
    else:
        assert not entered.is_set()
        assert owner.collect(owner=None, limit=1) == ()
        assert _submit(owner, 2, lambda: "replacement") == "submitted"


def test_shutdown_joins_without_dropping_original_result_or_disposing_its_owner(owned):
    owner = owned.make()
    release = owned.event()
    entered = threading.Event()
    closed = threading.Event()

    class RetainedOwner:
        def close(self):
            pytest.fail("physical worker join does not grant semantic disposal")

    original = RetainedOwner()
    assert _submit(owner, 1, lambda: _blocked(entered, release, original)) == "submitted"
    assert entered.wait(5)

    def shutdown():
        closed.set()
        owner.close()

    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(shutdown)
        assert closed.wait(5)
        assert not pending.done()
        assert owner._slots[1].worker.is_alive()
        release.set()
        pending.result(timeout=5)
    assert _submit(owner, 2, lambda: pytest.fail("closed owner")) == "closed"
    (result,) = owner.collect(owner=None, limit=1)
    assert result.result() is original
    owner.close()


def test_one_service_owns_configured_capacity_and_joins_before_dependency_shutdown(tmp_path, owned):
    config = RuntimeConfig.for_testing(
        database_url="sqlite://",
        archive_execution=ArchiveExecutionTuning(forward_workers=1, terminal_workers=3),
    )
    service = SqlAlchemyCollectionUploadService(config, ArchiveStoreRegistry({}))
    owner = service._archive_execution
    owned_owner = owned.make()
    assert owner is not owned_owner
    assert owner._tuning is config.archive_execution
    release = owned.event()
    entered = threading.Event()
    closing = threading.Event()
    disposed = threading.Event()
    assert _submit(owner, 1, lambda: _blocked(entered, release, "original")) == "submitted"
    assert entered.wait(5)

    def shutdown():
        closing.set()
        service.close()
        disposed.set()

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(shutdown)
            assert closing.wait(5)
            assert not disposed.is_set()
            assert owner._slots[1].worker.is_alive()
            release.set()
            pending.result(timeout=5)
        assert disposed.is_set()
        (result,) = owner.collect(owner=None, limit=1)
        assert result.result() == "original"
        assert _submit(owner, 2, lambda: None) == "closed"
    finally:
        release.set()
        service.close()


@pytest.mark.parametrize(
    "field",
    [
        "forward_workers",
        "terminal_workers",
        "forward_workers_per_target",
        "terminal_workers_per_target",
        "retained_constructions",
        "retained_constructions_per_target",
        "receive_poll_interval_ms",
    ],
)
@pytest.mark.parametrize("value", [0, -1, True, 1.5, "2"])
def test_physical_capacity_requires_explicit_positive_integer_grants(field, value):
    with pytest.raises(ValueError, match=field):
        replace(ArchiveExecutionTuning(), **{field: value})


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_collection_quantum_rejects_invalid_limits(owned, limit):
    with pytest.raises(ValueError, match="limit"):
        owned.make().collect(owner=None, limit=limit)


def test_point_status_tracks_local_execution_without_claiming_global_closure(owned):
    owner = owned.make()
    assert owner.status(_key()) == "untracked"
    release = owned.event()
    entered = threading.Event()
    assert _submit(owner, 1, lambda: _blocked(entered, release, "original")) == "submitted"
    assert entered.wait(5)
    assert owner.status(_key()) == "running"
    assert owner._slots[1].worker.daemon is False
    with pytest.raises(Conflict, match="original construction"):
        owner.status(_key(digest="b" * 64))
    with owner._lock:
        assert owner.status(_key()) == "busy"
    release.set()
    _joined(owner)
    assert owner.status(_key()) == "awaiting-join"
    (completion,) = owner.collect(owner=None, limit=1)
    assert completion.result() == "original"
    assert owner.status(_key()) == "untracked"


def test_missing_original_outcome_stays_charged_without_losing_independent_results(
    owned, monkeypatch
):
    owner = owned.make(forward_workers=3, forward_workers_per_target=3)
    assert _submit(owner, 1, lambda: "first original") == "submitted"
    actual_run = source._run
    monkeypatch.setattr(source, "_run", lambda work, future: None)
    assert _submit(owner, 2, lambda: pytest.fail("fixture bypasses this callback")) == "submitted"
    monkeypatch.setattr(source, "_run", actual_run)
    assert _submit(owner, 3, lambda: "third original") == "submitted"
    _joined(owner)
    assert owner.status(_key(2)) == "outcome-missing"
    results = owner.collect(owner=None, limit=3)
    assert [(r.key.collection_id, r.result()) for r in results] == [
        (1, "first original"),
        (3, "third original"),
    ]
    assert owner.status(_key(2)) == "outcome-missing"
    assert _submit(owner, 2, lambda: pytest.fail("cannot replace missing original")) == "retained"
    assert owner.collect(owner=None, limit=3) == ()


@pytest.mark.parametrize("collection_id", [0, -1, True, "1"])
def test_execution_identity_requires_the_original_collection(collection_id):
    with pytest.raises(ValueError, match="collection identity"):
        _key(collection_id)


@pytest.mark.parametrize("digest", ["a" * 63, "g" * 64, "A" * 64, ""])
def test_execution_identity_requires_the_exact_original_digest(digest):
    with pytest.raises(ValueError, match="original construction digest"):
        _key(digest=digest)


def test_receiver_identity_keeps_foreign_results_and_capacity_charged(owned, monkeypatch):
    class EqualOwner:
        def __eq__(self, other):
            pytest.fail("receiving compares exact owner identity, never equality")

    pool = owned.make(forward_workers=2, forward_workers_per_target=2)
    first, second = EqualOwner(), EqualOwner()
    foreign_result, own_result = object(), object()
    assert (
        pool.try_submit(
            _key(1),
            target_incarnation="target-a",
            terminal=False,
            owner=second,
            work=lambda: foreign_result,
        )
        == "submitted"
    )
    assert (
        pool.try_submit(
            _key(2),
            target_incarnation="target-a",
            terminal=False,
            owner=first,
            work=lambda: own_result,
        )
        == "submitted"
    )
    _joined(pool)
    foreign = pool._slots[1]

    def forbidden_join(timeout=None):
        pytest.fail("another receiver cannot join or retire this original")

    with monkeypatch.context() as patch:
        patch.setattr(foreign.worker, "join", forbidden_join)
        assert pool._counts[False] == 2 and pool.status(_key(1)) == "awaiting-join"
        (received,) = pool.collect(limit=1, owner=first)
        assert received.key == _key(2) and received.owner is first
        assert received.result() is own_result
        assert pool.collect(limit=2, owner=None) == ()
        assert pool.status(_key(1)) == "awaiting-join"
    assert pool._counts[False] == 1
    assert (
        pool.try_submit(
            _key(3),
            target_incarnation="target-b",
            terminal=False,
            owner=first,
            work=lambda: "another turn",
        )
        == "submitted"
    )
    assert (
        _submit(pool, 4, lambda: pytest.fail("foreign result still charges capacity")) == "capacity"
    )
    (received,) = pool.collect(limit=2, owner=second)
    assert received.key == _key(1) and received.owner is second
    assert received.result() is foreign_result


def test_foreign_failed_result_waits_for_its_original_receiver_after_shutdown(owned):
    pool = owned.make()
    original = object()
    failure = SystemExit("synthetic owner-specific failure")

    def fail():
        raise failure

    assert (
        pool.try_submit(
            _key(),
            target_incarnation="target-a",
            terminal=True,
            owner=original,
            work=fail,
        )
        == "submitted"
    )
    pool.close()
    assert pool.collect(limit=1, owner=object()) == ()
    assert pool.collect(limit=1, owner=None) == ()
    assert pool.status(_key()) == "awaiting-join" and pool._counts[True] == 1
    (completion,) = pool.collect(limit=1, owner=original)
    assert completion.owner is original and completion.terminal
    with pytest.raises(SystemExit) as received:
        completion.result()
    assert received.value is failure
    assert pool.status(_key()) == "untracked"


def test_each_receiving_quantum_is_bounded_across_foreign_and_stuck_work(owned):
    pool = owned.make(forward_workers=3, forward_workers_per_target=3)
    foreign, current = object(), object()
    release = owned.event()
    entered = threading.Event()
    assert (
        pool.try_submit(
            _key(1),
            target_incarnation="target-a",
            terminal=False,
            owner=foreign,
            work=lambda: "foreign",
        )
        == "submitted"
    )
    assert (
        pool.try_submit(
            _key(2),
            target_incarnation="target-a",
            terminal=False,
            owner=current,
            work=lambda: _blocked(entered, release),
        )
        == "submitted"
    )
    assert entered.wait(5)
    assert (
        pool.try_submit(
            _key(3),
            target_incarnation="target-a",
            terminal=False,
            owner=current,
            work=lambda: "independent",
        )
        == "submitted"
    )
    pool._slots[3].worker.join(timeout=5)
    assert not pool._slots[3].worker.is_alive()
    pool._slots[1].worker.join(timeout=5)
    assert not pool._slots[1].worker.is_alive()
    assert pool.collect(limit=1, owner=current) == ()
    (completion,) = pool.collect(limit=1, owner=current)
    assert completion.key == _key(3) and completion.result() == "independent"
    assert pool.status(_key(1)) == "awaiting-join"
    assert pool.status(_key(2)) == "running"


def test_same_receiver_cannot_adopt_another_original_invocation(owned):
    pool = owned.make()
    receiver, original, another = object(), object(), object()
    calls = []
    assert (
        pool.try_submit(
            _key(),
            target_incarnation="target-a",
            terminal=False,
            owner=receiver,
            identity=original,
            work=lambda: calls.append("original"),
        )
        == "submitted"
    )
    with pytest.raises(ArchiveExecutionOccupied):
        pool.try_submit(
            _key(),
            target_incarnation="target-a",
            terminal=False,
            owner=receiver,
            identity=another,
            work=lambda: pytest.fail("another invocation cannot adopt the original slot"),
        )
    assert (
        pool.try_submit(
            _key(),
            target_incarnation="target-a",
            terminal=False,
            owner=receiver,
            identity=original,
            work=lambda: pytest.fail("a retained original cannot dispatch twice"),
        )
        == "retained"
    )
    _joined(pool)
    (completion,) = pool.collect(limit=1, owner=receiver)
    assert completion.identity is original and calls == ["original"]
    assert (
        pool.try_submit(
            _key(),
            target_incarnation="target-a",
            terminal=False,
            owner=receiver,
            identity=another,
            work=lambda: "another",
        )
        == "submitted"
    )
    _joined(pool)
    (completion,) = pool.collect(limit=1, owner=receiver)
    assert completion.identity is another and completion.result() == "another"


@pytest.mark.parametrize("foreign_count", (0, 1, 4))
def test_receivers_make_independent_progress_with_interleaved_bounded_polls(owned, foreign_count):
    ready_count = 3
    pool = owned.make(
        forward_workers=1 + ready_count + foreign_count,
        forward_workers_per_target=1 + ready_count + foreign_count,
    )
    receiver = object()
    foreign_receivers = [object() for _ in range(foreign_count)]
    empty_receiver = object()
    release, entered = owned.event(), threading.Event()
    assert (
        pool.try_submit(
            _key(1),
            target_incarnation="target-a",
            terminal=False,
            owner=receiver,
            work=lambda: _blocked(entered, release),
        )
        == "submitted"
    )
    assert entered.wait(5)
    for number in range(2, ready_count + 2):
        assert (
            pool.try_submit(
                _key(number),
                target_incarnation="target-a",
                terminal=False,
                owner=receiver,
                work=lambda number=number: number,
            )
            == "submitted"
        )
        pool._slots[number].worker.join(timeout=5)
        assert not pool._slots[number].worker.is_alive()
    for offset, foreign in enumerate(foreign_receivers, start=ready_count + 2):
        assert (
            pool.try_submit(
                _key(offset),
                target_incarnation="target-a",
                terminal=False,
                owner=foreign,
                work=lambda: "foreign",
            )
            == "submitted"
        )
        pool._slots[offset].worker.join(timeout=5)
        assert not pool._slots[offset].worker.is_alive()
    received = []
    for _ in range(ready_count + 1):
        received.extend(pool.collect(limit=1, owner=receiver))
        assert pool.collect(limit=1, owner=empty_receiver) == ()
        for foreign in foreign_receivers:
            for completion in pool.collect(limit=1, owner=foreign):
                assert completion.owner is foreign and completion.result() == "foreign"
    assert [completion.result() for completion in received] == list(range(2, ready_count + 2))
    assert pool.status(_key(1)) == "running"
    assert pool._counts[False] == 1
