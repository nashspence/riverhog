from __future__ import annotations

import threading

import pytest
from riverhog_core.services import physical_execution as source
from riverhog_core.services.physical_execution import (
    PhysicalExecutionKey,
    PhysicalExecutionOccupied,
    PhysicalWorkspaceExecutionKey,
)

from tests.unit.test_archive_upload_execution import _blocked, _joined
from tests.unit.test_archive_upload_execution import owned as owned


def key(number=1):
    return PhysicalWorkspaceExecutionKey(f"{number:032x}", "a" * 64)


def submit(pool, receiver, original, number=1, *, work=lambda: None, target="local-storage"):
    return pool.try_submit_workspace(
        key(number),
        target_incarnation=target,
        work=work,
        owner=receiver,
        identity=original,
    )


def test_workspace_and_archive_share_global_terminal_capacity(owned):
    pool = owned.make(terminal_workers=1)
    receiver, original = object(), object()
    release, entered = owned.event(), threading.Event()
    assert submit(pool, receiver, original, work=lambda: _blocked(entered, release)) == "submitted"
    assert entered.wait(5)
    assert (
        pool.try_submit(
            PhysicalExecutionKey(1, "a" * 64),
            target_incarnation="archive",
            terminal=True,
            work=lambda: pytest.fail("shared terminal allowance is held"),
        )
        == "capacity"
    )
    assert (
        pool.try_submit(
            PhysicalExecutionKey(1, "a" * 64),
            target_incarnation="archive",
            terminal=False,
            work=lambda: "independent forward",
        )
        == "submitted"
    )
    release.set()
    _joined(pool)
    (result,) = pool.collect_workspace(limit=1, owner=receiver)
    assert result.key == key() and result.owner is receiver and result.identity is original
    assert result.terminal and result.result() is None
    assert pool._counts[True] == 0
    assert pool.collect(limit=1, owner=None)[0].result() == "independent forward"


def test_exact_target_allowance_is_shared_across_typed_lanes(owned):
    pool = owned.make(terminal_workers=2, terminal_workers_per_target=1)
    receiver, original = object(), object()
    assert (
        submit(pool, receiver, original, work=lambda: "workspace", target="same-storage")
        == "submitted"
    )
    assert (
        pool.try_submit(
            PhysicalExecutionKey(1, "b" * 64),
            target_incarnation="same-storage",
            terminal=True,
            work=lambda: pytest.fail("target allowance remains held"),
        )
        == "target-capacity"
    )
    assert (
        pool.try_submit(
            PhysicalExecutionKey(1, "b" * 64),
            target_incarnation="other-storage",
            terminal=True,
            work=lambda: "archive",
        )
        == "submitted"
    )
    _joined(pool)
    assert pool.collect_workspace(limit=1, owner=receiver)[0].result() == "workspace"
    assert pool.collect(limit=1, owner=None)[0].result() == "archive"


@pytest.mark.parametrize("change", ("owner", "identity", "target"))
def test_workspace_original_cannot_be_adopted_or_replaced(owned, change):
    pool = owned.make()
    receiver, original = object(), object()
    assert submit(pool, receiver, original, work=lambda: "original") == "submitted"
    with pytest.raises(PhysicalExecutionOccupied):
        submit(
            pool,
            object() if change == "owner" else receiver,
            object() if change == "identity" else original,
            target="other" if change == "target" else "local-storage",
            work=lambda: pytest.fail("replacement dispatched"),
        )
    assert (
        submit(pool, receiver, original, work=lambda: pytest.fail("duplicate dispatched"))
        == "retained"
    )
    _joined(pool)
    assert pool.collect_workspace(limit=1, owner=object()) == ()
    (result,) = pool.collect_workspace(limit=1, owner=receiver)
    assert result.result() == "original" and result.identity is original


def test_workspace_future_completion_waits_for_actual_worker_join(owned, monkeypatch):
    actual_thread = threading.Thread
    release, returned = owned.event(), threading.Event()

    class LateExit(actual_thread):
        def run(self):
            super().run()
            returned.set()
            assert release.wait(10)

    monkeypatch.setattr(source, "Thread", LateExit)
    pool = owned.make(terminal_workers=1)
    receiver, original, value = object(), object(), object()
    assert submit(pool, receiver, original, work=lambda: value) == "submitted"
    assert returned.wait(5) and pool._slots[key()].future.done()
    assert pool.collect_workspace(limit=1, owner=receiver) == ()
    assert submit(pool, receiver, object(), 2) == "capacity"
    release.set()
    _joined(pool)
    slot = pool._slots[key()]
    actual_join = slot.worker.join
    joins = []

    def joined(timeout=None):
        joins.append(timeout)
        return actual_join(timeout)

    monkeypatch.setattr(slot.worker, "join", joined)
    (result,) = pool.collect_workspace(limit=1, owner=receiver)
    assert joins == [0] and result.result() is value
    assert submit(pool, receiver, object(), 2) == "submitted"


def test_workspace_rotates_past_stuck_work_and_foreign_receiving_is_independent(owned):
    pool = owned.make(terminal_workers=3, terminal_workers_per_target=3)
    receiver, foreign = object(), object()
    release, entered = owned.event(), threading.Event()
    assert submit(pool, receiver, object(), work=lambda: _blocked(entered, release)) == "submitted"
    assert entered.wait(5)
    assert submit(pool, receiver, object(), 2, work=lambda: "own") == "submitted"
    assert submit(pool, foreign, object(), 3, work=lambda: "foreign") == "submitted"
    pool._slots[key(2)].worker.join(timeout=5)
    pool._slots[key(3)].worker.join(timeout=5)
    assert pool.collect_workspace(limit=1, owner=receiver) == ()
    assert pool.collect_workspace(limit=1, owner=foreign)[0].result() == "foreign"
    assert pool.collect_workspace(limit=1, owner=receiver)[0].result() == "own"
    assert pool.status(key()) == "running"


def test_wrong_receiving_kind_cannot_remove_the_original_workspace(owned):
    pool = owned.make()
    receiver, original = object(), object()
    assert submit(pool, receiver, original, work=lambda: "original") == "submitted"
    _joined(pool)
    with pytest.raises(PhysicalExecutionOccupied, match="another execution kind"):
        pool.collect(limit=1, owner=receiver)
    assert pool.status(key()) == "awaiting-join" and pool._counts[True] == 1
    assert pool.collect_workspace(limit=1, owner=receiver)[0].result() == "original"


@pytest.mark.parametrize("after_start", (False, True))
def test_workspace_startup_uncertainty_preserves_started_execution(owned, monkeypatch, after_start):
    actual_thread = threading.Thread
    release, entered = owned.event(), threading.Event()

    class FailedStart(actual_thread):
        def start(self):
            if after_start:
                super().start()
            raise OSError("actual startup reply uncertain")

    monkeypatch.setattr(source, "Thread", FailedStart)
    pool = owned.make(terminal_workers=1)
    receiver, original = object(), object()
    with pytest.raises(OSError, match="startup reply"):
        submit(pool, receiver, original, work=lambda: _blocked(entered, release, "original"))
    monkeypatch.setattr(source, "Thread", actual_thread)
    if after_start:
        assert entered.wait(5)
        assert submit(pool, receiver, original) == "retained"
        assert submit(pool, receiver, object(), 2) == "capacity"
        release.set()
        _joined(pool)
        assert pool.collect_workspace(limit=1, owner=receiver)[0].result() == "original"
    else:
        assert pool.status(key()) == "untracked"
        assert submit(pool, receiver, original) == "submitted"


def test_workspace_failure_and_shutdown_preserve_original_receiver(owned):
    pool = owned.make()
    receiver, original = object(), object()
    failure = SystemExit("original local receiving failure")

    def failed():
        raise failure

    assert submit(pool, receiver, original, work=failed) == "submitted"
    pool.close()
    assert pool.collect_workspace(limit=1, owner=object()) == ()
    with pytest.raises(PhysicalExecutionOccupied):
        pool.collect(limit=1, owner=receiver)
    (result,) = pool.collect_workspace(limit=1, owner=receiver)
    with pytest.raises(SystemExit) as observed:
        result.result()
    assert observed.value is failure and result.identity is original
    result.release_exception_frames()
    assert submit(pool, receiver, object(), 2) == "closed"


@pytest.mark.parametrize(
    "field,value",
    (
        ("workspace_id", "A" * 32),
        ("workspace_id", "f" * 31),
        ("workspace_id", 1),
        ("scope_sha256", "g" * 64),
        ("scope_sha256", ""),
    ),
)
def test_workspace_key_requires_exact_original_controls(field, value):
    controls = dict(workspace_id="a" * 32, scope_sha256="b" * 64)
    controls[field] = value
    with pytest.raises(ValueError):
        PhysicalWorkspaceExecutionKey(**controls)


@pytest.mark.parametrize("missing", ("owner", "identity"))
def test_workspace_requires_retained_receiver_and_original(owned, missing):
    arguments = dict(
        key=key(), owner=object(), identity=object(), target_incarnation="local", work=lambda: None
    )
    arguments[missing] = None
    with pytest.raises(ValueError):
        owned.make().try_submit_workspace(**arguments)
