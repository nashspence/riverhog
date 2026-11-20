from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.services.archive_finalizer_progress import ArchiveFinalizerProgress


class Original:
    has_pending = False

    def drain_terminal(self, operation_id):
        pytest.fail("terminal cannot enter an active feedback turn")


@pytest.mark.parametrize("progressed", (False, True))
def test_feedback_retries_preserve_the_exact_false_or_true_original_result(progressed):
    original = Original()
    advances, received = [], []

    def advance():
        advances.append("original")
        return progressed

    def receive(value):
        received.append(value)
        if len(received) < 3:
            raise OSError("synthetic feedback acknowledgement")

    owner = ArchiveFinalizerProgress(
        None, collection_id=1, original=original, advance=advance, receive=receive
    )
    for _ in range(2):
        with pytest.raises(OSError, match="feedback acknowledgement"):
            owner.advance()
        assert owner.has_pending and not original.has_pending
    assert owner.advance() is progressed and not owner.has_pending
    assert advances == ["original"] and received == [progressed] * 3


def test_original_child_failure_precedes_feedback_and_keeps_its_pending_state():
    original = Original()
    calls, received = [], []

    def advance():
        calls.append("original")
        original.has_pending = len(calls) == 1
        if original.has_pending:
            raise OSError("synthetic original outcome")
        return True

    owner = ArchiveFinalizerProgress(
        None, collection_id=1, original=original, advance=advance, receive=received.append
    )
    with pytest.raises(OSError, match="original outcome"):
        owner.advance()
    assert owner.has_pending and received == []
    assert owner.advance() is True and not owner.has_pending
    assert calls == ["original", "original"] and received == [True]


def test_terminal_poll_does_not_wait_on_an_active_original_feedback_receiver():
    original = Original()
    entered, release = threading.Event(), threading.Event()

    def receive(value):
        entered.set()
        assert release.wait(10)

    owner = ArchiveFinalizerProgress(
        None, collection_id=1, original=original, advance=lambda: False, receive=receive
    )
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            active = pool.submit(owner.advance)
            assert entered.wait(5)
            assert owner.has_pending
            assert owner.drain_terminal("terminal") == "busy"
            release.set()
            assert active.result(timeout=5) is False
    finally:
        release.set()
    assert not owner.has_pending


def test_failed_selection_keeps_its_workflow_turn_before_a_child_has_pending_work():
    original = Original()
    calls, received = [], []

    def advance():
        calls.append("original")
        if len(calls) == 1:
            raise OSError("selected workflow read failed before child construction")
        return False

    owner = ArchiveFinalizerProgress(
        None, collection_id=1, original=original, advance=advance, receive=received.append
    )
    with pytest.raises(OSError, match="workflow read"):
        owner.advance()
    assert owner.has_pending and not original.has_pending and received == []
    assert owner.advance() is False and not owner.has_pending
    assert calls == ["original", "original"] and received == [False]
