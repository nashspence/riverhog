"""Concurrent reuse of exact immutable reads preserves fixity and retry behavior."""

from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from threading import Event, Lock

import pytest
from riverhog_core import provenance_read_cache
from riverhog_core.provenance_read_cache import ProvenanceReadCache


@pytest.mark.parametrize("failed", [False, True])
def test_concurrent_cold_reads_share_one_verified_load_and_failure_is_retryable(
    failed, monkeypatch
):
    cache = ProvenanceReadCache(byte_budget=64, entry_budget=4)
    started, release, followers_started = Event(), Event(), Event()
    lock = Lock()
    reads = followers = 0

    def load():
        nonlocal reads
        with lock:
            reads += 1
        started.set()
        assert release.wait(5)
        if failed:
            raise ValueError("exact immutable read failed")
        return b"verified bytes"

    class LoadFuture(Future[bytes]):
        def result(self, timeout=None):
            nonlocal followers
            with lock:
                followers += 1
                if followers == 8:
                    followers_started.set()
            return super().result(timeout)

    monkeypatch.setattr(provenance_read_cache, "Future", LoadFuture)

    def follower():
        return cache.get_or_load("exact-root-and-object", load)

    with ThreadPoolExecutor(max_workers=9) as pool:
        owner = pool.submit(cache.get_or_load, "exact-root-and-object", load)
        assert started.wait(5)
        waiting = [pool.submit(follower) for _ in range(8)]
        assert followers_started.wait(5)
        release.set()
        for future in [owner, *waiting]:
            if failed:
                with pytest.raises(ValueError, match="exact immutable read failed"):
                    future.result(timeout=5)
            else:
                assert future.result(timeout=5) == b"verified bytes"
    assert reads == 1
    if failed:
        assert cache.get("exact-root-and-object") is None
        assert cache.get_or_load("exact-root-and-object", lambda: b"retried") == b"retried"
    else:
        assert cache.get_or_load("exact-root-and-object", lambda: pytest.fail("warm reread")) == (
            b"verified bytes"
        )


def test_read_sharing_does_not_serialize_different_objects_or_limit_valid_size():
    from threading import Barrier

    cache = ProvenanceReadCache(byte_budget=4, entry_budget=1)
    ready = Barrier(2)

    def load(value):
        ready.wait(timeout=5)
        return value

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(cache.get_or_load, "first", lambda: load(b"one"))
        second = pool.submit(cache.get_or_load, "second", lambda: load(b"two"))
        assert first.result(timeout=5) == b"one"
        assert second.result(timeout=5) == b"two"
    assert cache.get_or_load("larger", lambda: b"valid larger value") == b"valid larger value"
    assert cache.get("larger") is None


def test_unmaterialized_build_is_not_cached_and_a_later_result_can_be_reused():
    cache = ProvenanceReadCache(byte_budget=16, entry_budget=1)
    calls = 0

    def too_large():
        nonlocal calls
        calls += 1
        return None

    assert cache.get_or_load("large-valid-snapshot", too_large) is None
    assert cache.get_or_load("large-valid-snapshot", too_large) is None
    assert calls == 2
    assert cache.get_or_load("large-valid-snapshot", lambda: b"small") == b"small"
    assert cache.get_or_load("large-valid-snapshot", too_large) == b"small"
    assert calls == 2


def test_cancelled_builder_releases_waiters_and_can_be_retried(monkeypatch):
    cache = ProvenanceReadCache(byte_budget=16, entry_budget=1)
    started, waiting, release = Event(), Event(), Event()

    class WaitingFuture(Future):
        def result(self, timeout=None):
            waiting.set()
            return super().result(timeout)

    monkeypatch.setattr(provenance_read_cache, "Future", WaitingFuture)

    def cancelled():
        started.set()
        assert release.wait(5)
        raise CancelledError("builder stopped")

    with ThreadPoolExecutor(max_workers=2) as pool:
        owner = pool.submit(cache.get_or_load, "exact-selection", cancelled)
        assert started.wait(5)
        follower = pool.submit(cache.get_or_load, "exact-selection", lambda: b"unexpected")
        try:
            assert waiting.wait(5)
        finally:
            release.set()
        for future in (owner, follower):
            with pytest.raises(CancelledError):
                future.result(timeout=5)
    assert cache.get_or_load("exact-selection", lambda: b"retry") == b"retry"
