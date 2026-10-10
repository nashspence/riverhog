"""Concurrent reuse of exact immutable reads preserves fixity and retry behavior."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock

import pytest
from riverhog_core.provenance_read_cache import ProvenanceReadCache


@pytest.mark.parametrize("failed", [False, True])
def test_concurrent_cold_reads_share_one_verified_load_and_failure_is_retryable(failed):
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

    def follower():
        nonlocal followers
        with lock:
            followers += 1
            if followers == 8:
                followers_started.set()
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
