from __future__ import annotations

import hashlib

import pytest
from riverhog_core.stores.storage_adapter_retrieval_cache import StorageAdapterRetrievalCache
from riverhog_core.throughput import ArchiveThroughputTuning, ArchiveTransferResources
from riverhog_storage_adapter_protocol import (
    BeginWriteOperation,
    CompletedWriteLookupRequest,
    CompleteWriteOperation,
    LookupWriteOperation,
    WriteCompleteRequest,
    WritePartOperation,
    WriteSegmentListRequest,
    WriteSegmentRequest,
    WriteSession,
)

from tests.unit.test_archive_write_execution import _executor, _filesystem, _invocation
from tests.unit.test_storage_adapter_retrieval_cache import _Adapter, _cache

CONTENT = b"abc"
SHA = hashlib.sha256(CONTENT).hexdigest()
IDENTITY = {
    "source_store": "original-archive",
    "collection_id": 7,
    "object_id": "original-object",
    "expected_bytes": len(CONTENT),
}


def test_cache_population_request_is_pure_and_matches_the_actual_begin():
    adapter = _Adapter()
    cache = _cache(adapter)
    request = cache.population_write_request(**IDENTITY)
    assert adapter.created is None and not adapter.objects and not adapter.deleted
    assert request.object_path == cache.object_path(
        IDENTITY["source_store"], IDENTITY["collection_id"], IDENTITY["object_id"]
    )
    assert request.placement_policy == "immediate_default"
    cache.begin_population(**IDENTITY)
    assert adapter.created == request


def _candidate(adapter):
    tuning = ArchiveThroughputTuning()
    return StorageAdapterRetrievalCache(
        "original-cache",
        adapter,
        write_segment_bytes=adapter.descriptor().minimum_nonfinal_segment_bytes,
        throughput_tuning=tuning,
        transfer_resources=ArchiveTransferResources.from_tuning(tuning),
    )


def _operation(adapter, request, kind):
    if kind == "begin":
        return BeginWriteOperation(request=request)
    session = adapter.begin_write(request)
    if kind == "part":
        return WritePartOperation(
            request=WriteSegmentRequest(session=session, number=1, stored_bytes=len(CONTENT)),
            stored_sha256=SHA,
        )
    adapter.write_segment(session=session, number=1, stored_bytes=len(CONTENT), content=CONTENT)
    completion = adapter.list_segments(WriteSegmentListRequest(session=session)).completion
    assert completion is not None
    complete = WriteCompleteRequest(
        session=session,
        completion=completion,
        expected_bytes=len(CONTENT),
        expected_content_type=request.content_type,
        required_identity_assertions=request.required_identity_assertions,
        expected_placement_policy=request.placement_policy,
    )
    if kind == "complete":
        return CompleteWriteOperation(request=complete)
    adapter.complete_write(complete)
    return LookupWriteOperation(
        request=CompletedWriteLookupRequest(
            object_path=request.object_path,
            expected_bytes=len(CONTENT),
            expected_content_type=request.content_type,
            required_identity_assertions=request.required_identity_assertions,
            expected_placement_policy=request.placement_policy,
        )
    )


@pytest.mark.parametrize("mode", ["direct", "owned-http", "supplied"])
@pytest.mark.parametrize("kind", ["begin", "part", "complete", "lookup"])
def test_actual_cache_executor_keeps_original_identity_and_transport_closure(tmp_path, mode, kind):
    with _filesystem(tmp_path / "cache") as adapter, _executor(adapter, mode) as executor:
        cache = _candidate(executor)
        request = cache.population_write_request(**IDENTITY)
        operation = _operation(adapter, request, kind)
        invocation = _invocation(operation, cache.incarnation_id)
        result = cache.execute_write(invocation, content=CONTENT if kind == "part" else b"")
        assert result.call == invocation.call
        assert result.execution == ("unknown" if mode == "supplied" else "closed")
        assert result.outcome == {"begin": "session", "part": "part"}.get(kind, "completed")
        assert result.result is not None
        if isinstance(result.result, WriteSession):
            assert result.result.object_path == request.object_path
            assert adapter.begin_write(request) == result.result
        else:
            assert result.result.stored_bytes == len(CONTENT)
            if kind == "part":
                assert result.result.number == 1
            else:
                assert result.result.object_path == request.object_path
                recovered = cache.find_completed_population(**IDENTITY)
                assert recovered is not None and recovered.object_path == request.object_path


@pytest.mark.parametrize("mode", ["direct", "owned-http", "supplied"])
def test_cache_negative_lookup_preserves_its_own_execution_status(tmp_path, mode):
    with _filesystem(tmp_path / "cache") as adapter, _executor(adapter, mode) as executor:
        cache = _candidate(executor)
        request = cache.population_write_request(**IDENTITY)
        lookup = LookupWriteOperation(
            request=CompletedWriteLookupRequest(
                object_path=request.object_path,
                expected_bytes=len(CONTENT),
                expected_content_type=request.content_type,
                required_identity_assertions=request.required_identity_assertions,
                expected_placement_policy=request.placement_policy,
            )
        )
        invocation = _invocation(lookup, cache.incarnation_id)
        result = cache.execute_write(invocation)
        assert result.call == invocation.call
        assert result.outcome == ("unknown" if mode == "supplied" else "no_effect")
        assert result.execution == ("unknown" if mode == "supplied" else "closed")
        assert result.result is None
        assert not list((tmp_path / "cache" / "writes").iterdir())


@pytest.mark.parametrize("changed", ["request", "incarnation"])
def test_cache_mismatched_original_request_or_target_never_dispatches(tmp_path, changed):
    with _filesystem(tmp_path / "cache") as adapter:
        cache = _candidate(adapter)
        operation = BeginWriteOperation(request=cache.population_write_request(**IDENTITY))
        invocation = _invocation(operation, cache.incarnation_id)
        if changed == "request":
            other = BeginWriteOperation(
                request=cache.population_write_request(**{**IDENTITY, "object_id": "other"})
            )
            invocation = invocation.model_copy(update={"operation": other})
        else:
            invocation = invocation.model_copy(
                update={
                    "call": invocation.call.model_copy(
                        update={"target_incarnation": "00000000-0000-4000-8000-000000000001"}
                    )
                }
            )
            assert invocation.call.target_incarnation != cache.incarnation_id
        result = cache.execute_write(invocation)
        assert result.execution == "unknown" and result.result is None
        assert not list((tmp_path / "cache" / "writes").iterdir())
