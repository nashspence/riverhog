from __future__ import annotations

from io import BytesIO

import a_riverhog_s3_store_lib.adapter as implementation
import pytest
from a_riverhog_s3_store_lib import S3StorageAdapter, S3StorageAdapterConfig
from a_riverhog_s3_store_lib.incarnation import marker_document
from botocore.exceptions import ClientError
from riverhog_storage_adapter_protocol import (
    CleanupCandidate,
    CleanupInventoryPage,
    CleanupInvocation,
    CleanupStepRequest,
    WriteCallIdentity,
    cleanup_step_sha256,
)

_INCARNATION = "00000000-0000-4000-8000-000000000001"


class _Model:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.disposed: set[str] = set()
        self.lose_delete_reply = False

    def get_object(self, **kwargs):
        self.calls.append("incarnation")
        return {"Body": BytesIO(marker_document(_INCARNATION))}

    def delete_object(self, **kwargs):
        self.calls.append("delete")
        self.disposed.add(kwargs["VersionId"])
        if self.lose_delete_reply:
            raise TimeoutError("modeled lost deletion reply")
        return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    def abort_multipart_upload(self, **kwargs):
        self.calls.append("abort")
        self.disposed.add(kwargs["UploadId"])
        return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    def list_parts(self, **kwargs):
        self.calls.append("verify")
        assert kwargs["MaxParts"] == 1
        raise ClientError(
            {"Error": {"Code": "NoSuchUpload"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
            "ListParts",
        )


def _invocation(
    candidates, *, grant: int = 16, in_flight: int = 2, call: int = 2, exhausted: bool = True
) -> CleanupInvocation:
    request = CleanupStepRequest(
        selected_terminal_sha256="1" * 64,
        inventory=CleanupInventoryPage(
            object_prefix="archive/",
            target_incarnation=_INCARNATION,
            candidates=candidates,
            scanned_items=len(candidates),
            exhausted=exhausted,
            continuation=None if exhausted else str(call),
        ),
        maximum_provider_requests=grant,
        maximum_control_bytes=1024 * 1024,
        maximum_in_flight=in_flight,
    )
    return CleanupInvocation(
        call=WriteCallIdentity(
            call_id=f"{call:064x}",
            exact_request_sha256=cleanup_step_sha256(request),
            target_incarnation=_INCARNATION,
            dispatch_owner="3" * 64,
        ),
        request=request,
    )


def _adapter(client: _Model) -> S3StorageAdapter:
    return S3StorageAdapter(
        client,
        S3StorageAdapterConfig(
            implementation_id="fixture.s3/v1",
            implementation_version="1",
            bucket="fixture-bucket",
            root_prefix="owned",
        ),
    )


@pytest.mark.parametrize("count", [16, 256, 4096])
def test_cleanup_steps_cover_every_selected_identity_with_linear_model_requests(
    monkeypatch: pytest.MonkeyPatch,
    count: int,
    record_property,
) -> None:
    client = _Model()
    adapter = _adapter(client)
    # This is a synchronous SDK model. Actual owned-SDK/HTTP profile qualification
    # is separate; this predicate does not establish external provider behavior.
    monkeypatch.setattr(
        implementation, "uses_single_attempt_dispatch", lambda value: value is client
    )
    client.calls.clear()
    kinds = ("object_revision", "deletion_marker", "open_write")
    candidates = tuple(
        CleanupCandidate(
            object_path=f"archive/{index:08d}", kind=kinds[index % 3], token=f"token-{index}"
        )
        for index in range(count)
    )
    pages = 0
    charged = 0
    for start in range(0, count, 7):
        selected = candidates[start : start + 7]
        invocation = _invocation(selected, call=start + 2, exhausted=start + 7 >= count)
        before = len(client.calls)
        result = adapter.execute_cleanup(invocation)
        assert result.execution == "closed" and result.result is not None
        assert result.result.selection_complete
        assert len(client.calls) - before + 1 == result.result.provider_requests <= 16
        charged += result.result.provider_requests
        pages += 1
    assert client.disposed == {item.token for item in candidates}
    expected = pages + sum(2 if item.kind == "open_write" else 1 for item in candidates)
    assert len(client.calls) == expected
    assert charged == expected + pages
    record_property("selected_identities", count)
    record_property("lower_provider_requests", len(client.calls))
    record_property("reserved_adapter_hops", pages)


def test_cleanup_defers_before_budget_exhaustion_and_stops_after_ambiguous_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _Model()
    adapter = _adapter(client)
    selected = tuple(
        CleanupCandidate(
            object_path=f"archive/{index}",
            kind="object_revision",
            token=f"token-{index}",
        )
        for index in range(3)
    )
    client.calls.clear()
    assert adapter.execute_cleanup(_invocation(selected)).outcome == "unknown"
    assert not client.calls
    monkeypatch.setattr(
        implementation, "uses_single_attempt_dispatch", lambda value: value is client
    )
    for options in ({"grant": 1}, {"in_flight": 1}):
        assert adapter.execute_cleanup(_invocation(selected, **options)).outcome == "unknown"
        assert not client.calls
    first = adapter.execute_cleanup(_invocation(selected, grant=3))
    assert first.result is not None and first.execution == "closed"
    assert [item.outcome for item in first.result.items] == ["disposed", "deferred", "deferred"]
    assert not first.result.exhausted and first.result.continuation is None
    client.calls.clear()
    client.lose_delete_reply = True
    uncertain = adapter.execute_cleanup(_invocation(selected, call=20))
    assert uncertain.execution == "unknown" and uncertain.result is not None
    assert [item.outcome for item in uncertain.result.items] == ["unknown", "deferred", "deferred"]
    assert client.calls == ["incarnation", "delete"]
    assert uncertain.result.provider_requests == 3
