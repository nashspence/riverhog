from __future__ import annotations

import ast
import hashlib
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

import pytest
from a_riverhog_s3_store_lib import (
    S3ClientConfig,
    S3StorageAdapter,
    S3StorageAdapterConfig,
    S3TransportTuning,
    create_s3_client,
)
from a_riverhog_s3_store_lib.incarnation import StorageIncarnationError, marker_document, marker_key
from botocore.exceptions import ClientError
from riverhog_storage_adapter_protocol import (
    CleanupInventoryRequest,
    CompletedWriteLookupRequest,
    DeleteObjectRequest,
    DeletePrefixRequest,
    ObjectHeadRequest,
    ObjectLocator,
    ObjectReadRequest,
    OpenWriteListRequest,
    ReadPreparationRequest,
    ReadReadiness,
    ReadReady,
    ReadRequested,
    SmallObjectWriteRequest,
    StorageAdapterRejection,
    WriteCompleteRequest,
    WriteSegmentListRequest,
    WriteSession,
    WriteStartRequest,
)


def _client_error(code: str, status: int, operation: str) -> ClientError:
    return ClientError(
        cast(
            Any,
            {
                "Error": {"Code": code, "Message": code},
                "ResponseMetadata": {"HTTPStatusCode": status},
            },
        ),
        operation,
    )


class _Body(BytesIO):
    def iter_chunks(self, *, chunk_size: int) -> Any:
        while chunk := self.read(chunk_size):
            yield chunk


def _read_request_body(body: object) -> bytes:
    if isinstance(body, bytes):
        return body
    read = getattr(body, "read", None)
    if not callable(read):
        raise TypeError("request body is not readable")
    chunks: list[bytes] = []
    while chunk := read(64 * 1024):
        chunks.append(bytes(chunk))
    return b"".join(chunks)


class _Paginator:
    def __init__(self, client: _FakeS3Client, operation: str) -> None:
        self.client = client
        self.operation = operation

    def paginate(self, *, Bucket: str, Prefix: str) -> tuple[dict[str, object], ...]:
        assert Bucket == "fixture-bucket"
        if self.operation == "list_objects_v2":
            return (
                {
                    "Contents": [
                        {"Key": key}
                        for key in sorted(self.client.current)
                        if key.startswith(Prefix)
                    ]
                },
            )
        if self.operation == "list_object_versions":
            return (
                {
                    "Versions": [
                        {"Key": key, "VersionId": version}
                        for (key, version) in sorted(self.client.versions)
                        if key.startswith(Prefix)
                    ],
                    "DeleteMarkers": [],
                },
            )
        raise AssertionError(self.operation)


class _FakeS3Client:
    def __init__(self) -> None:
        self.incarnation_id = "00000000-0000-4000-8000-000000000001"
        self.current: dict[str, str] = {}
        self.versions: dict[tuple[str, str], dict[str, Any]] = {}
        self.uploads: dict[str, dict[str, Any]] = {}
        self.next_upload = 1
        self.next_version = 1
        self.get_attempts = 0
        self.deleted: list[dict[str, object]] = []

    def _store(self, request: dict[str, Any], content: bytes) -> dict[str, str]:
        key = str(request["Key"])
        version = f"v{self.next_version}"
        self.next_version += 1
        value = {
            "Body": content,
            "ContentLength": len(content),
            "ContentType": request.get("ContentType"),
            "Metadata": dict(request.get("Metadata") or {}),
            "StorageClass": request.get("StorageClass"),
            "ETag": f'"etag-{version}"',
            "LastModified": datetime(2026, 1, 1, tzinfo=UTC),
            "VersionId": version,
        }
        self.versions[(key, version)] = value
        self.current[key] = version
        return {"VersionId": version}

    def create_multipart_upload(self, **request: Any) -> dict[str, str]:
        upload_id = f"upload-{self.next_upload}"
        self.next_upload += 1
        self.uploads[upload_id] = {
            "request": request,
            "parts": {},
            "Initiated": datetime(2026, 1, 1, tzinfo=UTC),
        }
        return {"UploadId": upload_id}

    def upload_part(self, **request: Any) -> dict[str, str]:
        upload = self.uploads[str(request["UploadId"])]
        number = int(request["PartNumber"])
        content = _read_request_body(request["Body"])
        assert len(content) == int(request["ContentLength"])
        upload["parts"][number] = content
        return {"ETag": f'"part-{number}"'}

    def list_parts(self, **request: Any) -> dict[str, object]:
        parts = self.uploads[str(request["UploadId"])]["parts"]
        return {
            "IsTruncated": False,
            "Parts": [
                {"PartNumber": number, "ETag": f'"part-{number}"', "Size": len(content)}
                for number, content in sorted(parts.items())
            ],
        }

    def complete_multipart_upload(self, **request: Any) -> dict[str, str]:
        key = str(request["Key"])
        if request.get("IfNoneMatch") == "*" and key in self.current:
            raise _client_error("PreconditionFailed", 412, "CompleteWriteSession")
        if request.get("IfMatch") is not None:
            current_version = self.current.get(key)
            current = None if current_version is None else self.versions.get((key, current_version))
            if current is None or current["ETag"] != request["IfMatch"]:
                raise _client_error("PreconditionFailed", 412, "CompleteWriteSession")
        upload_id = str(request["UploadId"])
        if upload_id not in self.uploads:
            raise _client_error("NoSuchUpload", 404, "CompleteWriteSession")
        upload = self.uploads.pop(upload_id)
        content = b"".join(upload["parts"][number] for number in sorted(upload["parts"]))
        return self._store(upload["request"], content)

    def abort_multipart_upload(self, **request: Any) -> None:
        self.uploads.pop(str(request["UploadId"]), None)

    def list_multipart_uploads(self, **request: Any) -> dict[str, object]:
        prefix = str(request["Prefix"])
        limit = int(request.get("MaxUploads", 1000))
        uploads = [
            {
                "Key": str(upload["request"]["Key"]),
                "UploadId": upload_id,
                "Initiated": upload["Initiated"],
            }
            for upload_id, upload in sorted(self.uploads.items())
            if str(upload["request"]["Key"]).startswith(prefix)
        ][:limit]
        return {
            "IsTruncated": False,
            "Uploads": uploads,
        }

    def head_object(self, **request: Any) -> dict[str, object]:
        key = str(request["Key"])
        version = str(request.get("VersionId") or self.current.get(key, ""))
        value = self.versions.get((key, version))
        if value is None:
            raise _client_error("NoSuchKey", 404, "HeadObject")
        return {name: current for name, current in value.items() if name != "Body"}

    def get_object(self, **request: Any) -> dict[str, object]:
        key = str(request["Key"])
        if key in {marker_key("owned"), marker_key("")}:
            document = marker_document(self.incarnation_id)
            return {"Body": _Body(document), "ContentLength": len(document)}
        self.get_attempts += 1
        version = str(request.get("VersionId") or self.current[key])
        content = bytes(self.versions[(key, version)]["Body"])
        if "Range" not in request:
            return {"Body": _Body(content), "ContentLength": len(content)}
        start, end = (
            int(value) for value in str(request["Range"]).removeprefix("bytes=").split("-")
        )
        selected = content[start : end + 1]
        return {
            "Body": _Body(selected),
            "ContentLength": len(selected),
            "ContentRange": f"bytes {start}-{end}/{len(content)}",
        }

    def delete_object(self, **request: Any) -> None:
        key = str(request["Key"])
        if request.get("IfMatch") is not None:
            current_version = self.current.get(key)
            current = None if current_version is None else self.versions.get((key, current_version))
            if current is None or current["ETag"] != request["IfMatch"]:
                raise _client_error("PreconditionFailed", 412, "DeleteObject")
        if request.get("VersionId") is not None:
            version = str(request["VersionId"])
            self.versions.pop((key, version), None)
            if self.current.get(key) == version:
                self.current.pop(key, None)
        else:
            self.current.pop(key, None)

    def delete_objects(self, **request: Any) -> None:
        for item in request["Delete"]["Objects"]:
            current = dict(item)
            self.deleted.append(current)
            key = str(current["Key"])
            if current.get("VersionId") is None:
                self.current.pop(key, None)
            else:
                version = str(current["VersionId"])
                self.versions.pop((key, version), None)
                if self.current.get(key) == version:
                    self.current.pop(key, None)

    def get_paginator(self, operation: str) -> _Paginator:
        return _Paginator(self, operation)


def _config(**overrides: Any) -> S3StorageAdapterConfig:
    values: dict[str, Any] = {
        "implementation_id": "fixture.s3/v1",
        "implementation_version": "1.0.0",
        "bucket": "fixture-bucket",
        "root_prefix": "owned",
        "archive_storage_class": "DEEP_ARCHIVE",
    }
    values.update(overrides)
    return S3StorageAdapterConfig(**values)


def _small_request(
    content: bytes,
    *,
    identity: str = "logical/v1",
    content_type: str = "application/octet-stream",
) -> SmallObjectWriteRequest:
    return SmallObjectWriteRequest(
        object_path="archives/collection/manifest.age",
        content_type=content_type,
        required_identity_assertions={"riverhog-logical-identity": identity},
        placement_policy="immediate_default",
        mode="create_only",
        stored_bytes=len(content),
        stored_sha256=hashlib.sha256(content).hexdigest(),
    )


def test_repointed_s3_incarnation_refuses_destructive_effect() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    content = b"historical archive"
    receipt = adapter.put_small_object(_small_request(content), content)
    client.incarnation_id = "00000000-0000-4000-8000-000000000002"

    with pytest.raises(StorageIncarnationError, match="incarnation changed"):
        adapter.delete_object(
            DeleteObjectRequest(
                object=ObjectLocator(object_path=receipt.object_path, revision=receipt.revision),
                mode="exact_revision",
            )
        )
    assert ("owned/archives/collection/manifest.age", receipt.revision or "") in client.versions


def test_small_object_retry_binds_exact_stored_identity_without_rereading_ciphertext() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    first_content = b"first randomized ciphertext"
    retry_content = b"different randomized ciphertext for the same plaintext authority"

    first = adapter.put_small_object(_small_request(first_content), first_content)
    second = adapter.put_small_object(_small_request(first_content), first_content)
    with pytest.raises(StorageAdapterRejection, match="different identity"):
        adapter.put_small_object(_small_request(retry_content), retry_content)

    assert second == first
    assert client.get_attempts == 0
    stored = client.versions[("owned/archives/collection/manifest.age", first.revision or "")]
    assert stored["Body"] == first_content
    assert first.stored_sha256 == hashlib.sha256(first_content).hexdigest()
    assert first.verified_content_type == "application/octet-stream"
    assert first.verified_identity_assertions == {"riverhog-logical-identity": "logical/v1"}
    assert first.verified_placement_policy == "immediate_default"


def test_small_object_rejects_a_changed_logical_identity() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    adapter.put_small_object(_small_request(b"first"), b"first")

    with pytest.raises(StorageAdapterRejection) as raised:
        adapter.put_small_object(
            _small_request(b"second", identity="different/v1"),
            b"second",
        )

    assert raised.value.code == "identity_conflict"


def test_small_object_rejects_a_changed_content_type() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    content = b"first"
    adapter.put_small_object(_small_request(content), content)

    with pytest.raises(StorageAdapterRejection) as raised:
        adapter.put_small_object(
            _small_request(content, content_type="application/vnd.example.other"),
            content,
        )

    assert raised.value.code == "identity_conflict"


class _RacingS3Client(_FakeS3Client):
    def __init__(self, *, operation: str) -> None:
        super().__init__()
        self.operation = operation
        self.raced = False

    def _replace_concurrently(self, key: str) -> None:
        if self.raced:
            return
        self.raced = True
        self._store(
            {
                "Key": key,
                "ContentType": "application/octet-stream",
                "Metadata": {},
                "StorageClass": None,
            },
            b"concurrent",
        )

    def complete_multipart_upload(self, **request: Any) -> dict[str, str]:
        if self.operation == "write" and request.get("IfMatch") is not None:
            self._replace_concurrently(str(request["Key"]))
        return super().complete_multipart_upload(**request)

    def delete_object(self, **request: Any) -> None:
        if self.operation == "delete" and request.get("IfMatch") is not None:
            self._replace_concurrently(str(request["Key"]))
        super().delete_object(**request)


@pytest.mark.parametrize("operation", ("write", "delete"))
def test_current_object_fence_rejects_a_change_between_head_and_mutation(
    operation: str,
) -> None:
    client = _RacingS3Client(operation=operation)
    adapter = S3StorageAdapter(client, _config())
    first = adapter.put_small_object(_small_request(b"first"), b"first")

    with pytest.raises(StorageAdapterRejection) as raised:
        if operation == "write":
            adapter.put_small_object(
                _small_request(b"replacement", identity="logical/v2").model_copy(
                    update={
                        "mode": "replace_current",
                        "expected_current_stored_sha256": first.stored_sha256,
                    }
                ),
                b"replacement",
            )
        else:
            adapter.delete_object(
                DeleteObjectRequest(
                    object=ObjectLocator(object_path="archives/collection/manifest.age"),
                    mode="current",
                    expected_current_stored_sha256=first.stored_sha256,
                )
            )

    assert raised.value.code == "identity_conflict"
    current_version = client.current["owned/archives/collection/manifest.age"]
    assert (
        client.versions[("owned/archives/collection/manifest.age", current_version)]["Body"]
        == b"concurrent"
    )


def test_small_object_streams_once_before_atomic_conditional_publication() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    consumed: list[bytes] = []

    def content() -> Any:
        for chunk in (b"cipher", b"text"):
            consumed.append(chunk)
            yield chunk

    receipt = adapter.put_small_object(_small_request(b"ciphertext"), content())

    assert receipt.stored_bytes == 10
    assert consumed == [b"cipher", b"text"]
    assert client.get_attempts == 0
    assert not client.uploads


def test_resumable_write_reconciles_segments_and_lost_completion() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    create = WriteStartRequest(
        object_path="archives/collection/volume.age",
        expected_bytes=adapter.descriptor().minimum_nonfinal_segment_bytes + 6,
        content_type="application/octet-stream",
        required_identity_assertions={"riverhog-plan-sha256": "a" * 64},
        placement_policy="archive_default",
    )
    session = adapter.begin_write(create)
    first_content = b"f" * adapter.descriptor().minimum_nonfinal_segment_bytes
    first_segment = adapter.write_segment(
        session=session,
        number=1,
        stored_bytes=len(first_content),
        content=first_content,
    )
    first_page = adapter.list_segments(WriteSegmentListRequest(session=session))
    persisted_session = WriteSession.model_validate_json(session.model_dump_json())
    restarted_adapter = S3StorageAdapter(client, _config())
    assert (
        restarted_adapter.write_segment(
            session=persisted_session,
            number=1,
            stored_bytes=len(first_content),
            content=first_content,
        )
        == first_segment
    )
    assert (
        restarted_adapter.list_segments(WriteSegmentListRequest(session=persisted_session))
        == first_page
    )
    segments = (
        first_segment,
        restarted_adapter.write_segment(
            session=persisted_session,
            number=2,
            stored_bytes=6,
            content=b"second",
        ),
    )
    segment_page = restarted_adapter.list_segments(
        WriteSegmentListRequest(session=persisted_session)
    )
    assert segment_page.traversal_token != first_page.traversal_token
    assert segment_page.segments == segments
    assert segment_page.completion is not None
    completion = WriteCompleteRequest(
        session=persisted_session,
        completion=segment_page.completion,
        expected_bytes=len(first_content) + 6,
        expected_content_type=create.content_type,
        required_identity_assertions=create.required_identity_assertions,
        expected_placement_policy=create.placement_policy,
    )
    altered_completion = completion.model_copy(
        update={
            "completion": completion.completion.model_copy(
                update={"state_token": "altered-active-write-state"}
            )
        }
    )
    with pytest.raises(StorageAdapterRejection) as altered:
        restarted_adapter.complete_write(altered_completion)
    assert altered.value.code == "identity_conflict"

    first = restarted_adapter.complete_write(completion)
    recovered = restarted_adapter.complete_write(completion)

    assert recovered == first
    assert restarted_adapter.complete_write(altered_completion) == first
    assert not hasattr(first, "stored_sha256")
    assert (
        restarted_adapter.find_completed_write(
            CompletedWriteLookupRequest(
                object_path=first.object_path,
                expected_bytes=first.stored_bytes,
                expected_content_type=create.content_type,
                required_identity_assertions=create.required_identity_assertions,
                expected_placement_policy=create.placement_policy,
            )
        )
        == first
    )
    head = client.versions[("owned/archives/collection/volume.age", first.revision or "")]
    assert head["StorageClass"] == "DEEP_ARCHIVE"
    assert first.verified_content_type == create.content_type
    assert head["Metadata"] == {
        "riverhog-adapter-placement-policy": "archive_default",
        "riverhog-plan-sha256": "a" * 64,
    }
    with pytest.raises(StorageAdapterRejection) as raised:
        restarted_adapter.find_completed_write(
            CompletedWriteLookupRequest(
                object_path=first.object_path,
                expected_bytes=first.stored_bytes,
                expected_content_type="application/vnd.example.other",
                required_identity_assertions=create.required_identity_assertions,
                expected_placement_policy=create.placement_policy,
            )
        )
    assert raised.value.code == "identity_conflict"


def test_resumable_write_lists_sparse_provider_state_after_restart() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    session = adapter.begin_write(
        WriteStartRequest(
            object_path="archives/collection/sparse.age",
            expected_bytes=adapter.descriptor().minimum_nonfinal_segment_bytes,
            content_type="application/octet-stream",
            required_identity_assertions={"riverhog-plan-sha256": "a" * 64},
            placement_policy="archive_default",
        )
    )
    persisted_session = WriteSession.model_validate_json(session.model_dump_json())
    restarted_adapter = S3StorageAdapter(client, _config())
    content = b"s" * restarted_adapter.descriptor().minimum_nonfinal_segment_bytes
    second = restarted_adapter.write_segment(
        session=persisted_session,
        number=2,
        stored_bytes=len(content),
        content=content,
    )

    assert restarted_adapter.list_segments(
        WriteSegmentListRequest(session=persisted_session)
    ).segments == (second,)


def test_identity_assertions_are_inert_while_placement_remains_explicit() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    identity = {
        "riverhog-format": "riverhog-pack-volume/v1",
        "riverhog-plan-sha256": "a" * 64,
    }

    immediate = adapter.begin_write(
        WriteStartRequest(
            object_path="cache/collection/volume.age",
            expected_bytes=1,
            content_type="application/octet-stream",
            required_identity_assertions=identity,
            placement_policy="immediate_default",
        )
    )
    archive = adapter.begin_write(
        WriteStartRequest(
            object_path="archives/collection/volume.age",
            expected_bytes=1,
            content_type="application/octet-stream",
            required_identity_assertions=identity,
            placement_policy="archive_default",
        )
    )

    immediate_request = client.uploads[immediate.write_token]["request"]
    archive_request = client.uploads[archive.write_token]["request"]
    assert immediate_request["Metadata"] == {
        "riverhog-adapter-placement-policy": "immediate_default",
        **identity,
    }
    assert archive_request["Metadata"] == {
        "riverhog-adapter-placement-policy": "archive_default",
        **identity,
    }
    assert "StorageClass" not in immediate_request
    assert archive_request["StorageClass"] == "DEEP_ARCHIVE"


def test_full_and_exact_range_reads_preserve_version_and_length() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    content = b"0123456789"
    receipt = adapter.put_small_object(_small_request(content), content)
    locator = ObjectLocator(
        object_path="archives/collection/manifest.age",
        revision=receipt.revision,
    )

    assert (
        b"".join(
            adapter.read_object(
                ObjectReadRequest(object=locator, expected_bytes=len(content))
            ).content
        )
        == content
    )
    assert (
        b"".join(
            adapter.read_object(
                ObjectReadRequest(
                    object=locator,
                    expected_bytes=len(content),
                    offset=3,
                    size=4,
                )
            ).content
        )
        == b"3456"
    )


def test_metadata_head_hides_adapter_markers_but_keeps_opaque_identity() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    content = b"ciphertext"
    receipt = adapter.put_small_object(_small_request(content), content)

    head = adapter.head_object(
        ObjectHeadRequest(
            object=ObjectLocator(
                object_path="archives/collection/manifest.age",
                revision=receipt.revision,
            ),
            expected_placement_policy="immediate_default",
        )
    )

    assert head is not None
    assert head.observed_identity_assertions == {"riverhog-logical-identity": "logical/v1"}
    assert head.verified_placement_policy == "immediate_default"
    assert head.stored_sha256 == hashlib.sha256(content).hexdigest()


def test_deletion_is_exact_and_prefix_cleanup_removes_all_versions() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    first = adapter.put_small_object(_small_request(b"first"), b"first")
    adapter.put_small_object(
        _small_request(b"second", identity="logical/v2").model_copy(
            update={
                "mode": "replace_current",
                "expected_current_stored_sha256": first.stored_sha256,
            }
        ),
        b"second",
    )

    adapter.delete_object(
        DeleteObjectRequest(
            object=ObjectLocator(
                object_path="archives/collection/manifest.age",
                revision=first.revision,
            ),
            mode="exact_revision",
        )
    )
    assert ("owned/archives/collection/manifest.age", first.revision or "") not in client.versions

    affected = adapter.delete_prefix(DeletePrefixRequest(object_prefix="archives/collection/"))

    assert affected >= 1
    assert not client.current
    assert not client.versions
    assert all(item["Key"] == "owned/archives/collection/manifest.age" for item in client.deleted)


def test_prefix_cleanup_cannot_delete_root_incarnation_marker() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config(root_prefix=""))
    for prefix in (".river", ".riverhog-storage-incarnation"):
        with pytest.raises(StorageAdapterRejection, match="reserved storage identity"):
            adapter.delete_prefix(DeletePrefixRequest(object_prefix=prefix))
    assert client.deleted == []


def test_fenced_current_removal_then_exact_reclamation_removes_data_revision() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    receipt = adapter.put_small_object(_small_request(b"obsolete-tag-node"), b"obsolete-tag-node")
    key = "owned/archives/collection/manifest.age"

    adapter.delete_object(
        DeleteObjectRequest(
            object=ObjectLocator(object_path=receipt.object_path),
            mode="current",
            expected_current_stored_sha256=receipt.stored_sha256,
        )
    )
    assert key not in client.current
    assert (key, receipt.revision or "") in client.versions

    restarted = S3StorageAdapter(client, _config())
    restarted.delete_object(
        DeleteObjectRequest(
            object=ObjectLocator(
                object_path=receipt.object_path,
                revision=receipt.revision,
            ),
            mode="exact_revision",
        )
    )
    assert (key, receipt.revision or "") not in client.versions


def test_read_preparation_mechanics_remain_adapter_private() -> None:
    class Preparation:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[tuple[str, str | None], ...]]] = []

        def prepare(self, **kwargs: Any) -> ReadReadiness:
            self.calls.append(("prepare", kwargs["objects"]))
            return ReadRequested(estimated_ready_at="2026-01-03T00:00:00.000000000Z")

        def status(self, **kwargs: Any) -> ReadReadiness:
            self.calls.append(("status", kwargs["objects"]))
            return ReadReady(available_until="2026-01-04T00:00:00.000000000Z")

        def cleanup(self, **kwargs: Any) -> None:
            self.calls.append(("cleanup", kwargs["objects"]))

    preparation = Preparation()
    client = _FakeS3Client()
    adapter = S3StorageAdapter(
        client,
        _config(read_mode="restore_required"),
        read_preparation=preparation,
    )
    placement_session = adapter.begin_write(
        WriteStartRequest(
            object_path="archives/collection/immediate-policy.age",
            expected_bytes=1,
            content_type="application/octet-stream",
            required_identity_assertions={},
            placement_policy="immediate_default",
        )
    )
    provider_request = client.uploads[placement_session.write_token]["request"]
    assert provider_request["Metadata"]["riverhog-adapter-placement-policy"] == "immediate_default"
    assert "StorageClass" not in provider_request
    assert adapter.descriptor().read_mode == "restore_required"
    request = ReadPreparationRequest(
        objects=(ObjectLocator(object_path="archives/collection/volume.age", revision="v1"),)
    )

    assert adapter.prepare_read(request).readiness.state == "requested"
    assert adapter.read_status(request).readiness.state == "ready"
    adapter.cleanup_read(request)

    assert preparation.calls == [
        ("prepare", (("owned/archives/collection/volume.age", "v1"),)),
        ("status", (("owned/archives/collection/volume.age", "v1"),)),
        ("cleanup", (("owned/archives/collection/volume.age", "v1"),)),
    ]
    schema = str(ReadPreparationRequest.model_json_schema()).casefold()
    assert "tier" not in schema
    assert "hold" not in schema
    assert "storage_class" not in schema


def test_lost_begin_response_reconciles_the_exact_nonterminal_write() -> None:
    client = _FakeS3Client()
    adapter = S3StorageAdapter(client, _config())
    session = adapter.begin_write(
        WriteStartRequest(
            object_path="archives/collection/volume.age",
            expected_bytes=1,
            content_type="application/octet-stream",
            required_identity_assertions={"riverhog-plan-sha256": "a" * 64},
            placement_policy="archive_default",
        )
    )

    restarted = S3StorageAdapter(client, _config())
    recovered = restarted.begin_write(
        WriteStartRequest(
            object_path="archives/collection/volume.age",
            expected_bytes=1,
            content_type="application/octet-stream",
            required_identity_assertions={"riverhog-plan-sha256": "a" * 64},
            placement_policy="archive_default",
        )
    )

    assert recovered == session
    assert tuple(client.uploads) == (session.write_token,)


def test_s3_transport_and_source_boundary_are_adapter_owned() -> None:
    tuning = S3TransportTuning(max_pool_connections=64)
    assert tuning.max_pool_connections == 64

    source = Path(__file__).resolve().parents[1] / "src/a_riverhog_s3_store_lib"
    imported: set[str] = set()
    for path in source.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imported.add(node.module)
    assert all(not name.startswith("riverhog_core") for name in imported)
    assert all(not name.startswith("riverhog_storage_adapter_support") for name in imported)


def test_s3_client_preserves_one_pass_request_bodies() -> None:
    client = create_s3_client(
        S3ClientConfig(
            endpoint_url="https://s3.example.test",
            region="example-1",
            access_key_id="example-access-key",
            secret_access_key="example-secret-key",
        )
    )

    assert client.meta.config.request_checksum_calculation == "when_required"
    assert client.meta.config.s3["payload_signing_enabled"] is False


class _OpenWritePageClient(_FakeS3Client):
    def __init__(self, count: int) -> None:
        super().__init__()
        self.rows = [
            {"Key": "owned/archive/volume", "UploadId": f"token-{index}"} for index in range(count)
        ] + [
            {"Key": "owned/archive/volume-extra", "UploadId": f"foreign-token-{index}"}
            for index in range(7)
        ]
        self.positions = {
            (row["Key"], row["UploadId"]): index for index, row in enumerate(self.rows)
        }
        self.list_requests: list[dict[str, Any]] = []
        self.marker_reads = 0
        self.response_override: dict[str, Any] | None = None

    def get_object(self, **request: Any) -> dict[str, object]:
        self.marker_reads += 1
        return super().get_object(**request)

    def list_multipart_uploads(self, **request: Any) -> dict[str, object]:
        self.list_requests.append(request)
        assert request["Prefix"] == "owned/archive/volume"
        if self.response_override is not None:
            return {"EncodingType": "url", **self.response_override}
        marker = (request.get("KeyMarker"), request.get("UploadIdMarker"))
        offset = self.positions[marker] + 1 if marker[0] is not None else 0
        selected = self.rows[offset : offset + request["MaxUploads"]]
        more = offset + len(selected) < len(self.rows)
        response: dict[str, object] = {
            "EncodingType": "url",
            "Uploads": selected,
            "IsTruncated": more,
        }
        if more:
            response["NextKeyMarker"] = selected[-1]["Key"]
            response["NextUploadIdMarker"] = selected[-1]["UploadId"]
        return response

    def create_multipart_upload(self, **request: Any) -> dict[str, str]:
        raise AssertionError("observation must never begin a write")

    def abort_multipart_upload(self, **request: Any) -> None:
        raise AssertionError("observation must never abort a write")


@pytest.mark.parametrize("count", [1, 16, 256, 4096])
def test_open_write_observation_pages_every_exact_token_with_linear_provider_calls(
    count: int,
) -> None:
    client = _OpenWritePageClient(count)
    adapter = S3StorageAdapter(client, _config())
    request = OpenWriteListRequest(
        object_path="archive/volume", target_incarnation=client.incarnation_id, maximum_items=3
    )
    tokens: list[str] = []
    scanned = 0
    pages = 0
    zero_match_continuations = 0
    while True:
        # A fresh adapter instance consumes only provider-owned continuation state.
        adapter = S3StorageAdapter(client, _config())
        marker_reads = client.marker_reads
        list_calls = len(client.list_requests)
        page = adapter.list_open_writes(request)
        assert client.marker_reads == marker_reads + 1
        assert len(client.list_requests) == list_calls + 1
        assert all(item.begin_request_sha256 is None for item in page.observations)
        tokens.extend(item.write_token for item in page.observations)
        assert page.scanned_items <= 3 and len(page.observations) <= page.scanned_items
        scanned += page.scanned_items
        pages += 1
        zero_match_continuations += int(not page.observations and not page.exhausted)
        if page.exhausted:
            break
        request = request.model_copy(update={"continuation": page.continuation})
    assert tokens == [f"token-{index}" for index in range(count)]
    assert scanned == count + 7
    assert pages == (count + 9) // 3
    assert zero_match_continuations >= 1
    assert len(client.list_requests) == pages
    assert not client.uploads and not client.deleted


def test_open_write_continuation_is_bound_to_exact_target_and_rejects_bad_pages() -> None:
    client = _OpenWritePageClient(5)
    adapter = S3StorageAdapter(client, _config())
    request = OpenWriteListRequest(
        object_path="archive/volume", target_incarnation=client.incarnation_id, maximum_items=1
    )
    first = adapter.list_open_writes(request)
    assert first.continuation is not None and not first.exhausted
    continued = request.model_copy(update={"continuation": first.continuation})
    calls = len(client.list_requests)
    for foreign in (
        continued.model_copy(update={"object_path": "another/volume"}),
        continued.model_copy(update={"target_incarnation": "00000000-0000-4000-8000-000000000002"}),
    ):
        with pytest.raises(StorageAdapterRejection):
            adapter.list_open_writes(foreign)
        assert len(client.list_requests) == calls
    with pytest.raises(StorageAdapterRejection, match="response grant"):
        adapter.list_open_writes(request.model_copy(update={"maximum_response_bytes": 1}))
    client.response_override = {
        "Uploads": [],
        "IsTruncated": True,
        "NextKeyMarker": "owned/archive/volume",
        "NextUploadIdMarker": "token-0",
    }
    with pytest.raises(RuntimeError, match="advance"):
        adapter.list_open_writes(continued)
    client.response_override = {"Uploads": [], "IsTruncated": True}
    with pytest.raises(RuntimeError, match="advance"):
        adapter.list_open_writes(request)
    client.response_override = {"Uploads": []}
    with pytest.raises(RuntimeError, match="exhaustion"):
        adapter.list_open_writes(request)
    client.response_override = {
        "Uploads": [{"Key": "owned/unrelated", "UploadId": "foreign"}],
        "IsTruncated": False,
    }
    with pytest.raises(RuntimeError, match="invalid open-write"):
        adapter.list_open_writes(request)
    client.response_override = {"Uploads": client.rows[:2], "IsTruncated": False}
    with pytest.raises(RuntimeError, match="candidate grant"):
        adapter.list_open_writes(request)
    client.incarnation_id = "00000000-0000-4000-8000-000000000002"
    calls = len(client.list_requests)
    with pytest.raises(StorageIncarnationError, match="changed"):
        adapter.list_open_writes(request)
    assert len(client.list_requests) == calls


def test_denied_open_write_observation_is_not_empty_success() -> None:
    class Denied(_FakeS3Client):
        def list_multipart_uploads(self, **request: Any) -> dict[str, object]:
            raise _client_error("AccessDenied", 403, "ListMultipartUploads")

    client = Denied()
    adapter = S3StorageAdapter(client, _config())
    with pytest.raises(ClientError, match="AccessDenied"):
        adapter.list_open_writes(
            OpenWriteListRequest(
                object_path="archive/volume", target_incarnation=client.incarnation_id
            )
        )


class _CleanupInventoryClient(_FakeS3Client):
    def __init__(self, count: int) -> None:
        super().__init__()
        self.version_rows = [
            {
                "Key": "owned/archive/volume",
                "VersionId": f"version-{index:08d}",
                "field": "Versions" if index % 2 == 0 else "DeleteMarkers",
            }
            for index in range(count)
        ]
        self.write_rows = [
            {"Key": f"owned/archive/open-{index:08d}", "UploadId": f"write-%2B+-%25-{index:08d}"}
            for index in range(count)
        ]
        self.version_positions = {
            (row["Key"], row["VersionId"]): index for index, row in enumerate(self.version_rows)
        }
        self.write_positions = {
            (row["Key"], row["UploadId"]): index for index, row in enumerate(self.write_rows)
        }
        self.version_calls = 0
        self.write_calls = 0
        self.marker_reads = 0
        self.version_override: dict[str, Any] | None = None

    def get_object(self, **request: Any) -> dict[str, object]:
        self.marker_reads += 1
        return super().get_object(**request)

    def list_object_versions(self, **request: Any) -> dict[str, Any]:
        self.version_calls += 1
        assert request["Prefix"] == "owned/archive/"
        if self.version_override is not None:
            return self.version_override
        marker = (request.get("KeyMarker"), request.get("VersionIdMarker"))
        offset = self.version_positions[marker] + 1 if marker[0] is not None else 0
        rows = self.version_rows[offset : offset + request["MaxKeys"]]
        more = offset + len(rows) < len(self.version_rows)
        response: dict[str, Any] = {
            "Versions": [
                {k: row[k] for k in ("Key", "VersionId")}
                for row in rows
                if row["field"] == "Versions"
            ],
            "DeleteMarkers": [
                {k: row[k] for k in ("Key", "VersionId")}
                for row in rows
                if row["field"] == "DeleteMarkers"
            ],
            "IsTruncated": more,
        }
        if more:
            response.update(
                NextKeyMarker=rows[-1]["Key"], NextVersionIdMarker=rows[-1]["VersionId"]
            )
        return response

    def list_multipart_uploads(self, **request: Any) -> dict[str, Any]:
        self.write_calls += 1
        assert request["EncodingType"] == "url" and request["Prefix"] == "owned/archive/"
        marker = (request.get("KeyMarker"), request.get("UploadIdMarker"))
        offset = self.write_positions[marker] + 1 if marker[0] is not None else 0
        rows = self.write_rows[offset : offset + request["MaxUploads"]]
        more = offset + len(rows) < len(self.write_rows)
        response: dict[str, Any] = {
            "EncodingType": "url",
            "Uploads": [{**row, "Key": quote(row["Key"], safe="")} for row in rows],
            "IsTruncated": more,
        }
        if more:
            response.update(
                NextKeyMarker=quote(rows[-1]["Key"], safe=""),
                NextUploadIdMarker=rows[-1]["UploadId"],
            )
        return response

    def create_multipart_upload(self, **request: Any) -> dict[str, str]:
        raise AssertionError("inventory must not begin a write")

    def abort_multipart_upload(self, **request: Any) -> None:
        raise AssertionError("inventory must not dispose a write")


@pytest.mark.parametrize("count", [16, 256, 4096])
def test_cleanup_inventory_pages_every_version_marker_and_write_without_total_limit(
    count: int,
) -> None:
    client = _CleanupInventoryClient(count)
    request = CleanupInventoryRequest(
        object_prefix="archive/", target_incarnation=client.incarnation_id, maximum_items=7
    )
    observed = []
    while True:
        adapter = S3StorageAdapter(client, _config())
        reads = client.marker_reads
        calls = client.version_calls + client.write_calls
        page = adapter.list_cleanup_candidates(request)
        assert client.marker_reads == reads + 1
        assert client.version_calls + client.write_calls == calls + 1
        assert page.scanned_items == len(page.candidates) <= 7
        observed.extend(page.candidates)
        if page.exhausted:
            break
        request = request.model_copy(update={"continuation": page.continuation})
    expected = {
        (
            "object_revision" if row["field"] == "Versions" else "deletion_marker",
            "archive/volume",
            row["VersionId"],
        )
        for row in client.version_rows
    } | {
        ("open_write", row["Key"].removeprefix("owned/"), row["UploadId"])
        for row in client.write_rows
    }
    assert {(item.kind, item.object_path, item.token) for item in observed} == expected
    assert len(observed) == 2 * count
    assert client.version_calls == client.write_calls == (count + 6) // 7
    assert not client.uploads and not client.deleted


def test_cleanup_inventory_rejects_denied_malformed_and_foreign_continuations() -> None:
    client = _CleanupInventoryClient(2)
    adapter = S3StorageAdapter(client, _config())
    request = CleanupInventoryRequest(
        object_prefix="archive/", target_incarnation=client.incarnation_id, maximum_items=1
    )
    first = adapter.list_cleanup_candidates(request)
    assert first.continuation is not None
    with pytest.raises(StorageAdapterRejection, match="continuation"):
        adapter.list_cleanup_candidates(
            request.model_copy(
                update={"object_prefix": "another/", "continuation": first.continuation}
            )
        )
    with pytest.raises(StorageAdapterRejection, match="response grant"):
        adapter.list_cleanup_candidates(request.model_copy(update={"maximum_response_bytes": 1}))
    for response in (
        {"Versions": {}, "IsTruncated": False},
        {"Versions": [], "CommonPrefixes": [{"Prefix": "owned/archive/"}], "IsTruncated": False},
        {"Versions": []},
        {"Versions": [{"Key": "outside", "VersionId": "v"}], "IsTruncated": False},
        {"Versions": [], "IsTruncated": True},
    ):
        client.version_override = response
        with pytest.raises(RuntimeError):
            adapter.list_cleanup_candidates(request)
    client.version_override = {
        "IsTruncated": True,
        "NextKeyMarker": "owned/archive/volume",
        "NextVersionIdMarker": "version-00000000",
    }
    with pytest.raises(RuntimeError, match="advance"):
        adapter.list_cleanup_candidates(
            request.model_copy(update={"continuation": first.continuation})
        )

    class Denied(_CleanupInventoryClient):
        def list_object_versions(self, **request: Any) -> dict[str, Any]:
            raise _client_error("AccessDenied", 403, "ListObjectVersions")

    denied = Denied(1)
    with pytest.raises(ClientError, match="AccessDenied"):
        S3StorageAdapter(denied, _config()).list_cleanup_candidates(request)
    assert denied.write_calls == 0


def test_open_write_cursor_preserves_full_accepted_opaque_token_capacity() -> None:
    client = _OpenWritePageClient(2)
    client.rows[0]["UploadId"] = "\u0001" * 4000
    client.positions = {
        (row["Key"], row["UploadId"]): index for index, row in enumerate(client.rows)
    }
    adapter = S3StorageAdapter(client, _config())
    request = OpenWriteListRequest(
        object_path="archive/volume", target_incarnation=client.incarnation_id, maximum_items=1
    )
    page = adapter.list_open_writes(request)
    assert page.continuation is not None and len(page.continuation) > 16000
    continued = OpenWriteListRequest.model_validate_json(
        request.model_copy(update={"continuation": page.continuation}).model_dump_json()
    )
    assert adapter.list_open_writes(continued).observations[0].write_token == "token-1"
