from __future__ import annotations

import socket
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import pytest
from a_riverhog_s3_store_lib import (
    S3ClientConfig,
    S3StorageAdapter,
    S3StorageAdapterConfig,
    S3TransportTuning,
    create_s3_client,
)
from a_riverhog_s3_store_lib.incarnation import marker_document, marker_key
from botocore.exceptions import ClientError, ConnectionClosedError
from riverhog_storage_adapter_protocol import CleanupInventoryRequest, OpenWriteListRequest

_SUCCESS = {
    "begin": b"<InitiateMultipartUploadResult><UploadId>original-upload</UploadId>"
    b"</InitiateMultipartUploadResult>",
    "part": b"",
    "complete": b"<CompleteMultipartUploadResult><ETag>original-etag</ETag>"
    b"</CompleteMultipartUploadResult>",
    "put": b"",
}


@contextmanager
def _wire(operation: str, fault: str | None) -> Iterator[tuple[str, list[tuple[str, str]]]]:
    requests: list[tuple[str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: object) -> None:
            pass

        def dispatch(self) -> None:
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            requests.append((self.command, self.path))
            current_fault = fault if len(requests) == 1 else None
            if current_fault == "lost-response":
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                self.close_connection = True
                return
            status = 200
            body = _SUCCESS[operation]
            headers = {"ETag": "original-etag"}
            if current_fault == "server-error":
                status = 500
                body = b"<Error><Code>InternalError</Code></Error>"
            elif current_fault in {"region-redirect", "region-without-header"}:
                status = 307
                body = b"<Error><Code>PermanentRedirect</Code></Error>"
                if current_fault == "region-redirect":
                    headers["x-amz-bucket-region"] = "us-west-2"
            elif current_fault == "signing-region":
                status = 400
                body = (
                    b"<Error><Code>AuthorizationHeaderMalformed</Code>"
                    b"<Region>us-west-2</Region></Error>"
                )
            elif current_fault == "completion-body-error":
                body = b"<Error><Code>InternalError</Code></Error>"
            self.send_response(status)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        do_GET = dispatch
        do_HEAD = dispatch
        do_POST = dispatch
        do_PUT = dispatch

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(
        target=lambda: server.serve_forever(poll_interval=0.01),
        name="s3-dispatch-wire",
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def _client(endpoint: str) -> Any:
    return create_s3_client(
        S3ClientConfig(
            endpoint_url=endpoint,
            region="us-east-1",
            access_key_id="fixture-access-key",
            secret_access_key="fixture-secret-key",
            force_path_style=True,
        ),
        tuning=S3TransportTuning(connect_timeout_seconds=1, read_timeout_seconds=1),
    )


def _invoke(client: Any, operation: str) -> dict[str, Any]:
    target = {"Bucket": "fixture-bucket", "Key": "archive/volume.age"}
    if operation == "begin":
        return client.create_multipart_upload(**target)
    if operation == "part":
        return client.upload_part(
            **target, UploadId="original-upload", PartNumber=1, Body=b"ciphertext"
        )
    if operation == "complete":
        return client.complete_multipart_upload(
            **target,
            UploadId="original-upload",
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": "original-etag"}]},
        )
    if operation == "put":
        return client.put_object(**target, Body=b"ciphertext")
    raise AssertionError("unsupported fixture operation")


@pytest.mark.parametrize("operation", ["begin", "part", "complete", "put"])
@pytest.mark.parametrize(
    "fault",
    ["server-error", "region-redirect", "region-without-header", "signing-region", "lost-response"],
)
def test_s3_dispatch_does_not_replay_an_uncertain_mutation(
    operation: str,
    fault: str,
    record_property: Callable[[str, object], None],
) -> None:
    with _wire(operation, fault) as (endpoint, requests):
        client = _client(endpoint)
        try:
            expected_error = ConnectionClosedError if fault == "lost-response" else ClientError
            with pytest.raises(expected_error):
                _invoke(client, operation)
        finally:
            client.close()
            record_property("wire_attempts", len(requests))
    assert len(requests) == 1


def test_s3_completion_parses_body_error_without_replaying() -> None:
    with _wire("complete", "completion-body-error") as (endpoint, requests):
        client = _client(endpoint)
        try:
            with pytest.raises(ClientError) as raised:
                _invoke(client, "complete")
            assert raised.value.response["Error"]["Code"] == "InternalError"
        finally:
            client.close()
    assert len(requests) == 1


@pytest.mark.parametrize("operation", ["begin", "part", "complete", "put"])
def test_s3_dispatch_preserves_a_complete_successful_response(operation: str) -> None:
    with _wire(operation, None) as (endpoint, requests):
        client = _client(endpoint)
        try:
            result = _invoke(client, operation)
            assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert result["ResponseMetadata"]["RetryAttempts"] == 0
            if operation == "begin":
                assert result["UploadId"] == "original-upload"
        finally:
            client.close()
    assert len(requests) == 1


def test_s3_single_attempt_policy_overrides_environment_retry_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_MAX_ATTEMPTS", "9")
    monkeypatch.setenv("AWS_RETRY_MODE", "adaptive")
    with _wire("begin", "server-error") as (endpoint, requests):
        client = _client(endpoint)
        try:
            assert client.meta.config.retries == {"mode": "standard", "total_max_attempts": 1}
            with pytest.raises(ClientError):
                _invoke(client, "begin")
        finally:
            client.close()
    assert len(requests) == 1


def test_cleanup_inventory_and_open_writes_preserve_encoded_keys_through_owned_sdk() -> None:
    incarnation = "00000000-0000-4000-8000-000000000001"
    path = "archive/α%20&/volume.age"
    key = "owned/" + path
    encoded = quote(key, safe="")
    requests: list[tuple[str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: object) -> None:
            pass

        def do_GET(self) -> None:
            requests.append((self.command, self.path))
            url = urlsplit(self.path)
            query = parse_qs(url.query, keep_blank_values=True)
            if url.path == "/fixture-bucket/" + marker_key("owned"):
                body = marker_document(incarnation)
                content_type = "application/octet-stream"
            elif "versions" in query:
                assert query["max-keys"] == ["1"]
                assert query["encoding-type"] == ["url"]
                if "version-id-marker" not in query:
                    body = (
                        "<ListVersionsResult><EncodingType>url</EncodingType>"
                        "<IsTruncated>true</IsTruncated>"
                        f"<NextKeyMarker>{encoded}</NextKeyMarker>"
                        "<NextVersionIdMarker>revision-%2B+1</NextVersionIdMarker>"
                        f"<Version><Key>{encoded}</Key><VersionId>revision-%2B+1</VersionId></Version>"
                        "</ListVersionsResult>"
                    ).encode()
                else:
                    assert query["key-marker"] == [key]
                    assert query["version-id-marker"] == ["revision-%2B+1"]
                    body = (
                        "<ListVersionsResult><EncodingType>url</EncodingType>"
                        "<IsTruncated>false</IsTruncated>"
                        f"<DeleteMarker><Key>{encoded}</Key><VersionId>marker-1</VersionId></DeleteMarker>"
                        "</ListVersionsResult>"
                    ).encode()
                content_type = "application/xml"
            else:
                assert "uploads" in query and query["encoding-type"] == ["url"]
                assert query["prefix"] in (["owned/archive/"], [key])
                body = (
                    "<ListMultipartUploadsResult><EncodingType>url</EncodingType>"
                    "<IsTruncated>false</IsTruncated>"
                    f"<Upload><Key>{encoded}</Key><UploadId>write-%2B+%-1</UploadId></Upload>"
                    "</ListMultipartUploadsResult>"
                ).encode()
                content_type = "application/xml"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    worker = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
    worker.start()
    client = _client(f"http://127.0.0.1:{server.server_port}")
    try:
        adapter = S3StorageAdapter(
            client,
            S3StorageAdapterConfig(
                implementation_id="fixture.s3/v1",
                implementation_version="1",
                bucket="fixture-bucket",
                root_prefix="owned",
            ),
        )
        request = CleanupInventoryRequest(
            object_prefix="archive/", target_incarnation=incarnation, maximum_items=1
        )
        observed = []
        for index in range(3):
            before = len(requests)
            page = adapter.list_cleanup_candidates(request)
            assert len(requests) == before + 2
            observed.extend(page.candidates)
            assert page.exhausted is (index == 2)
            request = request.model_copy(update={"continuation": page.continuation})
        assert [(item.kind, item.object_path, item.token) for item in observed] == [
            ("object_revision", path, "revision-%2B+1"),
            ("deletion_marker", path, "marker-1"),
            ("open_write", path, "write-%2B+%-1"),
        ]
        open_page = adapter.list_open_writes(
            OpenWriteListRequest(object_path=path, target_incarnation=incarnation, maximum_items=1)
        )
        assert open_page.exhausted and open_page.observations[0].write_token == "write-%2B+%-1"
        assert len(requests) == 9 and {method for method, _path in requests} == {"GET"}
    finally:
        client.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
        assert not worker.is_alive()
