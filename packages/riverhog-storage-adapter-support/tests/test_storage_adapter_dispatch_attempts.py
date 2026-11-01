from __future__ import annotations

import hashlib
import socket
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx
import pytest
from http_api_contracts import canonical_json_bytes
from pydantic import BaseModel
from riverhog_storage_adapter_protocol import (
    AdapterDescriptor,
    CompletedObjectReceipt,
    ImmutableObjectReceipt,
    ObjectLocator,
    ObjectReadRequest,
    SmallObjectWriteRequest,
    WriteCompleteRequest,
    WriteCompletionPrecondition,
    WriteSegmentReceipt,
    WriteSession,
    WriteStartRequest,
)
from riverhog_storage_adapter_support import StorageAdapterClient, StorageAdapterProtocolError

_INCARNATION = "00000000-0000-4000-8000-000000000001"
_PATH = "archives/volume.age"
_CONTENT = b"abc"
_DIGEST = hashlib.sha256(_CONTENT).hexdigest()
_SESSION = WriteSession(object_path=_PATH, write_token="original-write", expected_bytes=3)
_DESCRIPTOR = AdapterDescriptor(
    storage_incarnation_id=_INCARNATION,
    implementation_id="fixture.storage/v1",
    implementation_version="1.0.0",
    read_mode="immediate",
    minimum_nonfinal_segment_bytes=1,
    maximum_segment_bytes=1024 * 1024,
    maximum_segment_count=10000,
)


def _response(operation: str) -> BaseModel:
    if operation == "begin":
        return _SESSION
    if operation == "part":
        return WriteSegmentReceipt(number=1, segment_token="original-segment", stored_bytes=3)
    fields = {
        "object_path": _PATH,
        "revision": "original-revision",
        "entity_token": "original-entity",
        "stored_bytes": 3,
        "verified_content_type": "application/octet-stream",
        "verified_identity_assertions": {},
        "verified_placement_policy": "archive_default",
        "completed_at": "2026-08-21T00:00:00.000000000Z",
    }
    if operation == "put":
        return ImmutableObjectReceipt(**fields, stored_sha256=_DIGEST)
    return CompletedObjectReceipt(**fields)


@contextmanager
def _wire(operation: str, fault: str | None) -> Iterator[tuple[str, list[tuple[str, str | None]]]]:
    writes: list[tuple[str, str | None]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: object) -> None:
            pass

        def do_GET(self) -> None:
            assert self.path == "/v1/adapter"
            self.respond(200, canonical_json_bytes(_DESCRIPTOR.model_dump(mode="json")))

        def do_POST(self) -> None:
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            writes.append((self.path, self.headers.get("Riverhog-Expected-Storage-Incarnation")))
            current = fault if len(writes) == 1 else None
            if current == "lost-response":
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                self.close_connection = True
                return
            if current == "server-error":
                self.respond(
                    500, b'{"error":{"code":"provider_unavailable","message":"fixture failure"}}'
                )
            elif current in {"redirect-307", "redirect-308"}:
                self.respond(int(current[-3:]), b"", location="/redirected-write")
            else:
                self.respond(
                    200, canonical_json_bytes(_response(operation).model_dump(mode="json"))
                )

        def respond(self, status: int, body: bytes, *, location: str | None = None) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            if location is not None:
                self.send_header("Location", location)
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(
        target=lambda: server.serve_forever(poll_interval=0.01),
        name="adapter-dispatch-wire",
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", writes
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


@contextmanager
def _client(endpoint: str, injected: bool) -> Iterator[StorageAdapterClient]:
    http = httpx.Client(follow_redirects=True, timeout=1) if injected else None
    client = StorageAdapterClient(
        endpoint,
        token="fixture-adapter-token",
        allow_insecure_http=True,
        timeout=1,
        client=http,
    )
    try:
        yield client
    finally:
        client.close()
        if http is not None:
            http.close()


def _invoke(client: StorageAdapterClient, operation: str) -> Any:
    if operation == "begin":
        return client.begin_write(
            WriteStartRequest(
                object_path=_PATH,
                expected_bytes=3,
                content_type="application/octet-stream",
                required_identity_assertions={},
                placement_policy="archive_default",
            )
        )
    if operation == "part":
        return client.write_segment(session=_SESSION, number=1, stored_bytes=3, content=_CONTENT)
    if operation == "complete":
        return client.complete_write(
            WriteCompleteRequest(
                session=_SESSION,
                completion=WriteCompletionPrecondition(
                    segment_count=1, stored_bytes=3, state_token="a" * 64
                ),
                expected_bytes=3,
                expected_content_type="application/octet-stream",
                required_identity_assertions={},
                expected_placement_policy="archive_default",
            )
        )
    if operation == "put":
        return client.put_small_object(
            SmallObjectWriteRequest(
                object_path=_PATH,
                content_type="application/octet-stream",
                required_identity_assertions={},
                placement_policy="archive_default",
                mode="create_only",
                stored_bytes=3,
                stored_sha256=_DIGEST,
            ),
            _CONTENT,
        )
    raise AssertionError("unsupported fixture operation")


@pytest.mark.parametrize("injected", [False, True], ids=["owned", "supplied"])
@pytest.mark.parametrize("operation", ["begin", "part", "complete", "put"])
@pytest.mark.parametrize("fault", ["server-error", "redirect-307", "redirect-308", "lost-response"])
def test_adapter_dispatch_does_not_replay_an_uncertain_write(
    injected: bool,
    operation: str,
    fault: str,
    record_property: Callable[[str, object], None],
) -> None:
    with _wire(operation, fault) as (endpoint, writes):
        try:
            with _client(endpoint, injected) as client:
                with pytest.raises(StorageAdapterProtocolError):
                    _invoke(client, operation)
        finally:
            record_property("write_requests", len(writes))
    assert len(writes) == 1
    assert writes[0][1] == _INCARNATION


@pytest.mark.parametrize("injected", [False, True], ids=["owned", "supplied"])
@pytest.mark.parametrize("operation", ["begin", "part", "complete", "put"])
def test_adapter_dispatch_preserves_the_original_successful_result(
    injected: bool, operation: str
) -> None:
    with _wire(operation, None) as (endpoint, writes):
        with _client(endpoint, injected) as client:
            assert _invoke(client, operation) == _response(operation)
    assert len(writes) == 1
    assert writes[0][1] == _INCARNATION


@pytest.mark.parametrize("injected", [False, True], ids=["owned", "supplied"])
def test_adapter_stream_does_not_follow_redirects(injected: bool) -> None:
    with _wire("complete", "redirect-307") as (endpoint, writes):
        with _client(endpoint, injected) as client:
            with pytest.raises(StorageAdapterProtocolError):
                client.read_object(
                    ObjectReadRequest(
                        object=ObjectLocator(object_path=_PATH),
                        expected_bytes=3,
                    )
                )
    assert len(writes) == 1
    assert writes[0][1] == _INCARNATION
