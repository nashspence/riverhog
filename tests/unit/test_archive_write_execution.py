from __future__ import annotations

import hashlib
import secrets
import socket
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
from a_riverhog_filesystem_store import (
    FilesystemStorageAdapter,
    FilesystemStorageAdapterConfig,
)
from a_riverhog_filesystem_store.incarnation import provision_storage_root
from fastapi import FastAPI
from pydantic import ValidationError
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterArchiveResumableObjectStore,
)
from riverhog_storage_adapter_asgi_support import create_storage_adapter_app
from riverhog_storage_adapter_protocol import (
    AdapterDescriptor,
    BeginWriteOperation,
    CleanupInventoryPage,
    CleanupInventoryRequest,
    CompletedWriteLookupRequest,
    CompleteWriteOperation,
    LookupWriteOperation,
    PutObjectOperation,
    SmallObjectWriteRequest,
    WriteCallIdentity,
    WriteCompleteRequest,
    WriteExecutionResult,
    WriteInvocation,
    WriteOperation,
    WritePartOperation,
    WriteSegmentListRequest,
    WriteSegmentRequest,
    WriteSession,
    WriteStartRequest,
    unknown_write_execution,
    validated_storage_adapter,
    write_operation_sha256,
)

_CONTENT = b"abc"
_SHA = hashlib.sha256(_CONTENT).hexdigest()
_INCARNATION = "00000000-0000-4000-8000-000000000001"


def _invocation(operation: WriteOperation, incarnation: str) -> WriteInvocation:
    return WriteInvocation(
        call=WriteCallIdentity(
            call_id=secrets.token_hex(32),
            exact_request_sha256=write_operation_sha256(operation),
            target_incarnation=incarnation,
            dispatch_owner=secrets.token_hex(32),
        ),
        operation=operation,
    )


def _start(path: str = "archives/volume.age") -> WriteStartRequest:
    return WriteStartRequest(
        object_path=path,
        expected_bytes=3,
        content_type="application/octet-stream",
        required_identity_assertions={"owner": "original"},
        placement_policy="archive_default",
    )


@contextmanager
def _filesystem(root: Path) -> Iterator[FilesystemStorageAdapter]:
    root.mkdir()
    provision_storage_root(root)
    with FilesystemStorageAdapter(
        FilesystemStorageAdapterConfig(
            root=root,
            segment_bytes=64 * 1024,
            read_chunk_bytes=64 * 1024,
            minimum_free_bytes=0,
        )
    ) as adapter:
        yield adapter


@contextmanager
def _asgi(app: FastAPI) -> Iterator[str]:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            log_level="critical",
            lifespan="off",
            timeout_graceful_shutdown=3,
        )
    )
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]})
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not server.started:
            assert thread.is_alive() and time.monotonic() < deadline
            time.sleep(0.01)
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        sock.close()
        assert not thread.is_alive()


@contextmanager
def _executor(adapter: FilesystemStorageAdapter, mode: str) -> Iterator[Any]:
    from riverhog_storage_adapter_support import StorageAdapterClient

    if mode == "direct":
        yield adapter
        return
    app = create_storage_adapter_app(
        service="fixture-storage", token="fixture-token", adapter=adapter
    )
    with _asgi(app) as endpoint:
        supplied = httpx.Client(timeout=2, follow_redirects=True) if mode == "supplied" else None
        client = StorageAdapterClient(
            endpoint, token="fixture-token", allow_insecure_http=True, timeout=2, client=supplied
        )
        try:
            yield client
        finally:
            client.close()
            if supplied is not None:
                supplied.close()


@pytest.mark.parametrize("mode", ["direct", "owned-http", "supplied"])
@pytest.mark.parametrize("kind", ["begin", "part", "complete", "lookup", "put"])
def test_actual_executor_retains_exact_results_and_separates_transport_closure(
    tmp_path: Path,
    mode: str,
    kind: str,
) -> None:
    with _filesystem(tmp_path / "store") as adapter, _executor(adapter, mode) as executor:
        request = _start()
        operation: WriteOperation
        if kind == "begin":
            operation = BeginWriteOperation(request=request)
        elif kind == "put":
            operation = PutObjectOperation(
                request=SmallObjectWriteRequest(
                    object_path="archives/manifest.age",
                    content_type=request.content_type,
                    required_identity_assertions=request.required_identity_assertions,
                    placement_policy=request.placement_policy,
                    mode="create_only",
                    stored_bytes=3,
                    stored_sha256=_SHA,
                )
            )
        else:
            session = adapter.begin_write(request)
            if kind == "part":
                operation = WritePartOperation(
                    request=WriteSegmentRequest(session=session, number=1, stored_bytes=3),
                    stored_sha256=_SHA,
                )
            else:
                adapter.write_segment(session=session, number=1, stored_bytes=3, content=_CONTENT)
                completion = adapter.list_segments(
                    WriteSegmentListRequest(session=session)
                ).completion
                assert completion is not None
                complete = WriteCompleteRequest(
                    session=session,
                    completion=completion,
                    expected_bytes=3,
                    expected_content_type=request.content_type,
                    required_identity_assertions=request.required_identity_assertions,
                    expected_placement_policy=request.placement_policy,
                )
                if kind == "complete":
                    operation = CompleteWriteOperation(request=complete)
                else:
                    adapter.complete_write(complete)
                    operation = LookupWriteOperation(
                        request=CompletedWriteLookupRequest(
                            object_path=request.object_path,
                            expected_bytes=3,
                            expected_content_type=request.content_type,
                            required_identity_assertions=request.required_identity_assertions,
                            expected_placement_policy=request.placement_policy,
                        )
                    )
        invocation = _invocation(operation, adapter.descriptor().storage_incarnation_id)
        store = StorageAdapterArchiveResumableObjectStore(executor)
        result = store.execute_write(
            invocation, content=_CONTENT if kind in {"part", "put"} else b""
        )
        assert result.call == invocation.call
        assert result.execution == ("unknown" if mode == "supplied" else "closed")
        assert result.outcome == {"begin": "session", "part": "part"}.get(kind, "completed")
        assert result.result is not None
        if isinstance(result.result, WriteSession):
            assert result.result.object_path == request.object_path
            assert adapter.begin_write(request) == result.result
        else:
            assert result.result.stored_bytes == 3


@pytest.mark.parametrize("mode", ["direct", "owned-http", "supplied"])
def test_negative_lookup_closes_only_the_accounted_invocation(tmp_path: Path, mode: str) -> None:
    with _filesystem(tmp_path / "store") as adapter, _executor(adapter, mode) as executor:
        request = _start()
        invocation = _invocation(
            LookupWriteOperation(
                request=CompletedWriteLookupRequest(
                    object_path=request.object_path,
                    expected_bytes=3,
                    expected_content_type=request.content_type,
                    required_identity_assertions=request.required_identity_assertions,
                    expected_placement_policy=request.placement_policy,
                )
            ),
            adapter.descriptor().storage_incarnation_id,
        )
        result = validated_storage_adapter(executor).execute_write(invocation)
        assert result.outcome == ("unknown" if mode == "supplied" else "no_effect")
        assert result.execution == ("unknown" if mode == "supplied" else "closed")
        assert result.result is None


def test_request_commitment_and_target_are_checked_before_effects(tmp_path: Path) -> None:
    with _filesystem(tmp_path / "store") as adapter:
        invocation = _invocation(BeginWriteOperation(request=_start()), _INCARNATION)
        port = validated_storage_adapter(adapter)
        assert port.execute_write(invocation).execution == "unknown"
        assert list((tmp_path / "store" / "writes").iterdir()) == []
        original = _invocation(invocation.operation, adapter.descriptor().storage_incarnation_id)
        tampered = original.model_copy(
            update={"operation": BeginWriteOperation(request=_start("archives/changed.age"))}
        )
        assert port.execute_write(tampered).outcome == "unknown"
        assert list((tmp_path / "store" / "writes").iterdir()) == []
        with pytest.raises(ValidationError):
            WriteInvocation.model_validate_json(tampered.model_dump_json())


@pytest.mark.parametrize("bad_content", [b"xyz", b"ab", b"abcd"])
def test_unmatched_or_incomplete_payload_never_reports_closed(
    tmp_path: Path,
    bad_content: bytes,
) -> None:
    with _filesystem(tmp_path / "store") as adapter:
        session = adapter.begin_write(_start())
        invocation = _invocation(
            WritePartOperation(
                request=WriteSegmentRequest(session=session, number=1, stored_bytes=3),
                stored_sha256=_SHA,
            ),
            adapter.descriptor().storage_incarnation_id,
        )
        result = validated_storage_adapter(adapter).execute_write(invocation, content=bad_content)
        assert result.execution == "unknown" and result.outcome == "unknown"


def test_lost_http_reply_does_not_stop_the_actual_adapter_executor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from riverhog_storage_adapter_support import StorageAdapterClient

    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    with _filesystem(tmp_path / "store") as adapter:
        original = adapter.begin_write
        calls = []

        def delayed(request: WriteStartRequest) -> WriteSession:
            calls.append(request)
            entered.set()
            assert release.wait(timeout=5)
            try:
                return original(request)
            finally:
                finished.set()

        monkeypatch.setattr(adapter, "begin_write", delayed)
        app = create_storage_adapter_app(
            service="fixture-storage", token="fixture-token", adapter=adapter
        )
        invocation = _invocation(
            BeginWriteOperation(request=_start()), adapter.descriptor().storage_incarnation_id
        )
        try:
            with _asgi(app) as endpoint:
                client = StorageAdapterClient(
                    endpoint, token="fixture-token", allow_insecure_http=True, timeout=0.2
                )
                try:
                    result = client.execute_write(invocation)
                    assert entered.is_set() and not finished.is_set()
                    assert result == unknown_write_execution(invocation)
                    assert list((tmp_path / "store" / "writes").iterdir()) == []
                    release.set()
                    assert finished.wait(timeout=3)
                    assert len(list((tmp_path / "store" / "writes").iterdir())) == 1
                    assert len(calls) == 1
                    assert result.execution == "unknown"
                finally:
                    client.close()
                    release.set()
        finally:
            release.set()


@contextmanager
def _wire(
    respond: Callable[[BaseHTTPRequestHandler], None],
) -> Iterator[tuple[str, list[str]]]:
    requests: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: object) -> None:
            pass

        def do_GET(self) -> None:
            self.send(
                200,
                (
                    '{"storage_incarnation_id":"' + _INCARNATION + '",'
                    '"implementation_id":"fixture.storage/v1","implementation_version":"1",'
                    '"read_mode":"immediate","minimum_nonfinal_segment_bytes":"1"}'
                ).encode(),
            )

        def do_POST(self) -> None:
            self.rfile.read(int(self.headers["Content-Length"]))
            requests.append(self.path)
            respond(self)

        def send(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            if status in {307, 308}:
                self.send_header("Location", "/replayed")
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


@pytest.mark.parametrize(
    "fault",
    [
        "call_id",
        "exact_request_sha256",
        "target_incarnation",
        "dispatch_owner",
        "result",
        "server-error",
        "redirect-307",
        "redirect-308",
        "lost-response",
    ],
)
def test_owned_http_refuses_mismatched_or_uncertain_reports_without_replay(fault: str) -> None:
    from riverhog_storage_adapter_support import StorageAdapterClient

    invocation = _invocation(BeginWriteOperation(request=_start()), _INCARNATION)
    response = WriteExecutionResult(
        call=invocation.call,
        execution="closed",
        outcome="session",
        result=WriteSession(
            object_path=_start().object_path, expected_bytes=3, write_token="original"
        ),
    ).model_dump(mode="json")
    if fault in response["call"]:
        response["call"][fault] = (
            "00000000-0000-4000-8000-000000000002" if fault == "target_incarnation" else "f" * 64
        )
    elif fault == "result":
        response["result"]["expected_bytes"] = "4"

    def respond(handler: Any) -> None:
        import json

        if fault == "lost-response":
            handler.connection.shutdown(socket.SHUT_RDWR)
            handler.connection.close()
            handler.close_connection = True
        else:
            status = (
                500
                if fault == "server-error"
                else (int(fault[-3:]) if fault.startswith("redirect") else 200)
            )
            handler.send(status, json.dumps(response).encode())

    with _wire(respond) as (endpoint, requests):
        client = StorageAdapterClient(
            endpoint, token="fixture-token", allow_insecure_http=True, timeout=1
        )
        try:
            result = client.execute_write(invocation)
        finally:
            client.close()
    assert result == unknown_write_execution(invocation)
    assert requests == ["/v1/writes/execute"]


@contextmanager
def _s3_wire(fault: str | None) -> Iterator[tuple[str, list[str]]]:
    from a_riverhog_s3_store_lib.incarnation import marker_document

    writes: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: object) -> None:
            pass

        def do_GET(self) -> None:
            if "uploads" in self.path:
                self.send(
                    200,
                    b"<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>"
                    b"</ListMultipartUploadsResult>",
                )
            else:
                self.send(200, marker_document(_INCARNATION))

        def do_POST(self) -> None:
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            writes.append(self.path)
            if fault == "lost-response":
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                self.close_connection = True
            elif fault == "server-error":
                self.send(500, b"<Error><Code>InternalError</Code></Error>")
            else:
                self.send(
                    200,
                    b"<InitiateMultipartUploadResult>"
                    b"<UploadId>original-session</UploadId></InitiateMultipartUploadResult>",
                )

        def send(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", writes
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


@pytest.mark.parametrize("fault", [None, "lost-response", "server-error"])
def test_actual_s3_owner_reports_only_joined_success_and_keeps_lost_response_unknown(
    fault: str | None,
) -> None:
    from a_riverhog_s3_store_lib import (
        S3ClientConfig,
        S3StorageAdapter,
        S3StorageAdapterConfig,
        S3TransportTuning,
        create_s3_client,
    )

    with _s3_wire(fault) as (endpoint, writes):
        client = create_s3_client(
            S3ClientConfig(
                endpoint_url=endpoint,
                region="us-east-1",
                access_key_id="fixture-access",
                secret_access_key="fixture-secret",
                force_path_style=True,
            ),
            tuning=S3TransportTuning(connect_timeout_seconds=1, read_timeout_seconds=1),
        )
        try:
            adapter = S3StorageAdapter(
                client,
                S3StorageAdapterConfig(
                    implementation_id="fixture.s3/v1",
                    implementation_version="1",
                    bucket="fixture-bucket",
                ),
            )
            invocation = _invocation(BeginWriteOperation(request=_start()), _INCARNATION)
            result = StorageAdapterArchiveResumableObjectStore(adapter).execute_write(invocation)
        finally:
            client.close()
    assert len(writes) == 1
    if fault is None:
        assert result.execution == "closed" and result.outcome == "session"
        assert isinstance(result.result, WriteSession)
        assert result.result.write_token == "original-session"
    else:
        assert result == unknown_write_execution(invocation)


def test_supplied_s3_client_can_preserve_session_without_asserting_closure() -> None:
    from io import BytesIO

    from a_riverhog_s3_store_lib import S3StorageAdapter, S3StorageAdapterConfig
    from a_riverhog_s3_store_lib.incarnation import marker_document

    class Supplied:
        def __init__(self) -> None:
            self.writes = 0

        def get_object(self, **kwargs: Any) -> dict[str, object]:
            return {"Body": BytesIO(marker_document(_INCARNATION))}

        def list_multipart_uploads(self, **kwargs: Any) -> dict[str, object]:
            return {"Uploads": []}

        def create_multipart_upload(self, **kwargs: Any) -> dict[str, str]:
            self.writes += 1
            return {"UploadId": "observed-session"}

    supplied = Supplied()
    adapter = S3StorageAdapter(
        supplied,
        S3StorageAdapterConfig(
            implementation_id="fixture.s3/v1",
            implementation_version="1",
            bucket="fixture-bucket",
        ),
    )
    invocation = _invocation(BeginWriteOperation(request=_start()), _INCARNATION)
    result = validated_storage_adapter(adapter).execute_write(invocation)
    assert supplied.writes == 1
    assert result.execution == "unknown" and result.outcome == "session"
    assert (
        isinstance(result.result, WriteSession) and result.result.write_token == "observed-session"
    )


def test_execution_port_annotations_resolve_for_contract_consumers() -> None:
    from typing import get_type_hints

    from riverhog_storage_adapter_protocol import StorageAdapterPort, ValidatedStorageAdapterPort

    for owner in (StorageAdapterPort, ValidatedStorageAdapterPort):
        hints = get_type_hints(owner.execute_write)
        assert hints["invocation"] is WriteInvocation
        assert hints["return"] is WriteExecutionResult


def test_upload_correlation_cannot_supply_execution_closure() -> None:
    invocation = _invocation(BeginWriteOperation(request=_start()), _INCARNATION)
    data = invocation.model_dump(mode="json")
    data["execution"] = "closed"
    with pytest.raises(ValidationError):
        WriteInvocation.model_validate(data)


def test_supplied_transport_replay_preserves_observation_but_never_closure() -> None:
    from riverhog_storage_adapter_support import StorageAdapterClient

    invocation = _invocation(BeginWriteOperation(request=_start()), _INCARNATION)
    response = WriteExecutionResult(
        call=invocation.call,
        execution="closed",
        outcome="session",
        result=WriteSession(
            object_path=_start().object_path, expected_bytes=3, write_token="original"
        ),
    )

    class ReplayTransport(httpx.BaseTransport):
        def __init__(self) -> None:
            self.inner = httpx.HTTPTransport(retries=0)

        def handle_request(self, request: httpx.Request) -> httpx.Response:
            request.read()
            first = self.inner.handle_request(request)
            first.read()
            first.close()
            return self.inner.handle_request(request)

        def close(self) -> None:
            self.inner.close()

    def respond(handler: Any) -> None:
        handler.send(200, response.model_dump_json().encode())

    with (
        _wire(respond) as (endpoint, requests),
        httpx.Client(transport=ReplayTransport()) as supplied,
    ):
        client = StorageAdapterClient(
            endpoint, token="fixture-token", allow_insecure_http=True, client=supplied
        )
        result = validated_storage_adapter(client).execute_write(invocation)
        client.close()
    assert requests == ["/v1/writes/execute", "/v1/writes/execute"]
    assert result.execution == "unknown" and result.outcome == "session"
    assert result.result == response.result


def test_execution_validation_rejects_put_digest_even_with_a_matching_claimed_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from riverhog_storage_adapter_protocol import ImmutableObjectReceipt

    with _filesystem(tmp_path / "store") as adapter:
        request = SmallObjectWriteRequest(
            object_path="archives/manifest.age",
            content_type="application/octet-stream",
            required_identity_assertions={},
            placement_policy="archive_default",
            mode="create_only",
            stored_bytes=3,
            stored_sha256=_SHA,
        )
        invocation = _invocation(
            PutObjectOperation(request=request), adapter.descriptor().storage_incarnation_id
        )

        def inconsistent(call: WriteInvocation, *, content: Any) -> WriteExecutionResult:
            assert b"".join(content) == b"xyz"
            return WriteExecutionResult(
                call=call.call,
                execution="closed",
                outcome="completed",
                result=ImmutableObjectReceipt(
                    object_path=request.object_path,
                    stored_bytes=3,
                    stored_sha256=_SHA,
                    verified_content_type=request.content_type,
                    verified_identity_assertions={},
                    verified_placement_policy=request.placement_policy,
                    completed_at="2026-08-21T00:00:00.000000000Z",
                ),
            )

        monkeypatch.setattr(adapter, "execute_write", inconsistent)
        result = validated_storage_adapter(adapter).execute_write(
            invocation, content=iter((b"xyz",))
        )
        assert result == unknown_write_execution(invocation)


@pytest.mark.parametrize("changed_incarnation", [False, True])
def test_http_incarnation_wait_does_not_block_independent_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changed_incarnation: bool,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    responses: list[httpx.Response] = []
    errors: list[BaseException] = []
    observed: list[CleanupInventoryRequest] = []
    with _filesystem(tmp_path / "store") as adapter:
        descriptor = adapter.descriptor()
        incarnation = descriptor.storage_incarnation_id
        original_descriptor = adapter.descriptor
        original_inventory = adapter.list_cleanup_candidates

        def held_descriptor() -> AdapterDescriptor:
            if not entered.is_set():
                entered.set()
                assert release.wait(10), "descriptor fixture was not released"
            value = original_descriptor()
            return (
                value.model_copy(update={"storage_incarnation_id": _INCARNATION})
                if changed_incarnation
                else value
            )

        def observe_inventory(request: CleanupInventoryRequest) -> CleanupInventoryPage:
            observed.append(request)
            return original_inventory(request)

        monkeypatch.setattr(adapter, "descriptor", held_descriptor)
        monkeypatch.setattr(adapter, "list_cleanup_candidates", observe_inventory)
        app = create_storage_adapter_app(
            service="fixture-storage",
            token="fixture-token",
            adapter=adapter,
        )
        request = CleanupInventoryRequest(
            object_prefix="archive/",
            target_incarnation=incarnation,
        )
        with _asgi(app) as endpoint:

            def invoke() -> None:
                try:
                    responses.append(
                        httpx.post(
                            endpoint + "/v1/cleanup/inventory",
                            headers={
                                "Authorization": "Bearer fixture-token",
                                "Riverhog-Expected-Storage-Incarnation": incarnation,
                            },
                            content=request.model_dump_json(),
                            timeout=8,
                        )
                    )
                except BaseException as exc:
                    errors.append(exc)

            caller = threading.Thread(target=invoke, name="held-incarnation-caller")
            caller.start()
            try:
                assert entered.wait(5), "incarnation precondition was not reached"
                assert not observed
                assert httpx.get(endpoint + "/health/live", timeout=2).status_code == 200
                assert caller.is_alive() and not release.is_set()
            finally:
                release.set()
                caller.join(timeout=5)
            assert not caller.is_alive() and not errors
            assert len(responses) == 1
            if changed_incarnation:
                assert responses[0].status_code == 503
                assert responses[0].json()["error"]["code"] == "provider_unavailable"
                assert observed == []
            else:
                assert responses[0].status_code == 200
                assert CleanupInventoryPage.model_validate_json(responses[0].content).exhausted
                assert observed == [request]
