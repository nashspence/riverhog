from __future__ import annotations

import hashlib
import ipaddress
import socket
import socketserver
import ssl
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import DataReceived, RequestReceived, StreamEnded
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


def _tls_context(tmp_path: Path) -> tuple[ssl.SSLContext, Path]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    certificate_path = tmp_path / "adapter-wire.crt"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path = tmp_path / "adapter-wire.key"
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_path.chmod(0o600)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate_path, key_path)
    context.set_alpn_protocols(["h2", "http/1.1"])
    return context, certificate_path


@contextmanager
def _goaway_wire(
    tmp_path: Path, operation: str
) -> Iterator[tuple[str, Path, list[tuple[str, str | None]], list[str]]]:
    context, certificate = _tls_context(tmp_path)
    writes: list[tuple[str, str | None]] = []
    protocols: list[str] = []
    errors: list[str] = []

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = False

        def get_request(self):
            connection, address = super().get_request()
            connection.settimeout(3)
            try:
                return context.wrap_socket(connection, server_side=True), address
            except BaseException:
                connection.close()
                raise

    class Handler(socketserver.BaseRequestHandler):
        def handle(self) -> None:
            try:
                protocol = self.request.selected_alpn_protocol()
                protocols.append(protocol)
                if protocol == "h2":
                    self.http2()
                else:
                    self.http1()
            except OSError:
                pass
            except Exception as exc:
                errors.append(type(exc).__name__)

        def http1(self) -> None:
            with self.request.makefile("rb") as reader:
                while line := reader.readline():
                    method, path, _ = line.decode("ascii").strip().split(" ")
                    headers = {}
                    while (line := reader.readline()) != b"\r\n":
                        name, value = line.decode("ascii").strip().split(":", 1)
                        headers[name.lower()] = value.strip()
                    reader.read(int(headers.get("content-length", "0")))
                    if method == "POST":
                        writes.append((path, headers.get("riverhog-expected-storage-incarnation")))
                        return
                    assert method == "GET" and path == "/v1/adapter"
                    body = canonical_json_bytes(_DESCRIPTOR.model_dump(mode="json"))
                    self.request.sendall(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                        + str(len(body)).encode("ascii")
                        + b"\r\n\r\n"
                        + body
                    )

        def http2(self) -> None:
            connection = H2Connection(
                config=H2Configuration(client_side=False, header_encoding="ascii")
            )
            connection.initiate_connection()
            self.request.sendall(connection.data_to_send())
            requests = {}
            while data := self.request.recv(65536):
                for event in connection.receive_data(data):
                    if isinstance(event, RequestReceived):
                        requests[event.stream_id] = dict(event.headers)
                    elif isinstance(event, DataReceived):
                        connection.acknowledge_received_data(
                            event.flow_controlled_length, event.stream_id
                        )
                    elif isinstance(event, StreamEnded):
                        headers = requests.pop(event.stream_id)
                        if headers[":method"] == "POST":
                            writes.append(
                                (
                                    headers[":path"],
                                    headers.get("riverhog-expected-storage-incarnation"),
                                )
                            )
                            if len(writes) == 1:
                                # The body has crossed the wire. The connection pool
                                # can replay this stream after this GOAWAY response.
                                connection.close_connection(last_stream_id=1)
                                self.request.sendall(connection.data_to_send())
                                return
                            body = canonical_json_bytes(
                                _response(operation).model_dump(mode="json")
                            )
                        else:
                            assert headers[":path"] == "/v1/adapter"
                            body = canonical_json_bytes(_DESCRIPTOR.model_dump(mode="json"))
                        connection.send_headers(
                            event.stream_id,
                            [
                                (":status", "200"),
                                ("content-type", "application/json"),
                                ("content-length", str(len(body))),
                            ],
                        )
                        connection.send_data(event.stream_id, body, end_stream=True)
                self.request.sendall(connection.data_to_send())

    server = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=lambda: server.serve_forever(poll_interval=0.01),
        name="adapter-goaway-wire",
    )
    thread.start()
    try:
        yield (
            f"https://127.0.0.1:{server.server_address[1]}",
            certificate,
            writes,
            protocols,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert errors == []


@pytest.mark.parametrize("operation", ["begin", "part", "complete", "put"])
def test_owned_adapter_transport_never_resubmits_a_sent_request_after_connection_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    record_property: Callable[[str, object], None],
) -> None:
    with _goaway_wire(tmp_path, operation) as (endpoint, certificate, writes, protocols):
        monkeypatch.setenv("SSL_CERT_FILE", str(certificate))
        monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
        client = StorageAdapterClient(endpoint, token="fixture-adapter-token", timeout=2)
        try:
            with pytest.raises(StorageAdapterProtocolError):
                _invoke(client, operation)
        finally:
            client.close()
            record_property("write_requests", len(writes))
            record_property("negotiated_protocols", ",".join(protocols))
    assert len(writes) == 1
    assert writes[0][1] == _INCARNATION
    assert protocols == ["http/1.1"]
