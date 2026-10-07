from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass

import pytest
from http_api_contracts import HttpOperationContract
from http_api_contracts.control import control_budget
from http_api_contracts.metadata_binding import METADATA_CONTROL_MAXIMUM_BYTES, read_control_body
from http_api_contracts.metadata_documents import (
    METADATA_CHUNK_BYTES,
    MetadataCall,
    MetadataChunk,
    MetadataModel,
    metadata_reference,
)
from http_api_contracts.metadata_exchange import (
    MetadataExchange,
    MetadataPreparationPending,
    MetadataRemoteFailure,
    ResumableMetadataCalls,
)
from http_api_contracts.metadata_staging import CanonicalMetadataServer, MetadataStagingError
from pydantic import Field, model_validator
from riverhog_canonical_json import canonical_json_bytes, canonical_json_sha256, parse_identity_json


class ExactDocument(MetadataModel):
    records: tuple[str, ...]
    request_sha256: str

    @model_validator(mode="after")
    def exact(self):
        if self.request_sha256 != canonical_json_sha256({"records": list(self.records)}):
            raise ValueError("incorrect original identity")
        return self


class Access(MetadataModel):
    token: str = Field(repr=False)


class Request(ExactDocument):
    access: Access = Field(json_schema_extra={"x-riverhog-transient": True})


@dataclass
class Reply:
    status: int
    body: bytes


OPERATION = HttpOperationContract("POST", "/v1/facts", Request, ExactDocument, "json")


def request(count=1):
    records = tuple(f"{index:08d}:" + "x" * 600 for index in range(count))
    return Request(
        records=records,
        request_sha256=canonical_json_sha256({"records": list(records)}),
        access=Access(token="test-transient-token"),
    )


def server_at(root, executions):
    def execute(method, path, raw):
        parsed = Request.model_validate(parse_identity_json(raw))
        executions.append((method, path, parsed))
        return Reply(200, canonical_json_bytes(parsed.model_dump(mode="json", exclude={"access"})))

    return CanonicalMetadataServer(root=root, operations=(OPERATION,), execute=execute)


def wire_for(server, segments=None):
    def wire(method, path, model, payload=None):
        raw = b"" if payload is None else canonical_json_bytes(payload.model_dump(mode="json"))
        if segments is not None and isinstance(payload, MetadataChunk):
            segments.append((int(payload.offset), len(payload.decoded()), len(raw)))
        try:
            return model.model_validate(server.handle(method, path, raw).model_dump(mode="json"))
        except MetadataStagingError as exc:
            raise MetadataRemoteFailure(
                status=exc.status, code=exc.code, message=exc.message
            ) from exc

    return wire


def wait_document(server, digest):
    deadline = time.monotonic() + 10
    while (status := server.document_status(digest)).state == "verifying":
        assert time.monotonic() < deadline
        time.sleep(0.01)
    return status


def wait_call(server, call_id):
    deadline = time.monotonic() + 10
    while (status := server.call_status(call_id)).state == "pending":
        assert time.monotonic() < deadline
        time.sleep(0.01)
    return status


def test_whole_canonical_question_and_reply_exceed_inline_limit_without_clipping(tmp_path):
    executions, segments = [], []
    server = server_at(tmp_path, executions)
    original = request(8192)
    raw = canonical_json_bytes(original.model_dump(mode="json", exclude={"access"}))
    assert len(raw) > 4 * 1024 * 1024
    try:
        result = MetadataExchange(wire_for(server, segments)).call(
            "POST",
            "/v1/facts",
            ExactDocument,
            original,
            lambda: {"access": original.access.model_dump(mode="json")},
        )
        assert result.records == original.records
        assert result.request_sha256 == original.request_sha256
        assert len(executions) == 1 and executions[0][2] == original
        assert len(segments) > 64
        assert all(
            decoded <= METADATA_CHUNK_BYTES and encoded < 100000 for _, decoded, encoded in segments
        )
        assert canonical_json_bytes(result.model_dump(mode="json")) == raw
        assert all(b"test-transient-token" not in path.read_bytes() for path in tmp_path.iterdir())
    finally:
        server.close()


def test_control_body_reader_stops_before_buffering_an_unbounded_request():
    import asyncio
    from types import SimpleNamespace

    consumed = []

    async def stream():
        for index in range(100):
            consumed.append(index)
            yield b"x" * METADATA_CHUNK_BYTES

    request = SimpleNamespace(
        url=SimpleNamespace(path="/v1/metadata/calls/" + "a" * 64), stream=stream
    )
    with pytest.raises(MetadataStagingError) as rejected:
        asyncio.run(read_control_body(request, maximum_request_bytes=16 * 1024 * 1024))
    assert rejected.value.code == "invalid_metadata"
    assert len(consumed) == METADATA_CONTROL_MAXIMUM_BYTES // METADATA_CHUNK_BYTES + 1


def test_required_null_is_preserved_in_the_exact_native_document(tmp_path):
    from typing import Literal

    class Nullable(MetadataModel):
        value: Literal[None]

    operation = HttpOperationContract("POST", "/v1/nullable", Nullable, Nullable, "json")
    received = []

    def execute(_method, _path, raw):
        parsed = Nullable.model_validate(parse_identity_json(raw))
        received.append(parsed)
        return Reply(200, canonical_json_bytes(parsed.model_dump(mode="json")))

    server = CanonicalMetadataServer(root=tmp_path, operations=(operation,), execute=execute)
    try:
        result = MetadataExchange(wire_for(server)).call(
            "POST",
            "/v1/nullable",
            Nullable,
            Nullable(value=None),
            lambda: {},
        )
        assert received == [Nullable(value=None)] and result == received[0]
        reference = metadata_reference(b'{"value":null}')
        assert server.get_chunk(reference.sha256, 0).decoded() == b'{"value":null}'
    finally:
        server.close()


def test_restart_and_lost_upload_ack_resume_exact_committed_prefix(tmp_path):
    executions = []
    server = server_at(tmp_path, executions)
    original = request(512)
    raw = canonical_json_bytes(original.model_dump(mode="json", exclude={"access"}))
    reference = metadata_reference(raw)
    first = MetadataChunk.from_bytes(reference, 0, raw[:METADATA_CHUNK_BYTES])
    assert server.put_chunk(reference.sha256, first).received_bytes == str(METADATA_CHUNK_BYTES)
    assert server.put_chunk(reference.sha256, first).received_bytes == str(METADATA_CHUNK_BYTES)
    # A crash after writing bytes but before checkpointing may leave a tail.
    with (tmp_path / f"{reference.sha256}.data").open("ab") as stream:
        stream.write(b"uncommitted tail")
    server.close()
    server = server_at(tmp_path, executions)
    segments = []
    try:
        result = MetadataExchange(wire_for(server, segments)).call(
            "POST",
            "/v1/facts",
            ExactDocument,
            original,
            lambda: {"access": original.access.model_dump(mode="json")},
        )
        assert segments[0][0] == METADATA_CHUNK_BYTES
        assert result.records == original.records and len(executions) == 1
        assert (tmp_path / f"{reference.sha256}.data").read_bytes() == raw
    finally:
        server.close()


@pytest.mark.parametrize("alteration", ["gap", "replay", "truncated"])
def test_incomplete_or_changed_transport_never_substitutes_bytes(tmp_path, alteration):
    server = server_at(tmp_path, [])
    raw = canonical_json_bytes({"records": ["x" * 100000]})
    reference = metadata_reference(raw)
    first = MetadataChunk.from_bytes(reference, 0, raw[:METADATA_CHUNK_BYTES])
    try:
        server.put_chunk(reference.sha256, first)
        if alteration == "truncated":
            (tmp_path / f"{reference.sha256}.data").write_bytes(b"lost committed prefix")
            changed = first
        elif alteration == "gap":
            changed = MetadataChunk.from_bytes(reference, METADATA_CHUNK_BYTES + 1, raw[-10:])
        else:
            changed = MetadataChunk.from_bytes(reference, 0, b"changed")
        with pytest.raises(MetadataStagingError) as error:
            server.put_chunk(reference.sha256, changed)
        assert error.value.code == "metadata_mismatch"
    finally:
        server.close()


@pytest.mark.parametrize("alteration", ["native-digest", "noncanonical", "stored-corruption"])
def test_owner_fully_validates_exact_complete_document_before_use(tmp_path, alteration):
    executions = []
    server = server_at(tmp_path, executions)
    original = request()
    document = original.model_dump(mode="json", exclude={"access"})
    if alteration == "native-digest":
        document["request_sha256"] = "0" * 64
    raw = canonical_json_bytes(document)
    if alteration == "noncanonical":
        raw += b"\n"
    reference = metadata_reference(raw)
    try:
        server.put_chunk(reference.sha256, MetadataChunk.from_bytes(reference, 0, raw))
        status = wait_document(server, reference.sha256)
        if alteration == "noncanonical":
            assert status.state == "invalid" and not executions
            return
        assert status.state == "complete"
        if alteration == "stored-corruption":
            (tmp_path / f"{reference.sha256}.data").write_bytes(
                raw.replace(b"xxxxxxxx", b"yyyyyyyy", 1)
            )
        server.call(
            "a" * 64,
            MetadataCall(
                method="POST",
                path="/v1/facts",
                document=reference,
                transient={"access": original.access.model_dump(mode="json")},
            ),
        )
        reply = wait_call(server, "a" * 64)
        assert reply.state == "failed" and reply.code == "invalid_metadata"
        assert not executions
    finally:
        server.close()


def test_slow_preparation_retains_actual_capacity_and_allows_independent_work():
    entered, release = threading.Event(), threading.Event()
    calls = ResumableMetadataCalls(maximum_calls=2)
    starts = []

    def held(_latest):
        starts.append("held")
        entered.set()
        assert release.wait(5)
        return "same complete result"

    try:
        for _ in range(2):
            started = time.monotonic()
            with pytest.raises(MetadataPreparationPending), control_budget(0.01):
                calls.call(key="held", payload=None, prepare=held, maximum_seconds=5)
            assert time.monotonic() - started < 0.5
        assert entered.is_set() and starts == ["held"]
        assert (
            calls.call(key="healthy", payload=None, prepare=lambda _: "healthy", maximum_seconds=1)
            == "healthy"
        )
        release.set()
        assert (
            calls.call(key="held", payload=None, prepare=held, maximum_seconds=1)
            == "same complete result"
        )
        assert starts == ["held"]
    finally:
        release.set()
        calls.close()


def test_transport_expiration_reclaims_only_idle_bytes_and_leaves_native_facts(tmp_path):
    executions = []
    server = server_at(tmp_path, executions)
    try:
        original = request()
        result = MetadataExchange(wire_for(server)).call(
            "POST",
            "/v1/facts",
            ExactDocument,
            original,
            lambda: {"access": original.access.model_dump(mode="json")},
        )
        assert result.request_sha256 == original.request_sha256
        reference = metadata_reference(canonical_json_bytes(result.model_dump(mode="json")))
        expiration = time.time() - server.retention_seconds - 10
        for path in tmp_path.iterdir():
            os.utime(path, (expiration, expiration))
        server.document_status(reference.sha256)
        server.prune_expired()
        assert server.get_chunk(reference.sha256, 0).decoded()
        assert len(executions) == 1 and executions[0][2] == original
        path = tmp_path / f"{reference.sha256}.document.json"
        os.utime(path, (expiration, expiration))
        server.prune_expired()
        assert not tuple(tmp_path.iterdir())
        result = MetadataExchange(wire_for(server)).call(
            "POST",
            "/v1/facts",
            ExactDocument,
            original,
            lambda: {"access": original.access.model_dump(mode="json")},
        )
        assert result.request_sha256 == original.request_sha256
    finally:
        server.close()
