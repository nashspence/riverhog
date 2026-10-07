"""Resumable byte transport and isolated preparation of native metadata calls."""

from __future__ import annotations

import hashlib
import secrets
import threading
import time
from collections.abc import Callable, Hashable, Sequence
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeout
from typing import Any, Protocol, cast

from pydantic import BaseModel, JsonValue
from riverhog_canonical_json import canonical_json_bytes, parse_identity_json

from http_api_contracts import HttpOperationContract, http_operation_for_request
from http_api_contracts.control import ControlBudgetExhausted, control_timeout
from http_api_contracts.metadata_documents import (
    METADATA_CHUNK_BYTES,
    METADATA_HTTP_ERRORS,
    MetadataCall,
    MetadataCallStatus,
    MetadataChunk,
    MetadataDocumentRef,
    MetadataDocumentStatus,
    metadata_reference,
)
from http_api_contracts.metadata_staging import transient_fields


class MetadataPreparationPending(ControlBudgetExhausted):
    """The same exact metadata computation retains its actual continuation."""


class MetadataRemoteFailure(RuntimeError):
    def __init__(self, *, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


class MetadataWire(Protocol):
    def __call__[T: BaseModel](
        self, method: str, path: str, model: type[T], payload: BaseModel | None = None
    ) -> T: ...


class ProtocolRejection(Protocol):
    failure_kind: str
    observed_status: int
    code: str
    message: str


class ProtocolErrorFactory(Protocol):
    def __call__(
        self,
        message: str,
        *,
        failure_kind: str,
        code: str | None = None,
        observed_status: int | None = None,
    ) -> RuntimeError: ...


class MetadataExchange:
    """Prepare the whole native object; every remote operation remains bounded.

    The caller runs this computation outside its scheduler. Remote checkpoints
    let a restarted caller continue a large transfer without resending its prefix.
    No timeout, page size or preferred batching changes the semantic document.
    """

    def __init__(self, wire: MetadataWire) -> None:
        self.wire = wire

    def _upload(self, raw: bytes) -> MetadataDocumentRef:
        reference = metadata_reference(raw)
        path = f"/v1/metadata/documents/{reference.sha256}"
        try:
            status = self.wire("GET", path, MetadataDocumentStatus)
        except MetadataRemoteFailure as exc:
            if exc.status != 404 or exc.code != "metadata_not_found":
                raise
            status = MetadataDocumentStatus(
                document=reference, received_bytes="0", state="receiving"
            )
        while True:
            if status.document != reference:
                raise ValueError("metadata upload changed its whole exact document")
            if status.state == "complete":
                return reference
            if status.state == "invalid":
                raise ValueError("metadata upload rejected its canonical document")
            if status.state == "receiving":
                offset = int(status.received_bytes)
                status = self.wire(
                    "PUT",
                    path,
                    MetadataDocumentStatus,
                    MetadataChunk.from_bytes(
                        reference,
                        offset,
                        raw[offset : offset + METADATA_CHUNK_BYTES],
                    ),
                )
            else:
                time.sleep(0.05)
                status = self.wire("GET", path, MetadataDocumentStatus)

    def call[T: BaseModel](
        self,
        method: str,
        path: str,
        model_type: type[T],
        payload: BaseModel | None,
        live_transient: Callable[[], dict[str, JsonValue]],
    ) -> T:
        def checked_wire[M: BaseModel](
            method: str, path: str, model: type[M], payload: BaseModel | None = None
        ) -> M:
            while True:
                live_transient()
                try:
                    return self.wire(method, path, model, payload)
                except MetadataRemoteFailure as exc:
                    if exc.status != 503 or exc.code != "metadata_unavailable":
                        raise
                    # Preserve the same exact transport continuation while
                    # actual owner preparation capacity remains occupied.
                    time.sleep(0.05)

        exchange = MetadataExchange(checked_wire)
        document = None
        if payload is not None:
            excluded = {
                name
                for name, field in type(payload).model_fields.items()
                if (field.alias or name) in transient_fields(type(payload))
            }
            raw = canonical_json_bytes(
                payload.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude=excluded,
                )
            )
            document = exchange._upload(raw)
        call_id = secrets.token_hex(32)
        call_path = f"/v1/metadata/calls/{call_id}"
        while True:
            status = checked_wire(
                "POST",
                call_path,
                MetadataCallStatus,
                MetadataCall.model_validate(
                    {
                        "method": method,
                        "path": path,
                        "document": document,
                        "transient": live_transient(),
                    }
                ),
            )
            if status.call_id != call_id:
                raise ValueError("metadata reply changed its exact transport call")
            if status.state == "failed":
                assert status.status is not None and status.code is not None
                assert status.message is not None
                raise MetadataRemoteFailure(
                    status=status.status, code=status.code, message=status.message
                )
            if status.state == "ready":
                break
            time.sleep(0.05)
        reference, offset, digest, parts = status.response, 0, hashlib.sha256(), []
        assert reference is not None
        while offset < int(reference.bytes):
            chunk = checked_wire(
                "GET",
                f"/v1/metadata/documents/{reference.sha256}/chunks/{offset}",
                MetadataChunk,
            )
            if chunk.document != reference or int(chunk.offset) != offset:
                raise ValueError("metadata response changed its exact document continuation")
            raw = chunk.decoded()
            digest.update(raw)
            parts.append(raw)
            offset += len(raw)
        raw = b"".join(parts)
        if offset != int(reference.bytes) or digest.hexdigest() != reference.sha256:
            raise ValueError("metadata response differs from its complete byte commitment")
        if canonical_json_bytes(parse_identity_json(raw)) != raw:
            raise ValueError("metadata response is not its canonical exact document")
        return model_type.model_validate(parse_identity_json(raw))


def metadata_routing_key(method: str, path: str, payload: BaseModel | None) -> Hashable:
    """A local continuation key, never a semantic identity or authorization fact."""
    if payload is None:
        return method, path
    for name in ("request_sha256", "invocation_sha256", "request_id", "job_id", "departure_id"):
        # ObservationInvocation.job_id is an expensive whole-document property;
        # its exact request and fencing declaration give the local routing key.
        if name == "job_id" and "job_id" not in type(payload).model_fields:
            continue
        value = getattr(payload, name, None)
        if value is not None:
            return method, path, type(payload), value
    request = getattr(payload, "request", None)
    if request is not None:
        claim_id, fence = getattr(payload, "claim_id", None), getattr(payload, "fence", None)
        if claim_id is None or fence is None:
            raise ValueError("metadata invocation has no exact claim generation")
        return (
            method,
            path,
            request.request_id,
            claim_id,
            fence,
        )
    raise ValueError("metadata payload has no exact native continuation binding")


class ResumableMetadataCalls:
    """Finite actual metadata capacity; keep late results for their exact caller.

    Preparation executes without the scheduler's deadline context. Individual
    HTTP contacts still have finite allowances. A caller timeout never drops or
    restarts a still-running transfer, decoder, validator or response assembly.
    """

    def __init__(self, maximum_calls: int = 16) -> None:
        if type(maximum_calls) is not int or maximum_calls < 1:
            raise ValueError("metadata preparation capacity must be positive")
        self._maximum = maximum_calls
        self._lock = threading.Lock()
        self._active: dict[Hashable, tuple[Future[Any], dict[str, JsonValue]]] = {}
        self._closing = False

    def call[T](
        self,
        *,
        key: Hashable,
        payload: BaseModel | None,
        prepare: Callable[[Callable[[], dict[str, JsonValue]]], T],
        maximum_seconds: float,
    ) -> T:
        allowance = control_timeout(maximum_seconds)
        transient: dict[str, JsonValue] = {}
        if payload is not None:
            for name, field in type(payload).model_fields.items():
                alias = field.alias or name
                if alias in transient_fields(type(payload)):
                    transient[alias] = payload.model_dump(
                        mode="json",
                        by_alias=True,
                        include={name},
                    )[alias]
        with self._lock:
            if self._closing:
                raise MetadataPreparationPending("metadata preparation is shutting down")
            current = self._active.get(key)
            future: Future[T]
            live: dict[str, JsonValue]
            if current is None:
                # Completed continuations consume no actual preparation capacity.
                # Their native state is durable; an evicted caller may reread it.
                for completed in tuple(self._active):
                    if len(self._active) < self._maximum:
                        break
                    if self._active[completed][0].done():
                        del self._active[completed]
                if len(self._active) >= self._maximum:
                    raise MetadataPreparationPending("metadata preparation capacity is busy")
                future, live = Future[T](), {}
                self._active[key] = (future, live)
                start = True
            else:
                future, live = cast(Future[T], current[0]), current[1]
                start = False
            live.clear()
            live.update(transient)
        if start:

            def latest() -> dict[str, JsonValue]:
                with self._lock:
                    if self._closing:
                        raise MetadataPreparationPending("metadata preparation is shutting down")
                    return dict(live)

            def run() -> None:
                try:
                    future.set_result(prepare(latest))
                except BaseException as exc:
                    future.set_exception(exc)

            # A fresh thread has no inherited scheduler/control deadline.
            try:
                threading.Thread(target=run, daemon=True, name="metadata-preparation").start()
            except BaseException:
                with self._lock:
                    self._active.pop(key, None)
                raise
        try:
            value = future.result(timeout=allowance)
        except FutureTimeout as exc:
            if not future.done():
                raise MetadataPreparationPending(
                    "exact metadata preparation is continuing"
                ) from exc
            value = future.result()
        finally:
            if future.done():
                with self._lock:
                    if self._active.get(key, (None,))[0] is future:
                        del self._active[key]
        return value

    def close(self) -> None:
        with self._lock:
            self._closing = True
            active = tuple(self._active.values())
        for future, _ in active:
            try:
                future.result()
            except BaseException:
                pass
        with self._lock:
            self._active.clear()


class NativeMetadataTransport:
    """Stage exact native models without replacing their parser or semantics."""

    def __init__(
        self,
        *,
        wire: MetadataWire,
        operations: Sequence[HttpOperationContract],
        protocol_error: type[RuntimeError],
        timeout: float,
    ) -> None:
        self.wire = wire
        self.operations = tuple(operations)
        self.protocol_error = protocol_error
        self._error = cast(ProtocolErrorFactory, protocol_error)
        self.timeout = timeout
        self.calls = ResumableMetadataCalls()

    def _wire[T: BaseModel](
        self, method: str, path: str, model: type[T], payload: BaseModel | None = None
    ) -> T:
        try:
            return self.wire(method, path, model, payload)
        except self.protocol_error as exc:
            rejection = cast(ProtocolRejection, exc)
            if rejection.failure_kind != "remote_rejection":
                raise
            raise MetadataRemoteFailure(
                status=rejection.observed_status, code=rejection.code, message=rejection.message
            ) from exc

    def request[T: BaseModel](
        self, method: str, path: str, model: type[T], payload: BaseModel | None = None
    ) -> T:
        operation = http_operation_for_request(self.operations, method, path)
        if operation is None or path.startswith("/v1/metadata/"):
            raise ValueError("staged request has no native operation")
        try:
            return self.calls.call(
                key=metadata_routing_key(method, path, payload),
                payload=payload,
                prepare=lambda latest: MetadataExchange(self._wire).call(
                    method, path, model, payload, latest
                ),
                maximum_seconds=self.timeout,
            )
        except MetadataRemoteFailure as exc:
            declared = operation.accepts_error(status=exc.status, code=exc.code) or any(
                error.status == exc.status and error.code == exc.code
                for error in METADATA_HTTP_ERRORS
            )
            if not declared:
                raise self._error(
                    "staged owner returned an undeclared native error",
                    failure_kind="invalid_response",
                ) from exc
            raise self._error(
                exc.message,
                failure_kind="remote_rejection",
                code=exc.code,
                observed_status=exc.status,
            ) from exc
        except (TypeError, ValueError) as exc:
            raise self._error(
                "staged owner returned invalid exact metadata", failure_kind="invalid_response"
            ) from exc

    def close(self) -> None:
        self.calls.close()
