"""Component-owned resumable metadata transport, separate from semantic jobs.

Only transport bytes and replay markers are stored here. Native request/result
models, their identities, live authorization and effects remain with the owner.
"""

from __future__ import annotations

import os
import re
import secrets
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, TypeAdapter
from riverhog_canonical_json import canonical_json_bytes, parse_identity_json

from http_api_contracts import HttpOperationContract, http_operation_for_request
from http_api_contracts.control import finite_control_seconds
from http_api_contracts.metadata_documents import (
    METADATA_CHUNK_BYTES,
    MetadataCall,
    MetadataCallStatus,
    MetadataChunk,
    MetadataDocumentRef,
    MetadataDocumentStatus,
    MetadataModel,
    metadata_reference,
)


class MetadataStagingError(RuntimeError):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def transient_fields(model_type: object) -> set[str]:
    return {
        field.alias or name
        for name, field in getattr(model_type, "model_fields", {}).items()
        if isinstance(field.json_schema_extra, dict)
        and field.json_schema_extra.get("x-riverhog-transient") is True
    }


def _atomic(
    root: Path,
    path: Path,
    raw: bytes,
    *,
    commit_lock: AbstractContextManager[Any] | None = None,
    after_replace: Callable[[], None] | None = None,
) -> None:
    if path.parent != root or path.is_symlink():
        raise ValueError("metadata state path is outside its owning component")
    temporary = root / ("." + secrets.token_hex(16) + ".part")
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        with commit_lock if commit_lock is not None else nullcontext():
            os.replace(temporary, path)
            if after_replace is not None:
                after_replace()
            directory = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


class MetadataResponse(Protocol):
    @property
    def status(self) -> int: ...

    @property
    def body(self) -> bytes: ...


class CanonicalMetadataServer:
    def __init__(
        self,
        *,
        root: Path,
        operations: Sequence[HttpOperationContract],
        execute: Callable[[str, str, bytes], MetadataResponse],
        maximum_workers: int = 2,
        retention_seconds: float = 24 * 60 * 60,
    ) -> None:
        if type(maximum_workers) is not int or maximum_workers < 1:
            raise ValueError("metadata preparation capacity must be positive")
        if root.is_symlink():
            raise ValueError("metadata staging root must not be a symlink")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.root = root.resolve()
        self.operations, self.execute = tuple(operations), execute
        self._lock = threading.RLock()
        self._workers = ThreadPoolExecutor(
            max_workers=maximum_workers, thread_name_prefix="metadata"
        )
        self._maximum = maximum_workers
        self._active: dict[str, Future[object]] = {}
        self._closing = False
        self.retention_seconds = finite_control_seconds(retention_seconds)
        self._next_prune = 0.0

    def _path(self, key: str, suffix: str) -> Path:
        if re.fullmatch(r"[a-f0-9]{64}", key) is None:
            raise ValueError("metadata transport key is not a digest")
        path = self.root / f"{key}.{suffix}"
        if path.is_symlink():
            raise ValueError("metadata transport path is a symlink")
        return path

    def _save(self, key: str, suffix: str, model: BaseModel) -> None:
        _atomic(
            self.root, self._path(key, suffix), canonical_json_bytes(model.model_dump(mode="json"))
        )

    def _load[T: BaseModel](self, key: str, suffix: str, model_type: type[T]) -> T | None:
        path = self._path(key, suffix)
        return model_type.model_validate_json(path.read_bytes()) if path.exists() else None

    def _start(self, key: str, operation: Callable[[], object]) -> None:
        with self._lock:
            for done in tuple(self._active):
                if self._active[done].done():
                    del self._active[done]
            if key in self._active:
                return
            if self._closing or len(self._active) >= self._maximum:
                raise MetadataStagingError(503, "metadata_unavailable", "metadata capacity is busy")
            self._active[key] = self._workers.submit(operation)

    def document_status(self, digest: str) -> MetadataDocumentStatus:
        with self._lock:
            status = self._load(digest, "document.json", MetadataDocumentStatus)
            if status is not None:
                self._path(digest, "document.json").touch()
        if status is None:
            raise MetadataStagingError(
                404, "metadata_not_found", "metadata document is unavailable"
            )
        if status.state == "verifying":
            self._start("document:" + digest, lambda: self._verify_document(status))
        return status

    def put_chunk(self, digest: str, chunk: MetadataChunk) -> MetadataDocumentStatus:
        if chunk.document.sha256 != digest:
            raise MetadataStagingError(
                409, "metadata_mismatch", "metadata path changed its document"
            )
        raw = chunk.decoded()
        with self._lock:
            status = self._load(digest, "document.json", MetadataDocumentStatus)
            if status is None:
                status = MetadataDocumentStatus(
                    document=chunk.document, received_bytes="0", state="receiving"
                )
            if status.document != chunk.document or status.state == "invalid":
                raise MetadataStagingError(409, "metadata_mismatch", "metadata document changed")
            offset, received = int(chunk.offset), int(status.received_bytes)
            path = self._path(digest, "data")
            descriptor = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
            with os.fdopen(descriptor, "r+b") as stream:
                if os.fstat(stream.fileno()).st_size < received:
                    raise MetadataStagingError(
                        409, "metadata_mismatch", "committed metadata bytes are missing"
                    )
                if offset < received:
                    stream.seek(offset)
                    if offset + len(raw) > received or stream.read(len(raw)) != raw:
                        raise MetadataStagingError(
                            409, "metadata_mismatch", "metadata replay changed committed bytes"
                        )
                    return status
                if offset != received:
                    raise MetadataStagingError(
                        409, "metadata_mismatch", "metadata segment has a gap"
                    )
                if status.state != "receiving":
                    if not raw:
                        return status
                    raise MetadataStagingError(
                        409, "metadata_mismatch", "metadata document is sealed"
                    )
                # Discard only an uncommitted tail left by an interrupted write.
                stream.truncate(received)
                stream.seek(received)
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            received += len(raw)
            status = MetadataDocumentStatus(
                document=chunk.document,
                received_bytes=str(received),
                state="verifying" if received == int(chunk.document.bytes) else "receiving",
            )
            self._save(digest, "document.json", status)
        if status.state == "verifying":
            self._start("document:" + digest, lambda: self._verify_document(status))
        return status

    def _read_exact(self, reference: MetadataDocumentRef) -> bytes:
        raw = self._path(reference.sha256, "data").read_bytes()
        if metadata_reference(raw) != reference:
            raise ValueError("complete staged metadata differs from its exact byte commitment")
        if canonical_json_bytes(parse_identity_json(raw)) != raw:
            raise ValueError("complete staged metadata is not its canonical document")
        return raw

    def _verify_document(self, status: MetadataDocumentStatus) -> None:
        try:
            self._read_exact(status.document)
        except (ValueError, OSError):
            state = "invalid"
        else:
            state = "complete"
        self._save(
            status.document.sha256, "document.json", status.model_copy(update={"state": state})
        )

    def get_chunk(self, digest: str, offset: int) -> MetadataChunk:
        status = self.document_status(digest)
        if status.state != "complete":
            raise MetadataStagingError(
                503, "metadata_unavailable", "metadata document is not complete"
            )
        if offset < 0 or offset > int(status.document.bytes):
            raise MetadataStagingError(400, "invalid_metadata", "metadata byte offset is invalid")
        with self._path(digest, "data").open("rb") as stream:
            stream.seek(offset)
            raw = stream.read(METADATA_CHUNK_BYTES)
        return MetadataChunk.from_bytes(status.document, offset, raw)

    def _publish(self, document: object) -> MetadataDocumentRef:
        raw = canonical_json_bytes(document)
        reference = metadata_reference(raw)

        def commit_status() -> None:
            self._save(
                reference.sha256,
                "document.json",
                MetadataDocumentStatus(
                    document=reference,
                    received_bytes=reference.bytes,
                    state="complete",
                ),
            )

        # Large writes stay outside the control lock. Only the byte-object
        # replacement and its small complete checkpoint commit together.
        _atomic(
            self.root,
            self._path(reference.sha256, "data"),
            raw,
            commit_lock=self._lock,
            after_replace=commit_status,
        )
        return reference

    def call(self, call_id: str, call: MetadataCall) -> MetadataCallStatus:
        operation = http_operation_for_request(self.operations, call.method, call.path)
        if operation is None or call.path.startswith("/v1/metadata/"):
            raise MetadataStagingError(
                400, "invalid_metadata", "metadata call has no native operation"
            )
        if (operation.request_type is not None) != (call.document is not None):
            raise MetadataStagingError(
                400, "invalid_metadata", "metadata call has an incorrect body"
            )
        if not set(call.transient) <= transient_fields(operation.request_type):
            raise MetadataStagingError(
                400, "invalid_metadata", "metadata call changed transient fields"
            )
        marker = call.model_copy(update={"transient": {}})
        with self._lock:
            previous = self._load(call_id, "call.json", MetadataCall)
            if previous is not None and previous != marker:
                raise MetadataStagingError(409, "metadata_mismatch", "metadata call was rebound")
            status = self._load(call_id, "reply.json", MetadataCallStatus)
            if status is not None and status.state != "pending":
                return status
            if call.document is not None:
                document = self._load(call.document.sha256, "document.json", MetadataDocumentStatus)
                if (
                    document is None
                    or document.document != call.document
                    or document.state != "complete"
                ):
                    raise MetadataStagingError(
                        503, "metadata_unavailable", "exact metadata is not complete"
                    )
                self._path(call.document.sha256, "document.json").touch()
            if previous is None:
                self._save(call_id, "call.json", marker)
            status = MetadataCallStatus(call_id=call_id, state="pending")
            self._save(call_id, "reply.json", status)
        self._start("call:" + call_id, lambda: self._execute_call(call_id, call, operation))
        return status

    def call_status(self, call_id: str) -> MetadataCallStatus:
        with self._lock:
            result = self._load(call_id, "reply.json", MetadataCallStatus)
            if result is not None:
                self._path(call_id, "reply.json").touch()
        if result is None:
            raise MetadataStagingError(404, "metadata_not_found", "metadata call is unavailable")
        return result

    def _execute_call(
        self, call_id: str, call: MetadataCall, operation: HttpOperationContract
    ) -> None:
        try:
            raw = b""
            if call.document is not None:
                document = parse_identity_json(self._read_exact(call.document))
                if not isinstance(document, dict) or set(document) & set(call.transient):
                    raise ValueError("metadata transient fields overlap staged fields")
                document.update(call.transient)
                # The actual owner's published parser validates the whole object.
                model: BaseModel = TypeAdapter(operation.request_type).validate_python(document)
                raw = canonical_json_bytes(model.model_dump(mode="json", by_alias=True))
        except (ValueError, TypeError, OSError):
            status = MetadataCallStatus(
                call_id=call_id,
                state="failed",
                status=400,
                code="invalid_metadata",
                message="complete metadata violates its exact native contract",
            )
            with self._lock:
                self._save(call_id, "reply.json", status)
            return
        try:
            response = self.execute(call.method, call.path, raw)
            if response.status >= 400:
                reply_document = parse_identity_json(response.body)
                error = reply_document.get("error") if isinstance(reply_document, dict) else None
                if not isinstance(error, dict):
                    raise ValueError("owner returned an invalid native error")
                code, message = error.get("code"), error.get("message")
                if not isinstance(code, str) or not isinstance(message, str):
                    raise ValueError("owner returned an invalid native error")
                if not operation.accepts_error(status=response.status, code=code):
                    raise ValueError("owner emitted an undeclared native error")
                status = MetadataCallStatus(
                    call_id=call_id,
                    state="failed",
                    status=response.status,
                    code=code,
                    message=message[:1000],
                )
            else:
                document = parse_identity_json(response.body)
                TypeAdapter(operation.response_type).validate_python(document)
                reference = self._publish(document)
                status = MetadataCallStatus(call_id=call_id, state="ready", response=reference)
        except (ValueError, TypeError):
            status = MetadataCallStatus(
                call_id=call_id,
                state="failed",
                status=500,
                code="metadata_failed",
                message="owner returned invalid complete metadata",
            )
        except Exception:
            status = MetadataCallStatus(
                call_id=call_id,
                state="failed",
                status=500,
                code="metadata_failed",
                message="metadata preparation failed",
            )
        with self._lock:
            self._save(call_id, "reply.json", status)

    def handle(self, method: str, path: str, raw: bytes) -> MetadataModel:
        """Return a typed bounded response; the HTTP adapter owns formatting/auth."""
        self._prune_due()
        document_match = re.fullmatch(
            r"/v1/metadata/documents/([a-f0-9]{64})(?:/chunks/(0|[1-9][0-9]*))?", path
        )
        call_match = re.fullmatch(r"/v1/metadata/calls/([a-f0-9]{64})", path)
        if document_match:
            digest, offset = document_match.groups()
            if method == "GET" and not raw:
                return (
                    self.document_status(digest)
                    if offset is None
                    else self.get_chunk(digest, int(offset))
                )
            if method == "PUT" and offset is None:
                return self.put_chunk(
                    digest, MetadataChunk.model_validate(parse_identity_json(raw))
                )
        if call_match:
            if method == "GET" and not raw:
                return self.call_status(call_match.group(1))
            if method == "POST":
                return self.call(
                    call_match.group(1), MetadataCall.model_validate(parse_identity_json(raw))
                )
        raise MetadataStagingError(
            400, "invalid_metadata", "metadata transport operation is invalid"
        )

    def _prune_due(self) -> None:
        with self._lock:
            observed = time.monotonic()
            if observed < self._next_prune:
                return
            try:
                self._start("transport-prune", self.prune_expired)
            except MetadataStagingError:
                return
            self._next_prune = observed + min(600, self.retention_seconds / 2)

    def prune_expired(self, *, now: float | None = None) -> None:
        """Reclaim idle transport bytes; native jobs retain their own exact state.

        Active transfer/read checkpoints refresh their transport lease. Expired
        staging is reconstructible from the same original document and native
        idempotent invocation, without authorizing any effect or changing it.
        """
        cutoff = (time.time() if now is None else now) - self.retention_seconds
        for path in self.root.glob("*.json"):
            if not path.name.endswith((".document.json", ".reply.json")):
                continue
            with self._lock:
                key = path.name[:64]
                if path.is_symlink():
                    continue
                try:
                    if path.stat().st_mtime > cutoff:
                        continue
                except FileNotFoundError:
                    continue
                protected = set()
                for active, future in self._active.items():
                    if future.done():
                        continue
                    if active.startswith("document:"):
                        protected.add(active.removeprefix("document:"))
                    elif active.startswith("call:"):
                        call_id = active.removeprefix("call:")
                        protected.add(call_id)
                        call = self._load(call_id, "call.json", MetadataCall)
                        if call is not None and call.document is not None:
                            protected.add(call.document.sha256)
                if key in protected:
                    continue
                suffixes = (
                    ("data", "document.json")
                    if path.name.endswith(".document.json")
                    else ("call.json", "reply.json")
                )
                for suffix in suffixes:
                    self._path(key, suffix).unlink(missing_ok=True)

    def close(self) -> None:
        with self._lock:
            self._closing = True
        # These workers perform validation/control metadata only. Ownership is
        # not released while an actual computation or contact remains active.
        self._workers.shutdown(wait=True)
