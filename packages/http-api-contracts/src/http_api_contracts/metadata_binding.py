"""Framework-neutral adapter for a component's staged metadata transport."""

from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel
from riverhog_canonical_json import canonical_json_bytes

from http_api_contracts import HttpOperationContract
from http_api_contracts.metadata_staging import (
    CanonicalMetadataServer,
    MetadataResponse,
    MetadataStagingError,
)

METADATA_CONTROL_MAXIMUM_BYTES = 128 * 1024


class ControlURL(Protocol):
    @property
    def path(self) -> str: ...


class ControlRequest(Protocol):
    @property
    def url(self) -> ControlURL: ...

    def stream(self) -> AsyncIterator[bytes]: ...


class ResponseFactory[Response: MetadataResponse](Protocol):
    def __call__(
        self, *, status: int, headers: tuple[tuple[str, str], ...], body: bytes
    ) -> Response: ...


async def read_control_body(request: ControlRequest, *, maximum_request_bytes: int) -> bytes:
    """Stop decoding control input at its actual bounded transport extent."""
    staged = request.url.path.startswith("/v1/metadata/")
    limit = METADATA_CONTROL_MAXIMUM_BYTES if staged else maximum_request_bytes
    parts, size = [], 0
    async for part in request.stream():
        size += len(part)
        if size > limit:
            raise MetadataStagingError(
                400 if staged else 413,
                "invalid_metadata" if staged else "request_too_large",
                "control request exceeds its bounded transport extent",
            )
        parts.append(part)
    return b"".join(parts)


class MetadataHttpBinding[Response: MetadataResponse]:
    def __init__(
        self,
        *,
        owner: object,
        root: Path | None,
        operations: Sequence[HttpOperationContract],
        execute: Callable[[str, str, bytes], MetadataResponse],
        response: ResponseFactory[Response],
        execute_model: Callable[[str, str, BaseModel], MetadataResponse] | None = None,
    ) -> None:
        self._response = response
        self.server: CanonicalMetadataServer | None = None
        if root is None:
            root = getattr(owner, "state_root", None)
            if root is not None:
                root = root / "metadata"
        if root is not None:
            self.server = CanonicalMetadataServer(
                root=root, operations=operations, execute=execute, execute_model=execute_model
            )
            register = getattr(owner, "register_metadata_shutdown", None)
            if register is not None:
                register(self.server.close)

    def handle(self, method: str, path: str, body: bytes) -> Response:
        document: dict[str, Any]
        try:
            if len(body) > METADATA_CONTROL_MAXIMUM_BYTES:
                raise MetadataStagingError(400, "invalid_metadata", "metadata segment is too large")
            if self.server is None:
                raise MetadataStagingError(
                    503, "metadata_unavailable", "metadata staging is unavailable"
                )
            model = self.server.handle(method.upper(), path, body)
            document = model.model_dump(mode="json", exclude_none=True)
            status = 200
        except (ValueError, TypeError):
            status = 400
            document = {
                "error": {"code": "invalid_metadata", "message": "invalid metadata segment"}
            }
        except MetadataStagingError as exc:
            status = exc.status
            document = {"error": {"code": exc.code, "message": exc.message}}
        except Exception:
            status = 500
            document = {"error": {"code": "metadata_failed", "message": "metadata staging failed"}}
        return self._response(
            status=status,
            headers=(("Content-Type", "application/json"),),
            body=canonical_json_bytes(document),
        )

    def close(self) -> None:
        if self.server is not None:
            self.server.close()
