"""Transport-only references and bounded segments of exact canonical documents.

The document's existing typed contract still owns its meaning and authority.
These references commit transport bytes; they grant no permission or approval.
"""

from __future__ import annotations

import base64
import hashlib
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from http_api_contracts import HttpErrorContract, HttpOperationContract, HttpPathParameterContract

MetadataDigest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
MetadataOffset = Annotated[str, Field(pattern=r"^(0|[1-9][0-9]*)$")]
METADATA_CHUNK_BYTES = 64 * 1024
_SEGMENT_EXTENT: dict[str, Any] = {
    "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "reason": "bounded-canonical-metadata-transport",
        "progression": "exact-document-byte-offset",
    }
}


class MetadataModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MetadataDocumentRef(MetadataModel):
    sha256: MetadataDigest
    bytes: MetadataOffset


class MetadataChunk(MetadataModel):
    document: MetadataDocumentRef
    offset: MetadataOffset
    data: str = Field(
        max_length=4 * ((METADATA_CHUNK_BYTES + 2) // 3), json_schema_extra=_SEGMENT_EXTENT
    )

    def decoded(self) -> bytes:
        try:
            raw = base64.b64decode(self.data, validate=True)
        except (ValueError, TypeError) as exc:
            raise ValueError("metadata segment is not canonical base64") from exc
        if base64.b64encode(raw).decode("ascii") != self.data:
            raise ValueError("metadata segment is not canonical base64")
        return raw

    @model_validator(mode="after")
    def bounded_segment(self) -> Self:
        raw = self.decoded()
        if (
            len(raw) > METADATA_CHUNK_BYTES
            or int(self.offset) + len(raw) > int(self.document.bytes)
            or not raw
            and int(self.offset) != int(self.document.bytes)
        ):
            raise ValueError("metadata segment exceeds its document or makes no progress")
        return self

    @classmethod
    def from_bytes(cls, document: MetadataDocumentRef, offset: int, data: bytes) -> Self:
        return cls(
            document=document, offset=str(offset), data=base64.b64encode(data).decode("ascii")
        )


class MetadataDocumentStatus(MetadataModel):
    document: MetadataDocumentRef
    received_bytes: MetadataOffset
    state: Literal["receiving", "verifying", "complete", "invalid"]

    @model_validator(mode="after")
    def exact_extent(self) -> Self:
        if int(self.received_bytes) > int(self.document.bytes) or (
            self.state != "receiving" and self.received_bytes != self.document.bytes
        ):
            raise ValueError("metadata staging status exceeds its exact document")
        return self


class MetadataCall(MetadataModel):
    method: Literal["GET", "POST", "PUT", "DELETE", "PATCH"]
    path: str = Field(min_length=4, max_length=2048, pattern=r"^/v1/")
    document: MetadataDocumentRef | None = None
    # The owning request type declares its transient fields. They never enter
    # staged bytes, persisted tickets, or document identities.
    transient: dict[str, JsonValue] = Field(default_factory=dict, repr=False)


class MetadataCallStatus(MetadataModel):
    call_id: MetadataDigest
    state: Literal["pending", "ready", "failed"]
    response: MetadataDocumentRef | None = None
    status: int | None = Field(default=None, ge=400, le=599)
    code: str | None = Field(default=None, min_length=1, max_length=160)
    message: str | None = Field(default=None, min_length=1, max_length=1000)

    @model_validator(mode="after")
    def terminal_shape(self) -> Self:
        if (self.response is not None) != (self.state == "ready") or (
            all(value is not None for value in (self.status, self.code, self.message))
        ) != (self.state == "failed"):
            raise ValueError("metadata call status has an inconsistent response")
        if self.state != "failed" and any(
            value is not None for value in (self.status, self.code, self.message)
        ):
            raise ValueError("metadata call carries undeclared failure material")
        return self


def metadata_reference(raw: bytes) -> MetadataDocumentRef:
    return MetadataDocumentRef(sha256=hashlib.sha256(raw).hexdigest(), bytes=str(len(raw)))


METADATA_HTTP_ERRORS = (
    HttpErrorContract("invalid_metadata", 400),
    HttpErrorContract("unauthorized", 401),
    HttpErrorContract("metadata_not_found", 404),
    HttpErrorContract("metadata_mismatch", 409),
    HttpErrorContract("metadata_unavailable", 503),
    HttpErrorContract("metadata_failed", 500),
)
_DIGEST_PATH = (HttpPathParameterContract("document_sha256", MetadataDigest),)
_CALL_PATH = (HttpPathParameterContract("metadata_call_id", MetadataDigest),)
METADATA_HTTP_OPERATIONS = (
    HttpOperationContract(
        "PUT",
        "/v1/metadata/documents/{document_sha256}",
        MetadataChunk,
        MetadataDocumentStatus,
        "json",
        errors=METADATA_HTTP_ERRORS,
        path_parameters=_DIGEST_PATH,
    ),
    HttpOperationContract(
        "GET",
        "/v1/metadata/documents/{document_sha256}",
        response_type=MetadataDocumentStatus,
        errors=METADATA_HTTP_ERRORS,
        path_parameters=_DIGEST_PATH,
    ),
    HttpOperationContract(
        "GET",
        "/v1/metadata/documents/{document_sha256}/chunks/{offset}",
        response_type=MetadataChunk,
        errors=METADATA_HTTP_ERRORS,
        path_parameters=(*_DIGEST_PATH, HttpPathParameterContract("offset", MetadataOffset)),
    ),
    HttpOperationContract(
        "POST",
        "/v1/metadata/calls/{metadata_call_id}",
        MetadataCall,
        MetadataCallStatus,
        "json",
        errors=METADATA_HTTP_ERRORS,
        path_parameters=_CALL_PATH,
    ),
    HttpOperationContract(
        "GET",
        "/v1/metadata/calls/{metadata_call_id}",
        response_type=MetadataCallStatus,
        errors=METADATA_HTTP_ERRORS,
        path_parameters=_CALL_PATH,
    ),
)
