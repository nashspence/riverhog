"""Opaque collection-local identity for immutable archive members."""

from __future__ import annotations

import hashlib
import re
import secrets
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field
from pydantic_core import core_schema

from riverhog_protocol.exact_scalar import NonnegativeDecimal

_ARTIFACT_ID_RE = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class ArtifactId(str):
    """A 256-bit opaque member ID, distinct from a payload digest or sequence."""

    def __new__(cls, value: str) -> ArtifactId:
        if not isinstance(value, str) or _ARTIFACT_ID_RE.fullmatch(value) is None:
            raise ValueError("artifact ID must be 64 lowercase hexadecimal characters")
        return str.__new__(cls, value)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: object, handler: object
    ) -> core_schema.CoreSchema:
        return core_schema.no_info_after_validator_function(
            cls,
            core_schema.str_schema(strict=True, pattern=r"^[0-9a-f]{64}$"),
        )


def new_artifact_id() -> ArtifactId:
    """Allocate before registration and persist the result across retries."""

    return ArtifactId(secrets.token_hex(32))


def derive_artifact_id(construction_key: bytes, item_key: bytes) -> ArtifactId:
    """Derive from explicit durable producer keys, never implicit path or byte identity."""

    if type(construction_key) is not bytes or type(item_key) is not bytes:
        raise TypeError("artifact allocation keys must be bytes")
    if not construction_key or not item_key:
        raise ValueError("artifact allocation keys must be nonempty")
    digest = hashlib.sha256(b"riverhog-artifact-allocation/v1\0")
    for key in (construction_key, item_key):
        if len(key) >= 1 << 64:
            raise ValueError("artifact allocation key exceeds its framing domain")
        digest.update(len(key).to_bytes(8, "big"))
        digest.update(key)
    return ArtifactId(digest.hexdigest())


class ArtifactMemberIdentityDocument(BaseModel):
    """The exact pathless member tuple committed by a collection archive."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    artifact_id: ArtifactId
    bytes: NonnegativeDecimal = Field(lt=1 << 63)
    sha256: Sha256


__all__ = [
    "ArtifactId",
    "ArtifactMemberIdentityDocument",
    "derive_artifact_id",
    "new_artifact_id",
]
