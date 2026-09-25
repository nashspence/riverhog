"""Exact statement bytes shared by the supplied independent witness applications."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Self

from riverhog_canonical_json import (
    SEQUENCE63_MAX,
    canonical_json_bytes,
    format_scalar,
    parse_scalar,
    require_canonical_json,
)
from riverhog_protocol import CatalogSyncDescriptor

STATEMENT_FORMAT = "a-riverhog-collection-witness/v1"
_PREFIX = STATEMENT_FORMAT.encode("ascii") + b"\n"
_FIELDS = frozenset(
    {
        "format",
        "source_identity",
        "collection_id",
        "archive_root_sha256",
        "content_identity",
    }
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _require_sha256(name: str, value: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 identity")


@dataclass(frozen=True)
class CollectionWitnessStatement:
    """A collection's immutable Riverhog identity, independent of mutable catalog metadata."""

    source_identity: str
    collection_id: int
    archive_root_sha256: str
    content_identity: str

    def __post_init__(self) -> None:
        _require_sha256("source_identity", self.source_identity)
        _require_sha256("archive_root_sha256", self.archive_root_sha256)
        _require_sha256("content_identity", self.content_identity)
        if (
            isinstance(self.collection_id, bool)
            or not isinstance(self.collection_id, int)
            or not 1 <= self.collection_id <= SEQUENCE63_MAX
        ):
            raise ValueError("collection_id is outside Riverhog's positive 63-bit domain")

    @classmethod
    def from_catalog(cls, source_identity: str, descriptor: CatalogSyncDescriptor) -> Self:
        return cls(
            source_identity=source_identity,
            collection_id=descriptor.collection_id,
            archive_root_sha256=descriptor.archive_root_sha256,
            content_identity=descriptor.content_identity,
        )

    def serialize(self) -> bytes:
        document = {
            "format": STATEMENT_FORMAT,
            "source_identity": self.source_identity,
            "collection_id": format_scalar("sequence63", self.collection_id),
            "archive_root_sha256": self.archive_root_sha256,
            "content_identity": self.content_identity,
        }
        return _PREFIX + canonical_json_bytes(document) + b"\n"

    def sha256(self) -> bytes:
        return hashlib.sha256(self.serialize()).digest()

    @classmethod
    def parse(cls, raw: bytes) -> Self:
        if not isinstance(raw, bytes) or not raw.startswith(_PREFIX) or not raw.endswith(b"\n"):
            raise ValueError("invalid collection witness statement envelope")
        if len(raw) > 512:
            raise ValueError("collection witness statement exceeds its byte bound")
        document = require_canonical_json(raw[len(_PREFIX) : -1])
        if not isinstance(document, dict) or document.keys() != _FIELDS:
            raise ValueError("collection witness statement has invalid fields")
        if document["format"] != STATEMENT_FORMAT:
            raise ValueError("collection witness statement has another format")
        collection_id = parse_scalar("sequence63", document["collection_id"])
        source_identity = document["source_identity"]
        archive_root_sha256 = document["archive_root_sha256"]
        content_identity = document["content_identity"]
        if (
            not isinstance(source_identity, str)
            or not isinstance(archive_root_sha256, str)
            or not isinstance(content_identity, str)
        ):
            raise ValueError("collection witness statement identity fields must be text")
        statement = cls(
            source_identity=source_identity,
            collection_id=collection_id,
            archive_root_sha256=archive_root_sha256,
            content_identity=content_identity,
        )
        if statement.serialize() != raw:
            raise ValueError("collection witness statement is not canonical")
        return statement


__all__ = ["STATEMENT_FORMAT", "CollectionWitnessStatement"]
