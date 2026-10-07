"""Shared RFC 8785 canonical JSON identity encoder for stove0 protocols."""

from __future__ import annotations

from typing import Protocol

from riverhog_canonical_json import canonical_json_bytes, canonical_json_sha256


class CommitmentDigest(Protocol):
    def update(self, data: bytes, /) -> object: ...


__all__ = ["CommitmentDigest", "canonical_json_bytes", "canonical_json_sha256"]
