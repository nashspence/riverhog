# SPDX-FileCopyrightText: 2026 Nash Spence
# SPDX-License-Identifier: Apache-2.0
"""Non-authoritative #903 examples: work identity and bounded opaque transport.

These helpers do not implement PostgreSQL, AWS, encryption or attestation.
Successful reassembly establishes byte fixity, not authenticity or legal truth.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Mapping, Sequence

CHUNK_BYTES = 16 * 1024
BATCH_ROWS = 8
MAX_CIPHERTEXT_BYTES = 16 * 1024 * 1024 + 64 * 1024
MAX_BATCH_JSON_BYTES = 256 * 1024
DIGEST_FIELDS = frozenset({
    "upstream_corpus_sha256", "upstream_license_sha256",
    "candidate_snapshot_sha256", "candidate_corpus_sha256",
    "toolchain_sha256", "profiles_sha256", "extraction_policy_sha256",
})
ID_FIELDS = frozenset({"upstream_repository_id", "candidate_repository_id"})


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def valid_digest(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def work_key(identity: Mapping[str, object]) -> str:
    """Exact selected inputs only; ciphertext, timestamps and attempts are separate."""
    if set(identity) != DIGEST_FIELDS | ID_FIELDS:
        raise ValueError("invalid work identity fields")
    if any(not valid_digest(identity[name]) for name in DIGEST_FIELDS):
        raise ValueError("invalid work digest")
    if any(type(identity[name]) is not int or not 0 < identity[name] < 2**63
           for name in ID_FIELDS):
        raise ValueError("invalid repository identity")
    encoded = json.dumps(dict(identity), sort_keys=True, separators=(",", ":"),
                         ensure_ascii=True, allow_nan=False).encode("ascii")
    return sha256(b"riverhog-reuse-work/v1\0" + encoded)


@dataclass(frozen=True)
class Chunk:
    ordinal: int
    digest: str
    payload: bytes


def split_ciphertext(raw: bytes) -> tuple[Chunk, ...]:
    """Split already-encrypted bytes; this function neither encrypts nor verifies age."""
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_CIPHERTEXT_BYTES:
        raise ValueError("invalid ciphertext length")
    return tuple(Chunk(i // CHUNK_BYTES, sha256(raw[i:i + CHUNK_BYTES]),
                       raw[i:i + CHUNK_BYTES])
                 for i in range(0, len(raw), CHUNK_BYTES))


def reassemble(chunks: Sequence[Chunk], expected_digest: str,
               expected_length: int) -> bytes:
    """Reject missing/reordered/duplicate/substituted data; no authenticity claim."""
    if not valid_digest(expected_digest) or type(expected_length) is not int:
        raise ValueError("invalid expected identity")
    if not 0 < expected_length <= MAX_CIPHERTEXT_BYTES:
        raise ValueError("invalid expected length")
    count = (expected_length + CHUNK_BYTES - 1) // CHUNK_BYTES
    if len(chunks) != count:
        raise ValueError("incomplete chunks")
    for position, chunk in enumerate(chunks):
        size = min(CHUNK_BYTES, expected_length - position * CHUNK_BYTES)
        if (type(chunk.ordinal) is not int or chunk.ordinal != position
                or not isinstance(chunk.payload, bytes) or len(chunk.payload) != size
                or not valid_digest(chunk.digest) or sha256(chunk.payload) != chunk.digest):
            raise ValueError("invalid chunk")
    result = b"".join(chunk.payload for chunk in chunks)
    if sha256(result) != expected_digest:
        raise ValueError("ciphertext digest mismatch")
    return result


def encoded_batch(chunks: Sequence[Chunk]) -> bytes:
    """Bound the specimen's base64/JSON rows; the adapter must bound its full request."""
    if not 1 <= len(chunks) <= BATCH_ROWS:
        raise ValueError("invalid batch count")
    rows = []
    for chunk in chunks:
        if (type(chunk.ordinal) is not int or not 0 <= chunk.ordinal < 2**31
                or not isinstance(chunk.payload, bytes)
                or not 0 < len(chunk.payload) <= CHUNK_BYTES
                or sha256(chunk.payload) != chunk.digest):
            raise ValueError("invalid batch chunk")
        rows.append({"ordinal": chunk.ordinal, "sha256": chunk.digest,
                     "payload_base64": base64.b64encode(chunk.payload).decode("ascii")})
    raw = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("ascii")
    if len(raw) > MAX_BATCH_JSON_BYTES:
        raise ValueError("batch too large")
    return raw


def reusable(*, result: str, coverage_complete: bool, custody_verified: bool) -> bool:
    """Only exact-key results can call this; no_match means a completed comparison."""
    return (result in {"matches", "no_match"}
            and coverage_complete is True and custody_verified is True)
