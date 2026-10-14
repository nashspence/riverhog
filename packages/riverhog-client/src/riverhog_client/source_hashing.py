"""Bounded-memory source hashing support for Riverhog producers."""

from __future__ import annotations

import hashlib
import tempfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import BinaryIO

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.raw_ingress import (
    RAW_SOURCE_DIGEST_BATCH_MAX,
    RawSourceDigestSummary,
    advance_raw_part_commitment,
)


@dataclass(slots=True)
class RawSourceHash:
    """One small authority plus a private bounded-step digest construction spool."""

    summary: RawSourceDigestSummary
    _parts: BinaryIO

    def iter_batches(
        self,
        *,
        limit: int = RAW_SOURCE_DIGEST_BATCH_MAX,
    ) -> Iterator[tuple[int, tuple[str, ...]]]:
        if limit < 1 or limit > RAW_SOURCE_DIGEST_BATCH_MAX:
            raise ValueError("raw digest batch limit is invalid")
        self._parts.seek(0)
        first = 0
        while first < self.summary.part_count:
            count = min(limit, self.summary.part_count - first)
            content = self._parts.read(count * 32)
            if len(content) != count * 32:
                raise RuntimeError("raw digest construction spool is incomplete")
            yield (
                first,
                tuple(content[offset : offset + 32].hex() for offset in range(0, len(content), 32)),
            )
            first += count

    def close(self) -> None:
        self._parts.close()

    def __del__(self) -> None:
        self._parts.close()


@declare_growth(
    Declaration(
        id="riverhog-client.raw-source-hashing.growth/v1",
        authority="riverhog-client",
        subjects=("riverhog_client.hash_raw_source_chunks",),
        scope="successful-call-primitive-work",
        starts_at="validate byte parameters and prepare the private digest spool",
        ends_at="flush the complete digest spool and return the exact raw source summary",
        dimensions=(
            Dimension(
                "payload_octets",
                OCTETS,
                "Total yielded payload octets; equals expected_bytes on success.",
            ),
            Dimension("input_chunks", COUNT, "Every yielded chunk, including empty chunks."),
            Dimension("parts", COUNT, "Final part_count, including the single empty-source part."),
        ),
        bounds=(
            Bound(
                "payload_hash_input_octets",
                OCTETS,
                "equal",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=2),
                        Expression("dimension", name="payload_octets"),
                    ),
                ),
                "Payload octets supplied once to the whole-source hash and once to part hashes.",
            ),
            Bound(
                "hash_input_octets",
                OCTETS,
                "equal",
                Expression(
                    "sum",
                    terms=(
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=2),
                                Expression("dimension", name="payload_octets"),
                            ),
                        ),
                        Expression(
                            "delegate",
                            name="commitment",
                            metric="hash_input_octets",
                            terms=(Expression("dimension", name="parts"),),
                            arguments=(("parts", Expression("constant", value=1, unit=COUNT)),),
                        ),
                    ),
                ),
                (
                    "All source/part hashing plus the explicitly composed ordered-commitment hash "
                    "inputs."
                ),
            ),
            Bound(
                "digest_spool_octets",
                OCTETS,
                "equal",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=32, unit=OCTETS_PER_COUNT),
                        Expression("dimension", name="parts"),
                    ),
                ),
                "Digest octets written to and retained in the private spool on successful return.",
            ),
            Bound(
                "iterator_pulls",
                COUNT,
                "equal",
                Expression(
                    "sum",
                    terms=(
                        Expression("dimension", name="input_chunks"),
                        Expression("constant", value=1, unit=COUNT),
                    ),
                ),
                "Input next calls, including the terminating StopIteration call.",
            ),
            Bound(
                "part_commitment_calls",
                COUNT,
                "equal",
                Expression("dimension", name="parts"),
                "One commitment delegate invocation for each complete or final partial/empty part.",
            ),
        ),
        conditions=(
            (
                "Finite successfully completed bytes iterable, exact declared byte count and "
                "valid part size."
            ),
            "Existing fixed-width SHA-256 commitment and ordinal parameters remain unchanged.",
            (
                "All temporary-file writes and flush complete; no warm-cache assumption is "
                "needed for primitive counts."
            ),
        ),
        invariants=(
            "Preserve exact whole/part hashes, ordered commitment, byte count and part count.",
            (
                "Close the private spool on a caught construction exception; transfer ownership "
                "on successful return."
            ),
        ),
        limitations=(
            (
                "The input producer's internal work/resources are caller-owned and require "
                "their own profile for a total-cost claim."
            ),
            (
                "Filesystem/allocator/cryptographic implementation costs and physical memory "
                "are not bounded by this primitive vector."
            ),
            (
                "Returned-spool traversal and final close belong to their own operations; "
                "repeated traversal is not free."
            ),
            (
                "Failed prefixes and retries remain observable attempts, not successful "
                "completed-call witnesses."
            ),
        ),
    ),
    delegates=(("commitment", advance_raw_part_commitment),),
)
def hash_raw_source_chunks(
    *,
    artifact_id: ArtifactId,
    chunks: Iterable[bytes],
    expected_bytes: int,
    part_plaintext_bytes: int,
) -> RawSourceHash:
    """Hash one logical source while retaining only a bounded part and private spool."""

    if expected_bytes < 0 or part_plaintext_bytes < 65536 or part_plaintext_bytes % 65536:
        raise ValueError("raw source hash byte values are invalid")
    parts = tempfile.TemporaryFile(mode="w+b")
    whole = hashlib.sha256()
    part = hashlib.sha256()
    part_bytes = 0
    total = 0
    part_count = 0
    commitment: str | None = None
    try:
        for source in chunks:
            chunk = bytes(source)
            whole.update(chunk)
            total += len(chunk)
            if total > expected_bytes:
                raise ValueError("raw source is longer than declared")
            view = memoryview(chunk)
            offset = 0
            while offset < len(view):
                take = min(part_plaintext_bytes - part_bytes, len(view) - offset)
                part.update(view[offset : offset + take])
                part_bytes += take
                offset += take
                if part_bytes == part_plaintext_bytes:
                    digest = part.hexdigest()
                    parts.write(bytes.fromhex(digest))
                    part_count, commitment = advance_raw_part_commitment(
                        commitment,
                        first_part=part_count,
                        part_sha256s=(digest,),
                    )
                    part = hashlib.sha256()
                    part_bytes = 0
        if total != expected_bytes:
            raise ValueError("raw source byte count mismatch")
        if part_bytes or expected_bytes == 0:
            digest = part.hexdigest()
            parts.write(bytes.fromhex(digest))
            part_count, commitment = advance_raw_part_commitment(
                commitment,
                first_part=part_count,
                part_sha256s=(digest,),
            )
        if commitment is None:
            raise RuntimeError("raw source digest commitment was not constructed")
        parts.flush()
        return RawSourceHash(
            summary=RawSourceDigestSummary(
                artifact_id=artifact_id,
                bytes=expected_bytes,
                sha256=whole.hexdigest(),
                part_plaintext_bytes=part_plaintext_bytes,
                part_count=part_count,
                ordered_part_sha256=commitment,
            ),
            _parts=parts,
        )
    except Exception:
        parts.close()
        raise


__all__ = ["RawSourceHash", "hash_raw_source_chunks"]
