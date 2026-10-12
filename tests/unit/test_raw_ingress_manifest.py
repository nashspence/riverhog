from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest
from riverhog_client import source_hashing
from riverhog_client.source_hashing import hash_raw_source_chunks
from riverhog_protocol.raw_ingress import (
    RawSourceDigestSummary,
    ordered_raw_part_commitment,
    raw_volume_part_span,
)


def test_raw_source_is_hashed_once_into_small_authority_and_bounded_batches() -> None:
    content = b"abcdefgh" * 20000
    result = hash_raw_source_chunks(
        artifact_id="a" * 64,
        chunks=(content[:1234], content[1234:]),
        expected_bytes=len(content),
        part_plaintext_bytes=65536,
    )
    try:
        batches = tuple(result.iter_batches(limit=2))
        parts = tuple(value for _first, batch in batches for value in batch)
        assert [first for first, _batch in batches] == [0, 2]
        assert result.summary.sha256 == hashlib.sha256(content).hexdigest()
        assert parts[0] == hashlib.sha256(content[:65536]).hexdigest()
        assert raw_volume_part_span(
            result.summary,
            file_offset=0,
            plaintext_bytes=len(content),
        ) == (0, len(parts))
        assert ordered_raw_part_commitment(parts) == (
            result.summary.part_count,
            result.summary.ordered_part_sha256,
        )
    finally:
        result.close()


def test_raw_source_digest_summary_has_no_resource_growing_member_list() -> None:
    digest = hashlib.sha256(b"content").hexdigest()
    count, commitment = ordered_raw_part_commitment((digest,))
    summary = RawSourceDigestSummary(
        artifact_id="a" * 64,
        bytes=7,
        sha256=digest,
        part_plaintext_bytes=65536,
        part_count=count,
        ordered_part_sha256=commitment,
    )

    assert summary.format == "raw-source-digest-summary/v1"
    assert not hasattr(summary, "part_sha256s")


@pytest.mark.growth_witness(
    "riverhog-raw-source-payload-hashing/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
)
def test_raw_payload_hash_updates_are_linear_and_keep_exact_digest_results(
    monkeypatch: pytest.MonkeyPatch, record_property
) -> None:
    metrics = {"source_bytes": 0, "payload_hashed_bytes": 0, "parts": 0, "cases": 0}

    class TrackedHash:
        def __init__(self):
            self.actual = hashlib.sha256()

        def update(self, value):
            metrics["payload_hashed_bytes"] += len(value)
            self.actual.update(value)

        def hexdigest(self):
            return self.actual.hexdigest()

    # Count the source and part payload hashes. The commitment delegate retains
    # its own scope; these observations do not count its fixed-width inputs.
    monkeypatch.setattr(source_hashing, "hashlib", SimpleNamespace(sha256=TrackedHash))
    for size in (65536 + 3, 8 * 65536 + 17):
        content = b"x" * size
        before = metrics["payload_hashed_bytes"]
        result = source_hashing.hash_raw_source_chunks(
            artifact_id="a" * 64,
            chunks=(content[:8191], content[8191:]),
            expected_bytes=size,
            part_plaintext_bytes=65536,
        )
        try:
            parts = tuple(value for _, batch in result.iter_batches(limit=2) for value in batch)
            assert result.summary.sha256 == hashlib.sha256(content).hexdigest()
            assert parts == tuple(
                hashlib.sha256(content[offset : offset + 65536]).hexdigest()
                for offset in range(0, size, 65536)
            )
            assert metrics["payload_hashed_bytes"] - before == 2 * size
            metrics["source_bytes"] += size
            metrics["parts"] += len(parts)
            metrics["cases"] += 1
        finally:
            result.close()
    record_property("growth_observations", metrics)
