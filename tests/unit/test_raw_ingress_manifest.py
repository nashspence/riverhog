from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest
from riverhog_client import source_hashing
from riverhog_client.source_hashing import hash_raw_source_chunks
from riverhog_protocol import raw_ingress
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
    "riverhog-raw-source-primitive-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_client.hash_raw_source_chunks",
)
def test_raw_source_primitive_work_keeps_exact_outputs_and_delegate_composition(
    monkeypatch: pytest.MonkeyPatch, record_property
) -> None:
    from riverhog_canonical_json import canonical_json_bytes
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    source = growth_binding(source_hashing.hash_raw_source_chunks)
    child = growth_binding(raw_ingress.advance_raw_part_commitment)
    projection = project_bindings((source,))
    declaration_rows = projection["declarations"]
    samples = []
    counts = {}
    calls = []
    real_file = source_hashing.tempfile.TemporaryFile
    real_commitment = source_hashing.advance_raw_part_commitment

    def sample(binding, dimensions, metrics, delegates):
        return {
            "declaration": binding.declaration.id,
            "declaration_sha256": hashlib.sha256(
                canonical_json_bytes(declaration_rows[binding.declaration.id])
            ).hexdigest(),
            "dimensions": dimensions,
            "metrics": metrics,
            "delegates": delegates,
        }

    class TrackedHash:
        def __init__(self):
            self.actual = hashlib.sha256()

        def update(self, value):
            counts["payload_hash_input_octets"] += len(value)
            self.actual.update(value)

        def hexdigest(self):
            return self.actual.hexdigest()

    def commitment_hash(value):
        counts["commitment_octets"] += len(value)
        counts["commitment_steps"] += 1
        return hashlib.sha256(value)

    def commitment(prior, *, first_part, part_sha256s):
        before_bytes = counts["commitment_octets"]
        before_steps = counts["commitment_steps"]
        result = real_commitment(prior, first_part=first_part, part_sha256s=part_sha256s)
        calls.append(
            sample(
                child,
                {"parts": len(part_sha256s)},
                {
                    "hash_input_octets": counts["commitment_octets"] - before_bytes,
                    "commitment_steps": counts["commitment_steps"] - before_steps,
                },
                {},
            )
        )
        return result

    class TrackedSpool:
        def __init__(self, actual):
            self.actual = actual

        def write(self, value):
            count = self.actual.write(value)
            counts["digest_spool_octets"] += count
            return count

        def __getattr__(self, name):
            return getattr(self.actual, name)

    class TrackedInput:
        def __init__(self, values):
            self.values = iter(values)

        def __iter__(self):
            return self

        def __next__(self):
            counts["iterator_pulls"] += 1
            return next(self.values)

    # Instrument the owner namespaces while preserving the native hash, spool,
    # iterator and delegate implementations and their actual results.
    monkeypatch.setattr(source_hashing, "hashlib", SimpleNamespace(sha256=TrackedHash))
    monkeypatch.setattr(raw_ingress, "hashlib", SimpleNamespace(sha256=commitment_hash))
    monkeypatch.setattr(source_hashing, "advance_raw_part_commitment", commitment)
    monkeypatch.setattr(
        source_hashing,
        "tempfile",
        SimpleNamespace(TemporaryFile=lambda **kwargs: TrackedSpool(real_file(**kwargs))),
    )
    cases = (
        (0, 65536, 0, 0),
        (0, 65536, 0, 64),
        (65536, 65536, 8191, 0),
        (65536 + 3, 65536, 8191, 0),
        (65536 + 3, 65536, 8191, 128),
        (8 * 65536 + 17, 65536, 8191, 0),
        (8 * 65536 + 17, 2 * 65536, 0, 0),
    )
    for size, part_size, chunk_size, empty_chunks in cases:
        content = b"x" * size
        chunks = (
            [content[offset : offset + chunk_size] for offset in range(0, size, chunk_size)]
            if chunk_size
            else ([content] if size else [])
        )
        chunks = [b""] * empty_chunks + chunks
        counts = dict.fromkeys(
            (
                "payload_hash_input_octets",
                "commitment_octets",
                "commitment_steps",
                "digest_spool_octets",
                "iterator_pulls",
            ),
            0,
        )
        calls = []
        result = source_hashing.hash_raw_source_chunks(
            artifact_id="a" * 64,
            chunks=TrackedInput(chunks),
            expected_bytes=size,
            part_plaintext_bytes=part_size,
        )
        try:
            observed = sample(
                source,
                {
                    "payload_octets": size,
                    "input_chunks": len(chunks),
                    "parts": result.summary.part_count,
                },
                {
                    "payload_hash_input_octets": counts["payload_hash_input_octets"],
                    "hash_input_octets": counts["payload_hash_input_octets"]
                    + counts["commitment_octets"],
                    "digest_spool_octets": counts["digest_spool_octets"],
                    "iterator_pulls": counts["iterator_pulls"],
                    "part_commitment_calls": len(calls),
                },
                {"commitment": calls},
            )
            validate_execution_samples(projection, source.declaration.id, [observed])
            samples.append(observed)
            parts = tuple(value for _, batch in result.iter_batches(limit=2) for value in batch)
            assert result.summary.sha256 == hashlib.sha256(content).hexdigest()
            expected_parts = tuple(
                hashlib.sha256(content[offset : offset + part_size]).hexdigest()
                for offset in range(0, size, part_size)
            ) or (hashlib.sha256(b"").hexdigest(),)
            assert parts == expected_parts
            assert ordered_raw_part_commitment(parts) == (
                result.summary.part_count,
                result.summary.ordered_part_sha256,
            )
        finally:
            result.close()
    record_property("growth_samples", samples)
