"""Actual immutable envelope work, bounded admission and root-row lifetime."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, replace
from functools import partial

import pytest
from riverhog_provenance._generation_envelope import GenerationEnvelopes, envelope_digest
from riverhog_provenance._generation_page_map import InvalidGenerationMap
from riverhog_provenance.errors import JournalValidationCapacity
from test_generation_roots import apply, build, scalar, storage

from tests.support.growth_batch import SampleBatch


def samples(tmp_path, mode, numeric_policy):
    operation = getattr(GenerationEnvelopes, mode)
    results = []
    with SampleBatch((operation,), limit=4, numeric_policy=numeric_policy) as batch:
        for retained, padding in ((0, 0), (0, 2048), (128, 0), (128, 2048)):
            path = tmp_path / f"retained-{retained}-padding-{padding}"
            path.mkdir()
            with storage(path, metadata=8192) as (owner, _frame, _admission):
                with owner.execution():
                    owner.epoch.apply(
                        lambda epoch, retained=retained: owner.db.executemany(
                            "INSERT INTO generation_roots(identity,status) VALUES(?,'building')",
                            ((f"other-{index}",) for index in range(retained)),
                        ).close(),
                        receive=lambda result: None,
                    )
                root = build(owner, "original", count=1)
                body = b"x" * padding
                if mode == "read":
                    apply(owner, partial(owner.envelopes.seal, "original", root, body))
                before = asdict(owner.envelopes.work)
                sql = []
                owner.db.set_trace_callback(sql.append)
                try:
                    if mode == "seal":
                        received = apply(
                            owner, partial(owner.envelopes.seal, "original", root, body)
                        )
                        assert (
                            received
                            == hashlib.sha256(
                                b"riverhog-native-generation-envelope/v1" + bytes((0,)) + body
                            ).hexdigest()
                        )
                    else:
                        received = owner.envelopes.read(
                            "original", root=root, expected_digest=envelope_digest(body)
                        )
                        assert received == body
                finally:
                    owner.db.set_trace_callback(None)
                metrics = {
                    key: value - before[key] for key, value in asdict(owner.envelopes.work).items()
                }
                assert (
                    metrics["sql_calls"]
                    == len([statement for statement in sql if "generation_roots" in statement])
                    == 1
                )
                assert metrics["hashed_octets"] == len(
                    b"riverhog-native-generation-envelope/v1\x00"
                ) + len(body)
                assert metrics["loaded_octets"] == (
                    len("original") if mode == "seal" else len(body) + 64
                )
                sample = batch.sample(operation, {"units": 1, "metadata_octets": 8192}, metrics)
                results.append(sample)
    return results


@pytest.mark.growth_witness(
    "riverhog-native-generation-envelope-seal-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_envelope.GenerationEnvelopes.seal",
)
def test_actual_envelope_seal_work(tmp_path, numeric_policy, record_property):
    record_property("growth_samples", samples(tmp_path, "seal", numeric_policy))


@pytest.mark.growth_witness(
    "riverhog-native-generation-envelope-read-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_envelope.GenerationEnvelopes.read",
)
def test_actual_envelope_read_work(tmp_path, numeric_policy, record_property):
    record_property("growth_samples", samples(tmp_path, "read", numeric_policy))


def test_exact_envelope_survives_reopen_and_retires_with_its_root(tmp_path):
    with storage(tmp_path, metadata=8192) as (owner, _frame, _admission):
        root = build(owner, "original", count=1)
        digest = apply(owner, partial(owner.envelopes.seal, "original", root, b"original"))
    with storage(tmp_path, metadata=8192, create=False) as (owner, _frame, _admission):
        assert owner.envelopes.read("original", root=root, expected_digest=digest) == b"original"
        reader = owner.roots.borrow("original", expected_root=root)
        try:
            with pytest.raises(RuntimeError, match="reader is not received"):
                owner.roots.retire("original")
            assert (
                owner.envelopes.read("original", root=root, expected_digest=digest) == b"original"
            )
        finally:
            reader.close()
        owner.roots.retire("original")
        assert scalar(owner.db, "SELECT count(*) FROM generation_roots") == 0
        with pytest.raises(InvalidGenerationMap, match="missing"):
            owner.envelopes.read("original", root=root, expected_digest=digest)


def test_original_envelope_cannot_be_overwritten(tmp_path):
    with storage(tmp_path, metadata=8192) as (owner, _frame, _admission):
        root = build(owner, "original", count=1)
        digest = apply(owner, partial(owner.envelopes.seal, "original", root, b"original"))
        with pytest.raises(InvalidGenerationMap, match="already sealed"):
            apply(owner, partial(owner.envelopes.seal, "original", root, b"replacement"))
        assert scalar(owner.db, "SELECT envelope FROM generation_roots") == b"original"
        assert scalar(owner.db, "SELECT envelope_sha256 FROM generation_roots") == digest


def test_envelope_capacity_refusal_precedes_hash_and_sql(tmp_path):
    with storage(tmp_path, metadata=256) as (owner, _frame, _admission):
        root = build(owner, "original", count=1)
        before = replace(owner.envelopes.work)
        with pytest.raises(JournalValidationCapacity, match="envelope admission"):
            apply(owner, partial(owner.envelopes.seal, "original", root, b"x" * 257))
        assert owner.envelopes.work == before
        assert scalar(owner.db, "SELECT envelope FROM generation_roots") is None
