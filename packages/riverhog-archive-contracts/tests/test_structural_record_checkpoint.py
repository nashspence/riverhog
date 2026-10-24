from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from riverhog_archive_contracts import RecordSetCheckpoint, RecordSetCommitment
from riverhog_canonical_json import canonical_json_bytes


@pytest.mark.parametrize("count", [0, 1, 127, 128, 129, 257])
def test_resumed_checkpoint_preserves_independent_existing_commitment(count):
    schema = "urn:test:structural-records"
    checkpoint = RecordSetCheckpoint.begin(schema)
    streamed = RecordSetCommitment(schema)
    prefix = b"riverhog-archive-record-set/v1\x00"
    encoded = schema.encode()
    oracle = hashlib.sha256(prefix + len(encoded).to_bytes(2, "big") + encoded)
    for index in range(count):
        key, value = f"{index:08x}", {"opaque": [index, "\u0000", "é"]}
        raw = canonical_json_bytes({"key": key, "value": value})
        oracle.update(len(raw).to_bytes(4, "big") + raw)
        streamed.update(key, value)
        checkpoint = RecordSetCheckpoint.from_json_bytes(
            checkpoint.append(key, value).to_json_bytes()
        )
    oracle.update(b"\x00count:" + str(count).encode())
    assert checkpoint.ref() == streamed.ref()
    assert checkpoint.ref().records_sha256 == oracle.hexdigest()


@pytest.mark.parametrize("key", ["a", ""])
def test_checkpoint_rejects_duplicate_reversed_and_empty_keys(key):
    checkpoint = RecordSetCheckpoint.begin("test").append("b", {})
    with pytest.raises(ValueError):
        checkpoint.append(key, {})
    assert checkpoint.count == 1


@pytest.mark.parametrize("fault", ["format", "count", "previous_key", "hash_state", "schema_id"])
def test_checkpoint_codec_rejects_inconsistent_or_untrusted_shape(fault):
    from riverhog_canonical_json import require_canonical_json

    checkpoint = RecordSetCheckpoint.begin("test")
    value = require_canonical_json(checkpoint.to_json_bytes())
    value[fault] = {
        "format": "other",
        "count": "1",
        "previous_key": "key",
        "hash_state": "{}",
        "schema_id": "other",
    }[fault]
    with pytest.raises(ValueError):
        RecordSetCheckpoint.from_json_bytes(canonical_json_bytes(value))


def test_checkpoint_extreme_valid_fields_fit_the_derived_codec_bound():
    checkpoint = RecordSetCheckpoint.begin("\u0000" * 256).append("\u0000" * 1024, {})
    assert len(checkpoint.to_json_bytes()) < 16 * 1024
    assert RecordSetCheckpoint.from_json_bytes(checkpoint.to_json_bytes()) == checkpoint
    with pytest.raises(ValueError):
        replace(checkpoint, count=True)
    with pytest.raises(ValueError):
        RecordSetCheckpoint.from_json_bytes(checkpoint.to_json_bytes() + b" ")
