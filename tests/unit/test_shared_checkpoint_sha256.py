from __future__ import annotations

import hashlib
import json

import pytest
import sha256_continuation as backend
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.checkpoint_sha256 import CheckpointSHA256 as RiverhogSHA256
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_protocol import CheckpointSHA256, sha256_compress
from stove0_core._checkpoint_sha256 import CheckpointSHA256 as Stove0SHA256


@pytest.mark.parametrize("owner", [CheckpointSHA256, RiverhogSHA256, Stove0SHA256])
@pytest.mark.parametrize("size", [0, 1, 55, 56, 63, 64, 65, 127, 128, 129, 4097])
def test_native_digest_and_partial_block_restart(owner, size):
    content = bytes((i * 37 + 19) % 256 for i in range(size))
    expected = hashlib.sha256(content).digest()
    for cut in sorted({0, min(size, 63), min(size, 64), size // 2, size}):
        digest = owner(content[:cut])
        before = digest.export_state()
        assert digest.digest() == hashlib.sha256(content[:cut]).digest()
        assert digest.export_state() == before
        restored = owner.from_state(before)
        assert type(restored) is owner and restored.total_bytes == cut
        restored.update_bytes(content[cut:])
        assert restored.total_bytes == size and restored.digest() == expected
        assert len(restored._buffer) == size % 64


@pytest.mark.parametrize("owner", [CheckpointSHA256, RiverhogSHA256, Stove0SHA256])
def test_native_digest_across_every_retained_tail_position(owner):
    content = bytes(range(193))
    for cut in range(64, 128):
        digest = owner(content[:cut])
        restored = owner.from_state(digest.export_state())
        restored.update_bytes(content[cut:])
        assert restored.digest() == hashlib.sha256(content).digest()
        assert restored.total_bytes == len(content)


@pytest.mark.parametrize(
    "owner, marker",
    [
        (RiverhogSHA256, "riverhog-private-sha256-checkpoint/v1"),
        (Stove0SHA256, "stove0-private-sha256-checkpoint/v1"),
    ],
)
def test_application_checkpoint_bytes_and_position_are_stable(owner, marker):
    state = (
        '{"buffer_hex":"404142434445464748494a4b4c","format":"'
        + marker
        + '","h":[4237927135,2297703034,2075775360,869123746,39220575,'
        '2640026192,1151978545,1522435239],"total_bytes":77}'
    )
    prefix = bytes(range(77))
    assert owner(prefix).export_state() == state
    restored = owner.from_state(state)
    assert restored.total_bytes == 77
    restored.update_bytes(b"continued input")
    assert restored.digest() == hashlib.sha256(prefix + b"continued input").digest()


def test_application_codecs_preserve_their_current_domains_and_reject_foreign_states():
    for owner, marker in [
        (RiverhogSHA256, "riverhog-private-sha256-checkpoint/v1"),
        (Stove0SHA256, "stove0-private-sha256-checkpoint/v1"),
    ]:
        state = owner(b"preserved prefix").export_state()
        assert json.loads(state)["format"] == marker
        assert (
            owner.from_state(state).hexdigest() == hashlib.sha256(b"preserved prefix").hexdigest()
        )
        for other in (CheckpointSHA256, RiverhogSHA256, Stove0SHA256):
            if other is not owner:
                with pytest.raises(ValueError, match="checkpoint"):
                    other.from_state(state)
    assert RiverhogSHA256.update_bytes is Stove0SHA256.update_bytes is CheckpointSHA256.update_bytes


@pytest.mark.parametrize(
    "mutation",
    [
        "extra",
        "bool-word",
        "negative-word",
        "large-word",
        "missing-word",
        "bool-position",
        "negative-position",
        "message-domain",
        "offset",
        "uppercase-tail",
        "long-tail",
        "odd-tail",
        "nonhex-tail",
        "format",
    ],
)
def test_restore_rejects_corrupt_or_noncanonical_private_state(mutation):
    payload = json.loads(CheckpointSHA256(b"a").export_state())
    if mutation == "extra":
        payload["extra"] = 1
    elif mutation == "bool-word":
        payload["h"][0] = True
    elif mutation == "negative-word":
        payload["h"][0] = -1
    elif mutation == "large-word":
        payload["h"][0] = 2**32
    elif mutation == "missing-word":
        payload["h"].pop()
    elif mutation == "bool-position":
        payload["total_bytes"] = True
    elif mutation == "negative-position":
        payload["total_bytes"] = -1
    elif mutation == "message-domain":
        payload["total_bytes"] = 2**61
    elif mutation == "offset":
        payload["total_bytes"] += 1
    elif mutation == "uppercase-tail":
        payload["buffer_hex"] = "AF"
    elif mutation == "long-tail":
        payload["buffer_hex"] = "00" * 64
    elif mutation == "odd-tail":
        payload["buffer_hex"] = "0"
    elif mutation == "nonhex-tail":
        payload["buffer_hex"] = "zz"
    elif mutation == "format":
        payload["format"] = "unknown/v1"
    with pytest.raises(ValueError, match="checkpoint"):
        CheckpointSHA256.from_state(json.dumps(payload, sort_keys=True, separators=(",", ":")))


@pytest.mark.parametrize("state", ["[]", "null", "true", "{}", "x" * 513])
def test_restore_rejects_wrong_roots_and_oversized_codec(state):
    with pytest.raises(ValueError, match="checkpoint"):
        CheckpointSHA256.from_state(state)


def test_restore_rejects_duplicate_fields_and_noncanonical_whitespace():
    state = CheckpointSHA256().export_state()
    for altered in (
        " " + state,
        state.replace('"total_bytes":0', '"total_bytes":1,"total_bytes":0'),
    ):
        with pytest.raises(ValueError, match="checkpoint"):
            CheckpointSHA256.from_state(altered)


def test_snapshot_adapter_preserves_mutable_and_noncontiguous_inputs():
    source = bytearray(range(128))
    for value in (source, memoryview(source)[::3], memoryview(source).cast("B", (8, 16))):
        expected = bytes(value)
        digest = CheckpointSHA256()
        digest.update(value)
        assert digest.digest() == hashlib.sha256(expected).digest()
    with pytest.raises(TypeError, match="immutable"):
        CheckpointSHA256().update_bytes(source)


def test_message_domain_rejection_does_not_change_the_continuation():
    digest = CheckpointSHA256(b"a" * 63)
    digest._total_bytes = (2**64 - 1) // 8
    before = digest.export_state()
    with pytest.raises(ValueError, match="message-length"):
        digest.update_bytes(b"x")
    assert digest.export_state() == before


@pytest.mark.growth_witness(
    "riverhog-sha256-continuation-primitive-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="sha256_continuation.CheckpointSHA256.update_bytes",
)
def test_bounded_sha256_update_records_actual_compression_and_buffer_work(
    monkeypatch, record_property
):
    source = growth_binding(CheckpointSHA256.update_bytes)
    child = growth_binding(sha256_compress)
    projection = project_bindings((source,))
    declarations = projection["declarations"]
    samples = []
    real_compress, real_rotate = backend.sha256_compress, backend._rotr

    def sample(binding, dimensions, metrics, delegates):
        return {
            "declaration": binding.declaration.id,
            "declaration_sha256": hashlib.sha256(
                canonical_json_bytes(declarations[binding.declaration.id])
            ).hexdigest(),
            "dimensions": dimensions,
            "metrics": metrics,
            "delegates": delegates,
        }

    class Buffer(bytearray):
        def __init__(self, value):
            super().__init__(value)
            self.peak = len(self)

        def extend(self, value):
            super().extend(value)
            self.peak = max(self.peak, len(self))

    for prefix_size, size in (
        (0, 0),
        (0, 1),
        (63, 0),
        (63, 1),
        (7, 4097),
        (63, 4097),
        (7, 8193),
        (0, 65539),
    ):
        prefix = bytes((i * 17 + 3) % 256 for i in range(prefix_size))
        payload = bytes((i * 37 + 19) % 256 for i in range(size))
        digest = CheckpointSHA256(prefix)
        digest._buffer = Buffer(digest._buffer)
        initial = len(digest._buffer)
        counts = {"rotates": 0}
        calls = []

        def rotate(value, count, *, counts=counts):
            counts["rotates"] += 1
            return real_rotate(value, count)

        def compress(words, block, *, counts=counts, calls=calls):
            before = counts["rotates"]
            result = real_compress(words, block)
            calls.append(
                sample(
                    child,
                    {"block_octets": len(block)},
                    {
                        "block_input_octets": len(block),
                        "rotate_calls": counts["rotates"] - before,
                        "returned_words": len(result),
                    },
                    {},
                )
            )
            return result

        with monkeypatch.context() as patch:
            patch.setattr(backend, "_rotr", rotate)
            patch.setattr(backend, "sha256_compress", compress)
            assert digest.update_bytes(payload) is digest
        observed = sample(
            source,
            {
                "input_octets": size,
                "initial_tail_octets": initial,
                "blocks": (initial + size) // 64,
            },
            {
                "compression_calls": len(calls),
                "block_input_octets": sum(c["metrics"]["block_input_octets"] for c in calls),
                "rotate_calls": counts["rotates"],
                "pending_buffer_octets": digest._buffer.peak,
                "retained_tail_octets": len(digest._buffer),
            },
            {"compression": calls},
        )
        validate_execution_samples(projection, source.declaration.id, [observed])
        samples.append(observed)
        assert digest.total_bytes == prefix_size + size
        assert digest.digest() == hashlib.sha256(prefix + payload).digest()
        assert digest._buffer.peak <= 64 and len(digest._buffer) == (prefix_size + size) % 64
    record_property("growth_samples", samples)


def test_protocol_exports_share_the_neutral_backend_and_preserve_default_checkpoint_bytes():
    from riverhog_protocol.checkpoint_sha256 import CheckpointSHA256 as ProtocolModuleSHA256

    assert CheckpointSHA256 is ProtocolModuleSHA256 is backend.CheckpointSHA256
    assert sha256_compress is backend.sha256_compress
    assert (
        RiverhogSHA256.update_bytes
        is Stove0SHA256.update_bytes
        is backend.CheckpointSHA256.update_bytes
    )
    expected = (
        '{"buffer_hex":"404142434445464748494a4b4c","format":"private-sha256-checkpoint/v1",'
        '"h":[4237927135,2297703034,2075775360,869123746,39220575,2640026192,1151978545,1522435239],'
        '"total_bytes":77}'
    )
    assert backend.CheckpointSHA256(bytes(range(77))).export_state() == expected
    assert (
        CheckpointSHA256.from_state(expected).hexdigest()
        == hashlib.sha256(bytes(range(77))).hexdigest()
    )
