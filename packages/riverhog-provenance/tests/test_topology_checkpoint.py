from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from riverhog_canonical_json import canonical_json_bytes, format_scalar
from riverhog_provenance.topology_checkpoint import TopologyCaptureCheckpoint
from riverhog_provenance.topology_facts import advance_topology_frame, plan_topology_frame


def test_empty_checkpoint_has_exact_canonical_bytes_and_no_mutable_controls():
    raw = '{"active":null,"facts":"0","last":null}'
    checkpoint = TopologyCaptureCheckpoint(None, 0, None)
    assert checkpoint.to_json() == raw
    assert TopologyCaptureCheckpoint.from_json(raw) == checkpoint
    with pytest.raises(FrozenInstanceError):
        checkpoint.facts = 1


@pytest.mark.parametrize(
    "count",
    [1, 17, (1 << 256) + 1, 1 << 15000],
    ids=["one", "seventeen", "beyond-256-bits", "beyond-4300-digits"],
)
def test_complete_counter_domain_survives_without_python_decimal_conversion_limit(count):
    text = format_scalar("nonnegative", count)
    raw = (
        '{"active":null,"facts":"'
        + text
        + '","last":["'
        + text
        + '","relations","0","'
        + "a" * 64
        + '"]}'
    )
    checkpoint = TopologyCaptureCheckpoint(None, count, (count, "relations", 0, "a" * 64))
    assert checkpoint.to_json() == raw
    restored = TopologyCaptureCheckpoint.from_json(raw)
    assert restored.facts == count and restored.last == checkpoint.last


def test_actual_native_partial_cursor_round_trips_and_resumes_without_prefix_revisit():
    raw = canonical_json_bytes(
        {
            "entry_kind": "assertion",
            "body": {
                "assertions": {
                    "relations": [
                        {
                            "type": "derivation",
                            "assertion_id": str(index),
                            "used_state": {"object_id": "root"},
                            "generated_state": {"object_id": "leaf:" + str(index)},
                        }
                        for index in range(129)
                    ]
                }
            },
        }
    )
    first = advance_topology_frame(
        plan_topology_frame(raw), lambda start, size: raw[start : start + size], item_budget=1
    )
    fact = first.facts[0]
    checkpoint = TopologyCaptureCheckpoint(
        first.cursor, 1, (0, fact.array, fact.index, fact.json_sha256)
    )
    restored = TopologyCaptureCheckpoint.from_json(checkpoint.to_json())
    assert restored == checkpoint and not first.complete
    calls = []

    def read(start, size):
        assert start >= first.cursor.relations.offset
        calls.append(start)
        return raw[start : start + size]

    second = advance_topology_frame(restored.active, read, item_budget=1)
    assert second.facts[0].index == 1 and calls


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "{}",
        "[]",
        '{"active":null}',
        '{"active":null,"facts":0,"last":null}',
        '{"active":null,"facts":"00","last":null}',
        '{"active":null,"facts":"1","last":null}',
        '{"active":null,"facts":"0","last":null} ',
        '{"active":null,"facts":"0","facts":"0","last":null}',
        '{"active":null,"facts":"0","last":[0,"relations","0","' + "a" * 64 + '"]}',
        canonical_json_bytes(
            {
                "active": {
                    "json_sha256": "a" * 64,
                    "retracts": ["0", "1", "2", "1"],
                    "relations": ["0", "0", "0", "0"],
                },
                "facts": "0",
                "last": None,
            }
        ).decode(),
        canonical_json_bytes(
            {
                "active": {
                    "json_sha256": "A" * 64,
                    "retracts": ["0", "0", "0", "0"],
                    "relations": ["0", "0", "0", "0"],
                },
                "facts": "0",
                "last": None,
            }
        ).decode(),
    ],
)
def test_changed_inconsistent_or_noncanonical_controls_are_explicitly_unavailable(raw):
    with pytest.raises(RuntimeError, match="invalid retained topology checkpoint"):
        TopologyCaptureCheckpoint.from_json(raw)


@pytest.mark.parametrize("value", [None, b'{"active":null,"facts":"0","last":null}', 0])
def test_nontext_checkpoint_never_supplies_control_authority(value):
    with pytest.raises(RuntimeError, match="invalid retained topology checkpoint"):
        TopologyCaptureCheckpoint.from_json(value)


@pytest.mark.parametrize("facts", [-1, True, 0.5])
def test_encoder_preserves_the_canonical_counter_type_domain(facts):
    with pytest.raises((ValueError, TypeError)):
        TopologyCaptureCheckpoint(None, facts, (0, "relations", 0, "a" * 64)).to_json()
