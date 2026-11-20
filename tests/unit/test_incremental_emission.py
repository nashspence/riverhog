from __future__ import annotations

import hashlib
import json
import random
from dataclasses import replace

import pytest
from riverhog_core import incremental_emission as owner
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.domain.archive import ArchiveArtifact
from riverhog_core.incremental_emission import (
    VolumeEmissionCapacity,
    VolumeEmissionFrontier,
    VolumeEmissionLimits,
    advance_volume_emission,
    parse_volume_emission_frontier,
    volume_emission_frontier_bytes,
    volume_emission_metadata,
)
from riverhog_core.incremental_plan import (
    OrderedArchiveArtifact,
    advance_incremental_volume_plan,
    new_incremental_volume_planner,
)

PART = 5 * 1024 * 1024


def policy():
    return CollectionVolumePolicy(
        pack_source_bytes=10,
        pack_artifacts=2,
        pack_member_bytes=8,
        pack_part_plaintext_bytes=PART,
        raw_volume_plaintext_bytes=PART,
        raw_part_plaintext_bytes=PART,
    )


def artifact(order, size):
    return OrderedArchiveArtifact(
        order,
        ArchiveArtifact(
            f"{order + 1:064x}",
            size,
            hashlib.sha256(f"{order}:{size}".encode()).hexdigest(),
        ),
    )


def limits(plans=3, *, transitions=4, metadata_bytes=100000, coverage_rows=16):
    return VolumeEmissionLimits(plans, coverage_rows, metadata_bytes, transitions)


def collect(checkpoint, inputs, grant, *, final=False):
    state = VolumeEmissionFrontier(checkpoint)
    emitted = []
    quanta = []
    while not state.done:
        quantum = advance_volume_emission(state, inputs, limits=grant, final=final)
        assert quantum.frontier != state
        assert len(quantum.volumes) <= grant.plans
        assert quantum.coverage_rows <= grant.coverage_rows
        assert quantum.metadata_bytes <= grant.metadata_bytes
        assert quantum.input_transitions <= grant.input_transitions
        assert quantum.metadata_bytes == sum(
            len(body) + len(edges)
            for body, _members, edges in map(volume_emission_metadata, quantum.volumes)
        )
        quanta.append(quantum)
        emitted.extend(quantum.volumes)
        state = parse_volume_emission_frontier(volume_emission_frontier_bytes(quantum.frontier))
    return state, tuple(emitted), quanta


@pytest.mark.parametrize("volumes", [1, 16, 17, 129, 1000])
@pytest.mark.parametrize("quantum_size", [1, 3, 16])
def test_raw_emission_resumes_at_retained_offset_with_linear_construction(
    monkeypatch, volumes, quantum_size, record_property
):
    inputs = (artifact(0, volumes * PART - 1),)
    checkpoint = new_incremental_volume_planner(policy=policy())
    expected = advance_incremental_volume_plan(checkpoint, inputs)
    made = []
    raw_type = owner.RawVolumePlan

    def construct(**kwargs):
        made.append(kwargs["artifact_offset"])
        return raw_type(**kwargs)

    monkeypatch.setattr(owner, "RawVolumePlan", construct)
    state, emitted, quanta = collect(
        checkpoint, inputs, limits(quantum_size, transitions=quantum_size)
    )
    assert made == list(range(0, inputs[0].artifact.bytes, PART))
    assert emitted == expected.volumes
    assert [volume_emission_metadata(plan) for plan in emitted] == [
        volume_emission_metadata(plan) for plan in expected.volumes
    ]
    assert state.checkpoint == expected.checkpoint
    assert state.input_index == 1 and state.raw_offset == 0
    assert len(quanta) <= (volumes + quantum_size - 1) // quantum_size + 1
    record_property("raw_volumes", volumes)
    record_property("plan_quantum", quantum_size)
    record_property("quantum_calls", len(quanta))
    record_property("raw_plan_constructions", len(made))
    record_property("peak_emitted_plans", max(len(quantum.volumes) for quantum in quanta))
    record_property(
        "peak_frontier_bytes",
        max(len(volume_emission_frontier_bytes(quantum.frontier)) for quantum in quanta),
    )
    for quantum in quanta:
        current = quantum.frontier
        if current.raw_offset:
            assert current.checkpoint.artifacts_seen == current.checkpoint.bytes_seen == 0
            assert current.input_index == 0
        assert len(volume_emission_frontier_bytes(current)) < 1200


def test_mixed_pack_raw_and_zero_byte_members_keep_original_plan_bytes():
    rng = random.Random(964)
    for count in range(1, 81):
        inputs = tuple(
            artifact(index, rng.choice([0, 1, 3, 7, 8, PART, 2 * PART + 5]))
            for index in range(count)
        )
        checkpoint = new_incremental_volume_planner(policy=policy())
        expected = advance_incremental_volume_plan(checkpoint, inputs, final=True)
        state, emitted, _ = collect(checkpoint, inputs, limits(2, transitions=3), final=True)
        assert state.checkpoint == expected.checkpoint
        assert emitted == expected.volumes


def test_pending_pack_and_raw_continuation_survive_separate_registration_boundaries():
    checkpoint = new_incremental_volume_planner(policy=policy())
    all_inputs = (artifact(0, 0), artifact(1, 3), artifact(2, 17 * PART + 1), artifact(3, 2))
    expected = advance_incremental_volume_plan(checkpoint, all_inputs, final=True)
    emitted = []
    for inputs in (all_inputs[:1], all_inputs[1:2], all_inputs[2:]):
        state, batch, _ = collect(checkpoint, inputs, limits(1, transitions=1))
        checkpoint = state.checkpoint
        emitted.extend(batch)
    state, batch, _ = collect(checkpoint, (), limits(1, transitions=1), final=True)
    emitted.extend(batch)
    assert tuple(emitted) == expected.volumes
    assert state.checkpoint == expected.checkpoint
    assert len([plan for plan in emitted if plan.volume_id.startswith("pack-")]) == 2


def test_small_member_transitions_are_bounded_even_when_no_plan_is_emitted():
    selected = replace(policy(), pack_artifacts=100, pack_source_bytes=1000)
    checkpoint = new_incremental_volume_planner(policy=selected)
    inputs = tuple(artifact(index, 0) for index in range(17))
    state = VolumeEmissionFrontier(checkpoint)
    first = advance_volume_emission(state, inputs, limits=limits(3, transitions=3))
    assert first.volumes == ()
    assert first.input_transitions == first.frontier.input_index == 3
    assert not first.frontier.done
    finished, emitted, _ = collect(checkpoint, inputs, limits(3, transitions=3))
    assert emitted == () and finished.checkpoint.artifacts_seen == 17


def test_byte_admission_accounts_for_coverage_and_keeps_the_unselected_frontier():
    checkpoint = new_incremental_volume_planner(policy=policy())
    inputs = (artifact(0, 3 * PART),)
    expected = advance_incremental_volume_plan(checkpoint, inputs)
    required = max(
        len(body) + len(edges)
        for body, _members, edges in map(volume_emission_metadata, expected.volumes)
    )
    initial = VolumeEmissionFrontier(checkpoint)
    with pytest.raises(VolumeEmissionCapacity) as refused:
        advance_volume_emission(initial, inputs, limits=limits(metadata_bytes=1))
    assert refused.value.dimension == "metadata_bytes"
    assert initial == VolumeEmissionFrontier(checkpoint)
    state, emitted, quanta = collect(checkpoint, inputs, limits(metadata_bytes=required))
    assert emitted == expected.volumes and state.checkpoint == expected.checkpoint
    assert all(len(quantum.volumes) <= 1 for quantum in quanta)


def test_pack_coverage_refusal_preserves_membership_before_selection():
    checkpoint = new_incremental_volume_planner(policy=policy())
    inputs = (artifact(0, 3), artifact(1, 4), artifact(2, PART))
    first = advance_volume_emission(
        VolumeEmissionFrontier(checkpoint), inputs, limits=limits(transitions=2)
    )
    assert first.volumes == () and len(first.frontier.checkpoint.pending_pack_artifacts) == 2
    saved = volume_emission_frontier_bytes(first.frontier)
    with pytest.raises(VolumeEmissionCapacity) as refused:
        advance_volume_emission(first.frontier, inputs, limits=limits(coverage_rows=1))
    assert refused.value.dimension == "coverage_rows"
    assert refused.value.required == 2
    assert volume_emission_frontier_bytes(first.frontier) == saved
    resumed = advance_volume_emission(first.frontier, inputs, limits=limits())
    expected = advance_incremental_volume_plan(checkpoint, inputs)
    assert resumed.volumes == expected.volumes


@pytest.mark.parametrize(
    "changes",
    [
        {"input_index": -1},
        {"input_index": True},
        {"raw_offset": 1},
        {"raw_offset": PART, "done": True},
        {"done": "true"},
    ],
)
def test_frontier_rejects_invalid_scalar_or_raw_state(changes):
    with pytest.raises(ValueError):
        VolumeEmissionFrontier(new_incremental_volume_planner(policy=policy()), **changes)


@pytest.mark.parametrize("field", ["plans", "coverage_rows", "metadata_bytes", "input_transitions"])
@pytest.mark.parametrize("value", [0, -1, True])
def test_emission_grants_require_positive_integral_admission(field, value):
    values = dict(plans=3, coverage_rows=16, metadata_bytes=10000, input_transitions=4)
    values[field] = value
    with pytest.raises(ValueError, match="positive integers"):
        VolumeEmissionLimits(**values)


def test_retained_request_bounds_completion_and_canonical_frontier():
    checkpoint = new_incremental_volume_planner(policy=policy())
    inputs = (artifact(0, 2 * PART),)
    state, _, _ = collect(checkpoint, inputs, limits())
    idle = advance_volume_emission(state, inputs, limits=limits())
    assert idle.frontier == state and idle.volumes == () and idle.input_transitions == 0
    for request, final in (((), False), (inputs, True)):
        with pytest.raises(ValueError):
            advance_volume_emission(state, request, limits=limits(), final=final)
    with pytest.raises(ValueError, match="no retained member"):
        advance_volume_emission(
            VolumeEmissionFrontier(checkpoint, raw_offset=PART), (), limits=limits()
        )
    payload = json.loads(volume_emission_frontier_bytes(state))
    payload["unknown"] = "field"
    with pytest.raises(ValueError, match="format"):
        parse_volume_emission_frontier(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    payload.pop("unknown")
    payload["done"] = "true"
    with pytest.raises(ValueError, match="boolean"):
        parse_volume_emission_frontier(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def test_frontier_preserves_sequence_domain_and_existing_plan_serialization_refusal():
    checkpoint = replace(
        new_incremental_volume_planner(policy=policy()), next_sequence=(1 << 256) - 1
    )
    frontier = VolumeEmissionFrontier(checkpoint)
    assert parse_volume_emission_frontier(volume_emission_frontier_bytes(frontier)) == frontier
    with pytest.raises(ValueError):
        advance_volume_emission(frontier, (artifact(0, PART),), limits=limits(1))

    checkpoint = replace(checkpoint, next_sequence=1 << 53)
    inputs = (artifact(0, 2 * PART),)
    original = advance_incremental_volume_plan(checkpoint, inputs)
    first = advance_volume_emission(VolumeEmissionFrontier(checkpoint), inputs, limits=limits(1))
    assert first.volumes == original.volumes[:1]
    assert volume_emission_metadata(first.volumes[0]) == volume_emission_metadata(
        original.volumes[0]
    )
    saved = volume_emission_frontier_bytes(first.frontier)
    with pytest.raises(ValueError, match="not exact in binary64"):
        volume_emission_metadata(original.volumes[1])
    with pytest.raises(ValueError, match="not exact in binary64"):
        advance_volume_emission(first.frontier, inputs, limits=limits(1))
    assert volume_emission_frontier_bytes(first.frontier) == saved
