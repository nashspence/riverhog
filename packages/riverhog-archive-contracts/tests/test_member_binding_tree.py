from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_archive_contracts import (
    MemberBindingTreeCheckpoint,
    MemberHistoryBinding,
    binding_tree_commitment,
    verify_binding_inclusion,
)


def _bindings(count: int) -> list[MemberHistoryBinding]:
    return [
        MemberHistoryBinding(
            artifact_id=f"{index:064x}",
            bytes=index,
            sha256="a" * 64,
            history_sha256=f"{index + 1:064x}",
            history_bytes=500,
        )
        for index in range(count)
    ]


@pytest.mark.parametrize("count", [1, 2, 3, 4, 5, 7, 8, 9, 16, 17])
def test_streamed_tree_proves_each_member_without_sibling_bindings(count: int) -> None:
    bindings = _bindings(count)
    root = binding_tree_commitment(iter(bindings))
    assert root.count == count
    assert root.target_index is None
    assert root.target_siblings == ()
    for index, binding in enumerate(bindings):
        proof = binding_tree_commitment(iter(bindings), target_artifact_id=binding.artifact_id)
        assert (proof.count, proof.root_sha256, proof.target_index) == (
            count,
            root.root_sha256,
            index,
        )
        verify_binding_inclusion(
            binding,
            count=count,
            index=index,
            siblings=proof.target_siblings,
            expected_root_sha256=root.root_sha256,
        )
        with pytest.raises(ValueError, match="differs from the source root"):
            verify_binding_inclusion(
                replace(binding, history_sha256="f" * 64),
                count=count,
                index=index,
                siblings=proof.target_siblings,
                expected_root_sha256=root.root_sha256,
            )
        if proof.target_siblings:
            with pytest.raises(ValueError, match="incomplete"):
                verify_binding_inclusion(
                    binding,
                    count=count,
                    index=index,
                    siblings=proof.target_siblings[:-1],
                    expected_root_sha256=root.root_sha256,
                )


def test_tree_rejects_duplicate_or_unordered_member_and_absent_target() -> None:
    bindings = _bindings(2)
    with pytest.raises(ValueError, match="ordered"):
        binding_tree_commitment((bindings[1], bindings[0]))
    with pytest.raises(ValueError, match="ordered"):
        binding_tree_commitment((bindings[0], bindings[0]))
    with pytest.raises(ValueError, match="target is absent"):
        binding_tree_commitment(bindings, target_artifact_id="f" * 64)


@pytest.mark.parametrize("count", [1, 2, 3, 4, 5, 7, 8, 9, 16, 17, 511, 512, 513, 1025])
def test_checkpoint_resumes_exact_archive_tree_at_every_binding(count: int) -> None:
    bindings = _bindings(count)
    checkpoint = MemberBindingTreeCheckpoint()
    for binding in bindings:
        checkpoint = checkpoint.append(binding)
        checkpoint = MemberBindingTreeCheckpoint.from_json_bytes(checkpoint.to_json_bytes())
    assert checkpoint.count == count
    assert checkpoint.root_sha256 == binding_tree_commitment(bindings).root_sha256
    with pytest.raises(ValueError, match="ordered"):
        checkpoint.append(bindings[-1])


def test_checkpoint_bound_follows_full_artifact_identity_space(monkeypatch) -> None:
    import riverhog_archive_contracts.member_binding_tree as owner

    checkpoint = MemberBindingTreeCheckpoint(2**256 - 1, f"{2**256 - 2:064x}", ("a" * 64,) * 256)
    calls = 0
    actual = owner._node

    def node(left, right):
        nonlocal calls
        calls += 1
        return actual(left, right)

    monkeypatch.setattr(owner, "_node", node)
    final = checkpoint.append(replace(_bindings(1)[0], artifact_id="f" * 64))
    assert calls == 256
    assert final.count == 2**256 and len(final.frontier) == 257
    assert len(final.to_json_bytes()) < 32 * 1024
    assert MemberBindingTreeCheckpoint.from_json_bytes(final.to_json_bytes()) == final
    assert len(final.root_sha256) == 64
    assert calls == 256
    with pytest.raises(ValueError, match="ordered"):
        final.append(replace(_bindings(1)[0], artifact_id="f" * 64))


@pytest.mark.parametrize(
    "count,previous,frontier",
    [
        (True, None, ()),
        (-1, None, ()),
        (2**256 + 1, "f" * 64, ()),
        (0, "a" * 64, ()),
        (1, None, ("a" * 64,)),
        (2, "0" * 64, (None, "a" * 64)),
        (2, "f" * 64, ("a" * 64, "a" * 64)),
        (1, "f" * 64, ("g" * 64,)),
        (1, "f" * 64, ()),
    ],
)
def test_checkpoint_rejects_invalid_position_or_frontier(count, previous, frontier):
    with pytest.raises(ValueError):
        MemberBindingTreeCheckpoint(count, previous, frontier)


def test_checkpoint_rejects_unbounded_or_noncanonical_state() -> None:
    checkpoint = MemberBindingTreeCheckpoint()
    with pytest.raises(ValueError, match="cannot be empty"):
        _ = checkpoint.root_sha256
    with pytest.raises(ValueError, match="byte bound"):
        MemberBindingTreeCheckpoint.from_json_bytes(b" " * (32 * 1024 + 1))
    with pytest.raises(ValueError):
        MemberBindingTreeCheckpoint.from_json_bytes(checkpoint.to_json_bytes() + b" ")


@pytest.mark.parametrize("field,value", [("previous_artifact_id", True), ("frontier", [7])])
def test_checkpoint_rejects_nonidentity_json_field_types(field, value):
    import json

    from riverhog_canonical_json import canonical_json_bytes

    checkpoint = MemberBindingTreeCheckpoint().append(_bindings(1)[0])
    document = json.loads(checkpoint.to_json_bytes())
    document[field] = value
    with pytest.raises(ValueError):
        MemberBindingTreeCheckpoint.from_json_bytes(canonical_json_bytes(document))
