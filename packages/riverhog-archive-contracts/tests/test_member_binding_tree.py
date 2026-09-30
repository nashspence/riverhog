from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_archive_contracts import (
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
