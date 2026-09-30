"""Bounded-memory inclusion proofs for root-bound member-history bindings."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from riverhog_canonical_json import canonical_json_bytes

from .member_history import MemberHistoryBinding

_LEAF = b"riverhog-member-history-binding-leaf/v1\x00"
_NODE = b"riverhog-member-history-binding-node/v1\x00"


def _hex(value: str) -> bytes:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("member binding tree digest is invalid")
    return bytes.fromhex(value)


def _leaf(binding: MemberHistoryBinding) -> bytes:
    return hashlib.sha256(_LEAF + canonical_json_bytes(binding.to_mapping())).digest()


def _node(left: bytes, right: bytes) -> bytes:
    return hashlib.sha256(_NODE + left + right).digest()


@dataclass(frozen=True, slots=True)
class BindingTreeCommitment:
    count: int
    root_sha256: str
    target_index: int | None
    target_siblings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _Subtree:
    digest: bytes
    includes_target: bool


def binding_tree_commitment(
    bindings: Iterable[MemberHistoryBinding],
    *,
    target_artifact_id: str | None = None,
) -> BindingTreeCommitment:
    """Commit ordered bindings and optionally prove one leaf in one streamed pass.

    The split rule is the largest power of two below the subtree size. The
    frontier holds only logarithmically many hashes, independent of collection
    size; proof siblings reveal no other member binding.
    """

    frontier: list[_Subtree | None] = []
    siblings: list[str] = []
    count = 0
    target_index: int | None = None
    previous_id: str | None = None

    def combine(left: _Subtree, right: _Subtree) -> _Subtree:
        if left.includes_target and right.includes_target:
            raise ValueError("member binding target occurs twice")
        if left.includes_target:
            siblings.append(right.digest.hex())
        elif right.includes_target:
            siblings.append(left.digest.hex())
        return _Subtree(
            _node(left.digest, right.digest),
            left.includes_target or right.includes_target,
        )

    for binding in bindings:
        if previous_id is not None and binding.artifact_id <= previous_id:
            raise ValueError("member history bindings are not strictly ID ordered")
        previous_id = binding.artifact_id
        selected = binding.artifact_id == target_artifact_id
        if selected:
            target_index = count
        carry = _Subtree(_leaf(binding), selected)
        level = 0
        while level < len(frontier) and frontier[level] is not None:
            left = frontier[level]
            assert left is not None
            carry = combine(left, carry)
            frontier[level] = None
            level += 1
        if level == len(frontier):
            frontier.append(carry)
        else:
            frontier[level] = carry
        count += 1
    if count == 0:
        raise ValueError("member binding tree cannot be empty")
    if target_artifact_id is not None and target_index is None:
        raise ValueError("member binding target is absent")
    folded: _Subtree | None = None
    for subtree in frontier:
        if subtree is None:
            continue
        folded = subtree if folded is None else combine(subtree, folded)
    assert folded is not None
    return BindingTreeCommitment(count, folded.digest.hex(), target_index, tuple(siblings))


def verify_binding_inclusion(
    binding: MemberHistoryBinding,
    *,
    count: int,
    index: int,
    siblings: Iterable[str],
    expected_root_sha256: str,
) -> None:
    """Verify one exact leaf against a source provenance root commitment."""

    # Unique 256-bit artifact IDs cannot describe more than this many leaves.
    # This is the identity representation bound, not an admission ceiling.
    if (
        type(count) is not int
        or type(index) is not int
        or not 1 <= count <= 2**256
        or not 0 <= index < count
    ):
        raise ValueError("member binding inclusion position is invalid")
    expected = _hex(expected_root_sha256)
    ordered: Iterator[str] = iter(siblings)

    def reconstruct(position: int, size: int) -> bytes:
        if size == 1:
            return _leaf(binding)
        split = 1 << ((size - 1).bit_length() - 1)
        if position < split:
            left = reconstruct(position, split)
            try:
                right = _hex(next(ordered))
            except StopIteration as exc:
                raise ValueError("member binding inclusion proof is incomplete") from exc
        else:
            right = reconstruct(position - split, size - split)
            try:
                left = _hex(next(ordered))
            except StopIteration as exc:
                raise ValueError("member binding inclusion proof is incomplete") from exc
        return _node(left, right)

    actual = reconstruct(index, count)
    if next(ordered, None) is not None or actual != expected:
        raise ValueError("member binding inclusion proof differs from the source root")


__all__ = [
    "BindingTreeCommitment",
    "binding_tree_commitment",
    "verify_binding_inclusion",
]
