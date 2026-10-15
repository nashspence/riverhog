"""Detached bounded degree updates for private epoch ranking."""

from __future__ import annotations

from collections.abc import Mapping

from riverhog_provenance_contracts import require_canonical_uuid_urn

EPOCH_RANK_ALGORITHM = "riverhog-journal-epoch-rank/v1"


def decrement_rank_degrees(
    targets: tuple[str, ...], degrees: Mapping[str, int]
) -> tuple[tuple[str, int], ...]:
    """Visit only the admitted unique adjacency targets and their point degrees."""
    if len(targets) > 64 or len(set(targets)) != len(targets):
        raise ValueError("invalid private rank adjacency window")
    result = []
    for vertex in targets:
        require_canonical_uuid_urn(vertex)
        degree = degrees[vertex]
        if type(degree) is not int or degree <= 0:
            raise RuntimeError("private rank adjacency lost an unsettled degree")
        result.append((vertex, degree - 1))
    return tuple(result)
