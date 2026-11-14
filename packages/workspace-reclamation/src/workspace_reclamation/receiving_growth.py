"""Inclusive native bounds for retained neutral file-extent attempts."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from . import _declaration

if TYPE_CHECKING:
    from . import FileReclamationOwner


def _count(value: int) -> Expression:
    return Expression("constant", value=value, unit=COUNT)


def _dimension(name: str) -> Expression:
    return Expression("dimension", name=name)


def _scale(value: int, term: Expression) -> Expression:
    return Expression("product", terms=(Expression("constant", value=value), term))


def _declarations() -> tuple[Declaration, Declaration]:
    original = _declaration()
    bounds = tuple(
        replace(
            bound,
            expression=Expression(
                "sum",
                terms=(
                    _scale(2, _dimension("candidate_files")),
                    _count(5),
                ),
            ),
            meaning="Both exact filename passes, original identity and native/Python type probes.",
        )
        if bound.metric == "metadata_calls"
        else bound
        for bound in original.bounds
    ) + (
        Bound(
            "encoded_directory_octets",
            OCTETS,
            "upper",
            _dimension("directory_octets"),
            "The complete original directory path is encoded once before acquisition.",
        ),
        Bound(
            "native_control_octets",
            OCTETS,
            "upper",
            Expression(
                "sum",
                terms=(
                    _scale(4, _dimension("directory_octets")),
                    _scale(4, _dimension("maximum_name_octets")),
                    Expression("constant", value=6, unit=OCTETS),
                ),
            ),
            "Complete operands of actual native directory and selected-name path validation.",
        ),
        Bound(
            "dup_calls",
            COUNT,
            "equal",
            _count(0),
            "Borrowing never duplicates or adopts a raw descriptor.",
        ),
    )
    attempt = replace(
        original,
        id="workspace-reclamation.native-file-extent-attempt.growth/v1",
        subjects=("workspace_reclamation.FileReclamationOwner.reclaim",),
        starts_at="caller retention and physical/ordinary admission before resource acquisition",
        ends_at="positive original native receiving before progress, or a retained failed attempt",
        dimensions=original.dimensions
        + (Dimension("directory_octets", OCTETS, "Complete encoded original directory path."),),
        bounds=bounds,
        conditions=original.conditions
        + (
            "The exact original receiver and charge survive every failed or interrupted reply.",
            "GIL-enabled Linux/Darwin CPython and cooperative cancellation remain required.",
        ),
        invariants=original.invariants
        + (
            "A receiver executes its original mutation at most once; close retries only receive.",
            "Unknown results quarantine old integers and preserve original parent dependencies.",
            "Actual executor exclusion precedes borrow receiving; worker join remains separate.",
        ),
    )
    receiving = replace(
        attempt,
        id="workspace-reclamation.native-file-receiving.growth/v1",
        subjects=("workspace_reclamation.FileReclamationOwner.close",),
        scope="one receiving retry on the exact original retained extent owner",
        starts_at="nonblocking exclusion of the actual original descriptor-using executor",
        ends_at="positive original native receiving or a retained refusal/unknown result",
        bounds=tuple(
            replace(
                bound,
                relation="upper" if bound.metric == "close_calls" else "equal",
                expression=Expression(
                    "constant",
                    value=2 if bound.metric == "close_calls" else 0,
                    unit=bound.unit,
                ),
                meaning=(
                    "At most one native close per original resource lifetime."
                    if bound.metric == "close_calls"
                    else "Receiving repeats no extent, namespace, encoding or acquisition work."
                ),
            )
            for bound in bounds
        ),
        conditions=(
            "The caller retains the exact original receiver and its resource charge.",
            "GIL-enabled Linux/Darwin CPython and cooperative cancellation remain required.",
        ),
    )
    return attempt, receiving


def declare_receiving_growth(owner: type[FileReclamationOwner]) -> None:
    attempt, receiving = _declarations()
    declare_growth(attempt)(owner.reclaim)
    declare_growth(receiving)(owner.close)
