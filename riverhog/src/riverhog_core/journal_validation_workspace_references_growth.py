"""Fixed release ports for an independently retained parent and child."""

from __future__ import annotations

from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)


def declare_growth_for_reference_group(owner: type[Any]) -> None:
    declare_growth(
        Declaration(
            id="riverhog-server.validation-workspace-references-close.growth/v1",
            authority="riverhog-server",
            subjects=(owner.__module__ + "." + owner.__qualname__ + ".close",),
            scope="direct actual child-before-parent reference release ports",
            starts_at="receive the retained actual parent and child references",
            ends_at="confirm each release in order or retain the same uncertain reference",
            dimensions=(
                Dimension("retained_journals", COUNT, "Independent retained source count."),
            ),
            bounds=(
                Bound(
                    "reference_closes",
                    COUNT,
                    "upper",
                    Expression("constant", value=2, unit=COUNT),
                    "At most the selected child and parent; no source or sibling traversal.",
                ),
            ),
            conditions=("The caller serializes closure after Native data confirms closure.",),
            invariants=(
                "Child closure confirms before parent closure begins.",
                "Failed closure retains the identical reference for retry.",
            ),
            limitations=(
                "Each reference close body retains its own declared and qualified costs.",
                "Physical I/O and operating-system latency are excluded.",
            ),
        )
    )(owner.close)
