"""Exact primitive bounds for shared local workspace retention."""

from collections.abc import Callable
from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)


def declare_workspace_retention_growth(operation: Callable[..., Any], *, retire: bool) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    name = "retire" if retire else "try_retain_workspace"
    declare_growth(
        Declaration(
            id="riverhog-server.construction-retention-"
            + ("retire" if retire else "workspace-admit")
            + ".growth/v1",
            authority="riverhog-server",
            subjects=(
                "riverhog_core.services.archive_construction_retention.ArchiveConstructionRetention."
                + name,
            ),
            scope="one exact shared original reservation " + ("release" if retire else "admission"),
            starts_at="receive the exact original, receiver and shared capacity",
            ends_at="return a reservation, exact withdrawal or explicit deferral",
            dimensions=(
                Dimension("retained_slots", COUNT, "Independent retained original reservations."),
                Dimension(
                    "control_octets_per_step",
                    OCTETS_PER_COUNT,
                    "Full widest actual mapping key/control width per primitive.",
                ),
            ),
            bounds=(
                Bound("mapping_steps", COUNT, "upper", count(16), "Complete mapping primitives."),
                Bound("mapping_enumerations", COUNT, "equal", count(0), "No reservation census."),
                Bound(
                    "reservations_created",
                    COUNT,
                    "upper",
                    count(0 if retire else 1),
                    "At most one new exact original charge.",
                ),
                Bound(
                    "reservations_released",
                    COUNT,
                    "upper",
                    count(1 if retire else 0),
                    "Only the identity-checked original can withdraw its charge.",
                ),
                Bound(
                    "mapping_key_octets",
                    OCTETS,
                    "upper",
                    Expression(
                        "product",
                        terms=(
                            count(16),
                            Expression("dimension", name="control_octets_per_step"),
                        ),
                    ),
                    "Full actual keys of all observed mapping primitives.",
                ),
                Bound(
                    "payload_octets",
                    OCTETS,
                    "equal",
                    Expression("constant", value=0, unit=OCTETS),
                    "No resource body is inspected.",
                ),
            ),
            conditions=(
                "The maintained process-local identity ledger and configured capacities apply.",
                "Workspace admissions use the ordinary shared allowance.",
            ),
            invariants=(
                "Independent cleanup never spends reserved global terminal retention.",
                "Reservation precedes construction; it supplies no semantic authority.",
                "Only the actual original reservation and exact receiver release a charge.",
                "Busy registry locks return promptly with every original charge intact.",
            ),
            limitations=(
                "Mapping/hash internals retain interpreter ownership.",
                "The caller proves actual resource closure and execution join before withdrawal.",
            ),
        )
    )(operation)
