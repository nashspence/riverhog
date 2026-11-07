"""Actual lease-reference acquisition, close ports and currentness calls."""

from __future__ import annotations

from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)


def declare_growth_for_workspace_reference(retain: Any, reference: type[Any]) -> None:
    for operation, retaining in ((retain, True), (reference.close, False)):
        path = Expression(
            "sum",
            terms=(
                Expression("dimension", name="directory_path_octets"),
                Expression("constant", value=7, unit=OCTETS),
            ),
        )
        limits = {
            "custody_checks": (COUNT, 2 if retaining else 0),
            "retention_callbacks": (COUNT, int(retaining)),
            "stream_constructions": (COUNT, int(retaining)),
            "descriptor_duplications": (COUNT, int(retaining)),
            "stream_closes": (COUNT, 1),
            "path_opens": (COUNT, 0),
            "directory_enumerations": (COUNT, 0),
            "payload_reads": (COUNT, 0),
            "payload_writes": (COUNT, 0),
        }
        declare_growth(
            Declaration(
                id="riverhog-server.validation-workspace-reference-"
                + ("retain" if retaining else "close")
                + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="direct independent lease-reference ports and separately owned child calls",
                starts_at="receive exact actual held custody or the same retained reference object",
                ends_at="retain the original lock or confirm closure; preserve failed cleanup",
                dimensions=(
                    Dimension(
                        "directory_path_octets", OCTETS, "Original complete native directory path."
                    ),
                    Dimension("sibling_workspaces", COUNT, "Independent retained sibling count."),
                ),
                bounds=(
                    *(
                        Bound(
                            metric,
                            unit,
                            "upper",
                            Expression("constant", value=value, unit=unit),
                            "One descriptor-owning stream and fixed currentness/retention work.",
                        )
                        for metric, (unit, value) in limits.items()
                    ),
                    Bound(
                        "stream_path_octets",
                        OCTETS,
                        "upper",
                        path if retaining else Expression("constant", value=0, unit=OCTETS),
                        "Only the original exact lease path labels the descriptor-owning stream.",
                    ),
                ),
                conditions=(
                    "The caller serializes custody, acquisition and close attempts.",
                    "Release follows confirmed closure of independently owned Native data.",
                    "The pinned stream owns duplicates and closes them idempotently.",
                ),
                invariants=(
                    "Duplication preserves the actual open-file description and flock.",
                    "An identity or path cannot replace current descriptor custody.",
                    "Failed acquisition cleanup and close keep the reference reachable.",
                    "Retries address the same stream throughout descriptor reuse.",
                    "No documentary bytes, sibling directories or catalog state are accessed.",
                ),
                limitations=(
                    "Currentness bodies keep their existing qualified bounds.",
                    "Retention callback bodies retain their caller-owned work and resources.",
                    "Logical stream ports exclude native internals and OS latency.",
                    "Native checkpoints, acceptance and application ownership stay separate.",
                ),
            )
        )(operation)
