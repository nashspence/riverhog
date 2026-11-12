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
        limits = {
            "custody_checks": (COUNT, 2 if retaining else 0),
            "retention_callbacks": (COUNT, int(retaining)),
            "native_resource_records": (COUNT, int(retaining)),
            "native_dup_calls": (COUNT, int(retaining)),
            "native_fstat_calls": (COUNT, 0),
            "native_close_calls": (COUNT, 1),
            "native_control_octets": (OCTETS, 0),
            "receiving_calls": (COUNT, 1),
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
                            "One original native duplicate and fixed currentness/retention work.",
                        )
                        for metric, (unit, value) in limits.items()
                    ),
                ),
                conditions=(
                    "The caller serializes custody, acquisition and close attempts.",
                    "Release follows confirmed closure of independently owned Native data.",
                    "Native outcome-owned duplication retains the original parent dependency.",
                ),
                invariants=(
                    "Duplication preserves the actual open-file description and flock.",
                    "An identity or path cannot replace current descriptor custody.",
                    "Failed acquisition cleanup and close keep the reference reachable.",
                    "Retries address the same native outcome record throughout descriptor reuse.",
                    "No documentary bytes, sibling directories or catalog state are accessed.",
                ),
                limitations=(
                    "Currentness bodies keep their existing qualified bounds.",
                    "Retention callback bodies retain their caller-owned work and resources.",
                    "Native syscall observations exclude OS-internal work and latency.",
                    "Native checkpoints, acceptance and application ownership stay separate.",
                ),
            )
        )(operation)
