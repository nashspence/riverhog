"""Bounded original-source union SQL and separately qualified Native/source ports."""

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


def declare_growth_for_journal_union(namespace: dict[str, Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    def scale(value: int, expression: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), expression))

    def add(*values: Expression) -> Expression:
        return Expression("sum", terms=values)

    width = Expression("dimension", name="metadata_octets")
    depth = Expression(
        "log2_ceil",
        terms=(
            add(
                count(2),
                Expression("dimension", name="retained_rows"),
            ),
        ),
    )
    for mode, name in (
        ("progress", "discovery_journal_union_progress"),
        ("begin", "begin_discovery_journal_union"),
        ("prepare", "prepare_discovery_journal_union"),
        ("apply", "apply_discovery_journal_union"),
    ):
        reading, preparing, publishing = mode == "progress", mode == "prepare", mode == "apply"
        limits = {
            "sql_statements": count(5 if reading else 320 if preparing or publishing else 1),
            "returned_rows": count(5 if reading else 320 if preparing else 0),
            "changed_rows": count(261 if publishing else int(mode == "begin")),
            "returned_octets": scale(320 if preparing else 5, width)
            if preparing or reading
            else octets(0),
            "parameter_octets": scale(2048, width),
            "sqlite_vm_instructions": add(
                count(65536),
                Expression("product", terms=(count(8192), depth)),
            ),
            "topology_progress_calls": count(int(not publishing)),
            "topology_page_calls": count(int(reading or preparing)),
            "epoch_frame_calls": count(int(preparing)),
            "progress_calls": count(int(not reading)),
            "native_plan_calls": count(int(preparing)),
            "native_plan_input_octets": scale(512, width) if preparing else octets(0),
            "native_occurrence_checks": count(64 if preparing else 0),
            "native_occurrence_input_octets": scale(64, width) if preparing else octets(0),
            "identity_checks": count(64 if preparing else 3 if reading else 0),
            "identity_input_octets": scale(64 if preparing else 3, width)
            if preparing or reading
            else octets(0),
            "scalar_encoded_octets": scale(2048, width),
            "scalar_decoded_octets": scale(512, width),
        }
        operation = namespace[name]
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-journal-union-" + mode + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="direct bounded cursor/graph SQL, controls and observed qualified ports",
                starts_at="receive current original topology and owning metadata custody",
                ends_at="prepare one bounded delta or publish it with its exact next cursor",
                dimensions=(
                    Dimension("retained_rows", COUNT, "Actual independent retained private rows."),
                    Dimension(
                        "metadata_octets", OCTETS, "Complete keys, controls and selected rows."
                    ),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        OCTETS if metric.endswith("octets") else COUNT,
                        "upper",
                        value,
                        "Fixed controls; at most 64 edges, 128 endpoints and four graph patches.",
                    )
                    for metric, value in limits.items()
                ),
                conditions=(
                    "Caller serializes the exact source under metadata/kernel/worker custody.",
                    "Preparation has no write transaction; publication uses the owner transaction.",
                    "The active seed index and maintained point keys exist without added triggers.",
                    "Source pages and the Native planner retain their maintained bounds.",
                    "Input widths include rejected controls and complete selected row values.",
                ),
                invariants=(
                    "Each correction seeds each remaining active assertion once.",
                    "Parallel assertions retain identity while unique arcs change degrees once.",
                    "Cursor and all delta rows publish together; stale tokens cannot publish.",
                    "No graph scan, terminal count, payload read or total-history cap is added.",
                    "Private unions grant neither semantic nor correction-reference acceptance.",
                ),
                limitations=(
                    "One bounded page and two graph kinds define the direct SQL envelope.",
                    "Logical VM/bytes exclude comparisons, allocation and machine latency.",
                    "Rank proof, Native replay and complete workflow composition are separate.",
                ),
            )
        )(operation)
