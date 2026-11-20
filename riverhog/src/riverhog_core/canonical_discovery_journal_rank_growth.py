"""Direct rank SQL, complete controls and separately qualified source/Native ports."""

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


def declare_growth_for_journal_rank(namespace: dict[str, Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    width = Expression("dimension", name="metadata_octets")
    depth = Expression(
        "log2_ceil",
        terms=(
            Expression(
                "sum",
                terms=(
                    count(2),
                    Expression("dimension", name="retained_rows"),
                ),
            ),
        ),
    )
    for mode, name in (
        ("progress", "discovery_journal_rank_progress"),
        ("begin", "begin_discovery_journal_rank"),
        ("prepare", "prepare_discovery_journal_rank"),
        ("apply", "apply_discovery_journal_rank"),
    ):
        reading, preparing, publishing = mode == "progress", mode == "prepare", mode == "apply"
        limits = {
            "sql_statements": count(7 if reading else 96 if preparing or publishing else 1),
            "returned_rows": count(7 if reading else 132 if preparing else 0),
            "changed_rows": count(65 if publishing else int(mode == "begin")),
            "returned_octets": scale(132 if preparing else 7, width)
            if preparing or reading
            else octets(0),
            "parameter_octets": scale(1024, width),
            "sqlite_vm_instructions": Expression(
                "sum",
                terms=(
                    count(65536),
                    Expression("product", terms=(count(8192), depth)),
                ),
            ),
            "union_progress_calls": count(int(not publishing)),
            "progress_calls": count(int(not reading)),
            "native_plan_calls": count(int(preparing)),
            "native_plan_input_octets": scale(256, width) if preparing else octets(0),
            "identity_checks": count(128 if preparing else 12 if reading else 0),
            "identity_input_octets": scale(128 if preparing else 12, width)
            if preparing or reading
            else octets(0),
            "scalar_encoded_octets": scale(1024, width),
            "scalar_decoded_octets": scale(1024, width),
        }
        operation = namespace[name]
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-journal-rank-" + mode + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="direct bounded rank cursor, vertex and graph SQL with qualified ports",
                starts_at="receive the current complete original union and owning metadata custody",
                ends_at="prepare one bounded rank atom or publish it with its exact next cursor",
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
                        "Fixed controls; at most 64 seed/adjacency rows and one ready vertex.",
                    )
                    for metric, value in limits.items()
                ),
                conditions=(
                    "Caller serializes the exact source under metadata/kernel/worker custody.",
                    "Preparation has no write transaction; publication uses the owner transaction.",
                    "The ready index and maintained union point/adjacency keys exist "
                    "without triggers.",
                    "Original union progress and the Native planner retain their "
                    "maintained bounds.",
                    "Input widths include rejected controls and complete selected row values.",
                ),
                invariants=(
                    "Each graph vertex is seeded once; each settled vertex's unique "
                    "arcs visit once.",
                    "A high-degree source retains its last target across bounded adjacency steps.",
                    "All vertex changes, graph results and cursor publish atomically.",
                    "No graph census, terminal count, payload read or total-history cap is added.",
                    "Cyclic graphs remain explicit and private ranking grants no "
                    "semantic acceptance.",
                ),
                limitations=(
                    "One bounded page and one graph transition define the direct SQL envelope.",
                    "Logical VM/bytes exclude comparisons, allocation and machine latency.",
                    "Certificate reads, Native replay and full workflow composition are separate.",
                ),
            )
        )(operation)
