"""Direct Core framing directory costs around separately owned source/Native calls."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    Unit,
    declare_growth,
)


def declare_growth_for_archive_framing(operations: dict[str, Callable[..., Any]]) -> None:
    def number(value: int, unit: Unit = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def product(*terms: Expression) -> Expression:
        return Expression("product", terms=terms)

    def summed(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    positions = dimension("position_budget")
    scalar_positions = product(number(1, (("count", -1),)), positions)
    depth = Expression("log2_ceil", terms=(summed(dimension("registered_inputs"), number(2)),))
    for mode, operation in operations.items():
        width = product(
            summed(number(64, ()), product(number(8, ()), scalar_positions)),
            dimension("metadata_octets"),
        )
        limits = {
            "sql_statements": summed(number(40), product(number(3, ()), positions)),
            "returned_rows": number(20),
            "returned_octets": product(number(32, ()), dimension("metadata_octets")),
            "parameter_octets": width,
            "written_octets": width if mode in {"begin", "apply"} else number(0, OCTETS),
            "encoded_octets": number(0, OCTETS),
            "decoded_octets": number(0, OCTETS),
            "hashed_octets": number(0, OCTETS),
            "sha256_block_octets": number(0, OCTETS),
            "source_reads": number(0),
            "source_read_octets": number(0, OCTETS),
            "source_current_calls": number(2 if mode in {"begin", "prepare", "apply"} else 1),
            "source_window_calls": number(int(mode == "prepare")),
            "native_framing_calls": number(int(mode == "prepare")),
            "returned_positions": positions,
            "scalar_encoded_octets": width,
            "scalar_decoded_octets": width,
            "sqlite_vm_instructions": summed(
                number(8192),
                product(number(2048), depth),
                product(
                    positions,
                    summed(number(128, ()), product(number(64, ()), depth)),
                ),
            ),
        }
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-archive-framing-" + mode + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="direct Core original-frame directory and bounded child-call counts",
                starts_at="receive current source owner, original cursor and explicit budgets",
                ends_at="return a detached unit, indexed position or atomically committed cursor",
                dimensions=(
                    Dimension("registered_inputs", COUNT, "All independently retained Core rows."),
                    Dimension(
                        "metadata_octets",
                        OCTETS,
                        "Largest full original incoming control, row or scalar coordinate.",
                    ),
                    Dimension(
                        "position_budget",
                        COUNT,
                        "Requested positions, one lookup, or original held unit size.",
                    ),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        OCTETS if metric.endswith("octets") else COUNT,
                        "upper",
                        expression,
                        "Fixed indexed control work plus the admitted original positions.",
                    )
                    for metric, expression in limits.items()
                ),
                conditions=(
                    "The caller serializes work beneath current Core metadata and kernel custody.",
                    "Begin/apply use the owner transaction; preparation has no write transaction.",
                    "The maintained primary keys remain present and no triggers are added.",
                    "Position budget includes the original held prepared unit on retry/read calls.",
                ),
                invariants=(
                    "Original full source identity and current worker precede directory access.",
                    "Original positions and their exact cursor commit or roll back together.",
                    "No preceding frame, source prefix or unrelated directory is scanned.",
                    "Source window count and Native cursor extent independently bound each unit.",
                    "Speculative framing failure supplies no semantic or publication acceptance.",
                ),
                limitations=(
                    "Source currentness/window bodies retain ArchiveJournalSource declarations.",
                    "Native framing byte visits retain advance_journal_framing's declaration.",
                    "Outer metadata fences, physical I/O/CPU and semantic replay retain owners.",
                    "Last-link restart checks are not a complete private-file corruption audit.",
                ),
            )
        )(operation)
