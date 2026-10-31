"""Core checkpoint costs around the separately owned Native corpus quanta."""

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
    declare_growth,
    growth_binding,
)

from riverhog_core.canonical_discovery_selection import frozen_corpus_selection


def declare_replay_growth(operations: dict[str, Callable[..., Any]]) -> None:
    child = growth_binding(frozen_corpus_selection).declaration
    for mode, operation in operations.items():
        repeats = 2 if mode == "begin" else 1
        statements = {"begin": 6, "progress": 2, "next": 4, "prepare": 4, "apply": 8}[mode]
        rows = {"begin": 2, "progress": 2, "next": 4, "prepare": 4, "apply": 4}[mode]

        def count(value: int) -> Expression:
            return Expression("constant", value=value, unit=COUNT)

        def width(value: int) -> Expression:
            return Expression(
                "product",
                terms=(
                    Expression("constant", value=value),
                    Expression("dimension", name="metadata_octets"),
                ),
            )

        own = {
            "sql_statements": count(statements),
            "returned_rows": count(rows),
            "parameter_octets": width(statements),
            "returned_octets": width(rows),
            "written_octets": width(int(mode in ("begin", "apply"))),
            "encoded_octets": width(0),
            "decoded_octets": width(int(mode in ("next", "prepare"))),
            "hashed_octets": width(0),
            "sha256_block_octets": width(0),
            "source_reads": count(0),
            "source_read_octets": width(0),
            "native_anchor_calls": count(2 if mode == "prepare" else 0),
            "sqlite_vm_instructions": Expression(
                "sum",
                terms=(
                    count(4096),
                    Expression(
                        "product",
                        terms=(
                            count(512),
                            Expression(
                                "log2_ceil",
                                terms=(
                                    Expression(
                                        "sum",
                                        terms=(
                                            Expression("dimension", name="registered_inputs"),
                                            count(2),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        }
        bounds = []
        for bound in child.bounds:
            base = bound.expression
            if repeats != 1:
                base = Expression("product", terms=(Expression("constant", value=repeats), base))
            bounds.append(
                Bound(
                    bound.metric,
                    bound.unit,
                    "upper",
                    Expression("sum", terms=(base, own[bound.metric])),
                    "Expanded frozen-input checks plus fixed indexed cursor/receipt and CAS work.",
                )
            )
        bounds.append(
            Bound(
                "native_phase_calls",
                COUNT,
                "upper",
                count(int(mode == "prepare")),
                "One public Native replay, selection, semantic, dependency or order quantum.",
            )
        )
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-corpus-replay-" + mode + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="direct Core cursor work and dispatch count around one owned Native quantum",
                starts_at="receive frozen input identities and the original persisted cursor",
                ends_at="detach a phase result or atomically publish its original continuation",
                dimensions=(
                    Dimension("registered_inputs", COUNT, "All independently retained Core rows."),
                    Dimension(
                        "metadata_octets",
                        OCTETS,
                        "Largest complete original control tuple, metadata row or preparation.",
                    ),
                ),
                bounds=tuple(bounds),
                conditions=(
                    "The application retains current archive/worker and exclusive kernel custody.",
                    "Frozen input records stay immutable under their owning registration protocol.",
                    "Each Native call retains its own measured declaration and bounded budgets.",
                    "Native body work, including callbacks into its source port, is charged there.",
                    "Core preparation runs outside writes; apply uses the owner's transaction.",
                    "The maintained schema has its original indexes and no added triggers.",
                ),
                invariants=(
                    "Full sources and clipped selections retain independent original counts.",
                    "One next input uses indexed keyset access, without OFFSET or whole-set scans.",
                    "Only the exact current detached result can advance the original Core cursor.",
                    "Lost Native/Core confirmations retry the same source or phase without reset.",
                    "Completion grants no member relevance, access or generation publication.",
                ),
                limitations=(
                    "This is Core dispatch/checkpoint evidence, not total Native corpus work.",
                    "Native public phase declarations own frames, semantic rows and graph edges.",
                    "VM excludes internal byte comparisons and physical I/O; timing is observed.",
                ),
            )
        )(operation)
