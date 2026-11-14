"""Logical work of exact preparation catalog operations and bounded discovery."""

from __future__ import annotations

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
)


def catalog_declaration(subject: str, mode: str, *, selecting: bool = False) -> Declaration:
    def constant(value: int, unit: tuple[tuple[str, int], ...] = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def times(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    statements = 15 if selecting else 3
    return Declaration(
        id="riverhog.primary-facts-catalog-" + mode + ".growth/v1",
        authority="riverhog",
        subjects=(subject,),
        scope="one exact primary preparation catalog " + mode + " operation",
        starts_at="receive current original controls or bounded seek hints",
        ends_at="return one detached record or the exact catalog decision",
        dimensions=(
            Dimension("retained_rows", COUNT, "Independent rows in the preparation catalog."),
            Dimension(
                "metadata_row_octets", OCTETS, "Largest complete actual or proposed row width."
            ),
            Dimension("control_octets", OCTETS, "Complete actual argument and predicate widths."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                constant(statements),
                "Point operations use one mutation/read; final absence receiving adds one read. "
                "Selection has five indexed phases, each one snapshot, one seek and one wrap.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                constant(10 if selecting else 2),
                "Every statement returns at most one exact row, including negative decisions.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                times(10 if selecting else 2, dimension("metadata_row_octets")),
                "Retain full rows including opaque construction and namespace identity widths.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                times(
                    statements,
                    add(
                        dimension("control_octets"),
                        dimension("metadata_row_octets"),
                        constant(256, OCTETS),
                    ),
                ),
                "All old fences, proposed fields, generated versions and bounded seek controls.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                times(
                    statements,
                    add(
                        constant(8192),
                        Expression(
                            "product",
                            terms=(
                                constant(2048),
                                Expression(
                                    "log2_ceil",
                                    terms=(add(constant(2), dimension("retained_rows")),),
                                ),
                            ),
                        ),
                    ),
                ),
                "Exact primary/collection seeks and state/workspace keysets; selection excludes "
                "at most three keys and does not filter an unbounded owner population in SQL.",
            ),
        ),
        conditions=(
            "The maintained catalog indexes and schema constraints are present.",
            "The caller owns metadata admission; one row is never split or silently omitted.",
            "Selection accepts at most three exclusions and returns one candidate.",
        ),
        invariants=(
            "Catalog ownership precedes all preparation namespace effects.",
            "No catalog operation reads index contents or enumerates a directory.",
            "Original, epoch, phase, version, root and namespace pins fence each mutation.",
            "One unfinished namespace per collection bounds repeated capacity refusals.",
        ),
        limitations=(
            "Logical SQL, metadata and SQLite VM work excludes latency, physical I/O, internal "
            "byte comparisons, and the separately owned physical reclamation worker.",
        ),
    )
