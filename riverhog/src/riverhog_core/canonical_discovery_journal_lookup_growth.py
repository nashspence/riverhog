"""Direct exact header lookup costs beneath separately qualified Native source ports."""

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


def declare_growth_for_journal_lookup(namespace: dict[str, Any]) -> None:
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

    for mode in ("documentary", "effective"):
        operation = namespace[mode + "_journal_assertion"]
        limits = {
            "sql_statements": count(4 if mode == "effective" else 3),
            "returned_rows": count(3),
            "parameter_octets": width(8),
            "returned_octets": width(3),
            "encoded_octets": width(3),
            "decoded_octets": width(0),
            "scalar_encoded_octets": width(2),
            "scalar_decoded_octets": width(8),
            "source_checks": count(1),
            "native_anchor_calls": count(1),
            "native_reference_calls": count(1),
            "source_payload_octets": width(0),
            "hashed_octets": width(0),
            "sqlite_vm_instructions": Expression(
                "sum",
                terms=(
                    count(512),
                    Expression(
                        "product",
                        terms=(
                            count(128),
                            Expression(
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
                            ),
                        ),
                    ),
                ),
            ),
        }
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-journal-lookup-" + mode + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="exact source assertion intervals and completed fact header reads",
                starts_at="receive an exact Native source, prefix and assertion or object ID",
                ends_at="return absence or an original address and completed fact reference",
                dimensions=(
                    Dimension(
                        "retained_rows", COUNT, "Actual retained source, assertion and fact rows."
                    ),
                    Dimension(
                        "metadata_octets",
                        OCTETS,
                        "Largest complete input, selected row or reference.",
                    ),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        OCTETS if metric.endswith("octets") else COUNT,
                        "upper",
                        value,
                        "At most four indexed metadata lookups and fixed Native source dispatch."
                        if mode == "effective"
                        else "Three indexed metadata lookups and fixed Native source dispatch.",
                    )
                    for metric, value in limits.items()
                ),
                conditions=(
                    "The caller holds current archive, build, worker and source custody.",
                    "Original source keys and the historical index exist without extra triggers.",
                    "Native source and documentary ports keep their own body costs.",
                ),
                invariants=(
                    "Incomplete source coverage never supplies a negative or positive lookup.",
                    "Results bind the original source, exact prefix and completed fact child.",
                    "Retirement preserves documentary history without effective fallback.",
                    "No assertion payload or expanded historical graph is read.",
                    "A header grants no corpus acceptance, member scope or current access.",
                ),
                limitations=(
                    "Native bodies, physical I/O and internal byte comparisons are separate.",
                    "Complete causal and effective-history scope remain separate work.",
                ),
            )
        )(operation)
