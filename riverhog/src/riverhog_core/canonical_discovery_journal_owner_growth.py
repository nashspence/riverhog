"""Primitive bounds for one original documentary owner-header seek."""

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


def declare_growth_for_journal_owner(namespace: dict[str, Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def width(value: int) -> Expression:
        return Expression(
            "product",
            terms=(
                Expression("constant", value=value, unit=()),
                Expression("dimension", name="metadata_octets"),
            ),
        )

    limits = {
        "sql_statements": count(8),
        "returned_rows": count(8),
        "parameter_octets": width(32),
        "returned_octets": width(8),
        "encoded_octets": width(8),
        "decoded_octets": width(0),
        "scalar_encoded_octets": width(8),
        "scalar_decoded_octets": width(24),
        "source_checks": count(2),
        "native_anchor_calls": count(2),
        "native_reference_calls": count(2),
        "source_payload_octets": width(0),
        "hashed_octets": width(0),
        "written_octets": width(0),
        "sqlite_vm_instructions": Expression(
            "sum",
            terms=(
                count(2048),
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
    declaration = Declaration(
        id="riverhog-server.discovery-journal-owner-next.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_journal_owners.next_owner_assertion",),
        scope="one original own-id assertion header at an exact qualified prefix",
        starts_at="receive a source, exact prefix, referent and optional original-header cursor",
        ends_at="detach at most one original documentary header, including withdrawals",
        dimensions=(
            Dimension(
                "retained_rows", COUNT, "All retained original source, assertion and fact rows."
            ),
            Dimension(
                "metadata_octets", OCTETS, "Maximum full input, returned row or original reference."
            ),
        ),
        bounds=tuple(
            Bound(
                metric,
                OCTETS if metric.endswith("octets") else COUNT,
                "upper",
                value,
                "Fixed source/cursor checks and three indexed numeric-width/coordinate seeks.",
            )
            for metric, value in limits.items()
        ),
        conditions=(
            "The application holds current archive/build/worker and original source custody.",
            "Original source/fact terminals and the historical-owner index remain available.",
            "The caller retains the cursor with its exact source/referent/prefix request.",
        ),
        invariants=(
            "Own-id lookup preserves documentary births even after their exact retirement.",
            "Results preserve original addresses and completed canonical fact references.",
            "A changed cursor or source cannot silently skip to another owner or assertion.",
            "Numeric-width transitions use indexed seeks without temporary sorting.",
            "No assertion payload, historical graph or per-member snapshot relation is built.",
        ),
        limitations=(
            "Native source/prefix/address child bodies retain their independently owned costs.",
            "Scope, current authorization and full corpus acceptance remain separate owners.",
            "Effective own-id lookup remains the separately owned exact effective point accessor.",
        ),
    )
    name = "next_owner_assertion"
    namespace[name] = declare_growth(declaration)(namespace[name])
