"""Primitive and total-work contracts for source-normal endpoint index atoms."""

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


def declare_growth_for_endpoint_index(namespace: dict[str, Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value, unit=()), term))

    def total(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    metadata, endpoint = dimension("metadata_octets"), dimension("endpoint_octets")
    for name, sql, rows in (
        ("begin_endpoint_index", 16, 10),
        ("endpoint_index_progress", 6, 6),
        ("prepare_endpoint_index", 56, 56),
        ("apply_endpoint_index", 20, 12),
        ("next_endpoint_candidate", 8, 8),
    ):
        preparing = name == "prepare_endpoint_index"
        payload = endpoint if preparing else Expression("constant", value=0, unit=OCTETS)
        declaration = Declaration(
            id="riverhog-server.discovery-endpoint-" + name.replace("_", "-") + ".growth/v1",
            authority="riverhog-server",
            subjects=("riverhog_core.canonical_discovery_endpoint_index." + name,),
            scope="Core primitives for one exact source-normal endpoint index atom",
            starts_at="receive source/attempt-bound work, cursor and endpoint execution capacity",
            ends_at="detach one directory/candidate row or atomically commit one bounded atom",
            dimensions=(
                Dimension(
                    "retained_rows", COUNT, "All retained source, fact, task and index rows."
                ),
                Dimension(
                    "metadata_octets", OCTETS, "Maximum complete input or stored control-row width."
                ),
                Dimension(
                    "endpoint_octets", OCTETS, "Four original control payload sizes in this atom."
                ),
            ),
            bounds=(
                Bound(
                    "sql_statements",
                    COUNT,
                    "upper",
                    count(sql),
                    "Fixed point/range seeks and bounded mutations.",
                ),
                Bound(
                    "returned_rows",
                    COUNT,
                    "upper",
                    count(rows),
                    "At most one row per metadata seek.",
                ),
                Bound(
                    "parameter_octets",
                    OCTETS,
                    "upper",
                    scale(384, metadata),
                    "Full controls, exact receipts and coordinate bindings.",
                ),
                Bound(
                    "returned_octets",
                    OCTETS,
                    "upper",
                    total(scale(256, metadata), payload),
                    "Full control rows plus one prepared endpoint.",
                ),
                Bound(
                    "written_octets",
                    OCTETS,
                    "upper",
                    scale(32, metadata),
                    "One index row and one fixed-field work header.",
                ),
                Bound(
                    "encoded_octets",
                    OCTETS,
                    "upper",
                    scale(128, metadata),
                    "Canonical source, child and last-output control receipts.",
                ),
                Bound(
                    "decoded_octets",
                    OCTETS,
                    "upper",
                    total(scale(8, metadata), payload),
                    "Fixed-field receipt decoding and endpoint controls.",
                ),
                Bound(
                    "scalar_encoded_octets",
                    OCTETS,
                    "upper",
                    scale(128, metadata),
                    "Canonical source positions and private arbitrary-width counters.",
                ),
                Bound(
                    "scalar_decoded_octets",
                    OCTETS,
                    "upper",
                    scale(256, metadata),
                    "Exact source, stream and cursor controls.",
                ),
                Bound(
                    "hashed_octets",
                    OCTETS,
                    "upper",
                    scale(2, payload),
                    "Only endpoint preparation verifies and hashes original control bytes.",
                ),
                Bound(
                    "source_checks",
                    COUNT,
                    "upper",
                    count(16),
                    "Actual source liveness dispatches, including endpoint child reads.",
                ),
                Bound(
                    "native_anchor_calls",
                    COUNT,
                    "upper",
                    count(8),
                    "Original source-prefix dispatches.",
                ),
                Bound(
                    "native_reference_calls",
                    COUNT,
                    "upper",
                    count(8),
                    "Original documentary-address dispatches.",
                ),
                Bound(
                    "source_payload_octets",
                    OCTETS,
                    "equal",
                    Expression("constant", value=0, unit=OCTETS),
                    "No source entry or assertion reconstruction.",
                ),
                Bound(
                    "sqlite_vm_instructions",
                    COUNT,
                    "upper",
                    total(
                        count(8192),
                        Expression(
                            "product",
                            terms=(
                                count(2048),
                                Expression(
                                    "log2_ceil",
                                    terms=(total(dimension("retained_rows"), count(2)),),
                                ),
                            ),
                        ),
                    ),
                    "Fixed indexed seeks and index-maintaining writes; no retained-row scan.",
                ),
            ),
            conditions=(
                "Existing metadata custody and current-worker epoch fences surround this work.",
                "The complete original journal and fact tasks retain qualified immutable "
                "terminals.",
                "All source, directory and inverse indexes are current and available.",
                "Native fixed-domain endpoint fields fit one physical fragment each.",
                "Endpoint capacity may explicitly reject preparation without advancing its cursor.",
            ),
            invariants=(
                "One successful build visits each original assertion and fact stream once.",
                "Without retries, F streams and A assertions require F+2*A+1 advancing atoms.",
                "Each original edge is prepared once, independent of member and snapshot counts.",
                "Directory stream/fragment/octet totals reconcile with the original fact-task "
                "terminal.",
                "Original assertion totals reconcile with the completed journal terminal.",
                "One exact output and its next cursor commit together under "
                "source/attempt/version fences.",
                "Identical committed retries add no rows or counters; rollback preserves the "
                "unfinished atom.",
                "Candidate digests retain exact source coordinates and never establish equality.",
            ),
            limitations=(
                "The complete source-normal pass includes non-edge stream-directory metadata.",
                "Native source liveness, prefix and address child bodies have separate owned "
                "costs.",
                "Corpus acceptance, original-byte equality, scope selectors and current "
                "access remain separate.",
                "SQLite index comparison byte costs and elapsed time are distinct from VM "
                "instructions.",
                "Ordinary retries repeat only their unfinished bounded atom, not an earlier "
                "source pass.",
            ),
        )
        namespace[name] = declare_growth(declaration)(namespace[name])
