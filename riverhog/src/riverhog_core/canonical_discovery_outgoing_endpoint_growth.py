"""Primitive bounds for one exact outgoing endpoint and its independent end."""

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


def declare_growth_for_outgoing_endpoint(namespace: dict[str, Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def total(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    metadata, endpoint = dim("metadata_octets"), dim("endpoint_octets")
    declaration = Declaration(
        id="riverhog-server.discovery-original-outgoing-endpoint.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_outgoing_endpoints.next_assertion_endpoint",),
        scope="Core primitives for one original outgoing endpoint or exact indexed end",
        starts_at="receive a completed source index, exact prefix and original assertion header",
        ends_at="detach one verified endpoint or an independently checked fact-directory end",
        dimensions=(
            Dimension("retained_rows", COUNT, "All retained source, fact, work and index rows."),
            Dimension("metadata_octets", OCTETS, "Maximum complete input or stored control row."),
            Dimension("endpoint_octets", OCTETS, "Maximum four-control payload for one endpoint."),
        ),
        bounds=(
            Bound(
                "sql_statements", COUNT, "upper", count(64), "Fixed controls and two keyset seeks."
            ),
            Bound(
                "returned_rows", COUNT, "upper", count(64), "Every seek returns at most one row."
            ),
            Bound(
                "parameter_octets", OCTETS, "upper", scale(384, metadata), "Exact seek bindings."
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                total(scale(256, metadata), endpoint),
                "Fixed metadata and original endpoint controls.",
            ),
            Bound(
                "encoded_octets", OCTETS, "upper", scale(128, metadata), "Original control JSON."
            ),
            Bound(
                "decoded_octets",
                OCTETS,
                "upper",
                total(scale(8, metadata), endpoint),
                "Original index and endpoint-control decoding.",
            ),
            Bound(
                "scalar_encoded_octets",
                OCTETS,
                "upper",
                scale(128, metadata),
                "Original source and interval positions.",
            ),
            Bound(
                "scalar_decoded_octets",
                OCTETS,
                "upper",
                scale(256, metadata),
                "Original source, fact and interval controls.",
            ),
            Bound(
                "hashed_octets", OCTETS, "upper", scale(2, endpoint), "Fragment and target hashes."
            ),
            Bound(
                "source_checks", COUNT, "upper", count(16), "Original source liveness dispatches."
            ),
            Bound(
                "native_anchor_calls", COUNT, "upper", count(12), "Exact original prefix checks."
            ),
            Bound(
                "native_reference_calls",
                COUNT,
                "upper",
                count(12),
                "Original assertion and endpoint addresses.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No source entry or whole assertion reconstruction.",
            ),
            Bound(
                "written_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Detached lookup writes no rows.",
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
                            Expression("log2_ceil", terms=(total(dim("retained_rows"), count(2)),)),
                        ),
                    ),
                ),
                "Two keyset seeks and fixed original metadata/fragment seeks.",
            ),
        ),
        conditions=(
            "Existing application metadata custody and current-worker fences remain current.",
            "The complete endpoint index retains its qualified original source terminals.",
            "The caller binds returned record keys to the exact source/prefix/assertion request.",
            "Original Native endpoint domains remain fixed; execution capacity may reject.",
        ),
        invariants=(
            "Each call visits at most one original endpoint, regardless of outgoing degree.",
            "Original fact-directory and index positions agree, including their explicit end.",
            "Full original endpoint bytes and assertion header qualify every returned index row.",
            "Documentary withdrawals remain visible at the exact requested original prefix.",
        ),
        limitations=(
            "Native source/prefix/address child bodies retain their independently owned costs.",
            "Effective selection, relevance, member attribution and access retain their owners.",
            "Total endpoint visits and retries belong to the caller's traversal profile.",
        ),
    )
    name = "next_assertion_endpoint"
    namespace[name] = declare_growth(declaration)(namespace[name])
