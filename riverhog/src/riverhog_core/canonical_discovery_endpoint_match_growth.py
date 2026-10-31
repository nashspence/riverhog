"""Primitive bounds for one exact original endpoint candidate comparison."""

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


def declare_growth_for_endpoint_match(namespace: dict[str, Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value, unit=()), term))

    def total(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    metadata, operand, endpoint = (
        dim("metadata_octets"),
        dim("target_octets"),
        dim("endpoint_octets"),
    )
    declaration = Declaration(
        id="riverhog-server.discovery-original-endpoint-match.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_endpoint_matches.advance_endpoint_match",),
        scope="Core primitives for one inverse candidate and original-byte equality check",
        starts_at="receive a completed source index, exact requested prefix and typed target",
        ends_at="detach one verified match, one unmatched position or explicit indexed end",
        dimensions=(
            Dimension("retained_rows", COUNT, "All retained source, fact, work and index rows."),
            Dimension("metadata_octets", OCTETS, "Maximum complete input or stored control row."),
            Dimension(
                "target_octets", OCTETS, "Exact local ID or complete foreign-reference operand."
            ),
            Dimension(
                "endpoint_octets",
                OCTETS,
                "Maximum four-control payload size among this source index's candidates.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(56),
                "One inverse candidate, original header and at most eight physical reads.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(56),
                "Each source/index/header/fragment seek detaches at most one row.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                scale(384, metadata),
                "Full source controls and exact point/range bindings.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                total(scale(256, metadata), endpoint),
                "Original endpoint controls and complete metadata.",
            ),
            Bound(
                "encoded_octets",
                OCTETS,
                "upper",
                total(scale(128, metadata), operand),
                "Exact typed operand and original source/reference controls.",
            ),
            Bound(
                "decoded_octets",
                OCTETS,
                "upper",
                total(scale(8, metadata), endpoint),
                "Original interval and endpoint-control decoding.",
            ),
            Bound(
                "scalar_encoded_octets",
                OCTETS,
                "upper",
                scale(128, metadata),
                "Exact original source positions and intervals.",
            ),
            Bound(
                "scalar_decoded_octets",
                OCTETS,
                "upper",
                scale(256, metadata),
                "Source, original interval, fact and index controls.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                total(operand, scale(2, endpoint)),
                "One operand digest plus original endpoint fragment/target hashes.",
            ),
            Bound(
                "compared_octets",
                OCTETS,
                "upper",
                total(operand, endpoint),
                "Bytes submitted to the one exact target comparison.",
            ),
            Bound(
                "source_checks",
                COUNT,
                "upper",
                count(16),
                "Actual original source liveness dispatches.",
            ),
            Bound(
                "native_anchor_calls",
                COUNT,
                "upper",
                count(12),
                "Requested prefix, complete original header and endpoint fields.",
            ),
            Bound(
                "native_reference_calls",
                COUNT,
                "upper",
                count(12),
                "Original assertion address and endpoint fields.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No source entry or assertion reconstruction.",
            ),
            Bound(
                "written_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Matching returns detached state and writes no rows.",
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
                "One inverse keyset seek and fixed original metadata/fragment seeks.",
            ),
        ),
        conditions=(
            "The existing application metadata custody and current-worker fences remain current.",
            "The complete source-normal endpoint index retains its qualified original terminals.",
            "The caller supplies maintained typed Native object-reference models.",
            "Original Native domains bound these endpoint controls; execution capacity may reject.",
            "The caller retains its position with this exact source/prefix/role/target/state "
            "request.",
        ),
        invariants=(
            "A digest only selects candidates; exact original bytes establish target equality.",
            "A foreign operand binds the complete original reference, including entry and "
            "assertion.",
            "Each call inspects at most one candidate, including collisions and ineligible "
            "intervals.",
            "Birth and retirement are checked against the original full-source assertion header.",
            "Effective matching uses the exact requested prefix; documentary matching retains "
            "withdrawals.",
            "Context normalization preserves each original full role and independent record "
            "coordinate.",
        ),
        limitations=(
            "Native source/prefix/address child bodies keep their independently owned costs.",
            "This accessor grants no scope, member attribution, semantic acceptance or "
            "current access.",
            "Shared selector compilation and correlated imported/causal applicability remain "
            "separate.",
            "Total candidate visits and retries belong to the caller's declared traversal profile.",
        ),
    )
    name = "advance_endpoint_match"
    namespace[name] = declare_growth(declaration)(namespace[name])
