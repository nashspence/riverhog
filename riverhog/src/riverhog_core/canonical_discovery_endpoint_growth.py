"""Primitive bounds for one fixed-domain original endpoint preparation."""

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


def declare_growth_for_endpoint(namespace: dict[str, Any]) -> None:
    def n(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value, unit=()), term))

    def total(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    metadata, payload = dim("metadata_octets"), dim("endpoint_octets")
    declaration = Declaration(
        id="riverhog-server.discovery-journal-endpoint-read.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_endpoint_source.read_journal_endpoint",),
        scope="Core primitives for one original fixed-domain Native endpoint",
        starts_at="receive exact source/prefix/assertion/edge coordinates and physical capacity",
        ends_at="detach verified edge controls and a candidate equality digest",
        dimensions=(
            Dimension(
                "retained_rows", COUNT, "All retained source/fact/stream/fragment work rows."
            ),
            Dimension("metadata_octets", OCTETS, "Maximum full input or stored control-row width."),
            Dimension("endpoint_octets", OCTETS, "Sum of four original control payload sizes."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                n(40),
                "At most eight five-statement physical reads.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                n(40),
                "Each physical read detaches at most five rows.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                scale(256, metadata),
                "Eight complete coordinate sets.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                total(scale(192, metadata), payload),
                "Complete metadata and four bounded payloads.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                scale(2, payload),
                "Fragment verification plus one target equality preimage.",
            ),
            Bound(
                "decoded_octets",
                OCTETS,
                "upper",
                payload,
                "One canonical header and at most one canonical reference.",
            ),
            Bound(
                "scalar_encoded_octets",
                OCTETS,
                "upper",
                scale(32, metadata),
                "Physical-reader source controls and edge ordinal.",
            ),
            Bound(
                "scalar_decoded_octets",
                OCTETS,
                "upper",
                scale(128, metadata),
                "Exact physical-reader source intervals and versions.",
            ),
            Bound("source_checks", COUNT, "upper", n(8), "Actual source liveness dispatches."),
            Bound(
                "native_anchor_calls", COUNT, "upper", n(8), "Actual original-prefix dispatches."
            ),
            Bound(
                "native_reference_calls",
                COUNT,
                "upper",
                n(8),
                "Actual documentary-address dispatches.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No original assertion or entry payload read.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                total(
                    n(4096),
                    Expression(
                        "product",
                        terms=(
                            n(1024),
                            Expression("log2_ceil", terms=(total(dim("retained_rows"), n(2)),)),
                        ),
                    ),
                ),
                "Eight physical-reader point/end seeks; Native child bodies are separate.",
            ),
        ),
        conditions=(
            "Completed current source/fact work retains its exact original primary keys.",
            "Native UUID/role/object/reference domains bound each control to one fragment.",
            "This includes Native's existing signed-64-bit entry-sequence reference domain.",
            "The supplied physical capacity may explicitly reject the next control payload.",
        ),
        invariants=(
            "Full original role and foreign-reference bytes survive in CoreEdge.",
            "Context roles share only an index selector; their original roles remain distinct.",
            "Candidate digests never establish equality without original-byte comparison.",
            "This primitive grants no causal, corpus, scope, access or publication authority.",
        ),
        limitations=(
            "Counts include Core subroutine SQL and hashing and actual Native dispatches.",
            "Native source/prefix/address child bodies retain their independently owned costs.",
            "Variable lexical/profile values have separate segmented work and costs.",
            "Index persistence, corpus acceptance, worker fences and queries have separate costs.",
        ),
    )
    name = "read_journal_endpoint"
    namespace[name] = declare_growth(declaration)(namespace[name])
