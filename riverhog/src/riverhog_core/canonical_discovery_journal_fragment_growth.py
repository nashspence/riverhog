"""Primitive bounds for one exact completed-source physical fact read."""

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


def declare_growth_for_journal_fragment(namespace: dict[str, Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value, unit=()), term))

    def total(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    metadata, payload = dim("metadata_octets"), dim("fragment_octets")
    declaration = Declaration(
        id="riverhog-server.discovery-journal-fact-fragment.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.canonical_discovery_journal_fragments.read_journal_fact_fragment",
        ),
        scope="one exact source-qualified completed physical fact read",
        starts_at="receive the original Native source, exact prefix, assertion, stream and ordinal",
        ends_at="close every Core cursor and detach at most one verified physical fragment or end",
        dimensions=(
            Dimension(
                "retained_rows",
                COUNT,
                "All retained source, assertion, task, stream and segment rows.",
            ),
            Dimension(
                "metadata_octets",
                OCTETS,
                "Complete input/control row width; fragment bytes are charged separately.",
            ),
            Dimension(
                "fragment_octets",
                OCTETS,
                "The next physical payload size, or zero for end/capacity rejection.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(5),
                "Three source/assertion/fact fences, a stream read and a fragment/end read.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(5),
                "Each Core statement detaches at most one row.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                scale(32, metadata),
                "All original coordinates, versions, stream keys and the physical ordinal.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                total(scale(24, metadata), payload),
                "Complete source/control rows and only the requested bounded payload.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                payload,
                "Only the returned physical bytes are hashed by this owner.",
            ),
            Bound(
                "scalar_encoded_octets",
                OCTETS,
                "upper",
                scale(4, metadata),
                "Canonical source entry-count controls.",
            ),
            Bound(
                "scalar_decoded_octets",
                OCTETS,
                "upper",
                scale(16, metadata),
                "Exact source versions, interval and original entry sequence.",
            ),
            Bound(
                "source_checks",
                COUNT,
                "equal",
                count(1),
                "One actual Native source liveness dispatch.",
            ),
            Bound(
                "native_anchor_calls",
                COUNT,
                "equal",
                count(1),
                "One exact original prefix dispatch.",
            ),
            Bound(
                "native_reference_calls",
                COUNT,
                "equal",
                count(1),
                "One original documentary assertion address dispatch.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No source assertion or journal entry is decoded or copied.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                total(
                    count(512),
                    Expression(
                        "product",
                        terms=(
                            count(128),
                            Expression("log2_ceil", terms=(total(dim("retained_rows"), count(2)),)),
                        ),
                    ),
                ),
                "Indexed point reads, predecessor-version join and terminal excess-row seek.",
            ),
        ),
        conditions=(
            "The current completed source and fact schema uses its original exact primary keys.",
            "Native owns admission, prefix/address validation and borrowed-source custody.",
            "The caller supplies positive capacity and retains its next exact stream/ordinal.",
            "A fragment has at most 4096 bytes; the stream has no additional total-size limit.",
            "Control width includes every field and arbitrary-width journal controls.",
        ),
        invariants=(
            "Capacity rejection precedes payload access and changes no cursor or facts.",
            "Retired documentary assertions retain their original source bytes.",
            "Missing or damaged work never becomes an empty value or successful end.",
            "A terminal proves the stored end, not that a caller read every preceding fragment.",
        ),
        limitations=(
            "Bounds count Core work and Native dispatches; child bodies have separate costs.",
            "This read grants no scope, access, publication or complete-value authority.",
            "Consumers own logical decoding, source-normal inverse indexes and full traversal.",
            "Storage-engine CPU/I/O, lock waits and elapsed time are separate observations.",
        ),
    )
    name = "read_journal_fact_fragment"
    namespace[name] = declare_growth(declaration)(namespace[name])
