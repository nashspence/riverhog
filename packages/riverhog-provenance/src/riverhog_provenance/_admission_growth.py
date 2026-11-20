"""Shared accounting for fixed-shape private child checkpoint admission."""

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
)


def admission_declaration(*, identifier: str, subject: str, planning: bool) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    width = Expression("dimension", name="metadata_row_octets")

    def octets(rows: int) -> Expression:
        return Expression("product", terms=(count(rows), width))

    zero_octets = Expression("constant", value=0, unit=OCTETS)
    return Declaration(
        id="riverhog-provenance.sqlite-"
        + identifier
        + "-admission-"
        + ("plan" if planning else "application")
        + ".growth/v1",
        authority="riverhog-provenance",
        subjects=(subject,),
        scope="detached-private-child-admission-plan"
        if planning
        else "owner-transaction-private-child-admission-application",
        starts_at="receive exact admitted source identity and probe the owned checkpoint"
        if planning
        else "receive an exact prepared admission inside the owner transaction",
        ends_at="return a fixed-shape proposal without native mutations"
        if planning
        else "return the admitted child state while preserving the owner transaction",
        dimensions=(
            Dimension("retained_rows", COUNT, "Rows retained in the indexed private workspace."),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Input-derived envelope for fixed-shape old/new checkpoint, prefix, source "
                "and subject columns and one statement's identity/control parameters.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(32),
                "Fixed prefix, subject, prior-header and quiescence checks plus admission writes.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(16),
                "Point headers/subjects and bounded first-row quiescence probes.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "equal" if planning else "upper",
                count(0 if planning else 2),
                "Planning writes nothing; application writes one header and at most "
                "one control frame.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                octets(16),
                "Actual fixed-shape metadata columns, retaining identity/scalar byte widths.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                octets(32),
                "Actual bound point-key, prefix, checkpoint and initial control values.",
            ),
            Bound(
                "written_value_octets",
                OCTETS,
                "equal" if planning else "upper",
                zero_octets if planning else octets(2),
                "Mutation parameters include the complete checkpoint and its exact "
                "identity fences.",
            ),
            Bound(
                "adjacency_rows",
                COUNT,
                "equal",
                count(0),
                "Admission never traverses a native graph adjacency window.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                zero_octets,
                "Source/assertion/identity payloads are not fetched by checkpoint admission.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(16384),
                        Expression(
                            "product",
                            terms=(
                                count(2048),
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
                "Fixed control and indexed seeks; native B-tree comparisons and physical I/O "
                "are separately observed costs.",
            ),
        ),
        conditions=(
            "The owner retains the exact source/catalog/modes and the declared private "
            "schema/indexes.",
            "Prior child projections satisfy their owner's quiescence and completion "
            "prerequisites.",
            "Planning is detached; application receives the exact proposal and an open "
            "parent transaction.",
            "The metadata envelope includes actual source/prefix/control and input "
            "identity widths.",
        ),
        invariants=(
            "Accepted prefix and existing source/assertion payloads remain unchanged.",
            "Planning never writes; application neither commits nor rolls back "
            "the owner transaction.",
            "Replaced or unfinished attempts cannot silently overwrite another child's progress.",
            "Private admission grants no accepted-prefix, archive or source-release authority.",
        ),
        limitations=(
            "Constructor decoding, source indexing and catalog construction/hash work are separate "
            "owner-admitted operations, not free work supplied by this profile.",
            "This successful-call profile does not qualify standalone initialize, "
            "parent publication "
            "or whole-session composition; actual caller/delegate observations remain required.",
            "Encoded values exclude Python/SQLite allocation overhead; instruction counts exclude "
            "B-tree byte comparisons, physical I/O, waits and fsync.",
        ),
    )
