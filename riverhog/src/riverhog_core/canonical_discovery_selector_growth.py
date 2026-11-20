"""Primitive bounds for exact fixed selector registration and incidence quanta."""

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


def declare_growth_for_selectors(namespace: dict[str, Any]) -> None:
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

    for name in (
        "intern_selector",
        "next_selector",
        "prepare_selector_page",
        "apply_selector_page",
    ):
        preparing = name == "prepare_selector_page"
        writes = name in {"intern_selector", "apply_selector_page"}
        limits = {
            "sql_statements": count(0 if preparing else 24),
            "returned_rows": count(0 if preparing else 18),
            "parameter_octets": width(0 if preparing else 128),
            "returned_octets": width(0 if preparing else 18),
            "encoded_octets": width(24),
            "decoded_octets": width(8),
            "hashed_octets": width(12),
            "compared_octets": width(24),
            "changed_rows": count(4 if writes else 0),
            "written_octets": width(8 if writes else 0),
            "source_payload_octets": width(0),
            "sqlite_vm_instructions": count(0)
            if preparing
            else Expression(
                "sum",
                terms=(
                    count(4096),
                    Expression(
                        "product",
                        terms=(
                            count(1024),
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
            id="riverhog-server.discovery-fixed-selector-" + name.replace("_", "-") + ".growth/v1",
            authority="riverhog-server",
            subjects=("riverhog_core.canonical_discovery_selector_work." + name,),
            scope="one exact fixed selector or one detached unary/imported-late incidence quantum",
            starts_at=(
                "receive current metadata custody, stable archive attempt "
                "and fixed typed coordinates"
            ),
            ends_at="detach one pending control or atomically register one incidence and cursor",
            dimensions=(
                Dimension(
                    "retained_rows", COUNT, "All retained archive, selector and adjacency rows."
                ),
                Dimension(
                    "metadata_octets",
                    OCTETS,
                    "Largest complete input, stored row or encoded control.",
                ),
            ),
            bounds=tuple(
                Bound(
                    metric,
                    OCTETS if metric.endswith("octets") else COUNT,
                    "upper",
                    value,
                    "Fixed coordinate, receipt and primary checks; "
                    "indexed pending/point work and one incidence.",
                )
                for metric, value in limits.items()
            ),
            conditions=(
                "The application retains metadata custody and current worker-epoch "
                "checks around use.",
                "Original source, member and history verification retain their independent owners.",
                "An execution budget rejects an oversized complete control without truncation.",
                "A page contains at most one unary incidence or one fixed "
                "imported-late conjunction.",
            ),
            invariants=(
                "Exact canonical coordinates, not traversal paths, determine shared deduplication.",
                "Digest candidates compare complete original coordinate preimages before reuse.",
                "Pending reads seek the partial index rather than scan completed selectors.",
                "Incidence output and cursor commit together; retry rechecks retained output.",
                "Imported late conjunctions retain both operands and never become unary OR edges.",
                "The causal operand equals the same imported history's verified "
                "exact primary State.",
            ),
            limitations=(
                "This stores fixed incidences, not a configurable rule engine or query evaluator.",
                "An empty pending queue is not complete source coverage "
                "or Native corpus acceptance.",
                "No member/snapshot/assertion relation or query support is materialized here.",
                "VM excludes physical I/O, lock waits and SQLite internal byte comparisons.",
                "Compared bytes count submitted exact coordinate preimages, "
                "not Python field comparisons.",
            ),
        )
        namespace[name] = declare_growth(declaration)(namespace[name])
