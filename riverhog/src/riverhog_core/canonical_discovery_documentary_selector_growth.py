"""Bounds for one original documentary context-dependency atom."""

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


def declare_growth_for_documentary_dependencies(namespace: dict[str, Any]) -> None:
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

    limits = {
        "sql_statements": count(512),
        "returned_rows": count(512),
        "parameter_octets": width(512),
        "returned_octets": width(512),
        "encoded_octets": width(2048),
        "decoded_octets": width(512),
        "hashed_octets": width(128),
        "compared_octets": width(128),
        "changed_rows": count(0),
        "written_octets": width(0),
        "source_payload_octets": width(0),
        "source_checks": count(256),
        "native_anchor_calls": count(256),
        "native_reference_calls": count(256),
        "sqlite_vm_instructions": Expression(
            "sum",
            terms=(
                count(16384),
                Expression(
                    "product",
                    terms=(
                        count(16384),
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
        id="riverhog-server.discovery-documentary-dependencies-prepare.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.canonical_discovery_documentary_selectors."
            "prepare_documentary_dependencies",
        ),
        scope="one immutable owner or original State-reference candidate",
        starts_at="receive exact pending coordinates under current archive and metadata custody",
        ends_at="detach one original-source-bound context incidence and monotone cursor",
        dimensions=(
            Dimension("retained_rows", COUNT, "All retained discovery input and graph rows."),
            Dimension("metadata_octets", OCTETS, "Maximum complete input, row or receipt control."),
        ),
        bounds=tuple(
            Bound(
                metric,
                OCTETS if metric.endswith("octets") else COUNT,
                "upper",
                value,
                "Fixed original controls and indexed seeks, including prior incidence replay.",
            )
            for metric, value in limits.items()
        ),
        conditions=(
            "The caller retains current metadata custody and actual worker-epoch fencing.",
            "Archive admission, source manifest, Native lease and index retain their owners.",
            "Observation and reported-description references are fixed fields, never arrays.",
            "Both the previous and next claim retain their complete original control receipts.",
            "Source capacity rejects a complete control that cannot fit; it never truncates one.",
        ),
        invariants=(
            "An Own atom resolves its immutable context; Observed advances one candidate or end.",
            "Withdrawn documentary claims retain dependencies; future claims advance without one.",
            "Only declared source_context_id fields create direct context incidences.",
            "Duplicate incidences retain set semantics without repeated prefix reconstruction.",
            "Own/Observed assertion leaves remain indexed and do not copy member-claim products.",
        ),
        limitations=(
            "Native/source child bodies retain their independently declared costs.",
            "The common journal-selector apply owner replays all original controls atomically.",
            "Claim selection, query, corpus acceptance and current access retain their owners.",
            "Own needs one atom; Observed needs candidate count plus one terminal.",
            "VM instructions exclude I/O, lock waits and SQLite internal byte comparisons.",
        ),
    )
    name = "prepare_documentary_dependencies"
    namespace[name] = declare_growth(declaration)(namespace[name])
