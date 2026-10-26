"""Bound the exact input-State bridge across a fixed pair of admitted sources."""

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


def declare_growth_for_state_input(namespace: dict[str, Any]) -> None:
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
        "sql_statements": count(32),
        "returned_rows": count(32),
        "parameter_octets": width(32),
        "returned_octets": width(32),
        "encoded_octets": width(128),
        "decoded_octets": width(32),
        "hashed_octets": width(16),
        "compared_octets": width(16),
        "changed_rows": count(0),
        "written_octets": width(0),
        "source_payload_octets": width(0),
        "source_checks": count(16),
        "native_anchor_calls": count(16),
        "native_reference_calls": count(16),
        "sqlite_vm_instructions": Expression(
            "sum",
            terms=(
                count(4096),
                Expression(
                    "product",
                    terms=(
                        count(4096),
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
        id="riverhog-server.discovery-state-input-source.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_state_source_work.state_input_selector",),
        scope="one exact external input State under at most two original source leases",
        starts_at="receive one complete typed reference and current exact source bindings",
        ends_at="detach its effective historical State selector or report a source prerequisite",
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
                "Fixed complete reference controls and indexed source/interval seeks.",
            )
            for metric, value in limits.items()
        ),
        conditions=(
            "The parent captures these controls alongside its original derivation/reference reads.",
            "The parent replays all controls atomically with its incidence and cursor.",
            "Both admitted Native source leases remain current through apply.",
            "The caller retains metadata custody and actual worker-epoch fencing.",
            "Capacity rejects a complete control that cannot fit; it never truncates one.",
        ),
        invariants=(
            "The exact original entry address selects its historical prefix, never the latest one.",
            "Original entry JSON and journal prefix commitments retain distinct identities.",
            "The selected State matches both original referent and effective assertion identity.",
            "A missing source is a prerequisite without cursor progress, not an empty result.",
            "No entry/assertion payload, source scan or source-map materialization is performed.",
        ),
        limitations=(
            "Native/source child bodies retain their independently declared costs.",
            "Complete independent corpus acceptance and current access retain their owners.",
            "Source loading, total graph traversal and retry counts belong to the caller.",
            "VM instructions exclude I/O, lock waits and SQLite internal byte comparisons.",
        ),
    )
    name = "state_input_selector"
    namespace[name] = declare_growth(declaration)(namespace[name])
