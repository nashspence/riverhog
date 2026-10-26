"""Bounds for fixed activity preparation and exact journal-control replay."""

from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)


def _declare(namespace: dict[str, Any], *, applying: bool) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def total(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    metadata = dim("metadata_octets")
    controls = dim("source_controls") if applying else count(128)
    receipts = (
        Expression("product", terms=(controls, dim("control_octets")))
        if applying
        else scale(512, metadata)
    )
    logarithm = Expression("log2_ceil", terms=(total(dim("retained_rows"), count(2)),))
    limits = {
        "sql_statements": total(count(32), controls) if applying else count(128),
        "returned_rows": total(count(24), controls) if applying else count(128),
        "parameter_octets": total(scale(128, metadata), receipts),
        "returned_octets": total(scale(128, metadata), receipts),
        "encoded_octets": total(scale(128, metadata), receipts),
        "decoded_octets": scale(128, metadata),
        "hashed_octets": scale(32, metadata),
        "compared_octets": total(scale(128, metadata), scale(2, receipts)),
        "changed_rows": count(4 if applying else 0),
        "written_octets": scale(8 if applying else 0, metadata),
        "source_payload_octets": Expression("constant", value=0, unit=OCTETS),
        "source_checks": count(16 if applying else 64),
        "native_anchor_calls": count(16 if applying else 64),
        "native_reference_calls": count(16 if applying else 64),
        "sqlite_vm_instructions": total(
            count(16384),
            Expression(
                "product",
                terms=(scale(1024, total(controls, count(1))), logarithm),
            ),
        ),
    }
    name = "apply_journal_selector" if applying else "prepare_activity_selector"
    module = (
        "canonical_discovery_journal_selector_work"
        if applying
        else ("canonical_discovery_activity_selectors")
    )
    declaration = Declaration(
        id="riverhog-server.discovery-"
        + ("journal-selector-apply" if applying else "activity-selector-prepare")
        + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core." + module + "." + name,),
        scope="one fixed activity atom or its exact original journal-control replay",
        starts_at="receive exact pending coordinates under current metadata custody",
        ends_at="detach one source-bound incidence or commit it with the same cursor",
        dimensions=(
            Dimension("retained_rows", COUNT, "All retained discovery input and graph rows."),
            Dimension("metadata_octets", OCTETS, "Maximum complete input, row or receipt control."),
            *(
                (
                    Dimension(
                        "source_controls", COUNT, "Original one-row reads in the preparation."
                    ),
                    Dimension(
                        "control_octets", OCTETS_PER_COUNT, "Maximum octets per original receipt."
                    ),
                )
                if applying
                else ()
            ),
        ),
        bounds=tuple(
            Bound(
                metric,
                OCTETS if metric.endswith("octets") else COUNT,
                "upper",
                value,
                "Fixed source fences, indexed seeks and exact source/incidence receipts.",
            )
            for metric, value in limits.items()
        ),
        conditions=(
            "The application retains current metadata custody and worker-epoch fencing.",
            "The complete source manifest, Native source and endpoint index retain their owners.",
            "Activity preparation visits one original outgoing endpoint or fixed phase per atom.",
            "Each receipt is a complete capacity-bounded one-row SELECT, including physical BLOBs.",
            "The number of prepared controls is bounded by the preparing fixed selector owner.",
        ),
        invariants=(
            "An effective activity has one documentary-own link and original context incidences.",
            "Non-context endpoints advance explicitly; duplicate incidences retain set semantics.",
            "Every source control is rechecked inside the incidence/cursor transaction.",
            "Source membership is exact archive admission, not independent corpus acceptance.",
            "No entry, assertion payload, snapshot view or member-context product is rebuilt.",
        ),
        limitations=(
            "Native/source child bodies retain their independently declared costs.",
            "Other selector meanings, query applicability and current access retain their owners.",
            "Total atoms and retries belong to the caller's fixed graph traversal profile.",
            "VM instructions exclude I/O, lock waits and SQLite internal byte comparisons.",
        ),
    )
    namespace[name] = declare_growth(declaration)(namespace[name])


def declare_growth_for_journal_selector_apply(namespace: dict[str, Any]) -> None:
    _declare(namespace, applying=True)


def declare_growth_for_activity_selector(namespace: dict[str, Any]) -> None:
    _declare(namespace, applying=False)
