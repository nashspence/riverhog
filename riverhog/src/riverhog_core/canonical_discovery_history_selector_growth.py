"""Bounds for one fixed history/set incidence and its original source receipts."""

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


def declare_growth_for_history_selectors(namespace: dict[str, Any]) -> None:
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

    for name in ("prepare_history_selector", "apply_history_selector"):
        apply = name.startswith("apply")
        limits = {
            "sql_statements": count(80 if apply else 48),
            "returned_rows": count(64 if apply else 48),
            "parameter_octets": width(256),
            "returned_octets": width(64),
            "encoded_octets": width(192),
            "decoded_octets": width(96),
            "hashed_octets": width(32),
            "compared_octets": width(128),
            "changed_rows": count(4 if apply else 0),
            "written_octets": width(8 if apply else 0),
            "source_payload_octets": width(0),
            "sqlite_vm_instructions": Expression(
                "sum",
                terms=(
                    count(16384 if apply else 8192),
                    Expression(
                        "product",
                        terms=(
                            count(4096 if apply else 2048),
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
            id="riverhog-server.discovery-history-selector-"
            + ("apply" if apply else "prepare")
            + ".growth/v1",
            authority="riverhog-server",
            subjects=("riverhog_core.canonical_discovery_history_selectors." + name,),
            scope="one fixed history phase or original authored root/import set record",
            starts_at="receive exact pending coordinates under current archive/metadata custody",
            ends_at="detach one source-bound incidence or commit it with the same cursor",
            dimensions=(
                Dimension(
                    "retained_rows",
                    COUNT,
                    "All retained source, set, snapshot, selector and adjacency rows.",
                ),
                Dimension(
                    "metadata_octets",
                    OCTETS,
                    "Largest complete input, selected control or source receipt.",
                ),
            ),
            bounds=tuple(
                Bound(
                    metric,
                    OCTETS if metric.endswith("octets") else COUNT,
                    "upper",
                    value,
                    "Fixed indexed controls and original incidence/receipt checks.",
                )
                for metric, value in limits.items()
            ),
            conditions=(
                "The application retains metadata custody and current worker "
                "epoch around every use.",
                "Original archive, set, snapshot and Native primary owners "
                "have verified their inputs.",
                "Each preparation executes the fixed one-row control reads "
                "in this owner and its helpers.",
                "Execution capacity bounds each complete control and "
                "retained receipt without truncation.",
            ),
            invariants=(
                "Histories refer to shared authored root/import set "
                "selectors with constant incidences.",
                "Each set record advances explicitly, including excluded retained-only roots.",
                "Observed original/bound counts reconcile with independent set terminals.",
                "Original record keys, exact snapshot links and imported "
                "primary States are checked.",
                "Every source control used by preparation is rechecked "
                "inside the apply transaction.",
                "Imported reachability and documentary history do not grant "
                "causal relevance alone.",
            ),
            limitations=(
                "State/lifecycle/bound selectors, exact query evaluation and "
                "publication retain their owners.",
                "No assertion payload, member-by-snapshot or "
                "snapshot-by-assertion relation is reconstructed.",
                "VM excludes physical I/O, lock waits and SQLite internal byte comparison costs.",
                "Source receipts retain a fixed number of controls, not "
                "source pages or traversal paths.",
            ),
        )
        namespace[name] = declare_growth(declaration)(namespace[name])
