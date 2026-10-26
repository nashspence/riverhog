"""Indexed epoch checkpoints with separately qualified original-source and Native ports."""

from __future__ import annotations

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


def declare_growth_for_journal_epochs(namespace: dict[str, Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def scale(value: int, expression: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), expression))

    width = Expression("dimension", name="metadata_octets")
    entry = Expression("dimension", name="frame_octets")
    depth = Expression(
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
    )
    for mode, name in (
        ("progress", "discovery_journal_epoch_progress"),
        ("begin", "begin_discovery_journal_epochs"),
        ("prepare", "prepare_discovery_journal_epoch"),
        ("apply", "apply_discovery_journal_epoch"),
    ):
        preparing, reading = mode == "prepare", mode == "progress"
        sql = 2 if mode in {"progress", "apply"} else int(mode == "begin")
        limits = {
            "sql_statements": count(sql),
            "returned_rows": count(2 if reading else 0),
            "changed_rows": count(2 if mode == "apply" else int(mode == "begin")),
            "returned_octets": scale(2, width)
            if reading
            else Expression("constant", value=0, unit=OCTETS),
            "parameter_octets": scale(16, width),
            "sqlite_vm_instructions": Expression(
                "sum", terms=(count(1024), Expression("product", terms=(count(256), depth)))
            ),
            "registration_calls": count(int(mode in {"progress", "begin"})),
            "progress_calls": count(int(mode != "progress")),
            "frame_position_calls": count(int(mode in {"progress", "prepare"})),
            "framing_progress_calls": count(int(mode in {"progress", "prepare"})),
            "source_exact_calls": count(3 if preparing else 0),
            "source_read_octets": entry
            if preparing
            else Expression("constant", value=0, unit=OCTETS),
            "native_classification_calls": count(int(preparing)),
            "native_classification_octets": entry
            if preparing
            else Expression("constant", value=0, unit=OCTETS),
            "scalar_encoded_octets": scale(12, width),
            "scalar_decoded_octets": scale(12, width),
        }
        operation = namespace[name]
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-journal-epochs-" + mode + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="direct cursor/fact SQL, complete scalar codecs and qualified child ports",
                starts_at="receive exact registered source and current original metadata custody",
                ends_at="return one prepared entry or atomically publish its fact and next cursor",
                dimensions=(
                    Dimension(
                        "retained_rows",
                        COUNT,
                        "Actual independent retained original and epoch rows.",
                    ),
                    Dimension(
                        "metadata_octets",
                        OCTETS,
                        "Complete keys, original binding and selected control rows.",
                    ),
                    Dimension(
                        "frame_octets",
                        OCTETS,
                        "Exact encoded original frame including its two separators.",
                    ),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        OCTETS if metric.endswith("octets") else COUNT,
                        "upper",
                        value,
                        "At most one selected frame; fixed primary-key rows and complete controls.",
                    )
                    for metric, value in limits.items()
                ),
                conditions=(
                    (
                        "Metadata/kernel/worker authority is current and the caller serializes "
                        "the atom."
                    ),
                    (
                        "Begin/apply use the owning short transaction; prepare has no write "
                        "transaction."
                    ),
                    (
                        "Each framing, registration, normalized source and Native child keeps its"
                        " own bounds."
                    ),
                    "SQLite uses the maintained primary keys without added triggers.",
                ),
                invariants=(
                    (
                        "Native interprets exact original bytes; private epoch facts grant no "
                        "acceptance."
                    ),
                    (
                        "Persist the original child binding before preparation; never relabel "
                        "identity."
                    ),
                    (
                        "Fact and progress publish together; stale/retried atoms cannot duplicate"
                        " progress."
                    ),
                    "Later speculative failure defers to ordered Native replay.",
                    "Counters use their original scalar domains without a total-history ceiling.",
                ),
                limitations=(
                    (
                        "These direct costs do not replace child bodies or full graph/replay "
                        "composition."
                    ),
                    (
                        "One existing permitted JSON entry is a bounded decode atom, even above "
                        "one page."
                    ),
                    (
                        "SQL VM, scalar/byte primitives exclude internal comparisons and physical"
                        " latency."
                    ),
                ),
            )
        )(operation)
