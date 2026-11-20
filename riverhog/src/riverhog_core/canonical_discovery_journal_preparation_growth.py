"""Direct coordination work across four separately qualified preparation owners."""

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


def declare_growth_for_journal_preparation(namespace: dict[str, Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    width = Expression("dimension", name="metadata_octets")
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
        ("progress", "discovery_journal_preparation_progress"),
        ("begin", "begin_discovery_journal_preparation"),
        ("prepare", "prepare_discovery_journal_preparation"),
        ("apply", "apply_discovery_journal_preparation"),
    ):
        reading, starting, preparing = mode == "progress", mode == "begin", mode == "prepare"
        limits = {
            "sql_statements": count(3 if starting else 0 if preparing else 2),
            "returned_rows": count(2 if starting else 0 if preparing else 1),
            "returned_octets": scale(2 if starting else 0 if preparing else 1, width),
            "parameter_octets": scale(32, width),
            "changed_rows": count(int(mode in {"begin", "apply"})),
            "sqlite_vm_instructions": Expression(
                "sum",
                terms=(
                    count(8192),
                    Expression("product", terms=(count(2048), depth)),
                ),
            ),
            "progress_calls": count(int(not reading)),
            "child_progress_calls": count(int(reading)),
            "child_begin_calls": count(int(mode in {"begin", "apply"})),
            "child_prepare_calls": count(int(preparing)),
            "child_apply_calls": count(int(mode == "apply")),
            "scalar_encoded_octets": scale(32, width),
            "scalar_decoded_octets": scale(8, width),
        }
        operation = namespace[name]
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-journal-preparation-" + mode + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="direct fixed-stage cursor SQL and separately qualified child dispatch",
                starts_at="receive registered original source custody and retained stage controls",
                ends_at="prepare or atomically publish one child atom or one stage transition",
                dimensions=(
                    Dimension("retained_rows", COUNT, "Actual independent retained private rows."),
                    Dimension(
                        "metadata_octets", OCTETS, "Complete source, cursor and selected rows."
                    ),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        OCTETS if metric.endswith("octets") else COUNT,
                        "upper",
                        value,
                        "One current child and fixed-shape original source/stage controls.",
                    )
                    for metric, value in limits.items()
                ),
                conditions=(
                    "Caller serializes work under original metadata/kernel/worker custody.",
                    "Child progress, begin, prepare and apply bodies keep their own "
                    "qualified bounds.",
                    "Child atoms are passed by owned reference without payload "
                    "copying or decoding.",
                    "The maintained source/stage point keys exist without added triggers.",
                ),
                invariants=(
                    "One child publication or one transition runs per bounded step.",
                    "Current child and parent versions publish in the same transaction.",
                    "Initialized missing children fail explicitly; no stage is reset or recreated.",
                    "Rollback reparses only the current bounded atom and preserves confirmed work.",
                    "Completion grants no Native semantic acceptance.",
                ),
                limitations=(
                    "Direct dispatch costs exclude child bodies, complete composition and latency.",
                    "Native journal opening/replay and certificate reads are separate operations.",
                ),
            )
        )(operation)
