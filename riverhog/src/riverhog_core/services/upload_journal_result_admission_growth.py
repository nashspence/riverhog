"""Bounded catalog-only admission and exact sealed-source currentness."""

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


def declare_growth_for_sealed_admission(prepare: Any, admission: type[Any]) -> None:
    for operation, preparing in ((prepare, True), (admission.verify_current, False)):

        def number(value: int, unit: tuple[tuple[str, int], ...] = COUNT) -> Expression:
            return Expression("constant", value=value, unit=unit)

        def width(multiplier: int) -> Expression:
            return Expression(
                "product",
                terms=(number(multiplier, ()), Expression("dimension", name="control_octets")),
            )

        depths = tuple(
            Expression(
                "log2_ceil",
                terms=(Expression("sum", terms=(number(2), Expression("dimension", name=name))),),
            )
            for name in ("catalog_journals", "catalog_jobs")
        )
        limits = {
            "sql_statements": number(2 if preparing else 1),
            "returned_rows": number(2 if preparing else 1),
            "returned_octets": width(1),
            "parameter_octets": width(2),
            "json_encodes": number(12 if preparing else 0),
            "json_decodes": number(8 if preparing else 0),
            "json_encoded_octets": width(12) if preparing else number(0, OCTETS),
            "json_decoded_octets": width(8) if preparing else number(0, OCTETS),
            "hashed_octets": number(0, OCTETS),
            "native_storage_calls": number(0),
            "source_payload_octets": number(0, OCTETS),
            "open_cursors": number(0),
            "detached_controls": number(int(preparing)),
            "sqlite_vm_instructions": Expression(
                "sum",
                terms=(
                    number(1024),
                    Expression(
                        "product",
                        terms=(number(256), Expression("sum", terms=depths)),
                    ),
                ),
            ),
        }
        declare_growth(
            Declaration(
                id="riverhog.sealed-journal-result-"
                + ("metadata-admission" if preparing else "admission-current")
                + ".growth/v1",
                authority="riverhog",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="catalog-only exact sealed metadata and immutable Native control validation",
                starts_at=(
                    "receive the caller's short catalog transaction and full original controls"
                ),
                ends_at="close all query cursors and return detached metadata or currentness",
                dimensions=(
                    Dimension("catalog_journals", COUNT, "All independent retained journal rows."),
                    Dimension("catalog_jobs", COUNT, "All independent retained Native job rows."),
                    Dimension(
                        "control_octets",
                        OCTETS,
                        "Complete incoming keys plus selected original source and job row values.",
                    ),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        OCTETS if metric.endswith("octets") else COUNT,
                        "upper",
                        expression,
                        "Fixed indexed control reads and a fixed number of complete header codecs.",
                    )
                    for metric, expression in limits.items()
                ),
                conditions=(
                    "The current sealed catalog supplies its already cached checkpoint identity.",
                    "Selected metadata widths include full canonical JSON and decimal quantities.",
                    "The pinned SQLite backend uses the maintained composite and unique indexes.",
                    "The caller owns the brief catalog transaction and its row-lock policy.",
                ),
                invariants=(
                    "The original sealed Native binding and complete checkpoint remain unchanged.",
                    "No Native file, workspace lease, replay or source payload is accessed.",
                    "The admission retains no session, cursor, connection or database row lock.",
                    "Currentness compares the exact selected source and Native job atomically.",
                    "Missing or mismatched confirmation is rejected without repairing stored data.",
                ),
                limitations=(
                    "Driver locking waits and internal comparisons/page I/O are excluded.",
                    (
                        "Native workspace custody, result opening and final borrower closure "
                        "are separate."
                    ),
                    (
                        "Logical header codecs exclude internal key comparisons "
                        "and allocation latency."
                    ),
                ),
            )
        )(operation)
