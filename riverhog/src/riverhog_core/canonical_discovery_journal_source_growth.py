"""Normalized source assembly and exact separately qualified child-call counts."""

from __future__ import annotations

from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    Unit,
    declare_growth,
)


def declare_growth_for_journal_source(owner: type[Any]) -> None:
    def number(value: int, unit: Unit = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scaled(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(number(value, ()), term))

    for mode in ("__init__", "verify_current", "read_window", "read_exact"):
        creating = mode == "__init__"
        reading = mode in {"read_window", "read_exact"}
        limits = {
            "sql_statements": number(0),
            "returned_rows": number(0),
            "returned_octets": number(0, OCTETS),
            "parameter_octets": number(0, OCTETS),
            "written_octets": number(0, OCTETS),
            "hashed_octets": number(0, OCTETS),
            "source_reads": number(0),
            "source_read_octets": number(0, OCTETS),
            "sqlite_vm_instructions": number(0),
            "metadata_read_calls": number(1),
            "framing_progress_calls": number(1),
            "framing_page_calls": dimension("page_windows") if reading else number(0),
            "original_window_calls": number(int(mode == "read_window")),
            "binding_constructions": number(int(creating)),
            "binding_regex_calls": number(5 if creating else 0),
            "binding_regex_octets": scaled(4, dimension("metadata_octets"))
            if creating
            else number(0, OCTETS),
            "binding_uuid_calls": number(int(creating)),
            "binding_uuid_octets": dimension("metadata_octets") if creating else number(0, OCTETS),
            "scalar_encoded_octets": scaled(3, dimension("metadata_octets"))
            if creating
            else number(0, OCTETS),
            "returned_source_octets": dimension("requested_octets")
            if reading
            else number(0, OCTETS),
            "copied_octets": scaled(2 if mode == "read_exact" else 1, dimension("requested_octets"))
            if reading
            else number(0, OCTETS),
            "retained_page_octets": number(0 if creating else 65536, OCTETS),
        }
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-journal-source-"
                + mode.strip("_").replace("_", "-")
                + ".growth/v1",
                authority="riverhog-server",
                subjects=(owner.__module__ + "." + owner.__qualname__ + "." + mode,),
                scope="direct normalized source assembly, binding validation and child-call counts",
                starts_at="receive original frozen framing, current owned metadata and exact range",
                ends_at="return exact normalized bytes or one short deferred original window",
                dimensions=(
                    Dimension(
                        "metadata_octets", OCTETS, "Complete original binding/control width."
                    ),
                    Dimension("requested_octets", OCTETS, "Original requested byte extent."),
                    Dimension(
                        "page_windows",
                        COUNT,
                        "Fixed pages intersecting the original admitted captured range.",
                    ),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        OCTETS if metric.endswith("octets") else COUNT,
                        "upper",
                        expression,
                        "Constant control/port work and the actual bounded range assembly.",
                    )
                    for metric, expression in limits.items()
                ),
                conditions=(
                    "The caller owns the exact child directory identity and serializes custody.",
                    "Framing is frozen complete/deferred; calls hold no Core write transaction.",
                    "Page windows come from original offset/length, not observed cache results.",
                    "Native exact reads retain the existing admitted frame byte domain.",
                ),
                invariants=(
                    "Every read checks actual current metadata and exact frozen source progress.",
                    "One normalized cache page never replaces current source or worker fences.",
                    "Exact reads never assemble arbitrary tiny original archive fragments.",
                    "Deferred windows retain ordered Native error and acceptance ownership.",
                    "No prefix, preceding source or unrelated retained directory is scanned.",
                ),
                limitations=(
                    "Metadata read and framing/page bodies retain their own qualified bounds.",
                    "Deferred original window bodies retain ArchiveJournalSource's bounds.",
                    "Logical counts exclude allocations, physical I/O, CPU and lock latency.",
                    "Currentness and caching do not audit all private-file contents.",
                    "Child session registration, custody and acceptance remain caller obligations.",
                ),
            )
        )(getattr(owner, mode))
