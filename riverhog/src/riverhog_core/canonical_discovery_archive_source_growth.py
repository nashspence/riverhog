"""Expanded custody/directory costs and authenticated archive payload bounds."""

from __future__ import annotations

from dataclasses import replace
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
    growth_binding,
)

from riverhog_core.canonical_discovery_archive_work import (
    archive_journal_header,
    archive_journal_segment_at,
)
from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata


def declare_archive_source_growth(owner: type[Any]) -> None:
    def number(value: int, unit: Unit = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def product(left: Expression, right: Expression) -> Expression:
        return Expression("product", terms=(left, right))

    def scaled(value: int, term: Expression) -> Expression:
        return product(Expression("constant", value=value), term)

    def summed(*terms: Expression) -> Expression:
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    def renamed(term: Expression) -> Expression:
        names = {"registered_inputs": "retained_core_rows", "control_octets": "metadata_octets"}
        return replace(
            term,
            name=names.get(term.name, term.name),
            terms=tuple(renamed(child) for child in term.terms),
        )

    storage = growth_binding(DiscoveryMetadata.read).declaration
    header = growth_binding(archive_journal_header).declaration
    seek = growth_binding(archive_journal_segment_at).declaration
    metric_names = {
        "sql_statements": "core_sql_statements",
        "returned_rows": "core_returned_rows",
        "returned_octets": "core_returned_octets",
        "parameter_octets": "core_parameter_octets",
        "sqlite_vm_instructions": "core_vm_instructions",
        "source_payload_octets": "source_read_octets",
    }
    for mode in ("__init__", "verify_current", "read_window", "read_exact"):
        reading = mode in ("read_window", "read_exact")
        window_count = (
            dimension("segment_windows")
            if mode == "read_exact"
            else number(1 if mode == "read_window" else 0)
        )
        windows = product(window_count, Expression("constant", value=1, unit=(("count", -1),)))
        storage_calls = number(1, ())
        header_calls = number(2 if reading else 1, ())
        seek_calls = scaled(2, windows)
        body = (
            (
                summed(dimension("requested_octets"), scaled(2, dimension("payload_octets")))
                if mode == "read_exact"
                else dimension("payload_octets")
            )
            if reading
            else number(0, OCTETS)
        )
        control = product(windows, dimension("metadata_octets"))
        extra = {
            "source_reads": (COUNT, scaled(2, window_count)),
            "source_read_octets": (OCTETS, summed(body, control)),
            "hashed_octets": (OCTETS, summed(body, scaled(8, control))),
            "encoded_octets": (OCTETS, scaled(16, control)),
            "decoded_octets": (OCTETS, scaled(8, control)),
            "returned_source_octets": (
                OCTETS,
                dimension("requested_octets") if reading else number(0, OCTETS),
            ),
            "retained_source_octets": (
                OCTETS,
                number(0, OCTETS) if mode == "__init__" else dimension("payload_octets"),
            ),
        }
        totals: dict[str, tuple[tuple[tuple[str, int], ...], list[Expression]]] = {}
        for child, calls in ((storage, storage_calls), (header, header_calls), (seek, seek_calls)):
            for bound in child.bounds:
                metric = metric_names.get(bound.metric, bound.metric)
                if metric not in totals:
                    totals[metric] = bound.unit, []
                totals[metric][1].append(product(calls, renamed(bound.expression)))
        for metric, (unit, expression) in extra.items():
            if metric not in totals:
                totals[metric] = unit, []
            totals[metric][1].append(expression)
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-archive-source-"
                + mode.strip("_").replace("_", "-")
                + ".growth/v1",
                authority="riverhog-server",
                subjects=(owner.__module__ + "." + owner.__qualname__ + "." + mode,),
                scope="current custody, original range directory and authenticated raw reads",
                starts_at="receive owned metadata and one exact original archive journal range",
                ends_at="close all cursors and return exact bytes with at most one cached segment",
                dimensions=(
                    Dimension("retained_core_rows", COUNT, "All independently retained Core rows."),
                    Dimension("retained_catalog_rows", COUNT, "All retained catalog/worker rows."),
                    Dimension(
                        "metadata_octets", OCTETS, "Largest complete original control/row/root."
                    ),
                    Dimension("path_octets", OCTETS, "Complete selected filesystem path width."),
                    Dimension("requested_octets", OCTETS, "Original requested range length."),
                    Dimension("payload_octets", OCTETS, "Largest original source segment payload."),
                    Dimension(
                        "segment_windows",
                        COUNT,
                        "Original nonoverlapping segments intersecting the exact requested range.",
                    ),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        unit,
                        "upper",
                        summed(*terms),
                        "Expanded custody and indexed directory work plus complete raw bytes.",
                    )
                    for metric, (unit, terms) in totals.items()
                ),
                conditions=(
                    "The caller serializes work under current archive/worker and kernel custody.",
                    "The original complete directory authenticates nonempty contiguous segments.",
                    "Cold reads verify the entire selected root and payload length/digest.",
                    "Windows stay within one original source; exact reads admit a frame.",
                    "Exact reads visit each segment once; only boundary payloads may overlap.",
                    "The maintained indexes remain present and no triggers are added.",
                ),
                invariants=(
                    "Cache hits retain live currentness and original descriptor fences.",
                    "No whole metadata sequence or preceding journal is scanned for a range.",
                    "Exact raw payload bytes are linear in the range plus two boundary segments.",
                    "The cached payload remains bounded by one existing archive segment extent.",
                    "Raw range authentication grants no Native semantic or publication terminal.",
                ),
                limitations=(
                    "Logical bytes exclude Python overhead, physical allocations and I/O latency.",
                    "Native framing, validation, certificates and session lifetimes keep owners.",
                    "Cold/evicted paths and scoped elapsed time retain independent observations.",
                ),
            )
        )(getattr(owner, mode))
