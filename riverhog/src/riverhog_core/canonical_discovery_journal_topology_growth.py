"""Direct private topology costs and separately qualified original-source/Native ports."""

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


def declare_growth_for_journal_topology(namespace: dict[str, Any]) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, expression: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), expression))

    def add(*values: Expression) -> Expression:
        return Expression("sum", terms=values)

    width = dimension("metadata_octets")
    frame = dimension("frame_octets")
    depth = Expression("log2_ceil", terms=(add(count(2), dimension("retained_rows")),))
    for mode, name in (
        ("progress", "discovery_journal_topology_progress"),
        ("page", "discovery_journal_topology_page"),
        ("begin", "begin_discovery_journal_topology"),
        ("prepare", "prepare_discovery_journal_topology"),
        ("apply", "apply_discovery_journal_topology"),
    ):
        reading, preparing, publishing = mode == "progress", mode == "prepare", mode == "apply"
        limits = {
            "sql_statements": count(
                2
                if reading
                else 64
                if mode == "page"
                else 65
                if publishing
                else int(mode == "begin")
            ),
            "returned_rows": count(2 if reading else 64 if mode == "page" else 0),
            "changed_rows": count(65 if publishing else int(mode == "begin")),
            "returned_octets": scale(2 if reading else 64, width)
            if reading or mode == "page"
            else octets(0),
            "parameter_octets": scale(256, width),
            "sqlite_vm_instructions": add(
                count(4096), Expression("product", terms=(count(4096), depth))
            ),
            "epoch_progress_calls": count(int(mode != "page")),
            "epoch_frame_calls": count(2 if reading else 64 if mode == "page" else int(preparing)),
            "progress_calls": count(int(not reading)),
            "checkpoint_decodes": count(int(reading)),
            "checkpoint_encodes": count(int(preparing or mode == "begin")),
            "source_exact_calls": count(int(preparing)),
            "source_exact_octets": frame if preparing else octets(0),
            "source_window_calls": add(dimension("frame_pages"), count(130))
            if preparing
            else count(0),
            "source_window_octets": add(frame, octets(64 * 65536)) if preparing else octets(0),
            "native_plan_calls": count(int(preparing)),
            "native_plan_octets": frame if preparing else octets(0),
            "native_advance_calls": count(int(preparing)),
            "published_facts": count(64 if publishing else 0),
            "scalar_encoded_octets": scale(256, width),
            "scalar_decoded_octets": scale(128 if mode == "page" else 32, width),
        }
        operation = namespace[name]
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-journal-topology-" + mode + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="Direct cursor/fact SQL, scalar controls and observed qualified ports",
                starts_at="receive current registered original epochs and owning metadata custody",
                ends_at="Return a bounded atom or publish its facts and next cursor together",
                dimensions=(
                    Dimension("retained_rows", COUNT, "Independent actual retained private rows."),
                    Dimension(
                        "metadata_octets",
                        OCTETS,
                        "Complete supplied keys, controls and facts.",
                    ),
                    Dimension("frame_octets", OCTETS, "Complete selected original JSON frame."),
                    Dimension(
                        "frame_pages",
                        COUNT,
                        "Frame pages at the fixed 65536-byte width.",
                    ),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        OCTETS if metric.endswith("octets") else COUNT,
                        "upper",
                        value,
                        "One frame, at most 64 items, point rows and complete controls.",
                    )
                    for metric, value in limits.items()
                ),
                conditions=(
                    "Caller serializes the source under metadata/kernel/worker custody.",
                    "Begin/apply use short transactions; preparation has no write transaction.",
                    "Source windows are fixed normalized pages; each read makes positive progress.",
                    "Native/source/epoch/checkpoint bodies keep their qualified bounds.",
                    "Native callbacks remain observed owner source ports.",
                    "SQLite uses maintained primary/item keys without added triggers.",
                ),
                invariants=(
                    "Facts and cursor publish atomically and grant no semantic acceptance.",
                    "Active original frames resume without decoding their preceding items.",
                    "Later speculative errors defer to ordered Native replay.",
                    "The original binding and canonical full counter domains are preserved.",
                ),
                limitations=(
                    "One admitted JSON entry and a final large item are bounded atoms.",
                    "Per-item page over-read and at most two alignment reads are included.",
                    "Logical VM/bytes exclude comparisons, allocation and machine latency.",
                ),
            )
        )(operation)
