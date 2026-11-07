"""Indexed child admission and separately qualified frozen-framing port calls."""

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


def declare_growth_for_journal_registration(namespace: dict[str, Any]) -> None:
    def number(value: int, unit: Unit = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(number(value, ()), term))

    for mode in ("find", "register", "confirm"):
        operation = namespace[mode + "_discovery_journal"]
        width = dimension("metadata_octets")
        logarithm = Expression(
            "log2_ceil",
            terms=(Expression("sum", terms=(dimension("retained_registrations"), number(2))),),
        )
        limits = {
            "sql_statements": number(1 if mode == "find" else 2),
            "returned_rows": number(1),
            "changed_rows": number(int(mode != "find")),
            "returned_octets": width,
            "parameter_octets": scale(12, width),
            "written_octets": number(0, OCTETS) if mode == "find" else scale(4, width),
            "sqlite_vm_instructions": Expression(
                "sum", terms=(number(256), Expression("product", terms=(number(64), logarithm)))
            ),
            "framing_progress_calls": number(2 if mode == "register" else 1),
            "json_decodes": number(1),
            "json_encodes": number(1 if mode == "find" else 2),
            "json_decoded_octets": width,
            "json_encoded_octets": scale(2, width),
            "binding_regex_calls": number(7 if mode == "register" else 6),
            "binding_regex_octets": scale(6, width),
            "binding_uuid_calls": number(1),
            "binding_uuid_octets": width,
            "scalar_encoded_octets": scale(6 if mode == "register" else 3, width),
            "hashed_octets": number(0, OCTETS),
            "source_reads": number(0),
            "source_read_octets": number(0, OCTETS),
        }
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-journal-registration-" + mode + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="direct indexed registration SQL, canonical binding codecs and child calls",
                starts_at="receive the actual current frozen source, exact owned key and controls",
                ends_at="return exact admission/confirmation or reject without replacing a child",
                dimensions=(
                    Dimension("retained_registrations", COUNT, "Independent total child rows."),
                    Dimension(
                        "metadata_octets",
                        OCTETS,
                        "Complete incoming/retained row, key and canonical binding control widths.",
                    ),
                ),
                bounds=tuple(
                    Bound(
                        metric,
                        OCTETS if metric.endswith("octets") else COUNT,
                        "upper",
                        value,
                        "One exact indexed row and fixed complete binding work.",
                    )
                    for metric, value in limits.items()
                ),
                conditions=(
                    "Actual metadata and worker/kernel custody fence the caller's quantum.",
                    "Registration and confirmation run within the caller's transaction.",
                    "The maintained primary/unique indexes have no additional triggers.",
                ),
                invariants=(
                    "Directory identity and original binding persist before Native opening.",
                    "Neither retries nor lost confirmation replace a registered child.",
                    "Confirmed opening grants no journal or corpus semantic acceptance.",
                    "Unrelated children and documentary payload are never scanned.",
                ),
                limitations=(
                    "Real framing progress bodies retain their separately qualified bounds.",
                    "Native opening, checkpointing, acceptance and kernel custody are caller work.",
                    "Logical SQL/codec counts exclude physical I/O, allocation and CPU latency.",
                ),
            )
        )(operation)
