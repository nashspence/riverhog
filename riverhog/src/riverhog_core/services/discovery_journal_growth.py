"""Fixed application dispatch ports for separately qualified child owners."""

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

LIMITS = {
    "journal_source": {
        "metadata_reads": 1,
        "metadata_transactions": 1,
        "registration_finds": 1,
        "source_constructions": 1,
        "registration_writes": 1,
        "uuid_values": 1,
    },
    "journal": {
        "source_preparations": 1,
        "metadata_reads": 1,
        "metadata_transactions": 1,
        "registration_finds": 1,
        "registration_confirms": 1,
        "child_constructions": 1,
        "child_opens": 1,
        "child_closes": 1,
        "native_progress_calls": 1,
        "source_checks": 1,
        "lost_catalog_calls": 1,
    },
    "open": {
        "reference_retains": 2,
        "lease_contexts": 1,
        "native_opens": 1,
        "path_probes": 1,
    },
    "close": {
        "native_closes": 1,
        "context_closes": 1,
        "reference_group_closes": 1,
    },
}


def _declare(operation: Any, mode: str, owner: str) -> None:
    declare_growth(
        Declaration(
            id="riverhog-server.discovery-journal-" + owner + "-" + mode + ".growth/v1",
            authority="riverhog-server",
            subjects=(operation.__module__ + "." + operation.__qualname__,),
            scope="direct application dispatch ports; child bodies retain their own bounds",
            starts_at="receive one exact original source beneath the active discovery slot",
            ends_at="return warm custody or keep unconfirmed storage and release resources",
            dimensions=(
                Dimension("retained_journals", COUNT, "Independent registered original sources."),
            ),
            bounds=tuple(
                Bound(
                    metric,
                    COUNT,
                    "upper",
                    Expression("constant", value=limit, unit=COUNT),
                    "One selected atom and fixed dispatch; no per-source or sibling loop.",
                )
                for metric, limit in LIMITS[mode].items()
            ),
            conditions=(
                "The application serializes all operations beneath the selected slot mutex.",
                "Every child port body retains its separately declared and measured costs.",
                "Native owns all validation checkpoints and frozen reader lifecycle.",
            ),
            invariants=(
                "Persist the exact child directory before Native or certificate construction.",
                "Only one selected unfinished atom is retained, without an all-source cache.",
                "Confirmed missing storage is lost, never recreated or reset.",
                "Actual partial handles and failed releases remain reachable for retry.",
                "Final Native borrowers retain both original kernel lock descriptions.",
            ),
            limitations=(
                "These are dispatch counts, not a complete composed algorithm cost.",
                "Core registration, metadata, source and Native work require their own evidence.",
                "Physical I/O, locking latency and native stream internals are excluded.",
            ),
        )
    )(operation)


def declare_growth_for_journal_owner(owner: type[Any]) -> None:
    _declare(owner.open, "open", "child")
    _declare(owner.close, "close", "child")


def declare_growth_for_journal_dispatch(work: type[Any]) -> None:
    _declare(work.journal_source, "journal_source", "dispatch")
    _declare(work.journal, "journal", "dispatch")


def declare_growth_for_journal_custody_source(owner: type[Any]) -> None:
    for mode in ("verify_current", "read_window", "read_exact"):
        reading = mode != "verify_current"
        limits = {
            "custody_checks": 2 if reading else 1,
            "source_checks": int(not reading),
            "source_windows": int(mode == "read_window"),
            "source_exact_reads": int(mode == "read_exact"),
        }
        operation = getattr(owner, mode)
        declare_growth(
            Declaration(
                id="riverhog-server.discovery-journal-custody-" + mode + ".growth/v1",
                authority="riverhog-server",
                subjects=(operation.__module__ + "." + operation.__qualname__,),
                scope="direct actual child-custody and original normalized source ports",
                starts_at="receive the original binding and actual retained child custody",
                ends_at="return currentness or unchanged original bytes after both fences",
                dimensions=(
                    Dimension("requested_octets", OCTETS, "Complete requested source range."),
                    Dimension("retained_journals", COUNT, "Independent registered source count."),
                ),
                bounds=(
                    *(
                        Bound(
                            metric,
                            COUNT,
                            "upper",
                            Expression("constant", value=limit, unit=COUNT),
                            "Fixed direct child checks and one original source call.",
                        )
                        for metric, limit in limits.items()
                    ),
                    Bound(
                        "returned_octets",
                        OCTETS,
                        "upper",
                        Expression("dimension", name="requested_octets")
                        if reading
                        else Expression("constant", value=0, unit=OCTETS),
                        "Return the actual source bytes without copying or truncating.",
                    ),
                ),
                conditions=(
                    "The application serializes reads and closure under the child slot mutex.",
                    "Each child currentness and source body retains its own qualified bounds.",
                ),
                invariants=(
                    "The bound original source identity is unchanged.",
                    "Actual child custody precedes source access and follows a successful read.",
                    "A stale workspace grants no cached bytes or Native acceptance.",
                ),
                limitations=(
                    "Direct dispatch counts do not replace the child operation declarations.",
                    "Native algorithms and independent frozen readers keep their own contracts.",
                ),
            )
        )(operation)
