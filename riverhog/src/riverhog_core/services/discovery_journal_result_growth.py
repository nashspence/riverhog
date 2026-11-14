"""Bounded dispatch ports for sealed discovery custody and its short catalog fence."""

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

_LIMITS = {
    "prepare": {
        "archive_checks": 1,
        "catalog_transactions": 1,
        "build_queries": 1,
        "admission_preparations": 1,
    },
    "open": {
        "reference_retains": 2,
        "lease_contexts": 1,
        "source_constructions": 1,
        "native_opens": 1,
        "anchor_reads": 1,
    },
    "borrow": {"source_checks": 1, "native_borrows": 1},
    "close": {
        "native_closes": 1,
        "opening_closes": 1,
        "context_closes": 1,
        "reference_group_closes": 1,
    },
    "verify_current": {
        "archive_checks": 2,
        "custody_checks": 2,
        "catalog_transactions": 1,
        "build_queries": 1,
        "admission_probes": 1,
    },
    "sealed_journal": {"preparations": 1, "child_opens": 1, "child_closes": 1, "child_borrows": 1},
}


def _declare(operation: Any, mode: str) -> None:
    declare_growth(
        Declaration(
            id="riverhog-server.discovery-sealed-result-" + mode.replace("_", "-") + ".growth/v1",
            authority="riverhog-server",
            subjects=(operation.__module__ + "." + operation.__qualname__,),
            scope="direct selected-result dispatch and one indexed build-to-collection probe",
            starts_at="receive the full original archive source beneath current discovery custody",
            ends_at="return detached metadata or one Native reader; retain failed cleanup",
            dimensions=(
                Dimension("retained_journals", COUNT, "Independent retained original journals."),
            ),
            bounds=tuple(
                Bound(
                    metric,
                    COUNT,
                    "upper",
                    Expression("constant", value=limit, unit=COUNT),
                    "Fixed selected-source ports; no history or sibling traversal.",
                )
                for metric, limit in _LIMITS[mode].items()
            ),
            conditions=(
                "The application serializes work and closure beneath the selected slot mutex.",
                "Each Native, archive, catalog admission and kernel port keeps its own bounds.",
                "The build probe uses one primary key, one scalar row and a closed cursor.",
            ),
            invariants=(
                "Preserve the original sealed binding, certificate, checkpoint and anchor.",
                "Catalog transactions end before Native opening and after each currentness probe.",
                "No source payload is read and no worker is replaced or replayed.",
                "Retain Native storage before I/O; close data before releasing actual leases.",
                "Final Native borrowers keep actual child and parent kernel references.",
            ),
            limitations=(
                "Dispatch counts do not replace qualified child bodies or complete workflow costs.",
                "The indexed build probe processes the complete scalar key and selected row.",
                "Physical I/O, transaction waits and native stream internals are excluded.",
            ),
        )
    )(operation)


def declare_growth_for_sealed_result(owner: type[Any], source: type[Any]) -> None:
    _declare(owner.prepare.__func__, "prepare")
    for mode in ("open", "borrow", "close"):
        _declare(getattr(owner, mode), mode)
    _declare(source.verify_current, "verify_current")


def declare_growth_for_sealed_dispatch(work: type[Any]) -> None:
    _declare(work.sealed_journal, "sealed_journal")


def declare_growth_for_build_collection(operation: Any) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    width = Expression("dimension", name="control_octets")
    declare_growth(
        Declaration(
            id="riverhog-server.discovery-sealed-build-collection-current.growth/v1",
            authority="riverhog-server",
            subjects=(operation.__module__ + "." + operation.__qualname__,),
            scope="actual indexed build-to-collection catalog lookup",
            starts_at="receive the full selected build and exact proposed collection key",
            ends_at="return one scalar match or absence with the actual result cursor closed",
            dimensions=(
                Dimension("catalog_builds", COUNT, "All retained catalog build rows."),
                Dimension("control_octets", OCTETS, "Full incoming keys plus selected scalar row."),
            ),
            bounds=(
                Bound("sql_statements", COUNT, "equal", count(1), "One build primary-key query."),
                Bound("returned_rows", COUNT, "upper", count(1), "At most the selected build."),
                Bound("parameter_octets", OCTETS, "upper", width, "Full original keys."),
                Bound("returned_octets", OCTETS, "upper", width, "Full selected scalar identity."),
                Bound("open_cursors", COUNT, "equal", count(0), "The result context closes."),
                Bound(
                    "sqlite_vm_instructions",
                    COUNT,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            count(1024),
                            Expression(
                                "product",
                                terms=(
                                    count(256),
                                    Expression(
                                        "log2_ceil",
                                        terms=(
                                            Expression(
                                                "sum",
                                                terms=(
                                                    count(2),
                                                    Expression("dimension", name="catalog_builds"),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                    "Primary-key point lookup without a table scan or temporary sort.",
                ),
            ),
            conditions=(
                "The caller owns a short transaction with autoflush suppressed for this query.",
                "SQLite uses the maintained generation primary key.",
            ),
            invariants=("A source from another collection supplies no sealed-result authority.",),
            limitations=(
                "Caller transaction acquisition and current archive authority are separate.",
                "Physical I/O and machine latency are observations, not logical work bounds.",
            ),
        )
    )(operation)
