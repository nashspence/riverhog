"""Fixed activity metadata from owner-admitted assertion staging.

The projection carries no semantic acceptance or archive authority. Staging
copies raw fields with its assertion and cursor; native row checks retain their
original reference, generation-kind, uniqueness and timestamp error order.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
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

from .errors import ProvenanceValidationError


def _declaration() -> Declaration:
    def constant(value: int, unit: Unit) -> Expression:
        return Expression("constant", value=value, unit=unit)

    return Declaration(
        id="riverhog-provenance.sqlite-lifecycle-activity.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._activity_projection.read_activity",),
        scope="native-sqlite-effective-lifecycle-activity-metadata",
        starts_at="receive one admitted lifecycle activity identity in its pinned effective view",
        ends_at="return activity kind and optional timestamps after at most two closed point reads",
        dimensions=(
            Dimension("indexed_assertions", COUNT, "Resident active and retired assertions."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                constant(2, COUNT),
                "One effective identity/type read and one assertion-keyed metadata read.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                constant(2, COUNT),
                "The effective-object uniqueness and projection primary key each admit one row.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                constant(192, OCTETS),
                "One UUID/type and a native activity kind plus two UTC nanosecond timestamps.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                constant(90, OCTETS),
                "Two canonical 45-octet UUID URNs.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                constant(0, COUNT),
                "This unit only reads its pinned effective view.",
            ),
            Bound(
                "historical_payload_octets",
                OCTETS,
                "equal",
                constant(0, OCTETS),
                "No retained assertion or frame BLOB is selected or decoded.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "product",
                    terms=(
                        constant(128, COUNT),
                        Expression(
                            "log2_ceil",
                            terms=(
                                Expression(
                                    "sum",
                                    terms=(
                                        Expression("dimension", name="indexed_assertions"),
                                        constant(2, COUNT),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Native indexed seek envelope; no scan or payload-dependent JSON delegate.",
            ),
        ),
        conditions=(
            "The owner has schema-admitted native UUID, activity-kind and timestamp shapes.",
            "Staging commits raw projection fields, assertions and its progress atomically.",
            "The effective-object and semantic-reference covering indexes are retained.",
            "The owning attempt authenticates the workspace and pins the effective view.",
        ),
        invariants=(
            "Missing and wrong-type effective referents retain native diagnostic order.",
            "Retired assertions never supply the selected activity metadata.",
            "Raw timestamp values are copied without early calendar or activity validation.",
            "No read changes facts, accepted prefix, source custody or archive authority.",
        ),
        limitations=(
            "Staging owns projection construction and its write/byte accounting.",
            "Physical SQLite page work, I/O and lock waits are separate indexed costs.",
            "The owning session checkpoints completion; an unfinished fixed unit may replay.",
            "Lifecycle uniqueness, event times and finish checks remain separate operations.",
        ),
    )


@declare_growth(_declaration())
def read_activity(db: sqlite3.Connection, identity: str) -> dict[str, Any]:
    """Resolve only the raw fields consumed by native lifecycle row checks."""
    from .graph import _Validator

    with closing(
        db.execute(
            "SELECT assertion_id,type FROM assertions INDEXED BY semantic_reference_type "
            "WHERE object_id=? AND active=1",
            (identity,),
        )
    ) as cursor:
        row = cursor.fetchone()
    if row is None:
        raise ProvenanceValidationError(f"unresolved local identity: {identity}")
    result = _Validator._node_type({"id": identity, "type": row[1]}, "activity")
    with closing(
        db.execute(
            "SELECT kind,started_at,ended_at FROM activity_projection WHERE assertion_id=?",
            (row[0],),
        )
    ) as cursor:
        fields = cursor.fetchone()
    if fields is None:
        raise RuntimeError("missing staged activity metadata")
    result["kind"] = fields[0]
    for key, value in zip(("started_at", "ended_at"), fields[1:], strict=True):
        if value is not None:
            result[key] = value
    return result
