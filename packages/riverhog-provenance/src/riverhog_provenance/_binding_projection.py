"""Fixed raw temporal points copied with staged locator-binding assertions."""

from __future__ import annotations

import sqlite3
from contextlib import closing

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
    def fixed(name: str, unit: Unit, value: int, reason: str) -> Bound:
        return Bound(name, unit, "upper", Expression("constant", value=value, unit=unit), reason)

    return Declaration(
        id="riverhog-provenance.sqlite-binding-time.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._binding_projection.read_binding_time",),
        scope="native-sqlite-effective-locator-binding-time-metadata",
        starts_at="receive one admitted locator-binding identity in its pinned effective view",
        ends_at="return its raw at/start point after at most two closed indexed reads",
        dimensions=(
            Dimension("indexed_assertions", COUNT, "Resident active and retired assertions."),
        ),
        bounds=(
            fixed(
                "sql_statements", COUNT, 2, "One effective identity/type and one projection read."
            ),
            fixed("returned_rows", COUNT, 2, "Both reads use unique indexed identities."),
            fixed(
                "returned_metadata_octets",
                OCTETS,
                128,
                "One UUID/type, a bounded timestamp prefix and its SQLite type marker.",
            ),
            fixed("bound_parameter_octets", OCTETS, 90, "Two canonical 45-octet UUID URNs."),
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                Expression("constant", value=0, unit=COUNT),
                "This unit only reads its pinned effective view.",
            ),
            Bound(
                "historical_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No retained assertion or frame payload is selected or decoded.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=128, unit=COUNT),
                        Expression(
                            "log2_ceil",
                            terms=(
                                Expression(
                                    "sum",
                                    terms=(
                                        Expression("dimension", name="indexed_assertions"),
                                        Expression("constant", value=2, unit=COUNT),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Two native indexed seeks and fixed-width projection expressions.",
            ),
        ),
        conditions=(
            "The owner admits canonical UUIDs and native ASCII timestamp shapes before staging.",
            "Staging commits raw projection fields, assertion and progress atomically.",
            "The effective-object and semantic-reference covering indexes are retained.",
            "The owning attempt authenticates the workspace and pins the effective view.",
        ),
        invariants=(
            "Missing and wrong-type effective referents retain native diagnostic order.",
            "Retired assertions never supply the selected binding's time.",
            "Calendar validation remains in native binding-end checks after duplicate detection.",
            "No read changes facts, accepted prefix, source custody or archive authority.",
        ),
        limitations=(
            "Staging owns projection construction and its write/byte accounting.",
            "Physical SQLite page work, I/O and lock waits are separate indexed costs.",
            "The owning session checkpoints completion; an unfinished fixed unit may replay.",
        ),
    )


@declare_growth(_declaration())
def read_binding_time(db: sqlite3.Connection, identity: str) -> str | None:
    """Resolve the raw temporal point without reading a binding's locator payload."""
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
    _Validator._node_type({"id": identity, "type": row[1]}, "locator_binding")
    with closing(
        db.execute(
            "SELECT substr(CAST(bound_at AS BLOB),1,31),typeof(bound_at) "
            "FROM binding_projection WHERE assertion_id=?",
            (row[0],),
        )
    ) as cursor:
        fields = cursor.fetchone()
    if fields is None:
        raise RuntimeError("missing staged locator-binding metadata")
    raw, kind = fields
    if raw is None and kind == "null":
        return None
    if type(raw) is not bytes or kind != "text" or len(raw) > 30:
        raise RuntimeError("invalid staged locator-binding time")
    try:
        return raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise RuntimeError("invalid staged locator-binding time") from exc
