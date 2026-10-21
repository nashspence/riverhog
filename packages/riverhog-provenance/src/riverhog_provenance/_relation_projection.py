"""Fixed usage and generation correlates copied with their staged assertions."""

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
        id="riverhog-provenance.sqlite-relation-metadata.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._relation_projection.read_relation_metadata",),
        scope="native-sqlite-effective-relation-reference-metadata",
        starts_at="receive an admitted usage or generation identity in its pinned view",
        ends_at="return its fixed row metadata after at most two closed indexed reads",
        dimensions=(
            Dimension("indexed_assertions", COUNT, "Resident active and retired assertions."),
        ),
        bounds=(
            fixed(
                "sql_statements",
                COUNT,
                2,
                "One effective identity/type and one projection read.",
            ),
            fixed("returned_rows", COUNT, 2, "Both reads use unique indexed identities."),
            fixed(
                "returned_metadata_octets",
                OCTETS,
                256,
                "An assertion UUID/type plus two canonical UUIDs and SQLite type markers.",
            ),
            fixed(
                "bound_parameter_octets",
                OCTETS,
                90,
                "Two canonical 45-octet UUID URNs.",
            ),
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
            "The owner admits canonical UUIDs and native usage/generation records before staging.",
            "Staging commits raw projection fields, assertion and progress atomically.",
            "The effective-object and semantic-reference covering indexes are retained.",
            "The owning attempt authenticates the workspace and pins the effective view.",
        ),
        invariants=(
            "Missing and wrong-type effective referents retain native diagnostic order.",
            "Retired assertions never supply the selected relation referent.",
            "Correlation checks retain native derivation row order after identity/type checks.",
            "No read changes facts, accepted prefix, source custody or archive authority.",
        ),
        limitations=(
            "Staging owns projection construction and its write/byte accounting.",
            "Physical SQLite page work, I/O and lock waits are separate indexed costs.",
            "The owning session checkpoints completion; an unfinished fixed unit may replay.",
        ),
    )


@declare_growth(_declaration())
def read_relation_metadata(
    db: sqlite3.Connection, identity: str, expected_type: str
) -> tuple[str, str]:
    """Read a usage/generation activity and state without documentary payloads."""
    from riverhog_provenance_contracts import require_canonical_uuid_urn

    from .graph import _Validator

    if expected_type not in {"usage", "generation"}:
        raise ValueError("relation metadata requires usage or generation")
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
    _Validator._node_type({"id": identity, "type": row[1]}, expected_type)
    with closing(
        db.execute(
            "SELECT substr(CAST(activity_id AS BLOB),1,46),typeof(activity_id),"
            "substr(CAST(state_id AS BLOB),1,46),typeof(state_id) "
            "FROM relation_projection WHERE assertion_id=?",
            (row[0],),
        )
    ) as cursor:
        fields = cursor.fetchone()
    if fields is None:
        raise RuntimeError("missing staged relation metadata")
    values = []
    for raw, kind in ((fields[0], fields[1]), (fields[2], fields[3])):
        if type(raw) is not bytes or kind != "text" or len(raw) != 45:
            raise RuntimeError("invalid staged relation metadata")
        try:
            value = raw.decode("ascii")
            require_canonical_uuid_urn(value)
        except (ValueError, UnicodeDecodeError) as error:
            raise RuntimeError("invalid staged relation identity") from error
        values.append(value)
    return values[0], values[1]
