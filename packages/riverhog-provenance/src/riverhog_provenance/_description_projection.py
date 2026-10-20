"""Fixed reference fields copied atomically with admitted assertion rows."""

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
        id="riverhog-provenance.sqlite-description-metadata.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._description_projection.read_description_metadata",),
        scope="native-sqlite-effective-description-reference-metadata",
        starts_at="receive an admitted object identity and native expected types "
        "in its pinned view",
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
                "One assertion UUID/type, three bounded scalar prefixes and their type markers.",
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
                "Two native indexed seeks and fixed-width description projection expressions.",
            ),
        ),
        conditions=(
            "The owner admits canonical UUIDs and native ASCII object kinds before staging.",
            "Staging commits raw projection fields, assertion and progress atomically.",
            "The effective-object and semantic-reference covering indexes are retained.",
            "The owning attempt authenticates the workspace and pins the effective view.",
        ),
        invariants=(
            "Missing and wrong-type effective referents retain native diagnostic order.",
            "Retired assertions never supply the selected delivery referent.",
            "The caller retains native context and filesystem-profile diagnostic order.",
            "No read changes facts, accepted prefix, source custody or archive authority.",
        ),
        limitations=(
            "Staging owns projection construction and its write/byte accounting.",
            "Physical SQLite page work, I/O and lock waits are separate indexed costs.",
            "The owning session checkpoints completion; an unfinished fixed unit may replay.",
        ),
    )


@declare_growth(_declaration())
def read_description_metadata(db: sqlite3.Connection, identity: str, *types: str) -> dict[str, str]:
    """Resolve native object fields without selecting retained documentary payloads."""
    from riverhog_provenance_contracts import require_canonical_uuid_urn

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
    value = {"id": identity, "type": row[1]}
    _Validator._node_type(value, *types)
    with closing(
        db.execute(
            "SELECT substr(CAST(occurrence_id AS BLOB),1,46),typeof(occurrence_id),"
            "substr(CAST(kind AS BLOB),1,46),typeof(kind),"
            "substr(CAST(source_context_id AS BLOB),1,46),typeof(source_context_id) "
            "FROM description_projection WHERE assertion_id=?",
            (row[0],),
        )
    ) as cursor:
        fields = cursor.fetchone()
    if fields is None:
        raise RuntimeError("missing staged description reference metadata")
    for index, key in enumerate(("occurrence_id", "kind", "source_context_id")):
        raw, kind = fields[index * 2 : index * 2 + 2]
        if raw is None and kind == "null":
            continue
        if type(raw) is not bytes or kind != "text" or not 1 <= len(raw) <= 45:
            raise RuntimeError("invalid staged description reference metadata")
        try:
            decoded = raw.decode("ascii")
            if key != "kind":
                require_canonical_uuid_urn(decoded)
        except (UnicodeDecodeError, ValueError) as exc:
            raise RuntimeError("invalid staged description reference metadata") from exc
        value[key] = decoded
    return value
