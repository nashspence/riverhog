"""Raw primary fixity copied with its admitted description assertion."""

from __future__ import annotations

import re
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
    def fixed(metric: str, unit: Unit, amount: int, reason: str) -> Bound:
        return Bound(metric, unit, "upper", Expression("constant", value=amount, unit=unit), reason)

    return Declaration(
        id="riverhog-provenance.sqlite-description-fixity.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._fixity_projection.read_fixity_metadata",),
        scope="native-sqlite-effective-description-primary-fixity",
        starts_at="receive an admitted description UUID in the pinned effective view",
        ends_at="return raw primary fixity or its absence after two closed indexed reads",
        dimensions=(Dimension("indexed_assertions", COUNT, "Active and retired assertions."),),
        bounds=(
            fixed("sql_statements", COUNT, 2, "Effective identity/type and fixed raw fixity."),
            fixed("returned_rows", COUNT, 2, "Both seeks identify at most one row."),
            fixed(
                "returned_metadata_octets",
                OCTETS,
                256,
                "Native UUID/type, 19-digit size, SHA-256 and SQLite type markers.",
            ),
            fixed("bound_parameter_octets", OCTETS, 90, "Two canonical UUID parameters."),
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                Expression("constant", value=0, unit=COUNT),
                "This operation cannot change staged facts or progress.",
            ),
            Bound(
                "historical_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No assertion, digest array or frame payload is selected.",
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
                "Two indexed seeks and fixed scalar projection expressions.",
            ),
        ),
        conditions=(
            "The owner admits native description shapes and canonical UUIDs before staging.",
            "The native content schema supplies a canonical 63-bit size and exactly one SHA-256.",
            "Staging copies raw fixity, assertion and its checkpoint atomically.",
            "The owning attempt authenticates the workspace and pins its effective view.",
        ),
        invariants=(
            "Raw fixity does not imply that its description has passed semantic validation.",
            "Absent content remains distinct from zero bytes with a SHA-256 digest.",
            "Missing, wrong-type and retired referents retain native identity diagnostics.",
            "No read publishes facts, source custody or archive authority.",
        ),
        limitations=(
            "The admitted source atom owns raw projection extraction and write accounting.",
            "Physical pages, B-tree comparisons, I/O and locks remain separate costs.",
        ),
    )


@declare_growth(_declaration())
def read_fixity_metadata(db: sqlite3.Connection, identity: str) -> dict[str, Any]:
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
    value = _Validator._node_type(
        {"id": identity, "type": row[1]}, "observation", "reported_description"
    )
    with closing(
        db.execute(
            "SELECT substr(CAST(size AS BLOB),1,20),typeof(size),"
            "substr(CAST(sha256 AS BLOB),1,65),typeof(sha256) "
            "FROM fixity_projection WHERE assertion_id=?",
            (row[0],),
        )
    ) as cursor:
        fields = cursor.fetchone()
    if fields is None:
        raise RuntimeError("missing staged description fixity")
    size, size_type, digest, digest_type = fields
    if size is None and digest is None and size_type == digest_type == "null":
        if value["type"] == "observation":
            raise RuntimeError("missing staged observation fixity")
        return value
    if (
        type(size) is not bytes
        or size_type != "text"
        or not 1 <= len(size) <= 19
        or type(digest) is not bytes
        or digest_type != "text"
        or len(digest) != 64
    ):
        raise RuntimeError("invalid staged description fixity")
    try:
        size_text, digest_text = size.decode("ascii"), digest.decode("ascii")
        if (
            re.fullmatch(r"0|[1-9][0-9]{0,18}", size_text) is None
            or int(size_text) > 9223372036854775807
            or re.fullmatch(r"[0-9a-f]{64}", digest_text) is None
        ):
            raise ValueError("native fixity shape")
    except (ValueError, UnicodeError) as error:
        raise RuntimeError("invalid staged description fixity") from error
    value["content"] = {
        "size_bytes": size_text,
        "digests": [{"algorithm": "sha-256", "value": digest_text}],
    }
    return value
