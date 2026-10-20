"""Raw capture and state metadata, with staged observer membership.

These projections precede semantic acceptance. Effective assertion identity pins
all reads, including forward references to activities not yet visited by replay.
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
    def fixed(metric: str, unit: Unit, value: int, reason: str) -> Bound:
        return Bound(metric, unit, "upper", Expression("constant", value=value, unit=unit), reason)

    return Declaration(
        id="riverhog-provenance.sqlite-observation-metadata.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._observation_projection._read",),
        scope="native-sqlite-effective-state-capture-and-observer-metadata",
        starts_at="receive admitted native object and optional agent UUIDs in the pinned view",
        ends_at="return fixed metadata or one membership boolean after closed indexed reads",
        dimensions=(Dimension("indexed_rows", COUNT, "Resident assertions and observer rows."),),
        bounds=(
            fixed(
                "sql_statements", COUNT, 3, "Effective identity, fixed fields and optional member."
            ),
            fixed("returned_rows", COUNT, 3, "Every read identifies at most one indexed record."),
            fixed("returned_metadata_octets", OCTETS, 384, "UUID/type and fixed scalar prefixes."),
            fixed("bound_parameter_octets", OCTETS, 180, "At most four canonical UUID parameters."),
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                Expression("constant", value=0, unit=COUNT),
                "Queries cannot publish or change staged facts.",
            ),
            Bound(
                "historical_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No retained assertion, association array or source BLOB is selected.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=256, unit=COUNT),
                        Expression(
                            "log2_ceil",
                            terms=(
                                Expression(
                                    "sum",
                                    terms=(
                                        Expression("dimension", name="indexed_rows"),
                                        Expression("constant", value=2, unit=COUNT),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Unique indexed seeks and fixed-width projection expressions.",
            ),
        ),
        conditions=(
            "The owner has admitted native state/activity shapes and canonical UUIDs.",
            "Raw fields and observer membership are staged with fenced atomic progress.",
            "An activity's completion marker follows all its source association units.",
            "The owner authenticates the private workspace and pins its effective view.",
        ),
        invariants=(
            "Missing and wrong-type effective referents retain native diagnostic order.",
            "Retired activities never provide capture fields or agent membership.",
            "Projection membership does not imply that any activity has passed semantics.",
        ),
        limitations=(
            "Projection writes, source decoding and current-atom admission have separate owners.",
            "Physical pages, B-tree comparisons, I/O, locks and retry waits remain separate costs.",
        ),
    )


@declare_growth(_declaration())
def _read(
    db: sqlite3.Connection, identity: str, expected: str, agent_id: str | None = None
) -> tuple[dict[str, Any], bool | None]:
    from .graph import _Validator

    with closing(
        db.execute(
            "SELECT assertion_id,type FROM assertions INDEXED BY semantic_reference_type "
            "WHERE object_id=? AND active=1",
            (identity,),
        )
    ) as cursor:
        selected = cursor.fetchone()
    if selected is None:
        raise ProvenanceValidationError(f"unresolved local identity: {identity}")
    value = _Validator._node_type({"id": identity, "type": selected[1]}, expected)
    with closing(
        db.execute(
            "SELECT substr(CAST(p.extent_kind AS BLOB),1,46),typeof(p.extent_kind),"
            "substr(CAST(p.outcome AS BLOB),1,46),typeof(p.outcome),p.observers_complete,"
            "substr(CAST(d.kind AS BLOB),1,46),typeof(d.kind) "
            "FROM observation_projection p JOIN description_projection d "
            "ON d.assertion_id=p.assertion_id WHERE p.assertion_id=?",
            (selected[0],),
        )
    ) as cursor:
        fields = cursor.fetchone()
    if fields is None:
        raise RuntimeError("missing staged observation metadata")
    if type(fields[4]) is not int or fields[4] != 1:
        raise RuntimeError("incomplete staged observation metadata")
    for key, offset in (("extent_kind", 0), ("outcome", 2), ("kind", 5)):
        raw, storage = fields[offset : offset + 2]
        if raw is None and storage == "null":
            continue
        if type(raw) is not bytes or storage != "text" or not 1 <= len(raw) <= 45:
            raise RuntimeError("invalid staged observation metadata")
        try:
            value[key] = raw.decode("ascii")
        except UnicodeDecodeError as exc:
            raise RuntimeError("invalid staged observation metadata") from exc
    if expected == "state":
        if "extent_kind" not in value:
            raise RuntimeError("missing staged observation state extent")
        value["extent"] = {"kind": value.pop("extent_kind")}
    elif "kind" not in value or "outcome" not in value:
        raise RuntimeError("missing staged capture activity fields")
    present = None
    if agent_id is not None:
        with closing(
            db.execute(
                "SELECT 1 FROM observation_observers WHERE assertion_id=? AND agent_id=?",
                (selected[0], agent_id),
            )
        ) as cursor:
            present = cursor.fetchone() is not None
    return value, present


def read_observation_metadata(
    db: sqlite3.Connection, identity: str, expected: str
) -> dict[str, Any]:
    """Resolve only fields consumed by direct-observation row semantics."""
    return _read(db, identity, expected)[0]


def has_observer(db: sqlite3.Connection, identity: str, agent_id: str) -> bool:
    """Check one raw capture association without decoding historical arrays."""
    return _read(db, identity, "activity", agent_id)[1] is True
