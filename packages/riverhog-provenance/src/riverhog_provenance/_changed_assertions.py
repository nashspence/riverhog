"""Admit one changed assertion without retaining a SQLite cursor.

The owner pins the private effective view and supplies its last completed key.
This read does not publish progress, retain source custody or accept a prefix.
Only the selected active assertion can supply a payload; missing and retired
identities still advance the caller's keyset cursor in one bounded unit.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import require_canonical_uuid_urn

from .errors import JournalValidationCapacity


class ChangedAssertionCapacity(JournalValidationCapacity):
    """The selected current atom needs more byte admission."""


@dataclass(frozen=True)
class ChangedAssertion:
    assertion_id: str
    raw: bytes | None


def _declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    return Declaration(
        id="riverhog-provenance.sqlite-changed-assertion-admission.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._changed_assertions.read_changed_assertion",),
        scope="native-sqlite-one-changed-identity-and-current-atom-admission",
        starts_at="receive the owner-pinned effective view and last completed assertion key",
        ends_at="close every cursor and return one selected identity with its admitted active atom",
        dimensions=(
            Dimension("indexed_assertions", COUNT, "Retained active and retired assertion rows."),
            Dimension("changed_identities", COUNT, "Entries in the indexed changes directory."),
            Dimension(
                "atom_octets", OCTETS, "Payload octets returned for the selected active atom."
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(3),
                "Key, active length and admitted atom seeks.",
            ),
            Bound(
                "returned_rows", COUNT, "upper", count(3), "At most one row from each closed seek."
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                Expression("constant", value=64, unit=OCTETS),
                "One canonical UUID URN and an entry-bounded decimal payload length.",
            ),
            Bound(
                "returned_atom_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="atom_octets"),
                "Only this selected active assertion is fetched after byte admission.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                Expression("constant", value=135, unit=OCTETS),
                "The preceding canonical UUID URN and at most two selected UUID URNs.",
            ),
            Bound("changed_rows", COUNT, "equal", count(0), "Admission is read-only."),
            Bound(
                "historical_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Retired, missing and unselected assertion bodies are never fetched.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
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
                                        Expression("dimension", name="indexed_assertions"),
                                        Expression("dimension", name="changed_identities"),
                                        count(2),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Three indexed seeks; no scan through retired gaps, offset or temporary sort.",
            ),
        ),
        conditions=(
            "The owning operation pins a single integrity-checked effective view across the read.",
            "Staged keys are canonical UUID URNs; active payloads passed the entry schema.",
            "Atoms retain the existing entry-byte envelope; byte admission is a work budget.",
            "SQLite reads an admitted BLOB length without materializing its payload.",
        ),
        invariants=(
            "Changed identities retain canonical key order, including retired and missing rows.",
            "A capacity refusal leaves all state and the caller's completed key unchanged.",
            "Every SQL cursor closes before return, decoding or subsequent semantic work.",
        ),
        limitations=(
            "The owner checkpoints the selected key after its semantic subphases complete.",
            "Atom decoding, semantics, lock waits and physical page I/O are separate costs.",
            "This read grants no durable replay, accepted-prefix or archive authority.",
        ),
    )


@declare_growth(_declaration())
def read_changed_assertion(
    db: sqlite3.Connection, *, after: str | None, byte_budget: int
) -> ChangedAssertion | None:
    if after is not None:
        require_canonical_uuid_urn(after)
    if type(byte_budget) is not int or byte_budget < 1:
        raise ValueError("changed assertion byte admission must be positive")
    with closing(
        db.execute(
            "SELECT assertion_id FROM changes WHERE assertion_id > ? ORDER BY assertion_id LIMIT 1",
            ("" if after is None else after,),
        )
    ) as cursor:
        selected = cursor.fetchone()
    if selected is None:
        return None
    identity = selected[0]
    require_canonical_uuid_urn(identity)
    with closing(
        db.execute(
            "SELECT length(value) FROM assertions WHERE assertion_id = ? AND active = 1",
            (identity,),
        )
    ) as cursor:
        metadata = cursor.fetchone()
    if metadata is None:
        return ChangedAssertion(identity, None)
    length = metadata[0]
    if type(length) is not int or length < 0:
        raise RuntimeError("invalid changed assertion payload length")
    if length > byte_budget:
        raise ChangedAssertionCapacity("the selected assertion needs more byte admission")
    with closing(
        db.execute(
            "SELECT value FROM assertions WHERE assertion_id = ? AND active = 1", (identity,)
        )
    ) as cursor:
        payload = cursor.fetchone()
    if payload is None or type(payload[0]) is not bytes or len(payload[0]) != length:
        raise RuntimeError("changed assertion source moved during atom admission")
    return ChangedAssertion(identity, payload[0])
