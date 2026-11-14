"""Source-coordinate traversal for shared documentary dependency closure."""

from __future__ import annotations

import sqlite3

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from riverhog_core.canonical_discovery_fact_work import FactTaskKey
from riverhog_core.canonical_discovery_journal_work import (
    JournalAssertion,
    JournalTaskKey,
    StaleJournalTask,
    _one,
    _progress,
    _task,
    initialize_journal_work,
)


def initialize_dependency_source(db: sqlite3.Connection) -> None:
    initialize_journal_work(db)
    db.execute("""
        CREATE INDEX IF NOT EXISTS discovery_journal_source_assertions
        ON discovery_journal_assertions (
            build_id,source_key,length(born_sequence),born_sequence,source_group,source_item
        )
    """)


def _declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    return Declaration(
        id="riverhog-server.discovery-documentary-source-next.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_dependency_source.next_source_assertion",),
        scope="one completed source's original assertion-coordinate selection",
        starts_at="receive current build/source/worker and one retained original position",
        ends_at="close cursors and detach at most one original assertion metadata row",
        dimensions=(
            Dimension("registered_children", COUNT, "All retained journal assertion rows."),
            Dimension(
                "metadata_octets",
                OCTETS,
                "Maximum complete input or returned metadata row, including cursor fields.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(4),
                "One parent fence and at most three indexed coordinate seeks.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(2),
                "The complete parent and at most one next original assertion.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Metadata selection does not hash source or fact payloads.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    Expression(
                        "product",
                        terms=(
                            Expression("constant", value=multiplier, unit=()),
                            dim("metadata_octets"),
                        ),
                    ),
                    "Complete logical metadata bytes; no abbreviated identities.",
                )
                for metric, multiplier in (("parameter_octets", 16), ("returned_octets", 2))
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(2048),
                        Expression(
                            "product",
                            terms=(
                                count(512),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum", terms=(dim("registered_children"), count(2))
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Each seek uses equality fields followed by a scalar or ordinary tuple range.",
            ),
        ),
        conditions=(
            "The owner maintains the exact source-coordinate index and current source lease.",
            "Original journal input and assertion fact tasks have qualified terminals.",
            "The saved position belongs to this build/source/worker and is owner-controlled.",
        ),
        invariants=(
            "Selections preserve original birth order, including later-retracted assertions.",
            "Advancing a required prefix resumes after the original source position.",
            "Completed source coordinates are never walked again to skip to a cursor.",
            "No source bytes, documentary facts, member histories or Native graphs are traversed.",
        ),
        limitations=(
            "This supplies the next source obligation, not closure completion or access authority.",
            "The closure owner must durably commit each position with its dependency effects.",
            "Forks, source atoms and documentary interpretation have separate costs.",
            "SQLite VM constants depend on the engine; I/O and elapsed time remain observations.",
        ),
    )


_COLUMNS = (
    "task_id,assertion_id,born_sequence,source_group,source_item,"
    "origin_entry_json,canonical_stream_ref"
)
_ORDER = " ORDER BY length(born_sequence),born_sequence,source_group,source_item LIMIT 1"


def _completed_source(db: sqlite3.Connection, key: JournalTaskKey) -> int:
    parent = _task(db, key)
    progress = _progress(parent)
    if (
        not progress.complete
        or progress.cursor.phase != "done"
        or progress.cursor.sequence != int(parent[3])
        or progress.pending
        or parent[14] != parent[2]
        or parent[16] is None
        or int(parent[16]) + 1 != progress.version
    ):
        raise StaleJournalTask("documentary source requires a completed original journal")
    return int(parent[3])


@declare_growth(_declaration())
def next_source_assertion(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    *,
    through: int,
    after: JournalAssertion | None = None,
) -> JournalAssertion | None:
    """Seek once past an original assertion, up to a requested exact prefix.

    Increasing the requested prefix keeps the same original cursor. Empty
    ranges do not advance it, so a later request can extend the shared journal's
    work without replaying previously inspected assertions.
    """
    entry_count = _completed_source(db, key)
    if type(through) is not int or not 0 <= through < entry_count:
        raise ValueError("documentary prefix exceeds the completed original source")
    if after is not None:
        if (
            after.key.build_id != key.build_id
            or after.key.source_key != key.source_key
            or after.key.worker_id != key.worker_id
            or type(after.sequence) is not int
            or not 0 <= after.sequence <= through
            or type(after.group) is not str
            or not after.group
            or type(after.item) is not int
            or after.item < 0
        ):
            raise StaleJournalTask("documentary cursor has another source or original position")
    query = (
        "SELECT " + _COLUMNS + " FROM discovery_journal_assertions "
        "INDEXED BY discovery_journal_source_assertions WHERE build_id=? AND source_key=?"
    )
    base = (key.build_id, key.source_key)
    if after is None:
        row = _one(db, query + _ORDER, base)
    else:
        sequence = str(after.sequence)
        # SQLite does not reliably seek a tuple range containing an expression
        # index key. Separate equality-width/sequence seeks keep every range
        # on ordinary columns, with a final scalar width seek when necessary.
        row = _one(
            db,
            query + " AND length(born_sequence)=? AND born_sequence=? "
            "AND (source_group,source_item)>(?,?) "
            "ORDER BY source_group,source_item LIMIT 1",
            (*base, len(sequence), sequence, after.group, after.item),
        )
        if row is None:
            row = _one(
                db,
                query + " AND length(born_sequence)=? AND born_sequence>? "
                "ORDER BY born_sequence,source_group,source_item LIMIT 1",
                (*base, len(sequence), sequence),
            )
        if row is None:
            row = _one(
                db,
                query + " AND length(born_sequence)>?" + _ORDER,
                (*base, len(sequence)),
            )
    if row is None or int(row[2]) > through:
        return None
    return JournalAssertion(
        FactTaskKey(key.build_id, row[0], key.source_key, key.worker_id),
        row[1],
        int(row[2]),
        row[3],
        int(row[4]),
        row[5],
        row[6],
    )
