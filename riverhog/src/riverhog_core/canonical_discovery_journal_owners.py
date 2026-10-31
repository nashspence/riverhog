"""Original documentary owner headers for fixed source-normal scope selectors."""

from __future__ import annotations

import sqlite3

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import format_scalar
from riverhog_provenance_contracts import require_canonical_uuid_urn

from riverhog_core.canonical_discovery_journal_lookup import (
    _COLUMNS,
    JournalAssertionHeader,
    _header,
    _source,
    documentary_journal_assertion,
)
from riverhog_core.canonical_discovery_journal_owner_growth import declare_growth_for_journal_owner
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
    _one,
)


def next_owner_assertion(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    source: QualifiedJournalSource,
    anchor: HistoryJournalAnchor,
    referent_id: str,
    *,
    after: JournalAssertionHeader | None = None,
) -> JournalAssertionHeader | None:
    """Seek one original own-id assertion, including later-withdrawn claims.

    The source is complete and the requested prefix is exact. The caller keeps
    its cursor with the referent and requested prefix; extending that prefix can
    continue from the last original header instead of replaying prior versions.
    This accessor does not assign a scope or establish corpus/current access.
    """
    require_canonical_uuid_urn(referent_id)
    _source(db, key, source, anchor)
    if after is not None:
        if (
            after.key.build_id != key.build_id
            or after.key.source_key != key.source_key
            or after.key.worker_id != key.worker_id
            or after.referent_id != referent_id
            or documentary_journal_assertion(db, key, source, anchor, after.assertion_id) != after
        ):
            raise StaleJournalTask("owner cursor differs from its exact original assertion")
    query = (
        "SELECT " + _COLUMNS + " FROM discovery_journal_assertions "
        "INDEXED BY discovery_journal_historical_objects "
        "WHERE build_id=? AND source_key=? AND referent_id=? "
    )
    base = (key.build_id, key.source_key, referent_id)
    ordered = "ORDER BY length(born_sequence),born_sequence,assertion_id LIMIT 1"
    if after is None:
        row = _one(db, query + ordered, base)
    else:
        born = format_scalar("nonnegative", after.born_sequence)
        row = _one(
            db,
            query + "AND length(born_sequence)=? AND born_sequence=? AND assertion_id>? "
            "ORDER BY assertion_id LIMIT 1",
            (*base, len(born), born, after.assertion_id),
        )
        if row is None:
            row = _one(
                db,
                query + "AND length(born_sequence)=? AND born_sequence>? "
                "ORDER BY born_sequence,assertion_id LIMIT 1",
                (*base, len(born), born),
            )
        if row is None:
            row = _one(
                db,
                query + "AND length(born_sequence)>? " + ordered,
                (*base, len(born)),
            )
    return _header(db, key, source, anchor, row, effective=False)


declare_growth_for_journal_owner(globals())
