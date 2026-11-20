"""Exact source-normal assertion headers without historical graph expansion."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes, format_scalar, parse_scalar
from riverhog_provenance_contracts import require_canonical_uuid_urn

from riverhog_core.canonical_discovery_fact_work import FactTaskKey
from riverhog_core.canonical_discovery_fact_work import _task as fact_task
from riverhog_core.canonical_discovery_journal_lookup_growth import (
    declare_growth_for_journal_lookup,
)
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
    _one,
    _task,
)


@dataclass(frozen=True, slots=True)
class JournalAssertionHeader:
    key: FactTaskKey
    assertion_id: str
    referent_id: str
    kind: str
    born_sequence: int
    retired_sequence: int | None
    reference_json: str
    canonical_stream_ref: str


def _natural(value: Any) -> int:
    try:
        return parse_scalar("nonnegative", value)
    except (TypeError, ValueError):
        raise StaleJournalTask("historical source control is not canonical") from None


def _source(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    source: QualifiedJournalSource,
    anchor: HistoryJournalAnchor,
) -> None:
    if db.in_transaction:
        raise ValueError("historical source lookup requires no write transaction")
    if not isinstance(anchor, HistoryJournalAnchor):
        raise TypeError("historical source lookup requires an exact anchor")
    source.check_open()
    if key.source_key != source.source_key:
        raise StaleJournalTask("historical source has another original binding")
    row = _task(db, key)
    if (
        row[1:4]
        != (
            source.binding_json,
            source.terminal_json,
            format_scalar("nonnegative", source.entry_count),
        )
        or row[15] != 1
        or row[13] != "0"
        or row[6] != "done"
        or row[5] != row[3]
        or row[14] != row[2]
        or _natural(row[4]) != _natural(row[16]) + 1
    ):
        raise StaleJournalTask("historical source lacks its exact completed input terminal")
    if source.anchor_at(anchor.through_sequence) != anchor.to_mapping():
        raise StaleJournalTask("historical source anchor differs from the original Native prefix")


def _header(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    source: QualifiedJournalSource,
    anchor: HistoryJournalAnchor,
    row: tuple[Any, ...] | None,
    *,
    effective: bool,
) -> JournalAssertionHeader | None:
    if row is None:
        return None
    assertion, referent, kind, born_raw, retired_raw, entry_json, task_id, stream, acknowledged = (
        row
    )
    born = _natural(born_raw)
    retired = None if retired_raw is None else _natural(retired_raw)
    if retired is not None and retired <= born:
        raise StaleJournalTask("historical assertion has another source interval")
    if born > anchor.through_sequence:
        return None
    if effective and retired is not None and retired <= anchor.through_sequence:
        return None
    child_key = FactTaskKey(key.build_id, task_id, key.source_key, key.worker_id)
    child = fact_task(db, child_key)
    if (
        acknowledged != 1
        or child[7] != 1
        or child[4] != 0
        or child[9] != stream
        or type(child[2]) is not int
        or type(child[8]) is not int
        or child[8] < 0
        or child[2] != child[8] + 1
    ):
        raise StaleJournalTask("historical assertion lost its completed exact fact child")
    reference = source.reference_for_assertion(assertion, object_id=referent, object_type=kind)
    if (
        canonical_json_bytes(reference["entry"]).decode() != entry_json
        or _natural(reference["entry"]["sequence"]) != born
    ):
        raise StaleJournalTask("historical assertion lost its original Native address")
    return JournalAssertionHeader(
        child_key,
        assertion,
        referent,
        kind,
        born,
        retired,
        canonical_json_bytes(reference).decode(),
        stream,
    )


_COLUMNS = (
    "assertion_id,referent_id,kind,born_sequence,retired_sequence,"
    "origin_entry_json,task_id,canonical_stream_ref,acknowledged"
)


def documentary_journal_assertion(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    source: QualifiedJournalSource,
    anchor: HistoryJournalAnchor,
    assertion_id: str,
) -> JournalAssertionHeader | None:
    """Return one original assertion born by this prefix, including retired claims."""
    require_canonical_uuid_urn(assertion_id)
    _source(db, key, source, anchor)
    row = _one(
        db,
        "SELECT " + _COLUMNS + " FROM discovery_journal_assertions "
        "WHERE build_id=? AND source_key=? AND assertion_id=?",
        (key.build_id, key.source_key, assertion_id),
    )
    return _header(db, key, source, anchor, row, effective=False)


def effective_journal_assertion(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    source: QualifiedJournalSource,
    anchor: HistoryJournalAnchor,
    referent_id: str,
) -> JournalAssertionHeader | None:
    """Select the last original birth at this prefix; a retired gap stays absent."""
    require_canonical_uuid_urn(referent_id)
    _source(db, key, source, anchor)
    sequence = format_scalar("nonnegative", anchor.through_sequence)
    prefix = (
        "SELECT " + _COLUMNS + " FROM discovery_journal_assertions "
        "WHERE build_id=? AND source_key=? AND referent_id=? "
    )
    row = _one(
        db,
        prefix + "AND length(born_sequence)=? AND born_sequence<=? "
        "ORDER BY born_sequence DESC,assertion_id DESC LIMIT 1",
        (key.build_id, key.source_key, referent_id, len(sequence), sequence),
    )
    if row is None:
        row = _one(
            db,
            prefix + "AND length(born_sequence)<? "
            "ORDER BY length(born_sequence) DESC,born_sequence DESC,assertion_id DESC LIMIT 1",
            (key.build_id, key.source_key, referent_id, len(sequence)),
        )
    return _header(db, key, source, anchor, row, effective=True)


declare_growth_for_journal_lookup(globals())
