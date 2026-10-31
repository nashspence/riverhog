"""Seek one original outgoing endpoint beneath an exact assertion header."""

from __future__ import annotations

import sqlite3
from typing import Any

from riverhog_archive_contracts import HistoryJournalAnchor

from riverhog_core.canonical_discovery_endpoint_index import (
    _ENDPOINT_COLUMNS,
    EndpointIndexWork,
    _output,
    endpoint_index_progress,
)
from riverhog_core.canonical_discovery_endpoint_source import JournalEndpoint, read_journal_endpoint
from riverhog_core.canonical_discovery_journal_lookup import (
    JournalAssertionHeader,
    _natural,
    documentary_journal_assertion,
)
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey, StaleJournalTask, _one
from riverhog_core.canonical_discovery_outgoing_endpoint_growth import (
    declare_growth_for_outgoing_endpoint,
)


def next_assertion_endpoint(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    work: EndpointIndexWork,
    anchor: HistoryJournalAnchor,
    assertion: JournalAssertionHeader,
    *,
    octet_budget: int,
    after: str | None = None,
) -> JournalEndpoint | None:
    """Detach one exact endpoint or the independently indexed end.

    The caller retains only returned record keys with this exact source, prefix
    and original assertion. Documentary withdrawals remain visible; effective
    selection, relevance and current authorization belong to their owners.
    """
    if not isinstance(assertion, JournalAssertionHeader):
        raise TypeError("outgoing endpoints require an original assertion header")
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("outgoing endpoint capacity must be positive")
    if after is not None:
        if type(after) is not str:
            raise ValueError("outgoing endpoint position must be an original record key")
        _natural(after)
    original = documentary_journal_assertion(db, key, work.source, anchor, assertion.assertion_id)
    if original != assertion:
        raise StaleJournalTask("outgoing endpoint lost its exact original assertion header")
    if not endpoint_index_progress(db, key, work).complete:
        raise StaleJournalTask("outgoing endpoint index has no completed source terminal")
    source_sql = (
        "SELECT record_key FROM discovery_fact_streams "
        "WHERE build_id=? AND task_id=? AND kind='edge-header' "
    )
    index_sql = (
        "SELECT " + _ENDPOINT_COLUMNS + " FROM discovery_endpoints "
        "WHERE build_id=? AND source_key=? AND assertion_id=? "
    )
    source_parameters: tuple[Any, ...] = key.build_id, original.key.task_id
    index_parameters: tuple[Any, ...] = key.build_id, key.source_key, original.assertion_id
    if after is not None:
        source_sql += "AND record_key>? "
        index_sql += "AND record_key>? "
        source_parameters += (after,)
        index_parameters += (after,)
    source_row = _one(db, source_sql + "ORDER BY record_key LIMIT 1", source_parameters)
    row = _one(db, index_sql + "ORDER BY record_key LIMIT 1", index_parameters)
    if (None if source_row is None else source_row[0]) != (None if row is None else row[3]):
        raise StaleJournalTask("outgoing endpoint index differs from its original fact directory")
    if row is None:
        return None
    endpoint = read_journal_endpoint(
        db, key, work.source, anchor, original.assertion_id, row[3], octet_budget=octet_budget
    )
    if endpoint.assertion != original or _output(key, endpoint) != row:
        raise StaleJournalTask("outgoing endpoint differs from its original fact record")
    return endpoint


declare_growth_for_outgoing_endpoint(globals())
