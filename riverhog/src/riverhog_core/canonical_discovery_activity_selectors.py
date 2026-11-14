"""Lower one effective activity through exact documentary and context selectors."""

from __future__ import annotations

import sqlite3
from typing import cast

from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask, _one
from riverhog_core.canonical_discovery_endpoint_index import _ENDPOINT_COLUMNS, _output
from riverhog_core.canonical_discovery_endpoint_source import JournalEndpoint, read_journal_endpoint
from riverhog_core.canonical_discovery_journal_lookup import (
    JournalAssertionHeader,
    effective_journal_assertion,
)
from riverhog_core.canonical_discovery_journal_selector_growth import (
    declare_growth_for_activity_selector,
)
from riverhog_core.canonical_discovery_journal_selector_work import (
    JournalSelectorWork,
    PreparedJournalSelector,
    journal_selector_reads,
)
from riverhog_core.canonical_discovery_outgoing_endpoints import next_assertion_endpoint
from riverhog_core.canonical_discovery_selector_work import (
    SelectorProgress,
    _encoded,
    _output_mapping,
    prepare_selector_page,
)
from riverhog_core.canonical_discovery_selectors import Selector


def _own(selected: Selector) -> Selector:
    return Selector(
        "own",
        source_key=selected.source_key,
        anchor=selected.anchor,
        object_id=selected.object_id,
        scope=selected.scope,
    )


def _context(selected: Selector, endpoint: JournalEndpoint) -> Selector | None:
    if not endpoint.edge.role.startswith("context:"):
        return None
    if endpoint.edge.target_scope != "local" or endpoint.edge.target_type != "context":
        raise StaleArchiveTask("activity context differs from its original Native type")
    return Selector(
        "direct",
        source_key=selected.source_key,
        anchor=selected.anchor,
        object_id=endpoint.edge.target_id,
        scope=selected.scope,
    )


def _previous(
    db: sqlite3.Connection,
    work: JournalSelectorWork,
    expected: SelectorProgress,
    activity: JournalAssertionHeader,
) -> str | None:
    selected, position = expected.selector, expected.position
    if position == ("contexts",) and expected.version == 1:
        output: Selector | None = _own(selected)
        after = None
    elif len(position) == 2 and position[0] == "contexts" and expected.version >= 2:
        after = position[1]
        assert selected.anchor is not None
        endpoint = read_journal_endpoint(
            db,
            work.journal_key,
            work.original.source,
            selected.anchor,
            activity.assertion_id,
            after,
            octet_budget=work.octet_budget,
        )
        row = _one(
            db,
            "SELECT " + _ENDPOINT_COLUMNS + " FROM discovery_endpoints "
            "WHERE build_id=? AND source_key=? AND assertion_id=? AND record_key=?",
            (work.journal_key.build_id, work.journal_key.source_key, activity.assertion_id, after),
        )
        if endpoint.assertion != activity or row != _output(work.journal_key, endpoint):
            raise StaleArchiveTask("activity selector lost its previous original endpoint")
        output = _context(selected, endpoint)
    else:
        raise StaleArchiveTask("activity selector has another exact original position")
    if expected.last_output_json != _encoded(_output_mapping(output), work.octet_budget):
        raise StaleArchiveTask("activity selector cursor differs from its original incidence")
    return after


def prepare_activity_selector(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: JournalSelectorWork,
    expected: SelectorProgress,
) -> PreparedJournalSelector:
    """Prepare one own link, one original context candidate or an exact end."""
    work._prepared_journal = None
    work._prepared = None
    if expected.selector.kind != "activity" or expected.complete:
        raise StaleArchiveTask("activity selector preparation has another fixed kind or owner")
    sources = journal_selector_reads(db, key, work, expected)
    source_db = cast(sqlite3.Connection, sources)
    selected = expected.selector
    assert selected.anchor is not None and selected.object_id is not None
    activity = effective_journal_assertion(
        source_db, work.journal_key, work.original.source, selected.anchor, selected.object_id
    )
    if activity is not None and activity.kind != "activity":
        raise StaleArchiveTask("activity selector target is another original Native kind")
    if not expected.position:
        if expected.version != 0 or expected.last_output_json is not None:
            raise StaleArchiveTask("activity selector initial position changed")
        page = prepare_selector_page(
            work,
            expected,
            position=("end",) if activity is None else ("contexts",),
            output=None if activity is None else _own(selected),
            complete=activity is None,
        )
    else:
        if activity is None:
            raise StaleArchiveTask("activity selector lost its original effective activity")
        after = _previous(source_db, work, expected, activity)
        endpoint = next_assertion_endpoint(
            source_db,
            work.journal_key,
            work.endpoints,
            selected.anchor,
            activity,
            octet_budget=work.octet_budget,
            after=after,
        )
        page = prepare_selector_page(
            work,
            expected,
            position=("end",) if endpoint is None else ("contexts", endpoint.record_key),
            output=None if endpoint is None else _context(selected, endpoint),
            complete=endpoint is None,
        )
    prepared = PreparedJournalSelector(page, tuple(sources.receipts))
    work._prepared_journal = prepared
    return prepared


declare_growth_for_activity_selector(globals())
