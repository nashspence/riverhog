"""Lower documentary Own/Observed context dependencies without expanding claims."""

from __future__ import annotations

import sqlite3
from hashlib import sha256
from typing import cast

from riverhog_provenance_contracts import LocalReference

from riverhog_core import canonical_discovery_endpoint_matches as endpoint_matches
from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask, _one
from riverhog_core.canonical_discovery_documentary_selector_growth import (
    declare_growth_for_documentary_dependencies,
)
from riverhog_core.canonical_discovery_endpoint_index import _ENDPOINT_COLUMNS, _output
from riverhog_core.canonical_discovery_endpoint_matches import advance_endpoint_match
from riverhog_core.canonical_discovery_endpoint_source import read_journal_endpoint
from riverhog_core.canonical_discovery_journal_lookup import (
    JournalAssertionHeader,
)
from riverhog_core.canonical_discovery_journal_owners import next_owner_assertion
from riverhog_core.canonical_discovery_journal_selector_work import (
    JournalSelectorWork,
    PreparedJournalSelector,
    journal_selector_reads,
)
from riverhog_core.canonical_discovery_outgoing_endpoints import next_assertion_endpoint
from riverhog_core.canonical_discovery_rows import _ID_EDGES, _REFERENCE_EDGES
from riverhog_core.canonical_discovery_selector_work import (
    SelectorProgress,
    _encoded,
    _output_mapping,
    prepare_selector_page,
)
from riverhog_core.canonical_discovery_selectors import Selector


def _source_context(
    db: sqlite3.Connection,
    work: JournalSelectorWork,
    selected: Selector,
    original: JournalAssertionHeader | None,
) -> Selector | None:
    if original is None or original.kind not in {"observation", "reported_description"}:
        return None
    assert selected.anchor is not None
    result: Selector | None = None
    after: str | None = None
    # These Native kinds have only fixed reference fields, with no repeated edges.
    references = len(_ID_EDGES[original.kind]) + len(_REFERENCE_EDGES[original.kind])
    for _ in range(references + 1):
        endpoint = next_assertion_endpoint(
            db,
            work.journal_key,
            work.endpoints,
            selected.anchor,
            original,
            octet_budget=work.octet_budget,
            after=after,
        )
        if endpoint is None:
            return result
        edge = endpoint.edge
        if edge.role == "source_context_id":
            if result is not None or edge.target_scope != "local" or edge.target_type != "context":
                raise StaleArchiveTask("documentary context differs from its fixed original field")
            result = Selector(
                "direct",
                source_key=selected.source_key,
                anchor=selected.anchor,
                object_id=edge.target_id,
                scope=selected.scope,
            )
        after = endpoint.record_key
    raise StaleArchiveTask("documentary context source exceeds its fixed Native reference fields")


def _observed_at(
    db: sqlite3.Connection,
    work: JournalSelectorWork,
    selected: Selector,
    assertion_id: str,
    record_key: str,
) -> JournalAssertionHeader | None:
    assert selected.anchor is not None and selected.object_id is not None
    # Future documentary candidates still advance. Verify their original controls
    # under the complete source before suppressing them at the selected prefix.
    endpoint = read_journal_endpoint(
        db,
        work.journal_key,
        work.original.source,
        work.original.anchor,
        assertion_id,
        record_key,
        octet_budget=work.octet_budget,
    )
    row = _one(
        db,
        "SELECT " + _ENDPOINT_COLUMNS + " FROM discovery_endpoints "
        "WHERE build_id=? AND source_key=? AND assertion_id=? AND record_key=?",
        (work.journal_key.build_id, work.journal_key.source_key, assertion_id, record_key),
    )
    edge, original = endpoint.edge, endpoint.assertion
    expected = selected.object_id.encode("ascii")
    if (
        row != _output(work.journal_key, endpoint)
        or (edge.role, edge.target_scope, edge.target_type) != ("state", "local", "state")
        or endpoint.target_sha256 != sha256(expected).hexdigest()
    ):
        raise StaleArchiveTask("documentary observation position differs from its original target")
    if (
        original.born_sequence > selected.anchor.through_sequence
        or not endpoint_matches._equal_bytes(edge.target_id.encode("ascii"), expected)
    ):
        return None
    return original


def prepare_documentary_dependencies(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: JournalSelectorWork,
    expected: SelectorProgress,
) -> PreparedJournalSelector:
    """Prepare one original documentary claim's optional context dependency.

    Own/Observed assertions remain virtual indexed leaves. Their dependency
    incidence never copies those assertions or assigns a member to a claim.
    """
    work._prepared_journal = None
    work._prepared = None
    selected, position = expected.selector, expected.position
    if selected.kind not in {"own", "observed"} or expected.complete:
        raise StaleArchiveTask("documentary dependencies require their fixed pending selector")
    sources = journal_selector_reads(db, key, work, expected)
    source_db = cast(sqlite3.Connection, sources)
    assert selected.anchor is not None and selected.object_id is not None
    if selected.kind == "own":
        if position or expected.version != 0 or expected.last_output_json is not None:
            raise StaleArchiveTask("documentary owner requires its original initial position")
        # Native freezes all observation/report fields except assertion identity and
        # evidence. One original own-id assertion therefore determines this context
        # for every documentary version, including later-withdrawn corrections.
        original = next_owner_assertion(
            source_db, work.journal_key, work.original.source, selected.anchor, selected.object_id
        )
        complete = True
        following: tuple[str, ...] = ("end",)
    else:
        after: tuple[str, str] | None = None
        if not position:
            if expected.version != 0 or expected.last_output_json is not None:
                raise StaleArchiveTask("documentary initial position changed")
        else:
            if expected.version < 1 or len(position) != 3 or position[0] != "observed":
                raise StaleArchiveTask("documentary cursor has another original position")
            after = position[1], position[2]
            previous = _observed_at(source_db, work, selected, *after)
            output = _source_context(source_db, work, selected, previous)
            if expected.last_output_json != _encoded(_output_mapping(output), work.octet_budget):
                raise StaleArchiveTask("documentary cursor lost its original context incidence")
        step = advance_endpoint_match(
            source_db,
            work.journal_key,
            work.endpoints,
            selected.anchor,
            role="state",
            target=LocalReference(
                scope="local",
                object_type="state",
                object_id=selected.object_id,
            ),
            assertion_state="documentary",
            octet_budget=work.octet_budget,
            after=after,
        )
        original = None if step.endpoint is None else step.endpoint.assertion
        complete = step.complete
        if complete:
            following = ("end",)
        else:
            assert step.after is not None
            following = ("observed", *step.after)
    page = prepare_selector_page(
        work,
        expected,
        position=following,
        output=_source_context(source_db, work, selected, original),
        complete=complete,
    )
    prepared = PreparedJournalSelector(page, tuple(sources.receipts))
    work._prepared_journal = prepared
    return prepared


declare_growth_for_documentary_dependencies(globals())
