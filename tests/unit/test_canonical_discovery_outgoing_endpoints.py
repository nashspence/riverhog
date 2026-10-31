from __future__ import annotations

import sqlite3
from dataclasses import replace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_core import canonical_discovery_outgoing_endpoints as outgoing
from riverhog_core.canonical_discovery_endpoint_index import EndpointIndexWork
from riverhog_core.canonical_discovery_fact_work import FactSegmentConflict
from riverhog_core.canonical_discovery_journal_fragments import JournalFactCapacity
from riverhog_core.canonical_discovery_journal_lookup import documentary_journal_assertion
from riverhog_core.canonical_discovery_journal_work import StaleJournalTask
from riverhog_provenance import JournalFrame

from tests.support.canonical_discovery_work import write
from tests.unit.test_canonical_discovery_endpoint_matches import interval_index as interval_index
from tests.unit.test_canonical_discovery_endpoint_matches import matching_case as matching_case
from tests.unit.test_canonical_discovery_endpoint_matches import matching_facts as matching_facts
from tests.unit.test_canonical_discovery_endpoint_matches import matching_index as matching_index
from tests.unit.test_canonical_discovery_endpoint_matches import original_edges
from tests.unit.test_canonical_discovery_endpoint_source import mixed_originals as mixed_originals

NEXT = outgoing.next_assertion_endpoint


def header(case, identity, anchor=None):
    return documentary_journal_assertion(
        case.db, case.key, case.source, anchor or case.anchor, identity
    )


def collect(case, assertion, *, anchor=None, cold=False):
    after, found = None, []
    while True:
        result = NEXT(
            case.db,
            case.key,
            case.work,
            anchor or case.anchor,
            assertion,
            octet_budget=4096,
            after=after,
        )
        assert result == NEXT(
            case.db,
            case.key,
            case.work,
            anchor or case.anchor,
            assertion,
            octet_budget=4096,
            after=after,
        )
        if result is None:
            return found
        assert after is None or result.record_key > after
        after = result.record_key
        found.append(result)
        if cold:
            case.db.close()
            case.db = sqlite3.connect(case.path, isolation_level=None)
            case.db.execute("PRAGMA foreign_keys=ON").close()
            case.work = EndpointIndexWork(case.source, case.anchor)


def test_all_original_outgoing_edges_and_empty_assertions_need_no_payload_reconstruction(
    matching_case, monkeypatch
):
    case = matching_case
    case.db.execute("PRAGMA foreign_keys=ON").close()
    expected = original_edges(case.native)
    assertions = [
        row["assertion_id"]
        for frame in case.native.frames
        for group in frame.document["body"].get("assertions", {}).values()
        for row in group
    ]

    def forbidden(*args, **kwargs):
        pytest.fail("outgoing endpoint reconstructed an entry or whole assertion")

    monkeypatch.setattr(case.source, "entry", forbidden)
    monkeypatch.setattr(case.source, "assertion", forbidden)
    monkeypatch.setattr(JournalFrame, "document", property(forbidden))
    empty, scopes, roles = 0, set(), set()
    for identity in assertions:
        found = collect(case, header(case, identity), cold=True)
        wanted = sorted((record, edge) for owner, record, edge in expected if owner == identity)
        assert [(item.record_key, item.edge) for item in found] == wanted
        empty += not found
        scopes.update(item.edge.target_scope for item in found)
        roles.update(item.edge.role for item in found)
    assert empty > 0 and scopes == {"local", "external"}
    assert "context:other" in roles and "context:source" in roles


def test_withdrawn_documentary_endpoints_remain_bound_to_the_requested_original_prefix(
    interval_index,
):
    case = interval_index
    for sequence in range(4):
        anchor = HistoryJournalAnchor.from_mapping(case.native.anchor_at(sequence))
        original = header(case, case.old, anchor)
        found = collect(case, original, anchor=anchor)
        assert found and all(item.assertion == original for item in found)
        assert original.retired_sequence == 1
    late = header(case, case.late)
    early = HistoryJournalAnchor.from_mapping(case.native.anchor_at(0))
    with pytest.raises(StaleJournalTask, match="original assertion header"):
        collect(case, late, anchor=early)


@pytest.mark.parametrize(
    "damage", ("missing-index", "excess-index", "changed-index", "missing-original-control")
)
def test_original_directory_and_full_endpoint_receipts_reject_damaged_candidates(
    matching_case, damage
):
    case = matching_case
    case.db.execute("PRAGMA foreign_keys=ON").close()
    identity, record, _ = next(row for row in original_edges(case.native) if row[1] == "0")
    original = header(case, identity)
    with write(case.db):
        if damage == "missing-index":
            case.db.execute(
                "DELETE FROM discovery_endpoints WHERE assertion_id=? AND record_key=?",
                (identity, record),
            ).close()
        elif damage == "excess-index":
            row = list(
                case.db.execute(
                    "SELECT * FROM discovery_endpoints WHERE assertion_id=? AND record_key=?",
                    (identity, record),
                ).fetchone()
            )
            row[3] = "999999"
            case.db.execute(
                "INSERT INTO discovery_endpoints VALUES(" + ",".join("?" for _ in row) + ")", row
            ).close()
        elif damage == "changed-index":
            case.db.execute(
                "UPDATE discovery_endpoints SET target_octets=target_octets+1 "
                "WHERE assertion_id=? AND record_key=?",
                (identity, record),
            ).close()
        else:
            case.db.execute(
                "DELETE FROM discovery_fact_segments WHERE task_id=? "
                "AND kind='edge-role' AND record_key=?",
                (original.key.task_id, record),
            ).close()
            case.db.execute(
                "DELETE FROM discovery_fact_streams WHERE task_id=? "
                "AND kind='edge-role' AND record_key=?",
                (original.key.task_id, record),
            ).close()
    with pytest.raises((StaleJournalTask, FactSegmentConflict)):
        collect(case, original)


@pytest.mark.parametrize("field,value", (("referent_id", "wrong"), ("retired_sequence", 19)))
def test_original_header_is_compared_even_when_no_candidate_is_left(matching_case, field, value):
    case = matching_case
    identity = original_edges(case.native)[0][0]
    original = header(case, identity)
    with pytest.raises(StaleJournalTask, match="original assertion header"):
        NEXT(
            case.db,
            case.key,
            case.work,
            case.anchor,
            replace(original, **{field: value}),
            octet_budget=4096,
            after="999999",
        )


def test_capacity_and_exact_source_index_boundaries_preserve_retry(matching_case):
    case = matching_case
    case.db.execute("PRAGMA foreign_keys=ON").close()
    identity = original_edges(case.native)[0][0]
    original = header(case, identity)
    with pytest.raises(JournalFactCapacity):
        NEXT(case.db, case.key, case.work, case.anchor, original, octet_budget=1)
    assert collect(case, original)
    with pytest.raises(ValueError, match="transaction"):
        with write(case.db):
            NEXT(case.db, case.key, case.work, case.anchor, original, octet_budget=4096)
    with pytest.raises(StaleJournalTask):
        collect(case, original, anchor=replace(case.anchor, prefix_sha256="f" * 64))
    with write(case.db):
        case.db.execute("UPDATE discovery_endpoint_tasks SET phase='streams'").close()
    with pytest.raises(StaleJournalTask, match="completed source terminal"):
        collect(case, original)


def test_empty_outgoing_end_still_requires_live_original_custody(matching_case):
    case = matching_case
    empty_id = case.db.execute(
        "SELECT a.assertion_id FROM discovery_journal_assertions a "
        "WHERE NOT EXISTS (SELECT 1 FROM discovery_endpoints e "
        "WHERE e.build_id=a.build_id AND e.source_key=a.source_key "
        "AND e.assertion_id=a.assertion_id) LIMIT 1"
    ).fetchone()[0]
    original = header(case, empty_id)
    assert collect(case, original) == []
    case.source.close()
    with pytest.raises(StaleJournalTask):
        collect(case, original)
