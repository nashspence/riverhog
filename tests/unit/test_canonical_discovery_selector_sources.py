from __future__ import annotations

import sqlite3
from typing import cast

import pytest
from riverhog_core.canonical_discovery_journal_work import _one
from riverhog_core.canonical_discovery_selector_sources import (
    SelectorSourceReads,
    source_control_json,
)
from riverhog_core.canonical_discovery_selector_work import SelectorCapacity

from tests.support.canonical_discovery_work import write
from tests.unit.test_canonical_discovery_outgoing_endpoints import NEXT, header
from tests.unit.test_canonical_discovery_outgoing_endpoints import matching_case as matching_case
from tests.unit.test_canonical_discovery_outgoing_endpoints import matching_facts as matching_facts
from tests.unit.test_canonical_discovery_outgoing_endpoints import matching_index as matching_index
from tests.unit.test_canonical_discovery_outgoing_endpoints import (
    mixed_originals as mixed_originals,
)


def replay(db, reads):
    return [
        source_control_json(
            receipt.sql,
            receipt.parameters,
            _one(db, receipt.sql, receipt.parameters),
            reads.octet_budget,
        )
        == receipt.exact_json
        for receipt in reads.receipts
    ]


def test_sqlite_scalars_keep_exact_types_full_binary_and_integer_domains():
    parameters = (None, "00ff", b"\x00\xff", -(2**63), 2**63 - 1, 1.5, True)
    db = sqlite3.connect(":memory:", isolation_level=None)
    reads = SelectorSourceReads(db, octet_budget=4096)
    sql = "SELECT " + ",".join("?" for _ in parameters)
    row = _one(cast(sqlite3.Connection, reads), sql, parameters)
    assert row == (*parameters[:-1], 1)
    assert replay(db, reads) == [True]
    assert source_control_json("SELECT ?", (b"00ff",), (b"00ff",), 4096) != (
        source_control_json("SELECT ?", ("00ff",), ("00ff",), 4096)
    )
    assert source_control_json("SELECT 1", (), None, 4096) != (
        source_control_json("SELECT 1", (), (None,), 4096)
    )
    db.close()


def test_original_native_endpoint_receipts_include_all_physical_blobs_and_replay_in_apply(
    matching_case,
):
    case = matching_case
    original = header(case, case.assertion)
    reads = SelectorSourceReads(case.db, octet_budget=16384)
    result = NEXT(
        cast(sqlite3.Connection, reads),
        case.key,
        case.work,
        case.anchor,
        original,
        octet_budget=4096,
    )
    assert result is not None and reads.receipts
    assert any('"blob"' in receipt.exact_json for receipt in reads.receipts)
    with write(case.db):
        assert all(replay(case.db, reads))
    again = SelectorSourceReads(case.db, octet_budget=16384)
    assert (
        NEXT(
            cast(sqlite3.Connection, again),
            case.key,
            case.work,
            case.anchor,
            original,
            octet_budget=4096,
        )
        == result
    )
    assert reads.receipts == again.receipts
    with write(case.db):
        case.db.execute(
            "UPDATE discovery_fact_segments SET exact_bytes=? "
            "WHERE build_id=? AND task_id=? AND kind='edge-role' AND record_key=? AND ordinal=0",
            (
                b"!" * len(result.edge.role),
                case.key.build_id,
                original.key.task_id,
                result.record_key,
            ),
        ).close()
        assert not all(replay(case.db, reads))


def test_failed_capacity_and_empty_controls_never_acquire_a_partial_receipt():
    db = sqlite3.connect(":memory:", isolation_level=None)
    reads = SelectorSourceReads(db, octet_budget=64)
    with pytest.raises(SelectorCapacity):
        _one(cast(sqlite3.Connection, reads), "SELECT ?", (b"\x00" * 100,))
    assert reads.receipts == []
    sufficient = SelectorSourceReads(db, octet_budget=4096)
    assert _one(cast(sqlite3.Connection, sufficient), "SELECT 1 WHERE 0", ()) is None
    assert replay(db, sufficient) == [True]
    assert len(sufficient.receipts) == 1
    db.close()


def test_capture_interface_detaches_only_one_row_and_never_executes_mutations():
    db = sqlite3.connect(":memory:", isolation_level=None)
    reads = SelectorSourceReads(db, octet_budget=4096)
    with pytest.raises(RuntimeError, match="only read immutable controls"):
        reads.execute("CREATE TABLE forbidden(value)")
    assert db.execute("SELECT count(*) FROM sqlite_master").fetchone() == (0,)
    cursor = reads.execute("SELECT 1")
    try:
        assert cursor.fetchone() == (1,)
        with pytest.raises(RuntimeError, match="at most one control row"):
            cursor.fetchone()
    finally:
        cursor.close()
    assert len(reads.receipts) == 1
    db.close()
