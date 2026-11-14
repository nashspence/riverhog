from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_journal_fragments as fragments
from riverhog_core.canonical_discovery_fact_work import FactSegmentConflict, FactStreamKey
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
)
from riverhog_provenance import JournalFrame, validate_journal

from tests.support.canonical_discovery import native_profile_fixture
from tests.support.canonical_discovery_work import ADMISSION, open_journal_work, write
from tests.unit.test_canonical_discovery_journal_extraction import run

READ = fragments.read_journal_fact_fragment
CANONICAL = FactStreamKey("canonical", "0")


@pytest.fixture(scope="module")
def original_facts(tmp_path_factory):
    fixture = native_profile_fixture("é" * 12000, {"type": "string"})
    with validate_journal(fixture.raw, catalog=fixture.catalog) as native:
        with QualifiedJournalSource(native, ADMISSION) as source:
            db = open_journal_work(tmp_path_factory.mktemp("fragment-source") / "source.sqlite3")
            try:
                key = JournalTaskKey("build", source.source_key, "worker")
                run(JournalFactExtraction(db, key, source), 128)
                row = next(
                    row
                    for frame in native.frames
                    for group in frame.document["body"].get("assertions", {}).values()
                    for row in group
                    if row["assertion_id"] == fixture.assertion_id
                )
                yield SimpleNamespace(
                    db=db,
                    key=key,
                    native=native,
                    fixture=fixture,
                    anchor=HistoryJournalAnchor.from_mapping(native.anchor),
                    canonical=canonical_json_bytes(row),
                )
            finally:
                db.close()


@pytest.fixture
def case(original_facts, tmp_path):
    original = original_facts
    path = tmp_path / "work.sqlite3"
    db = sqlite3.connect(path, isolation_level=None)
    original.db.backup(db)
    with QualifiedJournalSource(original.native, ADMISSION) as source:
        value = SimpleNamespace(
            db=db,
            key=original.key,
            source=source,
            anchor=original.anchor,
            assertion=original.fixture.assertion_id,
            canonical=original.canonical,
            path=path,
            native=original.native,
        )
        try:
            yield value
        finally:
            value.db.close()


def read(case, stream=CANONICAL, ordinal=0, budget=4096):
    return READ(
        case.db,
        case.key,
        case.source,
        case.anchor,
        case.assertion,
        stream,
        ordinal=ordinal,
        octet_budget=budget,
    )


def task(case):
    return case.db.execute(
        "SELECT task_id FROM discovery_journal_assertions WHERE assertion_id=?",
        (case.assertion,),
    ).fetchone()[0]


def test_all_original_fact_bytes_and_empty_streams_survive_exact_fragment_reads(case, monkeypatch):
    child = task(case)
    expected = {}
    for kind, key, count, octets in case.db.execute(
        "SELECT kind,record_key,fragments,octets FROM discovery_fact_streams "
        "WHERE build_id=? AND task_id=?",
        (case.key.build_id, child),
    ):
        value = b"".join(
            row[0]
            for row in case.db.execute(
                "SELECT exact_bytes FROM discovery_fact_segments "
                "WHERE build_id=? AND task_id=? AND kind=? AND record_key=? ORDER BY ordinal",
                (case.key.build_id, child, kind, key),
            )
        )
        assert len(value) == octets
        expected[FactStreamKey(kind, key)] = (value, count)
    assert any(not value for value, _ in expected.values())
    assert expected[CANONICAL][0] == case.canonical

    def forbidden(*args, **kwargs):
        pytest.fail("physical fact read decoded its original assertion")

    monkeypatch.setattr(case.source, "entry", forbidden)
    monkeypatch.setattr(case.source, "assertion", forbidden)
    monkeypatch.setattr(JournalFrame, "document", property(forbidden))
    for stream, (exact, count) in expected.items():
        parts = []
        for ordinal in range(count + 1):
            part = read(case, stream, ordinal)
            assert part == read(case, stream, ordinal)
            assert part.ordinal == ordinal and part.fragments == count
            assert part.octets == len(exact) and part.stream == stream
            assert part.terminal == (ordinal == count)
            assert len(part.exact_bytes) <= 4096
            parts.append(part.exact_bytes)
        assert b"".join(parts) == exact
    assert hashlib.sha256(case.canonical).hexdigest() == read(case).assertion.canonical_stream_ref


def test_capacity_rejection_precedes_any_payload_column_access_and_preserves_retry(case):
    reads = []

    def authorize(action, table, column, database, trigger):
        if action == sqlite3.SQLITE_READ and table == "discovery_fact_segments":
            reads.append(column)
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    case.db.set_authorizer(authorize)
    try:
        with pytest.raises(fragments.JournalFactCapacity):
            read(case, budget=4095)
    finally:
        case.db.set_authorizer(None)
    assert reads == []
    assert read(case).exact_bytes == case.canonical[:4096]


def test_reopen_resumes_only_the_exact_next_physical_fragment(case):
    first = read(case)
    case.db.close()
    case.db = sqlite3.connect(case.path, isolation_level=None)
    second = read(case, ordinal=first.ordinal + 1)
    assert second.exact_bytes == case.canonical[4096:8192]
    assert first.assertion == second.assertion


@pytest.mark.parametrize(
    "damage",
    (
        "missing-stream",
        "unfinished",
        "stream-version",
        "count",
        "tail",
        "octets",
        "missing-fragment",
        "checksum",
        "bytes",
        "oversized",
        "fragment-version",
        "previous-fragment",
        "previous-version",
        "excess",
        "unacknowledged",
        "source-pending",
    ),
)
def test_incomplete_or_corrupt_fact_work_never_becomes_a_value_or_end(case, damage):
    child = task(case)
    identity = (case.key.build_id, child, "canonical", "0")
    count, terminal = case.db.execute(
        "SELECT fragments,terminal_version FROM discovery_fact_streams "
        "WHERE build_id=? AND task_id=? AND kind=? AND record_key=?",
        identity,
    ).fetchone()
    ordinal = 1 if damage in {"previous-fragment", "previous-version"} else 0
    case.db.execute("PRAGMA ignore_check_constraints=ON")
    case.db.execute("PRAGMA foreign_keys=OFF")
    with write(case.db):
        if damage == "missing-stream":
            case.db.execute(
                "DELETE FROM discovery_fact_streams "
                "WHERE build_id=? AND task_id=? AND kind=? AND record_key=?",
                identity,
            ).close()
        elif damage in {"unfinished", "stream-version", "count", "tail", "octets"}:
            field, value = {
                "unfinished": ("complete", 0),
                "stream-version": ("terminal_version", 10**9),
                "count": ("fragments", count + 1),
                "tail": ("last_fragment_bytes", 0),
                "octets": ("octets", 0),
            }[damage]
            case.db.execute(
                "UPDATE discovery_fact_streams SET " + field + "=? "
                "WHERE build_id=? AND task_id=? AND kind=? AND record_key=?",
                (value, *identity),
            ).close()
        elif damage in {"missing-fragment", "previous-fragment"}:
            case.db.execute(
                "DELETE FROM discovery_fact_segments "
                "WHERE build_id=? AND task_id=? AND kind=? AND record_key=? AND ordinal=0",
                identity,
            ).close()
        elif damage == "excess":
            case.db.execute(
                "INSERT INTO discovery_fact_segments VALUES(?,?,?,?,?,?,?,?)",
                (*identity, count, b"x", hashlib.sha256(b"x").hexdigest(), terminal - 1),
            ).close()
            ordinal = count
        elif damage in {"unacknowledged", "source-pending"}:
            sql, args = (
                (
                    "UPDATE discovery_journal_assertions SET acknowledged=0 WHERE assertion_id=?",
                    (case.assertion,),
                )
                if damage == "unacknowledged"
                else ("UPDATE discovery_journal_tasks SET pending='1'", ())
            )
            case.db.execute(sql, args).close()
        else:
            field, value = {
                "checksum": ("sha256", "f" * 64),
                "bytes": ("exact_bytes", b"!" * 4096),
                "oversized": ("exact_bytes", b"!" * 8192),
                "fragment-version": ("staged_version", terminal),
                "previous-version": ("staged_version", terminal),
            }[damage]
            case.db.execute(
                "UPDATE discovery_fact_segments SET " + field + "=? "
                "WHERE build_id=? AND task_id=? AND kind=? AND record_key=? AND ordinal=0",
                (value, *identity),
            ).close()
    with pytest.raises((FactSegmentConflict, StaleJournalTask)):
        read(case, ordinal=ordinal)


@pytest.mark.parametrize("ordinal,budget", ((-1, 4096), (True, 4096), (0, 0), (0, True)))
def test_invalid_execution_coordinates_do_not_read_or_mutate(case, ordinal, budget):
    before = case.db.total_changes
    with pytest.raises(ValueError):
        read(case, ordinal=ordinal, budget=budget)
    assert case.db.total_changes == before


def test_out_of_range_position_and_unknown_stream_do_not_claim_end(case):
    with pytest.raises(ValueError):
        read(case, ordinal=10**5000)
    with pytest.raises(FactSegmentConflict):
        read(case, FactStreamKey("absent", "0"))
    with pytest.raises(StaleJournalTask):
        READ(
            case.db,
            case.key,
            case.source,
            case.anchor,
            "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff",
            CANONICAL,
            ordinal=0,
            octet_budget=4096,
        )


def test_exact_source_anchor_and_transaction_boundaries_are_preserved(case):
    anchor = case.anchor
    case.anchor = replace(anchor, prefix_sha256="f" * 64)
    with pytest.raises(StaleJournalTask):
        read(case)
    case.anchor = anchor
    with write(case.db):
        with pytest.raises(ValueError, match="transaction"):
            read(case)


def test_retired_documentary_source_bytes_remain_readable(tmp_path):
    from tests.unit.test_canonical_discovery_journal_lookup import completed

    with completed(tmp_path / "retired.sqlite3") as (db, key, source, fixture, native):
        anchor = HistoryJournalAnchor.from_mapping(native.anchor)
        part = READ(
            db, key, source, anchor, fixture.assertion_id, CANONICAL, ordinal=0, octet_budget=4096
        )
        assert part.assertion.retired_sequence == 1
        assert part.exact_bytes
