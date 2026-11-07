from __future__ import annotations

import copy
import hashlib
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_endpoint_source as endpoints
from riverhog_core.canonical_discovery_fact_work import FactSegmentConflict
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_fragments import JournalFactCapacity
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
)
from riverhog_core.canonical_discovery_rows import core_edges
from riverhog_provenance import JournalFrame, assertion, external_reference, validate_journal

from tests.support.canonical_discovery import native_scalar_fixture
from tests.support.canonical_discovery_work import ADMISSION, open_journal_work, write
from tests.unit.test_canonical_discovery_journal_extraction import run
from tests.unit.test_canonical_discovery_journal_fragments import case as case
from tests.unit.test_canonical_discovery_journal_fragments import original_facts as original_facts

READ = endpoints.read_journal_endpoint


def read(case, record_key="0", budget=4096):
    return READ(
        case.db,
        case.key,
        case.source,
        case.anchor,
        case.assertion,
        record_key,
        octet_budget=budget,
    )


def assert_edges(case, monkeypatch):
    expected = [
        (row["assertion_id"], ordinal, edge)
        for frame in case.native.frames
        for group in frame.document["body"].get("assertions", {}).values()
        for row in group
        for ordinal, edge in enumerate(core_edges(row))
    ]
    assert expected

    def forbidden(*args, **kwargs):
        pytest.fail("endpoint preparation decoded its original source assertion")

    with monkeypatch.context() as patch:
        patch.setattr(case.source, "entry", forbidden)
        patch.setattr(case.source, "assertion", forbidden)
        patch.setattr(JournalFrame, "document", property(forbidden))
        for identity, ordinal, edge in expected:
            case.assertion = identity
            result = read(case, str(ordinal))
            assert result == read(case, str(ordinal))
            assert result.edge == edge
            assert result.index_role == (
                "context" if edge.role.startswith("context:") else edge.role
            )
            exact = (
                edge.exact_foreign_reference
                if edge.target_scope == "external"
                else edge.target_id.encode()
            )
            assert result.target_sha256 == hashlib.sha256(exact).hexdigest()
            assert result.target_octets == len(exact)
    return expected


def test_every_original_edge_matches_native_projection_without_assertion_decode(case, monkeypatch):
    assert_edges(case, monkeypatch)


def test_reopen_and_retry_preserve_the_same_complete_endpoint(case):
    first = read(case)
    case.db.close()
    case.db = sqlite3.connect(case.path, isolation_level=None)
    assert read(case) == first


@pytest.fixture(scope="module")
def mixed_originals(tmp_path_factory):
    fixture = native_scalar_fixture(1)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as native:
        row = next(
            row
            for frame in native.frames
            for group in frame.document["body"].get("assertions", {}).values()
            for row in group
            if row["type"] == "artifact"
        )
        reference = external_reference(native, row["id"])
        reference["journal_id"] = "urn:uuid:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        reference["entry"]["sequence"] = "9223372036854775807"
        document = copy.deepcopy(native.frames[0].document)
        recorder = document["recorded_by_agent_id"]
        context = assertion("context", recorder, kind="opaque", label="endpoint fixture")
        graph = document["body"]["assertions"]
        graph.setdefault("contexts", []).append(context)
        graph["activities"][0]["contexts"] = [
            {
                "context_id": context["id"],
                "role": role,
                **(
                    {"role_uri": "urn:test:other-context-role:" + "x" * 12000}
                    if role == "other"
                    else {}
                ),
            }
            for role in ("source", "destination", "execution", "recording", "other")
        ]
        graph["extensions"][0]["value"] = {"type": "reference", "value": reference}
        raw = JournalFrame(canonical_json_bytes(document)).encoded
    with validate_journal(raw, catalog=fixture.catalog) as native:
        with QualifiedJournalSource(native, ADMISSION) as source:
            db = open_journal_work(tmp_path_factory.mktemp("mixed-endpoint") / "source.sqlite3")
            try:
                key = JournalTaskKey("build", source.source_key, "worker")
                run(JournalFactExtraction(db, key, source), 128)
                yield SimpleNamespace(
                    db=db,
                    key=key,
                    anchor=HistoryJournalAnchor.from_mapping(native.anchor),
                    assertion=fixture.assertion_id,
                    native=native,
                    catalog=fixture.catalog,
                    canonical=canonical_json_bytes(graph["extensions"][0]),
                )
            finally:
                db.close()


@pytest.fixture
def mixed_case(mixed_originals, tmp_path):
    original = mixed_originals
    path = tmp_path / "mixed-work.sqlite3"
    db = sqlite3.connect(path, isolation_level=None)
    original.db.backup(db)
    with QualifiedJournalSource(original.native, ADMISSION) as source:
        case = SimpleNamespace(
            db=db,
            key=original.key,
            source=source,
            anchor=original.anchor,
            assertion=original.assertion,
            native=original.native,
            path=path,
            canonical=original.canonical,
        )
        try:
            yield case
        finally:
            case.db.close()


def test_native_context_roles_and_foreign_reference_keep_original_fields(mixed_case, monkeypatch):
    expected = assert_edges(mixed_case, monkeypatch)
    assert any(edge.target_scope == "external" for _, _, edge in expected)
    assert {edge.role for _, _, edge in expected if edge.role.startswith("context:")} == {
        "context:" + role for role in ("source", "destination", "execution", "recording", "other")
    }


def test_capacity_and_write_boundaries_leave_the_same_endpoint_available(case):
    with pytest.raises(JournalFactCapacity):
        read(case, budget=1)
    assert read(case).edge.target_scope == "local"
    with write(case.db):
        with pytest.raises(ValueError, match="transaction"):
            read(case)


@pytest.mark.parametrize("damage", ("ordinal", "scope", "role", "target", "foreign", "missing"))
def test_damaged_control_fields_do_not_supply_endpoint_candidates(case, damage):
    child = read(case).assertion.key.task_id
    kind = {
        "ordinal": "edge-header",
        "scope": "edge-header",
        "role": "edge-role",
        "target": "edge-target",
        "foreign": "edge-foreign",
        "missing": "edge-role",
    }[damage]
    identity = (case.key.build_id, child, kind, "0")
    with write(case.db):
        if damage == "missing":
            case.db.execute(
                "DELETE FROM discovery_fact_segments "
                "WHERE build_id=? AND task_id=? AND kind=? AND record_key=?",
                identity,
            ).close()
        elif damage == "foreign":
            case.db.execute(
                "UPDATE discovery_fact_streams SET complete=0 "
                "WHERE build_id=? AND task_id=? AND kind=? AND record_key=?",
                identity,
            ).close()
        else:
            old = case.db.execute(
                "SELECT exact_bytes FROM discovery_fact_segments "
                "WHERE build_id=? AND task_id=? AND kind=? AND record_key=? AND ordinal=0",
                identity,
            ).fetchone()[0]
            if damage == "ordinal":
                value = endpoints.require_canonical_json(old)
                value["ordinal"] = 9
                raw = canonical_json_bytes(value)
            elif damage == "scope":
                value = endpoints.require_canonical_json(old)
                value["target_scope"] = "wrong"
                raw = canonical_json_bytes(value)
            else:
                raw = b"?" * len(old)
            case.db.execute(
                "UPDATE discovery_fact_segments SET exact_bytes=?,sha256=? "
                "WHERE build_id=? AND task_id=? AND kind=? AND record_key=? AND ordinal=0",
                (raw, hashlib.sha256(raw).hexdigest(), *identity),
            ).close()
            case.db.execute(
                "UPDATE discovery_fact_streams SET octets=?,last_fragment_bytes=? "
                "WHERE build_id=? AND task_id=? AND kind=? AND record_key=?",
                (len(raw), len(raw), *identity),
            ).close()
    with pytest.raises(FactSegmentConflict):
        read(case)


def test_changed_original_anchor_or_source_custody_never_supplies_endpoint(case):
    anchor = case.anchor
    case.anchor = replace(anchor, prefix_sha256="f" * 64)
    with pytest.raises(StaleJournalTask):
        read(case)
    case.anchor = anchor
    case.source.close()
    with pytest.raises((ValueError, RuntimeError)):
        read(case)


def test_fragmented_control_is_rejected_as_changed_native_grammar(case):
    endpoint = read(case)
    identity = (case.key.build_id, endpoint.assertion.key.task_id, "edge-role", "0")
    with write(case.db):
        case.db.execute(
            "UPDATE discovery_fact_segments SET exact_bytes=?,sha256=?,staged_version=0 "
            "WHERE build_id=? AND task_id=? AND kind=? AND record_key=? AND ordinal=0",
            (b"x" * 4096, hashlib.sha256(b"x" * 4096).hexdigest(), *identity),
        ).close()
        case.db.execute(
            "INSERT INTO discovery_fact_segments VALUES(?,?,?,?,?,?,?,?)",
            (*identity, 1, b"x", hashlib.sha256(b"x").hexdigest(), 1),
        ).close()
        case.db.execute(
            "UPDATE discovery_fact_streams SET fragments=2,octets=4097,last_fragment_bytes=1 "
            "WHERE build_id=? AND task_id=? AND kind=? AND record_key=?",
            identity,
        ).close()
    with pytest.raises(FactSegmentConflict, match="Native grammar"):
        read(case)
