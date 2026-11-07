from __future__ import annotations

import copy
import sqlite3
from collections import defaultdict
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json
from riverhog_core import canonical_discovery_endpoint_index as index
from riverhog_core import canonical_discovery_endpoint_matches as matches
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
from riverhog_provenance import (
    JournalFrame,
    append_assertion_batches,
    assertion,
    reference,
    validate_journal,
)
from riverhog_provenance_contracts import ExternalReference, LocalReference

from tests.support.canonical_discovery_work import (
    ADMISSION,
    corrected_journal_fixture,
    open_journal_work,
    write,
)
from tests.unit.test_canonical_discovery_endpoint_index import drive
from tests.unit.test_canonical_discovery_endpoint_source import mixed_originals as mixed_originals
from tests.unit.test_canonical_discovery_journal_extraction import run

MATCH = matches.advance_endpoint_match


def target_for(edge):
    if edge.target_scope == "external":
        return ExternalReference.model_validate(
            require_canonical_json(edge.exact_foreign_reference)
        )
    return LocalReference(object_id=edge.target_id, object_type=edge.target_type)


def original_edges(native):
    return [
        (row["assertion_id"], str(ordinal), edge)
        for frame in native.frames
        for group in frame.document["body"].get("assertions", {}).values()
        for row in group
        for ordinal, edge in enumerate(core_edges(row))
    ]


def initialize(case):
    index.initialize_endpoint_index(case.db)
    case.work = index.EndpointIndexWork(case.source, case.anchor)
    with write(case.db):
        index.begin_endpoint_index(case.db, case.key, case.work)
    drive(case)


@pytest.fixture(scope="module")
def matching_facts(mixed_originals, tmp_path_factory):
    original = mixed_originals
    document = copy.deepcopy(original.native.frames[0].document)
    graph, who = document["body"]["assertions"], document["recorded_by_agent_id"]
    second = assertion("context", who, kind="opaque", label="distinct original endpoint")
    graph["contexts"].append(second)
    graph["activities"][0]["contexts"].append(
        {"context_id": second["id"], "role": "other", "role_uri": "urn:test:second-context"}
    )
    foreign = copy.deepcopy(graph["extensions"][0]["value"]["value"])
    foreign["entry"]["sequence"] = "9223372036854775806"
    graph["extensions"].append(
        assertion(
            "extension",
            who,
            subject=graph["extensions"][0]["subject"],
            property="urn:test:another-documentary-reference",
            value={"type": "reference", "value": foreign},
        )
    )
    raw = JournalFrame(canonical_json_bytes(document)).encoded
    with validate_journal(raw, catalog=original.catalog) as native:
        with QualifiedJournalSource(native, ADMISSION) as source:
            path = tmp_path_factory.mktemp("match-original-facts") / "original.sqlite3"
            db = open_journal_work(path)
            try:
                key = JournalTaskKey("build", source.source_key, "worker")
                run(JournalFactExtraction(db, key, source), 128)
                yield SimpleNamespace(
                    db=db,
                    path=path,
                    key=key,
                    native=native,
                    anchor=HistoryJournalAnchor.from_mapping(native.anchor),
                    assertion=graph["extensions"][0]["assertion_id"],
                    canonical=canonical_json_bytes(graph["extensions"][0]),
                )
            finally:
                db.close()


@pytest.fixture(scope="module")
def matching_index(matching_facts, tmp_path_factory):
    original = matching_facts
    path = tmp_path_factory.mktemp("match-index") / "indexed.sqlite3"
    db = sqlite3.connect(path, isolation_level=None)
    original.db.backup(db)
    db.execute("PRAGMA foreign_keys=ON").close()
    with QualifiedJournalSource(original.native, ADMISSION) as source:
        case = SimpleNamespace(**vars(original), source=source)
        case.db, case.path = db, path
        try:
            initialize(case)
            yield case
        finally:
            db.close()


@pytest.fixture
def matching_case(matching_index, tmp_path):
    original = matching_index
    path = tmp_path / "match.sqlite3"
    db = sqlite3.connect(path, isolation_level=None)
    original.db.backup(db)
    db.execute("PRAGMA foreign_keys=ON").close()
    with QualifiedJournalSource(original.native, ADMISSION) as source:
        case = SimpleNamespace(
            **{k: v for k, v in vars(original).items() if k not in {"source", "work"}}
        )
        case.db, case.path, case.source = db, path, source
        case.work = index.EndpointIndexWork(source, case.anchor)
        try:
            yield case
        finally:
            case.db.close()


def collect(case, role, target, *, anchor=None, state="documentary", on_step=None):
    after, found, steps = None, [], []
    while True:
        step = MATCH(
            case.db,
            case.key,
            case.work,
            anchor or case.anchor,
            role=role,
            target=target,
            assertion_state=state,
            octet_budget=4096,
            after=after,
        )
        steps.append(step)
        if on_step:
            on_step(step)
        if step.complete:
            assert step.after == after and step.endpoint is None
            return found, steps
        assert step.after is not None and (after is None or step.after > after)
        after = step.after
        if step.endpoint is not None:
            found.append(step.endpoint)


def test_every_typed_target_matches_original_fields_without_assertion_reconstruction(
    matching_case, monkeypatch
):
    case = matching_case
    expected = original_edges(case.native)
    queries = defaultdict(list)
    for identity, record, edge in expected:
        target = target_for(edge)
        queries[(edge.role, target)].append((identity, record, edge))

    def forbidden(*args, **kwargs):
        pytest.fail("endpoint matching reconstructed an original assertion")

    monkeypatch.setattr(case.source, "entry", forbidden)
    monkeypatch.setattr(case.source, "assertion", forbidden)
    monkeypatch.setattr(JournalFrame, "document", property(forbidden))
    for (role, target), wanted in queries.items():
        found, _ = collect(case, role, target)
        assert [
            (item.assertion.assertion_id, item.record_key, item.edge) for item in found
        ] == sorted(wanted, key=lambda row: row[:2])


def test_context_selector_preserves_full_roles_and_returns_one_unmatched_candidate_at_a_time(
    matching_case,
):
    case = matching_case
    _, _, context = next(
        row for row in original_edges(case.native) if row[2].role == "context:source"
    )
    target = target_for(context)
    broad, broad_steps = collect(case, "context", target)
    exact, exact_steps = collect(case, "context:other", target)
    assert len(broad) == 5 and len(exact) == 1
    assert {item.edge.role for item in broad} == {
        "context:" + role for role in ("source", "destination", "execution", "recording", "other")
    }
    assert len(broad_steps) == len(exact_steps) == 6
    assert sum(step.endpoint is None and not step.complete for step in exact_steps) == 4


def test_cold_resume_keeps_the_same_candidate_position_and_original_role(matching_case):
    case = matching_case
    edge = next(row[2] for row in original_edges(case.native) if row[2].role == "context:source")
    target = target_for(edge)
    expected, _ = collect(case, "context", target)

    def reopen(_step):
        case.db.close()
        case.db = sqlite3.connect(case.path, isolation_level=None)
        case.db.execute("PRAGMA foreign_keys=ON").close()
        case.work = index.EndpointIndexWork(case.source, case.anchor)

    actual, _ = collect(case, "context", target, on_step=reopen)
    assert actual == expected


def test_collision_candidates_require_exact_local_ids_and_complete_foreign_references(
    matching_facts, tmp_path, monkeypatch
):
    original = matching_facts
    path = tmp_path / "collisions.sqlite3"
    db = sqlite3.connect(path, isolation_level=None)
    original.db.backup(db)
    db.execute("PRAGMA foreign_keys=ON").close()

    class Collision:
        def hexdigest(self):
            return "a" * 64

    fake_hash = SimpleNamespace(sha256=lambda raw: Collision())
    monkeypatch.setattr(matches, "hashlib", fake_hash)
    monkeypatch.setattr(endpoints, "hashlib", fake_hash)
    with QualifiedJournalSource(original.native, ADMISSION) as source:
        case = SimpleNamespace(**vars(original), source=source)
        case.db, case.path = db, path
        try:
            initialize(case)
            rows = original_edges(original.native)
            contexts = {target_for(edge) for _, _, edge in rows if edge.role.startswith("context:")}
            assert len(contexts) == 2
            for target in contexts:
                found, steps = collect(case, "context", target)
                assert found and all(item.edge.target_id == target.object_id for item in found)
                assert any(step.endpoint is None and not step.complete for step in steps)
            foreign = [target_for(edge) for _, _, edge in rows if edge.target_scope == "external"]
            assert len(foreign) == 2 and foreign[0].object_id == foreign[1].object_id
            for target in foreign:
                found, steps = collect(case, "value", target)
                assert len(found) == 1 and len(steps) == 3
                assert found[0].edge.exact_foreign_reference == canonical_json_bytes(
                    target.model_dump(mode="json")
                )
        finally:
            db.close()


@pytest.fixture(scope="module")
def interval_index(tmp_path_factory):
    fixture, raw, _ = corrected_journal_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as first:
        who = first.frames[0].document["recorded_by_agent_id"]
        artifact = first.graph["artifacts"][0]["id"]
    late = assertion(
        "extension",
        who,
        subject=reference(artifact, "artifact"),
        property="urn:test:future-endpoint",
        value={"type": "text", "value": "later"},
    )
    raw = append_assertion_batches(
        raw, ({"extensions": [late]},), recorded_by_agent_id=who, catalog=fixture.catalog
    )
    with validate_journal(raw, catalog=fixture.catalog) as native:
        with QualifiedJournalSource(native, ADMISSION) as source:
            path = tmp_path_factory.mktemp("endpoint-interval") / "interval.sqlite3"
            db = open_journal_work(path)
            case = SimpleNamespace(
                db=db,
                path=path,
                source=source,
                native=native,
                key=JournalTaskKey("build", source.source_key, "worker"),
                anchor=HistoryJournalAnchor.from_mapping(native.anchor),
                target=LocalReference(object_id=artifact, object_type="artifact"),
                old=fixture.assertion_id,
                late=late["assertion_id"],
            )
            try:
                run(JournalFactExtraction(db, case.key, source), 128)
                initialize(case)
                yield case
            finally:
                db.close()


def test_effective_and_documentary_matching_use_the_requested_original_prefix(
    interval_index, monkeypatch
):
    case = interval_index
    anchors = [HistoryJournalAnchor.from_mapping(case.native.anchor_at(i)) for i in range(4)]

    def forbidden(*args, **kwargs):
        pytest.fail("prefix matching reconstructed a snapshot or assertion")

    monkeypatch.setattr(case.source, "entry", forbidden)
    monkeypatch.setattr(case.source, "assertion", forbidden)
    monkeypatch.setattr(JournalFrame, "document", property(forbidden))
    for sequence, anchor in enumerate(anchors):
        documentary, documentary_steps = collect(case, "subject", case.target, anchor=anchor)
        effective, effective_steps = collect(
            case, "subject", case.target, anchor=anchor, state="effective"
        )
        assert {item.assertion.assertion_id for item in documentary} == (
            {case.old, case.late} if sequence == 3 else {case.old}
        )
        assert {item.assertion.assertion_id for item in effective} == (
            {case.old} if sequence == 0 else {case.late} if sequence == 3 else set()
        )
        assert len(documentary_steps) == len(effective_steps) == 3


@pytest.mark.parametrize(
    "field,value", (("born_sequence", "9"), ("retired_sequence", "0"), ("task_id", "missing"))
)
def test_candidate_interval_controls_are_verified_before_they_can_suppress_matches(
    matching_case, field, value
):
    case = matching_case
    identity, record, edge = next(
        row for row in original_edges(case.native) if row[2].target_scope == "external"
    )
    with write(case.db):
        case.db.execute(
            "UPDATE discovery_endpoints SET " + field + "=? WHERE assertion_id=? AND record_key=?",
            (value, identity, record),
        ).close()
    with pytest.raises(StaleJournalTask, match="original assertion interval"):
        collect(case, edge.role, target_for(edge), state="effective")


def test_changed_original_edge_or_missing_control_cannot_supply_a_match(matching_case):
    case = matching_case
    identity, record, edge = next(
        row for row in original_edges(case.native) if row[2].target_scope == "external"
    )
    case.db.execute("PRAGMA foreign_keys=OFF").close()
    with write(case.db):
        case.db.execute(
            "DELETE FROM discovery_fact_streams WHERE task_id=("
            "SELECT task_id FROM discovery_journal_assertions WHERE assertion_id=?) "
            "AND kind='edge-role' AND record_key=?",
            (identity, record),
        ).close()
    with pytest.raises(FactSegmentConflict):
        collect(case, edge.role, target_for(edge))


def test_capacity_empty_results_and_exact_source_boundaries(matching_case):
    case = matching_case
    edge = original_edges(case.native)[0][2]
    target = target_for(edge)
    with pytest.raises(JournalFactCapacity):
        MATCH(
            case.db,
            case.key,
            case.work,
            case.anchor,
            role=edge.role,
            target=target,
            assertion_state="documentary",
            octet_budget=1,
        )
    found, steps = collect(case, "no-such-role", target)
    assert found == [] and len(steps) == 1 and steps[0].complete
    with pytest.raises(StaleJournalTask):
        collect(case, "no-such-role", target, anchor=replace(case.anchor, prefix_sha256="f" * 64))
    with write(case.db):
        with pytest.raises(ValueError, match="transaction"):
            collect(case, edge.role, target)
    case.source.close()
    with pytest.raises(StaleJournalTask):
        collect(case, "no-such-role", target)
