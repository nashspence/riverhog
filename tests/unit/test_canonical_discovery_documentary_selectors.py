from __future__ import annotations

import copy
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.canonical_discovery_archive_work import (
    StaleArchiveTask,
    admit_archive_journal,
    initialize_archive_work,
)
from riverhog_core.canonical_discovery_corpus_inputs import (
    CorpusSourceInputWork,
    complete_corpus_source_inputs,
    initialize_corpus_source_inputs,
)
from riverhog_core.canonical_discovery_documentary_selectors import prepare_documentary_dependencies
from riverhog_core.canonical_discovery_endpoint_index import (
    EndpointIndexWork,
    begin_endpoint_index,
    initialize_endpoint_index,
)
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey, StaleJournalTask
from riverhog_core.canonical_discovery_rows import core_edges
from riverhog_core.canonical_discovery_selector_work import (
    SelectorCapacity,
    initialize_selector_work,
    intern_selector,
)
from riverhog_core.canonical_discovery_selectors import Selector
from riverhog_provenance import (
    JournalFrame,
    ProvenanceValidationError,
    append_correction,
    assertion,
    assertion_reference,
    new_id,
    reference,
    validate_journal,
)

from tests.support.canonical_discovery import native_profile_fixture
from tests.support.canonical_discovery_work import open_journal_work, write
from tests.unit.test_canonical_discovery_activity_selectors import APPLY, BUDGET, compiler, current
from tests.unit.test_canonical_discovery_corpus_inputs import commit_input, register_archive
from tests.unit.test_canonical_discovery_endpoint_index import drive
from tests.unit.test_canonical_discovery_history_selectors import graph
from tests.unit.test_canonical_discovery_journal_extraction import run

PREPARE = prepare_documentary_dependencies
ABSENT = "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"


@pytest.fixture(scope="module")
def documentary_originals(tmp_path_factory):
    lexical_context = new_id()
    fixture = native_profile_fixture(
        {"source_context_id": lexical_context},
        {"type": "object", "additionalProperties": {"type": "string"}},
    )
    with validate_journal(fixture.raw, catalog=fixture.catalog) as initial:
        document = copy.deepcopy(initial.frames[0].document)
    who = document["recorded_by_agent_id"]
    rows = document["body"]["assertions"]
    contexts = [assertion("context", who, kind="opaque", label=str(i)) for i in range(2)]
    contexts.append(assertion("context", who, object_id=lexical_context, kind="opaque"))
    rows.setdefault("contexts", []).extend(contexts)
    template = next(row for row in rows["descriptions"] if row["type"] == "observation")
    observation = copy.deepcopy(template)
    observation.update(id=new_id(), assertion_id=new_id(), source_context_id=contexts[0]["id"])
    reported = assertion(
        "reported_description",
        who,
        state=copy.deepcopy(template["state"]),
        source_context_id=contexts[1]["id"],
        content=copy.deepcopy(template["content"]),
    )
    plain = assertion(
        "reported_description",
        who,
        state=copy.deepcopy(template["state"]),
        note="x" * 12000,
    )
    other_state = assertion(
        "state",
        who,
        occurrence_id=rows["occurrences"][0]["id"],
        extent={"kind": "unknown", "reason": "independent documentary boundary"},
    )
    rows["states"].append(other_state)
    other = assertion(
        "reported_description",
        who,
        state=reference(other_state["id"], "state"),
        source_context_id=contexts[2]["id"],
    )
    rows["descriptions"].extend((observation, reported, plain, other))
    activity = copy.deepcopy(rows["activities"][0])
    activity.update(id=new_id(), assertion_id=new_id(), kind="creation")
    rows["activities"].append(activity)
    rows.setdefault("relations", []).append(
        assertion(
            "usage",
            who,
            activity_id=activity["id"],
            state=copy.deepcopy(template["state"]),
            role="urn:test:documentary-usage",
        )
    )
    raw = JournalFrame(canonical_json_bytes(document)).encoded
    with validate_journal(raw, catalog=fixture.catalog) as first:
        target = assertion_reference(first, observation["assertion_id"])
    replacement = copy.deepcopy(observation)
    replacement["assertion_id"] = new_id()
    raw = append_correction(
        raw,
        [target],
        reason="renew the same documentary observation",
        recorded_by_agent_id=who,
        assertions={"descriptions": [replacement]},
        catalog=fixture.catalog,
    )
    with validate_journal(raw, catalog=fixture.catalog) as second:
        target = assertion_reference(second, replacement["assertion_id"])
    future = assertion(
        "reported_description",
        who,
        state=copy.deepcopy(template["state"]),
        source_context_id=contexts[2]["id"],
    )
    raw = append_correction(
        raw,
        [target],
        reason="withdraw the independent observation and record another report",
        recorded_by_agent_id=who,
        assertions={"descriptions": [future]},
        catalog=fixture.catalog,
    )
    with validate_journal(raw, catalog=fixture.catalog) as native:
        path = tmp_path_factory.mktemp("actual-documentary-source") / "source.sqlite3"
        db = open_journal_work(path)
        db.execute("PRAGMA foreign_keys=ON").close()
        initialize_archive_work(db)
        initialize_corpus_source_inputs(db)
        initialize_selector_work(db)
        archive_key, admission = register_archive(db, raw, native)
        with admit_archive_journal(db, archive_key, admission, native) as source:
            commit_input(db, archive_key, CorpusSourceInputWork(admission, source))
            with write(db):
                manifest = complete_corpus_source_inputs(db, archive_key, admission)
            key = JournalTaskKey(archive_key.build_id, source.source_key, archive_key.worker_id)
            run(JournalFactExtraction(db, key, source), 128)
            initialize_endpoint_index(db)
            original_rows = [
                (sequence, row)
                for sequence, frame in enumerate(native.frames)
                for group in frame.document["body"].get("assertions", {}).values()
                for row in group
            ]
            case = SimpleNamespace(
                db=db,
                path=path,
                key=key,
                source=source,
                native=native,
                catalog=fixture.catalog,
                anchor=HistoryJournalAnchor.from_mapping(native.anchor),
                archive_key=archive_key,
                admission=admission,
                manifest=manifest,
                observation=observation,
                reported=reported,
                plain=plain,
                future=future,
                state=template["state"]["object_id"],
                artifact=rows["artifacts"][0]["id"],
                contexts=contexts,
                other=other,
                original_rows=original_rows,
            )
            case.work = EndpointIndexWork(source, case.anchor)
            with write(db):
                begin_endpoint_index(db, key, case.work)
            drive(case)
            try:
                yield case
            finally:
                case.db.close()


@pytest.fixture
def case(documentary_originals, tmp_path):
    original = documentary_originals
    path = tmp_path / "documentary.sqlite3"
    db = sqlite3.connect(path, isolation_level=None)
    db.execute("PRAGMA foreign_keys=ON").close()
    original.db.backup(db)
    selected = SimpleNamespace(**vars(original))
    selected.db, selected.path = db, path
    selected.source = admit_archive_journal(
        db, original.archive_key, original.admission, original.native
    )
    selected.work = EndpointIndexWork(selected.source, selected.anchor)
    try:
        yield selected
    finally:
        selected.source.close()
        selected.db.close()


def seed(case, kind="observed", sequence=0, scope="member", identity=None):
    spec = Selector(
        kind,
        source_key=case.source.source_key,
        anchor=HistoryJournalAnchor.from_mapping(case.native.anchor_at(sequence)),
        object_id=identity or (case.state if kind == "observed" else case.observation["id"]),
        scope=scope,
    )
    with write(case.db):
        intern_selector(case.db, case.archive_key, spec, octet_budget=BUDGET)
    return spec


def expected(case, spec):
    def matches(row):
        return (
            row["id"] == spec.object_id
            if spec.kind == "own"
            else (
                row["type"] in {"observation", "reported_description"}
                and row["state"] == reference(spec.object_id, "state")
            )
        )

    return {
        replace(spec, kind="direct", object_id=row["source_context_id"])
        for sequence, row in case.original_rows
        if sequence <= spec.anchor.through_sequence
        and matches(row)
        and row["type"] in {"observation", "reported_description"}
        and "source_context_id" in row
    }


def atom_count(case, spec):
    if spec.kind == "own":
        return 1
    return 1 + sum(
        (edge.role, edge.target_scope, edge.target_type, edge.target_id)
        == ("state", "local", "state", spec.object_id)
        for _, row in case.original_rows
        for edge in core_edges(row)
    )


def reopen(case):
    case.db.close()
    case.db = sqlite3.connect(case.path, isolation_level=None)
    case.db.execute("PRAGMA foreign_keys=ON").close()
    case.work = EndpointIndexWork(case.source, case.anchor)


def drain(case, spec):
    work, pages = compiler(case), []
    while not (before := current(case, spec)).complete:
        prepared = PREPARE(case.db, case.archive_key, work, before)
        pages.append(prepared)
        with write(case.db):
            assert APPLY(case.db, case.archive_key, work, prepared) == prepared.page.result
            assert APPLY(case.db, case.archive_key, work, prepared) == prepared.page.result
        reopen(case)
        work = compiler(case)
    return pages


@pytest.mark.parametrize("selected", ("observed", "observation", "reported", "plain", "artifact"))
@pytest.mark.parametrize("sequence", (0, 1, 2))
@pytest.mark.parametrize("scope", ("member", "input-history", "recorded-history"))
def test_original_documentary_dependencies_preserve_withdrawals_and_exclude_future_claims(
    case, monkeypatch, selected, sequence, scope
):
    identity = (
        None
        if selected == "observed"
        else (case.artifact if selected == "artifact" else getattr(case, selected)["id"])
    )
    spec = seed(case, "observed" if selected == "observed" else "own", sequence, scope, identity)
    contexts, atoms = expected(case, spec), atom_count(case, spec)

    def forbidden(*args, **kwargs):
        pytest.fail("documentary dependencies reconstructed an entry or assertion payload")

    monkeypatch.setattr(case.source, "entry", forbidden)
    monkeypatch.setattr(case.source, "assertion", forbidden)
    monkeypatch.setattr(JournalFrame, "document", property(forbidden))
    pages = drain(case, spec)
    nodes, edges = graph(case)
    assert edges == {(spec, context) for context in contexts}
    assert len(nodes) == 1 + len(contexts)
    assert len(pages) == atoms
    if selected == "observed":
        ids = {item.object_id for item in contexts}
        assert {item["id"] for item in case.contexts[:2]} <= ids
        assert (case.contexts[2]["id"] in ids) == (sequence == 2)
        assert any(not page.page.result.complete and page.page.output is None for page in pages)


@pytest.mark.parametrize("kind", ("own", "observed"))
@pytest.mark.parametrize("damage", ("blob", "manifest", "index"))
def test_documentary_preparation_cannot_commit_changed_original_controls(case, kind, damage):
    spec = seed(case, kind)
    work, before = compiler(case), current(case, spec)
    prepared = PREPARE(case.db, case.archive_key, work, before)
    if damage == "blob":
        while not any('"blob"' in row.exact_json for row in prepared.sources):
            assert not prepared.page.result.complete
            with write(case.db):
                APPLY(case.db, case.archive_key, work, prepared)
            before = current(case, spec)
            prepared = PREPARE(case.db, case.archive_key, work, before)
    with write(case.db):
        if damage == "blob":
            receipt = next(row for row in prepared.sources if '"blob"' in row.exact_json)
            original = case.db.execute(receipt.sql, receipt.parameters).fetchone()
            assert type(original[4]) is bytes
            case.db.execute(
                "UPDATE discovery_fact_segments SET exact_bytes=? WHERE build_id=? AND task_id=? "
                "AND kind=? AND record_key=? AND ordinal=?",
                (b"!" * len(original[4]), *receipt.parameters[1:]),
            ).close()
        elif damage == "manifest":
            case.db.execute("UPDATE discovery_corpus_source_manifests SET complete=0").close()
        else:
            case.db.execute("UPDATE discovery_endpoint_tasks SET phase='streams'").close()
    with write(case.db), pytest.raises(StaleArchiveTask, match="changed after preparation"):
        APPLY(case.db, case.archive_key, work, prepared)
    assert current(case, spec) == before


@pytest.mark.parametrize("kind", ("own", "observed"))
def test_documentary_source_capacity_rollback_forged_preparation_and_closed_lease(case, kind):
    missing = seed(case, kind, identity=ABSENT)
    assert len(drain(case, missing)) == 1 and not graph(case)[1]
    spec = seed(case, kind)
    before = current(case, spec)
    with pytest.raises(SelectorCapacity):
        PREPARE(case.db, case.archive_key, compiler(case, 32), before)
    work = compiler(case)
    prepared = PREPARE(case.db, case.archive_key, work, before)
    with write(case.db), pytest.raises(StaleArchiveTask, match="current exact preparation"):
        APPLY(case.db, case.archive_key, work, replace(prepared))
    case.db.execute("BEGIN IMMEDIATE").close()
    APPLY(case.db, case.archive_key, work, prepared)
    case.db.rollback()
    assert current(case, spec) == before and not graph(case)[1]
    with write(case.db):
        APPLY(case.db, case.archive_key, work, prepared)
    drain(case, spec)
    case.source.close()
    with pytest.raises(StaleJournalTask):
        PREPARE(case.db, case.archive_key, work, before)


@pytest.mark.parametrize("kind", ("own", "observed"))
@pytest.mark.parametrize("damage", ("phase", "version", "receipt", "original"))
def test_documentary_saved_cursor_is_its_original_claim_and_context(case, kind, damage):
    spec = seed(case, kind)
    work = compiler(case)
    prepared = PREPARE(case.db, case.archive_key, work, current(case, spec))
    with write(case.db):
        APPLY(case.db, case.archive_key, work, prepared)
    before = current(case, spec)
    candidate = replace(before, complete=False)
    if damage == "phase":
        broken = replace(candidate, position=("other",))
    elif damage == "version":
        broken = replace(candidate, version=0)
    elif damage == "receipt":
        broken = replace(candidate, last_output_json='{"changed":true}')
    elif kind == "own":
        broken = replace(candidate, position=("own", case.other["assertion_id"]))
    else:
        record = next(
            str(i) for i, edge in enumerate(core_edges(case.other)) if edge.role == "state"
        )
        broken = replace(candidate, position=("observed", case.other["assertion_id"], record))
    with pytest.raises(StaleArchiveTask):
        PREPARE(case.db, case.archive_key, work, broken)
    assert current(case, spec) == before


@pytest.mark.parametrize("kind", ("own", "observed"))
@pytest.mark.parametrize("damage", ("source", "prefix", "worker"))
def test_documentary_dependencies_keep_source_prefix_and_worker_fences(case, kind, damage):
    spec = seed(case, kind)
    work = compiler(case)
    key = case.archive_key
    if damage == "source":
        spec = replace(spec, source_key="0" * 64)
    elif damage == "prefix":
        spec = replace(spec, anchor=replace(spec.anchor, prefix_sha256="0" * 64))
    else:
        key = replace(key, worker_id="other")
    with write(case.db):
        intern_selector(case.db, case.archive_key, spec, octet_budget=BUDGET)
    with pytest.raises((StaleArchiveTask, StaleJournalTask)):
        PREPARE(case.db, key, work, current(case, spec))


def test_documentary_source_context_cannot_skip_a_missing_original_endpoint(case):
    spec = seed(case, "own")
    work, before = compiler(case), current(case, spec)
    record = next(
        str(i)
        for i, edge in enumerate(core_edges(case.observation))
        if edge.role == "source_context_id"
    )
    with write(case.db):
        case.db.execute(
            "DELETE FROM discovery_endpoints WHERE build_id=? AND source_key=? "
            "AND assertion_id=? AND record_key=?",
            (case.key.build_id, case.key.source_key, case.observation["assertion_id"], record),
        ).close()
    with pytest.raises(StaleJournalTask, match="original fact directory"):
        PREPARE(case.db, case.archive_key, work, before)
    assert current(case, spec) == before


def test_documentary_preparation_rejects_an_active_transaction_and_another_kind(case):
    spec = seed(case)
    work, before = compiler(case), current(case, spec)
    with write(case.db), pytest.raises(ValueError, match="transaction"):
        PREPARE(case.db, case.archive_key, work, before)
    with pytest.raises(StaleArchiveTask, match="fixed pending selector"):
        PREPARE(
            case.db, case.archive_key, work, replace(before, selector=replace(spec, kind="state"))
        )


@pytest.mark.parametrize("selected", ("observation", "reported"))
def test_native_immutable_context_justifies_one_own_dependency_atom(case, selected):
    row = getattr(case, selected)
    changed = copy.deepcopy(row)
    changed["assertion_id"] = new_id()
    changed["source_context_id"] = next(
        context["id"] for context in case.contexts if context["id"] != row["source_context_id"]
    )
    original = case.native.frames[0]
    target = assertion_reference(case.native, row["assertion_id"])
    with pytest.raises(ProvenanceValidationError, match="immutable"):
        append_correction(
            original.encoded,
            [target],
            recorded_by_agent_id=original.document["recorded_by_agent_id"],
            reason="attempt to change an immutable description context",
            assertions={"descriptions": [changed]},
            catalog=case.catalog,
        )
