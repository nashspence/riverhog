from __future__ import annotations

import copy
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_activity_selectors as owner
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
from riverhog_core.canonical_discovery_endpoint_index import (
    EndpointIndexWork,
    begin_endpoint_index,
    initialize_endpoint_index,
)
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_selector_work import (
    JournalSelectorWork,
    apply_journal_selector,
)
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey, StaleJournalTask
from riverhog_core.canonical_discovery_selector_work import (
    SelectorCapacity,
    initialize_selector_work,
    intern_selector,
)
from riverhog_core.canonical_discovery_selectors import Selector
from riverhog_provenance import (
    JournalFrame,
    append_correction,
    assertion,
    assertion_reference,
    new_id,
    validate_journal,
)

from tests.support.canonical_discovery import native_scalar_fixture
from tests.support.canonical_discovery_work import open_journal_work, write
from tests.unit.test_canonical_discovery_corpus_inputs import commit_input, register_archive
from tests.unit.test_canonical_discovery_endpoint_index import drive
from tests.unit.test_canonical_discovery_history_selectors import graph
from tests.unit.test_canonical_discovery_journal_extraction import run
from tests.unit.test_canonical_discovery_selector_work import progress

PREPARE, APPLY = owner.prepare_activity_selector, apply_journal_selector
BUDGET = 65536


@pytest.fixture(scope="module")
def activity_originals(tmp_path_factory):
    fixture = native_scalar_fixture(0)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as initial:
        document = copy.deepcopy(initial.frames[0].document)
    who = document["recorded_by_agent_id"]
    assertions = document["body"]["assertions"]
    contexts = [assertion("context", who, kind="opaque", label=str(i)) for i in range(2)]
    assertions.setdefault("contexts", []).extend(contexts)
    activity = copy.deepcopy(assertions["activities"][0])
    activity.update(
        id=new_id(),
        assertion_id=new_id(),
        kind="creation",
        contexts=[
            {"context_id": contexts[0]["id"], "role": "source"},
            {
                "context_id": contexts[0]["id"],
                "role": "other",
                "role_uri": "urn:test:" + "x" * 12000,
            },
            {"context_id": contexts[1]["id"], "role": "destination"},
        ],
    )
    assertions["activities"].append(activity)
    raw = JournalFrame(canonical_json_bytes(document)).encoded
    with validate_journal(raw, catalog=fixture.catalog) as first:
        target = assertion_reference(first, activity["assertion_id"])
    replacement = copy.deepcopy(activity)
    replacement["assertion_id"] = new_id()
    raw = append_correction(
        raw,
        [target],
        reason="renew the original immutable activity assertion",
        recorded_by_agent_id=who,
        assertions={"activities": [replacement]},
        catalog=fixture.catalog,
    )
    with validate_journal(raw, catalog=fixture.catalog) as second:
        target = assertion_reference(second, replacement["assertion_id"])
    raw = append_correction(
        raw,
        [target],
        reason="withdraw the independent activity",
        recorded_by_agent_id=who,
        catalog=fixture.catalog,
    )
    with validate_journal(raw, catalog=fixture.catalog) as native:
        path = tmp_path_factory.mktemp("actual-activity-source") / "source.sqlite3"
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
            case = SimpleNamespace(
                db=db,
                path=path,
                key=key,
                source=source,
                anchor=HistoryJournalAnchor.from_mapping(native.anchor),
                archive_key=archive_key,
                admission=admission,
                manifest=manifest,
                native=native,
                activity=activity,
                replacement=replacement,
                contexts=contexts,
                assertion=fixture.assertion_id,
            )
            case.work = EndpointIndexWork(source, case.anchor)
            with write(db):
                begin_endpoint_index(db, key, case.work)
            drive(case)
            try:
                yield case
            finally:
                db.close()


@pytest.fixture
def case(activity_originals, tmp_path):
    original = activity_originals
    path = tmp_path / "activity.sqlite3"
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


def compiler(case, budget=BUDGET):
    return JournalSelectorWork(
        case.archive_key, case.admission, case.manifest, case.work, octet_budget=budget
    )


def seed(case, sequence=0, scope="member", identity=None):
    selector = Selector(
        "activity",
        source_key=case.source.source_key,
        anchor=HistoryJournalAnchor.from_mapping(case.native.anchor_at(sequence)),
        object_id=identity or case.activity["id"],
        scope=scope,
    )
    with write(case.db):
        intern_selector(case.db, case.archive_key, selector, octet_budget=BUDGET)
    return selector


def current(case, selector):
    return progress(SimpleNamespace(db=case.db, key=case.archive_key), selector)


def drain(case, selector, *, cold=False):
    selected, pages = compiler(case), []
    while not (before := current(case, selector)).complete:
        page = PREPARE(case.db, case.archive_key, selected, before)
        pages.append(page)
        with write(case.db):
            assert APPLY(case.db, case.archive_key, selected, page) == page.page.result
            assert APPLY(case.db, case.archive_key, selected, page) == page.page.result
        if cold:
            case.db.close()
            case.db = sqlite3.connect(case.path, isolation_level=None)
            case.db.execute("PRAGMA foreign_keys=ON").close()
            case.work = EndpointIndexWork(case.source, case.anchor)
            selected = compiler(case)
    return pages


@pytest.mark.parametrize("sequence", (0, 1, 2))
@pytest.mark.parametrize("scope", ("member", "input-history", "recorded-history"))
def test_exact_native_activity_prefixes_lower_only_original_contexts(
    case, monkeypatch, sequence, scope
):
    selector = seed(case, sequence, scope)

    def forbidden(*args, **kwargs):
        pytest.fail("activity lowering reconstructed an entry or full assertion")

    monkeypatch.setattr(case.source, "entry", forbidden)
    monkeypatch.setattr(case.source, "assertion", forbidden)
    monkeypatch.setattr(JournalFrame, "document", property(forbidden))
    pages = drain(case, selector, cold=True)
    nodes, edges = graph(case)
    expected = set()
    if sequence < 2:
        expected.add(replace(selector, kind="own"))
        expected.update(
            replace(selector, kind="direct", object_id=context["id"]) for context in case.contexts
        )
    assert {target for source, target in edges if source == selector} == expected
    assert all(parent == selector for parent, _ in edges)
    assert len(nodes) == 1 + len(expected)
    if sequence == 0:
        assert any(not p.page.result.complete and p.page.output is None for p in pages)
        assert len(pages) > len(expected)


@pytest.mark.parametrize("damage", ("blob", "manifest", "index", "source"))
def test_changed_original_source_controls_cannot_commit_a_prepared_context(case, damage):
    selector = seed(case)
    selected = compiler(case)
    first = PREPARE(case.db, case.archive_key, selected, current(case, selector))
    with write(case.db):
        APPLY(case.db, case.archive_key, selected, first)
    before = current(case, selector)
    page = PREPARE(case.db, case.archive_key, selected, before)
    with write(case.db):
        if damage == "blob":
            receipt = next(r for r in page.sources if '"blob"' in r.exact_json)
            assert "discovery_fact_segments" in receipt.sql
            original = case.db.execute(receipt.sql, receipt.parameters).fetchone()
            assert type(original[4]) is bytes
            # Keep the stored digest and byte length unchanged: replay owns exact bytes.
            case.db.execute(
                "UPDATE discovery_fact_segments SET exact_bytes=? "
                "WHERE build_id=? AND task_id=? AND kind=? AND record_key=? AND ordinal=?",
                (b"!" * len(original[4]), *receipt.parameters[1:]),
            ).close()
        elif damage == "manifest":
            case.db.execute("UPDATE discovery_corpus_source_manifests SET complete=0").close()
        elif damage == "index":
            case.db.execute("UPDATE discovery_endpoint_tasks SET phase='streams'").close()
        else:
            case.db.execute("UPDATE discovery_journal_tasks SET complete=0").close()
    with write(case.db), pytest.raises(StaleArchiveTask, match="changed after preparation"):
        APPLY(case.db, case.archive_key, selected, page)
    assert current(case, selector) == before


def test_context_incidence_and_cursor_survive_rollback_retry_and_cold_reopen(case):
    selector = seed(case)
    selected = compiler(case)
    before = current(case, selector)
    page = PREPARE(case.db, case.archive_key, selected, before)
    with pytest.raises(StaleArchiveTask, match="current exact preparation"):
        with write(case.db):
            APPLY(case.db, case.archive_key, selected, replace(page))
    case.db.execute("BEGIN IMMEDIATE").close()
    APPLY(case.db, case.archive_key, selected, page)
    case.db.rollback()
    assert current(case, selector) == before and not graph(case)[1]
    with write(case.db):
        APPLY(case.db, case.archive_key, selected, page)
    drain(case, selector, cold=True)
    assert current(case, selector).complete


def test_missing_activity_capacity_and_owner_source_fences(case):
    missing = seed(case, identity="urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff")
    assert len(drain(case, missing)) == 1 and not graph(case)[1]
    selector = seed(case)
    with pytest.raises(SelectorCapacity):
        PREPARE(case.db, case.archive_key, compiler(case, 32), current(case, selector))
    with pytest.raises(StaleArchiveTask):
        PREPARE(
            case.db,
            replace(case.archive_key, worker_id="other"),
            compiler(case),
            current(case, selector),
        )
    selected = compiler(case)
    with write(case.db), pytest.raises(ValueError, match="transaction"):
        PREPARE(case.db, case.archive_key, selected, current(case, selector))
    case.source.close()
    with pytest.raises(StaleJournalTask):
        PREPARE(case.db, case.archive_key, selected, current(case, selector))


@pytest.mark.parametrize("damage", ("phase", "version", "receipt"))
def test_saved_activity_cursor_must_recheck_its_original_incidence(case, damage):
    spec = seed(case)
    work = compiler(case)
    first = PREPARE(case.db, case.archive_key, work, current(case, spec))
    with write(case.db):
        APPLY(case.db, case.archive_key, work, first)
    before = current(case, spec)
    broken = (
        replace(before, position=("other",))
        if damage == "phase"
        else replace(before, version=0)
        if damage == "version"
        else replace(before, last_output_json="null")
    )
    with pytest.raises(StaleArchiveTask):
        PREPARE(case.db, case.archive_key, work, broken)
    assert current(case, spec) == before


@pytest.mark.parametrize("damage", ("source", "prefix", "kind"))
def test_activity_preparation_keeps_exact_source_prefix_and_native_type(case, damage):
    spec = seed(case)
    if damage == "source":
        broken = replace(spec, source_key="0" * 64)
    elif damage == "prefix":
        broken = replace(spec, anchor=replace(spec.anchor, prefix_sha256="0" * 64))
    else:
        broken = replace(spec, object_id=case.contexts[0]["id"])
    with write(case.db):
        intern_selector(case.db, case.archive_key, broken, octet_budget=BUDGET)
    before = current(case, broken)
    with pytest.raises((StaleArchiveTask, StaleJournalTask)):
        PREPARE(case.db, case.archive_key, compiler(case), before)
    assert current(case, broken) == before


def test_missing_original_endpoint_cannot_become_activity_completion(case):
    spec = seed(case)
    work = compiler(case)
    first = PREPARE(case.db, case.archive_key, work, current(case, spec))
    with write(case.db):
        APPLY(case.db, case.archive_key, work, first)
        record = case.db.execute(
            "SELECT record_key FROM discovery_endpoints WHERE build_id=? AND source_key=? "
            "AND assertion_id=? ORDER BY record_key LIMIT 1",
            (case.key.build_id, case.key.source_key, case.activity["assertion_id"]),
        ).fetchone()[0]
        case.db.execute(
            "DELETE FROM discovery_endpoints WHERE build_id=? AND source_key=? "
            "AND assertion_id=? AND record_key=?",
            (case.key.build_id, case.key.source_key, case.activity["assertion_id"], record),
        ).close()
    before = current(case, spec)
    with pytest.raises(StaleJournalTask, match="original fact directory"):
        PREPARE(case.db, case.archive_key, work, before)
    assert current(case, spec) == before and not before.complete
