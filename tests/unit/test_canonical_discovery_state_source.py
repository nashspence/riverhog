from __future__ import annotations

import copy
import sqlite3
from contextlib import ExitStack
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor, ProvenanceRootDocument
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json
from riverhog_core import canonical_discovery_state_source_work as owner
from riverhog_core.canonical_discovery_archive_work import (
    ArchiveTaskKey,
    StaleArchiveTask,
    admit_archive_journal,
    apply_archive_descriptor,
    archive_metadata_progress,
    begin_archive_metadata,
    initialize_archive_work,
    prepare_archive_descriptor,
)
from riverhog_core.canonical_discovery_corpus_inputs import (
    CorpusSourceInputWork,
    begin_corpus_source_inputs,
    complete_corpus_source_inputs,
    initialize_corpus_source_inputs,
)
from riverhog_core.canonical_discovery_endpoint_index import (
    EndpointIndexWork,
    begin_endpoint_index,
    initialize_endpoint_index,
)
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_lookup import effective_journal_assertion
from riverhog_core.canonical_discovery_journal_selector_work import (
    PreparedJournalSelector,
    apply_journal_selector,
    journal_selector_reads,
)
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey, StaleJournalTask
from riverhog_core.canonical_discovery_outgoing_endpoints import next_assertion_endpoint
from riverhog_core.canonical_discovery_selector_work import (
    SelectorCapacity,
    initialize_selector_work,
    intern_selector,
    prepare_selector_page,
)
from riverhog_core.canonical_discovery_selectors import Selector
from riverhog_provenance import (
    JournalFrame,
    append_correction,
    assertion,
    assertion_reference,
    new_id,
    reference,
    validate_journal,
)
from riverhog_provenance_contracts import ExternalReference

from tests.support.canonical_discovery import native_scalar_fixture
from tests.support.canonical_discovery_work import open_journal_work, write
from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission
from tests.unit.test_canonical_discovery_corpus_inputs import commit_input
from tests.unit.test_canonical_discovery_endpoint_index import drive
from tests.unit.test_canonical_discovery_history_selectors import graph
from tests.unit.test_canonical_discovery_journal_extraction import run
from tests.unit.test_canonical_discovery_selector_work import progress

RESOLVE, APPLY = owner.state_input_selector, apply_journal_selector
BUDGET = 65536


def register_pair(db, primary, foreign_raw, foreign):
    ordered = sorted(((primary.native.journal_id, primary.raw), (foreign.journal_id, foreign_raw)))
    first_id, first_raw = ordered[0]
    reader, objects, _, _ = archive_reader_fixture(
        first_raw,
        first_id,
        additional_journals=tuple(ordered[1:]),
    )
    root = ProvenanceRootDocument.from_json_bytes(objects["provenance/root.json.age"])
    admission = discovery_archive_admission(root, total_bytes=7)
    key = ArchiveTaskKey("build", admission.identity, "worker")
    with write(db):
        begin_archive_metadata(db, key, admission)
    while not archive_metadata_progress(db, key).complete:
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    with write(db):
        begin_corpus_source_inputs(db, key, admission)
    return key, admission


@pytest.fixture(scope="module")
def state_originals(tmp_path_factory):
    foreign_fixture = native_scalar_fixture(0)
    with validate_journal(foreign_fixture.raw, catalog=foreign_fixture.catalog) as initial:
        doc = initial.frames[0].document
        state = doc["body"]["assertions"]["states"][0]
        who = doc["recorded_by_agent_id"]
        address = {
            "scope": "external",
            "journal_id": initial.journal_id,
            **assertion_reference(initial, state["assertion_id"]),
            "object_id": state["id"],
            "object_type": "state",
        }
    changed = copy.deepcopy(state)
    changed["assertion_id"] = new_id()
    foreign_raw = append_correction(
        foreign_fixture.raw,
        [{"entry": address["entry"], "assertion_id": address["assertion_id"]}],
        reason="renew the immutable State assertion",
        recorded_by_agent_id=who,
        assertions={"states": [changed]},
        catalog=foreign_fixture.catalog,
    )
    primary_fixture = native_scalar_fixture(0)
    with validate_journal(primary_fixture.raw, catalog=primary_fixture.catalog) as initial:
        doc = copy.deepcopy(initial.frames[0].document)
    primary_state = doc["body"]["assertions"]["states"][0]
    derivation = assertion(
        "derivation",
        doc["recorded_by_agent_id"],
        used_state=address,
        generated_state=reference(primary_state["id"], "state"),
        kind="copy",
    )
    doc["body"]["assertions"]["relations"] = [derivation]
    primary_raw = JournalFrame(canonical_json_bytes(doc)).encoded
    with ExitStack() as stack:
        native = stack.enter_context(validate_journal(primary_raw, catalog=primary_fixture.catalog))
        foreign = stack.enter_context(
            validate_journal(foreign_raw, catalog=foreign_fixture.catalog)
        )
        path = tmp_path_factory.mktemp("actual-state-sources") / "source.sqlite3"
        db = open_journal_work(path)
        stack.callback(db.close)
        db.execute("PRAGMA foreign_keys=ON").close()
        initialize_archive_work(db)
        initialize_corpus_source_inputs(db)
        initialize_selector_work(db)
        primary = SimpleNamespace(raw=primary_raw, native=native)
        key, admission = register_pair(db, primary, foreign_raw, foreign)
        sources = []
        for summary in (native, foreign):
            source = stack.enter_context(admit_archive_journal(db, key, admission, summary))
            commit_input(db, key, CorpusSourceInputWork(admission, source))
            jkey = JournalTaskKey(key.build_id, source.source_key, key.worker_id)
            run(JournalFactExtraction(db, jkey, source), 128)
            sources.append(source)
        with write(db):
            manifest = complete_corpus_source_inputs(db, key, admission)
        assert manifest.source_count == 2 and admission.root.journal_count == 2
        initialize_endpoint_index(db)
        anchor = HistoryJournalAnchor.from_mapping(native.anchor)
        case = SimpleNamespace(
            db=db,
            path=path,
            archive_key=key,
            admission=admission,
            manifest=manifest,
            source=sources[0],
            foreign_source=sources[1],
            native=native,
            foreign_native=foreign,
            key=JournalTaskKey(key.build_id, sources[0].source_key, key.worker_id),
            anchor=anchor,
            state=state,
            replacement=changed,
            primary_state=primary_state,
            reference=ExternalReference.model_validate(address),
            derivation=derivation,
        )
        case.local_reference = ExternalReference.model_validate(
            case.source.reference_for_assertion(
                primary_state["assertion_id"],
                object_id=primary_state["id"],
                object_type="state",
            )
        )
        case.work = EndpointIndexWork(case.source, anchor)
        with write(db):
            begin_endpoint_index(db, case.key, case.work)
        drive(case)
        yield case


def copied(original, path):
    db = sqlite3.connect(path, isolation_level=None)
    db.execute("PRAGMA foreign_keys=ON").close()
    original.db.backup(db)
    case = SimpleNamespace(**vars(original))
    case.db, case.path = db, path
    case.source = admit_archive_journal(db, case.archive_key, case.admission, case.native)
    case.foreign_source = admit_archive_journal(
        db,
        case.archive_key,
        case.admission,
        case.foreign_native,
    )
    case.work = EndpointIndexWork(case.source, case.anchor)
    return case


@pytest.fixture
def case(state_originals, tmp_path):
    selected = copied(state_originals, tmp_path / "state.sqlite3")
    try:
        yield selected
    finally:
        selected.source.close()
        selected.foreign_source.close()
        selected.db.close()


def compiler(case, *, foreign=True, budget=BUDGET):
    return owner.StateSelectorWork(
        case.archive_key,
        case.admission,
        case.manifest,
        case.work,
        octet_budget=budget,
        foreign=CorpusSourceInputWork(case.admission, case.foreign_source) if foreign else None,
    )


def seed(case):
    spec = Selector(
        "state",
        source_key=case.source.source_key,
        anchor=case.anchor,
        object_id=case.primary_state["id"],
        scope="member",
    )
    with write(case.db):
        intern_selector(case.db, case.archive_key, spec, octet_budget=BUDGET)
    return progress(SimpleNamespace(db=case.db, key=case.archive_key), spec)


def prepare(case, work, before):
    reads = journal_selector_reads(case.db, case.archive_key, work, before)
    relation = effective_journal_assertion(
        reads,
        work.journal_key,
        work.original.source,
        case.anchor,
        case.derivation["id"],
    )
    assert relation is not None and relation.kind == "derivation"
    after = None
    while True:
        endpoint = next_assertion_endpoint(
            reads,
            work.journal_key,
            work.endpoints,
            case.anchor,
            relation,
            octet_budget=work.octet_budget,
            after=after,
        )
        assert endpoint is not None
        if endpoint.edge.role == "used_state":
            break
        after = endpoint.record_key
    assert endpoint.edge.exact_foreign_reference is not None
    address = ExternalReference.model_validate(
        require_canonical_json(endpoint.edge.exact_foreign_reference)
    )
    assert address == case.reference
    child = RESOLVE(reads, case.archive_key, work, address)
    page = prepare_selector_page(work, before, position=("input",), output=child, complete=True)
    prepared = PreparedJournalSelector(page, tuple(reads.receipts))
    work._prepared_journal = prepared
    return prepared


def reopen(case):
    case.source.close()
    case.foreign_source.close()
    case.db.close()
    case.db = sqlite3.connect(case.path, isolation_level=None)
    case.db.execute("PRAGMA foreign_keys=ON").close()
    case.source = admit_archive_journal(case.db, case.archive_key, case.admission, case.native)
    case.foreign_source = admit_archive_journal(
        case.db,
        case.archive_key,
        case.admission,
        case.foreign_native,
    )
    case.work = EndpointIndexWork(case.source, case.anchor)
    if hasattr(case, "additional_sources"):
        case.additional_sources = (case.foreign_source,)


def test_foreign_original_state_uses_its_origin_prefix_not_its_later_correction(case, monkeypatch):
    work, before = compiler(case), seed(case)
    for source in (case.source, case.foreign_source):
        monkeypatch.setattr(source, "entry", lambda *a, **k: pytest.fail("entry reconstructed"))
        monkeypatch.setattr(
            source, "assertion", lambda *a, **k: pytest.fail("assertion reconstructed")
        )
    prepared = prepare(case, work, before)
    child = prepared.page.output
    assert child.kind == "state" and child.scope == "input-history"
    assert child.source_key == case.foreign_source.source_key
    assert child.anchor == HistoryJournalAnchor.from_mapping(case.foreign_native.anchor_at(0))
    assert child.anchor.through_json_sha256 != child.anchor.prefix_sha256
    assert child.anchor != HistoryJournalAnchor.from_mapping(case.foreign_native.anchor)
    jkey = JournalTaskKey(case.archive_key.build_id, child.source_key, case.archive_key.worker_id)
    latest = effective_journal_assertion(
        case.db,
        jkey,
        case.foreign_source,
        HistoryJournalAnchor.from_mapping(case.foreign_native.anchor),
        case.state["id"],
    )
    assert latest.assertion_id == case.replacement["assertion_id"] != case.reference.assertion_id
    with write(case.db):
        assert APPLY(case.db, case.archive_key, work, prepared) == prepared.page.result
        assert APPLY(case.db, case.archive_key, work, prepared) == prepared.page.result
    reopen(case)
    assert graph(case)[1] == {(before.selector, child)}


def test_same_journal_address_needs_no_foreign_lease(case):
    work = compiler(case, foreign=False)
    selected = RESOLVE(case.db, case.archive_key, work, case.local_reference)
    assert selected.source_key == case.source.source_key
    assert selected.object_id == case.primary_state["id"]
    assert selected.anchor == case.anchor


def test_missing_foreign_is_a_structured_prerequisite_without_progress(case):
    before, work = seed(case), compiler(case, foreign=False)
    snapshot = tuple(case.db.iterdump())
    with pytest.raises(owner.StateSourceRequired) as caught:
        prepare(case, work, before)
    assert caught.value.journal_id == case.reference.journal_id
    assert tuple(case.db.iterdump()) == snapshot
    prepared = prepare(case, compiler(case), before)
    assert prepared.page.output.object_id == case.reference.object_id


@pytest.mark.parametrize(
    "field,value",
    (
        ("journal_id", "urn:uuid:11111111-1111-4111-8111-111111111111"),
        ("assertion_id", "urn:uuid:22222222-2222-4222-8222-222222222222"),
        ("object_id", "urn:uuid:33333333-3333-4333-8333-333333333333"),
        ("object_type", "artifact"),
        ("entry.entry_id", "urn:uuid:44444444-4444-4444-8444-444444444444"),
        ("entry.sequence", "1"),
        ("entry.json_sha256", "0" * 64),
    ),
)
def test_every_exact_reference_coordinate_is_checked(case, field, value):
    address = case.reference.model_dump(mode="json")
    if field.startswith("entry."):
        address["entry"][field.split(".")[1]] = value
    else:
        address[field] = value
    changed = ExternalReference.model_validate(address)
    with pytest.raises((ValueError, TypeError, owner.StateSourceRequired)):
        RESOLVE(case.db, case.archive_key, compiler(case), changed)


def test_untyped_reference_and_active_transaction_are_rejected(case):
    work = compiler(case)
    with pytest.raises(TypeError, match="typed"):
        RESOLVE(case.db, case.archive_key, work, case.reference.model_dump(mode="json"))
    with write(case.db), pytest.raises(ValueError, match="transaction"):
        RESOLVE(case.db, case.archive_key, work, case.reference)


@pytest.mark.parametrize("field", ("worker_id", "build_id", "admission_sha256"))
def test_changed_archive_attempt_is_rejected(case, field):
    work = compiler(case)
    changed = replace(case.archive_key, **{field: "f" * 64})
    with pytest.raises(StaleArchiveTask):
        RESOLVE(case.db, changed, work, case.reference)


@pytest.mark.parametrize("damage", ("close", "binding", "replace", "admission"))
def test_foreign_lease_must_remain_exact_and_open_through_apply(case, damage):
    work, before = compiler(case), seed(case)
    prepared = prepare(case, work, before)
    snapshot = tuple(case.db.iterdump())
    if damage == "close":
        case.foreign_source.close()
    elif damage == "binding":
        case.foreign_source.binding_json = "{}"
    elif damage == "replace":
        work.foreign = CorpusSourceInputWork(case.admission, case.source)
    else:
        work.foreign.admission = SimpleNamespace(identity="f" * 64)
    with write(case.db), pytest.raises((StaleArchiveTask, StaleJournalTask)):
        APPLY(case.db, case.archive_key, work, prepared)
    assert tuple(case.db.iterdump()) == snapshot


@pytest.mark.parametrize(
    "table,column",
    (
        ("discovery_corpus_source_inputs", "source_binding_json"),
        ("discovery_journal_tasks", "binding_json"),
        ("discovery_journal_assertions", "kind"),
    ),
)
def test_changed_foreign_sql_controls_roll_back_incidence_and_cursor(case, table, column):
    work, before = compiler(case), seed(case)
    prepared = prepare(case, work, before)
    case.db.execute(
        "UPDATE " + table + " SET " + column + "=? WHERE build_id=? AND source_key=?",
        ("changed original", case.archive_key.build_id, case.foreign_source.source_key),
    ).close()
    snapshot = tuple(case.db.iterdump())
    with write(case.db), pytest.raises(StaleArchiveTask, match="source changed"):
        APPLY(case.db, case.archive_key, work, prepared)
    assert tuple(case.db.iterdump()) == snapshot


def test_source_missing_from_frozen_manifest_is_not_accepted(case):
    case.db.execute(
        "DELETE FROM discovery_corpus_source_inputs WHERE build_id=? AND source_key=?",
        (case.archive_key.build_id, case.foreign_source.source_key),
    ).close()
    with pytest.raises(StaleArchiveTask, match="manifest"):
        RESOLVE(case.db, case.archive_key, compiler(case), case.reference)


def test_capacity_interruption_retry_and_cold_reconstruction_keep_exact_edge(case):
    before = seed(case)
    with pytest.raises(SelectorCapacity):
        prepare(case, compiler(case, budget=32), before)
    work = compiler(case)
    prepared = prepare(case, work, before)
    case.db.execute("BEGIN IMMEDIATE").close()
    APPLY(case.db, case.archive_key, work, prepared)
    case.db.rollback()
    assert progress(SimpleNamespace(db=case.db, key=case.archive_key), before.selector) == before
    reopen(case)
    work = compiler(case)
    prepared = prepare(case, work, before)
    with write(case.db):
        APPLY(case.db, case.archive_key, work, prepared)
    assert graph(case)[1] == {(before.selector, prepared.page.output)}


def test_interrupted_apply_rolls_back_both_new_child_and_incidence_inside_owner_transaction(case):
    work, before = compiler(case), seed(case)
    prepared = prepare(case, work, before)
    snapshot = tuple(case.db.iterdump())

    def deny(action, table, column, database, trigger):
        if action == sqlite3.SQLITE_UPDATE and table == "discovery_selectors":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    with write(case.db):
        case.db.set_authorizer(deny)
        try:
            with pytest.raises(sqlite3.DatabaseError):
                APPLY(case.db, case.archive_key, work, prepared)
        finally:
            case.db.set_authorizer(None)
    assert tuple(case.db.iterdump()) == snapshot
    with write(case.db):
        APPLY(case.db, case.archive_key, work, prepared)
    assert graph(case)[1] == {(before.selector, prepared.page.output)}
