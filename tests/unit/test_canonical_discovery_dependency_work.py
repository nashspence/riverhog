from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import ExitStack, closing
from dataclasses import replace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor, ProvenanceRootDocument
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_corpus_inputs as inputs
from riverhog_core import canonical_discovery_dependency_work as owner
from riverhog_core import canonical_discovery_prefix_inputs as prefixes
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
from riverhog_core.canonical_discovery_dependency_source import initialize_dependency_source
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey, QualifiedJournalSource
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    append_checkpoint,
    append_correction,
    assertion,
    create_journal,
    external_reference,
    reference,
    validate_journal,
)

from tests.support.canonical_discovery import native_scalar_fixture
from tests.support.canonical_discovery_work import MeasuredConnection, metadata_octets, write
from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission
from tests.unit.test_canonical_discovery_corpus_inputs import commit_input


@pytest.fixture(scope="module")
def originals():
    fixture = native_scalar_fixture(1)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as first:
        endpoint = external_reference(first, first.states[0]["id"])
        who = first.frames[0].document["recorded_by_agent_id"]
    parent_raw = append_checkpoint(
        fixture.raw,
        recorded_by_agent_id=who,
        purpose="urn:test:parent",
        catalog=fixture.catalog,
    )
    with validate_journal(parent_raw, catalog=fixture.catalog) as parent:
        fork = parent.anchor_at(1)
    observed = BoundedSourceObserver(catalog=fixture.catalog).observe(BytesSource(b"child"))
    early = assertion(
        "extension",
        observed.observer_agent_id,
        subject=reference(observed.artifact_id, "artifact"),
        property="urn:test:early",
        value={"type": "reference", "value": endpoint},
    )
    graph = observed.graph_fragment()
    graph["extensions"] = [early]
    child_raw = create_journal(
        graph,
        recorded_by_agent_id=observed.observer_agent_id,
        forked_from=fork,
        catalog=fixture.catalog,
    )
    with validate_journal(child_raw, catalog=fixture.catalog) as child:
        retirement = {"entry": child.anchor_at(0)["through"], "assertion_id": early["assertion_id"]}
    late = assertion(
        "extension",
        observed.observer_agent_id,
        subject=reference(observed.artifact_id, "artifact"),
        property="urn:test:late",
        value={"type": "reference", "value": endpoint},
    )
    child_raw = append_correction(
        child_raw,
        [retirement],
        reason="retain original documentary coordinates",
        recorded_by_agent_id=observed.observer_agent_id,
        assertions={"extensions": [late]},
        catalog=fixture.catalog,
    )
    child_raw = append_checkpoint(
        child_raw,
        recorded_by_agent_id=observed.observer_agent_id,
        purpose="urn:test:child",
        catalog=fixture.catalog,
    )
    with validate_journal(parent_raw, catalog=fixture.catalog) as parent:
        with validate_journal(child_raw, catalog=fixture.catalog) as child:
            yield parent_raw, child_raw, parent, child


@pytest.fixture(scope="module")
def qualified(tmp_path_factory, originals):
    parent_raw, child_raw, parent, child = originals
    path = tmp_path_factory.mktemp("dependency-work") / "qualified.sqlite"
    db = sqlite3.connect(path, isolation_level=None)
    initialize_archive_work(db)
    initialize_dependency_source(db)
    prefixes.initialize_corpus_prefix_inputs(db)
    journals = sorted(((child.journal_id, child_raw), (parent.journal_id, parent_raw)))
    reader, objects, _, _ = archive_reader_fixture(
        journals[0][1], journals[0][0], additional_journals=tuple(journals[1:])
    )
    root = ProvenanceRootDocument.from_json_bytes(objects["provenance/root.json.age"])
    admission = discovery_archive_admission(root)
    key = ArchiveTaskKey("build", admission.identity, "worker")
    with write(db):
        begin_archive_metadata(db, key, admission)
    while not archive_metadata_progress(db, key).complete:
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    with write(db):
        inputs.begin_corpus_source_inputs(db, key, admission)
    for native in (parent, child):
        with admit_archive_journal(db, key, admission, native) as source:
            commit_input(db, key, inputs.CorpusSourceInputWork(admission, source))
            driver = JournalFactExtraction(
                db, JournalTaskKey(key.build_id, source.source_key, key.worker_id), source
            )
            while not driver.advance(action_budget=128).parent.complete:
                pass
    with write(db):
        manifest = inputs.complete_corpus_source_inputs(db, key, admission)
        prefixes.begin_corpus_prefix_inputs(db, key, admission, manifest)
    db.close()
    return path, key, admission, manifest


@pytest.fixture
def owned(tmp_path, qualified, originals):
    original, key, admission, manifest = qualified
    _, _, parent, child = originals
    path = tmp_path / "work.sqlite"
    with closing(sqlite3.connect(original)) as source:
        db = sqlite3.connect(path, isolation_level=None)
        source.backup(db)
    with ExitStack() as leases:
        works = {}
        for native in (parent, child):
            source = leases.enter_context(admit_archive_journal(db, key, admission, native))
            works[native.journal_id] = prefixes.CorpusPrefixWork(admission, manifest, source)
        try:
            yield db, key, admission, manifest, works, parent.journal_id, child.journal_id, path
        finally:
            db.close()


def request(db, key, work, sequence):
    anchor = HistoryJournalAnchor.from_mapping(work.original.source.anchor_at(sequence))
    page = prefixes.prepare_corpus_prefix(db, key, work, anchor)
    with write(db):
        return prefixes.apply_corpus_prefix(db, key, work, page)


def prepare(db, key, work, works):
    return owner.prepare_corpus_dependency(
        db, key, work, resolve=works.__getitem__, octet_budget=65536
    )


def run(db, key, admission, manifest, works, *, on_step=None):
    events = []
    while (current := owner.next_corpus_dependency(db, key, admission, manifest)) is not None:
        work = owner.CorpusDependencyWork(works[current.maximum.anchor.journal_id])
        page = prepare(db, key, work, works)
        with write(db):
            result = owner.apply_corpus_dependency(db, key, work, page)
        events.append((current.maximum.source_key, page.action, current.active))
        if on_step is not None:
            on_step(work, page, result)
    return events


def test_real_fork_documentary_inputs_and_retired_assertions_share_exact_original_prefixes(owned):
    db, key, admission, manifest, works, parent, child, _ = owned
    assert owner.next_corpus_dependency(db, key, admission, manifest) is None
    assert request(db, key, works[child], 2).pending == 1
    events = run(db, key, admission, manifest, works)
    assert sum(action == "fork" for _, action, _ in events) == 2
    assert sum(action == "reference" for _, action, _ in events) == 2
    progress = prefixes.corpus_prefix_progress(db, key, admission, manifest)
    assert (progress.prefixes, progress.journals, progress.pending) == (3, 2, 0)
    parent_progress = owner.corpus_dependency_progress(
        db, key, admission, manifest, works[parent].original.source_key
    )
    assert parent_progress.fork_complete
    assert parent_progress.scanned_through == 1
    assert not parent_progress.maximum.pending
    assert parent_progress.active is None and parent_progress.after_node is None
    assert run(db, key, admission, manifest, works) == []


def test_higher_prefix_reuses_completed_fork_and_original_assertion_positions(owned, monkeypatch):
    db, key, admission, manifest, works, _, child, _ = owned
    request(db, key, works[child], 0)
    initial = run(db, key, admission, manifest, works)
    assert sum(action == "reference" for _, action, _ in initial) == 1
    before = owner.corpus_dependency_progress(
        db, key, admission, manifest, works[child].original.source_key
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("completed original fork was replayed")

    monkeypatch.setattr(QualifiedJournalSource, "fork_anchor", forbidden)
    assert request(db, key, works[child], 2).pending == 1
    resumed = owner.corpus_dependency_progress(
        db, key, admission, manifest, works[child].original.source_key
    )
    assert resumed.after == before.after and resumed.version == before.version
    later = run(db, key, admission, manifest, works)
    assert sum(action == "fork" for _, action, _ in later) == 0
    assert sum(action == "reference" for _, action, _ in later) == 1
    assert request(db, key, works[child], 1).pending == 0
    assert owner.next_corpus_dependency(db, key, admission, manifest) is None


def test_child_registration_and_parent_checkpoint_roll_back_together_and_retry_once(owned):
    db, key, admission, manifest, works, _, child, _ = owned
    request(db, key, works[child], 0)
    work = owner.CorpusDependencyWork(works[child])
    page = prepare(db, key, work, works)
    assert page.action == "fork" and page.dependency is not None
    before = prefixes.corpus_prefix_progress(db, key, admission, manifest)
    db.execute(
        "CREATE TRIGGER fail_dependency BEFORE UPDATE ON discovery_corpus_dependency_cursors "
        "BEGIN SELECT RAISE(ABORT,'injected dependency cursor failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected dependency"):
        owner.apply_corpus_dependency(db, key, work, page)
    assert prefixes.corpus_prefix_progress(db, key, admission, manifest) == before
    assert (
        owner.corpus_dependency_progress(
            db, key, admission, manifest, work.source.original.source_key
        )
        == page.expected
    )
    db.execute("DROP TRIGGER fail_dependency")
    with write(db):
        result = owner.apply_corpus_dependency(db, key, work, page)
    changed = db.total_changes
    with write(db):
        assert owner.apply_corpus_dependency(db, key, work, page) == result
    assert db.total_changes == changed
    assert prefixes.corpus_prefix_progress(db, key, admission, manifest).prefixes == 2


def test_reopen_mid_original_node_preserves_cursor_without_replaying_fork(owned, monkeypatch):
    db, key, admission, manifest, works, _, child, path = owned
    request(db, key, works[child], 2)
    work = owner.CorpusDependencyWork(works[child])
    while True:
        page = prepare(db, key, work, works)
        with write(db):
            saved = owner.apply_corpus_dependency(db, key, work, page)
        if page.action == "reference":
            break
    assert saved.active is not None and saved.after_node is not None
    with closing(sqlite3.connect(path, isolation_level=None)) as reopened:
        assert (
            owner.corpus_dependency_progress(
                reopened, key, admission, manifest, work.source.original.source_key
            )
            == saved
        )

        def forbidden(*args, **kwargs):
            raise AssertionError("saved fork replayed")

        monkeypatch.setattr(work.source.original.source, "fork_anchor", forbidden)
        replacement = owner.CorpusDependencyWork(work.source)
        page = prepare(reopened, key, replacement, works)
        assert page.action == "assertion-done"
        with write(reopened):
            owner.apply_corpus_dependency(reopened, key, replacement, page)


def test_retry_of_completed_terminal_preserves_a_later_reactivated_maximum(owned):
    db, key, admission, manifest, works, _, child, _ = owned
    request(db, key, works[child], 0)
    retained = []

    def keep(work, page, result):
        if page.action == "source-done" and work.source is works[child]:
            retained.append((work, page, result))

    run(db, key, admission, manifest, works, on_step=keep)
    work, page, completed = retained[0]
    assert not completed.maximum.pending
    request(db, key, works[child], 2)
    with write(db):
        retried = owner.apply_corpus_dependency(db, key, work, page)
    assert retried.version == completed.version
    assert retried.maximum.pending and retried.maximum.anchor.through_sequence == 2
    assert prefixes.corpus_prefix_progress(db, key, admission, manifest).pending == 1
    later = run(db, key, admission, manifest, works)
    assert sum(action == "reference" for _, action, _ in later) == 1


def test_new_maximum_invalidates_prepared_terminal_without_losing_original_position(owned):
    db, key, admission, manifest, works, _, child, _ = owned
    request(db, key, works[child], 0)
    work = owner.CorpusDependencyWork(works[child])
    while True:
        page = prepare(db, key, work, works)
        if page.action == "source-done":
            break
        with write(db):
            owner.apply_corpus_dependency(db, key, work, page)
    request(db, key, works[child], 2)
    with write(db), pytest.raises(StaleArchiveTask, match="position or maximum"):
        owner.apply_corpus_dependency(db, key, work, page)
    assert (
        owner.corpus_dependency_progress(
            db, key, admission, manifest, work.source.original.source_key
        ).after
        == page.expected.after
    )
    run(db, key, admission, manifest, works)


@pytest.mark.parametrize("field", ["worker_id", "build_id", "admission_sha256"])
def test_current_owner_and_fixed_manifest_are_required(owned, field):
    db, key, admission, manifest, works, _, child, _ = owned
    request(db, key, works[child], 0)
    value = "0" * 64 if field == "admission_sha256" else "other"
    with pytest.raises(StaleArchiveTask):
        owner.next_corpus_dependency(db, replace(key, **{field: value}), admission, manifest)


@pytest.mark.parametrize("fault", ["counter", "queue", "cursor", "position"])
def test_corrupt_queue_or_cursor_does_not_report_exhaustion(owned, fault):
    db, key, admission, manifest, works, _, child, _ = owned
    request(db, key, works[child], 0)
    if fault == "counter":
        db.execute("UPDATE discovery_corpus_prefix_headers SET pending_sources='0'")
    elif fault == "queue":
        db.execute("UPDATE discovery_corpus_prefix_maxima SET pending=0")
    elif fault == "cursor":
        db.execute("DELETE FROM discovery_corpus_dependency_cursors")
    else:
        db.execute("UPDATE discovery_corpus_dependency_cursors SET after_node='1'")
    with pytest.raises(StaleArchiveTask):
        owner.next_corpus_dependency(db, key, admission, manifest)


def test_missing_target_or_closed_source_leaves_pending_obligation_unchanged(owned):
    db, key, admission, manifest, works, _, child, _ = owned
    request(db, key, works[child], 0)
    work = owner.CorpusDependencyWork(works[child])
    before = owner.corpus_dependency_progress(
        db, key, admission, manifest, work.source.original.source_key
    )
    with pytest.raises(KeyError):
        prepare(db, key, work, {})
    assert (
        owner.corpus_dependency_progress(
            db, key, admission, manifest, work.source.original.source_key
        )
        == before
    )
    work.source.original.source.close()
    with pytest.raises(RuntimeError):
        prepare(db, key, work, works)


def test_wrong_target_source_cannot_advance_the_original_fork(owned):
    db, key, admission, manifest, works, _, child, _ = owned
    request(db, key, works[child], 0)
    work = owner.CorpusDependencyWork(works[child])
    before = owner.corpus_dependency_progress(
        db, key, admission, manifest, work.source.original.source_key
    )
    with pytest.raises(StaleArchiveTask):
        owner.prepare_corpus_dependency(
            db, key, work, resolve=lambda _journal: works[child], octet_budget=65536
        )
    assert (
        owner.corpus_dependency_progress(
            db, key, admission, manifest, work.source.original.source_key
        )
        == before
    )


def test_preparation_and_effects_keep_their_transaction_boundaries(owned):
    db, key, admission, manifest, works, _, child, _ = owned
    request(db, key, works[child], 0)
    work = owner.CorpusDependencyWork(works[child])
    page = prepare(db, key, work, works)
    with write(db), pytest.raises(ValueError, match="no active"):
        prepare(db, key, work, works)
    with pytest.raises(ValueError, match="owner-managed"):
        owner.apply_corpus_dependency(db, key, work, page)
    assert prefixes.corpus_prefix_progress(db, key, admission, manifest).prefixes == 1


@pytest.mark.parametrize("fault", ["scanned", "version", "receipt", "child", "fork"])
def test_committed_cursor_fields_and_child_prefix_must_match_the_exact_receipt(owned, fault):
    db, key, admission, manifest, works, _, child, _ = owned
    request(db, key, works[child], 0)
    work = owner.CorpusDependencyWork(works[child])
    page = prepare(db, key, work, works)
    with write(db):
        owner.apply_corpus_dependency(db, key, work, page)
    identity = (key.build_id, key.admission_sha256, work.source.original.source_key)
    clause = " WHERE build_id=? AND admission_sha256=? AND source_key=?"
    if fault == "child":
        db.execute(
            "DELETE FROM discovery_corpus_prefix_inputs WHERE source_key=?",
            (page.dependency.source_key,),
        )
    else:
        field, value = {
            "scanned": ("scanned_through", "0"),
            "version": ("version", "99"),
            "receipt": ("receipt_json", "{}"),
            "fork": ("fork_complete", 0),
        }[fault]
        db.execute(
            "UPDATE discovery_corpus_dependency_cursors SET " + field + "=?" + clause,
            (value, *identity),
        )
    with pytest.raises(StaleArchiveTask):
        owner.corpus_dependency_progress(
            db, key, admission, manifest, work.source.original.source_key
        )


def test_saved_assertion_identity_rejects_nontext_without_coercing_it(owned):
    db, key, admission, manifest, works, _, child, _ = owned
    request(db, key, works[child], 0)
    work = owner.CorpusDependencyWork(works[child])
    while True:
        page = prepare(db, key, work, works)
        with write(db):
            result = owner.apply_corpus_dependency(db, key, work, page)
        if page.action == "assertion":
            break
    raw = json.loads(owner._assertion_json(result.active))
    raw["task_id"] = 17
    db.execute(
        "UPDATE discovery_corpus_dependency_cursors SET active_json=? WHERE source_key=?",
        (canonical_json_bytes(raw).decode(), work.source.original.source_key),
    )
    with pytest.raises(StaleArchiveTask, match="text"):
        owner.corpus_dependency_progress(
            db, key, admission, manifest, work.source.original.source_key
        )


def measured_queue_operation(mode, owned, monkeypatch, record_property):
    import riverhog_core.canonical_discovery_journal_work as journal_work
    import riverhog_provenance.journal as native_journal
    import riverhog_provenance_contracts.codec as native_codec
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    db, key, admission, manifest, works, _, child, path = owned
    request(db, key, works[child], 0)
    operation = {
        "next": owner.next_corpus_dependency,
        "progress": owner.corpus_dependency_progress,
        "prepare": owner.prepare_corpus_dependency,
        "apply": owner.apply_corpus_dependency,
    }[mode]
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    metadata = max(
        sum(map(metadata_octets, row))
        for table in (
            "discovery_archive_tasks",
            "discovery_archive_journals",
            "discovery_corpus_source_manifests",
            "discovery_corpus_source_inputs",
            "discovery_corpus_prefix_headers",
            "discovery_corpus_prefix_inputs",
            "discovery_corpus_prefix_maxima",
            "discovery_corpus_dependency_cursors",
        )
        for row in db.execute("SELECT * FROM " + table).fetchall()
    )
    initial = len(works[child].original.source._summary.frames[0].json_bytes)
    samples = []
    phases = (
        ("fork", "assertion", "reference", "assertion-done", "source-done")
        if mode in {"prepare", "apply"}
        else ("fork",)
    )
    regimes = (
        (width, reopen, phase)
        for width in (1, 17, 1025)
        for reopen in (False, True)
        for phase in phases
    )
    for width, reopen, phase in regimes:
        candidate_path = path.with_name(f"regime-{mode}-{phase}-{width}-{reopen}.sqlite")
        candidate = sqlite3.connect(candidate_path, isolation_level=None)
        db.backup(candidate)
        work = owner.CorpusDependencyWork(works[child])
        page = None
        if mode in {"prepare", "apply"}:
            while True:
                page = prepare(candidate, key, work, works)
                if page.action == phase:
                    break
                with write(candidate):
                    owner.apply_corpus_dependency(candidate, key, work, page)
        tables = [
            row[0]
            for row in candidate.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'discovery_%'"
            ).fetchall()
        ]
        metadata_tables = (
            "discovery_archive_tasks",
            "discovery_archive_journals",
            "discovery_journal_tasks",
            "discovery_journal_assertions",
            "discovery_fact_tasks",
            "discovery_corpus_source_manifests",
            "discovery_corpus_source_inputs",
            "discovery_corpus_prefix_headers",
            "discovery_corpus_prefix_inputs",
            "discovery_corpus_prefix_maxima",
            "discovery_corpus_dependency_cursors",
        )
        metadata = max(
            metadata,
            max(
                sum(map(metadata_octets, row))
                for table in metadata_tables
                for row in candidate.execute("SELECT * FROM " + table).fetchall()
            ),
        )
        with write(candidate):
            for index in range(width):
                # Isolated dense-index controls are intentionally corrupt
                # completed rows. None may be selected or decoded.
                padding = hashlib.sha256(f"completed-{index}".encode()).hexdigest()
                candidate.execute(
                    "INSERT INTO discovery_corpus_prefix_maxima VALUES (?,?,?,?,?,?,0)",
                    (
                        key.build_id,
                        key.admission_sha256,
                        padding,
                        "unreadable binding " * 100,
                        "unreadable anchor",
                        "0",
                    ),
                )
        if reopen:
            candidate.close()
            candidate = sqlite3.connect(candidate_path, isolation_level=None)
        try:
            if page is not None:
                metadata = max(metadata, len(page.receipt_json.encode()))
            registered = sum(
                candidate.execute("SELECT count(*) FROM " + table).fetchone()[0] for table in tables
            )
            recorder = MeasuredConnection(candidate)
            recorder.metrics.update(
                fork_calls=0, assertion_calls=0, reference_calls=0, target_calls=0
            )
            actual_execute = recorder.execute

            def execute(sql, parameters=(), recorder=recorder, actual=actual_execute):
                if sql.lstrip().startswith(("INSERT", "UPDATE")):
                    recorder.metrics["written_octets"] += sum(map(metadata_octets, parameters))
                return actual(sql, parameters)

            recorder.execute = execute
            actual_hash = hashlib.sha256
            actual_encode = canonical_json_bytes
            actual_fork = QualifiedJournalSource.fork_anchor
            actual_assertion = owner.next_source_assertion
            actual_reference = owner.next_documentary_reference
            actual_document = native_journal.canonical_document
            actual_codec_document = native_codec.canonical_document

            def hashed(value=b"", recorder=recorder, actual=actual_hash, **kwargs):
                recorder.metrics["hashed_octets"] += len(value)
                return actual(value, **kwargs)

            def encode(value, recorder=recorder, actual=actual_encode):
                result = actual(value)
                recorder.metrics["encoded_octets"] += len(result)
                return result

            def document(value, recorder=recorder, actual=actual_document):
                result = actual(value)
                recorder.metrics["encoded_octets"] += len(result)
                return result

            def codec_document(value, recorder=recorder, actual=actual_codec_document):
                result = actual(value)
                recorder.metrics["encoded_octets"] += len(result)
                return result

            def fork(source, recorder=recorder, actual=actual_fork):
                recorder.metrics["fork_calls"] += 1
                return actual(source)

            def assertion_read(*args, recorder=recorder, actual=actual_assertion, **kwargs):
                recorder.metrics["assertion_calls"] += 1
                return actual(*args, **kwargs)

            def reference_read(*args, recorder=recorder, actual=actual_reference, **kwargs):
                recorder.metrics["reference_calls"] += 1
                return actual(*args, **kwargs)

            def resolve(journal_id, recorder=recorder, works=works):
                recorder.metrics["target_calls"] += 1
                return works[journal_id]

            def tick(recorder=recorder):
                recorder.metrics["sqlite_vm_instructions"] += 1
                return 0

            if mode == "apply":
                candidate.execute("BEGIN IMMEDIATE")
            candidate.set_progress_handler(tick, 1)
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(hashlib, "sha256", hashed)
                    for module in (owner, prefixes, inputs, journal_work):
                        patch.setattr(module, "canonical_json_bytes", encode)
                    patch.setitem(
                        owner.require_canonical_json.__globals__, "canonical_json_bytes", encode
                    )
                    patch.setattr(native_journal, "canonical_document", document)
                    patch.setattr(native_codec, "canonical_document", codec_document)
                    patch.setattr(QualifiedJournalSource, "fork_anchor", fork)
                    patch.setattr(owner, "next_source_assertion", assertion_read)
                    patch.setattr(owner, "next_documentary_reference", reference_read)
                    if mode == "next":
                        selected = operation(recorder, key, admission, manifest)
                        assert selected.maximum.source_key == work.source.original.source_key
                    elif mode == "progress":
                        selected = operation(
                            recorder, key, admission, manifest, work.source.original.source_key
                        )
                        assert selected.version == 0
                    elif mode == "prepare":
                        selected = operation(
                            recorder, key, work, resolve=resolve, octet_budget=65536
                        )
                        assert selected.action == phase
                        if phase in {"fork", "reference"}:
                            assert selected.dependency is not None
                    else:
                        selected = operation(recorder, key, work, page)
                        assert selected.fork_complete
            finally:
                candidate.set_progress_handler(None, 0)
                if mode == "apply":
                    candidate.commit()
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": {
                    "registered_inputs": registered,
                    "metadata_octets": metadata,
                    "initial_atom_octets": initial if mode == "prepare" and phase == "fork" else 0,
                },
                "metrics": {
                    bound.metric: recorder.metrics[bound.metric]
                    for bound in binding.declaration.bounds
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
        finally:
            candidate.close()
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-dependency-next/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_dependency_work.next_corpus_dependency",
)
def test_measured_dependency_next(owned, monkeypatch, record_property):
    measured_queue_operation("next", owned, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-dependency-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_dependency_work.corpus_dependency_progress",
)
def test_measured_dependency_progress(owned, monkeypatch, record_property):
    measured_queue_operation("progress", owned, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-dependency-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_dependency_work.prepare_corpus_dependency",
)
def test_measured_dependency_prepare(owned, monkeypatch, record_property):
    measured_queue_operation("prepare", owned, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-dependency-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_dependency_work.apply_corpus_dependency",
)
def test_measured_dependency_apply(owned, monkeypatch, record_property):
    measured_queue_operation("apply", owned, monkeypatch, record_property)
