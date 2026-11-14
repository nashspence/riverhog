from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    RETAINED_HISTORY_EXTENT,
    ArchiveProvenanceIdentity,
    CollectionArchiveManifest,
    CollectionArtifactSetIdentity,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryDocument,
    MemberHistoryImport,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    ProvenanceRootDocument,
    ProvenanceRootIdentity,
    SourceMemberHistoryBindingProof,
    member_history_object_path,
    source_binding_proof_object_path,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_prefix_inputs as prefixes
from riverhog_core import canonical_discovery_root_work as roots
from riverhog_core import canonical_discovery_selection as owner
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask, admit_archive_journal
from riverhog_core.canonical_discovery_dependency_source import initialize_dependency_source
from riverhog_core.canonical_discovery_history_inputs import (
    CorpusImportWork,
    apply_corpus_import,
    next_corpus_history_input,
    prepare_corpus_import,
)
from riverhog_core.canonical_discovery_history_work import ArchiveHistoryWork, HistorySetWork
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey
from riverhog_provenance import validate_journal

from tests.support.canonical_discovery_work import corrected_journal_fixture, write
from tests.support.provenance_archive import archive_reader_fixture
from tests.unit.test_canonical_discovery_dependency_work import run as finish_dependencies
from tests.unit.test_canonical_discovery_history_inputs import structures
from tests.unit.test_canonical_discovery_history_work import record_set
from tests.unit.test_canonical_discovery_root_work import finish as finish_roots
from tests.unit.test_canonical_discovery_root_work import setup as setup_roots


@pytest.fixture(scope="module")
def original():
    fixture, raw, _ = corrected_journal_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as native:
        yield raw, native


def imported(original):
    raw, native = original
    anchors = tuple(HistoryJournalAnchor.from_mapping(native.anchor_at(i)) for i in range(3))
    objects = {}
    root_set = record_set(
        objects,
        MEMBER_HISTORY_ROOTS_SCHEMA,
        [
            {"key": value.key, "value": value.to_mapping()}
            for value in (
                MemberHistoryRoot(anchor, "bound" if i == 0 else "retained")
                for i, anchor in enumerate(anchors)
            )
        ],
    )
    empty = record_set(objects, MEMBER_HISTORY_IMPORTS_SCHEMA, [])
    state = "urn:uuid:33333333-3333-4333-8333-333333333333"
    history = MemberHistoryDocument(
        "e" * 64, 7, "a" * 64, MemberHistoryPrimary(anchors[0], state), root_set, empty
    )
    _, source_objects, _, _ = archive_reader_fixture(
        raw, native.journal_id, histories=(history,), history_objects=objects
    )
    source_root = ProvenanceRootDocument.from_json_bytes(source_objects["provenance/root.json.age"])
    archive = CollectionArchiveManifest(
        archive_generation=source_root.archive_generation,
        artifact_set=CollectionArtifactSetIdentity(1, 7, source_root.artifact_set_sha256),
        ordered_volume_sha256="f" * 64,
        provenance=ArchiveProvenanceIdentity(
            source_root.identity,
            ProvenanceRootIdentity(
                id="provenance-root",
                kind="provenance-root",
                path="provenance/root.json.age",
                plaintext_bytes=len(source_root.to_json_bytes()),
                sha256=source_root.identity,
                stored_bytes=1234,
                stored_sha256="0" * 64,
            ),
        ),
    )
    proof = SourceMemberHistoryBindingProof(
        "9" * 64,
        7,
        archive.to_json_bytes(),
        source_root.to_json_bytes(),
        MemberHistoryBinding(
            history.artifact_id, 7, history.sha256, history.identity, len(history.to_json_bytes())
        ),
        0,
        (),
    )
    value = MemberHistoryImport(
        proof.source_identity,
        proof.collection_id,
        hashlib.sha256(proof.archive_root).hexdigest(),
        source_root.artifact_set_sha256,
        history.artifact_id,
        history.identity,
        proof.identity,
        BOUND_HISTORY_EXTENT,
        {
            "scope": "external",
            "journal_id": anchors[0].journal_id,
            "entry": anchors[0].to_mapping()["through"],
            "assertion_id": "urn:uuid:22222222-2222-4222-8222-222222222222",
            "object_id": state,
            "object_type": "state",
        },
    )
    objects[source_binding_proof_object_path(proof.identity)] = proof.to_json_bytes()
    objects[member_history_object_path(history.identity)] = history.to_json_bytes()
    return (value,), objects


def setup(path, original, *, members=2, inherited=False, selections=None):
    imports, objects = imported(original) if inherited else ((), None)
    key, admission, reader, manifest, histories, anchors = setup_roots(
        path,
        original,
        composed_members=True,
        members=members,
        selected_roots=selections,
        history_imports=imports,
        extra_history_objects=objects,
    )
    db = sqlite3.connect(path, isolation_level=None)
    owner.initialize_corpus_selection(db)
    initialize_dependency_source(db)
    while True:
        structures(db, key, admission, reader)
        task = next_corpus_history_input(db, key, admission, kind="imports")
        if task is None:
            break
        work = CorpusImportWork(HistorySetWork(admission, reader, task.authority))
        while task.pending:
            page = prepare_corpus_import(db, key, work)
            with write(db):
                task = apply_corpus_import(db, key, work, page)
    with admit_archive_journal(db, key, admission, original[1]) as source:
        source_work = prefixes.CorpusPrefixWork(admission, manifest, source)
        works = {original[1].journal_id: source_work}
        extraction = JournalFactExtraction(
            db, JournalTaskKey(key.build_id, source.source_key, key.worker_id), source
        )
        while not extraction.advance(action_budget=128).parent.complete:
            pass
        while (task := next_corpus_history_input(db, key, admission, kind="roots")) is not None:
            finish_roots(
                db,
                key,
                roots.CorpusRootWork(HistorySetWork(admission, reader, task.authority), manifest),
                works,
            )
        finish_dependencies(db, key, admission, manifest, works)
    db.close()
    return key, admission, reader, manifest, histories, anchors


@pytest.fixture(scope="module")
def qualified(tmp_path_factory, original):
    path = tmp_path_factory.mktemp("selection") / "qualified.sqlite"
    return path, setup(path, original, inherited=True)


@pytest.fixture
def owned(tmp_path, qualified):
    initial, values = qualified
    key, admission, reader, manifest, histories, anchors = values
    path = tmp_path / "selection.sqlite"
    db = sqlite3.connect(path, isolation_level=None)
    with closing(sqlite3.connect(initial)) as source:
        source.backup(db)
    work = owner.CorpusSelectionWork(ArchiveHistoryWork(admission, reader), manifest)
    try:
        yield db, key, work, histories, anchors, path
    finally:
        db.close()


def begin(db, key, work):
    with write(db):
        return owner.begin_corpus_selection(db, key, work)


def advance(db, key, work):
    page = owner.prepare_corpus_selection(db, key, work)
    with write(db):
        return page, owner.apply_corpus_selection(db, key, work, page)


def finish(db, key, work):
    result = owner.corpus_selection_progress(db, key, work)
    actions = []
    while result.phase != "complete":
        page, result = advance(db, key, work)
        actions.append((page.expected.phase, page.position))
    return result, actions


def test_freeze_checks_original_members_imports_roots_and_exact_native_prefixes(owned):
    db, key, work, _, anchors, _ = owned
    initial = begin(db, key, work)
    assert initial.phase == "requirements" and initial.version == 0
    assert prefixes.corpus_prefix_progress(db, key, work.history.admission, work.sources).closed
    complete, actions = finish(db, key, work)
    assert [phase for phase, position in actions if position is None] == [
        "requirements",
        "members",
        "imports",
        "dependencies",
        "prefixes",
    ]
    counts = json.loads(complete.counts_json)
    assert counts == {
        "roots": "1",
        "imports": "2",
        "members": "2",
        "import_links": "1",
        "dependencies": "1",
        "prefixes": "3",
    }
    assert complete.required_import_links == 1
    frozen = owner.frozen_corpus_selection(db, key, work)
    assert frozen.sources == work.sources and frozen.selection_count == 3
    for anchor in anchors:
        assert owner.corpus_selection_admitted(db, key, work, frozen, anchor)
        assert not owner.corpus_selection_admitted(
            db, key, work, frozen, replace(anchor, prefix_bytes=anchor.prefix_bytes + 1)
        )
    assert not owner.corpus_selection_admitted(
        db,
        key,
        work,
        frozen,
        replace(anchors[0], journal_id="urn:uuid:11111111-1111-4111-8111-111111111111"),
    )


def test_freeze_hash_has_an_explicit_original_order_and_private_codec(owned):
    db, key, work, _, _, _ = owned
    begin(db, key, work)
    complete, _ = finish(db, key, work)
    digest = hashlib.sha256(
        canonical_json_bytes(
            {
                "format": "riverhog-discovery-corpus-selection/v1",
                "admission_sha256": work.history.admission.identity,
                "source_manifest_sha256": work.sources.manifest_sha256,
                "prefix_count": "3",
            }
        )
        + b"\n"
    )
    rows = db.execute(
        "SELECT p.anchor_json,i.source_binding_json FROM discovery_corpus_prefix_inputs AS p "
        "JOIN discovery_corpus_source_inputs AS i "
        "ON i.build_id=p.build_id AND i.admission_sha256=p.admission_sha256 "
        "AND i.source_key=p.source_key ORDER BY p.source_key,p.prefix_sha256"
    ).fetchall()
    for anchor, binding in rows:
        digest.update(
            canonical_json_bytes(
                {"source_binding": json.loads(binding), "anchor": json.loads(anchor)}
            )
            + b"\n"
        )
    assert complete.selection_sha256 == digest.hexdigest()


def test_reopen_each_phase_reuses_original_cursors_and_hash_state(owned):
    db, key, work, _, _, path = owned
    begin(db, key, work)
    while True:
        current = owner.corpus_selection_progress(db, key, work)
        if current.phase == "complete":
            break
        _, saved = advance(db, key, work)
        with closing(sqlite3.connect(path, isolation_level=None)) as reopened:
            fresh = owner.CorpusSelectionWork(
                ArchiveHistoryWork(work.history.admission, work.history.reader), work.sources
            )
            assert owner.corpus_selection_progress(reopened, key, fresh) == saved
    assert owner.frozen_corpus_selection(db, key, work).selection_count == 3


@pytest.mark.parametrize(
    "event,table",
    [
        ("INSERT", "discovery_corpus_selection"),
        ("UPDATE", "discovery_corpus_prefix_headers"),
    ],
)
def test_seal_and_independent_freeze_creation_rollback_together(owned, event, table):
    db, key, work, _, _, _ = owned
    db.execute(
        f"CREATE TRIGGER fail_seal BEFORE {event} ON {table} "
        "BEGIN SELECT RAISE(ABORT,'injected selection seal failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected selection"):
        begin(db, key, work)
    assert not prefixes.corpus_prefix_progress(db, key, work.history.admission, work.sources).closed
    assert db.execute("SELECT count(*) FROM discovery_corpus_selection").fetchone() == (0,)
    db.execute("DROP TRIGGER fail_seal")
    begin(db, key, work)


def test_prefix_input_and_maximum_cannot_advance_after_seal_even_if_prepared_before(
    tmp_path, original
):
    native = original[1]
    anchors = tuple(HistoryJournalAnchor.from_mapping(native.anchor_at(i)) for i in range(3))
    selections = (MemberHistoryRoot(anchors[0], "bound"), MemberHistoryRoot(anchors[2], "retained"))
    path = tmp_path / "seal-race.sqlite"
    key, admission, reader, manifest, _, _ = setup(path, original, selections=selections)
    with closing(sqlite3.connect(path, isolation_level=None)) as db:
        work = owner.CorpusSelectionWork(ArchiveHistoryWork(admission, reader), manifest)
        with admit_archive_journal(db, key, admission, native) as source:
            prefix = prefixes.CorpusPrefixWork(admission, manifest, source)
            page = prefixes.prepare_corpus_prefix(db, key, prefix, anchors[1])
            begin(db, key, work)
            with write(db), pytest.raises(StaleArchiveTask, match="sealed"):
                prefixes.apply_corpus_prefix(db, key, prefix, page)
            retry = prefixes.prepare_corpus_prefix(db, key, prefix, anchors[0])
            with write(db):
                assert prefixes.apply_corpus_prefix(db, key, prefix, retry).closed
        assert owner.corpus_selection_progress(db, key, work).version == 0


def test_saved_prefix_hash_cursor_and_last_receipt_rollback_as_one_advance(owned):
    db, key, work, _, _, _ = owned
    begin(db, key, work)
    while owner.corpus_selection_progress(db, key, work).phase != "prefixes":
        advance(db, key, work)
    before = owner.corpus_selection_progress(db, key, work)
    page = owner.prepare_corpus_selection(db, key, work)
    assert page.hash_state != before.hash_state
    db.execute(
        "CREATE TRIGGER fail_selection BEFORE UPDATE ON discovery_corpus_selection "
        "BEGIN SELECT RAISE(ABORT,'injected selection advance failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected selection"):
        owner.apply_corpus_selection(db, key, work, page)
    assert owner.corpus_selection_progress(db, key, work) == before
    db.execute("DROP TRIGGER fail_selection")
    with write(db):
        after = owner.apply_corpus_selection(db, key, work, page)
    assert json.loads(after.counts_json)["prefixes"] == "1"
    with write(db), pytest.raises(StaleArchiveTask, match="continuation changed"):
        owner.apply_corpus_selection(db, key, work, page)


@pytest.mark.parametrize(
    "damage",
    [
        "version",
        "counts",
        "hash",
        "receipt",
        "member",
        "import",
        "primary",
        "dependency",
        "seal",
    ],
)
def test_missing_original_inputs_or_changed_freeze_state_fail_closed(owned, damage):
    db, key, work, histories, _, _ = owned
    begin(db, key, work)
    if damage in ("version", "counts", "hash", "receipt"):
        advance(db, key, work)
        if damage == "version":
            db.execute("UPDATE discovery_corpus_selection SET version='100'")
        elif damage == "counts":
            db.execute(
                "UPDATE discovery_corpus_selection SET counts_json=?",
                (canonical_json_bytes(dict.fromkeys(owner._COUNTS, "0")).decode(),),
            )
        elif damage == "hash":
            db.execute("UPDATE discovery_corpus_selection SET hash_state='{}'")
        else:
            db.execute("UPDATE discovery_corpus_selection SET receipt_json=NULL")
    elif damage == "member":
        db.execute(
            "DELETE FROM discovery_archive_histories WHERE artifact_id=?",
            (histories[0].artifact_id,),
        )
    elif damage == "import":
        db.execute("DELETE FROM discovery_history_import_links")
    elif damage == "primary":
        primary = MemberHistoryRoot(histories[0].primary.journal, "bound")
        db.execute(
            "DELETE FROM discovery_history_set_records WHERE records_sha256=? AND record_key=?",
            (histories[0].roots.records_sha256, primary.key),
        )
    elif damage == "dependency":
        db.execute("DELETE FROM discovery_corpus_dependency_cursors")
    else:
        db.execute("UPDATE discovery_corpus_prefix_headers SET closed=0")
    with pytest.raises((StaleArchiveTask, ValueError)):
        finish(db, key, work)


@pytest.mark.parametrize("damage", ["members", "roots", "imports", "dependencies"])
def test_begin_requires_independent_seed_and_dependency_terminals(owned, damage):
    db, key, work, _, _, _ = owned
    if damage == "members":
        db.execute("UPDATE discovery_corpus_member_coverage SET members='0'")
    elif damage in ("roots", "imports"):
        db.execute(f"UPDATE discovery_corpus_history_headers SET pending_{damage}='1'")
    else:
        db.execute("UPDATE discovery_corpus_prefix_headers SET pending_sources='1'")
    with pytest.raises(StaleArchiveTask):
        begin(db, key, work)
    assert not prefixes.corpus_prefix_progress(db, key, work.history.admission, work.sources).closed


def test_exact_owner_worker_frozen_manifest_and_detached_preparation_are_required(owned):
    db, key, work, _, _, _ = owned
    begin(db, key, work)
    page = owner.prepare_corpus_selection(db, key, work)
    with write(db), pytest.raises(StaleArchiveTask, match="exact detached"):
        owner.apply_corpus_selection(db, key, work, replace(page))
    with pytest.raises(StaleArchiveTask):
        owner.corpus_selection_progress(db, replace(key, worker_id="other"), work)
    changed = owner.CorpusSelectionWork(
        work.history, replace(work.sources, manifest_sha256="f" * 64)
    )
    with pytest.raises(StaleArchiveTask):
        owner.corpus_selection_progress(db, key, changed)
    with write(db), pytest.raises(ValueError, match="no active"):
        owner.prepare_corpus_selection(db, key, work)
    with pytest.raises(StaleArchiveTask, match="independent freeze terminal"):
        owner.frozen_corpus_selection(db, key, work)


@pytest.fixture(scope="module")
def snapshots(tmp_path_factory, qualified):
    initial, values = qualified
    key, admission, reader, manifest, _, _ = values
    directory = tmp_path_factory.mktemp("selection-snapshots")
    db = sqlite3.connect(directory / "work.sqlite", isolation_level=None)
    with closing(sqlite3.connect(initial)) as source:
        source.backup(db)
    work = owner.CorpusSelectionWork(ArchiveHistoryWork(admission, reader), manifest)
    paths = {-1: initial}
    try:
        begin(db, key, work)
        while True:
            current = owner.corpus_selection_progress(db, key, work)
            target = directory / f"state-{current.version}.sqlite"
            with closing(sqlite3.connect(target)) as destination:
                db.backup(destination)
            paths[current.version] = target
            if current.phase == "complete":
                break
            advance(db, key, work)
    finally:
        db.close()
    assert max(paths) == 15
    return paths, values


def _observe(db, monkeypatch, reader):
    from contextlib import contextmanager

    @contextmanager
    def scope():
        import sys

        import sha256_continuation
        from riverhog_canonical_json import require_canonical_json
        from sha256_continuation import CheckpointSHA256

        from tests.support.canonical_discovery_work import observe_metadata

        update = CheckpointSHA256.update_bytes
        export = CheckpointSHA256.export_state
        compress = sha256_continuation.sha256_compress
        with observe_metadata(db, monkeypatch, reader=reader) as recorder:
            recorder.metrics.update(decoded_octets=0, sha256_block_octets=0)
            with monkeypatch.context() as patch:

                def decode(raw):
                    recorder.metrics["decoded_octets"] += len(raw)
                    return require_canonical_json(raw)

                def hash_bytes(self, raw):
                    recorder.metrics["hashed_octets"] += len(raw)
                    return update(self, raw)

                def encode_state(self):
                    result = export(self)
                    recorder.metrics["encoded_octets"] += len(result.encode())
                    return result

                def block(state, raw):
                    recorder.metrics["sha256_block_octets"] += len(raw)
                    return compress(state, raw)

                patch.setattr(CheckpointSHA256, "update_bytes", hash_bytes)
                patch.setattr(CheckpointSHA256, "export_state", encode_state)
                patch.setattr(sha256_continuation, "sha256_compress", block)
                for module in tuple(sys.modules.values()):
                    if module is not None and getattr(module, "__name__", "").startswith(
                        (
                            "riverhog_archive_contracts",
                            "riverhog_canonical_json",
                            "riverhog_core.canonical_discovery",
                            "riverhog_core.provenance_archive_read",
                        )
                    ):
                        if (
                            getattr(module, "require_canonical_json", None)
                            is require_canonical_json
                        ):
                            patch.setattr(module, "require_canonical_json", decode)
                yield recorder

    return scope()


def _measured(mode, tmp_path, snapshots, monkeypatch, record_property):
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    from tests.support.canonical_discovery_work import metadata_octets

    operation = {
        "begin": owner.begin_corpus_selection,
        "progress": owner.corpus_selection_progress,
        "next": owner._next,
        "prepare": owner.prepare_corpus_selection,
        "apply": owner.apply_corpus_selection,
        "frozen": owner.frozen_corpus_selection,
        "membership": owner.corpus_selection_admitted,
    }[mode]
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    paths, values = snapshots
    key, admission, reader, manifest, _, anchors = values
    steps = (
        (-1, 7, 15)
        if mode == "begin"
        else (15,)
        if mode in ("frozen", "membership")
        else (0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
        if mode == "progress"
        else (0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
    )
    samples, regimes = [], []
    for width in (1, 17, 1025):
        for reopen in (False, True):
            for step in steps:
                for miss in (False, True) if mode == "membership" else (False,):
                    path = tmp_path / f"{mode}-{width}-{reopen}-{step}-{miss}.sqlite"
                    db = sqlite3.connect(path, isolation_level=None)
                    try:
                        with closing(sqlite3.connect(paths[step])) as source:
                            source.backup(db)
                        with write(db):
                            for index in range(width):
                                # Other builds share the physical indexes; their bodies
                                # must never be decoded by this exact original cursor.
                                other = "other-" + str(index)
                                db.execute(
                                    "INSERT INTO discovery_corpus_prefix_inputs VALUES (?,?,?,?,?)",
                                    (
                                        other,
                                        key.admission_sha256,
                                        "f" * 64,
                                        f"{index:064x}",
                                        "unselected metadata " * 30,
                                    ),
                                )
                                db.execute(
                                    "INSERT INTO discovery_history_import_links "
                                    "VALUES (?,?,?,?,?,?)",
                                    (
                                        other,
                                        key.admission_sha256,
                                        "f" * 64,
                                        f"{index:064x}",
                                        "e" * 64,
                                        "d" * 64,
                                    ),
                                )
                        work = owner.CorpusSelectionWork(
                            ArchiveHistoryWork(admission, reader), manifest
                        )
                        if step >= 0:
                            owner.corpus_selection_progress(db, key, work)
                        if reopen:
                            db.close()
                            db = sqlite3.connect(path, isolation_level=None)
                            work = owner.CorpusSelectionWork(
                                ArchiveHistoryWork(admission, reader), manifest
                            )
                        current = (
                            owner.corpus_selection_progress(db, key, work) if step >= 0 else None
                        )
                        page = (
                            owner.prepare_corpus_selection(db, key, work)
                            if mode == "apply"
                            else None
                        )
                        frozen = (
                            owner.frozen_corpus_selection(db, key, work)
                            if mode == "membership"
                            else None
                        )
                        transactional = mode in ("begin", "apply")
                        if transactional:
                            db.execute("BEGIN IMMEDIATE")
                        try:
                            with _observe(db, monkeypatch, reader) as recorder:
                                if mode == "next":
                                    result = operation(recorder, key, current)
                                elif mode == "apply":
                                    result = operation(recorder, key, work, page)
                                    assert result.version == current.version + 1
                                elif mode == "membership":
                                    anchor = replace(
                                        anchors[0], prefix_bytes=anchors[0].prefix_bytes + int(miss)
                                    )
                                    result = operation(recorder, key, work, frozen, anchor)
                                    assert result is not miss
                                else:
                                    result = operation(recorder, key, work)
                        finally:
                            if transactional:
                                db.commit()
                        tables = [
                            row[0]
                            for row in db.execute(
                                "SELECT name FROM sqlite_master "
                                "WHERE type='table' AND name LIKE 'discovery_%'"
                            ).fetchall()
                        ]
                        rows = [
                            row
                            for table in tables
                            for row in db.execute("SELECT * FROM " + table).fetchall()
                        ]
                        metadata = max((sum(map(metadata_octets, row)) for row in rows), default=0)
                        metadata = max(metadata, len(str(page).encode()), len(str(result).encode()))
                        sample = {
                            "declaration": binding.declaration.id,
                            "declaration_sha256": digest,
                            "dimensions": {
                                "registered_inputs": len(rows),
                                "metadata_octets": metadata,
                            },
                            "metrics": {
                                bound.metric: recorder.metrics[bound.metric]
                                for bound in binding.declaration.bounds
                            },
                            "delegates": {},
                        }
                        validate_execution_samples(projection, binding.declaration.id, [sample])
                        samples.append(sample)
                        regimes.append(
                            {
                                "width": width,
                                "reopened": reopen,
                                "step": step,
                                "missing": miss,
                                "sqlite_vm_instructions": recorder.metrics[
                                    "sqlite_vm_instructions"
                                ],
                            }
                        )
                    finally:
                        db.close()
    record_property("growth_samples", samples)
    record_property("selection_regimes", regimes)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-selection-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selection.begin_corpus_selection",
)
def test_measured_original_selection_begin(tmp_path, snapshots, monkeypatch, record_property):
    _measured("begin", tmp_path, snapshots, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-selection-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selection.corpus_selection_progress",
)
def test_measured_original_selection_progress(tmp_path, snapshots, monkeypatch, record_property):
    _measured("progress", tmp_path, snapshots, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-selection-next/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selection._next",
)
def test_measured_original_selection_next(tmp_path, snapshots, monkeypatch, record_property):
    _measured("next", tmp_path, snapshots, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-selection-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selection.prepare_corpus_selection",
)
def test_measured_original_selection_prepare(tmp_path, snapshots, monkeypatch, record_property):
    _measured("prepare", tmp_path, snapshots, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-selection-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selection.apply_corpus_selection",
)
def test_measured_original_selection_apply(tmp_path, snapshots, monkeypatch, record_property):
    _measured("apply", tmp_path, snapshots, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-selection-frozen/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selection.frozen_corpus_selection",
)
def test_measured_original_selection_frozen(tmp_path, snapshots, monkeypatch, record_property):
    _measured("frozen", tmp_path, snapshots, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-selection-membership/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selection.corpus_selection_admitted",
)
def test_measured_original_selection_membership(tmp_path, snapshots, monkeypatch, record_property):
    _measured("membership", tmp_path, snapshots, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-selection-dense-next/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_selection._next",
)
def test_measured_dense_original_source_index_coordinates(
    tmp_path, snapshots, monkeypatch, record_property
):
    from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    from tests.support.canonical_discovery_work import metadata_octets, observe_metadata

    binding = growth_binding(owner._next)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    paths, values = snapshots
    original_key, admission, reader, manifest, _, _ = values
    with closing(sqlite3.connect(paths[0], isolation_level=None)) as original_db:
        progress = owner.corpus_selection_progress(
            original_db,
            original_key,
            owner.CorpusSelectionWork(ArchiveHistoryWork(admission, reader), manifest),
        )
    samples, regimes = [], []
    for phase in ("requirements", "members", "imports", "dependencies", "prefixes"):
        for width in (1, 17, 1025):
            path = tmp_path / f"index-only-{phase}-{width}.sqlite"
            db = sqlite3.connect(path, isolation_level=None)
            try:
                owner.initialize_corpus_selection(db)
                key = ArchiveTaskKey("index", "b" * 64, "worker")
                keys = []
                with write(db):
                    for index in range(width + 1):
                        a = "0" * 64 if index < width else "1" * 64
                        b = f"{index:064x}" if index < width else "0" * 64
                        if phase == "requirements":
                            a = "imports" if index < width else "roots"
                            db.execute(
                                "INSERT INTO discovery_corpus_history_inputs "
                                "VALUES (?,?,?,?,?,? ,?,'0',0,NULL,'0')",
                                (
                                    key.build_id,
                                    key.admission_sha256,
                                    a,
                                    b,
                                    key.worker_id,
                                    "unread source-only authority",
                                    "all" if a == "imports" else RETAINED_HISTORY_EXTENT,
                                ),
                            )
                        elif phase == "imports":
                            db.execute(
                                "INSERT INTO discovery_history_import_links VALUES (?,?,?,?,?,?)",
                                (key.build_id, key.admission_sha256, a, b, "e" * 64, "d" * 64),
                            )
                        elif phase == "prefixes":
                            db.execute(
                                "INSERT INTO discovery_corpus_prefix_inputs VALUES (?,?,?,?,?)",
                                (
                                    key.build_id,
                                    key.admission_sha256,
                                    a,
                                    b,
                                    "unread source-only anchor",
                                ),
                            )
                        elif phase == "members":
                            a, b = f"{index:064x}", ""
                            db.execute(
                                "INSERT INTO discovery_archive_members VALUES (?,?,?,?,'0',0)",
                                (
                                    key.build_id,
                                    key.admission_sha256,
                                    a,
                                    "unread source-only binding",
                                ),
                            )
                        else:
                            a, b = f"{index:064x}", ""
                            db.execute(
                                "INSERT INTO discovery_corpus_prefix_maxima "
                                "VALUES (?,?,?,?,?,'0',0)",
                                (
                                    key.build_id,
                                    key.admission_sha256,
                                    a,
                                    "unread source-only binding",
                                    "unread source-only anchor",
                                ),
                            )
                        keys.append((a, b))
                keys.sort()
                # This primitive selects source index keys only. Parent/Native provenance
                # is covered by the complete authenticated fixtures, not these opaque bodies.
                positions = [None] + [
                    keys[index] for index in sorted({0, width // 2, width - 1, width})
                ]
                for reopen in (False, True):
                    if reopen:
                        db.close()
                        db = sqlite3.connect(path, isolation_level=None)
                    for position in positions:
                        current = replace(progress, phase=phase, after=position)
                        expected = next(
                            (value for value in keys if position is None or value > position), None
                        )
                        with observe_metadata(db, monkeypatch) as recorder:
                            result = owner._next(recorder, key, current)
                        assert result == expected
                        metrics = dict(recorder.metrics, decoded_octets=0, sha256_block_octets=0)
                        metadata = max(
                            sum(map(metadata_octets, row))
                            for table in (
                                "discovery_corpus_history_inputs",
                                "discovery_history_import_links",
                                "discovery_corpus_prefix_inputs",
                                "discovery_archive_members",
                                "discovery_corpus_prefix_maxima",
                            )
                            for row in db.execute("SELECT * FROM " + table).fetchall()
                        )
                        sample = {
                            "declaration": binding.declaration.id,
                            "declaration_sha256": digest,
                            "dimensions": {
                                "registered_inputs": len(keys),
                                "metadata_octets": metadata,
                            },
                            "metrics": {
                                bound.metric: metrics[bound.metric]
                                for bound in binding.declaration.bounds
                            },
                            "delegates": {},
                        }
                        try:
                            validate_execution_samples(projection, binding.declaration.id, [sample])
                        except ValueError as error:
                            raise AssertionError(
                                (phase, width, reopen, position, sample)
                            ) from error
                        samples.append(sample)
                        regimes.append(
                            {
                                "phase": phase,
                                "width": width,
                                "reopened": reopen,
                                "position": position,
                                "sqlite_vm_instructions": recorder.metrics[
                                    "sqlite_vm_instructions"
                                ],
                            }
                        )
            finally:
                db.close()
    record_property("growth_samples", samples)
    record_property("selection_index_regimes", regimes)


def test_private_selection_codec_has_fixed_header_record_and_digest_vectors():
    from types import SimpleNamespace

    from riverhog_core.canonical_discovery_corpus_inputs import FrozenCorpusSourceInputs

    anchor = HistoryJournalAnchor(
        "urn:uuid:11111111-1111-4111-8111-111111111111",
        "urn:uuid:22222222-2222-4222-8222-222222222222",
        0,
        "3" * 64,
        "4" * 64,
        5,
    )
    binding_json = (
        '{"admission_sha256":"000000000000000000000000000000000000000000000000000000'
        '0000000000","anchor":{"journal_id":"urn:uuid:11111111-1111-4111-8111-111111'
        '111111","prefix_bytes":"5","prefix_sha256":"4444444444444444444444444444444'
        '444444444444444444444444444444444","through":{"entry_id":"urn:uuid:22222222'
        '-2222-4222-8222-222222222222","json_sha256":"333333333333333333333333333333'
        '3333333333333333333333333333333333","sequence":"0"}},"domain":"riverhog-dis'
        'covery-journal-source/v1"}'
    )
    record = (
        '{"anchor":{"journal_id":"urn:uuid:11111111-1111-4111-8111-111111111111","pr'
        'efix_bytes":"5","prefix_sha256":"444444444444444444444444444444444444444444'
        '4444444444444444444444","through":{"entry_id":"urn:uuid:22222222-2222-4222-'
        '8222-222222222222","json_sha256":"33333333333333333333333333333333333333333'
        '33333333333333333333333","sequence":"0"}},"source_binding":{"admission_sha2'
        '56":"0000000000000000000000000000000000000000000000000000000000000000","anc'
        'hor":{"journal_id":"urn:uuid:11111111-1111-4111-8111-111111111111","prefix_'
        'bytes":"5","prefix_sha256":"44444444444444444444444444444444444444444444444'
        '44444444444444444","through":{"entry_id":"urn:uuid:22222222-2222-4222-8222-'
        '222222222222","json_sha256":"3333333333333333333333333333333333333333333333'
        '333333333333333333","sequence":"0"}},"domain":"riverhog-discovery-journal-s'
        'ource/v1"}}'
    )
    state = owner._initial_hash(
        SimpleNamespace(
            history=SimpleNamespace(admission=SimpleNamespace(identity="0" * 64)),
            sources=FrozenCorpusSourceInputs("0" * 64, "1" * 64, 1),
        ),
        1,
    )
    assert state.hexdigest() == "0618704cc16782050331c201ad2be3bc236afa506da36bae4877cdb8a017c071"
    assert owner._selection_record(binding_json, anchor) == record
    state.update(record.encode() + b"\n")
    assert state.hexdigest() == "9dfa8ee7ec705bb4f4d35c31e451d2a6bc404aa69050935f94d739f4fbd1c6c9"
