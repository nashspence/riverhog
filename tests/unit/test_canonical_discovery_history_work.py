from __future__ import annotations

import hashlib
import sqlite3
import uuid
from dataclasses import replace

import pytest
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    PROVENANCE_BINDINGS_FORMAT,
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
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    RecordPage,
    RecordSetCommitment,
    SourceMemberHistoryBindingProof,
    binding_tree_commitment,
    history_record_page_object_path,
    member_history_object_path,
    ordered_provenance_commitment,
    source_binding_proof_object_path,
)
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json
from riverhog_core.canonical_discovery_archive_work import (
    ArchiveTaskKey,
    StaleArchiveTask,
    apply_archive_descriptor,
    archive_metadata_progress,
    begin_archive_metadata,
    initialize_archive_work,
    prepare_archive_descriptor,
)
from riverhog_core.canonical_discovery_history_work import (
    ArchiveHistoryWork,
    HistorySetWork,
    apply_history_descriptor,
    apply_history_import,
    apply_history_record,
    archive_history_ready,
    history_descriptor_ready,
    history_import_progress,
    history_set_progress,
    initialize_history_work,
    prepare_history_descriptor,
    prepare_history_import,
    prepare_history_record,
)
from riverhog_core.canonical_discovery_member_work import (
    ArchiveMemberRegistration,
    apply_archive_member,
    archive_member_progress,
    begin_archive_members,
    initialize_member_work,
    prepare_archive_member,
)
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader

from tests.support.canonical_discovery_work import (
    MeasuredConnection,
    RecordedConnection,
    metadata_octets,
    write,
)
from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission


def anchor(index):
    return HistoryJournalAnchor(
        f"urn:uuid:{uuid.UUID(int=index + 1)}",
        f"urn:uuid:{uuid.UUID(int=index + 1001)}",
        index,
        f"{index:064x}",
        f"{index + 1:064x}",
        index + 100,
    )


def record_set(objects, schema, rows, *, wrong_digest=False, page_size=64):
    rows = sorted(rows, key=lambda row: row["key"])
    commitment = RecordSetCommitment(schema)
    for row in rows:
        commitment.update(row["key"], row["value"])
    authority = commitment.ref()
    if wrong_digest:
        authority = replace(authority, records_sha256="f" * 64)
    ordinal = 0
    for offset in range(0, len(rows), page_size):
        page = RecordPage(authority, ordinal, tuple(rows[offset : offset + page_size]), False)
        objects[history_record_page_object_path(authority.records_sha256, ordinal)] = (
            page.to_json_bytes()
        )
        ordinal += 1
    terminal = RecordPage(authority, ordinal, (), True)
    objects[history_record_page_object_path(authority.records_sha256, ordinal)] = (
        terminal.to_json_bytes()
    )
    return authority


def fixture(
    *,
    count=129,
    members=3,
    fault=None,
    inherited=False,
    import_targets=(1,),
    distinct_import_sets=False,
):
    _, objects, _, _ = archive_reader_fixture()
    root = ProvenanceRootDocument.from_json_bytes(objects["provenance/root.json.age"])
    roots = [
        MemberHistoryRoot(anchor(i), "bound" if i % 2 == 0 else "retained") for i in range(count)
    ]
    primary = MemberHistoryPrimary(anchor(0), f"urn:uuid:{uuid.UUID(int=2001)}")
    if fault == "missing_primary":
        primary = replace(primary, journal=anchor(9999))
    if fault == "retained_primary":
        roots[0] = replace(roots[0], inclusion="retained")
    if fault == "conflicting_heads":
        roots.append(MemberHistoryRoot(replace(anchor(0), prefix_sha256="e" * 64), "bound"))
    roots_ref = record_set(
        objects,
        MEMBER_HISTORY_ROOTS_SCHEMA,
        [{"key": value.key, "value": value.to_mapping()} for value in roots],
        wrong_digest=fault == "digest",
    )
    imports = [
        MemberHistoryImport(
            f"{i + 1:064x}",
            1,
            "a" * 64,
            "b" * 64,
            "c" * 64,
            "d" * 64,
            "e" * 64,
            BOUND_HISTORY_EXTENT,
            {
                "scope": "external",
                "journal_id": anchor(0).journal_id,
                "entry": anchor(0).to_mapping()["through"],
                "assertion_id": f"urn:uuid:{uuid.UUID(int=3001)}",
                "object_id": f"urn:uuid:{uuid.UUID(int=4001)}",
                "object_type": "state",
            },
        )
        for i in range(3)
    ]
    if inherited:
        _, source_objects, source_bindings, _, _, _ = fixture(count=1, members=2)
        source_root = ProvenanceRootDocument.from_json_bytes(
            source_objects["provenance/root.json.age"]
        )
        source_archive = CollectionArchiveManifest(
            archive_generation=source_root.archive_generation,
            artifact_set=CollectionArtifactSetIdentity(
                len(source_bindings),
                sum(value.bytes for value in source_bindings),
                source_root.artifact_set_sha256,
            ),
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
        original_import = imports[0]
        imports = []
        for target_index in import_targets:
            target = source_bindings[target_index]
            tree = binding_tree_commitment(source_bindings, target_artifact_id=target.artifact_id)
            assert tree.target_index is not None
            proof = SourceMemberHistoryBindingProof(
                "9" * 64,
                7,
                source_archive.to_json_bytes(),
                source_root.to_json_bytes(),
                target,
                tree.target_index,
                tree.target_siblings,
            )
            imports.append(
                replace(
                    original_import,
                    source_identity=proof.source_identity,
                    source_collection_id=proof.collection_id,
                    source_archive_root_sha256=hashlib.sha256(proof.archive_root).hexdigest(),
                    source_artifact_set_sha256=source_root.artifact_set_sha256,
                    source_artifact_id=target.artifact_id,
                    source_history_sha256=target.history_sha256,
                    source_binding_proof_sha256=proof.identity,
                )
            )
            objects[source_binding_proof_object_path(proof.identity)] = proof.to_json_bytes()
        for path, raw in source_objects.items():
            if path.startswith(("provenance/history/", "provenance/sets/")):
                objects[path] = raw
    imports_ref = record_set(
        objects,
        MEMBER_HISTORY_IMPORTS_SCHEMA,
        [{"key": value.key, "value": value.to_mapping()} for value in imports],
    )
    histories, bindings = [], []
    for index in range(members):
        selected_imports = imports_ref
        if distinct_import_sets and index % 2:
            selected_imports = record_set(
                objects,
                MEMBER_HISTORY_IMPORTS_SCHEMA,
                [
                    {
                        "key": value.key,
                        "value": replace(value, extent=RETAINED_HISTORY_EXTENT).to_mapping(),
                    }
                    for value in imports
                ],
            )
        descriptor = MemberHistoryDocument(
            f"{index:064x}", 7, "a" * 64, primary, roots_ref, selected_imports
        )
        raw = descriptor.to_json_bytes()
        binding = MemberHistoryBinding(
            descriptor.artifact_id,
            descriptor.bytes,
            descriptor.sha256,
            descriptor.identity,
            len(raw),
        )
        objects[member_history_object_path(binding.history_sha256)] = raw
        histories.append(descriptor)
        bindings.append(binding)
    binding_page = canonical_json_bytes(
        {"format": PROVENANCE_BINDINGS_FORMAT, "bindings": [b.to_mapping() for b in bindings]}
    )
    old = ProvenanceVolumeDocument.from_json_bytes(
        objects["provenance/metadata/volume-" + "0" * 64 + ".json.age"]
    )
    updated = replace(
        old,
        payload=replace(
            old.payload, bytes=len(binding_page), sha256=hashlib.sha256(binding_page).hexdigest()
        ),
        first_artifact_id=bindings[0].artifact_id,
        last_artifact_id=bindings[-1].artifact_id,
        binding_count=members,
    )
    objects[updated.metadata_path] = updated.to_json_bytes()
    objects[updated.payload.path] = binding_page
    documents = [updated]
    for i in (1, 2):
        documents.append(
            ProvenanceVolumeDocument.from_json_bytes(
                objects[f"provenance/metadata/volume-{i:064x}.json.age"]
            )
        )
    terminal = ProvenanceTerminalDocument.from_json_bytes(
        objects[f"provenance/metadata/volume-{3:064x}.json.age"]
    )
    root = replace(
        root,
        binding_count=members,
        binding_tree_sha256=binding_tree_commitment(bindings).root_sha256,
        ordered_volume_sha256=ordered_provenance_commitment((*documents, terminal)),
    )
    objects["provenance/root.json.age"] = root.to_json_bytes()
    reads = []

    def read(path):
        reads.append(path)
        yield objects[path]

    reader = CanonicalProvenanceArchiveReader(
        read,
        expected_root_sha256=root.identity,
        archive_generation=root.archive_generation,
        artifact_set_sha256=root.artifact_set_sha256,
    )
    return reader, objects, bindings, histories, discovery_archive_admission(root), reads


def setup_work(
    path,
    *,
    count=129,
    members=3,
    fault=None,
    inherited=False,
    import_targets=(1,),
    distinct_import_sets=False,
):
    reader, objects, bindings, histories, admission, reads = fixture(
        count=count,
        members=members,
        fault=fault,
        inherited=inherited,
        import_targets=import_targets,
        distinct_import_sets=distinct_import_sets,
    )
    db = sqlite3.connect(path, isolation_level=None)
    initialize_archive_work(db)
    initialize_member_work(db)
    initialize_history_work(db)
    key = ArchiveTaskKey("build", admission.identity, "worker")
    with write(db):
        begin_archive_metadata(db, key, admission)
    while not archive_metadata_progress(db, key).complete:
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    with write(db):
        begin_archive_members(db, key, admission)
    member_owner = ArchiveMemberRegistration(admission, reader)
    while not archive_member_progress(db, key).complete:
        page = prepare_archive_member(db, key, member_owner)
        with write(db):
            apply_archive_member(db, key, member_owner, page)
    history_owner = ArchiveHistoryWork(admission, reader)
    for binding in bindings:
        page = prepare_history_descriptor(db, key, history_owner, binding.artifact_id)
        with write(db):
            apply_history_descriptor(db, key, history_owner, page)
    return db, key, history_owner, histories, objects, reads


def advance(db, key, owner):
    page = prepare_history_record(db, key, owner)
    with write(db):
        return apply_history_record(db, key, owner, page)


def finish(db, key, owner):
    while not history_set_progress(db, key, owner).complete:
        advance(db, key, owner)


def test_shared_sets_resume_exact_page_item_without_prior_page_replay(tmp_path):
    path = tmp_path / "work.sqlite"
    db, key, history, descriptors, _, reads = setup_work(path)
    descriptor = descriptors[0]
    owner = HistorySetWork(history.admission, history.reader, descriptor.roots)
    assert not archive_history_ready(db, key, history, descriptor.artifact_id)
    reads.clear()
    for _ in range(65):
        advance(db, key, owner)
    assert len(reads) == 4
    assert history_set_progress(db, key, owner).page_ordinal == 1
    assert history_set_progress(db, key, owner).item_index == 1
    db.close()
    db = sqlite3.connect(path, isolation_level=None)
    owner = HistorySetWork(history.admission, history.reader, descriptor.roots)
    reads.clear()
    finish(db, key, owner)
    assert len(reads) == 6  # Current second page, final data page and mandatory terminal.
    assert history_set_progress(db, key, owner).bound_roots == 65
    assert not any("metadata/" in value for value in reads)
    imports = HistorySetWork(history.admission, history.reader, descriptor.imports)
    finish(db, key, imports)
    assert history_set_progress(db, key, imports).bound_roots == 0
    assert db.execute("SELECT count(*) FROM discovery_history_sets").fetchone() == (2,)
    assert db.execute("SELECT count(*) FROM discovery_history_set_records").fetchone() == (132,)
    assert db.execute("SELECT count(*) FROM discovery_archive_histories").fetchone() == (3,)
    for value in descriptors:
        assert archive_history_ready(db, key, history, value.artifact_id)
    stored = db.execute(
        "SELECT record_key,value_json,source_page,source_item "
        "FROM discovery_history_set_records WHERE records_sha256=? ORDER BY record_key",
        (descriptor.roots.records_sha256,),
    ).fetchall()
    assert [(int(row[2]), row[3]) for row in stored] == [(i // 64, i % 64) for i in range(129)]
    assert [row[0] for row in stored] == sorted(row[0] for row in stored)
    db.close()


@pytest.mark.parametrize("fault", ["digest", "missing_terminal", "changed_record"])
def test_independent_set_digest_and_terminal_are_required(tmp_path, fault):
    db, key, history, descriptors, objects, _ = setup_work(
        tmp_path / "work.sqlite", count=3, fault="digest" if fault == "digest" else None
    )
    authority = descriptors[0].roots
    if fault == "missing_terminal":
        objects.pop(history_record_page_object_path(authority.records_sha256, 1))
    if fault == "changed_record":
        path = history_record_page_object_path(authority.records_sha256, 0)
        page = RecordPage.from_json_bytes(objects[path])
        rows = list(page.records)
        rows[0]["value"]["inclusion"] = (
            "retained" if rows[0]["value"]["inclusion"] == "bound" else "bound"
        )
        objects[path] = replace(page, records=tuple(rows)).to_json_bytes()
    owner = HistorySetWork(history.admission, history.reader, authority)
    with pytest.raises((StaleArchiveTask, KeyError)):
        finish(db, key, owner)
    assert not history_set_progress(db, key, owner).complete
    assert not archive_history_ready(db, key, history, descriptors[0].artifact_id)
    db.close()


@pytest.mark.parametrize("fault", ["missing_primary", "retained_primary"])
def test_exact_primary_must_be_bound_after_both_independent_terminals(tmp_path, fault):
    db, key, history, descriptors, _, _ = setup_work(tmp_path / "work.sqlite", count=3, fault=fault)
    descriptor = descriptors[0]
    for authority in (descriptor.roots, descriptor.imports):
        finish(db, key, HistorySetWork(history.admission, history.reader, authority))
    with pytest.raises(StaleArchiveTask, match="exact bound primary"):
        archive_history_ready(db, key, history, descriptor.artifact_id)
    db.close()


def test_conflicting_bound_heads_roll_back_the_child_and_continuation(tmp_path):
    db, key, history, descriptors, _, _ = setup_work(
        tmp_path / "work.sqlite", count=129, fault="conflicting_heads"
    )
    owner = HistorySetWork(history.admission, history.reader, descriptors[0].roots)
    while True:
        before = history_set_progress(db, key, owner)
        try:
            advance(db, key, owner)
        except sqlite3.IntegrityError:
            break
    assert history_set_progress(db, key, owner) == before
    assert not before.complete
    db.close()


def test_apply_failure_exact_retry_and_stale_preparation(tmp_path):
    db, key, history, descriptors, _, _ = setup_work(tmp_path / "work.sqlite", count=3)
    owner = HistorySetWork(history.admission, history.reader, descriptors[0].roots)
    page = prepare_history_record(db, key, owner)
    before = history_set_progress(db, key, owner)
    db.execute(
        "CREATE TRIGGER fail_record BEFORE INSERT ON discovery_history_set_records "
        "BEGIN SELECT RAISE(ABORT,'injected history failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected"):
        apply_history_record(db, key, owner, page)
    assert history_set_progress(db, key, owner) == before
    assert db.execute("SELECT count(*) FROM discovery_history_set_records").fetchone() == (0,)
    db.execute("DROP TRIGGER fail_record")
    with write(db):
        apply_history_record(db, key, owner, page)
    with write(db), pytest.raises(StaleArchiveTask, match="version"):
        apply_history_record(db, key, owner, page)
    db.close()


def test_source_worker_and_prepared_capability_cannot_be_substituted(tmp_path):
    db, key, history, descriptors, _, _ = setup_work(tmp_path / "work.sqlite", count=3)
    owner = HistorySetWork(history.admission, history.reader, descriptors[0].roots)
    page = prepare_history_record(db, key, owner)
    with write(db), pytest.raises(StaleArchiveTask, match="current exact"):
        apply_history_record(db, key, owner, replace(page, terminal=True))
    with write(db), pytest.raises(StaleArchiveTask, match="current exact"):
        apply_history_record(
            db, key, HistorySetWork(history.admission, history.reader, descriptors[0].roots), page
        )
    with pytest.raises(StaleArchiveTask, match="source/build/worker"):
        prepare_history_record(db, replace(key, worker_id="other"), owner)
    with pytest.raises(ValueError, match="owner-managed"):
        apply_history_record(db, key, owner, page)
    with write(db), pytest.raises(ValueError, match="no active"):
        prepare_history_record(db, key, owner)
    db.close()


def test_descriptor_and_shared_child_obligations_commit_together(tmp_path):
    db, key, history, descriptors, _, _ = setup_work(tmp_path / "work.sqlite", count=3)
    db.execute("DELETE FROM discovery_archive_histories")
    db.execute("DELETE FROM discovery_history_sets")
    page = prepare_history_descriptor(db, key, history, descriptors[0].artifact_id)
    db.execute(
        "CREATE TRIGGER fail_set BEFORE INSERT ON discovery_history_sets "
        "BEGIN SELECT RAISE(ABORT,'injected set failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected"):
        apply_history_descriptor(db, key, history, page)
    assert db.execute("SELECT count(*) FROM discovery_archive_histories").fetchone() == (0,)
    assert db.execute("SELECT count(*) FROM discovery_history_sets").fetchone() == (0,)
    db.execute("DROP TRIGGER fail_set")
    with write(db):
        apply_history_descriptor(db, key, history, page)
    db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-history-set-preparation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_work.prepare_history_record",
)
def test_measured_history_record_preparation_reads_only_current_page(
    tmp_path, monkeypatch, record_property
):
    measured(tmp_path, monkeypatch, record_property, prepare=True)


@pytest.mark.growth_witness(
    "canonical-discovery-history-set-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_work.apply_history_record",
)
def test_measured_history_record_apply_has_fixed_sql_without_source_hashing(
    tmp_path, monkeypatch, record_property
):
    measured(tmp_path, monkeypatch, record_property, prepare=False)


def measured(tmp_path, monkeypatch, record_property, *, prepare):
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )
    from sha256_continuation import CheckpointSHA256

    db, key, history, descriptors, _, reads = setup_work(tmp_path / "measured.sqlite")
    owner = HistorySetWork(history.admission, history.reader, descriptors[0].roots)
    binding = growth_binding(prepare_history_record if prepare else apply_history_record)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    actual = CheckpointSHA256.update_bytes
    while not (before := history_set_progress(db, key, owner)).complete:
        recorder = RecordedConnection(db)
        observed = {"source_hash_octets": 0}

        def update(self, raw, observed=observed):
            observed["source_hash_octets"] += len(raw)
            return actual(self, raw)

        if not prepare:
            page = prepare_history_record(db, key, owner)
        read_count = len(reads)
        with monkeypatch.context() as patch:
            patch.setattr(CheckpointSHA256, "update_bytes", update)
            if prepare:
                page = prepare_history_record(recorder, key, owner)
            else:
                with write(db):
                    apply_history_record(recorder, key, owner, page)
        metrics = {
            "sql_statements": recorder.metrics["sql_statements"],
            "returned_rows": recorder.metrics["returned_rows"],
        }
        if prepare:
            metrics["object_reads"] = len(reads) - read_count
        else:
            metrics.update(observed)
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": declaration_sha,
            "dimensions": {"registered_records": before.records},
            "metrics": metrics,
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)
        if prepare:
            with write(db):
                apply_history_record(db, key, owner, page)
    record_property("growth_samples", samples)
    db.close()


def imported_work(tmp_path):
    db, key, history, descriptors, objects, reads = setup_work(
        tmp_path / "imports.sqlite", count=3, inherited=True
    )
    authority = descriptors[0].imports
    owner = HistorySetWork(history.admission, history.reader, authority)
    source = RecordPage.from_json_bytes(
        objects[history_record_page_object_path(authority.records_sha256, 0)]
    )
    imported = MemberHistoryImport.from_mapping(source.records[0]["value"])
    proof = SourceMemberHistoryBindingProof.from_json_bytes(
        objects[source_binding_proof_object_path(imported.source_binding_proof_sha256)]
    )
    return db, key, history, owner, imported, proof, objects, reads


def test_import_preserves_original_membership_and_shared_history_obligations(tmp_path):
    db, key, history, owner, imported, proof, _, reads = imported_work(tmp_path)
    with pytest.raises(StaleArchiveTask, match="completely verified"):
        prepare_history_import(db, key, owner, imported.key)
    finish(db, key, owner)
    reads.clear()
    page = prepare_history_import(db, key, owner, imported.key)
    assert len(reads) == 4
    assert page.proof_json.encode() == proof.to_json_bytes()
    assert proof.index == 1 and proof.siblings
    assert proof.source_identity != key.admission_sha256
    with write(db):
        apply_history_import(db, key, owner, page)
    assert db.execute(
        "SELECT proof_json,history_sha256 FROM discovery_history_import_proofs"
    ).fetchone() == (proof.to_json_bytes().decode(), proof.binding.history_sha256)
    assert not history_descriptor_ready(db, key, history, proof.binding.history_sha256)
    descriptor = history.reader.history_descriptor(proof.binding)
    for authority in (descriptor.roots, descriptor.imports):
        finish(db, key, HistorySetWork(history.admission, history.reader, authority))
    assert history_descriptor_ready(db, key, history, proof.binding.history_sha256)
    before = db.execute(
        "SELECT records_sha256,version,checkpoint_json,complete "
        "FROM discovery_history_sets ORDER BY records_sha256"
    ).fetchall()
    with write(db):
        apply_history_import(db, key, owner, page)
    assert (
        db.execute(
            "SELECT records_sha256,version,checkpoint_json,complete "
            "FROM discovery_history_sets ORDER BY records_sha256"
        ).fetchall()
        == before
    )
    assert db.execute("SELECT count(*) FROM discovery_history_import_proofs").fetchone() == (1,)
    assert db.execute("SELECT count(*) FROM discovery_history_descriptors").fetchone() == (4,)
    db.close()


@pytest.mark.parametrize("fault", ["proof", "descriptor"])
def test_import_rejects_changed_proof_or_selected_descriptor_before_writing(tmp_path, fault):
    db, key, _, owner, imported, proof, objects, _ = imported_work(tmp_path)
    finish(db, key, owner)
    if fault == "proof":
        changed = replace(proof, collection_id=proof.collection_id + 1)
        objects[source_binding_proof_object_path(proof.identity)] = changed.to_json_bytes()
    else:
        objects[member_history_object_path(proof.binding.history_sha256)] += b" "
    before = db.execute("SELECT count(*) FROM discovery_history_sets").fetchone()
    with pytest.raises(ValueError):
        prepare_history_import(db, key, owner, imported.key)
    assert db.execute("SELECT count(*) FROM discovery_history_import_proofs").fetchone() == (0,)
    assert db.execute("SELECT count(*) FROM discovery_history_sets").fetchone() == before
    db.close()


def test_import_child_obligations_rollback_and_retry_with_the_original_proof(tmp_path):
    db, key, _, owner, imported, _, _, _ = imported_work(tmp_path)
    finish(db, key, owner)
    page = prepare_history_import(db, key, owner, imported.key)
    before = db.execute("SELECT count(*) FROM discovery_history_sets").fetchone()
    db.execute(
        "CREATE TRIGGER fail_proof BEFORE INSERT ON discovery_history_import_proofs "
        "BEGIN SELECT RAISE(ABORT,'injected proof failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected"):
        apply_history_import(db, key, owner, page)
    assert db.execute("SELECT count(*) FROM discovery_history_import_proofs").fetchone() == (0,)
    assert db.execute("SELECT count(*) FROM discovery_history_descriptors").fetchone() == (3,)
    assert db.execute("SELECT count(*) FROM discovery_history_sets").fetchone() == before
    db.execute("DROP TRIGGER fail_proof")
    with write(db), pytest.raises(StaleArchiveTask, match="current exact"):
        apply_history_import(db, key, owner, replace(page, proof_sha256="f" * 64))
    with write(db):
        apply_history_import(db, key, owner, page)
    db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-history-import-preparation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_work.prepare_history_import",
)
def test_measured_import_preparation_has_fixed_reads(tmp_path, monkeypatch, record_property):
    measured_import(tmp_path, monkeypatch, record_property, prepare=True)


@pytest.mark.growth_witness(
    "canonical-discovery-history-import-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_work.apply_history_import",
)
def test_measured_import_apply_has_fixed_sql_without_source_hashing(
    tmp_path, monkeypatch, record_property
):
    measured_import(tmp_path, monkeypatch, record_property, prepare=False)


def measured_import(tmp_path, monkeypatch, record_property, *, prepare):
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )
    from sha256_continuation import CheckpointSHA256

    db, key, _, owner, imported, _, _, reads = imported_work(tmp_path)
    finish(db, key, owner)
    binding = growth_binding(prepare_history_import if prepare else apply_history_import)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    actual = CheckpointSHA256.update_bytes
    for existing in (0, 1):
        recorder = RecordedConnection(db)
        observed = {"source_hash_octets": 0}

        def update(self, raw, observed=observed):
            observed["source_hash_octets"] += len(raw)
            return actual(self, raw)

        if not prepare:
            page = prepare_history_import(db, key, owner, imported.key)
        read_count = len(reads)
        with monkeypatch.context() as patch:
            patch.setattr(CheckpointSHA256, "update_bytes", update)
            if prepare:
                page = prepare_history_import(recorder, key, owner, imported.key)
            else:
                with write(db):
                    apply_history_import(recorder, key, owner, page)
        metrics = {
            "sql_statements": recorder.metrics["sql_statements"],
            "returned_rows": recorder.metrics["returned_rows"],
        }
        if prepare:
            metrics["object_reads"] = len(reads) - read_count
        else:
            metrics.update(observed)
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": declaration_sha,
            "dimensions": {"registered_imports": existing},
            "metrics": metrics,
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)
        if prepare:
            with write(db):
                apply_history_import(db, key, owner, page)
    record_property("growth_samples", samples)
    db.close()


def import_rows(owner, objects):
    page = RecordPage.from_json_bytes(
        objects[history_record_page_object_path(owner.authority.records_sha256, 0)]
    )
    return [MemberHistoryImport.from_mapping(row["value"]) for row in page.records]


def test_import_coverage_is_independent_of_shared_proofs_and_reopens(tmp_path):
    path = tmp_path / "coverage.sqlite"
    db, key, history, descriptors, objects, _ = setup_work(
        path, count=3, inherited=True, import_targets=(0, 1), distinct_import_sets=True
    )
    owners = [HistorySetWork(history.admission, history.reader, d.imports) for d in descriptors[:2]]
    assert owners[0].authority != owners[1].authority
    for owner in owners:
        finish(db, key, owner)
        initial = history_import_progress(db, key, owner)
        assert initial.expected_imports == 2
        assert initial.registered_imports == 0 and not initial.complete
        for count, imported in enumerate(reversed(import_rows(owner, objects)), 1):
            page = prepare_history_import(db, key, owner, imported.key)
            with write(db):
                apply_history_import(db, key, owner, page)
            progress = history_import_progress(db, key, owner)
            assert progress.registered_imports == count
            assert progress.complete is (count == 2)
            with write(db):
                apply_history_import(db, key, owner, page)
            assert history_import_progress(db, key, owner) == progress
        db.close()
        db = sqlite3.connect(path, isolation_level=None)
        assert history_import_progress(db, key, owner).complete
    assert db.execute("SELECT count(*) FROM discovery_history_import_proofs").fetchone() == (2,)
    assert db.execute("SELECT count(*) FROM discovery_history_import_links").fetchone() == (4,)
    db.close()


def test_verified_empty_import_set_has_an_explicit_zero_terminal(tmp_path):
    db, key, history, descriptors, _, _ = setup_work(
        tmp_path / "empty.sqlite", count=3, inherited=True, import_targets=()
    )
    owner = HistorySetWork(history.admission, history.reader, descriptors[0].imports)
    with pytest.raises(StaleArchiveTask, match="completely verified"):
        history_import_progress(db, key, owner)
    finish(db, key, owner)
    progress = history_import_progress(db, key, owner)
    assert progress.expected_imports == progress.registered_imports == 0
    assert progress.complete
    assert db.execute("SELECT count(*) FROM discovery_history_import_links").fetchone() == (0,)
    db.close()


def test_import_coverage_rolls_back_proof_children_and_incidence_together(tmp_path):
    db, key, _, owner, imported, _, _, _ = imported_work(tmp_path)
    finish(db, key, owner)
    page = prepare_history_import(db, key, owner, imported.key)
    tables = (
        "discovery_history_descriptors",
        "discovery_history_sets",
        "discovery_history_import_proofs",
        "discovery_history_import_links",
        "discovery_history_import_coverage",
    )
    before = {table: db.execute("SELECT * FROM " + table).fetchall() for table in tables}
    db.execute(
        "CREATE TRIGGER fail_import_coverage BEFORE UPDATE ON discovery_history_import_coverage "
        "BEGIN SELECT RAISE(ABORT,'injected import coverage failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected import coverage"):
        apply_history_import(db, key, owner, page)
    assert {table: db.execute("SELECT * FROM " + table).fetchall() for table in tables} == before
    db.execute("DROP TRIGGER fail_import_coverage")
    with write(db):
        apply_history_import(db, key, owner, page)
    assert history_import_progress(db, key, owner).complete
    db.execute("UPDATE discovery_history_import_links SET proof_sha256=?", ("f" * 64,))
    with write(db), pytest.raises(StaleArchiveTask, match="original coordinates"):
        apply_history_import(db, key, owner, page)
    db.close()


@pytest.mark.parametrize("fault", ["worker", "authority", "count", "counter", "missing", "cache"])
def test_import_coverage_rejects_stale_authority_and_corrupt_accounting(tmp_path, fault):
    db, key, _, owner, imported, _, _, _ = imported_work(tmp_path)
    finish(db, key, owner)
    page = prepare_history_import(db, key, owner, imported.key)
    with write(db):
        apply_history_import(db, key, owner, page)
    if fault == "cache":
        owner.authority = replace(owner.authority, record_count=0)
    elif fault == "missing":
        db.execute("DELETE FROM discovery_history_import_coverage")
        with write(db), pytest.raises(StaleArchiveTask, match="matching coverage"):
            apply_history_import(db, key, owner, page)
        db.close()
        return
    else:
        column, value = {
            "worker": ("worker_id", "replaced"),
            "authority": ("authority_json", "{}"),
            "count": ("registered_imports", "2"),
            "counter": ("registered_imports", "01"),
        }[fault]
        db.execute("UPDATE discovery_history_import_coverage SET " + column + "=?", (value,))
    with pytest.raises(StaleArchiveTask, match="authority|coverage"):
        history_import_progress(db, key, owner)
    db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-history-import-coverage/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_work.history_import_progress",
)
def test_measured_import_coverage_is_indexed_and_read_only(tmp_path, monkeypatch, record_property):
    from riverhog_core import canonical_discovery_history_work as histories
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    binding = growth_binding(history_import_progress)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    actual_encode = histories.canonical_json_bytes
    samples = []
    for retained in (1, 17, 129):
        path = tmp_path / ("import-coverage-" + str(retained) + ".sqlite")
        db, key, history, descriptors, objects, _ = setup_work(
            path, count=3, inherited=True, import_targets=(0, 1)
        )
        owner = HistorySetWork(history.admission, history.reader, descriptors[0].imports)
        finish(db, key, owner)
        imports = import_rows(owner, objects)
        for index in range(retained - 1):
            db.execute(
                "INSERT INTO discovery_history_import_coverage VALUES (?,?,?,?,?,'0')",
                (
                    "unrelated-" + str(index),
                    key.admission_sha256,
                    owner.authority.records_sha256,
                    "unrelated",
                    "unrelated damaged import authority" * 3000,
                ),
            )
        for state, selected in (
            ("unstarted", ()),
            ("partial", imports[:1]),
            ("complete", imports[1:]),
        ):
            for imported in selected:
                page = prepare_history_import(db, key, owner, imported.key)
                with write(db):
                    apply_history_import(db, key, owner, page)
            for cache in ("warm", "reopened"):
                if cache == "reopened":
                    db.close()
                    db = sqlite3.connect(path, isolation_level=None)
                widths = [
                    sum(
                        map(
                            metadata_octets,
                            (
                                key.build_id,
                                key.admission_sha256,
                                key.worker_id,
                                owner.authority_json,
                            ),
                        )
                    )
                ]
                for table, extra, values in (
                    ("discovery_archive_tasks", "", ()),
                    (
                        "discovery_history_sets",
                        " AND records_sha256=?",
                        (owner.authority.records_sha256,),
                    ),
                    (
                        "discovery_history_import_coverage",
                        " AND import_set_sha256=?",
                        (owner.authority.records_sha256,),
                    ),
                ):
                    row = db.execute(
                        "SELECT * FROM "
                        + table
                        + " WHERE build_id=? AND admission_sha256=?"
                        + extra,
                        (key.build_id, key.admission_sha256, *values),
                    ).fetchone()
                    if row is not None:
                        widths.append(sum(map(metadata_octets, row)))
                dimensions = {
                    "registered_records": sum(
                        db.execute("SELECT count(*) FROM " + table).fetchone()[0]
                        for table in (
                            "discovery_archive_tasks",
                            "discovery_history_sets",
                            "discovery_history_import_coverage",
                        )
                    ),
                    "metadata_octets": max(widths),
                }
                observer = MeasuredConnection(db)
                changes = db.total_changes

                def encode(value, observer=observer):
                    encoded = actual_encode(value)
                    observer.metrics["encoded_octets"] += len(encoded)
                    return encoded

                def instruction(observer=observer):
                    observer.metrics["sqlite_vm_instructions"] += 1
                    return 0

                db.set_progress_handler(instruction, 1)
                try:
                    with monkeypatch.context() as patch:
                        patch.setattr(histories, "canonical_json_bytes", encode)
                        patch.setitem(
                            require_canonical_json.__globals__, "canonical_json_bytes", encode
                        )
                        progress = history_import_progress(observer, key, owner)
                finally:
                    db.set_progress_handler(None, 0)
                assert db.total_changes == changes
                assert progress.expected_imports == 2
                assert (
                    progress.registered_imports
                    == {"unstarted": 0, "partial": 1, "complete": 2}[state]
                )
                assert progress.complete is (state == "complete")
                for sql, parameters in observer.statements:
                    assert "count(" not in sql.lower()
                    plans = db.execute("EXPLAIN QUERY PLAN " + sql, parameters).fetchall()
                    assert all("SCAN" not in row[3].upper() for row in plans)
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": declaration_sha,
                    "dimensions": dimensions,
                    "metrics": {
                        bound.metric: observer.metrics[bound.metric]
                        for bound in binding.declaration.bounds
                    },
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
        db.close()
    record_property("growth_samples", samples)


@pytest.mark.parametrize("counter", ["-1", "01", "4"])
def test_bound_input_count_rejects_noncanonical_or_impossible_values(tmp_path, counter):
    db, key, history, descriptors, _, _ = setup_work(tmp_path / "count.sqlite", count=3)
    owner = HistorySetWork(history.admission, history.reader, descriptors[0].roots)
    finish(db, key, owner)
    db.execute(
        "UPDATE discovery_history_sets SET bound_roots=? WHERE records_sha256=?",
        (counter, owner.authority.records_sha256),
    )
    with pytest.raises(StaleArchiveTask, match="bound-root count"):
        history_set_progress(db, key, owner)
    db.close()


def test_bound_input_count_and_record_checkpoint_roll_back_together(tmp_path):
    db, key, history, descriptors, _, _ = setup_work(tmp_path / "rollback.sqlite", count=3)
    owner = HistorySetWork(history.admission, history.reader, descriptors[0].roots)
    before = history_set_progress(db, key, owner)
    page = prepare_history_record(db, key, owner)
    db.execute(
        "CREATE TRIGGER fail_bound_count BEFORE UPDATE OF bound_roots ON discovery_history_sets "
        "BEGIN SELECT RAISE(ABORT,'injected bound count failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected bound count"):
        apply_history_record(db, key, owner, page)
    assert history_set_progress(db, key, owner) == before
    assert db.execute("SELECT count(*) FROM discovery_history_set_records").fetchone() == (0,)
    db.execute("DROP TRIGGER fail_bound_count")
    with write(db):
        result = apply_history_record(db, key, owner, page)
    assert result.records == 1
    assert result.bound_roots == int(page.bound_journal is not None)
    db.close()


@pytest.mark.parametrize("count", [1, 17, 129])
def test_bound_root_input_count_matches_maintained_archive_selector(tmp_path, count):
    db, key, history, descriptors, _, _ = setup_work(tmp_path / "parity.sqlite", count=count)
    descriptor = descriptors[0]
    raw = db.execute(
        "SELECT binding_json FROM discovery_archive_histories WHERE artifact_id=?",
        (descriptor.artifact_id,),
    ).fetchone()[0]
    binding = MemberHistoryBinding.from_mapping(require_canonical_json(raw.encode()))
    store = history.reader.history_store()
    expected = {
        extent: sum(1 for _ in store.roots(binding, extent=extent))
        for extent in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT)
    }
    owner = HistorySetWork(history.admission, history.reader, descriptor.roots)
    finish(db, key, owner)
    progress = history_set_progress(db, key, owner)
    assert progress.records == expected[RETAINED_HISTORY_EXTENT]
    assert progress.bound_roots == expected[BOUND_HISTORY_EXTENT]
    db.close()
