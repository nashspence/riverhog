from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace

import pytest
from riverhog_archive_contracts import (
    PROVENANCE_BINDINGS_FORMAT,
    MemberHistoryBinding,
    ProvenancePayload,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    binding_tree_commitment,
    ordered_provenance_commitment,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.canonical_discovery_archive_work import (
    ArchiveTaskKey,
    StaleArchiveTask,
    apply_archive_descriptor,
    archive_metadata_progress,
    begin_archive_metadata,
    initialize_archive_work,
    prepare_archive_descriptor,
)
from riverhog_core.canonical_discovery_member_work import (
    ArchiveMemberRegistration,
    apply_archive_member,
    archive_member_progress,
    begin_archive_members,
    initialize_member_work,
    prepare_archive_member,
)
from riverhog_core.provenance_archive_read import (
    CanonicalProvenanceArchiveReader,
    ProvenanceArchiveReadError,
)

from tests.support.canonical_discovery_work import RecordedConnection, write
from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission


def fixture(count=17, page_members=7, *, wrong_tree=False):
    _, objects, _, _ = archive_reader_fixture()
    old = ProvenanceRootDocument.from_json_bytes(objects["provenance/root.json.age"])
    bindings = [
        MemberHistoryBinding(f"{i:064x}", i, "a" * 64, f"{i + 1:064x}", 500) for i in range(count)
    ]
    documents = []
    for offset in range(0, count, page_members):
        page = bindings[offset : offset + page_members]
        raw = canonical_json_bytes(
            {"format": PROVENANCE_BINDINGS_FORMAT, "bindings": [b.to_mapping() for b in page]}
        )
        document = ProvenanceVolumeDocument(
            old.archive_generation,
            old.artifact_set_sha256,
            len(documents),
            ProvenancePayload(
                "bindings", len(documents), len(raw), hashlib.sha256(raw).hexdigest()
            ),
            first_artifact_id=page[0].artifact_id,
            last_artifact_id=page[-1].artifact_id,
            binding_count=len(page),
        )
        documents.append(document)
        objects[document.payload.path] = raw
    for i in (1, 2):
        original = ProvenanceVolumeDocument.from_json_bytes(
            objects[f"provenance/metadata/volume-{i:064x}.json.age"]
        )
        documents.append(
            replace(
                original,
                sequence=len(documents),
                payload=replace(original.payload, sequence=len(documents)),
            )
        )
    terminal = ProvenanceTerminalDocument(
        old.archive_generation, old.artifact_set_sha256, len(documents)
    )
    root = replace(
        old,
        binding_count=count,
        binding_tree_sha256="0" * 64
        if wrong_tree
        else binding_tree_commitment(bindings).root_sha256,
        ordered_volume_sha256=ordered_provenance_commitment((*documents, terminal)),
    )
    objects["provenance/root.json.age"] = root.to_json_bytes()
    for document in (*documents, terminal):
        objects[document.metadata_path] = document.to_json_bytes()
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
    return reader, objects, bindings, discovery_archive_admission(root), reads


def open_work(path, admission):
    db = sqlite3.connect(path, isolation_level=None)
    initialize_archive_work(db)
    initialize_member_work(db)
    key = ArchiveTaskKey("build", admission.identity, "worker")
    with write(db):
        begin_archive_metadata(db, key, admission)
    return db, key


def authenticate_metadata(db, key, admission, reader):
    while not archive_metadata_progress(db, key).complete:
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    with write(db):
        begin_archive_members(db, key, admission)


def advance(db, key, owner):
    page = prepare_archive_member(db, key, owner)
    with write(db):
        return apply_archive_member(db, key, owner, page)


def test_registration_resumes_at_exact_member_without_prior_page_replay(tmp_path):
    reader, _, bindings, admission, reads = fixture(1025, 512)
    path = tmp_path / "members.sqlite"
    db, key = open_work(path, admission)
    authenticate_metadata(db, key, admission, reader)
    owner = ArchiveMemberRegistration(admission, reader)
    reads.clear()
    for _ in range(513):
        advance(db, key, owner)
    assert len(reads) == 4  # Each current page reads its root and payload once.
    assert not any(path.startswith("provenance/metadata/") for path in reads)
    before = archive_member_progress(db, key)
    assert before.members == 513 and before.item_index == 1
    db.close()
    db, key = open_work(path, admission)
    owner = ArchiveMemberRegistration(admission, reader)
    reads.clear()
    while not archive_member_progress(db, key).complete:
        advance(db, key, owner)
    assert len(reads) == 4  # Current second page, then third page; no prior-page replay.
    assert not any(path.startswith("provenance/metadata/") for path in reads)
    rows = db.execute(
        "SELECT binding_json,source_page,source_item FROM discovery_archive_members "
        "ORDER BY artifact_id"
    ).fetchall()
    assert [row[0] for row in rows] == [
        canonical_json_bytes(binding.to_mapping()).decode() for binding in bindings
    ]
    assert [(int(row[1], 16), row[2]) for row in rows] == [(i // 512, i % 512) for i in range(1025)]
    assert archive_member_progress(db, key).members == 1025
    db.close()


def test_child_checkpoint_and_source_cursor_rollback_together(tmp_path):
    reader, _, _, admission, _ = fixture()
    db, key = open_work(tmp_path / "members.sqlite", admission)
    authenticate_metadata(db, key, admission, reader)
    owner = ArchiveMemberRegistration(admission, reader)
    before = archive_member_progress(db, key)
    page = prepare_archive_member(db, key, owner)
    db.execute(
        "CREATE TRIGGER fail_member BEFORE INSERT ON discovery_archive_members "
        "BEGIN SELECT RAISE(ABORT,'injected member failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected"):
        apply_archive_member(db, key, owner, page)
    assert archive_member_progress(db, key) == before
    assert db.execute("SELECT count(*) FROM discovery_archive_members").fetchone() == (0,)
    db.execute("DROP TRIGGER fail_member")
    with write(db):
        apply_archive_member(db, key, owner, page)
    with write(db), pytest.raises(StaleArchiveTask, match="version"):
        apply_archive_member(db, key, owner, page)
    db.close()


@pytest.mark.parametrize("damage", ["tree", "missing_page"])
def test_terminal_requires_independent_root_coverage(tmp_path, damage):
    reader, _, bindings, admission, _ = fixture(wrong_tree=damage == "tree")
    db, key = open_work(tmp_path / "members.sqlite", admission)
    authenticate_metadata(db, key, admission, reader)
    if damage == "missing_page":
        db.execute("DELETE FROM discovery_archive_binding_pages WHERE sequence=?", (f"{2:064x}",))
    owner = ArchiveMemberRegistration(admission, reader)
    with pytest.raises(StaleArchiveTask, match="terminal does not cover"):
        while not archive_member_progress(db, key).complete:
            advance(db, key, owner)
    result = archive_member_progress(db, key)
    assert not result.complete
    assert result.members == (len(bindings) if damage == "tree" else 14)
    db.close()


def test_unverified_or_corrupted_payload_and_changed_worker_cannot_register(tmp_path):
    reader, objects, _, admission, _ = fixture()
    db, key = open_work(tmp_path / "members.sqlite", admission)
    with write(db), pytest.raises(StaleArchiveTask, match="metadata is incomplete"):
        begin_archive_members(db, key, admission)
    authenticate_metadata(db, key, admission, reader)
    owner = ArchiveMemberRegistration(admission, reader)
    with pytest.raises(StaleArchiveTask, match="source/build/worker"):
        prepare_archive_member(db, replace(key, worker_id="other"), owner)
    with write(db), pytest.raises(ValueError, match="no active"):
        prepare_archive_member(db, key, owner)
    descriptor = ProvenanceVolumeDocument.from_json_bytes(
        db.execute("SELECT descriptor_json FROM discovery_archive_binding_pages LIMIT 1")
        .fetchone()[0]
        .encode()
    )
    objects[descriptor.payload.path] = b"changed"
    before = archive_member_progress(db, key)
    with pytest.raises(ProvenanceArchiveReadError, match="exact descriptor"):
        prepare_archive_member(db, key, owner)
    assert archive_member_progress(db, key) == before
    db.close()


def test_registration_requires_exact_detached_owner_page_and_transaction(tmp_path):
    reader, _, _, admission, _ = fixture()
    db, key = open_work(tmp_path / "members.sqlite", admission)
    authenticate_metadata(db, key, admission, reader)
    owner = ArchiveMemberRegistration(admission, reader)
    page = prepare_archive_member(db, key, owner)
    before = archive_member_progress(db, key)
    with write(db), pytest.raises(StaleArchiveTask, match="current exact"):
        apply_archive_member(db, key, owner, replace(page, terminal=True))
    with pytest.raises(ValueError, match="owner-managed"):
        apply_archive_member(db, key, owner, page)
    replacement = ArchiveMemberRegistration(admission, reader)
    with write(db), pytest.raises(StaleArchiveTask, match="current exact"):
        apply_archive_member(db, key, replacement, page)
    assert archive_member_progress(db, key) == before
    with write(db):
        apply_archive_member(db, key, owner, page)
    db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-archive-member-preparation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_member_work.prepare_archive_member",
)
def test_measured_member_preparation_bounds_current_source_work(
    tmp_path, monkeypatch, record_property
):
    _measured(tmp_path, monkeypatch, record_property, prepare=True)


@pytest.mark.growth_witness(
    "canonical-discovery-archive-member-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_member_work.apply_archive_member",
)
def test_measured_member_apply_has_fixed_sql_without_tree_hashing(
    tmp_path, monkeypatch, record_property
):
    _measured(tmp_path, monkeypatch, record_property, prepare=False)


def _measured(tmp_path, monkeypatch, record_property, *, prepare):
    import riverhog_archive_contracts.member_binding_tree as tree_owner
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    reader, _, _, admission, _ = fixture()
    db, key = open_work(tmp_path / "measured.sqlite", admission)
    authenticate_metadata(db, key, admission, reader)
    owner = ArchiveMemberRegistration(admission, reader)
    binding = growth_binding(prepare_archive_member if prepare else apply_archive_member)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    actual_leaf, actual_node = tree_owner._leaf, tree_owner._node
    samples = []
    while not (before := archive_member_progress(db, key)).complete:
        recorder = RecordedConnection(db)
        observed = {"binding_leaf_hashes": 0, "binding_branch_hashes": 0}

        def leaf(value, observed=observed):
            assert not db.in_transaction or not prepare
            observed["binding_leaf_hashes"] += 1
            return actual_leaf(value)

        def node(left, right, observed=observed):
            assert not db.in_transaction or not prepare
            observed["binding_branch_hashes"] += 1
            return actual_node(left, right)

        if not prepare:
            page = prepare_archive_member(db, key, owner)
        with monkeypatch.context() as patch:
            patch.setattr(tree_owner, "_leaf", leaf)
            patch.setattr(tree_owner, "_node", node)
            if prepare:
                page = prepare_archive_member(recorder, key, owner)
            else:
                with write(db):
                    apply_archive_member(recorder, key, owner, page)
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": declaration_sha,
            "dimensions": {"registered_members": before.members},
            "metrics": {
                "sql_statements": recorder.metrics["sql_statements"],
                "returned_rows": recorder.metrics["returned_rows"],
                **observed,
            },
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)
        if prepare:
            with write(db):
                apply_archive_member(db, key, owner, page)
    db.close()
    record_property("growth_samples", samples)
