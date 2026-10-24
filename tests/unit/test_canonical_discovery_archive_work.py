from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace

import pytest
from riverhog_archive_contracts import (
    ArchiveProvenanceIdentity,
    CollectionArchiveManifest,
    CollectionArtifactSetIdentity,
    ProvenanceRootDocument,
    ProvenanceRootIdentity,
    ProvenanceVolumeDocument,
    format_archive_sequence,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.canonical_discovery_archive_work import (
    ArchiveDiscoveryAdmission,
    ArchiveTaskKey,
    StaleArchiveTask,
    admit_archive_journal,
    apply_archive_descriptor,
    archive_journal_segment,
    archive_metadata_progress,
    begin_archive_metadata,
    initialize_archive_work,
    prepare_archive_descriptor,
)
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey, initialize_journal_work
from riverhog_core.provenance_archive_read import (
    CanonicalProvenanceArchiveReader,
    ProvenanceArchiveReadError,
)
from riverhog_provenance import validate_journal, validate_journal_chunks

from tests.support.canonical_discovery import native_scalar_fixture
from tests.support.canonical_discovery_work import write
from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission


def open_work(path, admission):
    db = sqlite3.connect(path, isolation_level=None)
    initialize_archive_work(db)
    key = ArchiveTaskKey("build", admission.identity, "worker")
    with write(db):
        begin_archive_metadata(db, key, admission)
    return db, key


def drive(db, key, admission, reader):
    while not archive_metadata_progress(db, key).complete:
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)


def root_for(objects):
    return ProvenanceRootDocument.from_json_bytes(objects["provenance/root.json.age"])


def test_root_pair_requires_exact_archive_preimage_and_provenance_binding():
    _, objects, _, _ = archive_reader_fixture()
    root = root_for(objects)
    admission = discovery_archive_admission(root)
    assert admission.root == root
    with pytest.raises(StaleArchiveTask, match="selected exact root"):
        ArchiveDiscoveryAdmission(b"{}", root.to_json_bytes(), expected_archive_sha256="a" * 64)
    changed = replace(root, journal_count=2)
    archive = CollectionArchiveManifest(
        archive_generation=root.archive_generation,
        artifact_set=CollectionArtifactSetIdentity(1, 7, root.artifact_set_sha256),
        ordered_volume_sha256="f" * 64,
        provenance=ArchiveProvenanceIdentity(
            root.identity,
            ProvenanceRootIdentity(
                "provenance-root",
                "provenance-root",
                "provenance/root.json.age",
                len(root.to_json_bytes()),
                root.identity,
                1234,
                "0" * 64,
            ),
        ),
    )
    raw = archive.to_json_bytes()
    with pytest.raises(StaleArchiveTask, match="roots disagree"):
        ArchiveDiscoveryAdmission(
            raw, changed.to_json_bytes(), expected_archive_sha256=hashlib.sha256(raw).hexdigest()
        )


def test_metadata_resume_reads_only_next_descriptors_and_registers_exact_children(tmp_path):
    _, objects, journal_id, text = archive_reader_fixture()
    root = root_for(objects)
    admission = discovery_archive_admission(root)
    reads = []

    def read(path):
        assert not db.in_transaction
        reads.append(path)
        yield objects[path]

    reader = CanonicalProvenanceArchiveReader(
        read,
        expected_root_sha256=root.identity,
        archive_generation=root.archive_generation,
        artifact_set_sha256=root.artifact_set_sha256,
    )
    db, key = open_work(tmp_path / "work.sqlite", admission)
    with write(db):
        assert begin_archive_metadata(db, key, admission) == archive_metadata_progress(db, key)
    for _ in range(2):
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    before = archive_metadata_progress(db, key)
    db.close()
    admission = discovery_archive_admission(root)
    db, key = open_work(tmp_path / "work.sqlite", admission)
    reads.clear()
    assert archive_metadata_progress(db, key) == before
    drive(db, key, admission, reader)
    metadata_reads = [path for path in reads if path.startswith("provenance/metadata/")]
    assert metadata_reads == [
        f"provenance/metadata/volume-{format_archive_sequence(i)}.json.age" for i in (2, 3)
    ]
    result = archive_metadata_progress(db, key)
    assert result.complete and result.bindings == result.journals == 1
    assert db.execute(
        "SELECT journal_id,source_bytes,source_sha256 FROM discovery_archive_journals"
    ).fetchall() == [
        (journal_id, str(len(text.encode())), hashlib.sha256(text.encode()).hexdigest())
    ]
    assert db.execute("SELECT count(*) FROM discovery_archive_binding_pages").fetchone() == (1,)
    db.close()


def test_archived_native_journal_feeds_bounded_fact_driver_only_after_metadata_terminal(tmp_path):
    fixture = native_scalar_fixture(3)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as original:
        reader, objects, _, _ = archive_reader_fixture(fixture.raw, original.journal_id)
    admission = discovery_archive_admission(root_for(objects))
    db, key = open_work(tmp_path / "work.sqlite", admission)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as pending:
        with pytest.raises(StaleArchiveTask, match="coverage is incomplete"):
            admit_archive_journal(db, key, admission, pending)
    drive(db, key, admission, reader)

    def archived_bytes():
        after = None
        while segment := archive_journal_segment(
            db, key, original.journal_id, after_sequence=after
        ):
            yield reader.payload_bytes(segment)
            after = segment.sequence

    with validate_journal_chunks(archived_bytes(), catalog=fixture.catalog) as summary:
        initialize_journal_work(db)
        with admit_archive_journal(db, key, admission, summary) as source:
            driver = JournalFactExtraction(
                db, JournalTaskKey(key.build_id, source.source_key, key.worker_id), source
            )
            while not driver.advance(action_budget=128).parent.complete:
                pass
            assert db.execute("SELECT count(*) FROM discovery_journal_assertions").fetchone() == (
                7,
            )
    db.close()


def test_source_child_and_cursor_registration_roll_back_together(tmp_path):
    reader, objects, _, _ = archive_reader_fixture()
    admission = discovery_archive_admission(root_for(objects))
    db, key = open_work(tmp_path / "work.sqlite", admission)
    before = archive_metadata_progress(db, key)
    page = prepare_archive_descriptor(db, key, admission, reader)
    db.execute(
        "CREATE TRIGGER fail_archive BEFORE INSERT ON discovery_archive_binding_pages "
        "BEGIN SELECT RAISE(ABORT,'injected archive failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected archive"):
        apply_archive_descriptor(db, key, admission, page)
    assert archive_metadata_progress(db, key) == before
    assert db.execute("SELECT count(*) FROM discovery_archive_binding_pages").fetchone() == (0,)
    db.execute("DROP TRIGGER fail_archive")
    with write(db):
        apply_archive_descriptor(db, key, admission, page)
    with write(db), pytest.raises(StaleArchiveTask, match="version"):
        apply_archive_descriptor(db, key, admission, page)
    db.close()


@pytest.mark.parametrize("change", ["commitment", "bindings", "journals"])
def test_terminal_requires_independently_rooted_commitment_and_counts(tmp_path, change):
    reader, objects, _, _ = archive_reader_fixture()
    root = root_for(objects)
    root = replace(
        root,
        **{
            "commitment": {"ordered_volume_sha256": "0" * 64},
            "bindings": {"binding_count": 2},
            "journals": {"journal_count": 2},
        }[change],
    )
    admission = discovery_archive_admission(root)
    db, key = open_work(tmp_path / "work.sqlite", admission)
    for _ in range(3):
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    with pytest.raises(StaleArchiveTask, match="terminal does not cover"):
        prepare_archive_descriptor(db, key, admission, reader)
    assert not archive_metadata_progress(db, key).complete
    db.close()


@pytest.mark.parametrize("change", ["sequence", "generation", "gap", "overflow"])
def test_descriptor_rejects_wrong_position_authority_and_journal_extent(tmp_path, change):
    reader, objects, _, _ = archive_reader_fixture()
    admission = discovery_archive_admission(root_for(objects))
    db, key = open_work(tmp_path / "work.sqlite", admission)
    if change in {"gap", "overflow"}:
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    sequence = archive_metadata_progress(db, key).next_sequence
    path = f"provenance/metadata/volume-{format_archive_sequence(sequence)}.json.age"
    doc = ProvenanceVolumeDocument.from_json_bytes(objects[path])
    if change == "overflow":
        value = doc.to_mapping()
        value["journal_range"]["bytes"] = "1"
        objects[path] = canonical_json_bytes(value)
    else:
        changed = replace(
            doc,
            **{
                "sequence": {
                    "sequence": sequence + 1,
                    "payload": replace(doc.payload, sequence=sequence + 1),
                },
                "generation": {"archive_generation": "e" * 64},
                "gap": {"journal_offset": 1},
            }[change],
        )
        objects[path] = changed.to_json_bytes()
    before = archive_metadata_progress(db, key)
    with pytest.raises((ValueError, StaleArchiveTask)):
        prepare_archive_descriptor(db, key, admission, reader)
    assert archive_metadata_progress(db, key) == before
    db.close()


def test_changed_worker_source_and_forged_preparation_are_rejected(tmp_path):
    reader, objects, _, _ = archive_reader_fixture()
    admission = discovery_archive_admission(root_for(objects))
    db, key = open_work(tmp_path / "work.sqlite", admission)
    with write(db), pytest.raises(StaleArchiveTask, match="registration"):
        begin_archive_metadata(db, replace(key, worker_id="other"), admission)
    with pytest.raises(StaleArchiveTask, match="source/build/worker"):
        prepare_archive_descriptor(db, replace(key, worker_id="other"), admission, reader)
    with write(db), pytest.raises(ValueError, match="no active"):
        prepare_archive_descriptor(db, key, admission, reader)
    page = prepare_archive_descriptor(db, key, admission, reader)
    with write(db), pytest.raises(StaleArchiveTask, match="current exact"):
        apply_archive_descriptor(db, key, admission, replace(page, terminal=True))
    with pytest.raises(StaleArchiveTask, match="admission binding"):
        with write(db):
            begin_archive_metadata(db, replace(key, admission_sha256="f" * 64), admission)
    db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-archive-descriptor-preparation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_work.prepare_archive_descriptor",
)
def test_measured_metadata_preparation_reads_and_hashes_one_next_descriptor(
    tmp_path, monkeypatch, record_property
):
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )
    from sha256_continuation import CheckpointSHA256

    from tests.support.canonical_discovery_work import RecordedConnection

    _, objects, _, _ = archive_reader_fixture()
    root = root_for(objects)
    admission = discovery_archive_admission(root)
    binding = growth_binding(prepare_archive_descriptor)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    db, key = open_work(tmp_path / "measured.sqlite", admission)
    samples = []
    while not archive_metadata_progress(db, key).complete:
        sequence = archive_metadata_progress(db, key).next_sequence
        path = f"provenance/metadata/volume-{format_archive_sequence(sequence)}.json.age"
        recorder = RecordedConnection(db)
        observed = {"object_reads": 0, "payload_reads": 0, "checkpoint_input_octets": 0}
        actual_update = CheckpointSHA256.update_bytes

        def read(path, observed=observed):
            assert not db.in_transaction
            observed["object_reads"] += 1
            observed["payload_reads"] += int(path.startswith("provenance/payloads/"))
            yield objects[path]

        def update(self, value, observed=observed, actual_update=actual_update):
            assert not db.in_transaction
            observed["checkpoint_input_octets"] += len(value)
            return actual_update(self, value)

        reader = CanonicalProvenanceArchiveReader(
            read,
            expected_root_sha256=root.identity,
            archive_generation=root.archive_generation,
            artifact_set_sha256=root.artifact_set_sha256,
        )
        with monkeypatch.context() as patch:
            patch.setattr(CheckpointSHA256, "update_bytes", update)
            page = prepare_archive_descriptor(recorder, key, admission, reader)
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": declaration_sha,
            "dimensions": {"descriptor_octets": len(objects[path])},
            "metrics": {
                "sql_statements": recorder.metrics["sql_statements"],
                "returned_rows": recorder.metrics["returned_rows"],
                **observed,
            },
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    db.close()
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "canonical-discovery-archive-descriptor-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_work.apply_archive_descriptor",
)
def test_measured_metadata_apply_has_fixed_sql_and_no_source_hashing(
    tmp_path, monkeypatch, record_property
):
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )
    from sha256_continuation import CheckpointSHA256

    from tests.support.canonical_discovery_work import RecordedConnection

    reader, objects, _, _ = archive_reader_fixture()
    admission = discovery_archive_admission(root_for(objects))
    binding = growth_binding(apply_archive_descriptor)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    db, key = open_work(tmp_path / "measured-apply.sqlite", admission)
    samples = []
    while not archive_metadata_progress(db, key).complete:
        page = prepare_archive_descriptor(db, key, admission, reader)
        recorder = RecordedConnection(db)
        checkpoint_octets = 0
        actual_update = CheckpointSHA256.update_bytes

        def update(self, value, actual_update=actual_update):
            nonlocal checkpoint_octets
            checkpoint_octets += len(value)
            return actual_update(self, value)

        with monkeypatch.context() as patch:
            patch.setattr(CheckpointSHA256, "update_bytes", update)
            with write(db):
                apply_archive_descriptor(recorder, key, admission, page)
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": declaration_sha,
            "dimensions": {"descriptor_octets": len(page.descriptor_json.encode())},
            "metrics": {
                "sql_statements": recorder.metrics["sql_statements"],
                "returned_rows": recorder.metrics["returned_rows"],
                "checkpoint_input_octets": checkpoint_octets,
            },
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)
    db.close()
    record_property("growth_samples", samples)


def test_native_prefix_with_same_journal_id_cannot_replace_archived_complete_source(tmp_path):
    from tests.support.canonical_discovery_work import corrected_journal_fixture

    fixture, raw, _ = corrected_journal_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as complete:
        reader, objects, _, _ = archive_reader_fixture(raw, complete.journal_id)
        admission = discovery_archive_admission(root_for(objects))
        db, key = open_work(tmp_path / "work.sqlite", admission)
        drive(db, key, admission, reader)
        with validate_journal(fixture.raw, catalog=fixture.catalog) as prefix:
            assert prefix.journal_id == complete.journal_id
            with pytest.raises(StaleArchiveTask, match="complete source"):
                admit_archive_journal(db, key, admission, prefix)
        with admit_archive_journal(db, key, admission, complete) as source:
            assert source.entry_count == len(complete.frames)
        db.close()


def test_missing_terminal_retains_incomplete_metadata_source(tmp_path):
    reader, objects, _, _ = archive_reader_fixture()
    admission = discovery_archive_admission(root_for(objects))
    db, key = open_work(tmp_path / "work.sqlite", admission)
    for _ in range(3):
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    before = archive_metadata_progress(db, key)
    objects.pop(f"provenance/metadata/volume-{format_archive_sequence(3)}.json.age")
    with pytest.raises(KeyError):
        prepare_archive_descriptor(db, key, admission, reader)
    assert archive_metadata_progress(db, key) == before
    assert not before.complete
    db.close()


def test_authenticated_segment_directory_does_not_rescan_and_checks_payload_fixity(
    tmp_path, monkeypatch
):
    reader, objects, journal_id, text = archive_reader_fixture()
    admission = discovery_archive_admission(root_for(objects))
    db, key = open_work(tmp_path / "work.sqlite", admission)
    drive(db, key, admission, reader)

    def forbidden():
        raise AssertionError("complete metadata sequence was rescanned")

    monkeypatch.setattr(reader, "scan", forbidden)
    first = archive_journal_segment(db, key, journal_id)
    assert first.sequence == 1
    second = archive_journal_segment(db, key, journal_id, after_sequence=first.sequence)
    assert second.sequence == 2
    assert archive_journal_segment(db, key, journal_id, after_sequence=second.sequence) is None
    assert reader.payload_bytes(first) + reader.payload_bytes(second) == text.encode()
    objects[first.payload.path] = b"corrupt"
    with pytest.raises(ProvenanceArchiveReadError, match="exact descriptor"):
        reader.payload_bytes(first)
    db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-archive-segment-selection/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_work.archive_journal_segment",
)
def test_measured_segment_selection_uses_detached_point_reads(tmp_path, record_property):
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    from tests.support.canonical_discovery_work import RecordedConnection

    reader, objects, journal_id, _ = archive_reader_fixture()
    admission = discovery_archive_admission(root_for(objects))
    db, key = open_work(tmp_path / "segment-selection.sqlite", admission)
    drive(db, key, admission, reader)
    binding = growth_binding(archive_journal_segment)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    after = None
    while True:
        recorder = RecordedConnection(db)
        selected = archive_journal_segment(recorder, key, journal_id, after_sequence=after)
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": declaration_sha,
            "dimensions": {"registered_segments": 2},
            "metrics": {
                "sql_statements": recorder.metrics["sql_statements"],
                "returned_rows": recorder.metrics["returned_rows"],
            },
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)
        if selected is None:
            break
        after = selected.sequence
    db.close()
    record_property("growth_samples", samples)
