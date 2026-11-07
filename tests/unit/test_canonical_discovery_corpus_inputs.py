from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from dataclasses import replace

import pytest
from riverhog_archive_contracts import ProvenanceRootDocument
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json
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
    FrozenCorpusSourceInputs,
    apply_corpus_source_input,
    begin_corpus_source_inputs,
    complete_corpus_source_inputs,
    corpus_source_admitted,
    initialize_corpus_source_inputs,
    prepare_corpus_source_input,
)
from riverhog_core.canonical_discovery_journal_work import QualifiedJournalSource, StaleJournalTask
from riverhog_provenance import validate_journal

from tests.support.canonical_discovery_work import (
    MeasuredConnection,
    corrected_journal_fixture,
    metadata_octets,
    write,
)
from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission


@pytest.fixture(scope="module")
def native():
    fixture, raw, _ = corrected_journal_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        yield raw, summary


def register_archive(db, raw, summary, *, total_bytes=7, worker="worker", build="build"):
    reader, objects, _, _ = archive_reader_fixture(raw, summary.journal_id)
    root = ProvenanceRootDocument.from_json_bytes(objects["provenance/root.json.age"])
    admission = discovery_archive_admission(root, total_bytes=total_bytes)
    key = ArchiveTaskKey(build, admission.identity, worker)
    with write(db):
        begin_archive_metadata(db, key, admission)
    while not archive_metadata_progress(db, key).complete:
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    with write(db):
        begin_corpus_source_inputs(db, key, admission)
    return key, admission


@pytest.fixture
def owned(tmp_path, native):
    raw, summary = native
    db = sqlite3.connect(tmp_path / "inputs.sqlite", isolation_level=None)
    initialize_archive_work(db)
    initialize_corpus_source_inputs(db)
    key, admission = register_archive(db, raw, summary)
    with admit_archive_journal(db, key, admission, summary) as source:
        yield db, key, admission, CorpusSourceInputWork(admission, source)
    db.close()


def commit_input(db, key, owner):
    page = prepare_corpus_source_input(db, key, owner)
    with write(db):
        progress = apply_corpus_source_input(db, key, owner, page)
    return page, progress


def test_independent_archive_count_retry_freeze_and_exact_membership(owned):
    db, key, admission, owner = owned
    assert admission.root.journal_count == 1
    with write(db), pytest.raises(StaleArchiveTask, match="incomplete"):
        complete_corpus_source_inputs(db, key, admission)
    page, progress = commit_input(db, key, owner)
    assert progress.registered == 1 and not progress.complete
    with write(db):
        assert apply_corpus_source_input(db, key, owner, page) == progress
        manifest = complete_corpus_source_inputs(db, key, admission)
        assert complete_corpus_source_inputs(db, key, admission) == manifest
        assert begin_corpus_source_inputs(db, key, admission).complete
    assert manifest.source_count == 1
    expected = canonical_json_bytes(
        {
            "format": "riverhog-discovery-corpus-source-inputs/v1",
            "archive_admission_sha256": admission.identity,
            "source_count": "1",
        }
    )
    assert manifest.manifest_sha256 == hashlib.sha256(expected).hexdigest()
    assert corpus_source_admitted(db, key, admission, manifest, owner.anchor)
    with write(db):
        assert apply_corpus_source_input(db, key, owner, page).complete


@pytest.mark.parametrize(
    "field,value",
    [
        ("journal_id", "urn:uuid:11111111-1111-4111-8111-111111111111"),
        ("through_entry_id", "urn:uuid:22222222-2222-4222-8222-222222222222"),
        ("through_sequence", 99),
        ("through_json_sha256", "0" * 64),
        ("prefix_sha256", "0" * 64),
        ("prefix_bytes", 1),
    ],
)
def test_membership_checks_each_original_anchor_coordinate(owned, field, value):
    db, key, admission, owner = owned
    commit_input(db, key, owner)
    with write(db):
        manifest = complete_corpus_source_inputs(db, key, admission)
    changed = replace(owner.anchor, **{field: value})
    assert changed != owner.anchor
    assert not corpus_source_admitted(db, key, admission, manifest, changed)


def test_same_native_anchor_preserves_independent_archive_admissions(owned, native):
    db, first, admission, owner = owned
    commit_input(db, first, owner)
    with write(db):
        first_manifest = complete_corpus_source_inputs(db, first, admission)
    raw, summary = native
    second, other = register_archive(db, raw, summary, total_bytes=8)
    assert other.identity != admission.identity
    with admit_archive_journal(db, second, other, summary) as source:
        other_owner = CorpusSourceInputWork(other, source)
        assert other_owner.anchor == owner.anchor
        assert other_owner.source_key != owner.source_key
        commit_input(db, second, other_owner)
        with write(db):
            second_manifest = complete_corpus_source_inputs(db, second, other)
    assert first_manifest.manifest_sha256 != second_manifest.manifest_sha256
    assert db.execute("SELECT count(*) FROM discovery_corpus_source_inputs").fetchone() == (2,)
    with pytest.raises(StaleArchiveTask, match="exact completed"):
        corpus_source_admitted(db, second, other, first_manifest, owner.anchor)


@pytest.mark.parametrize("field,value", [("worker_id", "other"), ("build_id", "other")])
def test_changed_owner_fails_before_input_registration(owned, field, value):
    db, key, admission, owner = owned
    changed = replace(key, **{field: value})
    with pytest.raises(StaleArchiveTask, match="not current"):
        prepare_corpus_source_input(db, changed, owner)
    with write(db), pytest.raises(StaleArchiveTask):
        complete_corpus_source_inputs(db, changed, admission)
    assert db.execute("SELECT count(*) FROM discovery_corpus_source_inputs").fetchone() == (0,)


def test_partial_native_prefix_is_not_a_complete_source_input(owned, native):
    db, key, admission, _ = owned
    _, summary = native
    with QualifiedJournalSource(summary, admission.identity, through=0) as source:
        with pytest.raises(StaleArchiveTask, match="complete root-selected"):
            prepare_corpus_source_input(db, key, CorpusSourceInputWork(admission, source))
    with write(db), pytest.raises(StaleArchiveTask, match="incomplete"):
        complete_corpus_source_inputs(db, key, admission)


def test_forged_preparation_and_changed_original_coordinates_fail_closed(owned):
    db, key, admission, owner = owned
    page = prepare_corpus_source_input(db, key, owner)
    with write(db), pytest.raises(StaleArchiveTask, match="exact preparation"):
        apply_corpus_source_input(db, key, owner, replace(page))
    altered = json.loads(owner.source.binding_json)
    altered["anchor"]["through"]["json_sha256"] = "e" * 64
    owner.source.binding_json = canonical_json_bytes(altered).decode()
    owner.source.source_key = hashlib.sha256(owner.source.binding_json.encode()).hexdigest()
    forged_owner = CorpusSourceInputWork(admission, owner.source)
    with pytest.raises(StaleArchiveTask, match="terminal coordinates"):
        prepare_corpus_source_input(db, key, forged_owner)
    assert db.execute("SELECT count(*) FROM discovery_corpus_source_inputs").fetchone() == (0,)


def test_closed_source_and_interrupted_registration_do_not_advance_counters(owned):
    db, key, admission, owner = owned
    page = prepare_corpus_source_input(db, key, owner)
    before = tuple(db.iterdump())

    def deny(action, table, column, database, trigger):
        if action == sqlite3.SQLITE_UPDATE and table == "discovery_corpus_source_manifests":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    db.set_authorizer(deny)
    try:
        with write(db), pytest.raises(sqlite3.DatabaseError):
            apply_corpus_source_input(db, key, owner, page)
    finally:
        db.set_authorizer(None)
    assert tuple(db.iterdump()) == before
    owner.source.close()
    with write(db), pytest.raises(StaleJournalTask, match="closed"):
        apply_corpus_source_input(db, key, owner, page)
    assert tuple(db.iterdump()) == before


def test_frozen_manifest_labels_alone_do_not_admit_an_input(owned):
    db, key, admission, owner = owned
    fake = FrozenCorpusSourceInputs(admission.identity, "0" * 64, 1)
    with pytest.raises(StaleArchiveTask, match="exact completed"):
        corpus_source_admitted(db, key, admission, fake, owner.anchor)
    commit_input(db, key, owner)
    with write(db):
        manifest = complete_corpus_source_inputs(db, key, admission)
    for altered in (replace(manifest, source_count=2), replace(manifest, manifest_sha256="f" * 64)):
        with pytest.raises(StaleArchiveTask, match="exact completed"):
            corpus_source_admitted(db, key, admission, altered, owner.anchor)


def test_manifest_identity_and_progress_survive_connection_replacement(owned):
    db, key, admission, owner = owned
    commit_input(db, key, owner)
    path = db.execute("PRAGMA database_list").fetchone()[2]
    with write(db):
        expected = complete_corpus_source_inputs(db, key, admission)
    with sqlite3.connect(path, isolation_level=None) as replacement:
        initialize_corpus_source_inputs(replacement)
        with write(replacement):
            assert begin_corpus_source_inputs(replacement, key, admission).complete
            assert complete_corpus_source_inputs(replacement, key, admission) == expected
        assert corpus_source_admitted(replacement, key, admission, expected, owner.anchor)


def test_mutated_root_count_cannot_replace_authenticated_expectations(owned):
    db, key, admission, _ = owned
    admission.root = replace(admission.root, journal_count=2)
    with write(db), pytest.raises(StaleArchiveTask, match="root preimage"):
        begin_corpus_source_inputs(db, key, admission)


def _selected_rows(db, key):
    result = {}
    for table in (
        "discovery_archive_tasks",
        "discovery_corpus_source_manifests",
        "discovery_corpus_source_inputs",
    ):
        result[table] = db.execute(
            "SELECT * FROM " + table + " WHERE build_id=? AND admission_sha256=?",
            (key.build_id, key.admission_sha256),
        ).fetchall()
    return result


@pytest.fixture(scope="module")
def source_templates(tmp_path_factory, native):
    raw, summary = native
    root = tmp_path_factory.mktemp("corpus-input-templates")
    templates = {}
    for retained in (1, 17, 129):
        path = root / (str(retained) + ".sqlite")
        db = sqlite3.connect(path, isolation_level=None)
        initialize_archive_work(db)
        initialize_corpus_source_inputs(db)
        key, admission = register_archive(db, raw, summary)
        for index in range(retained - 1):
            other, other_admission = register_archive(
                db, raw, summary, build="unrelated-" + str(index)
            )
            with admit_archive_journal(db, other, other_admission, summary) as source:
                commit_input(db, other, CorpusSourceInputWork(other_admission, source))
        db.execute(
            "UPDATE discovery_corpus_source_inputs SET source_binding_json=? WHERE build_id<>?",
            ("unrelated damaged cache record" * 3000, key.build_id),
        )
        db.close()
        templates[retained] = path, key, admission
    return templates


def _measured_inputs(tmp_path, native, monkeypatch, record_property, mode, source_templates):
    from riverhog_core import canonical_discovery_corpus_inputs as inputs
    from riverhog_core import canonical_discovery_journal_work as journal_work
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )
    from riverhog_provenance import JournalSummary

    operation = {
        "begin": inputs.begin_corpus_source_inputs,
        "prepare": inputs.prepare_corpus_source_input,
        "apply": inputs.apply_corpus_source_input,
        "complete": inputs.complete_corpus_source_inputs,
        "membership": inputs.corpus_source_admitted,
    }[mode]
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    _, summary = native
    actual_anchor = JournalSummary.anchor_at
    actual_encode, actual_hash = inputs.canonical_json_bytes, inputs.sha256
    samples = []
    for retained in (1, 17, 129):
        path = tmp_path / (mode + "-" + str(retained) + ".sqlite")
        template, key, admission = source_templates[retained]
        shutil.copyfile(template, path)
        db = sqlite3.connect(path, isolation_level=None)
        with admit_archive_journal(db, key, admission, summary) as source:
            owner = CorpusSourceInputWork(admission, source)
            if mode == "begin":
                db.execute(
                    "DELETE FROM discovery_corpus_source_manifests WHERE build_id=?",
                    (key.build_id,),
                )
            elif mode in ("complete", "membership"):
                commit_input(db, key, owner)
            frozen = None
            if mode == "membership":
                with write(db):
                    frozen = complete_corpus_source_inputs(db, key, admission)
            for cache in ("warm", "reopened"):
                if cache == "reopened":
                    db.close()
                    db = sqlite3.connect(path, isolation_level=None)
                page = prepare_corpus_source_input(db, key, owner) if mode == "apply" else None
                before = _selected_rows(db, key)
                row_widths = [
                    sum(metadata_octets(value) for value in row)
                    for rows in before.values()
                    for row in rows
                ]
                incoming = (
                    key.build_id,
                    key.admission_sha256,
                    key.worker_id,
                    admission.root_json,
                    admission.binding_json,
                )
                row_widths.append(sum(metadata_octets(value) for value in incoming))
                if mode in ("prepare", "apply"):
                    row_widths.append(
                        sum(
                            metadata_octets(value)
                            for value in (
                                key.build_id,
                                key.admission_sha256,
                                owner.anchor.journal_id,
                                owner.source_key,
                                owner.source_binding_json,
                                canonical_json_bytes(owner.anchor.to_mapping()).decode(),
                            )
                        )
                    )
                dimensions = {
                    "registered_inputs": sum(
                        db.execute("SELECT count(*) FROM " + table).fetchone()[0]
                        for table in (
                            "discovery_archive_tasks",
                            "discovery_archive_journals",
                            "discovery_corpus_source_manifests",
                            "discovery_corpus_source_inputs",
                        )
                    ),
                    "metadata_octets": max(row_widths),
                }
                recorder = MeasuredConnection(db)

                def anchor(self, sequence, recorder=recorder):
                    recorder.metrics["native_anchor_calls"] += 1
                    return actual_anchor(self, sequence)

                def encode(value, recorder=recorder):
                    encoded = actual_encode(value)
                    recorder.metrics["encoded_octets"] += len(encoded)
                    return encoded

                def digest(value=b"", recorder=recorder):
                    recorder.metrics["hashed_octets"] += len(value)
                    return actual_hash(value)

                def instruction(recorder=recorder):
                    recorder.metrics["sqlite_vm_instructions"] += 1
                    return 0

                def invoke(
                    db=db,
                    recorder=recorder,
                    key=key,
                    admission=admission,
                    owner=owner,
                    page=page,
                    frozen=frozen,
                ):
                    with monkeypatch.context() as patch:
                        patch.setattr(JournalSummary, "anchor_at", anchor)
                        patch.setattr(inputs, "canonical_json_bytes", encode)
                        patch.setitem(
                            require_canonical_json.__globals__, "canonical_json_bytes", encode
                        )
                        patch.setattr(journal_work, "canonical_json_bytes", encode)
                        patch.setattr(inputs, "sha256", digest)
                        db.set_progress_handler(instruction, 1)
                        try:
                            if mode == "begin":
                                return operation(recorder, key, admission)
                            if mode == "prepare":
                                return operation(recorder, key, owner)
                            if mode == "apply":
                                return operation(recorder, key, owner, page)
                            if mode == "complete":
                                return operation(recorder, key, admission)
                            return operation(recorder, key, admission, frozen, owner.anchor)
                        finally:
                            db.set_progress_handler(None, 0)

                if mode in ("begin", "apply", "complete"):
                    with write(db):
                        result = invoke()
                else:
                    result = invoke()
                after = _selected_rows(db, key)
                for table in before:
                    for row in after[table]:
                        if row not in before[table]:
                            recorder.metrics["written_octets"] += sum(
                                metadata_octets(v) for v in row
                            )
                for sql, parameters in recorder.statements:
                    assert "COUNT(" not in sql.upper()
                    if sql.startswith("SELECT"):
                        plan = db.execute("EXPLAIN QUERY PLAN " + sql, parameters).fetchall()
                        assert not any("SCAN " in row[3].upper() for row in plan), plan
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": declaration_sha,
                    "dimensions": dimensions,
                    "metrics": recorder.metrics,
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
                if mode == "prepare":
                    assert result.anchor == owner.anchor
                if mode == "membership":
                    assert result is True
                assert recorder.metrics["returned_rows"] <= 4
        db.close()
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-source-input-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_inputs.begin_corpus_source_inputs",
)
def test_measured_manifest_begin(tmp_path, native, monkeypatch, record_property, source_templates):
    _measured_inputs(tmp_path, native, monkeypatch, record_property, "begin", source_templates)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-source-input-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_inputs.prepare_corpus_source_input",
)
def test_measured_manifest_prepare(
    tmp_path, native, monkeypatch, record_property, source_templates
):
    _measured_inputs(tmp_path, native, monkeypatch, record_property, "prepare", source_templates)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-source-input-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_inputs.apply_corpus_source_input",
)
def test_measured_manifest_apply(tmp_path, native, monkeypatch, record_property, source_templates):
    _measured_inputs(tmp_path, native, monkeypatch, record_property, "apply", source_templates)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-source-input-complete/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_inputs.complete_corpus_source_inputs",
)
def test_measured_manifest_complete(
    tmp_path, native, monkeypatch, record_property, source_templates
):
    _measured_inputs(tmp_path, native, monkeypatch, record_property, "complete", source_templates)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-source-input-membership/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_inputs.corpus_source_admitted",
)
def test_measured_manifest_membership(
    tmp_path, native, monkeypatch, record_property, source_templates
):
    _measured_inputs(tmp_path, native, monkeypatch, record_property, "membership", source_templates)


def test_retries_compare_stored_source_preimages_inside_sql(owned):
    db, key, admission, owner = owned
    page, _ = commit_input(db, key, owner)
    with write(db):
        manifest = complete_corpus_source_inputs(db, key, admission)
    huge = "damaged retained metadata" * 10000
    db.execute(
        "UPDATE discovery_corpus_source_inputs SET source_binding_json=? "
        "WHERE build_id=? AND admission_sha256=?",
        (huge, key.build_id, key.admission_sha256),
    )
    recorder = MeasuredConnection(db)
    with write(db), pytest.raises(StaleArchiveTask, match="original source coordinates"):
        apply_corpus_source_input(recorder, key, owner, page)
    assert recorder.metrics["returned_octets"] < 4096
    assert not corpus_source_admitted(db, key, admission, manifest, owner.anchor)


@pytest.mark.parametrize("count", [True, 1.0, "1", -1])
def test_manifest_counts_keep_the_original_integer_domain(count):
    with pytest.raises(ValueError, match="nonnegative integer"):
        FrozenCorpusSourceInputs("a" * 64, "b" * 64, count)
