from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from dataclasses import replace

import pytest
from riverhog_archive_contracts import ProvenanceVolumeDocument, member_history_object_path
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.canonical_discovery_archive_work import (
    StaleArchiveTask,
    apply_archive_descriptor,
    archive_metadata_progress,
    begin_archive_metadata,
    initialize_archive_work,
    prepare_archive_descriptor,
)
from riverhog_core.canonical_discovery_corpus_members import (
    CorpusMemberRegistration,
    apply_corpus_member,
    begin_corpus_members,
    initialize_corpus_members,
    prepare_corpus_member,
)
from riverhog_core.canonical_discovery_history_work import (
    HistorySetWork,
    apply_history_record,
    archive_history_ready,
    history_descriptor_ready,
    history_set_progress,
    prepare_history_record,
)
from riverhog_core.canonical_discovery_member_work import (
    ArchiveMemberRegistration,
    apply_archive_member,
    archive_member_progress,
    prepare_archive_member,
)

from tests.support.canonical_discovery_work import MeasuredConnection, metadata_octets, write
from tests.unit.test_canonical_discovery_history_work import fixture


def setup(path, *, members=3):
    reader, objects, bindings, histories, admission, reads = fixture(count=1, members=members)
    db = sqlite3.connect(path, isolation_level=None)
    initialize_archive_work(db)
    initialize_corpus_members(db)
    from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey

    key = ArchiveTaskKey("build", admission.identity, "worker")
    with write(db):
        begin_archive_metadata(db, key, admission)
    while not archive_metadata_progress(db, key).complete:
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    with write(db):
        begin_corpus_members(db, key, admission)
    owner = CorpusMemberRegistration(admission, reader)
    return db, key, owner, bindings, histories, objects, reads


def advance(db, key, owner):
    page = prepare_corpus_member(db, key, owner)
    with write(db):
        return apply_corpus_member(db, key, owner, page)


def test_original_member_and_history_children_resume_together(tmp_path):
    path = tmp_path / "work.sqlite"
    db, key, owner, bindings, histories, _, reads = setup(path, members=17)
    reads.clear()
    advance(db, key, owner)
    assert not archive_member_progress(db, key).complete
    assert db.execute("SELECT count(*) FROM discovery_archive_histories").fetchone() == (1,)
    db.close()
    db = sqlite3.connect(path, isolation_level=None)
    owner = CorpusMemberRegistration(owner.members.admission, owner.members.reader)
    reads.clear()
    try:
        while not archive_member_progress(db, key).complete:
            advance(db, key, owner)
        with write(db):
            result = begin_corpus_members(db, key, owner.members.admission)
        assert result.members == 17 and result.complete
        assert db.execute("SELECT members FROM discovery_corpus_member_coverage").fetchone() == (
            "17",
        )
        assert db.execute("SELECT count(*) FROM discovery_archive_histories").fetchone() == (17,)
        assert db.execute("SELECT count(*) FROM discovery_history_descriptors").fetchone() == (17,)
        assert db.execute("SELECT count(*) FROM discovery_history_sets").fetchone() == (2,)
        assert db.execute("SELECT count(*) FROM discovery_history_set_records").fetchone() == (0,)
        rows = db.execute(
            "SELECT artifact_id,binding_json,history_sha256 FROM discovery_archive_histories "
            "ORDER BY artifact_id"
        ).fetchall()
        assert rows == [
            (b.artifact_id, canonical_json_bytes(b.to_mapping()).decode(), h.identity)
            for b, h in zip(bindings, histories, strict=True)
        ]
        assert not any(path.startswith("provenance/sets/") for path in reads)
        assert reads.count(member_history_object_path(bindings[0].history_sha256)) == 0
        for binding in bindings[1:]:
            assert reads.count(member_history_object_path(binding.history_sha256)) == 1
    finally:
        db.close()


@pytest.mark.parametrize(
    "table",
    [
        "discovery_archive_histories",
        "discovery_history_descriptors",
        "discovery_history_sets",
        "discovery_corpus_member_coverage",
        "discovery_corpus_history_inputs",
        "discovery_corpus_history_headers",
    ],
)
def test_child_failure_rolls_back_original_cursor_and_every_child(tmp_path, table):
    db, key, owner, _, _, _, _ = setup(tmp_path / "work.sqlite")
    try:
        page = prepare_corpus_member(db, key, owner)
        before = archive_member_progress(db, key)
        event = (
            "UPDATE"
            if table in ("discovery_corpus_member_coverage", "discovery_corpus_history_headers")
            else "INSERT"
        )
        db.execute(
            f"CREATE TRIGGER fail_child BEFORE {event} ON {table} "
            "BEGIN SELECT RAISE(ABORT,'injected child failure'); END"
        )
        with write(db), pytest.raises(sqlite3.IntegrityError, match="injected"):
            apply_corpus_member(db, key, owner, page)
        assert archive_member_progress(db, key) == before
        for child in (
            "discovery_archive_members",
            "discovery_archive_histories",
            "discovery_history_descriptors",
            "discovery_history_sets",
        ):
            assert db.execute("SELECT count(*) FROM " + child).fetchone() == (0,)
        assert db.execute("SELECT members FROM discovery_corpus_member_coverage").fetchone() == (
            "0",
        )
        db.execute("DROP TRIGGER fail_child")
        with write(db):
            result = apply_corpus_member(db, key, owner, page)
        assert result.members == 1
        with write(db), pytest.raises(StaleArchiveTask, match="coverage disagree"):
            apply_corpus_member(db, key, owner, page)
        assert archive_member_progress(db, key) == result
        assert advance(db, key, owner).members == 2
    finally:
        db.close()


@pytest.mark.parametrize("damage", ["missing", "count", "noncanonical", "worker", "bypass"])
def test_original_member_and_history_coverage_cannot_diverge(tmp_path, damage):
    db, key, owner, _, _, _, _ = setup(tmp_path / "work.sqlite")
    try:
        if damage == "missing":
            advance(db, key, owner)
            db.execute("DELETE FROM discovery_corpus_member_coverage")
        elif damage == "count":
            db.execute("UPDATE discovery_corpus_member_coverage SET members='1'")
        elif damage == "noncanonical":
            db.execute("UPDATE discovery_corpus_member_coverage SET members='00'")
        elif damage == "worker":
            db.execute("UPDATE discovery_corpus_member_coverage SET worker_id='other'")
        else:
            member = ArchiveMemberRegistration(owner.members.admission, owner.members.reader)
            page = prepare_archive_member(db, key, member)
            with write(db):
                apply_archive_member(db, key, member, page)
        with pytest.raises(StaleArchiveTask):
            prepare_corpus_member(db, key, owner)
        with write(db), pytest.raises(StaleArchiveTask):
            begin_corpus_members(db, key, owner.members.admission)
    finally:
        db.close()


def test_terminal_requires_original_root_even_with_all_history_children(tmp_path):
    db, key, owner, _, _, _, _ = setup(tmp_path / "work.sqlite", members=1)
    try:
        advance(db, key, owner)
        checkpoint = json.loads(
            db.execute("SELECT checkpoint_json FROM discovery_archive_member_tasks").fetchone()[0]
        )
        checkpoint["frontier"][0] = "0" * 64
        db.execute(
            "UPDATE discovery_archive_member_tasks SET checkpoint_json=?",
            (canonical_json_bytes(checkpoint).decode(),),
        )
        with pytest.raises(StaleArchiveTask, match="terminal does not cover"):
            prepare_corpus_member(db, key, owner)
        assert not archive_member_progress(db, key).complete
        assert db.execute("SELECT count(*) FROM discovery_archive_histories").fetchone() == (1,)
    finally:
        db.close()


def test_preparation_rejects_corrupt_exact_descriptor_without_advancement(tmp_path):
    db, key, owner, bindings, _, objects, _ = setup(tmp_path / "work.sqlite")
    try:
        objects[member_history_object_path(bindings[0].history_sha256)] = b"changed"
        before = archive_member_progress(db, key)
        with pytest.raises(ValueError, match="descriptor differs from its root binding"):
            prepare_corpus_member(db, key, owner)
        assert archive_member_progress(db, key) == before
        assert db.execute("SELECT count(*) FROM discovery_archive_members").fetchone() == (0,)
    finally:
        db.close()


def test_composite_requires_exact_owner_preparation_and_transaction(tmp_path):
    db, key, owner, _, _, _, _ = setup(tmp_path / "work.sqlite")
    try:
        page = prepare_corpus_member(db, key, owner)
        with write(db), pytest.raises(ValueError, match="no active transaction"):
            prepare_corpus_member(db, key, owner)
        with pytest.raises(ValueError, match="owner-managed"):
            apply_corpus_member(db, key, owner, page)
        for wrong_owner, wrong_page, wrong_key in (
            (owner, replace(page), key),
            (CorpusMemberRegistration(owner.members.admission, owner.members.reader), page, key),
            (owner, page, replace(key, worker_id="other")),
        ):
            with write(db), pytest.raises(StaleArchiveTask, match="exact detached"):
                apply_corpus_member(db, wrong_key, wrong_owner, wrong_page)
        replacement = prepare_corpus_member(db, key, owner)
        with write(db), pytest.raises(StaleArchiveTask):
            apply_corpus_member(db, key, owner, page)
        with write(db):
            apply_corpus_member(db, key, owner, replacement)
    finally:
        db.close()


def test_metadata_changed_after_preparation_cannot_commit_children(tmp_path):
    db, key, owner, _, _, _, _ = setup(tmp_path / "work.sqlite")
    try:
        page = prepare_corpus_member(db, key, owner)
        before = archive_member_progress(db, key)
        db.execute("UPDATE discovery_archive_tasks SET complete=0")
        with write(db), pytest.raises(StaleArchiveTask, match="metadata is incomplete"):
            apply_corpus_member(db, key, owner, page)
        assert archive_member_progress(db, key) == before
        assert db.execute("SELECT count(*) FROM discovery_archive_members").fetchone() == (0,)
    finally:
        db.close()


def test_history_children_do_not_bypass_the_independent_member_root_terminal(tmp_path):
    db, key, owner, bindings, histories, _, _ = setup(tmp_path / "work.sqlite", members=1)
    try:
        advance(db, key, owner)
        for authority in (histories[0].roots, histories[0].imports):
            work = HistorySetWork(owner.members.admission, owner.members.reader, authority)
            while not history_set_progress(db, key, work).complete:
                page = prepare_history_record(db, key, work)
                with write(db):
                    apply_history_record(db, key, work, page)
        assert history_descriptor_ready(db, key, owner.histories, histories[0].identity)
        assert not archive_history_ready(db, key, owner.histories, bindings[0].artifact_id)
        assert advance(db, key, owner).complete
        assert archive_history_ready(db, key, owner.histories, bindings[0].artifact_id)
    finally:
        db.close()


def _measured(mode, tmp_path, monkeypatch, record_property):
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    operation = prepare_corpus_member if mode == "prepare" else apply_corpus_member
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for width in (1, 17, 129):
        for reopen in (False, True):
            path = tmp_path / f"{mode}-{width}-{reopen}.sqlite"
            db, key, owner, bindings, histories, objects, _ = setup(path, members=width)
            original_reader = owner.members.reader
            source_octets = max(
                len(objects["provenance/root.json.age"]),
                *(len(objects[member_history_object_path(b.history_sha256)]) for b in bindings),
                *(
                    len(
                        objects[
                            ProvenanceVolumeDocument.from_json_bytes(row[0].encode()).payload.path
                        ]
                    )
                    for row in db.execute(
                        "SELECT descriptor_json FROM discovery_archive_binding_pages"
                    )
                ),
            )
            for index in range(width + 1):
                if index not in (0, width - 1, width):
                    advance(db, key, owner)
                    continue
                if reopen:
                    db.close()
                    db = sqlite3.connect(path, isolation_level=None)
                    owner = CorpusMemberRegistration(owner.members.admission, original_reader)
                if mode == "apply":
                    page = prepare_corpus_member(db, key, owner)
                tables = [
                    row[0]
                    for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name LIKE 'discovery_%'"
                    ).fetchall()
                ]
                registered = sum(
                    db.execute("SELECT count(*) FROM " + table).fetchone()[0] for table in tables
                )
                metadata = max(
                    sum(map(metadata_octets, row))
                    for table in tables
                    for row in db.execute("SELECT * FROM " + table).fetchall()
                )
                metadata = max(metadata, max(len(h.to_json_bytes()) for h in histories))
                recorder = MeasuredConnection(db)
                recorder.metrics.update(source_reads=0, source_read_octets=0)
                actual_execute = recorder.execute

                def execute(sql, parameters=(), actual=actual_execute, recorder=recorder):
                    if sql.lstrip().startswith(("INSERT", "UPDATE")):
                        recorder.metrics["written_octets"] += sum(map(metadata_octets, parameters))
                    return actual(sql, parameters)

                recorder.execute = execute
                actual_read = original_reader._read_object
                actual_hash, actual_encode = hashlib.sha256, canonical_json_bytes

                def read(source_path, actual=actual_read, recorder=recorder, db=db):
                    assert not db.in_transaction
                    recorder.metrics["source_reads"] += 1
                    for block in actual(source_path):
                        recorder.metrics["source_read_octets"] += len(block)
                        yield block

                def encode(value, actual=actual_encode, recorder=recorder):
                    result = actual(value)
                    recorder.metrics["encoded_octets"] += len(result)
                    return result

                class ObservedHash:
                    def __init__(
                        self, value=b"", *, recorder=recorder, actual=actual_hash, **kwargs
                    ):
                        self.metrics = recorder.metrics
                        self.metrics["hashed_octets"] += len(value)
                        self.actual = actual(value, **kwargs)

                    def update(self, value):
                        self.metrics["hashed_octets"] += len(value)
                        self.actual.update(value)

                    def digest(self):
                        return self.actual.digest()

                    def hexdigest(self):
                        return self.actual.hexdigest()

                def tick(recorder=recorder):
                    recorder.metrics["sqlite_vm_instructions"] += 1
                    return 0

                if mode == "apply":
                    db.execute("BEGIN IMMEDIATE")
                db.set_progress_handler(tick, 1)
                try:
                    with monkeypatch.context() as patch:
                        patch.setattr(original_reader, "_read_object", read)
                        patch.setattr(hashlib, "sha256", ObservedHash)
                        for module in tuple(sys.modules.values()):
                            if module is not None and getattr(module, "__name__", "").startswith(
                                (
                                    "riverhog_archive_contracts",
                                    "riverhog_canonical_json",
                                    "riverhog_core.canonical_discovery",
                                    "riverhog_core.provenance_archive_read",
                                )
                            ):
                                if getattr(module, "canonical_json_bytes", None) is actual_encode:
                                    patch.setattr(module, "canonical_json_bytes", encode)
                        if mode == "prepare":
                            page = operation(recorder, key, owner)
                        else:
                            result = operation(recorder, key, owner, page)
                            assert result.members == min(index + 1, width)
                finally:
                    db.set_progress_handler(None, 0)
                    if mode == "apply":
                        db.commit()
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": digest,
                    "dimensions": {
                        "registered_inputs": registered,
                        "metadata_octets": metadata,
                        "source_octets": source_octets,
                    },
                    "metrics": {
                        bound.metric: recorder.metrics[bound.metric]
                        for bound in binding.declaration.bounds
                    },
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
                if mode == "prepare":
                    with write(db):
                        apply_corpus_member(db, key, owner, page)
            db.close()
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-member-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_members.prepare_corpus_member",
)
def test_measured_member_history_preparation(tmp_path, monkeypatch, record_property):
    _measured("prepare", tmp_path, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-member-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_members.apply_corpus_member",
)
def test_measured_member_history_apply(tmp_path, monkeypatch, record_property):
    _measured("apply", tmp_path, monkeypatch, record_property)
