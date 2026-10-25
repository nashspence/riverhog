from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace

import pytest
from riverhog_archive_contracts import (
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    HistoryJournalAnchor,
    MemberHistoryDocument,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    ProvenanceRootDocument,
    RecordPage,
    RecordSetCommitment,
    history_record_page_object_path,
)
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
from riverhog_core.canonical_discovery_history_work import (
    ArchiveHistoryWork,
    HistorySetWork,
    apply_history_descriptor,
    apply_history_record,
    history_set_progress,
    initialize_history_work,
    prepare_history_descriptor,
    prepare_history_record,
)
from riverhog_core.canonical_discovery_journal_work import QualifiedJournalSource, StaleJournalTask
from riverhog_core.canonical_discovery_member_work import (
    ArchiveMemberRegistration,
    apply_archive_member,
    archive_member_progress,
    begin_archive_members,
    initialize_member_work,
    prepare_archive_member,
)
from riverhog_core.canonical_discovery_snapshot_work import (
    HistorySnapshotWork,
    apply_history_snapshot,
    history_snapshot_progress,
    initialize_snapshot_work,
    prepare_history_snapshot,
)
from riverhog_provenance import JournalSummary, validate_journal

from tests.support.canonical_discovery_work import (
    MeasuredConnection,
    RecordedConnection,
    corrected_journal_fixture,
    metadata_octets,
    write,
)
from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission


@pytest.fixture(scope="module")
def native():
    fixture, raw, _ = corrected_journal_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        anchors = tuple(HistoryJournalAnchor.from_mapping(summary.anchor_at(i)) for i in range(3))
    association = "urn:uuid:33333333-3333-4333-8333-333333333333"
    return fixture, raw, anchors, association


def put_set(objects, schema, roots):
    rows = sorted(
        ({"key": value.key, "value": value.to_mapping()} for value in roots),
        key=lambda row: row["key"],
    )
    commitment = RecordSetCommitment(schema)
    for row in rows:
        commitment.update(row["key"], row["value"])
    authority = commitment.ref()
    pages = [RecordPage(authority, 0, tuple(rows), False)] if rows else []
    pages.append(RecordPage(authority, len(pages), (), True))
    for page in pages:
        objects[history_record_page_object_path(authority.records_sha256, page.ordinal)] = (
            page.to_json_bytes()
        )
    return authority


def setup_work(path, native, *, fault=None):
    _, raw, anchors, association = native
    objects = {}
    imports = put_set(objects, MEMBER_HISTORY_IMPORTS_SCHEMA, ())
    histories, selections = [], []
    for member in range(2):
        roots = [
            MemberHistoryRoot(anchor, "bound" if i == member else "retained")
            for i, anchor in enumerate(anchors)
        ]
        if fault is not None:
            selected = roots[-1]
            changed = {
                "journal_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
                "through_entry_id": "urn:uuid:22222222-2222-4222-8222-222222222222",
                "through_sequence": 1,
                "through_json_sha256": "0" * 64,
                "prefix_sha256": "0" * 64,
                "prefix_bytes": selected.journal.prefix_bytes + 1,
            }[fault]
            roots[-1] = replace(selected, journal=replace(selected.journal, **{fault: changed}))
        authority = put_set(objects, MEMBER_HISTORY_ROOTS_SCHEMA, roots)
        history = MemberHistoryDocument(
            f"{member:064x}",
            7,
            "a" * 64,
            MemberHistoryPrimary(anchors[member], association),
            authority,
            imports,
        )
        histories.append(history)
        selections.append(tuple(sorted(roots, key=lambda value: value.key)))
    reader, archived, _, _ = archive_reader_fixture(
        raw, anchors[0].journal_id, histories=tuple(histories), history_objects=objects
    )
    root = ProvenanceRootDocument.from_json_bytes(archived["provenance/root.json.age"])
    admission = discovery_archive_admission(root, total_bytes=14)
    db = sqlite3.connect(path, isolation_level=None)
    initialize_archive_work(db)
    initialize_member_work(db)
    initialize_history_work(db)
    initialize_snapshot_work(db)
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
    owners = []
    for history in histories:
        page = prepare_history_descriptor(db, key, history_owner, history.artifact_id)
        with write(db):
            apply_history_descriptor(db, key, history_owner, page)
        owner = HistorySetWork(admission, reader, history.roots)
        while not history_set_progress(db, key, owner).complete:
            page = prepare_history_record(db, key, owner)
            with write(db):
                apply_history_record(db, key, owner, page)
        owners.append(owner)
    return db, key, admission, owners, selections


def test_exact_prefix_headers_are_shared_without_member_assertion_expansion(
    tmp_path, native, monkeypatch
):
    fixture, raw, anchors, _ = native
    db, key, admission, histories, selections = setup_work(tmp_path / "work.sqlite", native)
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with admit_archive_journal(db, key, admission, summary) as source:

            def no_iteration(*args):
                raise AssertionError("snapshot binding replayed source frames")

            monkeypatch.setattr(type(summary.frames), "__iter__", no_iteration)
            for history, roots in zip(histories, selections, strict=True):
                owner = HistorySnapshotWork(history, source)
                for selected in roots:
                    page = prepare_history_snapshot(db, key, owner, selected.key)
                    with write(db):
                        apply_history_snapshot(db, key, owner, page)
                    with write(db):
                        apply_history_snapshot(db, key, owner, page)
            assert db.execute("SELECT count(*) FROM discovery_snapshot_sources").fetchone() == (1,)
            assert db.execute("SELECT count(*) FROM discovery_snapshot_headers").fetchone() == (3,)
            assert db.execute(
                "SELECT count(*) FROM discovery_history_snapshot_links"
            ).fetchone() == (6,)
            headers = db.execute(
                "SELECT anchor_json FROM discovery_snapshot_headers ORDER BY prefix_sha256"
            ).fetchall()
            assert sorted(row[0] for row in headers) == sorted(
                canonical_json_bytes(anchor.to_mapping()).decode() for anchor in anchors
            )
            for history, roots in zip(histories, selections, strict=True):
                links = db.execute(
                    "SELECT record_key,inclusion FROM discovery_history_snapshot_links "
                    "WHERE root_set_sha256=? ORDER BY record_key",
                    (history.authority.records_sha256,),
                ).fetchall()
                assert links == [(value.key, value.inclusion) for value in roots]
    db.close()


@pytest.mark.parametrize(
    "fault",
    [
        "journal_id",
        "through_entry_id",
        "through_sequence",
        "through_json_sha256",
        "prefix_sha256",
        "prefix_bytes",
    ],
)
def test_full_native_anchor_is_required_even_after_structural_set_verification(
    tmp_path, native, fault
):
    fixture, raw, _, _ = native
    db, key, admission, histories, selections = setup_work(
        tmp_path / "work.sqlite", native, fault=fault
    )
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with admit_archive_journal(db, key, admission, summary) as source:
            owner = HistorySnapshotWork(histories[0], source)
            selected = next(
                value
                for value in selections[0]
                if (value.journal.to_mapping() not in [a.to_mapping() for a in native[2]])
            )
            with pytest.raises(StaleArchiveTask, match="exact admitted Native anchor"):
                prepare_history_snapshot(db, key, owner, selected.key)
            assert db.execute("SELECT count(*) FROM discovery_snapshot_headers").fetchone() == (0,)
    db.close()


def test_source_header_and_root_incidence_roll_back_and_retry_together(tmp_path, native):
    fixture, raw, _, _ = native
    db, key, admission, histories, selections = setup_work(tmp_path / "work.sqlite", native)
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with admit_archive_journal(db, key, admission, summary) as source:
            owner = HistorySnapshotWork(histories[0], source)
            page = prepare_history_snapshot(db, key, owner, selections[0][0].key)
            db.execute(
                "CREATE TRIGGER fail_link BEFORE INSERT ON discovery_history_snapshot_links "
                "BEGIN SELECT RAISE(ABORT,'injected snapshot failure'); END"
            )
            with write(db), pytest.raises(sqlite3.IntegrityError, match="injected"):
                apply_history_snapshot(db, key, owner, page)
            for table in (
                "discovery_snapshot_sources",
                "discovery_snapshot_headers",
                "discovery_history_snapshot_links",
                "discovery_history_snapshot_coverage",
            ):
                assert db.execute("SELECT count(*) FROM " + table).fetchone() == (0,)
            db.execute("DROP TRIGGER fail_link")
            with write(db):
                apply_history_snapshot(db, key, owner, page)
            db.execute("UPDATE discovery_snapshot_headers SET anchor_json='{}'")
            with write(db), pytest.raises(StaleArchiveTask, match="conflicting coordinates"):
                apply_history_snapshot(db, key, owner, page)
    db.close()


def test_closed_source_changed_root_and_forged_preparation_are_rejected(tmp_path, native):
    fixture, raw, _, _ = native
    db, key, admission, histories, selections = setup_work(tmp_path / "work.sqlite", native)
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with admit_archive_journal(db, key, admission, summary) as source:
            owner = HistorySnapshotWork(histories[0], source)
            page = prepare_history_snapshot(db, key, owner, selections[0][0].key)
            with write(db), pytest.raises(StaleArchiveTask, match="current exact"):
                apply_history_snapshot(db, key, owner, replace(page, prefix_sha256="f" * 64))
            with pytest.raises(ValueError, match="owner-managed"):
                apply_history_snapshot(db, key, owner, page)
            db.execute(
                "UPDATE discovery_history_sets SET version='100' WHERE records_sha256=?",
                (histories[0].authority.records_sha256,),
            )
            with write(db), pytest.raises(StaleArchiveTask, match="version"):
                apply_history_snapshot(db, key, owner, page)
            source.close()
            with write(db), pytest.raises(StaleJournalTask, match="closed"):
                apply_history_snapshot(db, key, owner, page)
    db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-history-snapshot-preparation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_snapshot_work.prepare_history_snapshot",
)
def test_measured_snapshot_preparation_uses_fixed_core_and_native_point_reads(
    tmp_path, native, monkeypatch, record_property
):
    measured(tmp_path, native, monkeypatch, record_property, prepare=True)


@pytest.mark.growth_witness(
    "canonical-discovery-history-snapshot-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_snapshot_work.apply_history_snapshot",
)
def test_measured_snapshot_apply_uses_fixed_reads_without_source_hashing(
    tmp_path, native, monkeypatch, record_property
):
    measured(tmp_path, native, monkeypatch, record_property, prepare=False)


def measured(tmp_path, native, monkeypatch, record_property, *, prepare):
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )
    from sha256_continuation import CheckpointSHA256

    fixture, raw, _, _ = native
    db, key, admission, histories, selections = setup_work(tmp_path / "measured.sqlite", native)
    binding = growth_binding(prepare_history_snapshot if prepare else apply_history_snapshot)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples, seen = [], set()
    actual_anchor, actual_update = JournalSummary.anchor_at, CheckpointSHA256.update_bytes
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with admit_archive_journal(db, key, admission, summary) as source:
            for history, roots in zip(histories, selections, strict=True):
                owner = HistorySnapshotWork(history, source)
                for selected in roots:
                    recorder = RecordedConnection(db)
                    observed = {"native_anchor_calls": 0, "source_hash_octets": 0}

                    def anchor(self, sequence, observed=observed):
                        observed["native_anchor_calls"] += 1
                        return actual_anchor(self, sequence)

                    def update(self, data, observed=observed):
                        observed["source_hash_octets"] += len(data)
                        return actual_update(self, data)

                    if not prepare:
                        page = prepare_history_snapshot(db, key, owner, selected.key)
                    with monkeypatch.context() as patch:
                        patch.setattr(JournalSummary, "anchor_at", anchor)
                        patch.setattr(CheckpointSHA256, "update_bytes", update)
                        if prepare:
                            page = prepare_history_snapshot(recorder, key, owner, selected.key)
                        else:
                            with write(db):
                                apply_history_snapshot(recorder, key, owner, page)
                    metrics = {
                        "sql_statements": recorder.metrics["sql_statements"],
                        "returned_rows": recorder.metrics["returned_rows"],
                        "native_anchor_calls": observed["native_anchor_calls"],
                    }
                    if not prepare:
                        metrics["source_hash_octets"] = observed["source_hash_octets"]
                    sample = {
                        "declaration": binding.declaration.id,
                        "declaration_sha256": declaration_sha,
                        "dimensions": {"registered_snapshots": len(seen)},
                        "metrics": metrics,
                        "delegates": {},
                    }
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
                    if prepare:
                        with write(db):
                            apply_history_snapshot(db, key, owner, page)
                    seen.add(page.prefix_sha256)
    record_property("growth_samples", samples)
    db.close()


def test_valid_shorter_native_prefix_cannot_replace_the_complete_archive_source(tmp_path, native):
    fixture, raw, anchors, _ = native
    db, key, admission, histories, _ = setup_work(tmp_path / "work.sqlite", native)
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with QualifiedJournalSource(summary, admission.identity, through=0) as prefix:
            owner = HistorySnapshotWork(histories[0], prefix)
            selected = MemberHistoryRoot(anchors[0], "bound")
            with pytest.raises(StaleArchiveTask, match="complete archived source"):
                prepare_history_snapshot(db, key, owner, selected.key)
            assert db.execute("SELECT count(*) FROM discovery_snapshot_headers").fetchone() == (0,)
    db.close()


def test_coverage_requires_each_original_root_incidence_and_survives_reopen(tmp_path, native):
    fixture, raw, _, _ = native
    path = tmp_path / "coverage.sqlite"
    db, key, admission, histories, selections = setup_work(path, native)
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with admit_archive_journal(db, key, admission, summary) as source:
            for history, roots in zip(histories, selections, strict=True):
                initial = history_snapshot_progress(db, key, history)
                assert initial.expected_roots == len(roots) == 3
                assert initial.registered_roots == 0 and not initial.complete
                owner = HistorySnapshotWork(history, source)
                for count, selected in enumerate(reversed(roots), 1):
                    page = prepare_history_snapshot(db, key, owner, selected.key)
                    with write(db):
                        apply_history_snapshot(db, key, owner, page)
                    expected = history_snapshot_progress(db, key, history)
                    assert expected.registered_roots == count
                    assert expected.complete is (count == len(roots))
                    with write(db):
                        apply_history_snapshot(db, key, owner, page)
                    assert history_snapshot_progress(db, key, history) == expected
                db.close()
                db = sqlite3.connect(path, isolation_level=None)
                assert history_snapshot_progress(db, key, history).complete
    assert db.execute("SELECT count(*) FROM discovery_snapshot_headers").fetchone() == (3,)
    assert db.execute("SELECT count(*) FROM discovery_history_snapshot_links").fetchone() == (6,)
    db.close()


def test_coverage_and_incidence_update_are_one_retryable_commit(tmp_path, native):
    fixture, raw, _, _ = native
    db, key, admission, histories, selections = setup_work(tmp_path / "coverage.sqlite", native)
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with admit_archive_journal(db, key, admission, summary) as source:
            owner = HistorySnapshotWork(histories[0], source)
            first = prepare_history_snapshot(db, key, owner, selections[0][0].key)
            with write(db):
                apply_history_snapshot(db, key, owner, first)
            before = history_snapshot_progress(db, key, histories[0])
            page = prepare_history_snapshot(db, key, owner, selections[0][1].key)
            db.execute(
                "CREATE TRIGGER fail_coverage BEFORE UPDATE ON discovery_history_snapshot_coverage "
                "BEGIN SELECT RAISE(ABORT,'injected coverage failure'); END"
            )
            with write(db), pytest.raises(sqlite3.IntegrityError, match="injected coverage"):
                apply_history_snapshot(db, key, owner, page)
            assert history_snapshot_progress(db, key, histories[0]) == before
            assert db.execute(
                "SELECT count(*) FROM discovery_history_snapshot_links"
            ).fetchone() == (1,)
            db.execute("DROP TRIGGER fail_coverage")
            with write(db):
                apply_history_snapshot(db, key, owner, page)
            assert history_snapshot_progress(db, key, histories[0]).registered_roots == 2
    db.close()


@pytest.mark.parametrize("counter", ["-1", "01", "4", None, "authority", "worker"])
def test_coverage_rejects_corrupt_or_missing_accounting(tmp_path, native, counter):
    fixture, raw, _, _ = native
    db, key, admission, histories, selections = setup_work(tmp_path / "coverage.sqlite", native)
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        with admit_archive_journal(db, key, admission, summary) as source:
            owner = HistorySnapshotWork(histories[0], source)
            page = prepare_history_snapshot(db, key, owner, selections[0][0].key)
            with write(db):
                apply_history_snapshot(db, key, owner, page)
            if counter is None:
                db.execute("DELETE FROM discovery_history_snapshot_coverage")
                with write(db), pytest.raises(StaleArchiveTask, match="matching coverage"):
                    apply_history_snapshot(db, key, owner, page)
            else:
                column = {"authority": "authority_json", "worker": "worker_id"}.get(
                    counter, "registered_roots"
                )
                value = "changed-" + counter if column != "registered_roots" else counter
                db.execute(
                    "UPDATE discovery_history_snapshot_coverage SET " + column + "=?", (value,)
                )
                with pytest.raises(StaleArchiveTask, match="coverage"):
                    history_snapshot_progress(db, key, histories[0])
    db.close()


def test_coverage_rejects_changed_root_authority_and_current_worker(tmp_path, native):
    db, key, _, histories, _ = setup_work(tmp_path / "coverage.sqlite", native)
    history = histories[0]
    original = history.authority
    history.authority = replace(original, record_count=1)
    with pytest.raises(StaleArchiveTask, match="authority"):
        history_snapshot_progress(db, key, history)
    history.authority = original
    with pytest.raises(StaleArchiveTask, match="worker"):
        history_snapshot_progress(db, replace(key, worker_id="replacement"), history)
    db.execute(
        "UPDATE discovery_history_sets SET complete=0 WHERE records_sha256=?",
        (history.authority.records_sha256,),
    )
    with pytest.raises(StaleArchiveTask, match="verified"):
        history_snapshot_progress(db, key, history)
    db.execute(
        "UPDATE discovery_history_sets SET complete=1 WHERE records_sha256=?",
        (history.authority.records_sha256,),
    )
    db.execute("UPDATE discovery_archive_tasks SET complete=0")
    with pytest.raises(StaleArchiveTask, match="incomplete"):
        history_snapshot_progress(db, key, history)
    db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-history-snapshot-coverage/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_snapshot_work.history_snapshot_progress",
)
def test_measured_coverage_is_indexed_read_only_and_independent_of_unrelated_rows(
    tmp_path, native, monkeypatch, record_property
):
    from riverhog_core import canonical_discovery_snapshot_work as snapshots
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    fixture, raw, _, _ = native
    binding = growth_binding(history_snapshot_progress)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    actual_encode = snapshots.canonical_json_bytes
    samples = []
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        for retained in (1, 17, 129):
            path = tmp_path / ("coverage-" + str(retained) + ".sqlite")
            db, key, admission, histories, selections = setup_work(path, native)
            history = histories[0]
            for index in range(retained - 1):
                db.execute(
                    "INSERT INTO discovery_history_snapshot_coverage VALUES (?,?,?,?,?,'0')",
                    (
                        "unrelated-" + str(index),
                        key.admission_sha256,
                        history.authority.records_sha256,
                        "unrelated",
                        "unrelated damaged root authority" * 3000,
                    ),
                )
            with admit_archive_journal(db, key, admission, summary) as source:
                owner = HistorySnapshotWork(history, source)
                for state, selected_roots in (
                    ("unstarted", ()),
                    ("partial", selections[0][:1]),
                    ("complete", selections[0][1:]),
                ):
                    for selected in selected_roots:
                        page = prepare_history_snapshot(db, key, owner, selected.key)
                        with write(db):
                            apply_history_snapshot(db, key, owner, page)
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
                                        history.authority_json,
                                    ),
                                )
                            )
                        ]
                        for table, extra, values in (
                            ("discovery_archive_tasks", "", ()),
                            (
                                "discovery_history_sets",
                                " AND records_sha256=?",
                                (history.authority.records_sha256,),
                            ),
                            (
                                "discovery_history_snapshot_coverage",
                                " AND root_set_sha256=?",
                                (history.authority.records_sha256,),
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
                                    "discovery_history_snapshot_coverage",
                                )
                            ),
                            "metadata_octets": max(widths),
                        }
                        observer = MeasuredConnection(db)
                        changes = db.total_changes

                        def encode(value, observer=observer):
                            result = actual_encode(value)
                            observer.metrics["encoded_octets"] += len(result)
                            return result

                        def instruction(observer=observer):
                            observer.metrics["sqlite_vm_instructions"] += 1
                            return 0

                        db.set_progress_handler(instruction, 1)
                        try:
                            with monkeypatch.context() as patch:
                                patch.setattr(snapshots, "canonical_json_bytes", encode)
                                patch.setitem(
                                    require_canonical_json.__globals__,
                                    "canonical_json_bytes",
                                    encode,
                                )
                                progress = history_snapshot_progress(observer, key, history)
                        finally:
                            db.set_progress_handler(None, 0)
                        assert db.total_changes == changes
                        assert progress.expected_roots == 3
                        assert (
                            progress.registered_roots
                            == {"unstarted": 0, "partial": 1, "complete": 3}[state]
                        )
                        assert progress.complete is (state == "complete")
                        for sql, parameters in observer.statements:
                            assert "count(" not in sql.lower()
                            plans = db.execute("EXPLAIN QUERY PLAN " + sql, parameters).fetchall()
                            assert all("SCAN" not in row[3].upper() for row in plans)
                        metrics = {
                            bound.metric: observer.metrics[bound.metric]
                            for bound in binding.declaration.bounds
                        }
                        sample = {
                            "declaration": binding.declaration.id,
                            "declaration_sha256": declaration_sha,
                            "dimensions": dimensions,
                            "metrics": metrics,
                            "delegates": {},
                        }
                        validate_execution_samples(projection, binding.declaration.id, [sample])
                        samples.append(sample)
            db.close()
    record_property("growth_samples", samples)
