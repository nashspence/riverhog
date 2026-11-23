"""Known nonpublication fences selection while original cleanup remains held."""

from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from threading import Barrier

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_protocol.errors import Conflict

from scripts.validation_numeric import numeric_policy_from_environment
from tests.support.catalog_work import CatalogRecorder, octets
from tests.unit.test_archive_write_admission import _guard
from tests.unit.test_collection_recovery_accounting import (
    _read,
    _reserve,
    _retain_original,
    _root_case,
    _select,
)
from tests.unit.test_journal_recovery_frontier import adopt, observe_codecs, snapshot
from tests.unit.test_journal_recovery_frontier import origin as origin


def reject(case, *, context=None, revision=None):
    return case.recovery.reject_generation(
        case.binding,
        revision=snapshot(case)[0].revision if revision is None else revision,
        identity=case.grant.identity,
        guard=_guard,
        retain_original=lambda session, grant: jobs.retain_recovery_rejection(
            session, context or case.context, grant, control_bytes=32768
        ),
    )


def test_actual_candidate_rejection_fences_selection_without_receiving_resources(origin):
    case = origin
    before, job = snapshot(case)
    assert not case.capture.received and not case.generation.received
    rejected = reject(case)
    assert rejected.revision == before.revision + 1
    assert rejected.accounting == before.accounting
    assert rejected.grants[0] == replace(case.grant, nonpublication_proven=True)
    assert snapshot(case) == (rejected, job)
    assert not case.capture.received and not case.generation.received
    assert (
        case.storage.envelopes.read(
            case.frontier.image.identity,
            root=case.frontier.image.root,
            expected_digest=case.frontier.generation,
        )
        == case.frontier.image.encoded
    )

    def unexpected_selection(*args):
        pytest.fail("rejected original reached the domain selection callback")

    with pytest.raises(Conflict, match="rejected"):
        adopt(case, revision=rejected.revision, callback=unexpected_selection)
    assert snapshot(case) == (rejected, job)
    with case.factory() as session, session.begin():
        with pytest.raises(Conflict, match="rejected"):
            jobs.select_resume_frontier(
                session,
                case.work,
                case.job,
                rejected.grants[0],
                case.frontier,
                generation=case.frontier.generation,
                control_bytes=32768,
            )
    assert snapshot(case) == (rejected, job)


def test_rejection_reply_retry_preserves_held_tail_after_gate_and_worker_change(origin):
    case = origin
    rejected = reject(case)
    with case.factory() as session, session.begin():
        control = session.get(Control, case.work.collection_id)
        control.gate, control.epoch = "closed", "f" * 64
        row = session.get(Job, case.job.workspace_id)
        row.worker_epoch += 1
    before = snapshot(case)
    assert reject(case, revision=0) == rejected
    assert snapshot(case) == before
    assert rejected.accounting.reserved_ns == case.grant.original_reserved_ns
    assert rejected.accounting.held_count_slots == 1


@pytest.mark.parametrize("after_write", [False, True])
def test_rejection_and_root_write_roll_back_with_the_same_original(
    origin, monkeypatch, after_write
):
    case = origin
    before = snapshot(case)
    original = case.recovery._save

    def failed(session, root, updated):
        assert updated.grants[0].nonpublication_proven
        row = session.get(Job, case.job.workspace_id)
        assert row.recovery_original_json == case.context.encoded.decode()
        if after_write:
            original(session, root, updated)
        raise InterruptedError("original rejection transaction rolled back")

    with monkeypatch.context() as patch:
        patch.setattr(case.recovery, "_save", failed)
        with pytest.raises(InterruptedError, match="rolled back"):
            reject(case)
    assert snapshot(case) == before
    assert reject(case).grants[0].nonpublication_proven


def test_selected_image_cannot_be_rejected_or_deleted_by_the_rejection_fence(origin):
    case = origin
    selected, _ = adopt(case)
    before = snapshot(case)
    with pytest.raises(Conflict, match="cannot become nonpublication"):
        reject(case)
    assert snapshot(case) == before
    assert selected.accounting.successful_checkpoints == 1
    reader = case.storage.roots.borrow(case.grant.identity, expected_root=case.frontier.image.root)
    try:
        assert reader.read(after=0, page_limit=1).pages
    finally:
        reader.close()


def test_rejection_requires_the_exact_retained_domain_original(origin):
    case = origin
    before = snapshot(case)
    for context in (
        replace(case.context, source_binding_json="different source"),
        replace(case.context, worker_epoch=case.context.worker_epoch + 1),
    ):
        with pytest.raises(Conflict, match="original owner"):
            reject(case, context=context)
        assert snapshot(case) == before
    with case.factory() as session, session.begin():
        row = session.get(Job, case.job.workspace_id)
        row.recovery_received = True
        row.recovery_receipt_json = "already received original"
    before = snapshot(case)
    with pytest.raises(Conflict, match="retained original"):
        reject(case)
    assert snapshot(case) == before


def selection_rejection_race(tmp_path, *, database_url=None):
    with _root_case(tmp_path, database_url=database_url) as case:
        _, grant, admitted = _reserve(case)
        ready = Barrier(2)

        def attempt(selecting):
            ready.wait(timeout=10)
            try:
                if selecting:
                    _select(case, grant, revision=admitted.revision)
                    return "selected"
                case.recovery.reject_generation(
                    case.binding,
                    revision=admitted.revision,
                    identity=grant.identity,
                    guard=_guard,
                    retain_original=_retain_original,
                )
                return "rejected"
            except Conflict:
                return "refused"

        with ThreadPoolExecutor(max_workers=2) as workers:
            outcomes = list(workers.map(attempt, (False, True)))
        assert outcomes.count("refused") == 1
        root = _read(case)
        actual = root.grants[0]
        assert actual.nonpublication_proven == (outcomes.count("rejected") == 1)
        assert (actual.generation is not None) == (outcomes.count("selected") == 1)
        assert root.accounting.held_count_slots + root.accounting.successful_checkpoints == 1
        assert root.accounting.reserved_ns > 0


def test_actual_root_selection_and_rejection_serialize_to_one_disposition(tmp_path):
    selection_rejection_race(tmp_path)


@pytest.mark.parametrize("damage", ["legacy-format", "missing-flag", "nonboolean-flag", "selected"])
def test_mandatory_rejection_state_fails_closed_on_corruption(tmp_path, damage):
    with _root_case(tmp_path) as case:
        _, grant, _ = _reserve(case)
        if damage == "selected":
            _select(case, grant)
        with case.factory() as session, session.begin():
            control = session.get(Control, case.root_id)
            value = json.loads(control.recovery_accounting_json)
            if damage == "legacy-format":
                value["format"] = "riverhog-recovery-accounting/v1"
            elif damage == "missing-flag":
                del value["active_grants"][0]["nonpublication_proven"]
            else:
                value["active_grants"][0]["nonpublication_proven"] = (
                    True if damage == "selected" else 1
                )
            raw = canonical_json_bytes(value).decode()
            control.recovery_accounting_json = raw
        with pytest.raises(RuntimeError, match="mandatory recovery accounting"):
            _read(case)
        with case.factory() as session:
            assert session.get(Control, case.root_id).recovery_accounting_json == raw


@pytest.mark.growth_witness(
    "riverhog-validation-original-rejection-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.retain_recovery_rejection",
)
def test_rejection_retains_only_indexed_original_metadata(origin, record_property):
    case = origin
    binding = growth_binding(jobs.retain_recovery_rejection)
    policy = numeric_policy_from_environment(os.environ)
    projection = project_bindings((binding,), numeric_policy=policy)
    samples = []
    for unrelated in (0, 128):
        if unrelated:
            with case.factory() as session, session.begin():
                session.add_all(
                    Job(
                        **{
                            **asdict(case.job),
                            "workspace_id": f"{number + 1:032x}",
                            "collection_id": None,
                            "journal_id": None,
                            "state": "retiring",
                        }
                    )
                    for number in range(unrelated)
                )
        with case.engine.connect() as connection:
            rows = connection.exec_driver_sql("SELECT * FROM journal_validation_jobs").all()
        controls = octets((asdict(case.job), case.context.encoded, case.grant.document()))
        dimensions = {
            "retained_rows": len(rows),
            "metadata_row_octets": max(controls, *(octets(row) for row in rows)),
            "control_octets": controls,
        }
        for cold in (False, True):
            recorder = CatalogRecorder(jobs)
            with observe_codecs(recorder, case.engine, cold=cold):
                with case.factory() as session, session.begin():
                    jobs.retain_recovery_rejection(
                        session, case.context, case.grant, control_bytes=32768
                    )
            assert recorder.metrics["sql_statements"] == 1
            assert recorder.metrics["changed_rows"] == 0
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": hashlib.sha256(
                    canonical_json_bytes(projection["declarations"][binding.declaration.id])
                ).hexdigest(),
                "dimensions": dimensions,
                "metrics": {
                    b.metric: recorder.metrics[b.metric] for b in binding.declaration.bounds
                },
                "delegates": {},
            }
            validate_execution_samples(
                projection, binding.declaration.id, [sample], numeric_policy=policy
            )
            samples.append(sample)
    record_property("growth_samples", samples)
