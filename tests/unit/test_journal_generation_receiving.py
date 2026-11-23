"""Selected Native original receiving and surviving Core cost settlement."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.journal_recovery_frontier import JournalGenerationReceipt
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_protocol.errors import Conflict
from sqlalchemy import update

from scripts.validation_numeric import numeric_policy_from_environment
from tests.support.catalog_work import CatalogRecorder, octets
from tests.unit.test_archive_write_admission import _guard
from tests.unit.test_journal_recovery_frontier import (
    NS,
    adopt,
    observe_codecs,
    snapshot,
)
from tests.unit.test_journal_recovery_frontier import (
    origin as origin,
)
from tests.unit.test_upload_journal_validation_owner import finish


def received(case, *, total=11 * NS):
    case.capture.close()
    assert case.generation_construction.close_step()
    return JournalGenerationReceipt.from_original(
        case.capture, case.generation_construction, total_actual_ns=total
    )


def settle(case, receipt, *, revision=None, receiver=None, acknowledge=True):
    receiver = receiver or jobs.JournalGenerationReceiving(
        receipt,
        control_bytes=32768,
        original=case.generation,
    )
    result = case.recovery.receive_cost(
        case.binding,
        revision=snapshot(case)[0].revision if revision is None else revision,
        identity=receipt.context.grant_identity,
        total_actual_ns=receipt.total_actual_ns,
        remaining_reserved_ns=0,
        guard=_guard,
        retain_original=receiver.retain,
        receive_original=receiver.receive,
    )
    if acknowledge:
        receiver.acknowledge(case.factory)
    return result


def test_actual_selected_original_receives_once_without_invalidating_semantic_job(origin):
    case = origin
    selected, current = adopt(case)
    receipt = received(case)
    status, root = settle(case, receipt)
    assert status == "received"
    assert root.accounting.successful_checkpoints == 1
    assert root.accounting.charged_ns == 11 * NS
    assert root.accounting.reserved_ns == root.accounting.held_count_slots == 0
    assert root.grants == ()
    _, job = snapshot(case)
    assert job.version == current.version
    assert job.resume_frontier_json == current.resume_frontier_json
    assert job.recovery_receipt_json == receipt.encoded
    assert JournalGenerationReceipt.from_json(receipt.encoded, control_bytes=32768) == receipt
    assert settle(case, receipt, revision=0) == ("already-received", root)
    recovered = jobs.JournalGenerationReceiving(
        JournalGenerationReceipt.from_json(receipt.encoded, control_bytes=32768),
        control_bytes=32768,
    )
    assert settle(case, receipt, revision=0, receiver=recovered) == ("already-received", root)
    assert snapshot(case) == (root, job)
    assert root.revision == selected.revision + 1
    case.slot.job = current
    outcome = finish(case.owner, case.work)
    assert outcome.failure is None
    _, terminal = snapshot(case)
    assert terminal.state == "complete"
    assert terminal.recovery_receipt_json == receipt.encoded
    assert jobs.outcome(terminal) == outcome


def test_receipt_requires_actual_capture_and_same_construction_receiving(
    origin, journal_constructions
):
    case = origin
    before = snapshot(case)
    literal = JournalGenerationReceipt(case.frontier.context, case.frontier.generation, 11 * NS)
    no_originals = jobs.JournalGenerationReceiving(literal, control_bytes=32768)
    with pytest.raises(Conflict, match="receiving proof is unavailable"):
        settle(case, literal, receiver=no_originals)
    with pytest.raises(BlockingIOError, match="have not received"):
        settle(case, literal)
    assert snapshot(case) == before
    with pytest.raises(BlockingIOError, match="have not received"):
        JournalGenerationReceipt.from_original(
            case.capture, case.generation_construction, total_actual_ns=11 * NS
        )
    case.capture.close()
    with pytest.raises(BlockingIOError, match="have not received"):
        JournalGenerationReceipt.from_original(
            case.capture, case.generation_construction, total_actual_ns=11 * NS
        )
    unrelated = journal_constructions.retain()
    assert unrelated.close_step()
    assert case.generation_construction.close_step()
    with pytest.raises(BlockingIOError, match="have not received"):
        JournalGenerationReceipt.from_original(case.capture, unrelated, total_actual_ns=11 * NS)
    assert snapshot(case) == before


def test_independent_native_reader_prevents_refunding_original_tail(origin):
    case = origin
    selected, _ = adopt(case)
    reader = case.storage.roots.borrow(
        case.frontier.image.identity, expected_root=case.frontier.image.root
    )
    try:
        case.capture.close()
        assert not case.generation_construction.close_step()
        with pytest.raises(BlockingIOError, match="have not received"):
            JournalGenerationReceipt.from_original(
                case.capture, case.generation_construction, total_actual_ns=11 * NS
            )
        assert snapshot(case)[0] == selected
        assert selected.accounting.reserved_ns == 2 * NS
    finally:
        reader.close()
    assert case.generation_construction.close_step()
    assert settle(case, received(case))[0] == "received"


@pytest.mark.parametrize("after_root_write", [False, True])
def test_cost_receipt_and_root_receiving_roll_back_together(origin, monkeypatch, after_root_write):
    case = origin
    adopt(case)
    receipt = received(case)
    before = snapshot(case)
    original = case.recovery._save

    def interrupted(*args):
        if after_root_write:
            original(*args)
        raise RuntimeError("original cost receiving was not committed")

    with monkeypatch.context() as patch:
        patch.setattr(case.recovery, "_save", interrupted)
        with pytest.raises(RuntimeError, match="not committed"):
            settle(case, receipt)
    assert snapshot(case) == before
    assert settle(case, receipt)[0] == "received"


def test_newer_worker_and_source_retirement_preserve_historical_original_cost(origin):
    case = origin
    adopt(case)
    receipt = received(case)
    with case.factory() as session, session.begin():
        session.execute(
            update(Job).values(
                collection_id=None,
                journal_id=None,
                state="retiring",
                worker_epoch=case.job.worker_epoch + 1,
            )
        ).close()
    _, before = snapshot(case)
    status, _ = settle(case, receipt)
    assert status == "received"
    _, after = snapshot(case)
    assert after.worker_epoch == before.worker_epoch
    assert after.state == "retiring" and after.collection_id is after.journal_id is None
    assert after.version == before.version
    assert after.resume_frontier_json == before.resume_frontier_json
    assert after.recovery_receipt_json == receipt.encoded


def test_received_original_cannot_claim_a_different_final_cost(origin):
    case = origin
    adopt(case)
    receipt = received(case)
    settle(case, receipt)
    before = snapshot(case)
    contradictory = replace(receipt, total_actual_ns=12 * NS)
    with pytest.raises(Conflict, match="immutable original receipt"):
        settle(case, contradictory, revision=0)
    assert snapshot(case) == before


def test_unselected_complete_image_does_not_manufacture_publication_receiving(origin):
    case = origin
    receipt = received(case)
    before = snapshot(case)
    with pytest.raises(Conflict, match="matching selected original grant"):
        settle(case, receipt)
    assert snapshot(case) == before
    assert before[0].accounting.successful_checkpoints == 0
    assert before[0].accounting.held_count_slots == 1


def test_receiving_pair_requires_same_transaction_and_exact_original_arguments(origin):
    case = origin
    selected, _ = adopt(case)
    receipt = received(case)
    receiver = jobs.JournalGenerationReceiving(
        receipt,
        control_bytes=32768,
        original=case.generation,
    )
    with case.factory() as session, session.begin():
        with pytest.raises(Conflict, match="same-transaction"):
            receiver.receive(session, receipt.context.grant_identity, receipt.total_actual_ns)
        receiver.retain(session, selected.grants[0])
    with session.begin():
        with pytest.raises(Conflict, match="same-transaction"):
            receiver.receive(session, receipt.context.grant_identity, receipt.total_actual_ns)
    with case.factory() as original, original.begin():
        receiver.retain(original, selected.grants[0])
    with case.factory() as another, another.begin():
        with pytest.raises(Conflict, match="same-transaction"):
            receiver.receive(another, receipt.context.grant_identity, receipt.total_actual_ns)
    assert settle(case, receipt, receiver=receiver)[0] == "received"


def test_original_receipt_format_is_exact_and_bounded(origin):
    receipt = received(origin)
    value = json.loads(receipt.encoded)
    for mutation in (
        {"format": "unowned-original-receipt"},
        {"total_actual_ns": receipt.total_actual_ns},
        {"total_actual_ns": "01"},
        {"generation": "f" * 63},
        {"extra": "unowned"},
    ):
        with pytest.raises(ValueError):
            JournalGenerationReceipt.from_json(
                canonical_json_bytes({**value, **mutation}).decode(), control_bytes=32768
            )
    with pytest.raises(ValueError, match="metadata admission"):
        JournalGenerationReceipt.from_json(receipt.encoded, control_bytes=1)


def test_original_receiving_uses_indexed_metadata_work_with_unrelated_jobs(origin, record_property):
    case = origin
    selected, _ = adopt(case)
    receipt = received(case)
    bindings = tuple(
        map(growth_binding, (jobs.retain_recovery_original, jobs.receive_recovery_original))
    )
    policy = numeric_policy_from_environment(os.environ)
    projection = project_bindings(bindings, numeric_policy=policy)
    samples = []
    for unrelated in (0, 128):
        if unrelated:
            with case.factory() as session, session.begin():
                session.add_all(
                    Job(
                        **{
                            **asdict(case.job),
                            "workspace_id": f"{index + 1:032x}",
                            "collection_id": None,
                            "journal_id": None,
                            "state": "retiring",
                        }
                    )
                    for index in range(unrelated)
                )
        with case.engine.connect() as connection:
            metadata = connection.exec_driver_sql("SELECT * FROM journal_validation_jobs").all()
        controls = octets((receipt.encoded, asdict(case.job), selected.grants[0].document()))
        dimensions = {
            "retained_rows": len(metadata),
            "metadata_row_octets": max(controls, *(octets(row) for row in metadata)),
            "control_octets": controls,
        }
        for mode, binding in zip(
            ("retain_recovery_original", "receive_recovery_original"), bindings, strict=True
        ):
            for cold in (False, True):
                recorder = CatalogRecorder(jobs)
                with observe_codecs(recorder, case.engine, cold=cold):
                    with case.factory() as session, session.begin():
                        if mode == "retain_recovery_original":
                            current = jobs.retain_recovery_original(
                                session, receipt, selected.grants[0], control_bytes=32768
                            )
                            assert current.resume_frontier_json == case.frontier.encoded
                        else:
                            # The exact selected job is retained before this
                            # separately declared receipt CAS observation.
                            current = replace(case.job, resume_frontier_json=case.frontier.encoded)
                            assert (
                                jobs.receive_recovery_original(
                                    session, current, receipt, selected.grants[0]
                                )
                                == "published"
                            )
                        session.rollback()
                assert recorder.metrics["sql_statements"] == 1
                assert recorder.metrics["changed_rows"] == int(mode == "receive_recovery_original")
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": hashlib.sha256(
                        canonical_json_bytes(projection["declarations"][binding.declaration.id])
                    ).hexdigest(),
                    "dimensions": dimensions,
                    "metrics": {
                        bound.metric: recorder.metrics[bound.metric]
                        for bound in binding.declaration.bounds
                    },
                    "delegates": {},
                }
                validate_execution_samples(
                    projection, binding.declaration.id, [sample], numeric_policy=policy
                )
                samples.append(sample)
        assert snapshot(case)[0] == selected
    record_property("growth_samples", samples)
