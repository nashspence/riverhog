"""Issued originals survive source and physical retirement until exact receiving."""

from __future__ import annotations

import hashlib
import os
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.journal_recovery_frontier import (
    JOURNAL_RECOVERY_DOMAIN,
    JournalGenerationReceipt,
    JournalRecoveryContext,
    journal_recovery_fence,
)
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services import upload_journal_validation_retirement as retirement_owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_protocol.errors import Conflict
from sqlalchemy import delete, update

from scripts.validation_numeric import numeric_policy_from_environment
from tests.support.catalog_work import CatalogRecorder, octets
from tests.support.journal_retirement import cleanups as cleanups
from tests.support.validation_workspace import leased_workspace
from tests.unit.test_archive_upload_execution import owned as owned
from tests.unit.test_archive_write_admission import _guard
from tests.unit.test_collection_recovery_accounting import _candidate
from tests.unit.test_journal_generation_receiving import received, settle
from tests.unit.test_journal_recovery_frontier import (
    NS,
    admitted_origin,
    adopt,
    observe_codecs,
    snapshot,
)
from tests.unit.test_journal_recovery_frontier import origin as origin
from tests.unit.test_journal_validation_retirement import retirement_step
from tests.unit.test_staged_journal_range import Journal


def reserve_again(case, current):
    return case.recovery.reserve(
        case.binding,
        epoch=case.epoch,
        revision=snapshot(case)[0].revision,
        domain=JOURNAL_RECOVERY_DOMAIN,
        owner_id=current.workspace_id,
        owner_fence=journal_recovery_fence(case.binding, current),
        useful_ns=200 * NS,
        candidate=_candidate(),
        guard=_guard,
        retain_domain=lambda session: jobs.retain_recovery_owner(session, case.work, current),
        retain_grant=lambda session, grant: jobs.retain_recovery_grant(
            session, case.binding, current, grant, control_bytes=32768
        ),
    )


@pytest.fixture
def unopened_origin(tmp_path, journal_constructions):
    with admitted_origin(tmp_path, journal_constructions) as case:
        yield case


def test_issued_original_is_durable_before_optional_generation_construction(unopened_origin):
    case = unopened_origin
    root, current = snapshot(case)
    assert current.recovery_original_json == case.context.encoded.decode()
    assert not current.recovery_received and not current.directory_received
    assert current.recovery_receipt_json is None
    assert root.grants == (case.grant,)
    assert case.generation.storage is None
    assert not (case.directory / "generations.sqlite3").exists()
    before = snapshot(case)
    with case.factory() as session, session.begin():
        with pytest.raises(Conflict, match="retained original owner"):
            jobs.retain_recovery_grant(
                session, case.binding, current, case.grant, control_bytes=32768
            )
    assert snapshot(case) == before
    premature = JournalGenerationReceipt(case.context, None, 0)
    with case.factory() as session, session.begin():
        with pytest.raises(Conflict, match="unavailable for acknowledgement"):
            jobs.acknowledge_recovery_original(session, premature)
    assert snapshot(case) == before


def test_slot_requires_its_acknowledged_original_before_optional_effects(
    unopened_origin, journal_constructions
):
    case = unopened_origin
    original, current = case.slot.generation, case.slot.job
    case.slot.generation = None
    try:
        for job, context in (
            (replace(current, state="complete"), case.context),
            (replace(current, recovery_received=True), case.context),
            (replace(current, recovery_original_json=None), case.context),
            (current, replace(case.context, worker_epoch=case.context.worker_epoch + 1)),
        ):
            case.slot.job = job
            construction = journal_constructions.retain()
            with pytest.raises(RuntimeError, match="original live slot"):
                case.slot.retain_generation(construction, context)
            assert case.slot.generation is None and construction.phase == "new"
            assert not (case.directory / "generations.sqlite3").exists()
    finally:
        case.slot.generation, case.slot.job = original, current


def test_later_grant_reopens_storage_after_exact_original_receiving(origin, journal_constructions):
    from riverhog_provenance import NativeGenerationStorage

    case = origin
    selected, _ = adopt(case)
    first = case.generation
    final = received(case)
    assert settle(case, final)[0] == "received"
    assert first.received and not case.slot.construction.received
    current = snapshot(case)[1]
    decision, grant, reserved = reserve_again(case, current)
    assert decision.admitted and grant is not None and grant.identity != case.grant.identity
    active = snapshot(case)[1]
    context = JournalRecoveryContext.original(case.binding, active, grant)
    case.slot.job = active
    construction = journal_constructions.retain()
    second = case.slot.retain_generation(construction, context)
    assert second is case.slot.generation and second is not first
    assert second.context == context and first.context == case.context
    assert first.received and first.storage.storage_received
    assert not second.received and second.storage is None
    storage = NativeGenerationStorage.open(
        case.directory,
        construction=construction,
        page_size=4096,
        reader_limit=2,
        metadata_octets=131072,
        create=False,
        on_storage_open=second.retain_storage,
        on_storage_close=case.slot.receive_generation,
    )
    image = case.frontier.image
    reader = storage.roots.borrow(image.identity, expected_root=image.root)
    try:
        assert reader.read(after=0, page_limit=1).pages
        assert not second.close_step()
        assert not second.received and first.received
        with leased_workspace(case.owner.root, active.workspace_id) as busy:
            assert busy is None
    finally:
        reader.close()
    assert second.close_step() and second.received
    assert not case.slot.construction.received
    # Reopening the optional store neither recaptures nor selects another image.
    root, job = snapshot(case)
    assert root == reserved and root.accounting.successful_checkpoints == 1
    assert job.resume_frontier_json == current.resume_frontier_json
    assert selected.accounting.successful_checkpoints == 1
    for _ in range(10000):
        if case.slot.session.advance().complete:
            break
    else:
        pytest.fail("finite live executor did not continue after optional receiving")
    with leased_workspace(case.owner.root, active.workspace_id) as busy:
        assert busy is None
    case.owner.close()
    assert first.received and second.received and case.slot.construction.received
    with leased_workspace(case.owner.root, active.workspace_id) as free:
        assert free == case.directory


def test_received_original_cannot_be_relabelled_as_another_grant(origin, journal_constructions):
    case = origin
    adopt(case)
    final = received(case)
    settle(case, final)
    first = case.generation
    construction = journal_constructions.retain()
    # Closure never makes the same grant identity available for reuse.
    with pytest.raises(RuntimeError, match="cannot replace"):
        case.slot.retain_generation(construction, case.context)
    assert first is case.slot.generation and first.received
    assert construction.phase == "new"
    current = snapshot(case)[1]
    decision, grant, _ = reserve_again(case, current)
    assert decision.admitted and grant is not None
    active = snapshot(case)[1]
    context = JournalRecoveryContext.original(case.binding, active, grant)
    before = snapshot(case)
    for job, claimed in (
        (current, context),
        (active, replace(context, worker_epoch=context.worker_epoch + 1)),
        (replace(active, recovery_received=True), context),
    ):
        case.slot.job = job
        with pytest.raises(RuntimeError, match="original live slot"):
            case.slot.retain_generation(construction, claimed)
        assert case.slot.generation is first and construction.phase == "new"
        assert snapshot(case) == before
    case.slot.job = active


@pytest.mark.parametrize("after_root_write", [False, True])
def test_new_grant_and_job_original_roll_back_together(origin, monkeypatch, after_root_write):
    case = origin
    adopt(case)
    final = received(case)
    settle(case, final)
    before = snapshot(case)
    current = before[1]
    original_save = case.recovery._save
    observed = []

    def fail(session, root, updated):
        row = session.get(Job, current.workspace_id)
        original = JournalRecoveryContext.from_bytes(row.recovery_original_json.encode())
        assert original.grant_identity == updated.grants[0].identity
        assert row.recovery_receipt_json == final.encoded and not row.recovery_received
        observed.append(original)
        if after_root_write:
            original_save(session, root, updated)
        raise InterruptedError("grant reply was not committed")

    with monkeypatch.context() as patch:
        patch.setattr(case.recovery, "_save", fail)
        with pytest.raises(InterruptedError, match="not committed"):
            reserve_again(case, current)
    assert len(observed) == 1 and observed[0] != final.context
    assert snapshot(case) == before
    decision, grant, root = reserve_again(case, current)
    assert decision.admitted and grant is not None
    _, active = snapshot(case)
    assert active.recovery_original_json == observed[0].encoded.decode()
    assert active.recovery_receipt_json == final.encoded and not active.recovery_received
    # The old exact final reply remains reconcilable while the next original
    # is held. Its retry cannot receive that new original or change its root.
    assert settle(case, final, revision=0) == ("already-received", root)
    assert snapshot(case)[1] == active


@pytest.mark.skipif(os.name != "posix", reason="POSIX retained retirement backend")
@pytest.mark.parametrize("selected", [False, True], ids=["unpublished", "published"])
def test_physical_retirement_keeps_job_until_its_exact_cost_receives(
    origin, cleanups, monkeypatch, selected
):
    case = origin
    if selected:
        adopt(case)
    else:
        proposed = JournalGenerationReceipt(case.context, None, 11 * NS)
        with pytest.raises(BlockingIOError, match="have not received"):
            settle(case, proposed)
    case.owner.close()
    final = received(case) if selected else JournalGenerationReceipt(case.context, None, 11 * NS)
    if not selected:
        before = snapshot(case)
        with pytest.raises(Conflict, match="physically received"):
            settle(case, final)
        assert snapshot(case) == before
    before = snapshot(case)[0]
    with case.factory() as session, session.begin():
        session.execute(delete(Journal)).close()
    original = cleanups(SimpleNamespace(factory=case.factory, root=case.owner.root))
    current = original.cleanup
    for _ in range(1000):
        _, job = snapshot(case)
        assert job.recovery_original_json == case.context.encoded.decode()
        assert not job.recovery_received
        if job.directory_received:
            break
        retirement_step(current, octet_budget=65536)
    else:
        pytest.fail("finite physical retirement did not receive")
    assert job.state == "directory" and not case.directory.exists()
    assert snapshot(case)[0] == before

    def unexpected_path_work(*args, **kwargs):
        pytest.fail("received physical retirement was repeated while awaiting cost")

    with monkeypatch.context() as patch:
        patch.setattr(
            retirement_owner, "remove_retired_journal_workspace_directory", unexpected_path_work
        )
        assert retirement_step(current) == 0
        assert snapshot(case)[1] == job
        status, root = settle(case, final, acknowledge=False)
        assert status == "received" and root.grants == ()
        _, committed = snapshot(case)
        assert committed.recovery_receipt_json == final.encoded and not committed.recovery_received
        # A committed-but-unknown reply still leaves the original routable.
        assert retirement_step(current) == 0
        assert snapshot(case)[1] == committed
        with case.factory() as session, session.begin():
            with pytest.raises(Conflict, match="unavailable for acknowledgement"):
                jobs.acknowledge_recovery_original(session, replace(final, total_actual_ns=12 * NS))
        assert snapshot(case)[1] == committed
        assert root.accounting.successful_checkpoints == int(selected)
        assert root.accounting.held_count_slots == root.accounting.reserved_ns == 0
        assert root.accounting.charged_ns == 11 * NS
        assert settle(case, final, revision=0) == ("already-received", root)
        recovered = jobs.JournalGenerationReceiving(final, control_bytes=32768)
        assert settle(case, final, revision=0, receiver=recovered) == ("already-received", root)
        _, received_job = snapshot(case)
        assert received_job.directory_received and received_job.recovery_received
        assert received_job.version == job.version
        assert retirement_step(current) == 1
    with case.factory() as session:
        assert session.get(Job, job.workspace_id) is None
    assert case.recovery.snapshot(case.binding, guard=_guard) == root
    assert not case.directory.exists() and case.engine.pool.checkedout() == 0


@pytest.mark.growth_witness(
    "riverhog-validation-grant-original-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.retain_recovery_grant",
)
def test_issued_original_retention_is_indexed_metadata_work(unopened_origin, record_property):
    case = unopened_origin
    binding = growth_binding(jobs.retain_recovery_grant)
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
                            "workspace_id": f"{index + 1:032x}",
                            "collection_id": None,
                            "journal_id": None,
                            "state": "retiring",
                        }
                    )
                    for index in range(unrelated)
                )
        with case.engine.connect() as connection:
            rows = connection.exec_driver_sql("SELECT * FROM journal_validation_jobs").all()
        controls = octets((asdict(case.job), case.binding.document(), case.grant.document()))
        dimensions = {
            "retained_rows": len(rows),
            "metadata_row_octets": max(controls, *(octets(row) for row in rows)),
            "control_octets": controls,
        }
        for cold in (False, True):
            with case.factory() as session, session.begin():
                session.execute(
                    update(Job)
                    .where(Job.workspace_id == case.job.workspace_id)
                    .values(recovery_original_json=None, recovery_received=True)
                ).close()
            recorder = CatalogRecorder(jobs)
            try:
                with observe_codecs(recorder, case.engine, cold=cold):
                    with case.factory() as session, session.begin():
                        # Isolate the issued-original callback's entry state;
                        # the actual root transaction is covered above.
                        entry = replace(
                            case.job, recovery_original_json=None, recovery_received=True
                        )
                        jobs.retain_recovery_grant(
                            session, case.binding, entry, case.grant, control_bytes=32768
                        )
                        session.rollback()
            finally:
                with case.factory() as session, session.begin():
                    session.execute(
                        update(Job)
                        .where(Job.workspace_id == case.job.workspace_id)
                        .values(
                            recovery_original_json=case.context.encoded.decode(),
                            recovery_received=False,
                        )
                    ).close()
            assert recorder.metrics["sql_statements"] == recorder.metrics["changed_rows"] == 1
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
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-validation-original-ack-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_jobs.acknowledge_recovery_original",
)
def test_known_receipt_acknowledgement_uses_indexed_metadata_work(origin, record_property):
    case = origin
    adopt(case)
    final = received(case)
    settle(case, final, acknowledge=False)
    binding = growth_binding(jobs.acknowledge_recovery_original)
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
                            "workspace_id": f"{index + 1:032x}",
                            "collection_id": None,
                            "journal_id": None,
                            "state": "retiring",
                        }
                    )
                    for index in range(unrelated)
                )
        with case.engine.connect() as connection:
            rows = connection.exec_driver_sql("SELECT * FROM journal_validation_jobs").all()
        controls = octets((final.encoded, asdict(case.job)))
        dimensions = {
            "retained_rows": len(rows),
            "metadata_row_octets": max(controls, *(octets(row) for row in rows)),
            "control_octets": controls,
        }
        for acknowledged in (False, True):
            with case.factory() as session, session.begin():
                session.execute(
                    update(Job)
                    .where(Job.workspace_id == case.job.workspace_id)
                    .values(recovery_received=acknowledged)
                ).close()
            for cold in (False, True):
                recorder = CatalogRecorder(jobs)
                with observe_codecs(recorder, case.engine, cold=cold):
                    with case.factory() as session, session.begin():
                        assert (
                            jobs.acknowledge_recovery_original(session, final) is not acknowledged
                        )
                        session.rollback()
                assert recorder.metrics["sql_statements"] == 1 + int(acknowledged)
                assert recorder.metrics["changed_rows"] == int(not acknowledged)
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
    record_property("growth_samples", samples)
