"""Real Native images selected with the surviving Core accounting root."""

from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import journal_recovery_frontier as context_owner
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.collection_recovery_state import RecoveryRootBinding
from riverhog_core.journal_recovery_frontier import (
    JOURNAL_RECOVERY_DOMAIN,
    JournalRecoveryContext,
    JournalResumeFrontier,
    journal_recovery_fence,
)
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services.collection_recovery import CollectionRecoveryAccounting
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_protocol.errors import Conflict
from riverhog_provenance import JournalValidationBudget, JournalValidationWorkBudget
from riverhog_provenance import _generation_envelope as envelope_owner
from riverhog_provenance import _journal_image as image_owner
from riverhog_provenance._generation_storage import NativeGenerationStorage
from sqlalchemy import update

from scripts.validation_numeric import numeric_policy_from_environment
from tests.support.catalog_work import CatalogRecorder, octets
from tests.unit.test_archive_write_admission import _guard
from tests.unit.test_collection_recovery_accounting import _candidate
from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_certificate_edge_growth import ready
from tests.unit.test_upload_journal_validation_owner import finish

NS = 1_000_000_000


@pytest.fixture
def origin(tmp_path, journal_constructions):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        factory.configure(autoflush=False, expire_on_commit=False)
        work, _ = ready(factory)
        owner = UploadJournalValidationOwner(
            factory, tmp_path / "native", construction_factory=journal_constructions.retain
        )
        try:
            step = owner.advance(
                work,
                catalog=source_owner.admission_provenance_catalog(),
                budget=JournalValidationBudget(work=JournalValidationWorkBudget(transitions=1)),
            )
            assert step.progressed and step.outcome is None
            slot = owner._slots[(work.collection_id, work.journal_id, work.attempt_id)]
            job = slot.job
            with factory() as session:
                control = session.get(Control, work.collection_id)
                binding = RecoveryRootBinding.original(
                    work.collection_id, control.construction_json
                )
                epoch = control.epoch
            recovery = CollectionRecoveryAccounting(
                factory, active_grant_limit=2, control_bytes=32768
            )
            # Synthetic durations exercise policy/receipts only. They are not
            # timing observations or measured-profile qualification.
            decision, grant, reserved = recovery.reserve(
                binding,
                epoch=epoch,
                revision=0,
                domain=JOURNAL_RECOVERY_DOMAIN,
                owner_id=job.workspace_id,
                owner_fence=journal_recovery_fence(binding, job),
                useful_ns=100 * NS,
                candidate=_candidate(),
                guard=_guard,
                retain_domain=lambda session: jobs.retain_recovery_owner(session, work, job),
            )
            assert decision.admitted and grant is not None
            context = JournalRecoveryContext.original(binding, job, grant)
            directory = tmp_path / "generations"
            directory.mkdir(mode=0o700)
            storage = NativeGenerationStorage.open(
                directory,
                construction=journal_constructions.retain(),
                page_size=4096,
                reader_limit=2,
                metadata_octets=131072,
                create=True,
            )
            capture = slot.session.image_capture()
            try:
                capture.begin(
                    storage,
                    grant.identity,
                    image_octets=16 * 1024**2,
                    owner_context=context.encoded,
                )
                while capture.image is None:
                    capture.advance(page_limit=4)
                frontier = JournalResumeFrontier(capture.image)
                assert (
                    storage.envelopes.read(
                        frontier.image.identity,
                        root=frontier.image.root,
                        expected_digest=frontier.image.envelope_sha256,
                    )
                    == frontier.image.encoded
                )
                yield SimpleNamespace(
                    engine=engine,
                    factory=factory,
                    work=work,
                    owner=owner,
                    slot=slot,
                    job=job,
                    binding=binding,
                    epoch=epoch,
                    recovery=recovery,
                    grant=grant,
                    reserved=reserved,
                    capture=capture,
                    frontier=frontier,
                )
            finally:
                capture.close()
                assert capture.received
                storage.close()
        finally:
            owner.close()
        assert engine.pool.checkedout() == 0


def snapshot(case):
    root = case.recovery.snapshot(case.binding, guard=_guard)
    with case.factory() as session:
        job = jobs.ValidationJob.read(session.get(Job, case.job.workspace_id))
    return root, job


def select_frontier(case, *, job=None, grant=None, frontier=None):
    return lambda session, selected, generation: jobs.select_resume_frontier(
        session,
        case.work,
        job or case.job,
        grant or selected,
        frontier or case.frontier,
        generation=generation,
        control_bytes=32768,
    )


def adopt(case, *, revision=None, callback=None):
    received = []

    def select_original(session, grant, generation):
        received.append((callback or select_frontier(case))(session, grant, generation))

    root = case.recovery.select_generation(
        case.binding,
        epoch=case.epoch,
        revision=case.reserved.revision if revision is None else revision,
        identity=case.grant.identity,
        generation=case.frontier.generation,
        total_actual_ns=8 * NS,
        remaining_reserved_ns=2 * NS,
        guard=_guard,
        select_frontier=select_original,
    )
    return root, received[0]


def test_actual_complete_native_envelope_and_core_selection_count_once(origin):
    case = origin
    assert snapshot(case) == (case.reserved, case.job)
    selected, current = adopt(case)
    assert selected.accounting.successful_checkpoints == 1
    assert selected.accounting.held_count_slots == 0
    assert current.version != case.job.version
    assert current.resume_frontier_json == case.frontier.encoded
    assert (
        JournalResumeFrontier.from_json(current.resume_frontier_json, control_bytes=32768)
        == case.frontier
    )
    # The original live executor/storage remain held until actual receiving.
    assert not case.capture.received
    with case.factory() as session, session.begin():
        control = session.get(Control, case.work.collection_id)
        control.gate, control.epoch = "closed", "f" * 64
    assert adopt(case, revision=0) == (selected, current)
    assert snapshot(case) == (selected, current)


@pytest.mark.parametrize("after_root_write", [False, True])
def test_native_frontier_and_surviving_root_rollback_together(
    origin, monkeypatch, after_root_write
):
    case = origin
    before = snapshot(case)
    original = case.recovery._save

    def interrupted(*args):
        if after_root_write:
            original(*args)
        raise RuntimeError("original selection reply was not committed")

    with monkeypatch.context() as patch:
        patch.setattr(case.recovery, "_save", interrupted)
        with pytest.raises(RuntimeError, match="not committed"):
            adopt(case)
    assert snapshot(case) == before
    assert not case.capture.received
    selected, _ = adopt(case)
    assert selected.accounting.successful_checkpoints == 1


@pytest.mark.parametrize(
    "fault", ["source", "source-identity", "worker", "version", "binding", "grant", "generation"]
)
def test_selection_refuses_changed_originals_without_root_or_frontier_write(origin, fault):
    case = origin
    before = snapshot(case)
    if fault == "source":
        case.work = replace(case.work, version=case.work.version + 1)
    elif fault == "source-identity":
        case.work = replace(case.work, attempt_id="other-original")
    elif fault == "worker":
        case.job = replace(case.job, worker_epoch=case.job.worker_epoch + 1)
    elif fault == "version":
        case.job = replace(case.job, version="f" * 64)
    elif fault == "binding":
        case.job = replace(case.job, source_binding_json=case.job.source_binding_json + " ")
    elif fault == "grant":
        case.grant = replace(case.grant, owner_id="f" * 32)
    callback = select_frontier(case, grant=case.grant)
    if fault == "generation":
        original = callback

        def changed_generation(session, grant, generation):
            return original(session, grant, "f" * 64)

        callback = changed_generation
    with pytest.raises((Conflict, ValueError)):
        adopt(case, callback=callback)
    assert snapshot(case) == before


def test_selected_job_snapshot_preserves_later_terminal_publication(origin):
    case = origin
    _, current = adopt(case)
    case.capture.close()
    assert case.capture.received
    case.slot.job = current
    outcome = finish(case.owner, case.work)
    assert outcome.failure is None
    _, terminal = snapshot(case)
    assert terminal.state == "complete"
    assert terminal.resume_frontier_json == case.frontier.encoded
    assert jobs.outcome(terminal) == outcome


def test_exact_context_and_frontier_formats_refuse_other_meanings(origin):
    case = origin
    original = case.frontier.context
    assert JournalRecoveryContext.from_bytes(original.encoded) == original
    value = json.loads(original.encoded)
    for mutation in (
        {"worker_epoch": original.worker_epoch},
        {"worker_epoch": "01"},
        {"grant_identity": original.binding.grant_prefix + "01"},
        {"phase": "complete"},
        {"extra": "unowned"},
    ):
        with pytest.raises(ValueError):
            JournalRecoveryContext.from_bytes(canonical_json_bytes({**value, **mutation}))
    for mutation in (
        {"identity": original.binding.grant_prefix + "2"},
        {"worker_epoch": original.worker_epoch + 1},
        {"attempt_id": "other-workspace"},
    ):
        with pytest.raises(ValueError):
            JournalResumeFrontier(replace(case.frontier.image, **mutation))
    with pytest.raises(ValueError, match="metadata admission"):
        JournalResumeFrontier.from_json(case.frontier.encoded, control_bytes=1)


def test_known_original_retry_cannot_rewind_a_newer_recorded_frontier(origin):
    case = origin
    selected, current = adopt(case)
    # This is a selector-only retry fixture, not another Native publication or
    # cost measurement. The exact immutable newer descriptor is supplied here.
    newer_context = replace(case.frontier.context, grant_identity=case.binding.grant_prefix + "2")
    newer = JournalResumeFrontier(
        replace(
            case.frontier.image,
            identity=newer_context.grant_identity,
            owner_context=newer_context.encoded,
        )
    )
    with case.factory() as session, session.begin():
        session.execute(update(Job).values(resume_frontier_json=newer.encoded)).close()
    retried, received = adopt(case, revision=0)
    assert retried == selected
    assert received.resume_frontier_json == newer.encoded
    assert snapshot(case) == (selected, received)
    assert received.version == current.version


@contextmanager
def observe_codecs(recorder, engine, *, cold):
    with recorder.observe(engine, cold=cold), pytest.MonkeyPatch.context() as patch:
        for module in (context_owner, image_owner):
            patch.setattr(module, "json", jobs.json)
            patch.setattr(module, "canonical_json_bytes", jobs.canonical_json_bytes)
        patch.setattr(context_owner, "hashlib", jobs.hashlib)
        patch.setattr(envelope_owner, "hashlib", jobs.hashlib)
        yield


def test_frontier_selection_uses_indexed_metadata_work_with_unrelated_rows(origin, record_property):
    case = origin
    bindings = tuple(map(growth_binding, (jobs.retain_recovery_owner, jobs.select_resume_frontier)))
    policy = numeric_policy_from_environment(os.environ)
    projection = project_bindings(bindings, numeric_policy=policy)
    samples = []
    for unrelated in (0, 128):
        if unrelated:
            with case.factory() as session, session.begin():
                values = asdict(case.job)
                session.add_all(
                    Job(
                        **{
                            **values,
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
            source_rows = connection.exec_driver_sql(
                "SELECT * FROM collection_upload_provenance_journals"
            ).all()
        controls = octets((case.frontier.encoded, asdict(case.job), case.grant.document()))
        dimensions = {
            "retained_rows": len(metadata) + len(source_rows),
            "metadata_row_octets": max(
                controls, *(octets(row) for row in (*metadata, *source_rows))
            ),
            "control_octets": controls,
        }
        for mode, binding in zip(
            ("retain_recovery_owner", "select_resume_frontier"), bindings, strict=True
        ):
            for cold in (False, True):
                recorder = CatalogRecorder(jobs)
                with observe_codecs(recorder, case.engine, cold=cold):
                    with case.factory() as session, session.begin():
                        if mode == "retain_recovery_owner":
                            received = jobs.retain_recovery_owner(session, case.work, case.job)
                            assert received == case.job
                        else:
                            received = select_frontier(case)(
                                session, case.grant, case.frontier.generation
                            )
                            assert received.resume_frontier_json == case.frontier.encoded
                        session.rollback()
                assert recorder.metrics["sql_statements"] == (
                    2 if mode == "retain_recovery_owner" else 3
                )
                assert recorder.metrics["changed_rows"] == int(mode == "select_resume_frontier")
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
        assert snapshot(case) == (case.reserved, case.job)
    record_property("growth_samples", samples)
