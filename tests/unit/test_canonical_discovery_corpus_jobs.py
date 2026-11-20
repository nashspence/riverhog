from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as owner
from riverhog_core import canonical_discovery_selection as selection
from riverhog_core.canonical_discovery_history_work import ArchiveHistoryWork
from riverhog_core.canonical_discovery_index import begin_index_build
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as UploadControl
from riverhog_core.catalog_db import Base, create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_core.catalog_models import CollectionDeletionRecord as Deletion
from riverhog_core.catalog_models import CollectionRecord as Collection
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexGenerationRecord as Generation,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexStateRecord as State,
)
from riverhog_core.services.upload_journal_result_receiving import UploadJournalResultResources
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import delete, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from tests.support.canonical_discovery_work import write
from tests.support.validation_workspace import leased_workspace
from tests.unit.test_canonical_discovery_index import _collection
from tests.unit.test_canonical_discovery_selection import original as original
from tests.unit.test_canonical_discovery_selection import qualified as qualified


@pytest.fixture(scope="module")
def fixed_inputs(qualified):
    path, values = qualified
    key, admission, reader, manifest, _, _ = values
    with closing(sqlite3.connect(":memory:", isolation_level=None)) as db:
        with closing(sqlite3.connect(path)) as source:
            source.backup(db)
        work = selection.CorpusSelectionWork(ArchiveHistoryWork(admission, reader), manifest)
        with write(db):
            selection.begin_corpus_selection(db, key, work)
        while selection.corpus_selection_progress(db, key, work).phase != "complete":
            page = selection.prepare_corpus_selection(db, key, work)
            with write(db):
                selection.apply_corpus_selection(db, key, work, page)
        return admission, selection.frozen_corpus_selection(db, key, work)


@pytest.fixture
def case(tmp_path, fixed_inputs):
    admission, frozen = fixed_inputs
    engine = create_catalog_engine("sqlite:///" + str(tmp_path / "catalog.sqlite3"))
    factory = sessionmaker(engine, expire_on_commit=False)
    Base.metadata.create_all(engine)
    with factory() as session, session.begin():
        collection = _collection()
        collection.archive_generation = admission.root.archive_generation
        collection.archive_root_sha256 = admission.archive_sha256
        collection.provenance_identity = admission.root.identity
        collection.artifact_set_identity = admission.root.artifact_set_sha256
        session.add(collection)
        session.flush()
        build_id = begin_index_build(session, collection_id=collection.id)
    catalog = ContractCatalog()
    selected = owner.select_discovery_job(
        factory, build_id=build_id, admission=admission, catalog=catalog
    )
    assert selected is not None
    try:
        yield SimpleNamespace(
            engine=engine,
            factory=factory,
            job=selected,
            admission=admission,
            frozen=frozen,
            root=tmp_path / "work",
            catalog=catalog,
            build_id=build_id,
        )
    finally:
        engine.dispose()


def claimed(case):
    result = owner.claim_discovery_worker(case.factory, case.job)
    assert result is not None
    confirmed = owner.confirm_discovery_storage(case.factory, result)
    assert confirmed is not None and confirmed.state == "seeding"
    return confirmed


def test_one_attempt_and_original_binding_survive_claim_pin_and_reopen(case):
    with leased_workspace(
        case.root,
        case.job.workspace_id,
        create=True,
        creation_current=lambda: owner.discovery_worker_current(case.factory, case.job),
    ) as path:
        assert path is not None
        first = claimed(case)
        assert first.worker_epoch == 1 and first.attempt_id == case.job.attempt_id
        assert not owner.discovery_worker_current(case.factory, case.job)
        assert owner.discovery_worker_current(case.factory, first)
        pinned = owner.pin_discovery_corpus(case.factory, first, case.frozen)
        assert pinned is not None and pinned.state == "ready"
        assert pinned.binding.source_count == case.admission.root.journal_count
        assert pinned.binding.selection_count == case.frozen.selection_count
        assert pinned.key.worker_id == case.job.attempt_id
        assert owner.pin_discovery_corpus(case.factory, pinned, case.frozen) == pinned
        active = owner.activate_discovery_corpus(case.factory, pinned)
        assert active is not None and active.state == "active"
        assert owner.pin_discovery_corpus(case.factory, pinned, case.frozen) == active
        with leased_workspace(case.root, first.workspace_id) as busy:
            assert busy is None
        assert case.engine.pool.checkedout() == 0
    with leased_workspace(case.root, case.job.workspace_id) as path:
        assert path is not None
        again = owner.select_discovery_job(
            case.factory, build_id=case.build_id, admission=case.admission, catalog=case.catalog
        )
        assert again == active
        second = owner.claim_discovery_worker(case.factory, again)
        assert second is not None and second.worker_epoch == 2
        assert second.binding == active.binding
        assert second.key == first.key
        assert not owner.discovery_worker_current(case.factory, first)
        assert owner.discovery_worker_current(case.factory, second)


@pytest.mark.parametrize(
    "damage",
    [
        "archive",
        "generation",
        "provenance",
        "artifacts",
        "build_root",
        "build_generation",
        "build_provenance",
        "build_core",
        "build_policy",
        "build_epoch",
        "complete",
        "pending",
        "epoch",
        "deletion",
        "worker",
        "attempt",
        "admission",
        "catalog",
        "lost",
    ],
)
def test_live_catalog_root_build_and_exact_job_fences_reject_stale_workers(case, damage):
    first = claimed(case)
    with case.factory() as session, session.begin():
        if damage in {"archive", "generation", "provenance", "artifacts"}:
            column = {
                "archive": "archive_root_sha256",
                "generation": "archive_generation",
                "provenance": "provenance_identity",
                "artifacts": "artifact_set_identity",
            }[damage]
            session.execute(update(Collection).values({column: "e" * 64}))
        elif damage.startswith("build_"):
            column = {
                "build_root": "archive_root_sha256",
                "build_generation": "archive_generation",
                "build_provenance": "provenance_identity",
                "build_core": "core_contract_sha256",
                "build_policy": "extraction_contract_sha256",
                "build_epoch": "expected_epoch",
            }[damage]
            session.execute(
                update(Generation).values({column: 2 if damage == "build_epoch" else "e" * 64})
            )
        elif damage == "complete":
            session.execute(
                update(Generation).values(
                    complete=True, dataset_sha256="e" * 64, generation_id="e" * 64
                )
            )
        elif damage in {"pending", "epoch"}:
            session.execute(
                update(State).values(
                    {
                        ("pending_build_id" if damage == "pending" else "epoch"): "other"
                        if damage == "pending"
                        else 2
                    }
                )
            )
        elif damage == "deletion":
            session.add(
                Deletion(collection_id=1, challenge="selected", plan_json="{}", started_at="now")
            )
        else:
            values = {
                "worker": {"worker_epoch": 2},
                "attempt": {"attempt_id": "e" * 32},
                "admission": {"admission_sha256": "e" * 64},
                "catalog": {"catalog_sha256": "e" * 64},
                "lost": {"state": "lost", "failure": "missing retained workspace"},
            }[damage]
            session.execute(update(Job).values(**values))
    assert not owner.discovery_worker_current(case.factory, first)
    assert owner.claim_discovery_worker(case.factory, first) is None
    assert owner.discovery_worker_snapshot(case.factory, first) is None
    assert owner.pin_discovery_corpus(case.factory, first, case.frozen) is None
    assert case.engine.pool.checkedout() == 0


def test_pin_requires_claimed_original_source_count_and_never_rebinds(case):
    with pytest.raises(owner.StaleArchiveTask, match="claimed"):
        owner.pin_discovery_corpus(case.factory, case.job, case.frozen)
    first = claimed(case)
    for frozen in (
        replace(case.frozen, sources=replace(case.frozen.sources, source_count=2)),
        replace(case.frozen, sources=replace(case.frozen.sources, admission_sha256="e" * 64)),
    ):
        with pytest.raises(owner.StaleArchiveTask, match="claimed"):
            owner.pin_discovery_corpus(case.factory, first, frozen)
    pinned = owner.pin_discovery_corpus(case.factory, first, case.frozen)
    assert pinned is not None
    with pytest.raises(owner.StaleArchiveTask, match="changed"):
        owner.pin_discovery_corpus(
            case.factory, pinned, replace(case.frozen, selection_sha256="e" * 64)
        )
    assert owner.discovery_worker_current(case.factory, pinned)


def test_missing_workspace_disposition_cannot_recreate_or_reclaim_storage(case):
    first = claimed(case)
    marker = case.root / "operator-custody"
    marker.parent.mkdir()
    marker.write_bytes(b"retained")
    lost = owner.lose_discovery_corpus(case.factory, first, failure="retained database missing")
    assert lost is not None and lost.state == "lost"
    assert not owner.discovery_worker_current(case.factory, first)
    assert owner.claim_discovery_worker(case.factory, lost) is None
    assert marker.read_bytes() == b"retained"
    with case.factory() as session, session.begin():
        retired = owner.retire_discovery_job(session, lost)
    assert retired is not None and retired.build_id is None and retired.state == "retiring"
    assert marker.read_bytes() == b"retained"
    with pytest.raises(owner.StaleArchiveTask, match="retired"):
        _ = retired.key


def test_job_survives_deleted_generation_without_retaining_current_authority(case):
    first = claimed(case)
    with case.factory() as session, session.begin():
        session.execute(delete(Generation).where(Generation.build_id == case.build_id))
    assert not owner.discovery_worker_current(case.factory, first)
    with case.factory() as session:
        row = session.get(Job, first.workspace_id)
        assert row is not None and row.build_id is None
        orphan = owner.DiscoveryJob.read(row)
    with case.factory() as session, session.begin():
        assert owner.retire_discovery_job(session, orphan).state == "retiring"


@pytest.mark.parametrize("raw", ["", "00", "-1", "١", " 1", "1.0"])
def test_native_input_binding_rejects_malformed_original_counts(case, raw):
    first = claimed(case)
    pinned = owner.pin_discovery_corpus(case.factory, first, case.frozen)
    assert pinned is not None
    with pytest.raises(owner.StaleArchiveTask, match="counts"):
        _ = replace(pinned, source_count=raw).binding


def test_failed_pin_rolls_back_catalog_update_and_closes_owned_connection(case):
    first = claimed(case)
    with case.engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TRIGGER fail_pin BEFORE UPDATE ON discovery_validation_jobs "
            "WHEN NEW.state='ready' BEGIN SELECT RAISE(ABORT,'injected pin failure'); END"
        )
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError, match="injected pin"):
        owner.pin_discovery_corpus(case.factory, first, case.frozen)
    with case.factory() as session:
        assert owner.DiscoveryJob.read(session.get(Job, first.workspace_id)) == first
    assert case.engine.pool.checkedout() == 0


def test_unconfirmed_storage_cannot_pin_and_uncertain_confirmation_preserves_progress(case):
    with pytest.raises(owner.StaleArchiveTask, match="claimed"):
        owner.confirm_discovery_storage(case.factory, case.job)
    first = owner.claim_discovery_worker(case.factory, case.job)
    assert first is not None and first.state == "preparing"
    assert owner.pin_discovery_corpus(case.factory, first, case.frozen) is None
    confirmed = owner.confirm_discovery_storage(case.factory, first)
    assert confirmed is not None and confirmed.state == "seeding"
    assert owner.confirm_discovery_storage(case.factory, first) == confirmed
    assert owner.discovery_worker_snapshot(case.factory, first) == confirmed
    assert owner.confirm_discovery_storage(case.factory, confirmed) == confirmed
    pinned = owner.pin_discovery_corpus(case.factory, confirmed, case.frozen)
    assert pinned is not None and pinned.state == "ready"
    assert owner.confirm_discovery_storage(case.factory, pinned) == pinned
    active = owner.activate_discovery_corpus(case.factory, pinned)
    assert active is not None
    assert owner.confirm_discovery_storage(case.factory, pinned) == active
    assert owner.confirm_discovery_storage(case.factory, active) == active


def test_storage_confirmation_rejects_superseded_and_stale_initialization(case):
    first = owner.claim_discovery_worker(case.factory, case.job)
    assert first is not None
    assert owner.confirm_discovery_storage(case.factory, replace(first, version="e" * 64)) is None
    with case.factory() as session, session.begin():
        session.execute(update(State).values(epoch=2))
    assert owner.confirm_discovery_storage(case.factory, first) is None


def _retirement_rollback(factory, job):
    with factory() as session:
        before = owner.DiscoveryJob.read(session.get(Job, job.workspace_id))
        epoch = session.get(Generation, job.build_id).expected_epoch
    with pytest.raises(OSError, match="composing owner rollback"):
        with factory() as session, session.begin():
            retired = owner.retire_discovery_job(session, job)
            assert retired is not None and retired.build_id is None
            # A preceding catalog mutation belongs to this same transaction.
            session.execute(
                update(Generation)
                .where(Generation.build_id == job.build_id)
                .values(expected_epoch=Generation.expected_epoch + 1)
            )
            raise OSError("composing owner rollback")
    with factory() as session:
        assert owner.DiscoveryJob.read(session.get(Job, job.workspace_id)) == before
        assert session.get(Generation, job.build_id).expected_epoch == epoch
    with factory() as session, session.begin():
        retired = owner.retire_discovery_job(session, job)
        assert retired is not None and retired.build_id is None and retired.state == "retiring"
    return retired


def test_discovery_job_retirement_rolls_back_with_the_composing_catalog_transaction(case):
    first = claimed(case)
    marker = case.root / "independently-retained"
    marker.parent.mkdir()
    marker.write_bytes(b"original workspace")
    retired = _retirement_rollback(case.factory, first)
    assert retired.attempt_id == first.attempt_id and marker.read_bytes() == b"original workspace"


def _retirement_stale(factory, job):
    current = owner.lose_discovery_corpus(factory, job, failure="retained workspace unavailable")
    assert current is not None and current.version != job.version
    with factory() as session, session.begin():
        assert owner.retire_discovery_job(session, job) is None
        assert owner.DiscoveryJob.read(session.get(Job, job.workspace_id)) == current
        retired = owner.retire_discovery_job(session, current)
    assert retired is not None and retired.build_id is None and retired.state == "retiring"


def test_discovery_job_retirement_keeps_the_complete_original_and_version_fence(case):
    _retirement_stale(case.factory, claimed(case))


def _source_gate(factory, *, gate="closed", collection_id=1):
    with factory() as session, session.begin():
        session.add(
            UploadControl(
                collection_id=collection_id,
                construction_json="{}",
                epoch="a" * 64,
                gate=gate,
                next_call="0",
            )
        )


def _closed_source(factory, job, admission, catalog, frozen):
    first = owner.claim_discovery_worker(factory, job)
    assert first is not None
    confirmed = owner.confirm_discovery_storage(factory, first)
    assert confirmed is not None
    pinned = owner.pin_discovery_corpus(factory, confirmed, frozen)
    assert pinned is not None
    _source_gate(factory)
    assert not owner.discovery_worker_current(factory, pinned)
    assert owner.discovery_worker_snapshot(factory, pinned) is None
    assert owner.claim_discovery_worker(factory, pinned) is None
    assert owner.confirm_discovery_storage(factory, pinned) is None
    assert owner.pin_discovery_corpus(factory, pinned, frozen) is None
    assert owner.activate_discovery_corpus(factory, pinned) is None
    assert (
        owner.select_discovery_job(
            factory, build_id=pinned.build_id, admission=admission, catalog=catalog
        )
        is None
    )
    with factory() as session:
        assert owner.DiscoveryJob.read(session.get(Job, pinned.workspace_id)) == pinned
    return pinned


def test_closed_native_upload_invalidates_discovery_source_without_erasing_its_job(case):
    _closed_source(case.factory, case.job, case.admission, case.catalog, case.frozen)
    assert case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("gate", ("open", "unrelated"))
def test_open_or_unrelated_native_gate_preserves_exact_discovery_source(case, gate):
    _source_gate(
        case.factory,
        gate="open" if gate == "open" else "closed",
        collection_id=1 if gate == "open" else 2,
    )
    assert owner.discovery_worker_current(case.factory, case.job)
    assert (
        owner.select_discovery_job(
            case.factory, build_id=case.build_id, admission=case.admission, catalog=case.catalog
        )
        == case.job
    )


def test_closed_native_gate_cannot_construct_a_new_discovery_job(case):
    with case.factory() as session, session.begin():
        session.execute(delete(Job).where(Job.workspace_id == case.job.workspace_id))
    _source_gate(case.factory)
    assert (
        owner.select_discovery_job(
            case.factory, build_id=case.build_id, admission=case.admission, catalog=case.catalog
        )
        is None
    )
    with case.factory() as session:
        assert session.get(Job, case.job.workspace_id) is None


def _closed_projection(tmp_path, monkeypatch, *, database_url=None, journal_constructions):
    from riverhog_protocol.errors import Conflict

    from tests.unit.test_archive_write_admission import _case, _selected

    with _case(tmp_path, database_url=database_url) as native:
        _selected(native)

        def forbidden(*args, **kwargs):
            pytest.fail("closed native upload reentered catalog projection")

        monkeypatch.setattr(native.service, "_commit_catalog_projection", forbidden)
        with pytest.raises(Conflict, match="admission is closed"):
            native.service._advance_catalog_projection(
                native.collection_id,
                result_resources=UploadJournalResultResources(
                    construction_factory=journal_constructions.retain
                ),
            )


def test_native_catalog_projection_keeps_the_original_upload_write_interlock(
    tmp_path, monkeypatch, *, journal_constructions
):
    _closed_projection(tmp_path, monkeypatch, journal_constructions=journal_constructions)


def _original_storage_retirement(factory, job, root, *, detach):
    from pathlib import Path

    from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask

    with leased_workspace(
        root,
        job.workspace_id,
        create=True,
        creation_current=lambda: owner.discovery_worker_current(factory, job),
    ) as directory:
        assert directory is not None
        marker = Path(directory) / "independent-owner-marker"
        marker.write_bytes(b"retained original storage")
        first = owner.claim_discovery_worker(factory, job)
        assert first is not None
        assert first.storage_key == first.key
        assert first.original_build_id == job.build_id
        if detach:
            with factory() as session, session.begin():
                session.execute(delete(Generation).where(Generation.build_id == job.build_id))
            with factory() as session:
                original = owner.DiscoveryJob.read(session.get(Job, job.workspace_id))
            assert original.build_id is None and original.original_build_id == job.build_id
        else:
            original = first
        with factory() as session, session.begin():
            retired = owner.retire_discovery_job(session, original)
        assert retired is not None and retired.build_id is None and retired.state == "retiring"
        assert retired.original_build_id == job.build_id
        assert retired.storage_key == first.key
        with pytest.raises(StaleArchiveTask, match="no current build"):
            _ = retired.key
        assert not owner.discovery_worker_current(factory, first)
        assert not owner.discovery_worker_current(factory, retired)
        assert owner.discovery_worker_snapshot(factory, retired) is None
        with factory() as session:
            assert owner.DiscoveryJob.read(session.get(Job, job.workspace_id)) == retired
        assert marker.read_bytes() == b"retained original storage"
    return retired


@pytest.mark.parametrize("detach", (False, True))
def test_original_storage_build_survives_exact_retirement_and_generation_detachment(case, detach):
    _original_storage_retirement(case.factory, case.job, case.root, detach=detach)


def _original_storage_fence(factory, job):
    changed = replace(job, original_build_id="ffffffff-ffff-ffff-ffff-ffffffffffff")
    assert owner.claim_discovery_worker(factory, changed) is None
    with factory() as session, session.begin():
        assert owner.retire_discovery_job(session, changed) is None
        assert owner.DiscoveryJob.read(session.get(Job, job.workspace_id)) == job


def test_original_storage_identity_participates_in_the_exact_worker_and_retirement_cas(case):
    _original_storage_fence(case.factory, case.job)


def _original_storage_constraint(factory, job):
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError, match="ck_discovery_validation_original_build"):
        with factory() as session, session.begin():
            session.execute(
                update(Job)
                .where(Job.workspace_id == job.workspace_id)
                .values(original_build_id="ffffffff-ffff-ffff-ffff-ffffffffffff")
            )
    with factory() as session:
        assert owner.DiscoveryJob.read(session.get(Job, job.workspace_id)) == job


def test_live_catalog_binding_preserves_its_original_storage_identity(case):
    _original_storage_constraint(case.factory, case.job)


def _reclamation_stages(factory, job):
    storage = job.storage_state
    with factory() as session, session.begin():
        retired = owner.retire_discovery_job(session, job)
    assert retired is not None and retired.build_id is None
    assert retired.storage_state == storage
    assert retired.reclamation_phase == ("children" if storage == "confirmed" else "metadata")
    assert retired.original_build_id == job.original_build_id

    def read():
        with factory() as session:
            return owner.DiscoveryJob.read(session.get(Job, job.workspace_id))

    def advance(selected, phase, **keys):
        with factory() as session, session.begin():
            return owner.advance_discovery_reclamation(session, selected, phase, **keys)

    if storage == "confirmed":
        with factory() as session, session.begin():
            assert (
                owner.retire_discovery_job(
                    session,
                    replace(retired, storage_state="unconfirmed"),
                )
                is None
            )
        child = advance(
            retired,
            "child_directory",
            journal_id="registered original journal",
            directory_id="d" * 32,
        )
        assert child is not None
        assert (child.reclamation_journal_id, child.reclamation_directory_id) == (
            "registered original journal",
            "d" * 32,
        )
        with factory() as session, session.begin():
            assert owner.retire_discovery_job(session, child) == child
        assert read() == child

        with pytest.raises(OSError, match="phase rollback"):
            with factory() as session, session.begin():
                assert owner.advance_discovery_reclamation(session, child, "children") is not None
                raise OSError("phase rollback")
        assert read() == child

        with pytest.raises(OSError, match="phase acknowledgement"):
            with factory() as session, session.begin():
                received = owner.advance_discovery_reclamation(session, child, "children")
                assert received is not None
            raise OSError("phase acknowledgement")
        current = read()
        assert current == received and current.reclamation_journal_id is None
        assert advance(child, "children") is None
        assert current.storage_state == "confirmed"
        with pytest.raises(ValueError, match="next irreversible"):
            advance(current, "metadata")
        for phase in ("corpus", "journals_root", "metadata", "directory"):
            selected = advance(current, phase)
            assert selected is not None and selected.storage_state == storage
            current = selected
    else:
        with factory() as session, session.begin():
            assert (
                owner.retire_discovery_job(
                    session,
                    replace(retired, storage_state="confirmed"),
                )
                is None
            )
        with pytest.raises(ValueError, match="next irreversible"):
            advance(retired, "children")
        current = advance(retired, "directory")
        assert current is not None

    assert current.state == current.reclamation_phase == "directory"
    assert current.reclamation_directory_id is current.reclamation_journal_id is None
    with factory() as session, session.begin():
        assert owner.retire_discovery_job(session, current) == current
        assert owner.retire_discovery_job(session, job) is None
    assert advance(current, "children") is None
    assert not owner.discovery_worker_current(factory, current)
    assert read() == current
    assert factory.kw["bind"].pool.checkedout() == 0


@pytest.mark.parametrize(
    "confirmed,lost", [(False, False), (False, True), (True, False), (True, True)]
)
def test_original_storage_and_irreversible_reclamation_stages_survive_restart_and_receiving(
    case,
    confirmed,
    lost,
):
    current = claimed(case) if confirmed else case.job
    if lost:
        current = owner.lose_discovery_corpus(
            case.factory,
            current,
            failure="original storage open unavailable",
        )
        assert current is not None
    with leased_workspace(
        case.root,
        current.workspace_id,
        create=True,
        creation_current=lambda: True,
    ) as path:
        assert path is not None
        marker = path / "unclassified"
        marker.write_bytes(b"not catalog disposal authority")
        _reclamation_stages(case.factory, current)
        assert marker.read_bytes() == b"not catalog disposal authority"


@pytest.mark.parametrize(
    "values",
    [
        {"reclamation_phase": "metadata"},
        {"state": "retiring", "build_id": None},
        {"state": "retiring", "build_id": None, "reclamation_phase": "children"},
        {
            "state": "retiring",
            "build_id": None,
            "reclamation_phase": "child_directory",
            "storage_state": "confirmed",
        },
        {
            "state": "directory",
            "build_id": None,
            "reclamation_phase": "children",
            "storage_state": "confirmed",
        },
    ],
)
def test_catalog_cannot_admit_live_or_unconfirmed_discovery_reclamation_authority(case, values):
    with pytest.raises(IntegrityError):
        with case.factory() as session, session.begin():
            session.execute(
                update(Job).where(Job.workspace_id == case.job.workspace_id).values(**values)
            )
    with case.factory() as session:
        assert owner.DiscoveryJob.read(session.get(Job, case.job.workspace_id)) == case.job


def test_original_native_binding_preserves_counts_beyond_default_python_digit_limit(case):
    import sys

    from riverhog_canonical_json import format_scalar

    raw = "9" * (sys.int_info.default_max_str_digits + 1)
    pinned = replace(
        case.job,
        source_manifest_sha256="a" * 64,
        selection_manifest_sha256="b" * 64,
        source_count=raw,
        selection_count=raw,
        journal_count=raw,
    )
    binding = pinned.binding
    assert format_scalar("nonnegative", binding.source_count) == raw
    assert format_scalar("nonnegative", binding.selection_count) == raw
    assert raw.encode() in binding.to_bytes()
    assert binding.attempt_id == pinned.attempt_id
    assert binding.directory_id == pinned.workspace_id
