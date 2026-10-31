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
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import delete, update
from sqlalchemy.orm import sessionmaker

from tests.support.canonical_discovery_work import write
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
    with lease_journal_validation_workspace(
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
        with lease_journal_validation_workspace(case.root, first.workspace_id) as busy:
            assert busy is None
        assert case.engine.pool.checkedout() == 0
    with lease_journal_validation_workspace(case.root, case.job.workspace_id) as path:
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
    retired = owner.retire_discovery_job(case.factory, lost)
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
    assert owner.retire_discovery_job(case.factory, orphan).state == "retiring"


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
