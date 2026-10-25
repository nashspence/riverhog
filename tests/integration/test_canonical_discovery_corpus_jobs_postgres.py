from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as owner
from riverhog_core.canonical_discovery_index import begin_index_build
from riverhog_core.catalog_db import create_catalog_engine, initialize_db
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexGenerationRecord as Generation,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexStateRecord as State,
)
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import delete, select
from sqlalchemy.orm import sessionmaker

from tests.integration.test_canonical_discovery_rebuild_postgres import database_url as database_url
from tests.unit.test_canonical_discovery_corpus_jobs import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_jobs import original as original
from tests.unit.test_canonical_discovery_corpus_jobs import qualified as qualified
from tests.unit.test_canonical_discovery_index import _collection

pytestmark = pytest.mark.integration


@pytest.fixture
def prepared(database_url, fixed_inputs):
    admission, frozen = fixed_inputs
    initialize_db(database_url)
    engine = create_catalog_engine(database_url)
    factory = sessionmaker(engine, autoflush=False, expire_on_commit=False)
    with factory() as session, session.begin():
        collection = _collection()
        collection.archive_generation = admission.root.archive_generation
        collection.archive_root_sha256 = admission.archive_sha256
        collection.provenance_identity = admission.provenance_sha256
        collection.artifact_set_identity = admission.root.artifact_set_sha256
        session.add(collection)
        session.flush()
        build_id = begin_index_build(session, collection_id=1)
    job = owner.select_discovery_job(
        factory, build_id=build_id, admission=admission, catalog=ContractCatalog()
    )
    assert job is not None
    try:
        yield factory, engine, job, frozen
    finally:
        engine.dispose()


def test_postgresql_discovery_claim_skips_locked_build_and_keeps_its_epoch(prepared, tmp_path):
    factory, engine, job, _ = prepared
    with lease_journal_validation_workspace(
        tmp_path / "owned",
        job.workspace_id,
        create=True,
        creation_current=lambda: owner.discovery_worker_current(factory, job),
    ) as path:
        assert path is not None
        executor = ThreadPoolExecutor(1)
        timed_out = False
        try:
            with factory() as session, session.begin():
                session.execute(select(State).where(State.collection_id == 1).with_for_update())
                future = executor.submit(owner.claim_discovery_worker, factory, job)
                try:
                    result = future.result(timeout=10)
                except TimeoutError:
                    timed_out = True
                else:
                    assert result is None
            assert not timed_out, "claim waited for a locked current build"
        finally:
            executor.shutdown(wait=True)
        assert owner.discovery_worker_current(factory, job)
        claimed = owner.claim_discovery_worker(factory, job)
        assert claimed is not None and claimed.worker_epoch == 1
    assert engine.pool.checkedout() == 0


def test_postgresql_discovery_claim_cas_and_pinned_job_survive_generation_deletion(
    prepared, tmp_path
):
    factory, engine, job, frozen = prepared
    with lease_journal_validation_workspace(
        tmp_path / "owned",
        job.workspace_id,
        create=True,
        creation_current=lambda: owner.discovery_worker_current(factory, job),
    ) as path:
        assert path is not None
        ready = Barrier(2)

        def claim():
            ready.wait(timeout=10)
            return owner.claim_discovery_worker(factory, job)

        with ThreadPoolExecutor(2) as workers:
            results = list(workers.map(lambda _: claim(), range(2)))
        claimed = [result for result in results if result is not None]
        assert len(claimed) == 1 and claimed[0].worker_epoch == 1
        confirmed = owner.confirm_discovery_storage(factory, claimed[0])
        assert confirmed is not None and confirmed.state == "seeding"
        assert owner.confirm_discovery_storage(factory, claimed[0]) == confirmed
        current = owner.pin_discovery_corpus(factory, confirmed, frozen)
        assert current is not None and current.binding.selection_count == frozen.selection_count
        assert owner.pin_discovery_corpus(factory, current, frozen) == current
        active = owner.activate_discovery_corpus(factory, current)
        assert active is not None and active.state == "active"
        assert owner.pin_discovery_corpus(factory, current, frozen) == active
        current = active
        with factory() as session, session.begin():
            session.execute(delete(Generation).where(Generation.build_id == job.build_id))
        assert not owner.discovery_worker_current(factory, current)
        with factory() as session:
            orphan = owner.DiscoveryJob.read(session.get(Job, job.workspace_id))
        assert orphan.build_id is None and orphan.binding == current.binding
        retired = owner.retire_discovery_job(factory, orphan)
        assert retired is not None and retired.state == "retiring"
    assert engine.pool.checkedout() == 0
