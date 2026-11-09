from __future__ import annotations

from types import SimpleNamespace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_provenance_contracts import ContractCatalog

from tests.integration.test_canonical_discovery_corpus_jobs_postgres import (
    database_url as database_url,
)
from tests.integration.test_canonical_discovery_corpus_jobs_postgres import (
    fixed_inputs as fixed_inputs,
)
from tests.integration.test_canonical_discovery_corpus_jobs_postgres import original as original
from tests.integration.test_canonical_discovery_corpus_jobs_postgres import prepared as prepared
from tests.integration.test_canonical_discovery_corpus_jobs_postgres import qualified as qualified
from tests.unit.test_canonical_discovery_retired_metadata import prepare_retired, retired

pytestmark = pytest.mark.integration


@pytest.fixture
def case(prepared, fixed_inputs, tmp_path):
    factory, engine, job, _ = prepared
    return SimpleNamespace(
        factory=factory,
        engine=engine,
        job=job,
        build_id=job.build_id,
        admission=fixed_inputs[0],
        root=tmp_path / "owned",
        catalog=ContractCatalog(),
    )


@pytest.mark.parametrize("confirmed", (False, True))
def test_postgresql_retired_core_routing_fences_snapshot_and_exact_selected_child(case, confirmed):
    job, _, bindings = prepare_retired(case, confirmed=confirmed)
    with retired(case, job) as metadata:
        metadata.open()
        assert metadata.child().binding == bindings[0]
        assert metadata.child().confirmed is confirmed
        with case.factory() as session, session.begin():
            selected = jobs.advance_discovery_reclamation(
                session,
                job,
                "child_directory",
                journal_id=bindings[1].journal_id,
                directory_id=bindings[1].directory_id,
            )
        assert selected is not None and not metadata.verify_current()
        with pytest.raises(StaleArchiveTask):
            metadata.child()
        metadata.close()
        fresh = RetiredDiscoveryMetadata(case.factory, selected, metadata.retirement)
        try:
            fresh.open()
            assert fresh.child().binding == bindings[1]
        finally:
            fresh.close()
    assert case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("committed", (False, True))
def test_postgresql_retired_core_routing_receives_rollback_or_lost_phase_reply(case, committed):
    job, _, bindings = prepare_retired(case)
    with retired(case, job) as metadata:
        metadata.open()
        with pytest.raises(OSError, match="phase reply"):
            with case.factory() as session:
                with session.begin():
                    successor = jobs.advance_discovery_reclamation(
                        session,
                        job,
                        "child_directory",
                        journal_id=bindings[1].journal_id,
                        directory_id=bindings[1].directory_id,
                    )
                    assert successor is not None
                    if not committed:
                        raise OSError("phase reply rolled back")
                raise OSError("phase reply lost")
        assert metadata.verify_current() is (not committed)
        if committed:
            with pytest.raises(StaleArchiveTask):
                metadata.child()
            with case.factory() as session:
                current = jobs.DiscoveryJob.read(session.get(Job, job.workspace_id))
            assert current == successor
            metadata.close()
            fresh = RetiredDiscoveryMetadata(case.factory, current, metadata.retirement)
            try:
                fresh.open()
                assert fresh.child().binding == bindings[1]
            finally:
                fresh.close()
        else:
            assert metadata.child().binding == bindings[0]
    assert case.engine.pool.checkedout() == 0
