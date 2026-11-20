from __future__ import annotations

from types import SimpleNamespace

import pytest
from riverhog_provenance_contracts import ContractCatalog

from tests.integration.test_canonical_discovery_retired_metadata_postgres import (
    database_url as database_url,
)
from tests.integration.test_canonical_discovery_retired_metadata_postgres import (
    fixed_inputs as fixed_inputs,
)
from tests.integration.test_canonical_discovery_retired_metadata_postgres import (
    original as original,
)
from tests.integration.test_canonical_discovery_retired_metadata_postgres import (
    prepared as prepared,
)
from tests.integration.test_canonical_discovery_retired_metadata_postgres import (
    qualified as qualified,
)
from tests.unit.test_canonical_discovery_corpus_reclamation import (
    current_job,
    finish,
    prepare_corpus,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def case(prepared, fixed_inputs, tmp_path):
    factory, engine, job, frozen = prepared
    return SimpleNamespace(
        factory=factory,
        engine=engine,
        job=job,
        frozen=frozen,
        build_id=job.build_id,
        admission=fixed_inputs[0],
        root=tmp_path / "owned",
        catalog=ContractCatalog(),
    )


@pytest.mark.parametrize("pinned", (False, True))
def test_postgresql_original_corpus_file_receiving_preserves_parent_and_exact_binding(case, pinned):
    job, path = prepare_corpus(case, pinned=pinned, extent=17 if pinned else None)
    result = finish(case, job, budget=3)
    assert result.reclamation_phase == "journals_root" and result.build_id is None
    assert current_job(case, job.workspace_id) == result
    assert not (path / "corpus.sqlite3").exists()
    assert path.is_dir() and (path / "discovery.sqlite3").is_file()
    if pinned:
        assert result.binding == job.binding
    assert case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("committed", (False, True))
def test_postgresql_original_corpus_phase_receives_rollback_or_committed_lost_reply(
    case, monkeypatch, committed
):
    from tests.unit.test_canonical_discovery_corpus_reclamation import (
        test_original_corpus_phase_rollback_or_committed_lost_reply as receive,
    )

    receive(case, monkeypatch, committed)
    assert case.engine.pool.checkedout() == 0
