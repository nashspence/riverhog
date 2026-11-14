from __future__ import annotations

import pytest

from tests.integration.test_canonical_discovery_corpus_reclamation_postgres import case as case
from tests.integration.test_canonical_discovery_corpus_reclamation_postgres import (
    database_url as database_url,
)
from tests.integration.test_canonical_discovery_corpus_reclamation_postgres import (
    fixed_inputs as fixed_inputs,
)
from tests.integration.test_canonical_discovery_corpus_reclamation_postgres import (
    original as original,
)
from tests.integration.test_canonical_discovery_corpus_reclamation_postgres import (
    prepared as prepared,
)
from tests.integration.test_canonical_discovery_corpus_reclamation_postgres import (
    qualified as qualified,
)
from tests.unit.test_canonical_discovery_root_reclamation import current_job, prepare_root, step

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("variant", ("absent", "prefix", "control"))
def test_postgresql_root_receiving_closes_core_before_exact_metadata_phase(case, variant):
    job, path = prepare_root(case, variant=variant)
    result = step(case, job)
    assert result.metadata_closed and result.job.reclamation_phase == "metadata"
    assert current_job(case, job.workspace_id) == result.job
    assert not (path / "journals").exists()
    assert path.is_dir() and (path / "discovery.sqlite3").is_file()
    assert case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("committed", (False, True))
def test_postgresql_root_phase_receives_rollback_or_committed_lost_reply(
    case, monkeypatch, committed
):
    from tests.unit.test_canonical_discovery_root_reclamation import (
        test_root_phase_rollback_or_committed_lost_reply_preserves_metadata_bytes as receive,
    )

    receive(case, monkeypatch, committed)
    assert case.engine.pool.checkedout() == 0
