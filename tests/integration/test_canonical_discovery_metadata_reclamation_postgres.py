from __future__ import annotations

import pytest

from tests.integration.test_canonical_discovery_root_reclamation_postgres import case as case
from tests.integration.test_canonical_discovery_root_reclamation_postgres import (
    database_url as database_url,
)
from tests.integration.test_canonical_discovery_root_reclamation_postgres import (
    fixed_inputs as fixed_inputs,
)
from tests.integration.test_canonical_discovery_root_reclamation_postgres import (
    original as original,
)
from tests.integration.test_canonical_discovery_root_reclamation_postgres import (
    prepared as prepared,
)
from tests.integration.test_canonical_discovery_root_reclamation_postgres import (
    qualified as qualified,
)
from tests.unit.test_canonical_discovery_metadata_reclamation import (
    current_job,
    finish,
    prepare_metadata,
    step,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("absent", (False, True))
def test_postgresql_metadata_extents_preserve_exact_directory_state_and_parent(case, absent):
    job, path = prepare_metadata(case)
    if absent:
        (path / "discovery.sqlite3").unlink()
    else:
        result = step(case, job, budget=3)
        assert result.job == job and result.files.removed_octets == 3
    result = finish(case, job)
    assert result.state == result.reclamation_phase == "directory"
    assert result.storage_key == job.storage_key
    assert current_job(case, job.workspace_id) == result
    assert not (path / "discovery.sqlite3").exists()
    assert path.is_dir() and (path / ".lease").is_file()
    assert case.engine.pool.checkedout() == 0


def test_postgresql_actual_unconfirmed_creation_prefix_needs_no_core_header_repair(
    case, monkeypatch
):
    from tests.unit.test_canonical_discovery_metadata_reclamation import (
        test_actual_unconfirmed_partial_core_creation_needs_no_header_reconstruction as receive,
    )

    receive(case, monkeypatch)
    assert case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("committed", (False, True))
def test_postgresql_metadata_phase_receives_rollback_or_committed_lost_reply(
    case, monkeypatch, committed
):
    from tests.unit.test_canonical_discovery_metadata_reclamation import (
        test_metadata_phase_uncertain_reply_preserves_directory_receiving as receive,
    )

    receive(case, monkeypatch, committed)
    assert case.engine.pool.checkedout() == 0
