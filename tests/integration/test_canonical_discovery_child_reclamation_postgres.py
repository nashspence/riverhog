from __future__ import annotations

import pytest

from tests.integration.test_canonical_discovery_retired_metadata_postgres import case as case
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
from tests.unit.test_canonical_discovery_child_reclamation import (
    current_job,
    directory_job,
    prepare_child,
    step,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("prefix", ("data", "root", "directory"))
def test_postgresql_original_child_files_namespace_and_routing_are_received_in_order(case, prefix):
    job, path, bindings = prepare_child(case, prefix=prefix, confirmed=prefix == "data")
    selected = directory_job(case, job, budget=3)
    assert current_job(case, job.workspace_id) == selected
    assert selected.reclamation_journal_id == bindings[0].journal_id
    result = step(case, selected)
    assert result.job.reclamation_phase == "children"
    assert result.job.reclamation_directory_id is None
    assert current_job(case, job.workspace_id) == result.job
    assert not (path / "journals" / bindings[0].directory_id).exists()
    assert path.is_dir() and case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("committed", (False, True))
def test_postgresql_exact_child_phase_rollback_and_committed_lost_reply(
    case, monkeypatch, committed
):
    from tests.unit.test_canonical_discovery_child_reclamation import (
        test_phase_rollback_or_lost_commit_retains_exact_selected_child as receive,
    )

    receive(case, monkeypatch, committed)
    assert case.engine.pool.checkedout() == 0
