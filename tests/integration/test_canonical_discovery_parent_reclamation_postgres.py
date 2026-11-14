from __future__ import annotations

import pytest

from tests.integration.test_canonical_discovery_metadata_reclamation_postgres import case as case
from tests.integration.test_canonical_discovery_metadata_reclamation_postgres import (
    database_url as database_url,
)
from tests.integration.test_canonical_discovery_metadata_reclamation_postgres import (
    fixed_inputs as fixed_inputs,
)
from tests.integration.test_canonical_discovery_metadata_reclamation_postgres import (
    original as original,
)
from tests.integration.test_canonical_discovery_metadata_reclamation_postgres import (
    prepared as prepared,
)
from tests.integration.test_canonical_discovery_metadata_reclamation_postgres import (
    qualified as qualified,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("variant", ("absent", "root", "directory", "lease"))
def test_postgresql_original_empty_prefix_receiving_preserves_catalog(case, variant):
    from tests.unit.test_canonical_discovery_parent_reclamation import (
        test_actual_initial_prefix_is_received_without_creation_or_repair as receive,
    )

    receive(case, variant)
    assert case.engine.pool.checkedout() == 0


def test_postgresql_confirmed_core_and_handle_chain_precedes_parent_receiving(case):
    from tests.unit.test_canonical_discovery_parent_reclamation import (
        test_actual_confirmed_core_file_and_handle_receiving_precede_parent_removal as receive,
    )

    receive(case)
    assert case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("committed", (False, True))
def test_postgresql_parent_phase_rollback_and_committed_lost_reply(case, monkeypatch, committed):
    from tests.unit.test_canonical_discovery_parent_reclamation import (
        test_phase_rollback_or_committed_lost_reply_preserves_cold_receiving as receive,
    )

    receive(case, monkeypatch, committed)
    assert case.engine.pool.checkedout() == 0
