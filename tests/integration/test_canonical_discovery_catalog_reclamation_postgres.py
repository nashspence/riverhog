from __future__ import annotations

import pytest

from tests.integration.test_canonical_discovery_parent_reclamation_postgres import case as case
from tests.integration.test_canonical_discovery_parent_reclamation_postgres import (
    database_url as database_url,
)
from tests.integration.test_canonical_discovery_parent_reclamation_postgres import (
    fixed_inputs as fixed_inputs,
)
from tests.integration.test_canonical_discovery_parent_reclamation_postgres import (
    original as original,
)
from tests.integration.test_canonical_discovery_parent_reclamation_postgres import (
    prepared as prepared,
)
from tests.integration.test_canonical_discovery_parent_reclamation_postgres import (
    qualified as qualified,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("variant", ("absent", "root", "directory", "lease"))
def test_postgresql_original_physical_then_catalog_receiving(case, variant):
    from tests.unit.test_canonical_discovery_catalog_reclamation import (
        test_actual_parent_close_precedes_exact_catalog_deletion_and_cold_receiving as receive,
    )

    receive(case, variant)
    assert case.engine.pool.checkedout() == 0


def test_postgresql_confirmed_core_chain_then_catalog_receiving(case):
    from tests.unit.test_canonical_discovery_catalog_reclamation import (
        test_actual_confirmed_core_chain_finishes_without_reopening_disposed_metadata as receive,
    )

    receive(case)
    assert case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("committed", (False, True))
def test_postgresql_catalog_rollback_and_committed_lost_reply(case, monkeypatch, committed):
    from tests.unit.test_canonical_discovery_catalog_reclamation import (
        test_catalog_delete_rollback_or_lost_reply_requires_new_physical_receiving as receive,
    )

    receive(case, monkeypatch, committed)
    assert case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("unknown", (False, True))
def test_postgresql_missing_catalog_preserves_surviving_original_namespace(case, unknown):
    from tests.unit.test_canonical_discovery_catalog_reclamation import (
        test_missing_catalog_cannot_dispose_even_an_empty_surviving_namespace as receive,
    )

    receive(case, unknown)
    assert case.engine.pool.checkedout() == 0
