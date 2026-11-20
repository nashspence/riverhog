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


def test_postgresql_actual_complete_discovery_retirement_chain(case):
    from tests.unit.test_canonical_discovery_retirement import (
        test_actual_full_core_child_metadata_parent_and_catalog_chain as receive,
    )

    receive(case)
    assert case.engine.pool.checkedout() == 0


def test_postgresql_actual_pinned_corpus_turn(case):
    from tests.unit.test_canonical_discovery_retirement import (
        test_actual_pinned_corpus_extent_keeps_original_binding_and_finishes as receive,
    )

    receive(case)
    assert case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("variant", ("absent", "root", "directory", "lease"))
def test_postgresql_unconfirmed_prefix_turn(case, variant):
    from tests.unit.test_canonical_discovery_retirement import (
        test_actual_unconfirmed_creation_prefix_has_noncreating_receiving as receive,
    )

    receive(case, variant)
    assert case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("mode", ("prefix-probe", "child-lease", "child-directory"))
def test_postgresql_actual_child_close_receiving_keeps_parent(case, monkeypatch, mode):
    from tests.unit.test_canonical_discovery_retirement import (
        test_failed_child_descriptor_close_retains_child_and_parent_ownership as receive,
    )

    receive(case, monkeypatch, mode)
    assert case.engine.pool.checkedout() == 0


@pytest.mark.parametrize("lost_reply", (False, True))
def test_postgresql_core_close_receiving_keeps_original_parent(case, monkeypatch, lost_reply):
    from tests.unit.test_canonical_discovery_retirement import (
        test_actual_core_close_refusal_retains_exclusive_parent_until_receiving as receive,
    )

    receive(case, monkeypatch, lost_reply)
    assert case.engine.pool.checkedout() == 0
