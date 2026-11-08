from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_finalizer_registry import (
    _actual_description_terminal,
)
from tests.unit.test_archive_finalizer_registry import (
    owned as owned,
)
from tests.unit.test_archive_initial_description_checkpoint import _description_source

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("mode", ["direct", "http"])
@pytest.mark.parametrize("committed", [False, True])
def test_postgres_registry_keeps_original_inputs_after_joined_execution_receiving(
    database_url, tmp_path, monkeypatch, owned, mode, committed
):
    with _description_source(
        tmp_path, monkeypatch, mode=mode, database_url=database_url
    ) as fixture:
        _actual_description_terminal(fixture, monkeypatch, owned, committed)


@pytest.mark.parametrize("committed", (False, True))
def test_postgres_proposed_claim_resolves_transaction_exit_before_semantic_children(
    database_url, tmp_path, monkeypatch, committed
):
    from tests.unit.test_archive_finalization_admission import (
        test_original_claim_handoff_resolves_rollback_and_lost_commit_ack_before_children,
    )

    test_original_claim_handoff_resolves_rollback_and_lost_commit_ack_before_children(
        tmp_path, monkeypatch, committed, database_url=database_url
    )


def test_postgres_contended_claim_observation_retains_original_pipeline(
    database_url, tmp_path, monkeypatch
):
    from tests.unit.test_archive_finalization_admission import (
        test_contended_claim_observation_stays_in_the_same_retained_pipeline,
    )

    test_contended_claim_observation_stays_in_the_same_retained_pipeline(
        tmp_path, monkeypatch, database_url=database_url
    )
