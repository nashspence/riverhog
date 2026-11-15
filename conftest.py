from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.support.journal_result import upload_result_openings as upload_result_openings
from tests.support.native_construction import journal_constructions as journal_constructions
from tests.support.native_construction import (
    module_journal_constructions as module_journal_constructions,
)


@pytest.fixture
def numeric_policy(request):
    """Use the qualification runner's explicit evidence resource condition."""
    from scripts.validation_numeric import (
        numeric_policy_from_environment,
        numeric_policy_identity,
        numeric_policy_snapshot,
        qualification_numeric_policy,
    )

    supplied = numeric_policy_from_environment(os.environ)
    recorder = request.config.pluginmanager.get_plugin("riverhog-ci-timing-recorder")
    if recorder is not None and recorder.numeric_policy is not None:
        owned = numeric_policy_snapshot(recorder.numeric_policy)
        if supplied != owned or numeric_policy_identity(supplied) != recorder.numeric_identity:
            raise ValueError("qualification numeric resources changed after producer admission")
        return owned
    if request.node.get_closest_marker("growth_witness") is not None and recorder is not None:
        raise ValueError("growth witness has no admitted qualification numeric resources")
    if supplied != qualification_numeric_policy():
        raise ValueError("numeric resources differ from the owned qualification profile")
    return numeric_policy_snapshot(supplied)


@pytest.fixture(autouse=True)
def isolated_provenance_user_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep repository tests from establishing a real client installation identity."""

    monkeypatch.setenv("RIVERHOG_PROVENANCE_STATE_HOME", str(tmp_path / "user-state"))


@pytest.fixture
def primary_store(tmp_path):
    """Explicit durable producer placement for one test-owned allocation."""
    from riverhog_client import PrimaryScope, PrimaryStore, PrimaryStoreLimits

    with PrimaryStore(
        tmp_path.resolve() / "producer-primary",
        allocation_id="test-allocation",
        scope=PrimaryScope(service="fixture-service", account="fixture-account"),
        limits=PrimaryStoreLimits(
            pending_bytes=64 * 1024**2,
            total_bytes=512 * 1024**2,
            preparation_bytes=32 * 1024**2,
            control_bytes=64 * 1024,
        ),
        initialize=True,
    ) as store:
        yield store
