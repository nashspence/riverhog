from __future__ import annotations

from pathlib import Path

import pytest


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
