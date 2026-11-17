"""Decisive reply-loss witnesses at the unchanged relay and witness owners."""

import json
import os
import sqlite3
import sys
from collections.abc import Iterator
from pathlib import Path
from time import perf_counter_ns
from typing import Any

import pytest
from effects_experiment import (
    idle_poll_case,
    idle_setup,
    observe_idle_page,
    relay_case,
    witness_case,
)

RESULTS: list[dict[str, Any]] = []


@pytest.fixture(scope="module", autouse=True)
def retain_actual_observations() -> Iterator[None]:
    started = perf_counter_ns()
    yield
    destination = os.environ.get("RIVERHOG_EFFECTS_REPORT")
    if destination:
        Path(destination).write_text(
            json.dumps(
                {
                    "schema": "riverhog-reference-application-effects-1",
                    "source_sha": "c28eceaa2dfec1b3712135469ba12230e74f5ffa",
                    "scope": "actual stores; proposed idle-poll branch; fixture external ports",
                    "python": sys.version.split()[0],
                    "sqlite": sqlite3.sqlite_version,
                    "rows": RESULTS,
                    "elapsed_ns_including_test_fixtures": perf_counter_ns() - started,
                    "limits": [
                        "no live effects, native signer, installed or new sampler contract tested",
                        "commit reply-loss injection is not process-death or power-loss evidence",
                        "no physical sync count or performance improvement is claimed",
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )


@pytest.mark.parametrize("cursor_reply_loss", [False, True])
def test_delivery_retry_retains_exact_event_identity_and_page_boundary(
    tmp_path: Path, cursor_reply_loss: bool
) -> None:
    result = relay_case(tmp_path / "case", cursor_reply_loss=cursor_reply_loss)
    assert result["cursor_after_retry"] == "2"
    assert result["distinct_event_ids"] == 2
    RESULTS.append(result)


@pytest.mark.parametrize("opentimestamps", [False, True])
def test_witness_accepted_evidence_survives_lost_commit_reply_and_rebaseline(
    tmp_path: Path, opentimestamps: bool
) -> None:
    result = witness_case(tmp_path / "case", opentimestamps=opentimestamps)
    assert result["fixture_effect_calls"] == 1
    assert result["exact_result_received_from_reopened_store"]
    RESULTS.append(result)


@pytest.mark.parametrize("opentimestamps", [False, True])
def test_idle_witness_page_keeps_currentness_without_progress_write(
    tmp_path: Path, opentimestamps: bool
) -> None:
    result = idle_poll_case(tmp_path / "case", opentimestamps=opentimestamps)
    assert result["same_accepted_position"]
    RESULTS.append(result)


def test_idle_witness_page_refuses_concurrent_generation_change(tmp_path: Path) -> None:
    store, api, module = idle_setup(tmp_path / "case", opentimestamps=False)
    original = api.list_catalog_sync_changes

    def changed(cursor: str, *, limit: int = 100) -> Any:
        batch = original(cursor, limit=limit)
        store.rebaseline()
        return batch

    api.list_catalog_sync_changes = changed
    with pytest.raises(module.StaleProposal, match="generation or serial"):
        observe_idle_page(store, api, stale_error=module.StaleProposal)
    assert store.progress().generation == 1
    RESULTS.append({"case": "idle_page_concurrent_generation_change", "stale_rejected": True})
