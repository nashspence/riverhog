#!/usr/bin/env python3
"""Finite reaper/queue-selection comparison, not publication elapsed qualification.

Both modes use real Core point readiness and claim selection on small maintained
catalog fixtures. The maintenance body is deliberately narrowed to that selector;
no provider, crypto, worker, finalization stage or archive publication is timed.
The separate maintained activation test covers actual original worker receiving.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_baseline(path):
    spec = importlib.util.spec_from_file_location("reference_baseline_api_app", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


async def observe(api, case, *, interval):
    from riverhog_core.catalog_db import session_scope
    from riverhog_core.catalog_models import CollectionUploadRecord as Upload
    from riverhog_core.services import collection_uploads as owner

    service = object.__new__(owner.SqlAlchemyCollectionUploadService)
    service._session_factory = case.factory
    service.has_unreceived_archive_work = lambda: False
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.identity)
        upload.state = "open"
        upload.archive_next_attempt_at = None
    loop = asyncio.get_running_loop()
    idle, selected = asyncio.Event(), asyncio.Event()
    local = threading.local()
    committed = []
    calls = []
    claims = []
    original_scope = owner.session_scope

    @contextmanager
    def observed_scope(factory):
        with original_scope(factory) as session:
            yield session
        if getattr(local, "point", False):
            committed.append(time.monotonic())

    def process(_container, *, startup_recovery=False, execution_only=False):
        claim = service._claim_due_finalization()
        calls.append({"startup": startup_recovery, "execution_only": execution_only})
        if claim is not None:
            claims.append({"when": time.monotonic(), "calls": len(calls), "collection": claim.collection_id})
            loop.call_soon_threadsafe(selected.set)
            return True
        if len(calls) == 1:
            loop.call_soon_threadsafe(idle.set)
        return False

    def make_ready():
        with session_scope(case.factory) as session:
            session.get(Upload, case.identity).state = "closing"
        local.point = True
        try:
            service._schedule_finalization_if_ready(case.identity)
        finally:
            local.point = False

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(owner, "session_scope", observed_scope)
        patch.setattr(api, "_process_archive_maintenance", process)
        task = asyncio.create_task(
            api._run_archive_upload_reaper(
                lambda: SimpleNamespace(collection_uploads=service),
                sweep_interval=timedelta(seconds=interval),
                receive_interval=timedelta(milliseconds=10),
                operation_lock=asyncio.Lock(),
            )
        )
        try:
            await asyncio.wait_for(idle.wait(), timeout=5)
            # Allow the idle control call to return to its wait. Correctness
            # also survives earlier delivery, covered by maintained race tests.
            await asyncio.sleep(0.01)
            bound = service._archive_maintenance_wakeup is not None
            await asyncio.to_thread(make_ready)
            await asyncio.wait_for(selected.wait(), timeout=5)
            assert len(committed) == len(claims) == 1
            assert claims[0]["collection"] == case.identity
            assert not calls[claims[0]["calls"] - 1]["execution_only"]
            return {
                "fallback_seconds": interval,
                "post_commit_to_claim_seconds": claims[0]["when"] - committed[0],
                "wake_listener_bound": bound,
                "control_calls_through_first_claim": claims[0]["calls"],
                "full_sweeps_through_first_claim": sum(not item["execution_only"] for item in calls[:claims[0]["calls"]]),
                "one_current_claim": True,
            }
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--baseline-app", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("--output must be a new file")
    sys.path.insert(0, str(args.repo))
    from riverhog_api import app as current
    from tests.unit import test_finalization_readiness_growth as fixture

    baseline = load_baseline(args.baseline_app)
    observations = []
    paths = [
        "riverhog/src/riverhog_api/app.py",
        "riverhog/src/riverhog_core/services/collection_uploads.py",
        "tests/unit/test_finalization_readiness_growth.py",
    ]
    identities = {path: sha(args.repo / path) for path in paths}
    with tempfile.TemporaryDirectory(prefix="idle-wake-probe-") as temporary:
        for repeat in range(3):
            for mode, api in (("baseline", baseline), ("wake", current)):
                cases = fixture.cases(Path(temporary) / f"{repeat}-{mode}", large=False)
                try:
                    case = next(cases)
                    _, ready = fixture.prepare(case, "match")
                    assert ready
                    observation = asyncio.run(observe(api, case, interval=0.4))
                    observations.append({"mode": mode, "repeat": repeat + 1, **observation})
                finally:
                    cases.close()
    assert identities == {path: sha(args.repo / path) for path in paths}
    result = {
        "scope": "Actual old/new reaper, real Core point transition and claim selector, synthetic valid catalog facts; maintenance narrowed to selector. Not a production publication benchmark.",
        "baseline_api_sha256": sha(args.baseline_app),
        "candidate_and_fixture_sha256": identities,
        "probe_sha256": sha(Path(__file__)),
        "source_unchanged_during_probe": True,
        "qualification": False,
        "observations": observations,
        "limits": [
            "0.4s fallback is a finite test configuration, not a proposed production polling change.",
            "The narrowed maintenance body excludes the other existing owner calls; maintained tests separately count all 15.",
            "No provider, crypto, physical worker, full finalization or publication time is measured.",
            "Baseline uses the unmodified requested-source reaper. Both modes use the candidate point transition; its unbound hint adds no SQL or authority.",
        ],
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"observations": len(observations), "qualification": False, "output": str(args.output)}))


if __name__ == "__main__":
    main()
