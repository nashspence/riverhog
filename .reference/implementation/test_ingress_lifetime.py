"""Actual-owner counterexamples plus bounded proposed composition witnesses.

No remote Riverhog calls or installed native service controls are used. Baseline
tests assert observed gaps intentionally; they are not shipped-behavior passes.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from collections.abc import Callable
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import a_riverhog_ftp_spool.landing as landing
import pytest
from a_riverhog_ftp_spool.app import create_app
from a_riverhog_ftp_spool.completion import CompletionHandoff
from a_riverhog_ftp_spool.config import FtpSpoolConfig, SourceConfig
from a_riverhog_ftp_spool.landing import FtpSpool
from a_riverhog_ftp_spool.operational_state import FtpOperationalState
from a_riverhog_ftp_spool_client.events import ATTEMPT_FAILED, CLAIM_PUBLISHED
from gogurt_listener_runtime.listener import ListenerStore
from ingress_lifetime import (
    ATTEMPT_EVENT_INDEX,
    CloseBeforeRetire,
    IngressLifetime,
    LifetimePending,
    PassOutcome,
    cold_retry_seed,
    dispatch_disposition,
    may_remove_unregistered_candidate,
    retry_page,
)
from riverhog_client.producer import ProducedCollection

from tests.provenance_observer import native_provenance_provider


def _config(tmp_path: Path) -> FtpSpoolConfig:
    return FtpSpoolConfig(
        primary_state={"scope": {"service": "fixture-service", "account": "fixture-account"}},
        host_id="urn:uuid:00000000-0000-4000-8000-000000000001",
        riverhog_base_url="https://riverhog.invalid",
        riverhog_token="fixture-token",
        api_token="fixture-api-token",
        provenance_observer=native_provenance_provider().name,
        sources=(
            SourceConfig(id="camera-a", root=tmp_path / "source", ingest_source="ftp:camera-a"),
        ),
    )


class _Api:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls = 0
        self.fail = False

    def close(self) -> None:
        self.calls += 1
        self.events.append("api-close")
        if self.fail:
            raise OSError("fixture API close outcome is unknown")


def _receive(events: list[str]) -> Callable[[PassOutcome], None]:
    def receive(outcome: PassOutcome) -> None:
        events.append("pass-received")
        assert outcome.error is None

    return receive


def test_baseline_asgi_lifespan_closes_api_before_original_pass_terminates(
    tmp_path: Path,
) -> None:
    """Exercise actual create_app, without real HTTP or remote API calls."""
    entered, release, finished = Event(), Event(), Event()
    events: list[str] = []
    api = _Api(events)

    def run_once() -> None:
        entered.set()
        try:
            assert release.wait(5)
        finally:
            events.append("pass-exited")
            finished.set()

    composition = SimpleNamespace(
        config=_config(tmp_path), api=api, adapter=SimpleNamespace(run_once=run_once)
    )
    app = create_app(composition)  # type: ignore[arg-type]

    async def scenario() -> None:
        try:
            async with app.router.lifespan_context(app):
                assert await asyncio.to_thread(entered.wait, 5)
            assert api.calls == 1
            assert not finished.is_set()
        finally:
            release.set()
            assert await asyncio.to_thread(finished.wait, 5)

    asyncio.run(scenario())  # joins the loop's actual default executor on exit
    assert events == ["api-close", "pass-exited"]


def test_proposal_stop_preserves_original_worker_slot_and_receives_before_api_close() -> None:
    entered, release = Event(), Event()
    events: list[str] = []
    api = _Api(events)
    owner = IngressLifetime(api, resources_received=lambda: True, receive_pass=_receive(events))

    def operation() -> str:
        entered.set()
        assert release.wait(5)
        events.append("pass-exited")
        return "original-result"

    original = owner.submit(operation)
    try:
        assert entered.wait(5)
        with pytest.raises(LifetimePending, match="slot"):
            owner.submit(lambda: None)
        owner.begin_stop()
        with pytest.raises(LifetimePending, match="stopped"):
            owner.submit(lambda: None)
        assert owner.status()["pass"] == "running"
        assert not owner.receive_pass()
        assert not owner.settle_stop()
        assert original.retained and owner.original_pass is original
        assert api.calls == 0
    finally:
        release.set()
        original.original_worker.join(5)
    assert not original.original_worker.is_alive()
    assert owner.receive_pass()
    assert owner.settle_stop()
    assert not original.retained
    assert events == ["pass-exited", "pass-received", "api-close"]


def test_proposal_lost_result_receiving_retains_same_result_and_receiver() -> None:
    received: list[PassOutcome] = []
    failed = True
    api = _Api([])

    def receive(outcome: PassOutcome) -> None:
        received.append(outcome)
        if failed:
            raise ConnectionError("receipt committed but reply was lost")

    owner = IngressLifetime(api, resources_received=lambda: True, receive_pass=receive)
    original = owner.submit(lambda: object())
    original.original_worker.join(5)
    owner.begin_stop()
    with pytest.raises(ConnectionError):
        owner.receive_pass()
    assert owner.status()["pass"] == "receiving-unknown"
    assert original.retained
    assert not owner.settle_stop()
    with pytest.raises(LifetimePending):
        owner.receive_pass()
    assert len(received) == 1 and api.calls == 0
    failed = False
    assert owner.receive_pass(retry_original=True)
    assert received[0] is received[1]
    assert owner.settle_stop()


def test_proposal_storage_receiving_and_api_close_have_independent_obligations() -> None:
    resources_received = False
    api = _Api([])
    owner = IngressLifetime(
        api,
        resources_received=lambda: resources_received,
        receive_pass=lambda outcome: None,
    )
    owner.begin_stop()
    assert not owner.settle_stop() and api.calls == 0
    resources_received = True
    api.fail = True
    with pytest.raises(OSError):
        owner.settle_stop()
    assert owner.status()["api_close_received"] is False
    with pytest.raises(LifetimePending):
        owner.settle_stop()
    assert api.calls == 1
    api.fail = False
    assert owner.settle_stop(retry_api_close=True)
    assert api.calls == 2


@pytest.mark.parametrize("after", [False, True])
def test_proposal_refused_or_lost_close_retains_original_before_claim_reclamation(
    tmp_path: Path, after: bool
) -> None:
    root = tmp_path / "claim"
    root.mkdir()
    (root / "original-observation").write_bytes(b"original nondeterministic graph")
    events: list[str] = []
    fail = True

    class Storage:
        calls = 0
        closed = False

        def close(self) -> None:
            self.calls += 1
            events.append("original-close")
            if after or not fail:
                self.closed = True
            if fail:
                raise OSError("original close unconfirmed")

    storage = Storage()

    def retire() -> None:
        assert storage.closed
        events.append("claim-retired")
        (root / "original-observation").unlink()
        root.rmdir()

    owner = CloseBeforeRetire(storage, retire)
    with pytest.raises(OSError):
        owner.settle()
    assert owner.original is storage and owner.retained and root.exists()
    with pytest.raises(LifetimePending):
        owner.settle()
    assert storage.calls == 1
    fail = False
    owner.settle(retry_original=True)
    assert not root.exists() and not owner.retained
    assert events == ["original-close", "original-close", "claim-retired"]


def test_proposal_retirement_failure_does_not_reclose_received_original() -> None:
    api = _Api([])
    fail = True

    def retire() -> None:
        if fail:
            raise OSError("unlink receipt unknown")

    owner = CloseBeforeRetire(api, retire)
    with pytest.raises(OSError):
        owner.settle()
    assert owner.closed and owner.retained and not owner.retired
    fail = False
    owner.settle()
    assert api.calls == 1 and not owner.retained


class _RegistrationReplyLost(RuntimeError):
    pass


@pytest.mark.parametrize("guard_candidate", [False, True], ids=["baseline-gap", "proposed-guard"])
def test_actual_claim_registration_reply_loss_and_original_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, guard_candidate: bool
) -> None:
    """Real completion/fsync, native observation, PrimaryStore and SQLite commit.

    The proposal's sole splice is its cleanup predicate in the existing exception
    path. No registration or custody primitive is replaced by a model.
    """
    config = _config(tmp_path)
    source = config.sources[0]
    source.root.mkdir(parents=True)
    path = source.root / "sample.bin"
    path.write_bytes(b"original ingress bytes")
    handoff = CompletionHandoff(source.root, source.id).complete(path)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]
    original_commit = FtpOperationalState.commit
    injected = False

    def commit(state: FtpOperationalState) -> None:
        nonlocal injected
        original_commit(state)
        if not injected and state.one("SELECT COUNT(*) FROM claims")[0]:
            injected = True
            raise _RegistrationReplyLost("actual claim committed; original reply lost")

    original_rmtree = landing.shutil.rmtree
    retained_candidates: list[Path] = []
    if guard_candidate:

        def guarded_rmtree(root: Path, *args: object, **kwargs: object) -> None:
            if not may_remove_unregistered_candidate(root, all_original_resources_received=True):
                retained_candidates.append(root)
                return
            original_rmtree(root, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(landing.shutil, "rmtree", guarded_rmtree)
    monkeypatch.setattr(FtpOperationalState, "commit", commit)
    with pytest.raises(_RegistrationReplyLost):
        adapter._claim_new_batch(source)
    assert injected
    with adapter._open_state(source) as state:
        [claim_id, encoded] = state.one("SELECT claim_id, manifest_json FROM claims")
    manifest = json.loads(encoded)
    claim_root = adapter._claims_root(source) / claim_id
    assert (source.root / handoff.custody).read_bytes() == b"original ingress bytes"
    assert claim_root.exists() is guard_candidate
    if guard_candidate:
        assert retained_candidates == [claim_root]
        [file] = manifest["files"]
        observation = adapter._load_observation(claim_root, file)
        original_manifest = (claim_root / "claim.json").read_bytes()
        observation_digest = hashlib.sha256(
            (claim_root / file["observation"]["graph_path"]).read_bytes()
        ).hexdigest()
        adapter._register_claim(source, manifest)
        assert (claim_root / "claim.json").read_bytes() == original_manifest
        assert observation is not None
        assert observation_digest == file["observation"]["graph_sha256"]
    assert adapter.retained_state_storage == ()


def test_baseline_receipted_claim_reclaims_before_primary_store_exit(tmp_path: Path) -> None:
    """Actual _complete_receipted_claim control path with an observed store port."""
    config = _config(tmp_path)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]
    source = config.sources[0]
    root = adapter._claims_root(source) / "fixture-claim"
    root.mkdir(parents=True)
    events: list[str] = []

    class OriginalStore:
        custody_settled = True

        def __enter__(self) -> OriginalStore:
            return self

        def __exit__(self, *args: object) -> None:
            events.append("original-close")

    adapter._primary_store = lambda *args, **kwargs: OriginalStore()  # type: ignore[method-assign]
    adapter._record_publication = lambda *args: events.append("publication")  # type: ignore[method-assign]
    adapter._finish_claim_cleanup = lambda *args: events.append("reclaim")  # type: ignore[method-assign]
    receipt = ProducedCollection(
        collection_id=41,
        archive_root_sha256="a" * 64,
        artifact_set_identity="b" * 64,
        receipt={"state": "finalized"},
    )
    assert adapter._complete_receipted_claim(source, "fixture-claim", receipt) is receipt
    assert events == ["publication", "reclaim", "original-close"]


def test_proposal_cold_retry_fairness_uses_original_outcomes_without_cursor_writes(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]
    source = config.sources[0]
    claims = [f"{number:064x}" for number in range(1, 6)]
    with adapter._open_state(source) as state:
        state.execute(ATTEMPT_EVENT_INDEX)
        for claim_id in claims:
            state.execute(
                "INSERT INTO claims(claim_id, manifest_json, claim_bytes) VALUES (?, ?, 0)",
                (claim_id, "{}"),
            )
        state.commit()
    attempted = []
    for number in range(10):
        # Reopen each pass: the previous volatile cursor is deliberately lost.
        with adapter._open_state(source) as state:
            initial_changes = state._db.total_changes
            seed = cold_retry_seed(state)
            [(ordinal, claim_id)] = retry_page(state, volatile_after=seed, limit=1)
            assert state._db.total_changes == initial_changes
            assert (
                state.one("SELECT value FROM adapter_state WHERE key = 'reconcile_after'") is None
            )
            plan = state.all(
                "EXPLAIN QUERY PLAN SELECT event_json FROM lifecycle_events "
                "INDEXED BY lifecycle_events_kind_sequence_idx "
                "WHERE json_extract(event_json, '$.type') = ? ORDER BY sequence DESC LIMIT 1",
                (ATTEMPT_FAILED,),
            )
            assert "USING INDEX lifecycle_events_kind_sequence_idx" in str(plan)
        attempted.append(claim_id)
        assert ordinal == claims.index(claim_id) + 1
        adapter._record_claim_event(
            source,
            {"claim_id": claim_id, "source_event_id": claim_id},
            event_type=ATTEMPT_FAILED,
            details={"error_type": "ConnectionError"},
        )
        # A newer successful admission/publication must not reset failed-claim fairness.
        adapter._record_claim_event(
            source,
            {"claim_id": f"{100 + number:064x}", "source_event_id": str(number)},
            event_type=CLAIM_PUBLISHED,
            details={
                "collection_id": "41",
                "archive_root_sha256": "a" * 64,
                "artifact_set_identity": "b" * 64,
            },
        )
    assert attempted == claims * 2


def test_proposal_retired_outcome_seed_falls_back_without_historical_scan(tmp_path: Path) -> None:
    config = _config(tmp_path)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]
    source = config.sources[0]
    with adapter._open_state(source) as state:
        state.execute(ATTEMPT_EVENT_INDEX)
        state.commit()
        assert cold_retry_seed(state) == 0
    adapter._record_claim_event(
        source,
        {"claim_id": "a" * 64, "source_event_id": "retired"},
        event_type=ATTEMPT_FAILED,
        details={"error_type": "ConnectionError"},
    )
    with adapter._open_state(source) as state:
        assert cold_retry_seed(state) == 0


def _dispatch(store: ListenerStore) -> str:
    store.create()
    with sqlite3.connect(store.path) as db:
        db.execute(
            "INSERT INTO dispatches(dispatch_id, mount_point, generation, marker_identity, "
            "route, plan_json, state, attempts, observed_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "original-dispatch",
                "/fixture/mount",
                1,
                "original-marker",
                "fixture-route",
                "{}",
                "queued",
                0,
                1,
            ),
        )
    return "original-dispatch"


def test_actual_gogurt_nonzero_exit_currently_authorizes_retry(tmp_path: Path) -> None:
    """Current source evidence; the proposed hard cut deliberately changes this."""
    store = ListenerStore(tmp_path / "listener.sqlite3")
    dispatch_id = _dispatch(store)
    assert store.start_dispatch(dispatch_id, now=2) == {}
    assert store.finish_dispatch(dispatch_id, return_code=73, error=None, now=3) == "retry"
    assert store.runnable(now=100, limit=1) == [dispatch_id]
    assert (
        dispatch_disposition(entry="entered", return_code=73, original_wait_received=True)
        == "failed-effects-unproven"
    )


def test_actual_gogurt_restart_preserves_unknown_entry_without_automatic_retry(
    tmp_path: Path,
) -> None:
    store = ListenerStore(tmp_path / "listener.sqlite3")
    dispatch_id = _dispatch(store)
    assert store.start_dispatch(dispatch_id, now=2) == {}
    ListenerStore(store.path).create()
    assert store.summary()["counts"] == {"uncertain": 1}
    assert store.runnable(now=100, limit=1) == []


@pytest.mark.parametrize(
    ("entry", "exit_code", "received", "expected"),
    [
        ("not-entered", None, False, "retryable-before-entry"),
        ("unknown", None, False, "uncertain"),
        ("entered", 0, False, "uncertain"),
        ("entered", 0, True, "completed"),
        ("entered", 73, True, "failed-effects-unproven"),
    ],
)
def test_proposed_arbitrary_command_entry_and_original_wait_boundary(
    entry: str, exit_code: int | None, received: bool, expected: str
) -> None:
    assert (
        dispatch_disposition(entry=entry, return_code=exit_code, original_wait_received=received)
        == expected
    )
