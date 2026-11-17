"""Real application owners at reply-loss boundaries; no live external effects.

This preserves the source implementation. HTTP/calendar/signer ports are fixtures;
the SQLite stores, canonical statements, proof handling and relay are real owners.
It is correctness evidence, not a proposed delivery engine or performance baseline.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path
from time import perf_counter_ns
from types import ModuleType
from typing import Any
from unittest.mock import patch

import a_riverhog_minisign_witness.store as minisign_store
import a_riverhog_opentimestamps_witness.store as ots_store
import httpx
from a_riverhog_event_relay.relay import (
    EventRelay,
    EventRelayConfig,
    SourceConfig,
    SourceRelayError,
)
from a_riverhog_event_relay.schema import upgrade_state as upgrade_relay
from a_riverhog_minisign_witness.schema import upgrade_state as upgrade_minisign
from a_riverhog_opentimestamps_witness.schema import upgrade_state as upgrade_ots
from lifecycle_events import lifecycle_event
from riverhog_client.following import CatalogFollower

ROOT = Path(__file__).resolve().parents[2]
APPS = ROOT / "some-implementations/riverhog/applications"


def fixture(application: str, filename: str) -> ModuleType:
    path = APPS / application / "tests" / filename
    spec = importlib.util.spec_from_file_location("effects_" + application.replace("-", "_"), path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def relay_case(directory: Path, *, cursor_reply_loss: bool) -> dict[str, Any]:
    directory.mkdir(parents=True)
    token, webhook = directory / "token", directory / "webhook"
    token.write_text("fixture-only-token\n")
    webhook.write_text("https://receiver.invalid/hook\n")
    source = SourceConfig(
        name="fixture",
        events_url="https://source.invalid/v1/events",
        token_file=token,
        webhook_url_file=webhook,
    )
    config = EventRelayConfig(state_path=directory / "relay.db", sources=(source,))
    upgrade_relay(config.state_path)
    events = [
        lifecycle_event(type="io.riverhog.stove0.attempt.issue", subject=subject)
        for subject in ("first", "second")
    ]
    accepted: list[bytes] = []
    fetches: list[str] = []

    def contact(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            cursor = request.url.params["after"]
            fetches.append(cursor)
            return httpx.Response(
                200,
                json={
                    "events": [event.model_dump(mode="json") for event in events]
                    if cursor == "0"
                    else [],
                    "next_cursor": "2",
                    "has_more": False,
                },
            )
        accepted.append(request.content)
        if not cursor_reply_loss and len(accepted) == 1:
            raise httpx.ReadError("fixture accepted the event but lost its reply", request=request)
        return httpx.Response(200)

    relay = EventRelay(config)
    original_advance = relay.state.advance

    def advance(source_name: str, cursor: str) -> None:
        original_advance(source_name, cursor)
        raise OSError("fixture lost cursor commit reply after real commit")

    with httpx.Client(transport=httpx.MockTransport(contact)) as client:
        try:
            if cursor_reply_loss:
                with patch.object(relay.state, "advance", advance):
                    relay.relay_source_once(source, client=client)
            else:
                relay.relay_source_once(source, client=client)
        except SourceRelayError as error:
            failed_phase = error.phase
        else:
            raise AssertionError("the injected reply loss did not reach its owner")
        retained = relay.state.cursor(source.name)
        reopened = EventRelay(config)
        retried_delivery_count = reopened.relay_source_once(source, client=client)
        final_cursor = reopened.state.cursor(source.name)
    ids = [json.loads(body)["id"] for body in accepted]
    if cursor_reply_loss:
        assert (failed_phase, retained, retried_delivery_count) == ("state", "2", 0)
        assert len(ids) == 2 and len(set(ids)) == 2 and fetches == ["0", "2"]
    else:
        assert (failed_phase, retained, retried_delivery_count) == ("delivery", "0", 2)
        assert len(ids) == 3 and ids[0] == ids[1] and accepted[0] == accepted[1]
        assert fetches == ["0", "0"]
    assert final_cursor == "2"
    return {
        "failure": "cursor_commit_reply_loss" if cursor_reply_loss else "webhook_reply_loss",
        "failed_phase": failed_phase,
        "accepted_fixture_deliveries": len(accepted),
        "distinct_event_ids": len(set(ids)),
        "fetch_cursors": fetches,
        "cursor_after_failure": retained,
        "cursor_after_retry": final_cursor,
        "retried_delivery_count": retried_delivery_count,
        "physical_sync_calls": None,
    }


class LostCommit:
    """Delegate the real commit, then inject its missing caller acknowledgement."""

    def __init__(self, connection: sqlite3.Connection, marker: list[bool]) -> None:
        self.connection = connection
        self.marker = marker

    def __getattr__(self, name: str) -> Any:
        return getattr(self.connection, name)

    def commit(self) -> None:
        self.connection.commit()
        if not self.marker[0]:
            self.marker[0] = True
            raise OSError("fixture lost witness commit reply after real commit")


def witness_case(directory: Path, *, opentimestamps: bool) -> dict[str, Any]:
    directory.mkdir(parents=True)
    application = (
        "a-riverhog-opentimestamps-witness" if opentimestamps else "a-riverhog-minisign-witness"
    )
    filename = "test_opentimestamps_witness.py" if opentimestamps else "test_minisign_witness.py"
    maintained = fixture(application, filename)
    module = ots_store if opentimestamps else minisign_store
    path = directory / "witness.db"
    (upgrade_ots if opentimestamps else upgrade_minisign)(path)
    store = (
        module.WitnessStore(path, (maintained.URL,))
        if opentimestamps
        else module.WitnessStore(path)
    )
    api = maintained.Api()
    store.ingest_once(api, now=100)
    store.ingest_once(api, now=100)
    with sqlite3.connect(path) as db:
        digest = db.execute("SELECT digest FROM statements").fetchone()[0]
    before = store.evidence(digest)
    assert before is not None
    port = maintained.Calendar() if opentimestamps else maintained.Signer()
    original_connection = module._connection
    marker = [False]

    def connect(database: Path) -> LostCommit:
        return LostCommit(original_connection(database), marker)

    with patch.object(module, "_connection", connect):
        try:
            if opentimestamps:
                store.mature_once(port, now=100)
            else:
                store.sign_once(port, now=100)
        except OSError as error:
            assert "commit reply" in str(error)
        else:
            raise AssertionError("witness commit reply loss was not observed")
    assert marker[0] and port.calls == 1
    reopened = (
        module.WitnessStore(path, (maintained.URL,))
        if opentimestamps
        else module.WitnessStore(path)
    )
    accepted = reopened.evidence(digest)
    assert accepted is not None
    if opentimestamps:
        assert accepted["proof"] is not None and len(accepted["proof_revisions"]) == 1
        assert accepted["job"].nonce == before["job"].nonce
        assert accepted["job"].submission_commitment == before["job"].submission_commitment
        exact_result = accepted["proof"]
    else:
        assert accepted["state"] == "signed" and accepted["signature"] == b"signed"
        assert reopened.sign_once(port, now=100) is None and port.calls == 1
        exact_result = accepted["signature"]
    reopened.rebaseline()
    after_reset = reopened.evidence(digest)
    key = "proof" if opentimestamps else "signature"
    assert after_reset is not None and after_reset[key] == exact_result
    assert port.calls == 1
    return {
        "application": application,
        "fixture_effect_calls": port.calls,
        "commit_performed_before_injected_reply_loss": marker[0],
        "exact_result_received_from_reopened_store": True,
        "evidence_survives_explicit_view_rebaseline": True,
        "nonce_and_commitment_preserved": True if opentimestamps else None,
        "physical_sync_calls": None,
    }


def idle_setup(directory: Path, *, opentimestamps: bool) -> tuple[Any, Any, ModuleType]:
    """Use the maintained catalog fixture with a stable, already accepted empty tail."""
    directory.mkdir(parents=True)
    maintained = fixture("a-riverhog-minisign-witness", "test_minisign_witness.py")
    module = ots_store if opentimestamps else minisign_store
    path = directory / "witness.db"
    (upgrade_ots if opentimestamps else upgrade_minisign)(path)
    store = (
        module.WitnessStore(path, ("https://calendar.example",))
        if opentimestamps
        else module.WitnessStore(path)
    )
    api = maintained.Api()
    for _ in range(3):
        store.ingest_once(api, now=100)
    return store, api, module


def observe_idle_page(store: Any, api: Any, *, stale_error: type[RuntimeError]) -> None:
    """Proposed narrow no-change branch; changed pages need original atomic acceptance.

    A production refactor passes the same fetched batch to the existing acceptance
    owner. This experiment refuses other pages instead of fetching them a second time.
    """
    before = store.progress()
    batch = CatalogFollower(api).step(before.position, limit=100)
    if batch.collections or batch.changes or batch.after != before.position:
        raise ValueError("not an unchanged idle page; use the original page acceptance owner")
    if store.progress() != before:
        raise stale_error("catalog generation or serial changed during the idle observation")


def idle_poll_case(directory: Path, *, opentimestamps: bool) -> dict[str, Any]:
    store, api, module = idle_setup(directory, opentimestamps=opentimestamps)
    statements: list[str] = []
    connect = module._connection

    def trace(database: Path) -> sqlite3.Connection:
        connection = connect(database)
        connection.set_trace_callback(statements.append)
        return connection

    def counts() -> dict[str, int]:
        return {
            "begin_immediate": sum(sql.startswith("BEGIN IMMEDIATE") for sql in statements),
            "progress_upserts": sum(sql.startswith("INSERT INTO progress") for sql in statements),
            "commits": sum(sql == "COMMIT" for sql in statements),
        }

    with patch.object(module, "_connection", trace):
        before = store.progress()
        store.ingest_once(api, now=100)
        after = store.progress()
        baseline = counts()
        statements.clear()
        observe_idle_page(store, api, stale_error=module.StaleProposal)
        proposed = counts()
        final = store.progress()
    assert before.position == after.position == final.position
    assert after.serial == before.serial + 1 and final == after
    assert baseline == {"begin_immediate": 1, "progress_upserts": 1, "commits": 1}
    assert proposed == {"begin_immediate": 0, "progress_upserts": 0, "commits": 0}
    return {
        "application": "opentimestamps" if opentimestamps else "minisign",
        "case": "fixture_no_change_catalog_page",
        "baseline_sql_statements": baseline,
        "proposed_sql_statements": proposed,
        "same_accepted_position": True,
        "fresh_generation_and_serial_checked": True,
        "physical_sync_calls": None,
    }


def run(directory: Path) -> dict[str, Any]:
    started = perf_counter_ns()
    rows = [
        relay_case(directory / "webhook", cursor_reply_loss=False),
        relay_case(directory / "cursor", cursor_reply_loss=True),
        witness_case(directory / "minisign", opentimestamps=False),
        witness_case(directory / "opentimestamps", opentimestamps=True),
    ]
    return {
        "schema": "riverhog-reference-application-effects-1",
        "source_sha": "c28eceaa2dfec1b3712135469ba12230e74f5ffa",
        "scope": "unchanged actual application owners; fixture external ports",
        "rows": rows,
        "elapsed_ns_including_fixture": perf_counter_ns() - started,
        "limits": [
            "not live webhook, calendar, Bitcoin RPC, Minisign binary or installed qualification",
            "no new replay policy, sampler receiving contract or performance improvement is tested",
            "exception injection at real commit is not process-death or power-loss evidence",
        ],
    }
