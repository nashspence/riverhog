"""Operator entry point for the independent OpenTimestamps collection witness."""

from __future__ import annotations

import argparse
import base64
import importlib.metadata
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path

import httpx
from a_riverhog_witness_contract_lib._cli_contract import (
    BOOLEAN,
    COUNT,
    OPTIONAL_COUNT,
    OPTIONAL_TEXT,
    PROGRESS,
    STATUS,
    TEXT,
    object_schema,
    result_contract,
)
from riverhog_client import ApiClient
from riverhog_protocol.errors import RiverhogError
from state_schema import StateSchemaError

from a_riverhog_opentimestamps_witness import proof
from a_riverhog_opentimestamps_witness.network import BitcoinRpc, HttpCalendar
from a_riverhog_opentimestamps_witness.schema import state_schema
from a_riverhog_opentimestamps_witness.store import StaleProposal, WitnessStore

_CHECK = object_schema(
    {
        "status": {"enum": ["valid", "invalid", "unavailable"]},
        "height": COUNT,
        "reason": TEXT,
        "block_hash": OPTIONAL_TEXT,
        "block_time": OPTIONAL_COUNT,
        "confirmations": OPTIONAL_COUNT,
        "tip_hash": OPTIONAL_TEXT,
    }
)
_VERIFICATION = object_schema(
    {
        "proof_digest": TEXT,
        "checks": {"type": "array", "items": _CHECK},
        "pending_count": COUNT,
        "unsupported_count": COUNT,
    }
)
_CLI_RESULT_CONTRACT = result_contract(
    "a-riverhog-opentimestamps-witness",
    {
        **{
            f"state {name}": object_schema({"status": STATUS})
            for name in ("status", "upgrade", "verify")
        },
        "ingest": object_schema(
            {
                "catalog_batch": {"enum": ["checkpoint", "catalog", "changes", "reset"]},
                "progress": PROGRESS,
            }
        ),
        "mature": object_schema({"matured_statement": OPTIONAL_TEXT}),
        "rebaseline": object_schema({"progress": PROGRESS}),
        "reschedule": object_schema({"rescheduled": BOOLEAN}),
        "evidence": object_schema(
            {
                "digest": TEXT,
                "statement_base64": TEXT,
                "proof_base64": OPTIONAL_TEXT,
                "job_base64": TEXT,
                "proof_revisions": {
                    "type": "array",
                    "items": object_schema(
                        {
                            "revision": COUNT,
                            "proof_digest": TEXT,
                            "recorded_at": COUNT,
                        }
                    ),
                },
                "due": OPTIONAL_COUNT,
            }
        ),
        "verify": object_schema(
            {
                "verification": _VERIFICATION,
                "confirmation_policy_met": BOOLEAN,
            }
        ),
    },
)


def _calendars(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--calendar",
        action="append",
        required=True,
        help="Exact HTTPS calendar base URL; repeat to use independent calendars.",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="a-riverhog-opentimestamps-witness")
    parser.add_argument(
        "--version",
        action="version",
        version=importlib.metadata.version("a-riverhog-opentimestamps-witness"),
    )
    parser.add_argument("--state", type=Path, required=True, help="Application-owned SQLite file.")
    commands = parser.add_subparsers(dest="command", required=True)
    state = commands.add_parser("state", help="Inspect or explicitly upgrade state.")
    state_commands = state.add_subparsers(dest="state_command", required=True)
    for name in ("status", "upgrade", "verify"):
        state_commands.add_parser(name)
    ingest = commands.add_parser(
        "ingest", help="Commit one bounded catalog page and proof intents."
    )
    _calendars(ingest)
    ingest.add_argument("--limit", type=int, default=100)
    mature = commands.add_parser("mature", help="Attempt at most one due calendar exchange.")
    _calendars(mature)
    run = commands.add_parser("run", help="Poll catalog and calendar work continuously.")
    _calendars(run)
    run.add_argument("--poll-seconds", type=int, default=60)
    commands.add_parser("rebaseline", help="Explicitly start a new authorization view generation.")
    reschedule = commands.add_parser(
        "reschedule", help="Reconsider a paused job with new calendars."
    )
    _calendars(reschedule)
    reschedule.add_argument("digest")
    evidence = commands.add_parser("evidence", help="Emit retained statement and proof bytes.")
    evidence.add_argument("digest")
    verify = commands.add_parser("verify", help="Recompute a proof against a trusted mainnet node.")
    verify.add_argument("digest")
    verify.add_argument("--bitcoin-rpc-url", required=True)
    verify.add_argument("--bitcoin-cookie", type=Path, required=True)
    verify.add_argument("--minimum-confirmations", type=int)
    return parser


def _progress(store: WitnessStore) -> dict[str, object]:
    progress = store.progress()
    return {
        "generation": progress.generation,
        "serial": progress.serial,
        "phase": progress.position.phase,
        "source_identity": progress.position.source_identity,
        "authorization_view_identity": progress.position.authorization_view_identity,
        "through_revision": progress.position.through_revision,
        "reset_reason": progress.position.reset_reason,
    }


def _api_client() -> ApiClient:
    token_file = os.environ.get("RIVERHOG_TOKEN_FILE")
    if token_file is None:
        return ApiClient()
    token = Path(token_file).read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError("Riverhog token file is empty")
    return ApiClient(token=token)


def _run_once(
    store: WitnessStore, calendar: proof.Calendar, *, announce_paused: bool = True
) -> dict[str, object]:
    batch_kind: str | None = None
    if store.progress().position.phase == "reset_required":
        if announce_paused:
            print("witness run ingestion paused: explicit rebaseline required", file=sys.stderr)
    else:
        try:
            batch = store.ingest_once(_api_client())
            batch_kind = batch.kind
            observed = len(batch.collections) + len(batch.changes)
            if observed:
                print(f"witness run retained {observed} catalog observations", file=sys.stderr)
            if batch_kind == "reset":
                print("witness run ingestion paused: explicit rebaseline required", file=sys.stderr)
        except (RiverhogError, httpx.HTTPError, OSError, StaleProposal) as exc:
            print(f"witness run ingestion failed: {type(exc).__name__}", file=sys.stderr)
    digest: str | None = None
    try:
        digest = store.mature_once(calendar)
        if digest is not None:
            evidence = store.evidence(digest)
            assert evidence is not None
            revisions = evidence["proof_revisions"]
            assert isinstance(revisions, tuple)
            job = evidence["job"]
            assert isinstance(job, proof.Job)
            retrying = any(work.error is not None and work.due is not None for work in job.work)
            paused = any(work.error is not None and work.due is None for work in job.work)
            print(
                f"witness run calendar job advanced for {digest}; "
                f"{len(revisions)} proof revisions retained"
                + ("; retry scheduled" if retrying else "")
                + ("; calendar work paused" if paused else ""),
                file=sys.stderr,
            )
    except (proof.CalendarError, httpx.HTTPError, OSError, StaleProposal) as exc:
        print(f"witness run maturation failed: {type(exc).__name__}", file=sys.stderr)
    return {"catalog_batch": batch_kind, "matured_statement": digest, "progress": _progress(store)}


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "state":
            schema = state_schema(args.state)
            result: dict[str, object] = {
                "status": schema.status().as_dict()
                if args.state_command == "status"
                else (
                    schema.upgrade().as_dict()
                    if args.state_command == "upgrade"
                    else schema.validate().as_dict()
                )
            }
        else:
            calendars = tuple(getattr(args, "calendar", []) or ())
            store = WitnessStore(args.state, calendars)
            if args.command == "ingest":
                batch = store.ingest_once(_api_client(), limit=args.limit)
                result = {"catalog_batch": batch.kind, "progress": _progress(store)}
            elif args.command == "mature":
                result = {"matured_statement": store.mature_once(HttpCalendar())}
            elif args.command == "run":
                if not 1 <= args.poll_seconds <= 3600:
                    raise ValueError("poll interval must be between 1 and 3600 seconds")
                calendar = HttpCalendar()
                paused_announced = False
                while True:
                    _run_once(store, calendar, announce_paused=not paused_announced)
                    paused_announced = store.progress().position.phase == "reset_required"
                    time.sleep(args.poll_seconds)
            elif args.command == "rebaseline":
                store.rebaseline()
                result = {"progress": _progress(store)}
            elif args.command == "reschedule":
                result = {"rescheduled": store.reschedule(args.digest)}
            elif args.command == "verify":
                bitcoin = proof.BitcoinCore(BitcoinRpc(args.bitcoin_rpc_url, args.bitcoin_cookie))
                verification, accepted = store.verify(
                    args.digest, bitcoin, minimum_confirmations=args.minimum_confirmations
                )
                result = {"verification": asdict(verification), "confirmation_policy_met": accepted}
            else:
                evidence = store.evidence(args.digest)
                if evidence is None:
                    raise ValueError("statement digest is unknown")
                raw_proof = evidence["proof"]
                job = evidence["job"]
                statement = evidence["statement"]
                assert isinstance(statement, bytes)
                assert isinstance(job, proof.Job)
                result = {
                    "digest": args.digest,
                    "statement_base64": base64.b64encode(statement).decode("ascii"),
                    "proof_base64": (
                        base64.b64encode(raw_proof).decode("ascii")
                        if isinstance(raw_proof, bytes)
                        else None
                    ),
                    "job_base64": base64.b64encode(proof.dump_job(job)).decode("ascii"),
                    "proof_revisions": evidence["proof_revisions"],
                    "due": evidence["due"],
                }
        print(json.dumps(result, sort_keys=True))
        return 0
    except (
        StateSchemaError,
        RiverhogError,
        proof.CalendarError,
        proof.BitcoinUnavailable,
        proof.ProofError,
        proof.StateError,
        StaleProposal,
        ValueError,
        OSError,
        RuntimeError,
    ) as exc:
        print(f"witness command failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
