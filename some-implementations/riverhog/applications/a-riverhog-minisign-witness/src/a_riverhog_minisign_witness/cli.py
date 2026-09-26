"""Operator entry point for the independent Minisign collection witness."""

from __future__ import annotations

import argparse
import base64
import importlib.metadata
import json
import os
import sys
import time
from pathlib import Path

import httpx
from a_riverhog_witness_contract_lib._cli_contract import (
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

from a_riverhog_minisign_witness.minisign import MinisignSigner
from a_riverhog_minisign_witness.schema import state_schema
from a_riverhog_minisign_witness.store import Signer, SignerError, StaleProposal, WitnessStore

_CLI_RESULT_CONTRACT = result_contract(
    "a-riverhog-minisign-witness",
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
        "sign": object_schema({"signed_statement": OPTIONAL_TEXT}),
        "rebaseline": object_schema({"progress": PROGRESS}),
        "evidence": object_schema(
            {
                "digest": TEXT,
                "state": {"enum": ["pending", "signed", "blocked"]},
                "statement_base64": TEXT,
                "signature_base64": OPTIONAL_TEXT,
                "key_identity": OPTIONAL_TEXT,
                "error": OPTIONAL_TEXT,
                "attempts": COUNT,
                "due": OPTIONAL_COUNT,
            }
        ),
    },
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="a-riverhog-minisign-witness")
    parser.add_argument(
        "--version",
        action="version",
        version=importlib.metadata.version("a-riverhog-minisign-witness"),
    )
    parser.add_argument("--state", type=Path, required=True, help="Application-owned SQLite file.")
    commands = parser.add_subparsers(dest="command", required=True)
    state = commands.add_parser("state", help="Inspect or explicitly upgrade state.")
    state_commands = state.add_subparsers(dest="state_command", required=True)
    for name in ("status", "upgrade", "verify"):
        state_commands.add_parser(name)
    ingest = commands.add_parser("ingest", help="Commit one bounded catalog page and sign intents.")
    ingest.add_argument("--limit", type=int, default=100)
    sign = commands.add_parser("sign", help="Attempt one due signing job.")
    sign.add_argument("--secret-key", type=Path, required=True)
    sign.add_argument("--public-key", type=Path, required=True)
    run = commands.add_parser("run", help="Poll catalog and signing work continuously.")
    run.add_argument("--secret-key", type=Path, required=True)
    run.add_argument("--public-key", type=Path, required=True)
    run.add_argument("--poll-seconds", type=int, default=60)
    commands.add_parser("rebaseline", help="Explicitly start a new authorization view generation.")
    evidence = commands.add_parser("evidence", help="Emit retained statement and signature bytes.")
    evidence.add_argument("digest")
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
    store: WitnessStore, signer: Signer, *, announce_paused: bool = True
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
        digest = store.sign_once(signer)
        if digest is not None:
            evidence = store.evidence(digest)
            assert evidence is not None
            state = evidence["state"]
            if state == "signed":
                print(f"witness run retained signature for {digest}", file=sys.stderr)
            elif state == "blocked":
                print(f"witness run signing blocked for {digest}", file=sys.stderr)
            else:
                print(f"witness run signing retry scheduled for {digest}", file=sys.stderr)
    except (OSError, StaleProposal) as exc:
        print(f"witness run signing failed: {type(exc).__name__}", file=sys.stderr)
    return {"catalog_batch": batch_kind, "signed_statement": digest, "progress": _progress(store)}


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
            store = WitnessStore(args.state)
            if args.command == "ingest":
                batch = store.ingest_once(_api_client(), limit=args.limit)
                result = {"catalog_batch": batch.kind, "progress": _progress(store)}
            elif args.command == "sign":
                signer = MinisignSigner(args.secret_key, args.public_key)
                result = {"signed_statement": store.sign_once(signer)}
            elif args.command == "run":
                if not 1 <= args.poll_seconds <= 3600:
                    raise ValueError("poll interval must be between 1 and 3600 seconds")
                signer = MinisignSigner(args.secret_key, args.public_key)
                paused_announced = False
                while True:
                    _run_once(store, signer, announce_paused=not paused_announced)
                    paused_announced = store.progress().position.phase == "reset_required"
                    time.sleep(args.poll_seconds)
            elif args.command == "rebaseline":
                store.rebaseline()
                result = {"progress": _progress(store)}
            else:
                evidence = store.evidence(args.digest)
                if evidence is None:
                    raise ValueError("statement digest is unknown")
                signature = evidence["signature"]
                statement = evidence["statement"]
                assert isinstance(statement, bytes)
                result = {
                    "digest": args.digest,
                    "state": evidence["state"],
                    "statement_base64": base64.b64encode(statement).decode("ascii"),
                    "signature_base64": (
                        base64.b64encode(signature).decode("ascii")
                        if isinstance(signature, bytes)
                        else None
                    ),
                    "key_identity": evidence["key_identity"],
                    "error": evidence["error"],
                    "attempts": evidence["attempts"],
                    "due": evidence["due"],
                }
        print(json.dumps(result, sort_keys=True))
        return 0
    except (
        StateSchemaError,
        RiverhogError,
        SignerError,
        StaleProposal,
        ValueError,
        OSError,
        RuntimeError,
    ) as exc:
        print(f"witness command failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
