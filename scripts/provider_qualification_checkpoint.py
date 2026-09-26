#!/usr/bin/env python3
"""Fail-closed checks for the disposable provider-qualification continuation.

No provider API is used here. Snapshot validation restores the exact pg_dump into
an isolated database, inspects only ownership/lifecycle fields, then drops it.
The pair manifest detects mismatched artifacts; it is not a signature or release
qualification evidence. The existing provider checkpoint loader remains authority.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

APP = "provider-qualification"
KEY_EXPIRY_MARGIN = timedelta(hours=24)
MAX_DUMP_BYTES = 512 * 1024 * 1024
PAIR_FORMAT = "riverhog-provider-qualification-continuation/v1"
TERMINAL_PHASES = frozenset({"cleaned", "failed"})
PRE_RETRIEVAL_PHASES = frozenset(
    {"created", "immediate-qualified", "deep-archive-uploaded", "deep-archive-cache-observed"}
)
RETRIEVAL_PHASES = frozenset({"restore-requested", "restore-pending", "restored", "verified"})


class CheckpointStateError(RuntimeError):
    """Continuation must not be uploaded or resumed."""


def _time(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise CheckpointStateError(f"{label} is missing or invalid")
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
            raise ValueError
    except ValueError as exc:
        raise CheckpointStateError(f"{label} is missing or invalid") from exc
    return parsed


def _row(snapshot: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    row = snapshot.get(name)
    if not isinstance(row, dict):
        raise CheckpointStateError(f"snapshot is missing its {name}")
    return row


def validate_snapshot(
    checkpoint: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    now: datetime,
    pending_timeout_seconds: int,
    monthly_download_quota_bytes: int,
) -> None:
    """Check rows read from the restored dump, never a separate live database.

    A verified checkpoint is cleanup-only: its acknowledged job must be completed,
    not live. All earlier retrieval checkpoints require a live job. Terminal
    checkpoints are evidence-only and must never be packaged as continuations.
    """
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise CheckpointStateError("validation clock must be UTC")
    phase = checkpoint.get("phase")
    if phase not in PRE_RETRIEVAL_PHASES | RETRIEVAL_PHASES:
        raise CheckpointStateError("phase cannot be uploaded as a continuation")
    key_id = checkpoint.get("qualification_key_id")
    job_id = checkpoint.get("retrieval_job_id")
    collection_id = checkpoint.get("collection_id")
    if phase in RETRIEVAL_PHASES:
        if not isinstance(job_id, str) or not job_id:
            raise CheckpointStateError("retrieval checkpoint requires a job identity")
    elif job_id is not None:
        raise CheckpointStateError("pre-retrieval checkpoint unexpectedly names a job")
    if key_id is None:
        if phase != "created" or collection_id is not None or job_id is not None:
            raise CheckpointStateError("checkpoint requires its qualification key identity")
        return
    if not isinstance(key_id, str) or re.fullmatch(r"[0-9a-f]{16}", key_id) is None:
        raise CheckpointStateError("qualification key identity is invalid")
    key = _row(snapshot, "key")
    if key.get("id") != key_id or key.get("app") != APP:
        raise CheckpointStateError("snapshot qualification key owner differs")
    if "revoked_at" not in key or key["revoked_at"] is not None:
        raise CheckpointStateError("snapshot qualification key is revoked or incomplete")
    if "expires_at" not in key:
        raise CheckpointStateError("snapshot qualification key expiry is missing")
    if key.get("has_access") is not True:
        raise CheckpointStateError("snapshot qualification key lost its qualification access")
    if key.get("monthly_download_quota_bytes") != monthly_download_quota_bytes:
        raise CheckpointStateError("snapshot qualification key quota differs")
    deadline = _time(checkpoint.get("restore_deadline_at"), "restore deadline")
    if phase != "verified" and deadline <= now:
        raise CheckpointStateError("restore deadline has elapsed")
    if key["expires_at"] is not None:
        expiry = _time(key["expires_at"], "qualification key expiry")
        if expiry <= now or expiry < deadline + KEY_EXPIRY_MARGIN:
            raise CheckpointStateError(
                "qualification key expiry does not cover deadline plus margin"
            )
    if collection_id is None:
        if phase != "created":
            raise CheckpointStateError("checkpoint requires a collection identity")
    else:
        if type(collection_id) is not int or collection_id <= 0:
            raise CheckpointStateError("collection identity is invalid")
        collection = _row(snapshot, "collection")
        if (
            collection.get("id") != collection_id
            or collection.get("created_by_principal_id") != APP
            or collection.get("created_by_key_id") != key_id
        ):
            raise CheckpointStateError("snapshot collection owner or identity differs")
    if job_id is None:
        return
    job = _row(snapshot, "job")
    plan = _row(snapshot, "plan")
    if (
        job.get("id") != job_id
        or job.get("principal_id") != APP
        or job.get("initiated_by_key_id") != key_id
        or plan.get("principal_id") != APP
        or plan.get("initiated_by_key_id") != key_id
    ):
        raise CheckpointStateError("snapshot retrieval owner or identity differs")
    if (
        not isinstance(plan.get("id"), str)
        or not plan["id"]
        or job.get("plan_id") != plan["id"]
        or plan.get("state") != "consumed"
        or not isinstance(plan.get("etag"), str)
        or re.fullmatch(r"[0-9a-f]{64}", plan["etag"]) is None
        or job.get("plan_etag") != plan["etag"]
        or plan.get("collection_ids") != [collection_id]
    ):
        raise CheckpointStateError("snapshot retrieval plan differs from checkpoint collection")
    state = job.get("state")
    if phase == "verified":
        if state != "completed" or job.get("completed_at") is None:
            raise CheckpointStateError("verified checkpoint requires an acknowledged retrieval")
        return
    allowed = {"ready"} if phase == "restored" else {"requested", "ready"}
    if state not in allowed:
        raise CheckpointStateError("snapshot retrieval is not live for the checkpoint phase")
    if any(name not in job or job[name] is not None for name in ("completed_at", "canceled_at")):
        raise CheckpointStateError("snapshot live retrieval contains terminal lifecycle evidence")
    if state == "ready":
        if _time(job.get("expires_at"), "retrieval lease expiry") <= now:
            raise CheckpointStateError("snapshot retrieval lease has expired")
    else:
        if pending_timeout_seconds <= 0:
            raise CheckpointStateError("pending timeout must be positive")
        requested = _time(job.get("created_at"), "retrieval creation time")
        if requested > now or requested + timedelta(seconds=pending_timeout_seconds) <= now:
            raise CheckpointStateError(
                "snapshot retrieval pending lifetime has elapsed or is invalid"
            )


# psql variables are SQL-quoted by psql; never interpolate checkpoint identities.
# Select an explicit projection: tokens, token hashes, and event context are not read.
SNAPSHOT_SQL = """
BEGIN TRANSACTION READ ONLY;
SELECT json_build_object(
  'key', (SELECT json_build_object(
    'id', k.id, 'app', k.app, 'expires_at', k.expires_at, 'revoked_at', k.revoked_at,
    'monthly_download_quota_bytes', k.monthly_download_quota_bytes,
    'has_access', EXISTS (SELECT 1 FROM app_key_access_grants g
      WHERE g.key_id = k.id AND g.permission = '*' AND g.resource = '*'))
    FROM app_keys k WHERE k.id = :'key_id'),
  'collection', (SELECT json_build_object(
    'id', c.id, 'created_by_principal_id', c.created_by_principal_id,
    'created_by_key_id', c.created_by_key_id)
    FROM collections c WHERE CAST(c.id AS TEXT) = :'collection_id'),
  'job', (SELECT json_build_object(
    'id', j.id, 'principal_id', j.principal_id, 'initiated_by_key_id', j.initiated_by_key_id,
    'plan_id', j.plan_id, 'plan_etag', j.plan_etag, 'state', j.state,
    'created_at', j.created_at, 'expires_at', j.expires_at,
    'completed_at', j.completed_at, 'canceled_at', j.canceled_at)
    FROM retrieval_jobs j WHERE j.id = :'job_id'),
  'plan', (SELECT json_build_object(
    'id', p.id, 'principal_id', p.principal_id, 'initiated_by_key_id', p.initiated_by_key_id,
    'state', p.state, 'etag', p.etag,
    'collection_ids', (SELECT json_agg(DISTINCT f.collection_id ORDER BY f.collection_id)
      FROM retrieval_plan_files f WHERE f.plan_id = p.id))
    FROM retrieval_plans p JOIN retrieval_jobs j ON j.plan_id = p.id
    WHERE j.id = :'job_id')
);
COMMIT;
"""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def pair_manifest(checkpoint: Mapping[str, Any], state_dir: Path) -> dict[str, Any]:
    dump = state_dir / "database.dump"
    if not dump.is_file() or not 0 < dump.stat().st_size <= MAX_DUMP_BYTES:
        raise CheckpointStateError("continuation database dump is empty, missing, or oversized")
    return {
        "format": PAIR_FORMAT,
        "run_id": checkpoint["run_id"],
        "source_sha": checkpoint["source_sha"],
        "checkpoint_sha256": checkpoint["checkpoint_sha256"],
        "checkpoint_file_sha256": _sha256(state_dir / "checkpoint.json"),
        "database_dump_sha256": _sha256(dump),
    }


def verify_pair(checkpoint: Mapping[str, Any], state_dir: Path) -> None:
    if checkpoint.get("phase") not in PRE_RETRIEVAL_PHASES | RETRIEVAL_PHASES:
        raise CheckpointStateError("phase cannot be resumed as a continuation")
    try:
        supplied = json.loads((state_dir / "continuation.json").read_bytes())
    except (OSError, ValueError) as exc:
        raise CheckpointStateError(
            "continuation pairing manifest is missing or invalid; restart"
        ) from exc
    if supplied != pair_manifest(checkpoint, state_dir):
        raise CheckpointStateError(
            "checkpoint and database dump are not the sealed continuation pair"
        )


def _run(command: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(command, check=True, stderr=subprocess.PIPE, **kwargs)
    except subprocess.CalledProcessError as exc:
        # Neither SQL output nor credential-adjacent process diagnostics are public evidence.
        raise CheckpointStateError("disposable database command failed") from exc


def capture_snapshot(
    checkpoint: Mapping[str, Any],
    state_dir: Path,
    compose: Sequence[str],
    *,
    pending_timeout_seconds: int,
    monthly_download_quota_bytes: int,
    run: Callable[..., Any] = _run,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> None:
    """Publish a pair only after successful exact-dump restore, checks and cleanup."""
    manifest_path = state_dir / "continuation.json"
    manifest_path.unlink(missing_ok=True)  # An old seal must not survive a failed new capture.
    if checkpoint["phase"] in TERMINAL_PHASES:
        (state_dir / "database.dump").unlink(missing_ok=True)
        return
    checkpoint_digest = _sha256(state_dir / "checkpoint.json")
    run([*compose, "stop", "app"], stdout=subprocess.DEVNULL)
    postgres = [*compose, "exec", "-T", "postgres"]
    database = f"qualification_verify_{uuid.uuid4().hex}"
    with tempfile.TemporaryDirectory(prefix="qualification-snapshot-", dir=state_dir) as raw:
        temporary_dump = Path(raw) / "database.dump"
        with temporary_dump.open("wb") as output:
            run(
                [
                    *postgres,
                    "pg_dump",
                    "--username",
                    "riverhog",
                    "--dbname",
                    "riverhog",
                    "--format",
                    "custom",
                ],
                stdout=output,
            )
        if not 0 < temporary_dump.stat().st_size <= MAX_DUMP_BYTES:
            raise CheckpointStateError("database snapshot is empty or oversized")
        run(
            [*postgres, "createdb", "--username", "riverhog", database],
            stdout=subprocess.DEVNULL,
        )
        try:
            with temporary_dump.open("rb") as source:
                run(
                    [
                        *postgres,
                        "pg_restore",
                        "--exit-on-error",
                        "--no-owner",
                        "--no-privileges",
                        "--username",
                        "riverhog",
                        "--dbname",
                        database,
                    ],
                    stdin=source,
                    stdout=subprocess.DEVNULL,
                )
            variables = {
                "key_id": checkpoint.get("qualification_key_id"),
                "job_id": checkpoint.get("retrieval_job_id"),
                "collection_id": checkpoint.get("collection_id"),
            }
            result = run(
                [
                    *postgres,
                    "psql",
                    "-X",
                    "--quiet",
                    "--tuples-only",
                    "--no-align",
                    "--set",
                    "ON_ERROR_STOP=1",
                    "--username",
                    "riverhog",
                    "--dbname",
                    database,
                    *(
                        f"--set={name}={value if value is not None else ''}"
                        for name, value in variables.items()
                    ),
                ],
                input=SNAPSHOT_SQL.encode(),
                stdout=subprocess.PIPE,
            )
            try:
                snapshot = json.loads(result.stdout)
            except (ValueError, UnicodeError) as exc:
                raise CheckpointStateError(
                    "snapshot ownership query returned invalid JSON"
                ) from exc
            if not isinstance(snapshot, dict):
                raise CheckpointStateError("snapshot ownership query returned invalid state")
            validate_snapshot(
                checkpoint,
                snapshot,
                now=clock(),
                pending_timeout_seconds=pending_timeout_seconds,
                monthly_download_quota_bytes=monthly_download_quota_bytes,
            )
        finally:
            run(
                [*postgres, "dropdb", "--username", "riverhog", "--if-exists", database],
                stdout=subprocess.DEVNULL,
            )
        if _sha256(state_dir / "checkpoint.json") != checkpoint_digest:
            raise CheckpointStateError("checkpoint changed while its database was being captured")
        os.replace(temporary_dump, state_dir / "database.dump")
        manifest = pair_manifest(checkpoint, state_dir)
        temporary_manifest = Path(raw) / "continuation.json"
        temporary_manifest.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary_manifest, manifest_path)


def revoke_terminal_owner(checkpoint: Mapping[str, Any], client_factory: Callable[[], Any]) -> None:
    """Never revoke on a nonterminal exception or an ordinary pending return."""
    key_id = checkpoint.get("qualification_key_id")
    if checkpoint.get("phase") not in TERMINAL_PHASES or key_id is None:
        return
    client = client_factory()
    try:
        result = client.revoke_app_key(APP, key_id)
        if (
            result.get("id") != key_id
            or result.get("app") != APP
            or result.get("status") != "revoked"
        ):
            raise CheckpointStateError("terminal qualification key revocation was not confirmed")
    finally:
        client.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    verify = commands.add_parser("verify-pair")
    verify.add_argument("state_dir", type=Path)
    capture = commands.add_parser("capture")
    capture.add_argument("state_dir", type=Path)
    capture.add_argument("--runtime-env", required=True)
    capture.add_argument("--project-name", required=True)
    revoke = commands.add_parser("revoke-terminal")
    revoke.add_argument("state_dir", type=Path)
    revoke.add_argument("--base-url", required=True)
    revoke.add_argument("--allow-insecure-http", action="store_true")
    args = parser.parse_args(argv)

    from provider_qualification import (
        QUALIFICATION_MONTHLY_DOWNLOAD_QUOTA_BYTES,
        QUALIFICATION_PENDING_TIMEOUT_SECONDS,
        QualificationError,
        load_checkpoint,
    )

    try:
        checkpoint = load_checkpoint(args.state_dir / "checkpoint.json").as_dict()
        if args.command == "verify-pair":
            verify_pair(checkpoint, args.state_dir)
        elif args.command == "capture":
            compose = [
                "docker",
                "compose",
                "--env-file",
                args.runtime_env,
                "--project-name",
                args.project_name,
                "-f",
                "riverhog/compose.yaml",
                "-f", "tests/harness/provider-qualification.compose.yaml",
            ]
            capture_snapshot(
                checkpoint,
                args.state_dir,
                compose,
                pending_timeout_seconds=QUALIFICATION_PENDING_TIMEOUT_SECONDS,
                monthly_download_quota_bytes=QUALIFICATION_MONTHLY_DOWNLOAD_QUOTA_BYTES,
            )
        else:

            def client_factory() -> Any:
                from riverhog_client.client import ApiClient

                token = os.environ.get("RIVERHOG_QUALIFICATION_BOOTSTRAP_TOKEN", "")
                if not token:
                    raise CheckpointStateError("qualification bootstrap token is missing")
                return ApiClient(
                    base_url=args.base_url,
                    token=token,
                    allow_insecure_http=args.allow_insecure_http,
                )

            revoke_terminal_owner(checkpoint, client_factory)
    except (CheckpointStateError, QualificationError, OSError) as exc:
        parser.exit(2, f"qualification checkpoint: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
