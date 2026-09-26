"""Exercise the provider continuation with a disposable real PostgreSQL database."""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4

import pytest

from scripts import provider_qualification as qualification
from scripts import provider_qualification_checkpoint as checkpoint

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RIVERHOG_PROVIDER_CHECKPOINT_HOST_TEST") != "1",
        reason="host-driven Docker Compose continuation test",
    ),
]
ROOT = Path(__file__).resolve().parents[2]


def _compose_command() -> list[str]:
    command = ["docker", "compose", "--file", str(ROOT / "riverhog/compose.yaml")]
    env_file = os.getenv("RIVERHOG_COMPOSE_ENV_FILE")
    if env_file and Path(env_file).is_file():
        command.extend(("--env-file", env_file))
    return command


def _database_action(compose: list[str], action: str) -> None:
    subprocess.run(
        [
            *compose,
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "--entrypoint",
            "python",
            "--env",
            "RIVERHOG_TEST_POSTGRES_URL",
            "test",
            "tests/harness/provider_checkpoint_postgres.py",
            action,
        ],
        cwd=ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
    )


def _restored_checkpoint(state_dir: Path) -> qualification.QualificationCheckpoint:
    config = qualification.load_config(ROOT / "qualification/provider/config.toml")
    corpus = qualification.CorpusManifest(
        profile="regular",
        files=(qualification.CorpusFile(path="sample.bin", bytes=1, sha256="4" * 64),),
        bytes=1,
        sha256="1" * 64,
    )
    values = {
        definition.name_env: f"qualification-{definition.logical_name}"
        for definition in config.buckets
    }
    values.update(
        {
            definition.region_env: "us-west-004" if definition.provider == "b2" else "us-west-2"
            for definition in config.buckets
        }
    )
    current = qualification.new_checkpoint(
        source_sha="a" * 40,
        source_ref="release/v1",
        config=config,
        corpus=corpus,
        buckets=qualification.resolve_buckets(config, values),
        run_id=uuid4().hex,
    )
    current = qualification.bind_qualification_key(current, "a" * 16)
    current = qualification.advance_checkpoint(
        current, phase="immediate-qualified", collection_id=42
    )
    current = qualification.advance_checkpoint(current, phase="deep-archive-uploaded")
    current = qualification.advance_checkpoint(current, phase="deep-archive-cache-observed")
    current = qualification.advance_checkpoint(
        current, phase="restore-requested", retrieval_job_id="job-42"
    )
    current = qualification.advance_checkpoint(current, phase="restored")
    qualification.write_checkpoint(state_dir / "checkpoint.json", current)
    return current


def test_sealed_postgres_continuation_survives_rotation_and_rejects_wrong_owner() -> None:
    compose = _compose_command()
    state_root = ROOT / ".riverhog"
    state_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="provider-checkpoint-postgres-", dir=state_root) as raw:
        state_dir = Path(raw)
        _database_action(compose, "seed")
        restored = _restored_checkpoint(state_dir)

        def capture() -> None:
            checkpoint.capture_snapshot(
                restored.as_dict(),
                state_dir,
                compose,
                pending_timeout_seconds=qualification.QUALIFICATION_PENDING_TIMEOUT_SECONDS,
                monthly_download_quota_bytes=(
                    qualification.QUALIFICATION_MONTHLY_DOWNLOAD_QUOTA_BYTES
                ),
            )

        capture()
        checkpoint.verify_pair(restored.as_dict(), state_dir)

        postgres = [*compose, "exec", "-T", "postgres"]
        subprocess.run(
            [*postgres, "dropdb", "--username", "riverhog", "--if-exists", "--force", "riverhog"],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        subprocess.run(
            [*postgres, "createdb", "--username", "riverhog", "riverhog"],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        with (state_dir / "database.dump").open("rb") as source:
            subprocess.run(
                [
                    *postgres,
                    "pg_restore",
                    "--exit-on-error",
                    "--no-owner",
                    "--no-privileges",
                    "--username",
                    "riverhog",
                    "--dbname",
                    "riverhog",
                ],
                check=True,
                stdin=source,
                stdout=subprocess.DEVNULL,
            )

        resumed = qualification.load_checkpoint(state_dir / "checkpoint.json")
        checkpoint.verify_pair(resumed.as_dict(), state_dir)
        assert resumed.phase == "restored"
        _database_action(compose, "rotate")
        capture()
        checkpoint.verify_pair(resumed.as_dict(), state_dir)

        _database_action(compose, "poison-owner")
        with pytest.raises(checkpoint.CheckpointStateError, match="owner or identity differs"):
            capture()
        with pytest.raises(checkpoint.CheckpointStateError, match="pairing manifest"):
            checkpoint.verify_pair(resumed.as_dict(), state_dir)
        _database_action(compose, "repair-owner")
        capture()
        checkpoint.verify_pair(resumed.as_dict(), state_dir)

        terminal = qualification.advance_checkpoint(resumed, phase="failed")
        qualification.write_checkpoint(state_dir / "checkpoint.json", terminal)

        class DatabaseKeyClient:
            closed = False

            def revoke_app_key(self, app: str, key_id: str) -> dict[str, str]:
                assert (app, key_id) == (checkpoint.APP, "a" * 16)
                _database_action(compose, "revoke")
                return {"app": app, "id": key_id, "status": "revoked"}

            def close(self) -> None:
                self.closed = True

        client = DatabaseKeyClient()
        checkpoint.revoke_terminal_owner(terminal.as_dict(), lambda: client)
        assert client.closed
        _database_action(compose, "assert-revoked")
        checkpoint.capture_snapshot(
            terminal.as_dict(),
            state_dir,
            compose,
            pending_timeout_seconds=qualification.QUALIFICATION_PENDING_TIMEOUT_SECONDS,
            monthly_download_quota_bytes=qualification.QUALIFICATION_MONTHLY_DOWNLOAD_QUOTA_BYTES,
        )
        assert not (state_dir / "continuation.json").exists()
        assert not (state_dir / "database.dump").exists()
