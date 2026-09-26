#!/usr/bin/env python3
"""Exercise one built runtime image through its supported Compose deployment."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON_IMAGE = (
    "python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203"
)
COMPOSE_DIRS = {
    "a-riverhog-minisign-witness": "applications/a-riverhog-minisign-witness",
    "a-riverhog-opentimestamps-witness": "applications/a-riverhog-opentimestamps-witness",
    "a-riverhog-event-relay": "applications/a-riverhog-event-relay",
    "a-riverhog-aws-store": "storage/aws",
    "a-riverhog-b2-store": "storage/backblaze",
    "a-riverhog-filesystem-store": "storage/filesystem",
}


def run(*command: str, env: dict[str, str] | None = None) -> str:
    completed = subprocess.run(
        command, env=env, check=False, capture_output=True, text=True, timeout=180
    )
    if completed.returncode:
        raise RuntimeError(
            f"command failed ({completed.returncode}): {' '.join(command)}\n"
            f"{completed.stdout[-2000:]}\n{completed.stderr[-2000:]}"
        )
    return completed.stdout.strip()


def write(path: Path, value: str, *, mode: int = 0o644) -> Path:
    path.write_text(value, encoding="utf-8")
    path.chmod(mode)
    return path


def configure(target: str, scratch: Path, env: dict[str, str]) -> None:
    token = write(scratch / "token", "compose-smoke-token\n")
    if "witness" in target:
        prefix = target.upper().replace("-", "_")
        env[f"{prefix}_RIVERHOG_BASE_URL"] = "http://storage-peer:8081"
        env[f"{prefix}_RIVERHOG_TOKEN_FILE"] = str(token)
        env["RIVERHOG_ALLOW_INSECURE_HTTP"] = "true"
        env[f"{prefix}_POLL_SECONDS"] = "3600"
        if "minisign" in target:
            scratch.chmod(0o777)
            run(
                "docker",
                "run",
                "--rm",
                "--user",
                "65532:65532",
                "--entrypoint",
                "minisign",
                "--volume",
                f"{scratch}:/keys",
                f"{target}:dev",
                "-G",
                "-W",
                "-s",
                "/keys/secret.key",
                "-p",
                "/keys/public.key",
            )
            env[f"{prefix}_SECRET_KEY_FILE"] = str(scratch / "secret.key")
            env[f"{prefix}_PUBLIC_KEY_FILE"] = str(scratch / "public.key")
        else:
            env[f"{prefix}_CALENDAR_URL_1"] = "https://calendar1.example.invalid"
            env[f"{prefix}_CALENDAR_URL_2"] = "https://calendar2.example.invalid"
        return
    if target == "a-riverhog-event-relay":
        env.update(
            A_RIVERHOG_EVENT_RELAY_CONFIG_HOST_PATH=str(
                write(
                    scratch / "relay.yaml",
                    "state_path: /state/event-relay.sqlite3\n"
                    "poll_interval_seconds: 3600\nsources:\n"
                    "  - name: smoke\n"
                    "    events_url: http://storage-peer:8081/events\n"
                    "    token_file: /run/secrets/event-source-token\n"
                    "    webhook_url_file: /run/secrets/event-webhook-url\n",
                )
            ),
            A_RIVERHOG_EVENT_RELAY_SOURCE_TOKEN_FILE=str(token),
            A_RIVERHOG_EVENT_RELAY_WEBHOOK_URL_FILE=str(
                write(scratch / "webhook-url", "http://storage-peer:8081/hooks/smoke\n")
            ),
        )
        return
    prefix = target.upper().replace("-", "_")
    env[f"{prefix}_TOKEN_FILE"] = str(token)
    if target == "a-riverhog-filesystem-store":
        return
    kind = "aws" if target == "a-riverhog-aws-store" else "b2"
    config = write(
        scratch / "store.yaml",
        "bucket: fake-bucket\nregion: us-east-1\n"
        "endpoint_url: http://storage-peer:8081\nforce_path_style: true\n"
        "token_file: /run/secrets/storage-adapter-token\n"
        f"access_key_id_file: /run/secrets/{kind}-access-key-id\n"
        f"secret_access_key_file: /run/secrets/{kind}-secret-access-key\n"
        + ("read_mode: immediate\n" if kind == "aws" else ""),
    )
    env[f"{prefix}_CONFIG_HOST_PATH"] = str(config)
    env[f"{prefix}_ACCESS_KEY_ID_FILE"] = str(write(scratch / "access-key-id", "SMOKEKEY\n"))
    env[f"{prefix}_SECRET_ACCESS_KEY_FILE"] = str(
        write(scratch / "secret-access-key", "smoke-secret-key\n")
    )


def smoke(target: str) -> None:
    identity = uuid.uuid4().hex[:12]
    network = f"riverhog-compose-smoke-{identity}"
    project = f"riverhog-{identity}"
    peer = f"riverhog-storage-peer-{identity}"
    compose_file = ROOT / "some-implementations/riverhog" / COMPOSE_DIRS[target] / "compose.yaml"
    with tempfile.TemporaryDirectory(prefix="riverhog-runtime-compose-") as temporary:
        scratch = Path(temporary)
        peer_root = scratch / "peer"
        (peer_root / "fake-bucket").mkdir(parents=True)
        marker = (
            peer_root
            / "fake-bucket"
            / ".riverhog-storage-incarnation"
            / hashlib.sha256(b"").hexdigest()
        )
        marker.parent.mkdir()
        marker.write_text(f"riverhog-storage-incarnation/v1\n{uuid.uuid4()}\n", encoding="ascii")
        env = dict(os.environ, RIVERHOG_CONTROL_NETWORK=network, SOURCE_REVISION="compose-smoke")
        configure(target, scratch, env)
        compose = ["docker", "compose", "--project-name", project, "--file", str(compose_file)]
        run("docker", "network", "create", network)
        try:
            run(
                "docker",
                "run",
                "--detach",
                "--name",
                peer,
                "--network",
                network,
                "--network-alias",
                "storage-peer",
                "--volume",
                f"{peer_root}:/srv:ro",
                "--volume",
                f"{ROOT / 'tests/harness/fake_s3_readiness.py'}:/server.py:ro",
                PYTHON_IMAGE,
                "python",
                "/server.py",
            )
            service = (
                "run"
                if target
                in {
                    "a-riverhog-minisign-witness",
                    "a-riverhog-opentimestamps-witness",
                    "a-riverhog-event-relay",
                }
                else "store"
            )
            run(*compose, "up", "--detach", "--wait", "--no-build", service, env=env)
            run(
                *compose,
                "exec",
                "-T",
                service,
                "python",
                "-c",
                "import socket; "
                "socket.create_connection(('storage-peer', 8081), timeout=5).close()",
                env=env,
            )
            if "witness" in target:
                module = target.replace("-", "_")
                assert (
                    run(
                        *compose,
                        "exec",
                        "-T",
                        service,
                        "python",
                        "-c",
                        f"from {module}.cli import _api_client; "
                        "assert _api_client().token == 'compose-smoke-token'; print('ok')",
                        env=env,
                    )
                    == "ok"
                )
                command = ["--state", "/state/witness.sqlite3", "state", "status"]
            elif target == "a-riverhog-event-relay":
                command = [
                    "--config",
                    "/etc/riverhog/event-relay.yaml",
                    "state",
                    "status",
                    "--json",
                ]
            else:
                command = []
            if command:
                status = json.loads(
                    run(*compose, "run", "--rm", "--no-deps", "state", *command, env=env)
                )
                assert status.get("status", status)["condition"] == "current"
            if target == "a-riverhog-filesystem-store":
                marker_before = run(
                    *compose,
                    "exec",
                    "-T",
                    service,
                    "sha256sum",
                    "/var/lib/riverhog-filesystem/.riverhog-incarnation",
                    env=env,
                )
            run(*compose, "restart", service, env=env)
            run(*compose, "up", "--detach", "--wait", "--no-build", service, env=env)
            if command:
                status = json.loads(
                    run(*compose, "run", "--rm", "--no-deps", "state", *command, env=env)
                )
                assert status.get("status", status)["condition"] == "current"
            if target == "a-riverhog-filesystem-store":
                assert marker_before == run(
                    *compose,
                    "exec",
                    "-T",
                    service,
                    "sha256sum",
                    "/var/lib/riverhog-filesystem/.riverhog-incarnation",
                    env=env,
                )
        except Exception as exc:
            logs = subprocess.run(
                [*compose, "logs", "--no-color", "--tail", "80"],
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )
            raise RuntimeError(f"{target} Compose smoke failed: {logs.stdout[-4000:]}") from exc
        finally:
            subprocess.run(
                [*compose, "down", "--volumes", "--remove-orphans"],
                env=env,
                check=False,
                capture_output=True,
            )
            subprocess.run(["docker", "rm", "-f", peer], check=False, capture_output=True)
            subprocess.run(["docker", "network", "rm", network], check=False, capture_output=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=sorted(COMPOSE_DIRS))
    smoke(parser.parse_args().target)
    print("runtime Compose smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
