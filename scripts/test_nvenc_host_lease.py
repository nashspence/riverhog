#!/usr/bin/env python3
"""Qualify the maintained NVENC image against a separate host lock participant."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import yaml
from a_stove0_media_archive_contract_lib import AV1_OPUS_ARCHIVE_OPERATION
from riverhog_canonical_json import canonical_json_bytes
from stove0_target_protocol import TargetDescriptor, TargetPreflightResponse

from tests.fixtures.stove0_media import sealed_media_job

ROOT = Path(__file__).resolve().parents[1]
TARGET = "a-stove0-nvenc-av1-opus-target"
PREFIX = "A_STOVE0_NVENC_AV1_OPUS_TARGET"
OVERLAY = ROOT / "some-implementations/stove0/targets/nvenc-av1-opus/compose.host-lease.yaml"


def run(command: list[str], *, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=90)
    if result.returncode:
        descriptor, name = tempfile.mkstemp(prefix="riverhog-nvenc-lease-failure-", suffix=".log")
        with os.fdopen(descriptor, "w") as log:
            log.write(result.stdout + result.stderr)
        raise RuntimeError(
            f"NVENC lease fixture command failed with status {result.returncode}; "
            f"diagnostics: {name}"
        )
    return result.stdout.strip()


def await_condition(predicate: Callable[[], bool], *, seconds: float = 15) -> None:
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("NVENC host/container lease witness stopped making progress")
        time.sleep(0.02)


def hold_lock(path: Path, ready: Path) -> None:
    import fcntl

    with path.open("r+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ready.write_text("locked\n")
        sys.stdin.buffer.read(1)


def holder(path: Path, ready: Path) -> subprocess.Popen[bytes]:
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "scripts.test_nvenc_host_lease",
            "--hold-lock",
            str(path),
            str(ready),
        ],
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        await_condition(lambda: ready.exists() or process.poll() is not None)
        assert ready.exists() and process.poll() is None
        return process
    except BaseException:
        process.terminate()
        process.wait(timeout=10)
        raise


def release(process: subprocess.Popen[bytes]) -> None:
    if process.stdin is not None:
        process.stdin.close()
    assert process.wait(timeout=10) == 0


class PublicTarget:
    def __init__(self, client: httpx.Client) -> None:
        self.client = client

    def descriptor(self) -> TargetDescriptor:
        response = self.client.get("/v1/target")
        response.raise_for_status()
        return TargetDescriptor.model_validate(response.json())

    def preflight(self, request: Any) -> TargetPreflightResponse:
        response = self.client.post(
            "/v1/preflight", content=canonical_json_bytes(request.model_dump(mode="json"))
        )
        response.raise_for_status()
        return TargetPreflightResponse.model_validate(response.json())


def qualify(image: str) -> dict[str, Any]:
    inspected = json.loads(run(["docker", "image", "inspect", image]))[0]
    image_id = inspected["Id"]
    revision = run(["git", "rev-parse", "HEAD"])
    assert inspected["Config"]["Labels"]["org.opencontainers.image.revision"] == revision
    project = "riverhog-nvenc-lease-" + uuid4().hex[:12]
    processes: list[subprocess.Popen[bytes]] = []
    with tempfile.TemporaryDirectory(prefix="riverhog-nvenc-host-lease-") as temporary:
        scratch = Path(temporary)
        lease = scratch / "lease"
        lease.mkdir()
        lock = lease / "exclusive.lock"
        lock.touch(mode=0o600)
        independent = scratch / "independent-lease"
        independent.mkdir()
        for name in ["held", "independent"]:
            fixture = scratch / name
            fixture.mkdir()
            (fixture / "token").write_text("fixture-target\n")
        base = yaml.safe_load(
            (ROOT / "some-implementations/stove0/application/compose.yaml").read_text()
        )
        services = {}
        for name, data, lease_volume in [
            (TARGET, "held", "a-stove0-nvenc-av1-opus-execution-lease:/run/stove0-execution-lease"),
            ("independent", "independent", f"{independent}:/run/stove0-execution-lease"),
        ]:
            service = copy.deepcopy(base["services"][TARGET])
            for field in [
                "build",
                "gpus",
                "profiles",
                "secrets",
                "networks",
                "tmpfs",
                "healthcheck",
            ]:
                service.pop(field, None)
            service.update(
                image=image_id,
                user=f"{os.getuid()}:{os.getgid()}",
                command=["python", "/probe.py"],
                ports=["127.0.0.1::8080"],
                volumes=[
                    lease_volume,
                    f"{scratch / data}:/fixture",
                    f"{ROOT / 'tests/harness/nvenc_host_lease_target.py'}:/probe.py:ro",
                ],
                environment={
                    f"{PREFIX}_TOKEN_FILE": "/fixture/token",
                    f"{PREFIX}_IMAGE_ID": image_id,
                    f"{PREFIX}_SOURCE_REVISION": revision,
                    f"{PREFIX}_STATE_ROOT": "/fixture/state",
                    f"{PREFIX}_PRIMARY_STATE": (
                        '{"scope":{"service":"qualification-service",'
                        '"account":"qualification-account"}}'
                    ),
                    f"{PREFIX}_WORKSPACE": "/fixture/workspace",
                    f"{PREFIX}_EXECUTION_LEASE_FILE": "/run/stove0-execution-lease/exclusive.lock",
                },
            )
            services[name] = service
        fixture_file = scratch / "compose.yaml"
        fixture_file.write_text(
            yaml.safe_dump(
                {"services": services, "volumes": {"a-stove0-nvenc-av1-opus-execution-lease": {}}}
            )
        )
        env = dict(
            os.environ,
            **{
                f"{PREFIX}_EXECUTION_LEASE_DIRECTORY": str(lease),
                f"{PREFIX}_EXECUTION_LEASE_GID": str(os.getgid()),
            },
        )
        compose = [
            "docker",
            "compose",
            "--project-name",
            project,
            "--file",
            str(fixture_file),
            "--file",
            str(OVERLAY),
        ]
        clients: list[httpx.Client] = []
        started = time.monotonic()
        try:
            external = holder(lock, scratch / "host-held")
            processes.append(external)
            configured = json.loads(run([*compose, "config", "--format", "json"], env=env))
            shared = next(
                v
                for v in configured["services"][TARGET]["volumes"]
                if v["target"] == "/run/stove0-execution-lease"
            )
            assert shared["type"] == "bind" and Path(shared["source"]) == lease
            assert shared["bind"]["create_host_path"] is False
            run([*compose, "up", "--detach", "--no-build", "--pull", "never"], env=env)
            for name in [TARGET, "independent"]:
                address = run([*compose, "port", name, "8080"], env=env)
                assert address.startswith("127.0.0.1:")
                client = httpx.Client(
                    base_url=f"http://{address}",
                    timeout=5,
                    headers={
                        "Authorization": "Bearer fixture-target",
                        "Content-Type": "application/json",
                    },
                    trust_env=False,
                )
                clients.append(client)

                def ready(client: httpx.Client = client) -> bool:
                    try:
                        return client.get("/health/live").status_code == 200
                    except httpx.TransportError:
                        return False

                await_condition(ready, seconds=30)
            blocked, other = clients
            intent = {
                "codec": "av1",
                "container": "mkv",
                "quality": 23,
                "audio_bitrate_kbps": 128,
                "salvage": "safe-remux",
            }
            request = sealed_media_job(PublicTarget(blocked), AV1_OPUS_ARCHIVE_OPERATION, intent)
            job_id = request.declaration.job_id
            response = blocked.put(
                f"/v1/jobs/{job_id}", content=canonical_json_bytes(request.model_dump(mode="json"))
            )
            response.raise_for_status()
            assert response.json()["state"] == "queued"
            control = scratch / "held/control.json"
            await_condition(control.exists)
            for _ in range(20):
                response = blocked.get(f"/v1/jobs/{job_id}")
                response.raise_for_status()
                assert response.json()["state"] == "queued"
                observed = json.loads(control.read_bytes())
                inode = lock.stat()
                assert (observed["lease_device"], observed["lease_inode"]) == (
                    inode.st_dev,
                    inode.st_ino,
                )
                assert (
                    observed["payload_consumers"]
                    == observed["sessions"]
                    == observed["workspace_entries"]
                    == 0
                )
                assert not (scratch / "held/native-boundary.json").exists()
                time.sleep(0.01)
            accepted = scratch / f"held/state/{job_id}.accepted.json"
            accepted_digest = hashlib.sha256(accepted.read_bytes()).hexdigest()
            # Hard process loss leaves the durable queued identity dormant.
            # Runtime credentials are intentionally not persisted: the controller
            # must refresh the same accepted invocation after component restart.
            run([*compose, "kill", "--signal", "SIGKILL", TARGET], env=env)
            run([*compose, "up", "--detach", "--no-build", "--pull", "never", TARGET], env=env)
            # An ephemeral published host port can change after container start.
            address = run([*compose, "port", TARGET, "8080"], env=env)
            assert address.startswith("127.0.0.1:")
            blocked.base_url = f"http://{address}"
            await_condition(lambda: _queued(blocked, job_id), seconds=30)
            assert hashlib.sha256(accepted.read_bytes()).hexdigest() == accepted_digest
            response = blocked.put(
                f"/v1/jobs/{job_id}", content=canonical_json_bytes(request.model_dump(mode="json"))
            )
            response.raise_for_status()
            assert response.json()["state"] == "queued"
            observed = json.loads(control.read_bytes())
            assert (
                observed["payload_consumers"]
                == observed["sessions"]
                == observed["workspace_entries"]
                == 0
            )
            assert not (scratch / "held/native-boundary.json").exists()
            other_request = sealed_media_job(
                PublicTarget(other), AV1_OPUS_ARCHIVE_OPERATION, {**intent, "quality": 24}
            )
            response = other.put(
                f"/v1/jobs/{other_request.declaration.job_id}",
                content=canonical_json_bytes(other_request.model_dump(mode="json")),
            )
            response.raise_for_status()
            await_condition(lambda: _stopped(other, other_request.declaration.job_id))
            assert json.loads((scratch / "independent/native-boundary.json").read_bytes())[
                "lease_retained"
            ]
            assert _queued(blocked, job_id)
            release(external)
            await_condition(lambda: _stopped(blocked, job_id))
            assert json.loads((scratch / "held/native-boundary.json").read_bytes())[
                "lease_retained"
            ]
            released = holder(lock, scratch / "host-reacquired")
            processes.append(released)
            release(released)
            return {
                "source_sha": revision,
                "image_id": image_id,
                "source_clean": not bool(run(["git", "status", "--porcelain"])),
                "shared_inode_verified": True,
                "queued_payload_consumers": 0,
                "queued_survives_restart": True,
                "independent_work_progresses": True,
                "release_permits_native_boundary": True,
                "host_reacquires_after_containment": True,
                "cleanup_verified": True,
                "native_execution": "guarded; real GPU encode is separate qualification",
                "seconds": round(time.monotonic() - started, 3),
            }
        finally:
            failure = sys.exception()
            if failure is not None:
                descriptor, name = tempfile.mkstemp(
                    prefix="riverhog-nvenc-lease-components-", suffix=".log"
                )
                try:
                    with os.fdopen(descriptor, "wb") as log:
                        logs = subprocess.run(
                            [*compose, "logs", "--no-color", "--tail", "100"],
                            env=env,
                            capture_output=True,
                            timeout=30,
                        )
                        log.write(logs.stdout + logs.stderr)
                    failure.add_note(f"Owned fixture component diagnostics: {name}")
                except Exception as error:
                    failure.add_note(f"Fixture diagnostics unavailable: {type(error).__name__}")
            for client in clients:
                client.close()
            try:
                run([*compose, "down", "--volumes", "--remove-orphans", "--timeout", "10"], env=env)
            finally:
                for process in processes:
                    if process.poll() is None:
                        release(process)
            assert not run(
                [
                    "docker",
                    "ps",
                    "--all",
                    "--quiet",
                    "--filter",
                    f"label=com.docker.compose.project={project}",
                ]
            )
            assert not run(
                [
                    "docker",
                    "volume",
                    "ls",
                    "--quiet",
                    "--filter",
                    f"label=com.docker.compose.project={project}",
                ]
            )
            assert not run(
                [
                    "docker",
                    "network",
                    "ls",
                    "--quiet",
                    "--filter",
                    f"label=com.docker.compose.project={project}",
                ]
            )


def _queued(client: httpx.Client, job_id: str) -> bool:
    try:
        response = client.get(f"/v1/jobs/{job_id}")
        response.raise_for_status()
        return response.json()["state"] == "queued"
    except httpx.TransportError:
        return False


def _stopped(client: httpx.Client, job_id: str) -> bool:
    response = client.get(f"/v1/jobs/{job_id}")
    response.raise_for_status()
    document = response.json()
    if document["state"] in {"failed", "canceled", "succeeded"}:
        raise AssertionError("Guarded execution did not stop at the expected native boundary")
    return document["state"] == "inapplicable"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image", nargs="?", default=f"{TARGET}:dev")
    parser.add_argument("--hold-lock", nargs=2, type=Path)
    args = parser.parse_args()
    if args.hold_lock:
        hold_lock(*args.hold_lock)
        return 0
    document = qualify(args.image)
    output = os.environ.get("RIVERHOG_CI_TIMING_DIR")
    if output:
        directory = Path(output) / "nvenc-host-lease"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "evidence.json").write_bytes(canonical_json_bytes(document))
    print(canonical_json_bytes(document).decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
