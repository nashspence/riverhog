import os
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
import requests
from minio import Minio

REPO_ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        return int(sock.getsockname()[1])


def wait_until(check, description: str, timeout: float = 90.0, interval: float = 1.0):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = check()
        except Exception as exc:  # pragma: no cover - exercised in failure paths
            last_error = exc
        else:
            if value:
                return value
        time.sleep(interval)
    detail = f": {last_error}" if last_error else ""
    raise AssertionError(f"Timed out waiting for {description}{detail}")


@dataclass
class RunningStack:
    project_name: str
    env_file: Path
    runtime_dir: Path
    archive_port: int
    garage_port: int
    bucket: str
    access_key: str
    secret_key: str
    passphrase: str

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.archive_port}"

    @property
    def s3_endpoint(self) -> str:
        return f"127.0.0.1:{self.garage_port}"

    def compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--env-file",
                str(self.env_file),
                "--project-name",
                self.project_name,
                *args,
            ],
            cwd=REPO_ROOT,
            env=os.environ.copy(),
            text=True,
            capture_output=True,
            check=False,
        )
        if check and result.returncode != 0:
            raise AssertionError(
                f"docker compose {' '.join(args)} failed\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )
        return result

    def api(self, method: str, path: str, **kwargs) -> requests.Response:
        timeout = kwargs.pop("timeout", 30)
        return requests.request(method, f"{self.base_url}{path}", timeout=timeout, **kwargs)

    def minio(self) -> Minio:
        return Minio(
            self.s3_endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
            region="garage",
        )

    def logs(self) -> str:
        result = self.compose("logs", "--no-color", check=False)
        return f"{result.stdout}\n{result.stderr}".strip()


@pytest.fixture(scope="session")
def stack(tmp_path_factory: pytest.TempPathFactory):
    runtime_dir = tmp_path_factory.mktemp("archive-stack-e2e")
    for name in (
        "garage-meta",
        "garage-data",
        "archive-state",
        "packages",
        "rehydrated",
    ):
        (runtime_dir / name).mkdir(parents=True, exist_ok=True)

    archive_port = free_port()
    garage_port = free_port()
    bucket = f"archive-e2e-{uuid.uuid4().hex[:8]}"
    passphrase = "archive-stack-test-passphrase"
    env_file = runtime_dir / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"GARAGE_PORT={garage_port}",
                f"ARCHIVE_PORT={archive_port}",
                f"GARAGE_META_DIR={runtime_dir / 'garage-meta'}",
                f"GARAGE_DATA_DIR={runtime_dir / 'garage-data'}",
                f"ARCHIVE_STATE_DIR={runtime_dir / 'archive-state'}",
                f"PACKAGES_DIR={runtime_dir / 'packages'}",
                f"REHYDRATED_DIR={runtime_dir / 'rehydrated'}",
                f"BASE_URL=http://127.0.0.1:{archive_port}",
                f"S3_BUCKET={bucket}",
                "S3_ACCESS_KEY=GK000000000000000000000001",
                "S3_SECRET_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                f"DISC_PASSPHRASE={passphrase}",
                "HOMEASSISTANT_WEBHOOK=",
                "STAGE_THRESHOLD_BYTES=10485760",
                "ISO_TARGET_BYTES=1048576",
                "ISO_RESERVE_BYTES=65536",
                "POLL_SECONDS=1",
                "",
            ]
        ),
        encoding="utf-8",
    )

    running = RunningStack(
        project_name=f"archive-stack-e2e-{uuid.uuid4().hex[:8]}",
        env_file=env_file,
        runtime_dir=runtime_dir,
        archive_port=archive_port,
        garage_port=garage_port,
        bucket=bucket,
        access_key="GK000000000000000000000001",
        secret_key="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        passphrase=passphrase,
    )

    try:
        running.compose("up", "-d", "--build")

        try:
            wait_until(
                lambda: running.api("GET", "/health").ok,
                "archive API health check",
                timeout=120,
            )
            wait_until(
                lambda: running.minio().bucket_exists(bucket),
                "Garage bucket creation",
                timeout=60,
            )
        except Exception as exc:  # pragma: no cover - exercised in failure paths
            raise AssertionError(f"{exc}\n\nCompose logs:\n{running.logs()}") from exc

        yield running
    finally:
        running.compose("down", "-v", "--remove-orphans", check=False)
