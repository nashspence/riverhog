from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def _rsync_command() -> list[str]:
    rsync = shutil.which("rsync")
    if rsync is None:
        raise RuntimeError("rsync is required for resumable file transfers")
    return [
        rsync,
        "--archive",
        "--delete-delay",
        "--partial",
        "--inplace",
        "--numeric-ids",
    ]


def sync_tree(source: Path, destination: Path) -> None:
    if not source.exists() or not source.is_dir():
        raise RuntimeError(f"source directory does not exist: {source}")
    destination.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [*_rsync_command(), f"{source}/", f"{destination}/"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout).strip() or "rsync directory sync failed"
        raise RuntimeError(message)


def sync_file(source: Path, destination: Path) -> None:
    if not source.exists() or not source.is_file():
        raise RuntimeError(f"source file does not exist: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_name(f".{destination.name}.rsync")
    result = subprocess.run(
        [*_rsync_command(), str(source), str(temp)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout).strip() or "rsync file sync failed"
        raise RuntimeError(message)
    temp.replace(destination)
