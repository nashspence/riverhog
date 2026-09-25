#!/usr/bin/env python3
"""Smoke a built witness image with a disposable, durable SQLite volume."""

from __future__ import annotations

import argparse
import json
import subprocess
import uuid
from typing import cast

TARGETS = {
    "a-riverhog-minisign-witness",
    "a-riverhog-opentimestamps-witness",
}


def _run(*args: str) -> str:
    return subprocess.run(
        args, check=True, capture_output=True, text=True, timeout=120
    ).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=sorted(TARGETS))
    target = parser.parse_args().target
    image = f"{target}:dev"
    volume = "riverhog-witness-smoke-" + uuid.uuid4().hex
    _run("docker", "volume", "create", volume)
    try:
        _run(
            "docker",
            "run",
            "--rm",
            "--network=none",
            "--user=0",
            "--entrypoint=chown",
            "-v",
            f"{volume}:/state",
            image,
            "65532:65532",
            "/state",
        )

        def state(command: str) -> dict[str, object]:
            output = _run(
                "docker",
                "run",
                "--rm",
                "--network=none",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "-v",
                f"{volume}:/state",
                image,
                "--state",
                "/state/witness.sqlite3",
                "state",
                command,
            )
            document = json.loads(output)
            if not isinstance(document, dict) or not isinstance(document.get("status"), dict):
                raise RuntimeError(f"{target} image returned no state status")
            return cast(dict[str, object], document["status"])

        upgraded = state("upgrade")
        verified = state("verify")
        status = state("status")
        if any(item["condition"] != "current" for item in (upgraded, verified, status)):
            raise RuntimeError(f"{target} image did not retain current witness state")
        if not _run("docker", "run", "--rm", "--network=none", image, "--version"):
            raise RuntimeError(f"{target} image has no executable version")
        print(json.dumps({"target": target, "state": "current"}, sort_keys=True))
        return 0
    finally:
        _run("docker", "volume", "rm", "-f", volume)


if __name__ == "__main__":
    raise SystemExit(main())
