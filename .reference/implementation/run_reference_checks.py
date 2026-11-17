"""Reproduce this reference suite with the repository's frozen Python environment.

Run from the repository root. This is a reference-only evidence producer, not an
additional product qualification command. Product profiling stays at make profile.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from time import perf_counter


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root))
    from scripts.validation_numeric import bind_numeric_environment, qualification_numeric_policy

    out = root / ".reference/evidence"
    observations = out / "final_restore_observations"
    observations.mkdir(exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix="riverhog-999-final-", dir=root.parent))
    env = dict(os.environ)
    bind_numeric_environment(env, qualification_numeric_policy())
    declared = {
        "TMPDIR": str(scratch),
        "NO_PROXY": "*",
        "no_proxy": "*",
        "PYTHONDONTWRITEBYTECODE": "1",
        "RIVERHOG_CI_TIMING_DIR": str(out / "reference_final_timing"),
        "RIVERHOG_CI_LANE": "reference-999-final",
        "REFERENCE_999_RESTORE_EVIDENCE": str(observations),
        "RIVERHOG_DISCOVERY_REPORT": str(out / "discovery_final_counters.json"),
    }
    env.update(declared)
    inputs = sorted((root / ".reference/implementation").glob("*.py"))
    digests = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs}
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-p",
        "scripts.ci_timing",
        ".reference/implementation",
        "--basetemp=" + str(scratch / "pytest"),
        "--junitxml=" + str(out / "reference_final.xml"),
    ]
    metadata = {
        "command": command,
        "cwd": str(root),
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip(),
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "input_sha256": digests,
        "environment": declared
        | {k: v for k, v in env.items() if k.startswith("RIVERHOG_VALIDATION_NUMERIC_")},
        "scope": (
            "Reference experiments against unchanged c28 product owners; controlled local/"
            "fixture transport only. Loopback-only client configuration. No provider/"
            "power-loss/PostgreSQL/installed-only/full workflow qualification."
        ),
    }
    report = out / "reference_final_command.json"
    report.write_text(json.dumps(metadata, indent=2) + "\n")
    started = perf_counter()
    with (out / "reference_final.txt").open("w") as output:
        result = subprocess.run(command, cwd=root, env=env, stdout=output, stderr=subprocess.STDOUT)
    metadata.update(exit_code=result.returncode, wall_seconds=perf_counter() - started)
    metadata["inputs_unchanged_during_run"] = all(
        hashlib.sha256((root / path).read_bytes()).hexdigest() == digest
        for path, digest in digests.items()
    )
    report.write_text(json.dumps(metadata, indent=2) + "\n")
    print(
        json.dumps(
            {k: metadata[k] for k in ("exit_code", "wall_seconds", "inputs_unchanged_during_run")},
            indent=2,
        )
    )
    print((out / "reference_final.txt").read_text()[-6500:])
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
