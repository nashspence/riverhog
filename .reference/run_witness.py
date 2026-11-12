"""Run maintained owner witnesses and retain their exact separate-scope evidence."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
import sqlite3
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
import pytest  # noqa: E402
import riverhog_core  # noqa: E402

FILES = (
    "test_journal_retirement_registry.py",
    "test_journal_retirement_async_scopes.py",
    "test_journal_retirement_exception_growth.py",
    "test_upload_journal_validation_retirement_catalog_growth.py",
    "test_upload_journal_validation_retirement_growth.py",
    "test_upload_journal_validation_retirement_loop_growth.py",
    "test_physical_workspace_execution_growth.py",
    "test_workspace_construction_retention_growth.py",
)


def main() -> int:
    assert Path(riverhog_core.__file__).resolve().is_relative_to(ROOT)
    subprocess.run([sys.executable, str(ROOT / ".reference/apply.py"), "--verify"], check=True)
    output = ROOT / "reference-results"
    output.mkdir(exist_ok=True)
    counts, samples = Counter(), Counter()
    maxima = defaultdict(dict)
    digest = hashlib.sha256()
    stream = gzip.open(output / "samples.jsonl.gz", "wb")
    started = time.monotonic()

    class Evidence:
        def pytest_runtest_logreport(self, report):
            if report.when != "call":
                if report.failed:
                    counts[report.when + "_errors"] += 1
                if report.skipped:
                    counts["skipped"] += 1
                return
            counts[report.outcome] += 1
            for name, value in report.user_properties:
                if name not in {"growth_samples", "original_invocations"}:
                    continue
                encoded = (
                    json.dumps(
                        {"test": report.nodeid, "kind": name, "value": value},
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                    + b"\n"
                )
                stream.write(encoded)
                digest.update(encoded)
                if name == "growth_samples":
                    for row in value:
                        declaration = row["declaration"]
                        samples[declaration] += 1
                        for key, metric in row["metrics"].items():
                            maxima[declaration][key] = max(maxima[declaration].get(key, 0), metric)

    try:
        status = int(
            pytest.main(
                ["-q", *[str(ROOT / "tests/unit" / name) for name in FILES]], plugins=[Evidence()]
            )
        )
    finally:
        stream.close()
    summary = {
        "reference_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT)
        .decode()
        .strip(),
        "base_sha": "dd09f4ab5e824935e680f26fba3d73d2484cb426",
        "patch_manifest_sha256": hashlib.sha256(
            (ROOT / ".reference/PATCH.json").read_bytes()
        ).hexdigest(),
        "python": sys.version,
        "platform": platform.platform(),
        "sqlite": sqlite3.sqlite_version,
        "counts": dict(counts),
        "sample_counts": dict(samples),
        "maxima": dict(maxima),
        "sample_stream_sha256": digest.hexdigest(),
        "elapsed_seconds": time.monotonic() - started,
        "pytest_exit": status,
        "scope": "hash-checked reference patch on the exact base; not final product integration",
    }
    (output / "result.json").write_text(json.dumps(summary, sort_keys=True, indent=2) + "\n")
    print(
        json.dumps(
            {key: value for key, value in summary.items() if key != "maxima"}, sort_keys=True
        )
    )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
