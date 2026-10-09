"""The container scratch proof runs from stdin and must also reclaim killed owners."""

import json
import os
import subprocess
import sys
from pathlib import Path


def test_inline_provenance_probe_reclaims_large_sqlite_and_preserves_live_owner(tmp_path):
    probe = Path(__file__).parents[1] / "harness" / "provenance_workspace_probe.py"
    result = subprocess.run(
        [sys.executable, "-"],
        input=probe.read_text(),
        text=True,
        capture_output=True,
        env={**os.environ, "TMPDIR": str(tmp_path)},
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    receipt = json.loads(result.stdout)
    assert receipt == {
        "proof": "provenance-workspace",
        "sqlite_bytes": receipt["sqlite_bytes"],
        "removed": True,
        "hard_kill_reclaimed": True,
        "live_owner_preserved": True,
    }
    assert receipt["sqlite_bytes"] > 64 * 1024 * 1024
