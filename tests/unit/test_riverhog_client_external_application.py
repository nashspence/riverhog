from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests/fixtures/external_riverhog_client_application.py"


def test_generic_client_supports_an_external_non_stove0_processing_application() -> None:
    completed = subprocess.run(
        [sys.executable, "-I", str(FIXTURE)],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
