from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from riverhog_canonical_json import require_canonical_json

from scripts.validation_process import run_validation_child

ROOT = Path(__file__).resolve().parents[2]


def test_validation_child_propagates_failure_after_one_execution(tmp_path: Path) -> None:
    marker = tmp_path / "attempts"
    status = run_validation_child(
        [
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(marker)!r}).write_text('one'); "
            "raise SystemExit(9)",
        ],
        cwd=tmp_path,
        env=dict(os.environ),
    )
    assert status == 9
    assert marker.read_text() == "one"


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group cancellation")
@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_cancelled_validation_joins_child_and_descendant_cleanup(
    tmp_path: Path, signum: int
) -> None:
    leaf = tmp_path / "leaf.py"
    leaf.write_text(
        "import signal, time\n"
        "from pathlib import Path\n"
        f"root = Path({str(tmp_path)!r})\n"
        "def stop(*_):\n"
        "    time.sleep(0.1)\n"
        "    (root / 'leaf-stopped').write_text('cleaned')\n"
        "    raise SystemExit(0)\n"
        "signal.signal(signal.SIGINT, stop)\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        "(root / 'ready').write_text('ready')\n"
        "while True: time.sleep(1)\n"
    )
    owner = tmp_path / "owner.py"
    owner.write_text(
        "import signal, subprocess, sys\n"
        "from pathlib import Path\n"
        f"root = Path({str(tmp_path)!r})\n"
        "def stop(*_): raise KeyboardInterrupt\n"
        "signal.signal(signal.SIGINT, stop)\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        f"child = subprocess.Popen([sys.executable, {str(leaf)!r}])\n"
        "try: child.wait()\n"
        "except KeyboardInterrupt:\n"
        "    signal.signal(signal.SIGINT, signal.SIG_IGN)\n"
        "    signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "    child.wait()\n"
        "    (root / 'owner-stopped').write_text('cleaned')\n"
    )
    driver = tmp_path / "driver.py"
    output = tmp_path / "evidence.json"
    driver.write_text(
        "import sys\n"
        "from pathlib import Path\n"
        "from scripts import ci_qualification, ci_timing\n"
        "from scripts.validation_evidence import ValidationWork\n"
        f"work = ValidationWork('cancellation-proof', (sys.executable, {str(owner)!r}), "
        "target_seconds=0.001)\n"
        "ci_qualification.validation_workloads = lambda: (work,)\n"
        f"raise SystemExit(ci_timing.execute_validation(work.name, Path({str(output)!r}), "
        "'cancelled-owner'))\n"
    )
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    for variable in ("MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES", "args"):
        env.pop(variable, None)
    with subprocess.Popen(
        [sys.executable, str(driver)],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    ) as parent:
        deadline = time.monotonic() + 15
        while not (tmp_path / "ready").exists():
            assert parent.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.01)
        # The tiny advisory target has elapsed; execution still needs real cancellation.
        assert parent.poll() is None
        os.kill(parent.pid, signum)
        _, stderr = parent.communicate(timeout=15)
    assert parent.returncode == 128 + signum
    assert (tmp_path / "leaf-stopped").read_text() == "cleaned"
    assert (tmp_path / "owner-stopped").read_text() == "cleaned"
    evidence = require_canonical_json(output.read_bytes())
    assert evidence["state"] == "interrupted"
    assert evidence["exit_status"] == parent.returncode
    assert evidence["elapsed_seconds"] >= 0.1
    assert evidence["finished_at"]
    assert "Advisory speed goal exceeded" in stderr
    assert json.loads(output.read_bytes())["profiling_target"]["report_only"] is True
