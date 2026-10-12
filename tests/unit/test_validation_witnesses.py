from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_bytes

from scripts.validation_witnesses import collect_test_witnesses, discover_test_witnesses

ROOT = Path(__file__).resolve().parents[2]
IDENTITY = "example-payload-visits/v1"
RUN = "actual-witness-execution"


@pytest.fixture(scope="module")
def executed_witness(tmp_path_factory):
    temporary = tmp_path_factory.mktemp("actual-witness")
    root, output = temporary / "source", temporary / "observations"
    tests = root / "tests/unit"
    tests.mkdir(parents=True)
    (root / ".gitignore").write_text("__pycache__/\n.pytest_cache/\n")
    (root / "pytest.ini").write_text(
        "[pytest]\nmarkers =\n    growth_witness(identity, gates): owned test observations\n"
    )
    (tests / "test_example.py").write_text(
        "import pytest\n"
        f"@pytest.mark.growth_witness({IDENTITY!r}, gates=('unit',))\n"
        "def test_actual_visits(record_property):\n"
        "    values = range(17)\n"
        "    visits = sum(1 for _ in values)\n"
        "    assert visits == 17\n"
        "    record_property('growth_observations', {'visits': visits})\n"
    )
    for command in (
        ["git", "init", "-q"],
        ["git", "add", "."],
        [
            "git",
            "-c",
            "user.name=Example",
            "-c",
            "user.email=example@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-qm",
            "Define source witness",
        ],
    ):
        subprocess.run(command, cwd=root, check=True, capture_output=True)
    source_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    observed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "scripts.ci_timing"],
        cwd=root,
        env={
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "RIVERHOG_CI_RUN_ID": RUN,
            "RIVERHOG_CI_LANE": "unit",
            "RIVERHOG_CI_TIMING_DIR": str(output),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert observed.returncode == 0, observed.stdout + observed.stderr
    paths = tuple(output.glob(f"*/{RUN}/*.json"))
    assert len(paths) == 1
    return root, paths, source_sha


def test_actual_pytest_execution_supplies_source_bound_owner_observations(executed_witness):
    root, paths, source = executed_witness
    assert collect_test_witnesses(
        paths, root=root, expected=(IDENTITY,), source_sha=source, run_id=RUN
    ) == {IDENTITY: "pass"}


@pytest.mark.parametrize(
    "defect", ["source", "run", "missing", "stale-owner", "skipped", "expected-failure", "metrics"]
)
def test_invalid_required_owner_artifact_never_becomes_a_growth_pass(
    executed_witness, tmp_path: Path, defect: str
):
    root, paths, source = executed_witness
    record = json.loads(paths[0].read_bytes())
    node = discover_test_witnesses(root)[0].node_id
    if defect == "source":
        record["source_sha"] = "a" * 40
    elif defect == "run":
        record["run_id"] = "different-run"
    elif defect == "missing":
        record["tests"] = {}
    elif defect == "stale-owner":
        record["growth_witnesses"][node]["source_sha256"] = "a" * 64
    elif defect == "skipped":
        record["tests"][node]["call"]["outcome"] = "skipped"
    elif defect == "expected-failure":
        record["tests"][node]["call"]["expected_failure"] = True
    else:
        record["growth_observations"][node] = {"visits": True}
    path = tmp_path / "invalid.json"
    path.write_bytes(canonical_json_bytes(record))
    with pytest.raises(ValueError):
        collect_test_witnesses(
            (path,), root=root, expected=(IDENTITY,), source_sha=source, run_id=RUN
        )


def test_duplicate_actual_owner_execution_is_rejected(executed_witness):
    root, paths, source = executed_witness
    with pytest.raises(ValueError, match="duplicate"):
        collect_test_witnesses(
            (*paths, *paths), root=root, expected=(IDENTITY,), source_sha=source, run_id=RUN
        )
