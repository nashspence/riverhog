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
    paths = tuple((output / "pytest").glob(f"*/{RUN}/*/*.json"))
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


@pytest.fixture(scope="module")
def executed_declared_witness(tmp_path_factory):
    temporary = tmp_path_factory.mktemp("declared-witness")
    root, output = temporary / "source", temporary / "observations"
    tests = root / "tests/unit"
    tests.mkdir(parents=True)
    (root / ".gitignore").write_text("__pycache__/\n.pytest_cache/\n")
    (root / "pytest.ini").write_text(
        "[pytest]\nmarkers =\n    growth_witness(identity, gates, operation): owned observations\n"
    )
    (root / "test_witness_fixture_owner.py").write_text("""
from riverhog_growth_contracts import (
    COUNT, Bound, Declaration, Dimension, Expression, declare_growth,
)

def declaration(identity, subject, expression):
    return Declaration(
        id=identity, authority="fixture", subjects=(subject,),
        scope="successful-call-primitive-work", starts_at="accept members", ends_at="return visits",
        dimensions=(Dimension("members", COUNT, "Input members."),),
        bounds=(Bound("visits", COUNT, "equal", expression, "Actually visited members."),),
        conditions=("Finite valid input.",), invariants=("Visit all members.",),
        limitations=("No elapsed-time promise.",),
    )

@declare_growth(declaration(
    "fixture.leaf.growth/v1", "test_witness_fixture_owner.leaf",
    Expression("dimension", name="members"),
))
def leaf(values):
    return sum(1 for value in values)

@declare_growth(declaration(
    "fixture.parent.growth/v1", "test_witness_fixture_owner.parent",
    Expression("delegate", name="leaf", metric="visits",
               terms=(Expression("dimension", name="members"),),
               arguments=(("members", Expression("constant", value=1, unit=COUNT)),)),
), delegates=(("leaf", leaf),))
def parent(values):
    calls = [leaf((value,)) for value in values]
    return sum(calls), calls
""")
    (tests / "test_example.py").write_text("""
import hashlib
import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings
from test_witness_fixture_owner import parent, leaf

@pytest.mark.growth_witness(
    "declared-visits/v1", gates=("unit",), operation="test_witness_fixture_owner.parent",
)
def test_actual_visits(record_property):
    binding = growth_binding(parent)
    child = growth_binding(leaf)
    projection = project_bindings((binding,))
    def sample(owner, members, visits, delegates):
        return {
            "declaration": owner.declaration.id,
            "declaration_sha256": hashlib.sha256(canonical_json_bytes(
                projection["declarations"][owner.declaration.id])).hexdigest(),
            "dimensions": {"members": members}, "metrics": {"visits": visits},
            "delegates": delegates,
        }
    observations = []
    for count in (7, 17):
        visits, calls = parent(range(count))
        assert visits == count
        observations.append(sample(binding, count, visits, {
            "leaf": [sample(child, 1, visits, {}) for visits in calls]
        }))
    record_property("growth_samples", observations)
""")
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
            "Define declared witness",
        ],
    ):
        subprocess.run(command, cwd=root, check=True, capture_output=True)
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    observed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "scripts.ci_timing"],
        cwd=root,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join((str(root), str(ROOT))),
            "RIVERHOG_CI_RUN_ID": RUN,
            "RIVERHOG_CI_LANE": "unit",
            "RIVERHOG_CI_TIMING_DIR": str(output),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert observed.returncode == 0, observed.stdout + observed.stderr
    paths = tuple((output / "pytest").glob(f"*/{RUN}/*/*.json"))
    assert len(paths) == 1
    return root, paths, source


def test_actual_delegated_execution_produces_standalone_normative_evidence(
    executed_declared_witness, monkeypatch
):
    from scripts.validation_witnesses import operation_projection, validate_growth_execution

    root, paths, source = executed_declared_witness
    monkeypatch.syspath_prepend(str(root))
    evidence = []
    assert collect_test_witnesses(
        paths,
        root=root,
        expected=("declared-visits/v1",),
        source_sha=source,
        run_id=RUN,
        evidence=evidence,
    ) == {"declared-visits/v1": "pass"}
    projection, _ = operation_projection("test_witness_fixture_owner.parent", root=root)
    assert len(evidence) == 1
    validate_growth_execution(
        projection, evidence[0], source={"source_sha": source, "source_clean": True}
    )
    assert [len(sample["delegates"]["leaf"]) for sample in evidence[0]["samples"]] == [7, 17]


@pytest.mark.parametrize(
    "defect",
    [
        "binding",
        "missing",
        "bound",
        "delegate",
        "multiplicity",
        "arguments",
        "delegate-bound",
        "delegate-digest",
        "source-end",
    ],
)
def test_required_declared_evidence_rejects_stale_missing_or_contradictory_execution(
    executed_declared_witness, monkeypatch, tmp_path, defect
):
    root, paths, source = executed_declared_witness
    monkeypatch.syspath_prepend(str(root))
    record = json.loads(paths[0].read_bytes())
    node = discover_test_witnesses(root)[0].node_id
    sample = record["growth_samples"][node][0]
    if defect == "binding":
        record["growth_witnesses"][node]["declaration"]["projection_sha256"] = "a" * 64
    elif defect == "missing":
        record["growth_samples"] = {}
    elif defect == "bound":
        sample["metrics"]["visits"] += 1
    elif defect == "delegate":
        sample["delegates"] = {}
    elif defect == "multiplicity":
        sample["delegates"]["leaf"].pop()
    elif defect == "arguments":
        sample["delegates"]["leaf"][0]["dimensions"]["members"] = 2
        sample["delegates"]["leaf"][0]["metrics"]["visits"] = 2
    elif defect == "delegate-bound":
        sample["delegates"]["leaf"][0]["metrics"]["visits"] += 1
    elif defect == "delegate-digest":
        sample["delegates"]["leaf"][0]["declaration_sha256"] = "b" * 64
    else:
        record["source_end"]["source_clean"] = False
    path = tmp_path / "invalid-declared.json"
    path.write_bytes(canonical_json_bytes(record))
    with pytest.raises(ValueError):
        collect_test_witnesses(
            (path,), root=root, expected=("declared-visits/v1",), source_sha=source, run_id=RUN
        )


def test_declaration_outside_required_checkout_cannot_supply_a_source_binding(tmp_path):
    from scripts.validation_witnesses import operation_projection

    with pytest.raises(ValueError, match="outside"):
        operation_projection("riverhog_client.hash_raw_source_chunks", root=tmp_path)
