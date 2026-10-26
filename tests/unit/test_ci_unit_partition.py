from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json

from scripts import ci_unit

ROOT = Path(__file__).resolve().parents[2]


def test_actual_canonical_collection_has_exhaustive_unique_shard_ownership(tmp_path: Path) -> None:
    inventory = tmp_path / "inventory.json"
    observed = subprocess.run(
        ["make", "unit-shards-check", "TESTS=focused-selection", f"args=--output {inventory}"],
        cwd=ROOT,
        env={**os.environ, "MISE_TRUSTED_CONFIG_PATHS": str(ROOT)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert observed.returncode == 0, observed.stdout + observed.stderr
    require_canonical_json(inventory.read_bytes())
    record = json.loads(inventory.read_bytes())
    assert set(record["shards"]) == set(ci_unit.SHARDS)
    nodes = [node for shard in record["shards"].values() for node in shard]
    assert len(nodes) == len(set(nodes))
    assert all(record["shards"].values())
    assert any("test_riverhog_age_c2sp_vectors.py" in node for node in nodes)
    assert any("some-implementations/" in node for node in nodes)
    assert any("test_ci_unit_partition.py" in node for node in nodes)
    assert record["format"] == "riverhog-ci-unit-inventory/v2"
    parts = record["unit_partitions"]
    assert set(parts) == set(ci_unit.UNIT_PARTITIONS)
    physical = [node for values in parts.values() for node in values]
    assert len(physical) == len(set(physical))
    assert set(physical) == set(record["shards"]["unit"])
    module_owners = {}
    for part, values in parts.items():
        assert values
        for node in values:
            module = node.partition("::")[0]
            assert module_owners.setdefault(module, part) == part
    assert set(ci_unit.measured_modules()) <= module_owners.keys()
    from scripts.ci_qualification import validation_workloads
    from scripts.validation_witnesses import discover_test_witnesses

    definitions = {
        item.name: item
        for item in validation_workloads(
            names=("unit-unit", *(f"unit-unit-{part}" for part in ci_unit.UNIT_PARTITIONS))
        )
    }
    expected = set(definitions["unit-unit"].growth_witnesses)
    assigned = [
        identity
        for part in ci_unit.UNIT_PARTITIONS
        for identity in definitions[f"unit-unit-{part}"].growth_witnesses
    ]
    assert len(assigned) == len(set(assigned)) and set(assigned) == expected
    for witness in discover_test_witnesses(ROOT):
        if witness.id in expected:
            part = module_owners[witness.node_id.partition("::")[0]]
            assert witness.node_id in {node.partition("[")[0] for node in parts[part]}
            assert witness.id in definitions[f"unit-unit-{part}"].growth_witnesses
    assert record["contract_fixture_modules"]
    assert not {node.partition("::")[0] for node in record["shards"]["unit"]} & set(
        record["contract_fixture_modules"]
    )


@pytest.mark.parametrize("owners", [(), ("release", "unit"), ("unknown",)])
def test_partition_rejects_missing_duplicate_or_unknown_ownership(
    monkeypatch: pytest.MonkeyPatch, owners: tuple[str, ...]
) -> None:
    monkeypatch.setattr(ci_unit, "owners", lambda _: owners)
    with pytest.raises(ValueError, match="exactly one shard owner"):
        ci_unit.partition(["tests/unit/test_example.py::test_example"])


def test_partition_rejects_duplicate_canonical_collection() -> None:
    node = "tests/unit/test_example.py::test_example"
    with pytest.raises(ValueError, match="duplicate canonical"):
        ci_unit.partition([node, node])


def test_contract_fixture_consumers_keep_their_whole_module_together() -> None:
    module = "tests/unit/test_example.py"
    release = "tests/unit/test_release_example.py"
    nodes = [f"{module}::test_one", f"{module}::test_two", f"{release}::test_release"]
    observed = ci_unit.partition(nodes, contract_modules=frozenset({module, release}))
    assert observed["contract"] == nodes[:2]
    assert observed["release"] == nodes[2:]
    assert observed["unit"] == []


@pytest.mark.parametrize(
    "expected, repeat_collection",
    [
        (["test_one.py::test_fact"], False),
        ([], False),
        (["test_one.py::test_other"], False),
        (["test_one.py::test_fact", "test_one.py::test_fact"], False),
        (["test_one.py::test_fact"], True),
    ],
)
def test_executing_workers_must_collect_their_exact_canonical_shard(
    tmp_path: Path, expected: list[str], repeat_collection: bool
) -> None:
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    (tmp_path / "test_one.py").write_text("def test_fact():\n    assert True\n")
    if repeat_collection:
        (tmp_path / "conftest.py").write_text(
            "def pytest_collection_modifyitems(items):\n    items.extend(items.copy())\n"
        )
    membership = tmp_path / "expected.json"
    membership.write_bytes(canonical_json_bytes({"nodeids": expected}))
    observed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-n", "2", "-p", "scripts.ci_unit"],
        cwd=tmp_path,
        env={
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "RIVERHOG_CI_UNIT_EXPECTED_FILE": str(membership),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    if expected == ["test_one.py::test_fact"] and not repeat_collection:
        assert observed.returncode == 0, observed.stdout + observed.stderr
    else:
        assert observed.returncode != 0
        assert "shard collection differs" in observed.stdout + observed.stderr


@pytest.mark.parametrize("mode", ["--collect-only", "--setup-only"])
def test_a_collected_shard_must_actually_execute_before_claiming_success(
    tmp_path: Path, mode: str
) -> None:
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    (tmp_path / "test_one.py").write_text("def test_fact():\n    assert True\n")
    membership = tmp_path / "expected.json"
    membership.write_bytes(canonical_json_bytes({"nodeids": ["test_one.py::test_fact"]}))
    observed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-n", "2", "-p", "scripts.ci_unit", mode],
        cwd=tmp_path,
        env={
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "RIVERHOG_CI_UNIT_EXPECTED_FILE": str(membership),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert observed.returncode != 0
    assert "shard execution differs" in observed.stdout + observed.stderr


@pytest.mark.parametrize("fail_first", [False, True])
def test_real_shard_propagates_failure_and_requires_every_test_for_success(tmp_path, fail_first):
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    tests = tmp_path / "tests" / "unit"
    tests.mkdir(parents=True)
    (tests / "test_release_example.py").write_text("def test_release():\n    assert True\n")
    (tests / "test_contract_example.py").write_text("def test_contract():\n    assert True\n")
    marker = tmp_path / "second-executed"
    (tests / "test_example.py").write_text(
        "from pathlib import Path\n"
        "def test_first():\n"
        f"    assert {not fail_first!r}\n"
        "def test_second():\n"
        f"    Path({str(marker)!r}).write_text('executed')\n"
    )
    environment = {
        key: value for key, value in os.environ.items() if key != "RIVERHOG_CI_UNIT_EXPECTED_FILE"
    }
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/ci_unit.py"),
            "run",
            "--shard",
            "unit",
            "tests/unit",
        ],
        cwd=tmp_path,
        env={**environment, "PYTHONPATH": str(ROOT)},
        text=True,
        capture_output=True,
        check=False,
    )
    if fail_first:
        assert result.returncode != 0
        # Other workers may finish tests already running when the failure arrives.
        assert "test_first" in result.stdout + result.stderr
        assert "1 failed" in result.stdout + result.stderr
    else:
        assert result.returncode == 0, result.stdout + result.stderr
        assert marker.read_text() == "executed"
        assert "2 passed" in result.stdout
        assert "[gw" in result.stdout
        assert "tests/unit/test_example.py::test_first" in result.stdout
        assert "tests/unit/test_example.py::test_second" in result.stdout


def test_measured_module_placement_balances_work_and_is_order_independent():
    costs = {
        f"tests/unit/test_{index}.py": seconds
        for index, seconds in enumerate((160, 150, 140, 130, 70, 60, 50, 40))
    }
    placement = ci_unit.weighted_modules(costs)
    reverse = ci_unit.weighted_modules(dict(reversed(tuple(costs.items()))))
    assert placement == reverse
    loads = {part: 0 for part in ci_unit.UNIT_PARTITIONS}
    for module, part in placement.items():
        loads[part] += costs[module]
    assert set(loads.values()) == {200}


@pytest.mark.parametrize("seconds", [0, -1, True, float("inf"), float("nan"), "1"])
def test_invalid_module_costs_fail_closed(seconds):
    with pytest.raises(ValueError, match="scheduling costs"):
        ci_unit.weighted_modules({"tests/unit/test_example.py": seconds})


@pytest.mark.parametrize(
    "module", ["/outside/test_example.py", "../test_example.py", "fixture.txt"]
)
def test_module_costs_require_relative_test_modules(module):
    with pytest.raises(ValueError, match="scheduling costs"):
        ci_unit.weighted_modules({module: 1})


def test_unmeasured_modules_keep_all_nodes_together_without_collection_dependence():
    module = "tests/unit/test_unmeasured_fixture.py"
    nodes = [module + "::test_one", module + "::test_two[parameter]"]
    assert ci_unit.unit_partition(nodes[0]) == ci_unit.unit_partition(nodes[1])
    before = ci_unit.partitioned_units(nodes)
    after = ci_unit.partitioned_units(
        ["tests/unit/test_new_fixture.py::test_new", *reversed(nodes)]
    )
    owner = ci_unit.unit_partition(nodes[0])
    assert set(before[owner]) <= set(after[owner])
    assert owner in ci_unit.UNIT_PARTITIONS


@pytest.fixture(scope="module")
def partition_source(tmp_path_factory):
    root = tmp_path_factory.mktemp("unit-partition-source")
    source, output = root / "source", root / "timing"
    tests = source / "tests/unit"
    tests.mkdir(parents=True)
    (source / ".gitignore").write_text("__pycache__/\n.pytest_cache/\n")
    (source / "pytest.ini").write_text(
        "[pytest]\nmarkers =\n    growth_witness(identity, gates): source observations\n"
    )
    (tests / "test_release_example.py").write_text("def test_release():\n    assert True\n")
    (tests / "test_contract_example.py").write_text("def test_contract():\n    assert True\n")
    chosen = {}
    for index in range(100):
        module = f"tests/unit/test_partition_fixture_{index}.py"
        part = ci_unit.unit_partition(module)
        if part in chosen:
            continue
        chosen[part] = module
        (source / module).write_text(
            "import pytest\n"
            f"@pytest.mark.growth_witness('partition-{part}', gates=('unit-unit',))\n"
            "def test_actual_witness(record_property):\n"
            "    assert sum(1 for _ in range(17)) == 17\n"
            "    record_property('growth_observations', {'visits': 17})\n"
            "def test_tail():\n"
            "    assert True\n"
        )
        if len(chosen) == len(ci_unit.UNIT_PARTITIONS):
            break
    assert set(chosen) == set(ci_unit.UNIT_PARTITIONS)
    (source / "Makefile").write_text(
        "SHELL := bash\n"
        "VALIDATION_TARGETS := unit-shard\n"
        "ifneq ($(RIVERHOG_VALIDATION_MAKE_DIRECTORY),)\n"
        "SHELL := $(RIVERHOG_VALIDATION_MAKE_SHELL)\n"
        ".SHELLFLAGS := -c\n"
        "export RIVERHOG_VALIDATION_RECIPE_TARGET = $@\n"
        "endif\n"
        "# Maintained target recipes.\n"
        ".PHONY: unit-shard\n"
        "unit-shard:\n"
        f"\t{shlex.quote(sys.executable)} {shlex.quote(str(ROOT / 'scripts/ci_unit.py'))} "
        'run --shard "$(UNIT_SHARD)" --partition "$(UNIT_PARTITION)" '
        "tests/unit -- -p scripts.ci_timing\n"
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
            "Define independent unit fixtures",
        ],
    ):
        subprocess.run(command, cwd=source, check=True, capture_output=True)
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    return source, output, chosen, sha


@pytest.mark.parametrize("part", ci_unit.UNIT_PARTITIONS)
def test_real_partition_executes_only_its_exact_nodes_and_supplies_growth_evidence(
    partition_source, part
):
    from scripts.validation_witnesses import collect_test_witnesses

    source, output, modules, sha = partition_source
    run = "partition-" + part
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"RIVERHOG_CI_UNIT_EXPECTED_FILE", "UNIT_PARTITION"}
    }
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/ci_unit.py"),
            "run",
            "--shard",
            "unit",
            "--partition",
            part,
            "tests/unit",
            "--",
            "-p",
            "scripts.ci_timing",
        ],
        cwd=source,
        env={
            **environment,
            "PYTHONPATH": str(ROOT),
            "RIVERHOG_CI_TIMING_DIR": str(output),
            "RIVERHOG_CI_RUN_ID": run,
        },
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "2 passed" in result.stdout and modules[part] + "::test_actual_witness" in result.stdout
    for other, module in modules.items():
        if other != part:
            assert module + "::test_actual_witness" not in result.stdout
    paths = tuple((output / "pytest").glob(f"*/{run}/*/*.json"))
    assert collect_test_witnesses(
        paths, root=source, expected=(run,), source_sha=sha, run_id=run
    ) == {run: "pass"}


def test_real_partition_rejects_a_deselected_required_test(partition_source):
    source, _, modules, _ = partition_source
    part = ci_unit.UNIT_PARTITIONS[0]
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"RIVERHOG_CI_UNIT_EXPECTED_FILE", "UNIT_PARTITION"}
    }
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/ci_unit.py"),
            "run",
            "--shard",
            "unit",
            "--partition",
            part,
            "tests/unit",
            "--",
            "--deselect",
            modules[part] + "::test_actual_witness",
        ],
        cwd=source,
        env={**environment, "PYTHONPATH": str(ROOT)},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
    assert "shard collection differs from canonical ownership" in result.stdout + result.stderr


@pytest.mark.parametrize("shard", ["release", "contract"])
def test_general_partition_is_rejected_for_another_family(tmp_path, shard):
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/ci_unit.py"),
            "run",
            "--shard",
            shard,
            "--partition",
            "1",
            "tests/unit",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
    assert "a unit partition requires run --shard unit" in result.stderr


def test_actual_make_partition_preserves_execution_and_parameter_evidence(
    partition_source, monkeypatch
):
    from scripts import ci_make

    source, output, _, sha = partition_source
    part = ci_unit.UNIT_PARTITIONS[0]
    run = "make-partition"
    monkeypatch.chdir(source)
    monkeypatch.setenv("UNIT_SHARD", "unit")
    monkeypatch.setenv("UNIT_PARTITION", part)
    monkeypatch.setenv("PYTHONPATH", str(ROOT))
    monkeypatch.setenv("RIVERHOG_CI_TIMING_DIR", str(output))
    monkeypatch.setenv("RIVERHOG_CI_RUN_ID", run)
    monkeypatch.delenv("RIVERHOG_CI_UNIT_EXPECTED_FILE", raising=False)
    monkeypatch.delenv("MAKEFLAGS", raising=False)
    monkeypatch.delenv("MFLAGS", raising=False)
    assert ci_make.observe_make(source / "Makefile", ("unit-shard",)) == 0
    receipt = require_canonical_json(next(output.glob(f"make/{run}/*/manifest.json")).read_bytes())
    assert receipt["state"] == "completed" and receipt["exit_status"] == 0
    assert receipt["source_sha"] == sha
    assert receipt["parameters"] == {"UNIT_SHARD": "unit", "UNIT_PARTITION": part}
    assert receipt["work_definitions"] == ci_make.make_work_definitions(
        ("unit-shard",), receipt["parameters"]
    )
    assert receipt["work_definitions"][0]["name"] == "unit-unit-" + part
    rows = [
        require_canonical_json(path.read_bytes())
        for path in output.glob(f"pytest/unit-unit-{part}/{run}/*/*.json")
    ]
    workers = [row for row in rows if row.get("worker") != "controller"]
    assert sum(len(row["tests"]) for row in workers) == 2
