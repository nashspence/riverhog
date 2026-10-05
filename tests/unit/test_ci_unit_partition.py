from __future__ import annotations

import json
import os
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
