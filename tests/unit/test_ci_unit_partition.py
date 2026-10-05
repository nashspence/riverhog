from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
from riverhog_canonical_json import require_canonical_json

from scripts import ci_unit

ROOT = Path(__file__).resolve().parents[2]


def test_actual_canonical_collection_has_exhaustive_unique_shard_ownership(tmp_path: Path) -> None:
    inventory = tmp_path / "inventory.json"
    observed = subprocess.run(
        ["make", "unit-shards-check", f"args=--output {inventory}"],
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
