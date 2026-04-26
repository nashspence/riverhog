from __future__ import annotations

import ast
from pathlib import Path

FEATURES_DIR = Path(__file__).resolve().parents[1] / "acceptance" / "features"
ACCEPTANCE_SUITE = FEATURES_DIR.parent / "test_bdd_acceptance.py"
INTEGRATION_SUITE = FEATURES_DIR.parents[0].parent / "integration" / "test_bdd_spec_harness.py"


def _feature_names_on_disk() -> set[str]:
    return {path.name for path in FEATURES_DIR.glob("*.feature")}


def _scenario_feature_names(test_module: Path) -> set[str]:
    tree = ast.parse(test_module.read_text(encoding="utf-8"), filename=str(test_module))
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Name) or node.func.id != "scenarios":
            continue
        for arg in node.args:
            if not isinstance(arg, ast.Constant) or not isinstance(arg.value, str):
                continue
            names.add(Path(arg.value).name)
    return names


def test_acceptance_bdd_suite_loads_every_feature_file() -> None:
    assert _scenario_feature_names(ACCEPTANCE_SUITE) == _feature_names_on_disk()


def test_integration_bdd_suite_loads_every_feature_file() -> None:
    assert _scenario_feature_names(INTEGRATION_SUITE) == _feature_names_on_disk()
