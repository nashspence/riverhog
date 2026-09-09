from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/implementation_policy.py"
ARTIFACT = REPO_ROOT / "qualification/policies/implementation-witnesses.json"


def load_script() -> ModuleType:
    if str(SCRIPT.parent) not in sys.path:
        sys.path.insert(0, str(SCRIPT.parent))
    spec = importlib.util.spec_from_file_location("riverhog_implementation_policy", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_checked_implementation_policy_is_scoped_and_executable() -> None:
    module = load_script()

    rendered = module._render()

    assert ARTIFACT.read_text(encoding="utf-8") == rendered
    projection = json.loads(rendered)
    assert projection["schema"] == "riverhog-implementation-policy-witnesses/v1"
    assert projection["authority"] == "nonnormative-implementation-correctness"
    assert projection["coverage"] == {
        "classifications": {
            "external_contract": 1,
            "internal_correctness": 3,
            "operational_policy": 1,
        },
        "policies": 5,
        "test_links": 31,
    }
    policies = projection["policies"]
    assert {policy["id"] for policy in policies} == {
        "bounded-obligation-progression/v1",
        "exact-external-effect-authority/v1",
        "exact-read-authority-lifetime/v1",
        "external-result-acceptance-and-cleanup-custody/v1",
        "structured-authority-dependency-liveness/v1",
    }
    assert all(policy["scopes"] for policy in policies)
    assert all(policy["applies_when"] for policy in policies)
    assert all(policy["test_node_ids"] for policy in policies)
    assert all(policy["gates"] for policy in policies)

    contract = json.loads(
        (REPO_ROOT / "qualification/contracts/riverhog-v1.json").read_text(encoding="utf-8")
    )
    trace = json.loads(
        (REPO_ROOT / "qualification/contracts/riverhog-v1-trace.json").read_text(encoding="utf-8")
    )
    assert "authority_policies" not in contract
    assert "authority_policies" not in trace
