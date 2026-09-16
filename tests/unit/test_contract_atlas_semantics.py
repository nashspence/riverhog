from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from typing import Any, cast

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import contract_atlas as atlas  # noqa: E402
from contract_atlas import discovery as atlas_discovery  # noqa: E402

ARTIFACT = REPO_ROOT / "qualification/contracts/riverhog-v1.json"
_CHECKED_ATLAS: atlas.ContractAtlas | None = None


@pytest.fixture(scope="module", autouse=True)
def _bind_checked_atlas(checked_contract_closure: dict[str, Any]) -> None:
    global _CHECKED_ATLAS
    _CHECKED_ATLAS = cast(atlas.ContractAtlas, checked_contract_closure["atlas"])


def checked_atlas() -> atlas.ContractAtlas:
    """Return the session-validated immutable closure for presentation assertions."""

    assert _CHECKED_ATLAS is not None
    return _CHECKED_ATLAS


def test_contract_atlas_is_an_internal_one_way_package() -> None:
    checked = checked_atlas()
    package = REPO_ROOT / "scripts/contract_atlas"
    allowed_internal_imports = {
        "model": set(),
        "discovery": {"model"},
        "navigation": {"model"},
        "relationships": {"discovery", "model", "navigation"},
        "dossier_rendering": {"discovery", "model", "navigation"},
        "rendering": {
            "discovery",
            "dossier_rendering",
            "model",
            "navigation",
            "relationships",
        },
        "validation": {
            "discovery",
            "dossier_rendering",
            "model",
            "navigation",
            "relationships",
            "rendering",
        },
        "__init__": {
            "discovery",
            "dossier_rendering",
            "model",
            "navigation",
            "relationships",
            "rendering",
            "validation",
        },
    }
    assert {path.stem for path in package.glob("*.py")} == set(allowed_internal_imports)
    for module, allowed in allowed_internal_imports.items():
        parsed = ast.parse((package / f"{module}.py").read_text(encoding="utf-8"))
        observed = {
            str(node.module).split(".", 1)[0]
            for node in ast.walk(parsed)
            if isinstance(node, ast.ImportFrom) and node.level == 1
        }
        assert observed <= allowed

    assert not (REPO_ROOT / "scripts/contract_atlas.py").exists()
    assert all(
        "contract-atlas" not in path.read_text(encoding="utf-8")
        for path in REPO_ROOT.rglob("pyproject.toml")
    )
    components = checked.root["projection"]["boundaries"]["components"]
    assert all(not str(component["path"]).startswith("scripts/") for component in components)
    publication = checked.root["projection"]["external_contract"]["release"]["publication"]
    assert "contract-atlas" not in json.dumps(publication, sort_keys=True)


def test_semantic_json_identity_does_not_distinguish_integral_float_spelling() -> None:
    integer = {"limit": 100}
    floating = {"limit": 100.0}

    assert atlas.normalized_json(integer) == atlas.normalized_json(floating)
    assert atlas.canonical_sha256(atlas.normalized_json(integer)) == atlas.canonical_sha256(
        atlas.normalized_json(floating)
    )


def test_machine_closure_round_trips_unsafe_integers_without_sharding() -> None:
    encoded, paths = atlas._encoded_json({"large": 2**63 - 1, "nested": [{"value": -(2**63)}]})

    assert paths == ["/large", "/nested/0/value"]
    assert atlas._decode_unsafe_integers(encoded, paths) == {
        "large": 2**63 - 1,
        "nested": [{"value": -(2**63)}],
    }


def test_checked_atlas_rejects_a_stale_unreferenced_document(tmp_path: Path) -> None:
    checked = checked_atlas()
    root = tmp_path / ARTIFACT.name
    root.write_bytes(ARTIFACT.read_bytes())
    for relative, payload in checked.files.items():
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
    stale = tmp_path / "riverhog-v1/stale.md"
    stale.write_text("stale\n", encoding="utf-8")

    with pytest.raises(atlas.ContractAtlasError, match="stale or unreferenced"):
        atlas.load_atlas(root)


def test_checked_atlas_rejects_document_identity_or_path_escape() -> None:
    checked = checked_atlas()
    changed = json.loads(json.dumps(checked.root))
    changed["atlas"]["documents"][0]["bytes"] += 1
    with pytest.raises(atlas.ContractAtlasError, match="document identity mismatch"):
        atlas.validate_atlas(atlas.ContractAtlas(root=changed, files=checked.files))

    escaped = json.loads(json.dumps(checked.root))
    escaped["atlas"]["documents"][0]["path"] = "../outside.md"
    with pytest.raises(atlas.ContractAtlasError, match="unsafe"):
        atlas.validate_atlas(atlas.ContractAtlas(root=escaped, files=checked.files))


def test_every_machine_terminal_and_extent_decision_has_one_human_owner() -> None:
    checked = checked_atlas()
    discovery = checked.root["discovery"]

    assert discovery["anomalies"] == {
        "duplicate": 0,
        "missing": 0,
        "multiply_disposed": 0,
        "multiply_represented": 0,
        "stale": 0,
        "undecided": 0,
    }
    coverage = discovery["projection_coverage"]
    assert coverage["projection_terminals"] > coverage["semantic_terminals"]
    assert coverage["policy_terminals"] == 3
    assert coverage["extent_decisions"] == checked.root["counts"]["extent_decisions"]
    assert coverage["missing"] == 0
    assert coverage["multiply_represented"] == 0
    assert coverage["stale"] == 0


def test_every_frozen_component_has_an_exact_boundary_audit_result() -> None:
    checked = checked_atlas()
    projection = checked.root["projection"]
    relationships = checked.root["atlas"]["relationships"]
    elements = checked.root["elements"]
    components = {item["distribution"]: item for item in projection["boundaries"]["components"]}
    component_nodes = {
        item["name"]: item for item in relationships["nodes"] if item["kind"] == "component"
    }

    assert set(component_nodes) == set(components)
    for name, node in component_nodes.items():
        assert node["role"] == components[name]["role"]
        assert node["contract_elements"] == sum(item["authority"] == name for item in elements)


def test_semantic_identity_excludes_boundary_governance() -> None:
    checked = checked_atlas()
    projection = json.loads(json.dumps(checked.root["projection"]))
    policies = checked.root["policies"]
    unsafe_paths = checked.root["projection_unsafe_integer_paths"]
    before = atlas._semantic_identity(projection, policies, unsafe_paths)
    projection["boundaries"]["components"][0]["role"] = "changed-only-for-test"

    assert atlas._semantic_identity(projection, policies, unsafe_paths) == before


def test_authority_registry_rejects_an_undeclared_synthetic_owner() -> None:
    checked = checked_atlas()
    elements = json.loads(json.dumps(checked.root["elements"]))
    elements[0]["authority"] = "nearest-looking-bucket"

    with pytest.raises(atlas.ContractAtlasError, match="lack legitimate authorities"):
        atlas._validate_authority_registry(
            elements,
            checked.root["projection"],
            checked.root["trace"],
        )


def test_exact_protocol_and_python_unit_validation_fails_closed_on_drift() -> None:
    checked = checked_atlas()
    changed_elements = json.loads(json.dumps(checked.root["elements"]))
    process_operation = next(
        item for item in changed_elements if item["interface"] == "process-protocol-operations"
    )
    process_operation["interface"] = "process-protocol-schemas"
    with pytest.raises(atlas.ContractAtlasError, match="operation lacks exact ownership"):
        atlas_discovery._validate_process_protocol_units(
            changed_elements, checked.root["projection"]
        )

    changed_trace = json.loads(json.dumps(checked.root["trace"]))
    protected = next(
        item
        for item in changed_trace["python_registry"]["dispositions"]
        if item["disposition"] == "protected"
    )
    changed_trace["python_registry"]["dispositions"].remove(protected)
    with pytest.raises(atlas.ContractAtlasError, match="does not protect every exact public unit"):
        atlas_discovery._validate_python_units(
            checked.root["elements"], checked.root["projection"], changed_trace
        )


def test_policy_registry_is_contract_focused_and_application_counted() -> None:
    checked = checked_atlas()
    policies = checked.root["policies"]
    elements = checked.root["elements"]
    declared = {item["id"] for category in policies.values() for item in category}
    applied = {policy for item in elements for policy in item["policy_ids"]}

    assert applied <= declared
    assert "external-contract-fact/v1" not in declared
    assert all("implementation-witness" not in identity for identity in declared)
    assert checked.root["counts"]["by_policy"]
    assert applied == declared
    policy_page = checked.files["riverhog-v1/policies/index.md"].decode()
    assert "Applications:" in policy_page
    assert policy_page.count("- Applicability:") == len(declared)
    assert policy_page.count("- Observable result or violation:") == len(declared)
    assert policy_page.count("- Executable authorities:") == len(declared)
    assert "Implementation-correctness witnesses remain outside" in policy_page
