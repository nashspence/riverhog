from __future__ import annotations

import ast
import copy
import sys
from pathlib import Path
from typing import Any, cast

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import contract_atlas as contract  # noqa: E402
from contract_atlas import discovery  # noqa: E402
from contract_atlas.model import ContractAtlasError  # noqa: E402


def test_contract_builder_has_only_current_record_and_html_dependencies() -> None:
    package = REPO_ROOT / "scripts/contract_atlas"
    allowed_imports = {
        "model": set(),
        "discovery": {"model"},
        "records": {"model"},
        "human_contract": {"model"},
        "html_rendering": {"human_contract", "model", "records"},
        "__init__": {"discovery", "model"},
    }
    assert {path.stem for path in package.glob("*.py")} == set(allowed_imports)
    for name, allowed in allowed_imports.items():
        parsed = ast.parse((package / f"{name}.py").read_text(encoding="utf-8"))
        imported = {
            str(node.module).split(".", 1)[0]
            for node in ast.walk(parsed)
            if isinstance(node, ast.ImportFrom) and node.level == 1
        }
        assert imported <= allowed


def test_canonical_identity_and_exact_integer_encoding() -> None:
    assert contract.normalized_json({"limit": 100}) == contract.normalized_json({"limit": 100.0})
    encoded, paths = contract._encoded_json({"large": 2**63 - 1, "nested": [{"value": -(2**63)}]})
    assert paths == ["/large", "/nested/0/value"]
    assert contract._decode_unsafe_integers(encoded, paths) == {
        "large": 2**63 - 1,
        "nested": [{"value": -(2**63)}],
    }


def test_discovery_covers_every_owned_source_subject_without_anomalies(
    checked_contract_closure: dict[str, Any],
) -> None:
    root = checked_contract_closure["discovered"].root
    closure = checked_contract_closure["bundle"].closure
    accounting = root["discovery"]
    assert accounting["anomalies"] == {
        "duplicate": 0,
        "missing": 0,
        "multiply_disposed": 0,
        "multiply_represented": 0,
        "stale": 0,
        "undecided": 0,
    }
    coverage = accounting["projection_coverage"]
    assert coverage["missing"] == coverage["stale"] == coverage["multiply_represented"] == 0
    assert coverage["extent_decisions"] == len(
        root["projection"]["external_contract"]["extents"]["decisions"]
    )
    assert len(root["elements"]) == len(closure["elements"])
    assert not any(
        pointer.startswith("/external_contract/extents/decisions/")
        for element in root["elements"]
        for pointer in element["pointers"]
    )
    assert all("dossier" not in element for element in root["elements"])


def test_discovery_rejects_an_undeclared_owner_and_changed_exact_units(
    checked_contract_closure: dict[str, Any],
) -> None:
    root = checked_contract_closure["discovered"].root
    elements = copy.deepcopy(root["elements"])
    elements[0]["authority"] = "nearest-looking-bucket"
    with pytest.raises(ContractAtlasError, match="lack legitimate authorities"):
        discovery._validate_authority_registry(elements, root["projection"], root["trace"])

    elements = copy.deepcopy(root["elements"])
    operation = next(
        item for item in elements if item["interface"] == "process-protocol-operations"
    )
    operation["interface"] = "process-protocol-schemas"
    with pytest.raises(ContractAtlasError, match="operation lacks exact ownership"):
        discovery._validate_process_protocol_units(elements, root["projection"])

    trace = copy.deepcopy(root["trace"])
    protected = next(
        item
        for item in trace["python_registry"]["dispositions"]
        if item["disposition"] == "protected"
    )
    trace["python_registry"]["dispositions"].remove(protected)
    with pytest.raises(ContractAtlasError, match="does not protect every exact public unit"):
        discovery._validate_python_units(root["elements"], root["projection"], trace)


def test_policy_definitions_resolve_to_closure_and_applications_stay_audit_only(
    checked_contract_closure: dict[str, Any],
) -> None:
    bundle = checked_contract_closure["bundle"]
    policies = cast(dict[str, list[dict[str, object]]], bundle.audit["policies"])
    closure = bundle.closure
    declared = {item["id"] for records in policies.values() for item in records}
    overlays = cast(list[dict[str, object]], bundle.audit["element_overlays"])
    applied = {identity for item in overlays for identity in cast(list[str], item["policy_ids"])}
    assert applied <= declared
    assert "policy_ids" not in closure["elements"][0]
    for records in policies.values():
        for policy in records:
            pointer = str(policy["definition_pointer"])
            assert contract.pointer_value(closure, pointer) == policy["meaning"]


def test_normative_extent_rules_are_explicit_and_analysis_rules_have_no_contract_element(
    checked_contract_closure: dict[str, Any],
) -> None:
    bundle = checked_contract_closure["bundle"]
    extents = bundle.closure["external_contract"]["extents"]
    assert set(extents["rules"]) == {
        "schema-bound/v1",
        "bounded-segment/v1",
        "route-progression/v1",
        "extension-contract/v1",
    }
    assert set(bundle.audit["extent_analysis"]["analysis_rules"]) == {
        "no-semantic-maximum/v1",
        "configuration-composition/v1",
        "configured-capacity/v1",
    }
    assert all(
        all(
            not pointer.startswith("/external_contract/extents/rules/no-semantic-maximum")
            for pointer in item["pointers"]
        )
        for item in bundle.closure["elements"]
    )
