from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import contract_atlas as atlas  # noqa: E402

ARTIFACT = REPO_ROOT / "qualification/contracts/riverhog-v1.json"


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
    checked = atlas.load_atlas(ARTIFACT)
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
    checked = atlas.load_atlas(ARTIFACT)
    changed = json.loads(json.dumps(checked.root))
    changed["atlas"]["documents"][0]["bytes"] += 1
    with pytest.raises(atlas.ContractAtlasError, match="document identity mismatch"):
        atlas.validate_atlas(atlas.ContractAtlas(root=changed, files=checked.files))

    escaped = json.loads(json.dumps(checked.root))
    escaped["atlas"]["documents"][0]["path"] = "../outside.md"
    with pytest.raises(atlas.ContractAtlasError, match="unsafe"):
        atlas.validate_atlas(atlas.ContractAtlas(root=escaped, files=checked.files))


def test_every_machine_terminal_and_extent_decision_has_one_human_owner() -> None:
    checked = atlas.load_atlas(ARTIFACT)
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
    assert coverage["extent_decisions"] == 1962
    assert coverage["missing"] == 0
    assert coverage["multiply_represented"] == 0
    assert coverage["stale"] == 0


def test_atlas_rollups_and_dossiers_are_exact_and_descriptive() -> None:
    checked = atlas.load_atlas(ARTIFACT)
    root = checked.root
    elements = root["elements"]
    documents = root["atlas"]["documents"]
    dossier_documents = [item for item in documents if item["kind"] == "dossier"]

    assert len(dossier_documents) == len(elements) == root["counts"]["contract_elements"]
    assert root["counts"]["extent_decisions"] == 1962
    assert sum(root["counts"]["by_authority"].values()) == len(elements)
    assert sum(root["counts"]["by_interface"].values()) == len(elements)
    assert all(item["path"].endswith(".md") for item in documents)
    assert all("/contexts/" not in item["path"] for item in documents)
    assert all("/traces/" not in item["path"] for item in documents)
    assert all(not Path(item["path"]).name.startswith("c-") for item in dossier_documents)
    assert checked.files[root["atlas"]["root"]].startswith(b"# Riverhog v1 contract atlas\n")
    assert hashlib.sha256(checked.files[root["atlas"]["root"]]).hexdigest() == next(
        item["sha256"] for item in documents if item["kind"] == "root-index"
    )
    assert max(len(payload) for payload in checked.files.values()) <= atlas.MAX_HUMAN_DOCUMENT_BYTES
    assert all(b"## Complete owned contract" in checked.files[item["dossier"]] for item in elements)


def test_human_entrypoint_exposes_closure_exclusions_and_relationships() -> None:
    checked = atlas.load_atlas(ARTIFACT)
    root = checked.root
    root_page = checked.files[root["atlas"]["root"]].decode()
    exclusions_page = checked.files["riverhog-v1/exclusions/index.md"].decode()
    relationships_page = checked.files["riverhog-v1/relationships/index.md"].decode()
    relationship = root["atlas"]["relationships"]

    assert root_page.index("## Closure status") < root_page.index("## Complete authority inventory")
    assert root_page.index("## Aggregate contract shape") < root_page.index(
        "## Complete authority inventory"
    )
    assert all(f"| `{name}` | 0 |" in root_page for name in root["discovery"]["anomalies"])
    assert "Relationship-aware boundary map" in root_page
    assert exclusions_page.count("| `excluded:") == root["counts"]["excluded_candidates"]
    assert "## Riverhog service boundary" in relationships_page
    assert "Public service | [riverhog]" in relationships_page
    assert "Packaged implementation | [riverhog-server]" in relationships_page
    assert relationship["schema"] == atlas.RELATIONSHIP_SCHEMA
    assert any(item["kind"] == "runtime-image" for item in relationship["nodes"])
    assert any(item["type"] == "implements-protocol" for item in relationship["edges"])


def test_large_interfaces_route_through_semantic_families_and_local_references() -> None:
    checked = atlas.load_atlas(ARTIFACT)
    documents = checked.root["atlas"]["documents"]
    family_documents = [item for item in documents if item["kind"] == "family-index"]

    assert family_documents
    assert all(item["counts"]["contract_elements"] > 0 for item in family_documents)
    retrieval_cache = checked.files[
        "riverhog-v1/authorities/riverhog/http/get-v1-retrieval-cache.md"
    ].decode()
    assert "## Referenced contract dossiers" in retrieval_cache
    assert (
        "[schemas: RetrievalCacheStatusOut](schemas-retrievalcachestatusout.md)" in retrieval_cache
    )


def test_policy_registry_is_contract_focused_and_application_counted() -> None:
    checked = atlas.load_atlas(ARTIFACT)
    policies = checked.root["policies"]
    elements = checked.root["elements"]
    declared = {item["id"] for category in policies.values() for item in category}
    applied = {policy for item in elements for policy in item["policy_ids"]}

    assert applied <= declared
    assert "external-contract-fact/v1" not in declared
    assert all("implementation-witness" not in identity for identity in declared)
    assert checked.root["counts"]["by_policy"]
    assert (
        checked.root["counts"]["by_policy"]["exclusion/process-launcher-not-cli/v1"]
        == checked.root["counts"]["excluded_candidates"]
        == 13
    )
    policy_page = checked.files["riverhog-v1/policies/index.md"].decode()
    assert "Applications:" in policy_page
    assert "Applications: **13**" in policy_page
    assert "Implementation-correctness witnesses remain outside" in policy_page
