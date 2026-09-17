from __future__ import annotations

import copy
import posixpath
import re
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import contract_atlas as atlas  # noqa: E402
from contract_atlas import navigation as nav  # noqa: E402
from contract_atlas import reference_rendering as references  # noqa: E402


def test_retained_inventories_have_separate_reachable_reading_pages(
    checked_contract_closure: dict[str, Any],
) -> None:
    checked = checked_contract_closure["atlas"]
    routes = {
        "riverhog-v1/evidence/sources.md": (
            nav.SOURCE_AUTHORITIES_PATH,
            nav.QUALIFICATION_ROUTES_PATH,
            nav.FIXTURES_PATH,
            nav.QUALIFICATIONS_PATH,
        ),
        "riverhog-v1/evidence/configuration.md": (
            nav.CONFIGURATION_SETTINGS_PATH,
            nav.CONFIGURATION_FAMILIES_PATH,
            nav.CONFIGURATION_DOCUMENTS_PATH,
        ),
        "riverhog-v1/evidence/relationships.md": (
            nav.RELATIONSHIP_NODES_PATH,
            nav.RELATIONSHIP_EDGES_PATH,
        ),
    }
    for parent, children in routes.items():
        page = checked.files[parent].decode()
        for child in children:
            assert f"]({nav._relative_link(parent, child)})" in page
            content = checked.files[child].decode()
            assert f"]({nav._relative_link(child, parent)})" in content
            # Each retained inventory is one table, with navigation before its rows.
            assert len(re.findall(r"^\|---", content, re.MULTILINE)) == 1
            assert content.index("Reference navigation") < content.index("\n| ")

    root = checked.root
    inventories = (
        (nav.SOURCE_AUTHORITIES_PATH, len(root["sources"])),
        (nav.FIXTURES_PATH, sum(len(s.get("fixtures", [])) for s in root["sources"])),
        (nav.CONFIGURATION_SETTINGS_PATH, len(root["trace"]["configuration_registry"]["records"])),
        (nav.CONFIGURATION_FAMILIES_PATH, len(root["trace"]["configuration_registry"]["patterns"])),
        (
            nav.CONFIGURATION_DOCUMENTS_PATH,
            len(root["trace"]["configuration_document_registry"]["candidates"]),
        ),
        (nav.RELATIONSHIP_NODES_PATH, len(root["atlas"]["relationships"]["nodes"])),
        (nav.RELATIONSHIP_EDGES_PATH, len(root["atlas"]["relationships"]["edges"])),
    )
    for path, count in inventories:
        rows = [line for line in checked.files[path].decode().splitlines() if line.startswith("| ")]
        assert len(rows) - 1 == count, path


def test_policies_keep_one_definition_and_exact_application_lists(
    checked_contract_closure: dict[str, Any],
) -> None:
    checked = checked_contract_closure["atlas"]
    root = checked.root
    definitions = nav._policy_definition_elements(root["elements"])
    assert len(definitions) == 21
    for category, policies in root["policies"].items():
        category_path = f"riverhog-v1/policies/{category}/index.md"
        category_page = checked.files[category_path].decode()
        for policy in policies:
            identity = policy["id"]
            target, anchor = nav._policy_destination(identity, definitions)
            assert f"]({nav._anchor_link(category_path, target, anchor)})" in category_page
            definition_page = checked.files[target].decode()
            application_path = nav._policy_applications_path(identity)
            assert f"]({nav._relative_link(target, application_path)})" in definition_page
            application_pages = {
                path: content.decode()
                for path, content in checked.files.items()
                if path == application_path
                or path.startswith(application_path.removesuffix(".md") + "/")
            }
            expected = {
                item["dossier"] + "#" + nav._policy_application_anchor(item["id"], identity)
                for item in root["elements"]
                if identity in item["policy_ids"]
            }
            targets = []
            for path, content in application_pages.items():
                rows = [line for line in content.splitlines() if line.startswith("| `")]
                for row in rows:
                    link = re.search(r"\]\(([^)]+)\)", row)[1]
                    targets.append(
                        posixpath.normpath(posixpath.join(posixpath.dirname(path), link))
                    )
            assert len(targets) == len(expected)
            assert set(targets) == expected
            if identity in definitions:
                assert (
                    atlas.pointer_value(root["projection"], definitions[identity]["pointers"][0])
                    == policy["meaning"]
                )
            else:
                assert category == "publication"
                assert policy["meaning"] in definition_page
                assert policy["source_pointer"] in definition_page

    publication = "riverhog-v1/policies/publication/index.md"
    for parent in (root["atlas"]["root"], "riverhog-v1/authorities/release/index.md"):
        assert f"]({nav._relative_link(parent, publication)})" in checked.files[parent].decode()


def test_configuration_comparison_is_available_from_each_owning_interface(
    checked_contract_closure: dict[str, Any],
) -> None:
    checked = checked_contract_closure["atlas"]
    root = checked.root
    for target, records in (
        (nav.CONFIGURATION_SETTINGS_PATH, root["trace"]["configuration_registry"]["records"]),
        (nav.CONFIGURATION_FAMILIES_PATH, root["trace"]["configuration_registry"]["patterns"]),
        (
            nav.CONFIGURATION_DOCUMENTS_PATH,
            root["trace"]["configuration_document_registry"]["candidates"],
        ),
    ):
        for owner in {record["owner"] for record in records}:
            interface = (
                "configuration"
                if target == nav.CONFIGURATION_DOCUMENTS_PATH
                else "configuration-environment"
            )
            parent = nav._interface_index_path(owner, interface)
            anchor = nav._anchor_id("configuration-owner", owner)
            assert (
                f"]({nav._anchor_link(parent, target, anchor)})" in checked.files[parent].decode()
            )
            assert checked.files[target].decode().count(f'id="{anchor}"') == 1


def test_each_evidence_group_routes_contracts_and_tests_before_either_inventory(
    checked_contract_closure: dict[str, Any],
) -> None:
    checked = checked_contract_closure["atlas"]
    root = checked.root
    commands = checked.files[nav.QUALIFICATION_ROUTES_PATH].decode()
    for witness in root["trace"]["segmented_extent_witnesses"]:
        path = nav._witness_path(witness["id"])
        page = checked.files[path].decode()
        for name in ("contracts", "tests"):
            child = nav._witness_path(witness["id"], name)
            assert f"]({nav._relative_link(path, child)})" in page
            assert f"]({nav._relative_link(child, path)})" in checked.files[child].decode()
        tests = checked.files[nav._witness_path(witness["id"], "tests")].decode()
        for node in witness["test_node_ids"]:
            label = re.sub(r"([\\`*_\[\]])", r"\\\1", node)
            assert label in tests
            assert node not in page
        for scope in witness["test_scopes"]:
            assert nav._md(scope["scope"]) in tests
        for route in witness["gates"]:
            assert commands.count(f'id="{nav._qualification_anchor(route)}"') == 1
        assert "no executed qualification result or CI attestation" in tests
        assert "first page" in page
        assert "hidden limit" in page


def test_evidence_scope_keeps_differing_open_claims_exact() -> None:
    elements = [
        {"id": "a", "authority": "owner", "title": "first", "dossier": "riverhog-v1/a.md"},
        {"id": "b", "authority": "owner", "title": "second", "dossier": "riverhog-v1/b.md"},
        {"id": "c", "authority": "owner", "title": "unaffected", "dossier": "riverhog-v1/c.md"},
    ]
    page = references._render_qualification_scope(
        "owner",
        "riverhog-v1/evidence-gaps.md",
        "riverhog-v1/index.md",
        elements,
        {"a": ["one"], "b": ["two"], "c": []},
        {
            "one": {"unestablished_claims": ["restart"]},
            "two": {"unestablished_claims": ["forward_progress"]},
        },
    ).decode()
    assert "**2 affected contract elements**" in page
    assert "Each group's page identifies its exact open guarantees" in page
    assert nav._PROGRESSION_CLAIM_LABELS["restart"] in page
    assert nav._PROGRESSION_CLAIM_LABELS["forward_progress"] in page
    assert nav._PROGRESSION_CLAIM_LABELS["bounded_step"] not in page
    assert "c.md" not in page
    assert "[one](evidence/qualifications/one/index.md)" in page
    with pytest.raises(atlas.ContractAtlasError, match="lack a reader explanation"):
        nav._qualification_explanation(["unknown"])


def test_clearing_group_claims_preserves_bindings_without_reporting_open_gaps(
    checked_contract_closure: dict[str, Any],
) -> None:
    checked = checked_contract_closure["atlas"]
    root = checked.root
    trace = copy.deepcopy(root["trace"])

    def render() -> dict[str, bytes]:
        return references._render_evidence_references(
            root["elements"],
            root["projection"],
            trace,
            {k: v for k, v in root["identities"].items() if k != "atlas_representation_sha256"},
            root["discovery"],
            root["counts"],
            {record["id"]: record for record in root["sources"]},
            root["atlas"]["relationships"],
            nav._element_progression_witnesses(root["elements"], trace),
            nav._policy_definition_elements(root["elements"]),
        )

    # The current groups' output is unchanged by the empty-state handling.
    for path, payload in render().items():
        if path.startswith("riverhog-v1/evidence/qualifications/"):
            assert payload == checked.files[path], path
    for witness in trace["segmented_extent_witnesses"]:
        witness["unestablished_claims"] = []
    files = render()
    assert b"No open guarantees are recorded" in files[nav.QUALIFICATIONS_PATH]
    for witness in trace["segmented_extent_witnesses"]:
        overview = files[nav._witness_path(witness["id"])].decode()
        assert "No open guarantees are recorded" in overview
        assert "not an approval claim" in overview
        contracts = files[nav._witness_path(witness["id"], "contracts")].decode()
        assert "#evidence-gaps" not in contracts
        assert "Bound contract elements" in overview
        assert files[nav._witness_path(witness["id"], "tests")]
