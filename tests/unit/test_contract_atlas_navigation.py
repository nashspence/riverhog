from __future__ import annotations

import re
import sys
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import contract_atlas as atlas  # noqa: E402
from contract_atlas import navigation as atlas_navigation  # noqa: E402

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


def test_human_entrypoint_exposes_closure_exclusions_and_relationships() -> None:
    checked = checked_atlas()
    root = checked.root
    root_page = checked.files[root["atlas"]["root"]].decode()
    evidence_page = checked.files["riverhog-v1/evidence/index.md"].decode()
    exclusions_page = checked.files["riverhog-v1/evidence/exclusions.md"].decode()
    relationships_page = checked.files["riverhog-v1/evidence/relationships.md"].decode()
    authority_inventory = checked.files["riverhog-v1/evidence/authorities.md"].decode()
    configuration_inventory = checked.files["riverhog-v1/evidence/configuration.md"].decode()
    relationship = root["atlas"]["relationships"]

    ordered_sections = (
        "> **Audit question:**",
        "**Audit path:** Scope → Semantics → Evidence",
        "## Contract map",
        "## Contract-wide policies",
        "## Freeze evidence",
    )
    assert [root_page.index(section) for section in ordered_sections] == sorted(
        root_page.index(section) for section in ordered_sections
    )
    assert all(
        f"| {name.replace('_', ' ')} | pass |" in evidence_page
        for name in root["discovery"]["anomalies"]
    )
    assert "### Riverhog product" in root_page
    assert root_page.index("### Release envelope") < root_page.index("### Riverhog product")
    assert "### Maintainer-selected nonnormative references" in root_page
    assert "### [Cross-cutting v1 authorities]" in root_page
    assert "Guided contract map" not in root_page
    assert "SHA-256" not in root_page
    exclusions = root["discovery"]["exclusions"]
    assert len(exclusions) == root["counts"]["excluded_candidates"]
    assert all(
        exclusions_page.count(f'id="{atlas._anchor_id("exclusion", str(item["id"]))}"') == 1
        for item in exclusions
    )
    assert "# Relationship-edge inventory" in relationships_page
    assert "not a second navigation hierarchy" in relationships_page
    assert "intentionally an alphabetical reconciliation inventory" in authority_inventory
    assert "## Declared aggregate authorities" in authority_inventory
    assert "## Non-contractual projection machinery" in authority_inventory
    assert "`contract-projection-envelope`" in authority_inventory
    assert "`extent-contract` | Repository-wide v1 external extent" in authority_inventory
    assert "does not own any setting's semantics" in configuration_inventory
    assert "Environment contracts: **250**" in configuration_inventory
    assert "Unique names: **240**" in configuration_inventory
    assert "Raw implementation reads: **199**" in configuration_inventory
    assert "Explicit ambiguity resolutions: **6**" in configuration_inventory
    assert "| unowned | pass |" in configuration_inventory
    assert "[RIVERHOG_BASE_URL]" in configuration_inventory
    assert "`riverhog-client`" in configuration_inventory
    assert "`riverhog-ftp-adapter`" in configuration_inventory
    assert "`stove0-server`" in configuration_inventory
    assert relationship["schema"] == atlas.RELATIONSHIP_SCHEMA
    assert relationship["contract_map"]["schema"] == atlas.CONTRACT_MAP_SCHEMA
    assert any(item["kind"] == "runtime-image" for item in relationship["nodes"])
    assert any(item["type"] == "implements-protocol" for item in relationship["edges"])
    mapped = [
        item["authority"]
        for node in relationship["contract_map"]["nodes"]
        for item in node["authorities"]
    ]
    exact = {item["authority"] for item in root["elements"]}
    assert len(mapped) == len(set(mapped))
    assert set(mapped) == exact
    assert authority_inventory.count("| [") >= len(exact)
    assert atlas._reachable_atlas_documents(root["atlas"]["root"], checked.files) == set(
        checked.files
    )
    assert not any(path.startswith("riverhog-v1/relationships/") for path in checked.files)

    release_elements = [item for item in root["elements"] if item["authority"] == "release"]
    expected_interfaces = {
        "artifact-verification": 3,
        "compatibility-guarantees": 9,
        "installation-roots": 4,
        "publication-locations": 2,
        "python-distributions": 71,
        "release-artifacts": 12,
        "runtime-images": 13,
        "versioning-tags": 5,
    }
    assert len(release_elements) == 119
    assert Counter(item["interface"] for item in release_elements) == expected_interfaces
    assert "release" not in {item["interface"] for item in release_elements}
    assert "riverhog-v1/authorities/release/release/index.md" not in checked.files
    for interface, count in expected_interfaces.items():
        interface_path = f"riverhog-v1/authorities/release/{interface}/index.md"
        interface_page = checked.files[interface_path].decode()
        interface_elements = [item for item in release_elements if item["interface"] == interface]
        assert len(interface_elements) == count
        assert "| Exact unit | Classification |" in interface_page
        assert "\n### " not in interface_page
        assert all(
            interface_page.count(f"]({atlas._relative_link(interface_path, str(item['dossier']))})")
            == 1
            for item in interface_elements
        )
    runtime_page = checked.files["riverhog-v1/authorities/release/runtime-images/index.md"].decode()
    assert "`product`" in runtime_page
    assert "`reference_application`" in runtime_page
    assert "`reference_component`" in runtime_page

    publication_policies = {item["id"]: item for item in root["policies"]["publication"]}
    assert {key: len(value["applies_to"]) for key, value in publication_policies.items()} == {
        "publication/image-digest-scope/v1": 13,
        "publication/platform-scope/v1": 17,
        "publication/role-retention/v1": 88,
    }
    policy_counts = Counter(
        policy
        for item in release_elements
        for policy in item["policy_ids"]
        if policy.startswith("publication/")
    )
    assert policy_counts == {
        "publication/image-digest-scope/v1": 13,
        "publication/platform-scope/v1": 17,
        "publication/role-retention/v1": 88,
    }
    assert "`release-publication-envelope`" in authority_inventory


def test_contract_map_routes_every_interface_and_extension_without_duplicate_semantics() -> None:
    checked = checked_atlas()
    root = checked.root
    root_path = root["atlas"]["root"]
    root_page = checked.files[root_path].decode()
    elements = root["elements"]
    relationship = root["atlas"]["relationships"]
    interface_counts: dict[tuple[str, str], int] = {}
    for item in elements:
        key = (item["authority"], item["interface"])
        interface_counts[key] = interface_counts.get(key, 0) + 1

    for (authority, interface), count in interface_counts.items():
        target = atlas._interface_index_path(authority, interface)
        assert f"]({atlas._relative_link(root_path, target)}) ({count})" in root_page

    extension_nodes = [
        item
        for item in relationship["nodes"]
        if item["kind"] in {"extension-point", "process-protocol"}
    ]
    assert extension_nodes
    assert all(item["contract_elements"] == 0 for item in extension_nodes)
    assert all(item["semantic_interfaces"] for item in extension_nodes)
    for node in extension_nodes:
        extension_path = atlas._extension_context_path(node)
        extension_link = atlas._relative_link(root_path, extension_path)
        extension_page = checked.files[extension_path].decode()
        descriptor = next(
            item for item in root["atlas"]["documents"] if item["path"] == extension_path
        )

        assert f"]({extension_link})" in root_page
        assert descriptor["kind"] == "extension-context"
        assert descriptor["counts"] == {}
        assert descriptor["extension_id"] == node["id"]
        assert f"- Identity: `{node['id']}`" in extension_page
        assert node["description"] in extension_page
        assert "## Checked-in nonnormative implementations" in extension_page
        assert "Contract elements" not in extension_page
        assert "Extent decisions" not in extension_page
        for interface in node["semantic_interfaces"]:
            key = (interface["authority"], interface["interface"])
            assert interface["contract_elements"] == interface_counts[key]
            target = atlas._interface_index_path(*key)
            assert f"]({atlas._relative_link(extension_path, target)})" in extension_page


def test_primary_semantic_path_is_exact_without_aggregate_accounting() -> None:
    checked = checked_atlas()
    root = checked.root
    root_page = checked.files[root["atlas"]["root"]].decode()

    assert "contract elements" not in root_page
    assert not re.search(r"— \d+ authorit(?:y|ies)", root_page)
    assert "Python extension:" not in root_page
    assert "Process protocol:" not in root_page

    for document in root["atlas"]["documents"]:
        page = checked.files[document["path"]].decode()
        if document["kind"] == "interface-index":
            assert "Contract elements:" not in page
            assert "Extent decisions:" not in page
            assert "| Policy | Count |" not in page
            assert "| Dossier | Extent decisions |" not in page
            assert "## Semantic dossiers" in page
        elif document["kind"] == "dossier":
            assert "| Contract elements |" not in page
            assert "| Extent decisions |" not in page


def test_interfaces_are_flat_exact_inventories_with_local_references() -> None:
    checked = checked_atlas()
    documents = checked.root["atlas"]["documents"]

    assert not any(item["kind"] == "family-index" for item in documents)
    assert not any("family" in item for item in checked.root["elements"])
    assert not any("/families/" in item["path"] for item in documents)
    retrieval_cache = checked.files[
        "riverhog-v1/authorities/riverhog/http-operations/get-v1-retrieval-cache.md"
    ].decode()
    assert "## Referenced contract dossiers" in retrieval_cache
    assert (
        "[schemas: RetrievalCacheStatusOut](../http-schemas/schemas-retrievalcachestatusout.md)"
        in retrieval_cache
    )


def test_cli_dossiers_expose_exact_result_and_failure_contracts() -> None:
    checked = checked_atlas()
    cli_elements = [item for item in checked.root["elements"] if item["interface"] == "cli"]
    executable = [item for item in cli_elements if item.get("details", {}).get("executable")]

    assert len(executable) == 132
    assert len({item["details"]["result_identity"] for item in executable}) == 132
    assert {
        tuple(item["details"]["command_path"])
        for item in executable
        if len(item["details"]["command_path"]) == 1
    } == {
        ("mango-fish",),
        ("riverhog-ftp-adapter",),
        ("riverhog-recover",),
        ("riverhog-storage-adapter-conformance",),
        ("riverhog-storage-adapter-filesystem-materialize",),
        ("riverhog-storage-adapter-schemas",),
        ("stove0-observer-conformance",),
        ("stove0-observer-schemas",),
        ("stove0-review-planning",),
        ("stove0-review-sampler-conformance",),
        ("stove0-review-sampler-schemas",),
        ("stove0-target-conformance",),
        ("stove0-target-schemas",),
    }
    assert not any("mango-fish mango-fish" in item["title"] for item in cli_elements)
    assert not any(
        "riverhog-ftp-adapter riverhog-ftp-adapter" in item["title"] for item in cli_elements
    )

    upload = next(item for item in executable if item["title"] == "piggity collection upload start")
    page = checked.files[upload["dossier"]].decode()
    assert "### Result and failure contract" in page
    assert "`piggity-cli-result/collection/upload/start/v1`" in page
    assert "`custody-timeout`" in page
    assert "`124`" in page
    piggity_index = checked.files["riverhog-v1/authorities/piggity/cli/index.md"].decode()
    assert "Executable commands: **66** · Command groups: **20**" in piggity_index
    assert "### Command tree" in piggity_index
    assert "### Executable commands" not in piggity_index
    assert "### Command groups" not in piggity_index
    assert "piggity-cli-human-json/v1" not in piggity_index
    assert "- [piggity](piggity.md)" in piggity_index
    assert "    - [upload](piggity-collection-upload.md)" in piggity_index
    assert "      - [start](piggity-collection-upload-start.md)" in piggity_index
    app_key_create = next(item for item in executable if item["title"] == "piggity app key create")
    assert atlas._dossier_navigation_labels(upload, [app_key_create]) == {
        app_key_create["id"]: "app key create"
    }

    collection_list = next(
        item for item in executable if item["title"] == "piggity collection list"
    )
    list_page = checked.files[collection_list["dossier"]].decode()
    assert (
        "json: [HTTP list_collections response 200]"
        "(../../riverhog/http-operations/get-v1-collections.md#" in list_page
    )
    assert (
        "json: [http-api-contracts.ErrorResponse]"
        "(../../http-api-contracts/python/http-api-contracts-errorresponse.md)" in list_page
    )

    for item in executable:
        result = atlas.pointer_value(
            checked.root["projection"],
            next(pointer for pointer in item["pointers"] if pointer.endswith("/result_contract")),
        )
        structured = sum(
            isinstance(semantics, dict)
            for outcome in [*result["success"], *result["failures"]]
            for channel in (outcome["stdout"], outcome["stderr"])
            for semantics in channel.values()
        )
        result_section = (
            checked.files[item["dossier"]]
            .decode()
            .split("### Result and failure contract\n", 1)[1]
            .split("\n### ", 1)[0]
            .split("\n## ", 1)[0]
        )
        assert result_section.count("](") == structured
        assert "schema-authority" not in result_section


def test_process_protocols_own_exact_metadata_operations_and_schemas() -> None:
    checked = checked_atlas()
    elements = checked.root["elements"]
    projection = checked.root["projection"]
    by_interface: dict[str, list[dict[str, object]]] = {}
    for item in elements:
        by_interface.setdefault(item["interface"], []).append(item)

    assert len(by_interface["process-protocol"]) == 4
    assert len(by_interface["process-protocol-operations"]) == 24
    assert len(by_interface["process-protocol-schemas"]) == 43
    assert len(by_interface["schema"]) == 31
    assert "protocol" not in by_interface

    generated = {
        name: document
        for name, document in projection["external_contract"]["protocol_schemas"].items()
        if name.startswith("generated:")
    }
    for name, document in generated.items():
        base = f"/external_contract/protocol_schemas/{atlas._escape_pointer(name)}"
        parent = next(
            item
            for item in by_interface["process-protocol"]
            if f"{base}/format" in item["pointers"]
        )
        assert not any(
            pointer.startswith(f"{base}/http_binding") or pointer.startswith(f"{base}/schemas")
            for pointer in parent["pointers"]
        )
        operation_pointers = {
            pointer
            for item in by_interface["process-protocol-operations"]
            for pointer in item["pointers"]
            if pointer.startswith(f"{base}/")
        }
        schema_pointers = {
            pointer
            for item in by_interface["process-protocol-schemas"]
            for pointer in item["pointers"]
            if pointer.startswith(f"{base}/")
        }
        assert operation_pointers == {
            f"{base}/http_binding/operations/{index}"
            for index in range(len(document["http_binding"]["operations"]))
        }
        assert schema_pointers == {
            f"{base}/schemas/{atlas._escape_pointer(schema)}" for schema in document["schemas"]
        }


def test_python_contract_units_are_exact_and_navigate_module_export_member() -> None:
    checked = checked_atlas()
    root = checked.root
    surfaces = root["projection"]["external_contract"]["python"]
    elements = [item for item in root["elements"] if item["interface"] == "python"]
    registry = root["trace"]["python_registry"]

    assert len(elements) == len(surfaces) == registry["coverage"]["protected"]
    assert {surface["unit"] for surface in surfaces.values()} == {"export", "member"}
    assert "riverhog_client.ApiClient" in surfaces
    assert "riverhog_client.ApiClient.list_collections" in surfaces
    assert "riverhog_client.transform.CapabilityApiClient.__enter__" in surfaces
    assert surfaces["riverhog_client.ApiClient.list_collections"]["contract"]["signature"]
    assert "members" not in surfaces["riverhog_client.ApiClient"]["contract"]

    index = checked.files["riverhog-v1/authorities/riverhog-client/python/index.md"].decode()
    assert index.index("### `riverhog_client`") < index.index("### `riverhog_client.transform`")
    export_line = "- [ApiClient](riverhog-client-apiclient.md)"
    member_line = "  - [list_collections](riverhog-client-apiclient-list-collections.md)"
    assert export_line in index
    assert member_line in index
    assert index.index(export_line) < index.index(member_line)


def test_interface_inventory_labels_are_contextual_unique_and_canonically_ordered() -> None:
    checked = checked_atlas()
    assert set(atlas.INTERFACE_REGISTRY) == set(atlas.INTERFACE_LABELS)
    assert all(
        key == descriptor.identity
        and descriptor.label
        and descriptor.purpose
        and descriptor.qualification_routes
        and descriptor.navigation_provider
        and descriptor.renderer
        for key, descriptor in atlas.INTERFACE_REGISTRY.items()
    )
    assert {
        descriptor.navigation_provider for descriptor in atlas.INTERFACE_REGISTRY.values()
    } == set(atlas_navigation._NAVIGATION_FUNCTIONS)
    runtime_path = "riverhog-v1/authorities/release/runtime-images/index.md"
    runtime_page = checked.files[runtime_path].decode()
    runtime_elements = sorted(
        (
            item
            for item in checked.root["elements"]
            if item["authority"] == "release" and item["interface"] == "runtime-images"
        ),
        key=lambda item: item["title"],
    )
    assert "[Runtime image:" not in runtime_page
    assert "[riverhog](runtime-image-riverhog.md)" in runtime_page
    assert [
        runtime_page.index(f"]({atlas._relative_link(runtime_path, item['dossier'])})")
        for item in runtime_elements
    ] == sorted(
        runtime_page.index(f"]({atlas._relative_link(runtime_path, item['dossier'])})")
        for item in runtime_elements
    )

    schemas = checked.files["riverhog-v1/authorities/riverhog/http-schemas/index.md"].decode()
    assert "[schemas: " not in schemas
    assert "[CollectionSummaryOut](schemas-collectionsummaryout.md)" in schemas

    mango = checked.files["riverhog-v1/authorities/mango-fish/cli/index.md"].decode()
    assert "- [mango-fish](mango-fish.md)" in mango

    durable = checked.files[
        "riverhog-v1/authorities/riverhog-catalog/durable-state/index.md"
    ].decode()
    assert "| Exact unit | Kind |" in durable
    assert "| [Schema identity](riverhog-catalog-durable-state-identity.md) | " in durable
    assert "| Relational table |" in durable

    versioning = checked.files["riverhog-v1/authorities/release/versioning-tags/index.md"].decode()
    for label in (
        "distribution version",
        "versioning policy",
        "versioning series",
        "tag immutability",
        "tag template",
    ):
        assert f"| [{label}](" in versioning

    for authority in {str(item["authority"]) for item in checked.root["elements"]}:
        for interface in {
            str(item["interface"])
            for item in checked.root["elements"]
            if item["authority"] == authority
        }:
            values = [
                item
                for item in checked.root["elements"]
                if item["authority"] == authority and item["interface"] == interface
            ]
            labels = atlas._interface_navigation_labels(interface, values)
            page_path = atlas._interface_index_path(authority, interface)
            page = checked.files[page_path].decode()
            for item in values:
                link = atlas._relative_link(page_path, str(item["dossier"]))
                assert page.count(f"[{atlas._md(labels[str(item['id'])])}]({link})") == 1

    items = [
        {"id": "a", "title": "alpha.same"},
        {"id": "b", "title": "beta.same"},
    ]
    assert atlas._contextual_labels(items, lambda item: ["same", str(item["title"])]) == {
        "a": "alpha.same",
        "b": "beta.same",
    }
    with pytest.raises(atlas.ContractAtlasError, match="remain ambiguous"):
        atlas._contextual_labels(
            [{"id": "a", "title": "same"}, {"id": "b", "title": "same"}],
            lambda item: [str(item["title"])],
        )


def test_navigation_is_representation_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checked = checked_atlas()
    component_descriptions = {
        str(item["name"]): str(item["description"])
        for item in checked.root["atlas"]["relationships"]["nodes"]
        if item["kind"] == "component"
    }
    original = atlas_navigation._NAVIGATION_FUNCTIONS["atomic"]
    semantic_identity_names = {
        "boundary_canonical_sha256",
        "boundary_legacy_sha256",
        "external_contract_sha256",
        "semantic_contract_sha256",
        "coverage_sha256",
        "trace_sha256",
    }
    semantic_identities = {key: checked.root["identities"][key] for key in semantic_identity_names}

    def changed_schema_navigation(item: Mapping[str, object]) -> atlas.NavigationIdentity:
        identity = original(item)
        return atlas.NavigationIdentity(
            (f"Rendered {identity.components[0]}",),
            kind=identity.kind,
        )

    monkeypatch.setitem(atlas_navigation._NAVIGATION_FUNCTIONS, "atomic", changed_schema_navigation)
    _files, documents, relationship = atlas._render_atlas(
        checked.root["elements"],
        checked.root["policies"],
        checked.root["projection"],
        checked.root["trace"],
        checked.root["identities"],
        checked.root["discovery"]["exclusions"],
        checked.root["discovery"],
        component_descriptions,
    )

    changed_representation = {
        "schema": atlas.REPRESENTATION_IDENTITY_SCHEMA,
        "documents": documents,
        "relationships": relationship,
    }
    assert semantic_identities == {
        key: checked.root["identities"][key] for key in semantic_identity_names
    }
    assert checked.root["identities"]["atlas_representation_sha256"] != atlas.canonical_sha256(
        changed_representation
    )


def test_atlas_routes_policies_sources_and_relationships_to_exact_subjects() -> None:
    checked = checked_atlas()
    root = checked.root
    policy_page = checked.files["riverhog-v1/policies/index.md"].decode()
    source_page = checked.files["riverhog-v1/evidence/sources.md"].decode()
    relationship_page = checked.files["riverhog-v1/evidence/relationships.md"].decode()

    policy_ids = {item["id"] for category in root["policies"].values() for item in category}
    assert all(
        policy_page.count(f'id="{atlas._policy_anchor(identity)}"') == 1 for identity in policy_ids
    )
    assert all(
        source_page.count(f'id="{atlas._source_anchor(item["id"])}"') == 1
        for item in root["sources"]
    )
    assert all(
        relationship_page.count(f'id="{atlas._relationship_node_anchor(item["id"])}"') == 1
        for item in root["atlas"]["relationships"]["nodes"]
    )
    assert all(
        relationship_page.count(f'id="{atlas._relationship_edge_anchor(item)}"') == 1
        for item in root["atlas"]["relationships"]["edges"]
    )
