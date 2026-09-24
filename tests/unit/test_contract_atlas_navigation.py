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
from contract_atlas import rendering as atlas_rendering  # noqa: E402

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


@pytest.mark.parametrize("checkout", ("project", "riverhog", "riverhog/riverhog"))
def test_repository_source_links_do_not_depend_on_checkout_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, checkout: str
) -> None:
    worktree = tmp_path / checkout
    worktree.mkdir(parents=True)
    monkeypatch.chdir(worktree)
    document = "riverhog-v1/authorities/riverhog/http-operations/operation.md"
    location = {"path": "riverhog/src/riverhog_api/routers/apps.py", "line": 219}

    link = atlas_navigation._repository_source_link(document, location, "Handler")

    assert link == "[Handler](../../../../../../riverhog/src/riverhog_api/routers/apps.py#L219)"
    assert atlas._reachable_atlas_documents(
        document,
        {document: link.encode()},
        repository_sources={"../../riverhog/src/riverhog_api/routers/apps.py#L219"},
    ) == {document}


def test_human_entrypoint_exposes_complete_inclusion_and_relationships() -> None:
    checked = checked_atlas()
    root = checked.root
    root_page = checked.files[root["atlas"]["root"]].decode()
    evidence_page = checked.files["riverhog-v1/evidence/index.md"].decode()
    relationships_page = checked.files["riverhog-v1/evidence/relationships.md"].decode()
    authority_inventory = checked.files["riverhog-v1/evidence/authorities.md"].decode()
    configuration_inventory = checked.files["riverhog-v1/evidence/configuration.md"].decode()
    relationship = root["atlas"]["relationships"]

    ordered_sections = (
        "> **Audit question:**",
        "## Audit references",
        "## Authorities and interfaces",
    )
    assert [root_page.index(section) for section in ordered_sections] == sorted(
        root_page.index(section) for section in ordered_sections
    )
    assert all(
        f"| {name.replace('_', ' ')} | pass |" in evidence_page
        for name in root["discovery"]["anomalies"]
    )
    assert "Discovery means inclusion" in root_page
    assert "Complete accounting does not establish" in root_page
    assert "[machine artifact (raw JSON)](../riverhog-v1.json?raw=1)" in root_page
    for target in (
        "policies/index.md",
        "evidence/index.md",
        "evidence/sources.md",
        "evidence/configuration.md",
        "evidence/relationships.md",
        "evidence/identities.md",
    ):
        assert root_page.index(f"]({target})") < root_page.index("## Authorities and interfaces")
    assert {item["candidate_id"] for item in root["discovery"]["dispositions"]} == {
        item["id"] for item in root["discovery"]["candidates"]
    }
    assert {item["disposition"] for item in root["discovery"]["dispositions"]} == {"protected"}
    assert "# Declared relationships" in relationships_page
    for target in (
        atlas_navigation.RELATIONSHIP_NODES_PATH,
        atlas_navigation.RELATIONSHIP_EDGES_PATH,
    ):
        link = atlas._relative_link("riverhog-v1/evidence/relationships.md", target)
        assert f"]({link})" in relationships_page
    assert "## Declared aggregate scopes" in authority_inventory
    assert "## Non-contractual projection machinery" in authority_inventory
    assert "`contract-projection-envelope`" in authority_inventory
    assert "[extent-contract](../authorities/extent-contract/index.md)" in authority_inventory
    assert "owning contract element" in configuration_inventory
    reconciliation = checked.files["riverhog-v1/evidence/configuration/reconciliation.md"].decode()
    assert "Unique environment names: **240**" in reconciliation
    assert "Implementation reads: **198**" in reconciliation
    assert "Explicit ambiguity resolutions: **6**" in reconciliation
    assert "| unowned | pass |" in reconciliation
    settings = checked.files[atlas_navigation.CONFIGURATION_SETTINGS_PATH].decode()
    assert "[RIVERHOG_BASE_URL]" in settings
    assert all(
        f"`{owner}`" in settings
        for owner in ("riverhog-client", "a-riverhog-ftp-spool", "stove0-server")
    )
    assert relationship["format"] == atlas.RELATIONSHIP_FORMAT
    assert any(item["kind"] == "runtime-image" for item in relationship["nodes"])
    assert any(item["type"] == "implements-protocol" for item in relationship["edges"])
    exact = {item["authority"] for item in root["elements"]}
    listed = re.findall(
        r"^- (?:\*\*)?\[([^]]+)\]\(authorities/[^/]+/index.md\)", root_page, re.MULTILINE
    )
    assert listed == sorted(exact)
    assert {line for line in root_page.splitlines() if line.startswith("## ")} == {
        "## Audit references",
        "## Authorities and interfaces",
    }
    assert set(relationship) == {
        "format",
        "center",
        "product",
        "nodes",
        "edges",
    }
    for authority in exact:
        target = atlas_rendering._authority_index_path(authority)
        assert root_page.count(f"]({atlas._relative_link(root['atlas']['root'], target)})") == 1
        declared = {a["id"] for a in root["trace"]["authority_registry"]["declared_authorities"]}
        link = f"]({atlas._relative_link('riverhog-v1/evidence/authorities.md', target)})"
        assert (link in authority_inventory) == (authority in declared)
    assert atlas._reachable_atlas_documents(
        root["atlas"]["root"],
        checked.files,
        repository_sources=atlas_navigation._repository_source_targets(
            root["trace"], root["sources"]
        ),
    ) == set(checked.files)
    assert not any(path.startswith("riverhog-v1/relationships/") for path in checked.files)

    release_elements = [item for item in root["elements"] if item["authority"] == "release"]
    expected_interfaces = {
        "artifact-verification": 3,
        "compatibility-guarantees": 9,
        "installation-roots": 4,
        "publication-locations": 2,
        "python-distributions": 72,
        "release-artifacts": 12,
        "runtime-images": 13,
        "versioning-tags": 5,
    }
    assert len(release_elements) == 120
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
    assert "`application`" in runtime_page
    assert "`component`" in runtime_page

    publication_policies = {item["id"]: item for item in root["policies"]["publication"]}
    assert {key: len(value["applies_to"]) for key, value in publication_policies.items()} == {
        "publication/image-identity-scope/v1": 13,
        "publication/platform-scope/v1": 17,
        "publication/role-retention/v1": 89,
    }
    policy_counts = Counter(
        policy
        for item in release_elements
        for policy in item["policy_ids"]
        if policy.startswith("publication/")
    )
    assert policy_counts == {
        "publication/image-identity-scope/v1": 13,
        "publication/platform-scope/v1": 17,
        "publication/role-retention/v1": 89,
    }
    assert "`release-publication-envelope`" in authority_inventory


def test_authority_inventory_routes_every_interface_and_extension_without_duplicate_semantics() -> (
    None
):
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
        line = next(
            line
            for line in root_page.splitlines()
            if f"]({atlas._relative_link(root_path, target)})" in line
        )
        assert f" ({count})" in line

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
        assert "## Supplied implementations" in extension_page
        assert "Contract elements" not in extension_page
        assert "Extent decisions" not in extension_page
        for interface in node["semantic_interfaces"]:
            key = (interface["authority"], interface["interface"])
            assert interface["contract_elements"] == interface_counts[key]
            target = atlas._interface_index_path(*key)
            assert f"]({atlas._relative_link(extension_path, target)})" in extension_page


def test_primary_semantic_path_keeps_detailed_accounting_in_reference() -> None:
    checked = checked_atlas()
    root = checked.root
    root_page = checked.files[root["atlas"]["root"]].decode()

    assert f"Included contract elements: **{root['counts']['contract_elements']}**" in root_page
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
            assert "## Contract elements" in page
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
    assert "## Referenced contract elements" in retrieval_cache
    assert (
        "[schemas: RetrievalCacheStatusOut](../http-schemas/schemas-retrievalcachestatusout.md)"
        in retrieval_cache
    )


def test_cli_dossiers_expose_exact_result_and_failure_contracts() -> None:
    checked = checked_atlas()
    cli_elements = [item for item in checked.root["elements"] if item["interface"] == "cli"]
    executable = [item for item in cli_elements if item.get("details", {}).get("executable")]

    assert len(executable) == 153
    assert len({item["details"]["result_identity"] for item in executable}) == len(executable)
    assert {
        tuple(item["details"]["command_path"])
        for item in executable
        if len(item["details"]["command_path"]) == 1
    } == {
        ("a-riverhog-event-relay",),
        ("riverhog-api",),
        ("a-riverhog-ftp-spool",),
        ("a-riverhog-recovery-tool",),
        ("a-riverhog-aws-store",),
        ("a-riverhog-b2-store",),
        ("riverhog-storage-adapter-conformance",),
        ("a-riverhog-filesystem-store",),
        ("a-riverhog-filesystem-store-materialize",),
        ("riverhog-storage-adapter-schemas",),
        ("a-stove0-exiftool-observer",),
        ("a-stove0-ffprobe-sampling-observer",),
        ("a-review0-nvenc-av1-opus-sampler",),
        ("a-stove0-nvenc-av1-opus-target",),
        ("stove0-observer-conformance",),
        ("stove0-observer-schemas",),
        ("a-review0-opus-sampler",),
        ("a-stove0-opus-target",),
        ("a-review0-materializer",),
        ("review0-planner",),
        ("a-review0-rclone-target",),
        ("review0-sampler-conformance",),
        ("review0-sampler-schemas",),
        ("stove0-target-conformance",),
        ("stove0-target-schemas",),
    }
    assert not any(
        "a-riverhog-event-relay a-riverhog-event-relay" in item["title"] for item in cli_elements
    )
    assert not any(
        "a-riverhog-ftp-spool a-riverhog-ftp-spool" in item["title"] for item in cli_elements
    )

    upload = next(
        item for item in executable if item["title"] == "a-riverhog-cli collection upload start"
    )
    page = checked.files[upload["dossier"]].decode()
    assert "### Result and failure contract" in page
    assert "`a-riverhog-cli-result/collection/upload/start/v1`" in page
    assert "`custody-timeout`" in page
    assert "`124`" in page
    a_riverhog_cli_index = checked.files[
        "riverhog-v1/authorities/a-riverhog-cli/cli/index.md"
    ].decode()
    assert "Executable commands: **67** · Command groups: **20**" in a_riverhog_cli_index
    assert "### Command tree" in a_riverhog_cli_index
    assert "### Executable commands" not in a_riverhog_cli_index
    assert "### Command groups" not in a_riverhog_cli_index
    assert "a-riverhog-cli-human-json/v1" not in a_riverhog_cli_index
    assert "- [a-riverhog-cli](a-riverhog-cli.md)" in a_riverhog_cli_index
    assert "    - [upload](a-riverhog-cli-collection-upload.md)" in a_riverhog_cli_index
    assert "      - [start](a-riverhog-cli-collection-upload-start.md)" in a_riverhog_cli_index
    app_key_create = next(
        item for item in executable if item["title"] == "a-riverhog-cli app key create"
    )
    assert atlas._dossier_navigation_labels(upload, [app_key_create]) == {
        app_key_create["id"]: "app key create"
    }

    collection_list = next(
        item for item in executable if item["title"] == "a-riverhog-cli collection list"
    )
    list_page = checked.files[collection_list["dossier"]].decode()
    assert (
        "json: [HTTP list_collections response 200]"
        "(../../riverhog/http-operations/post-v1-collections-search.md#" in list_page
    )
    assert (
        "json: [http-api-contracts.ErrorOut]"
        "(../../http-api-contracts/python/http-api-contracts-errorout.md)" in list_page
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
    assert "riverhog_client.processing.CapabilityApiClient.__enter__" in surfaces
    assert surfaces["riverhog_client.ApiClient.list_collections"]["contract"]["signature"]
    assert "members" not in surfaces["riverhog_client.ApiClient"]["contract"]

    index = checked.files["riverhog-v1/authorities/riverhog-client/python/index.md"].decode()
    assert index.index("### `riverhog_client`") < index.index("### `riverhog_client.processing`")
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

    mango = checked.files["riverhog-v1/authorities/a-riverhog-event-relay/cli/index.md"].decode()
    assert "- [a-riverhog-event-relay](a-riverhog-event-relay.md)" in mango

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
        checked.root["discovery"],
        component_descriptions,
        projection_integer_paths=checked.root["projection_unsafe_integer_paths"],
    )

    changed_representation = {
        "format": atlas.REPRESENTATION_IDENTITY_FORMAT,
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
    definitions = atlas_navigation._policy_definition_elements(root["elements"], root["policies"])
    for category in root["policies"].values():
        for policy in category:
            path, anchor = atlas_navigation._policy_destination(policy["id"], definitions)
            assert checked.files[path].decode().count(f'id="{anchor}"') == 1
    source_page = checked.files[atlas_navigation.SOURCE_AUTHORITIES_PATH].decode()
    node_page = checked.files[atlas_navigation.RELATIONSHIP_NODES_PATH].decode()
    edge_page = checked.files[atlas_navigation.RELATIONSHIP_EDGES_PATH].decode()
    assert all(
        source_page.count(f'id="{atlas._source_anchor(item["id"])}"') == 1
        for item in root["sources"]
    )
    assert all(
        node_page.count(f'id="{atlas._relationship_node_anchor(item["id"])}"') == 1
        for item in root["atlas"]["relationships"]["nodes"]
    )
    assert all(
        edge_page.count(f'id="{atlas._relationship_edge_anchor(item)}"') == 1
        for item in root["atlas"]["relationships"]["edges"]
    )


def test_every_recorded_qualification_follows_exact_ordinary_selection_paths() -> None:
    checked = checked_atlas()
    root = checked.root
    elements = root["elements"]
    witness_by_id = {w["id"]: w for w in root["trace"]["segmented_extent_witnesses"]}
    # Derive the expectation directly from the trace, independently of the renderer helper.
    expected = {
        item["id"]: {
            witness
            for source in root["trace"]["extent_sources"]
            if source["id"] in item["extent_decision_ids"]
            for witness in source.get("segmented_extent_witnesses", [])
            if witness_by_id[witness]["unestablished_claims"]
        }
        for item in elements
    }
    assert sum(bool(ids) for ids in expected.values()) == 81
    root_path = root["atlas"]["root"]
    root_page = checked.files[root_path].decode()
    for authority in {item["authority"] for item in elements}:
        authority_path = atlas_rendering._authority_index_path(authority)
        authority_link = atlas._relative_link(root_path, authority_path)
        root_line = next(line for line in root_page.splitlines() if f"]({authority_link})" in line)
        owned = [item for item in elements if item["authority"] == authority]
        assert ("[📦](" in root_line) == any(expected[e["id"]] for e in owned)
        authority_page = checked.files[authority_path].decode()
        for interface in {item["interface"] for item in owned}:
            values = [item for item in owned if item["interface"] == interface]
            interface_path = atlas._interface_index_path(authority, interface)
            for parent_path, parent_page in (
                (root_path, root_page),
                (authority_path, authority_page),
            ):
                target = atlas._relative_link(parent_path, interface_path)
                line = next(line for line in parent_page.splitlines() if f"]({target})" in line)
                assert ("[📦](" in line) == any(expected[e["id"]] for e in values)
            interface_page = checked.files[interface_path].decode()
            for item in values:
                target = atlas._relative_link(interface_path, item["dossier"])
                line = next(line for line in interface_page.splitlines() if f"]({target})" in line)
                assert ("[📦](" in line) == bool(expected[item["id"]])
                for witness in expected[item["id"]]:
                    assert f"]({target}#evidence-gaps)" in line
                    witness_path = atlas_navigation._witness_path(witness)
                    dossier = checked.files[item["dossier"]].decode()
                    assert f"]({atlas._relative_link(item['dossier'], witness_path)})" in dossier
                    witness_page = checked.files[witness_path].decode()
                    for claim in witness_by_id[witness]["unestablished_claims"]:
                        assert atlas_navigation._PROGRESSION_CLAIM_LABELS[claim] in witness_page
                    contract_path = atlas_navigation._witness_path(witness, "contracts")
                    assert (
                        f"]({atlas._relative_link(contract_path, item['dossier'])}#evidence-gaps)"
                        in checked.files[contract_path].decode()
                    )
            for parent_path, parent_page in (
                (root_path, root_page),
                (authority_path, authority_page),
            ):
                target = atlas._relative_link(parent_path, interface_path)
                line = next(line for line in parent_page.splitlines() if f"]({target})" in line)
                affected = [item for item in values if expected[item["id"]]]
                if affected:
                    scope_path = atlas_navigation._scope_qualification_path(interface_path)
                    assert f"[📦]({atlas._relative_link(parent_path, scope_path)})" in line
                    scope_page = checked.files[scope_path].decode()
                    for item in values:
                        link = (
                            f"]({atlas._relative_link(scope_path, item['dossier'])}#evidence-gaps)"
                        )
                        assert (link in scope_page) == bool(expected[item["id"]])
                    assert "first page" in scope_page
                    assert "hidden limit" in scope_page
                    assert "group-wide scope" in scope_page
    for node in root["atlas"]["relationships"]["nodes"]:
        if node["kind"] not in {"extension-point", "process-protocol"}:
            continue
        path = atlas._extension_context_path(node)
        page = checked.files[path].decode()
        for interface in node["semantic_interfaces"]:
            target = atlas._interface_index_path(interface["authority"], interface["interface"])
            line = next(
                line
                for line in page.splitlines()
                if f"]({atlas._relative_link(path, target)})" in line
            )
            affected = any(
                expected[e["id"]]
                for e in elements
                if e["authority"] == interface["authority"]
                and e["interface"] == interface["interface"]
            )
            assert ("[📦](" in line) == affected
    # An operation's related CLI/client records do not inherit its qualification.
    operation = next(
        e
        for e in elements
        if e["authority"] == "riverhog" and e["title"] == "POST /v1/collections:search"
    )
    assert expected[operation["id"]]
    assert operation["related_element_ids"]
    assert all(not expected[identity] for identity in operation["related_element_ids"])


def test_qualification_selection_reacts_to_exact_binding_and_open_claim_changes() -> None:
    elements = [
        {"id": "bound", "extent_decision_ids": ["extent:a"], "related_element_ids": ["related"]},
        {"id": "related", "extent_decision_ids": [], "related_element_ids": ["bound"]},
        {"id": "same-looking-name", "extent_decision_ids": ["extent:b"]},
    ]
    trace = {
        "extent_sources": [{"id": "extent:a", "segmented_extent_witnesses": ["witness:a"]}],
        "segmented_extent_witnesses": [{"id": "witness:a", "unestablished_claims": ["restart"]}],
    }
    assert atlas_navigation._element_progression_witnesses(elements, trace) == {
        "bound": ("witness:a",),
        "related": (),
        "same-looking-name": (),
    }
    trace["extent_sources"][0]["id"] = "extent:b"
    assert atlas_navigation._element_progression_witnesses(elements, trace) == {
        "bound": (),
        "related": (),
        "same-looking-name": ("witness:a",),
    }
    trace["segmented_extent_witnesses"][0]["unestablished_claims"] = []
    assert not any(atlas_navigation._element_progression_witnesses(elements, trace).values())
    trace["extent_sources"][0]["segmented_extent_witnesses"] = ["missing"]
    with pytest.raises(atlas.ContractAtlasError, match="unresolved witness"):
        atlas_navigation._element_progression_witnesses(elements, trace)


def test_shared_qualification_is_scoped_once_and_mixed_entries_are_exact() -> None:
    page = "riverhog-v1/authorities/example/schema/index.md"
    values = [
        {"id": name, "dossier": page.replace("index.md", f"{name}.md")} for name in ("a", "b")
    ]
    mapping = {"a": ("witness:a",), "b": ("witness:a",)}
    lines, suffixes = atlas_rendering._interface_qualifications(values, mapping, page)
    assert "All contract elements on this page" in "\n".join(lines)
    assert "same recorded evidence gaps" in "\n".join(lines)
    assert "[📦](evidence-gaps.md)" in "\n".join(lines)
    assert not suffixes
    mapping["b"] = ()
    lines, suffixes = atlas_rendering._interface_qualifications(values, mapping, page)
    assert "All contract elements on this page" not in "\n".join(lines)
    assert suffixes == {"a": " [📦](a.md#evidence-gaps)"}
    mapping["a"] = ()
    assert atlas_rendering._interface_qualifications(values, mapping, page) == ([], {})


def test_machine_artifact_route_accepts_only_the_exact_companion() -> None:
    root = "riverhog-v1/index.md"
    assert atlas._reachable_atlas_documents(
        root, {root: b"[Machine](../riverhog-v1.json?raw=1)"}
    ) == {root}
    for target in (
        "../other.json?raw=1",
        "../riverhog-v1.json",
        "../riverhog-v1.json?raw=1#invented",
        "../../riverhog-v1.json?raw=1",
        "../riverhog-v1.json?raw=0",
        "../riverhog-v1.json?raw=1&other=1",
    ):
        with pytest.raises(atlas.ContractAtlasError, match="unresolved local link"):
            atlas._reachable_atlas_documents(root, {root: f"[Machine]({target})".encode()})
