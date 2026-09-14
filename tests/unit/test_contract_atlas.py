from __future__ import annotations

import hashlib
import json
import re
import sys
from collections import Counter
from functools import cache
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import contract_atlas as atlas  # noqa: E402

ARTIFACT = REPO_ROOT / "qualification/contracts/riverhog-v1.json"


@cache
def checked_atlas() -> atlas.ContractAtlas:
    """Load and validate the large checked closure once for presentation assertions."""

    return atlas.load_atlas(ARTIFACT)


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
    assert coverage["extent_decisions"] == 1976
    assert coverage["missing"] == 0
    assert coverage["multiply_represented"] == 0
    assert coverage["stale"] == 0


def test_atlas_rollups_and_dossiers_are_exact_and_descriptive() -> None:
    checked = checked_atlas()
    root = checked.root
    elements = root["elements"]
    documents = root["atlas"]["documents"]
    dossier_documents = [item for item in documents if item["kind"] == "dossier"]

    assert len(dossier_documents) == len(elements) == root["counts"]["contract_elements"]
    assert root["counts"]["extent_decisions"] == 1976
    assert sum(root["counts"]["by_authority"].values()) == len(elements)
    assert sum(root["counts"]["by_interface"].values()) == len(elements)
    assert all(item["path"].endswith(".md") for item in documents)
    assert all("/contexts/" not in item["path"] for item in documents)
    assert all("/traces/" not in item["path"] for item in documents)
    assert all(not Path(item["path"]).name.startswith("c-") for item in dossier_documents)
    assert checked.files[root["atlas"]["root"]].startswith(
        b"# Riverhog repository v1 contract audit\n"
    )
    assert hashlib.sha256(checked.files[root["atlas"]["root"]]).hexdigest() == next(
        item["sha256"] for item in documents if item["kind"] == "root-index"
    )
    # Presentation-quality witness, deliberately outside machine-closure validation.
    assert max(len(payload) for payload in checked.files.values()) <= (
        atlas.AUDIT_DOCUMENT_TARGET_BYTES
    )
    assert all(b"### Exact owned JSON" in checked.files[item["dossier"]] for item in elements)


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
        "compatibility-guarantees": 7,
        "installation-roots": 4,
        "publication-locations": 2,
        "python-distributions": 71,
        "release-artifacts": 12,
        "runtime-images": 13,
        "versioning-tags": 5,
    }
    assert len(release_elements) == 117
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


def test_http_semantics_are_owned_once_and_operation_parity_remains_exact_evidence() -> None:
    checked = checked_atlas()
    root = checked.root
    elements = root["elements"]
    records = root["trace"]["operation_qualification"]["records"]
    http_operations = [item for item in elements if item["interface"] == "http-operations"]
    qualified = {tuple(item["details"]["qualification_key"]): item for item in http_operations}

    assert len(records) == len(http_operations) == len(qualified) == 147
    assert not any(item["interface"] == "operation" for item in elements)
    assert root["counts"]["by_authority"]["riverhog"] == 364
    assert (
        sum(
            item["authority"] == "riverhog" and item["interface"] == "http-operations"
            for item in elements
        )
        == 109
    )
    assert (
        sum(
            item["authority"] == "riverhog" and item["interface"] == "http-schemas"
            for item in elements
        )
        == 253
    )
    assert len(root["projection"]["external_contract"]["http_route_supplements"]) == 2
    assert set(qualified) == {(item["application"], item["operation_id"]) for item in records}
    for record in records:
        item = qualified[(record["application"], record["operation_id"])]
        page = checked.files[item["dossier"]].decode()
        assert atlas._pretty_json(record) in page


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


def test_every_dossier_is_lossless_and_representative_contract_classes_are_semantics_first() -> (
    None
):
    checked = checked_atlas()
    elements = checked.root["elements"]
    projection = checked.root["projection"]

    for item in elements:
        page = checked.files[item["dossier"]].decode()
        assert page.index("## External contract") < page.index("## Evidence")
        assert "object (" not in page
        assert "array (" not in page
        for pointer in item["pointers"]:
            value = atlas.pointer_value(projection, pointer)
            exact = (
                f"<!-- exact-contract-value: {atlas.canonical_sha256(value)} -->\n\n"
                f"```json\n{atlas._pretty_json(value)}\n```"
            )
            assert exact in page

    representative_interfaces = {
        "http-operations",
        "cli",
        "configuration",
        "process-protocol",
        "process-protocol-operations",
        "process-protocol-schemas",
        "schema",
        "durable-state",
        "runtime-images",
    }
    representatives = {
        interface: next(item for item in elements if item["interface"] == interface)
        for interface in representative_interfaces
    }
    assert set(representatives) == representative_interfaces
    recovery = next(item for item in elements if "recovery-descriptor" in item["id"])
    for item in [*representatives.values(), recovery]:
        page = checked.files[item["dossier"]].decode()
        assert page.index("## External contract") < page.index("## Governing policies")
        assert page.index("## Governing policies") < page.index("## Evidence")


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
    assert piggity_index.index("### Executable commands") < piggity_index.index(
        "### Command groups"
    )


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
    export_line = "- [riverhog_client.ApiClient](riverhog-client-apiclient.md)"
    member_line = (
        "  - [riverhog_client.ApiClient.list_collections]"
        "(riverhog-client-apiclient-list-collections.md)"
    )
    assert export_line in index
    assert member_line in index
    assert index.index(export_line) < index.index(member_line)


def test_exact_protocol_and_python_unit_validation_fails_closed_on_drift() -> None:
    checked = checked_atlas()
    changed_elements = json.loads(json.dumps(checked.root["elements"]))
    process_operation = next(
        item for item in changed_elements if item["interface"] == "process-protocol-operations"
    )
    process_operation["interface"] = "process-protocol-schemas"
    with pytest.raises(atlas.ContractAtlasError, match="operation lacks exact ownership"):
        atlas._validate_process_protocol_units(changed_elements, checked.root["projection"])

    changed_trace = json.loads(json.dumps(checked.root["trace"]))
    protected = next(
        item
        for item in changed_trace["python_registry"]["dispositions"]
        if item["disposition"] == "protected"
    )
    changed_trace["python_registry"]["dispositions"].remove(protected)
    with pytest.raises(atlas.ContractAtlasError, match="does not protect every exact public unit"):
        atlas._validate_python_units(
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
    assert checked.root["counts"]["by_policy"]["exclusion/process-launcher-not-cli/v1"] == 13
    assert checked.root["counts"]["by_policy"]["exclusion/python-package-no-declared-api/v1"] == 22
    assert checked.root["counts"]["excluded_candidates"] == 35
    policy_page = checked.files["riverhog-v1/policies/index.md"].decode()
    assert "Applications:" in policy_page
    assert "Applications: **13**" in policy_page
    assert "Applications: **22**" in policy_page
    assert policy_page.count("- Applicability:") == len(declared)
    assert policy_page.count("- Observable result or violation:") == len(declared)
    assert policy_page.count("- Executable authorities:") == len(declared)
    assert "Implementation-correctness witnesses remain outside" in policy_page


def test_every_extent_fact_names_and_links_its_exact_subject() -> None:
    checked = checked_atlas()
    decisions = {
        item["id"]: item
        for item in checked.root["projection"]["external_contract"]["extents"]["decisions"]
    }

    for element in checked.root["elements"]:
        decision_ids = element["extent_decision_ids"]
        if not decision_ids:
            continue
        page = checked.files[element["dossier"]].decode()
        section = page.split("### Progression, limits, and lifecycle\n", 1)[1].split(
            "\n## Governing policies", 1
        )[0]
        rows = [
            line
            for line in section.splitlines()
            if line.startswith("| ")
            and not line.startswith("| Applies to ")
            and not line.startswith("|---")
        ]

        assert "| Applies to | Contract | Bounds or reason |" in section
        assert len(rows) == len(decision_ids)
        assert all("](#" in row for row in rows)
        for pointer in {decisions[identity]["source_pointer"] for identity in decision_ids}:
            anchor = atlas._subject_anchor(pointer)
            assert page.count(f'id="{anchor}"') == 1
            assert f"](#{anchor})" in section or (f'id="{anchor}"' in section and "](#" in section)

        if element["interface"].startswith("http-"):
            assert not re.search(r"\bparameter \d+\b", section)


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


def test_atlas_rejects_broken_fragments_and_duplicate_explicit_anchors() -> None:
    with pytest.raises(atlas.ContractAtlasError, match="unresolved local anchor"):
        atlas._reachable_atlas_documents(
            "index.md",
            {
                "index.md": b"[detail](detail.md#missing)\n",
                "detail.md": b"# Detail\n",
            },
        )

    with pytest.raises(atlas.ContractAtlasError, match="repeats a stable local anchor"):
        atlas._reachable_atlas_documents(
            "index.md",
            {"index.md": b'<a id="subject"></a>\n<a id="subject"></a>\n'},
        )
