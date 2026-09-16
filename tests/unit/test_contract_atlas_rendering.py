from __future__ import annotations

import hashlib
import inspect
import re
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, cast
from urllib.parse import unquote

import piggity.main
import pytest
from riverhog_api.routers import collections as collection_routes
from riverhog_client import ApiClient

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import contract_atlas as atlas  # noqa: E402
from contract_atlas import discovery, navigation  # noqa: E402

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


def test_durable_state_and_python_structures_are_exact_human_audit_units() -> None:
    checked = checked_atlas()
    projection = checked.root["projection"]
    elements = checked.root["elements"]
    state_owners = projection["external_contract"]["durable_state"]["owners"]
    state_elements = [item for item in elements if item["interface"] == "durable-state"]
    expected_units = 0
    for owner in state_owners:
        structure = owner["structure"]
        collections = [value for value in structure.values() if isinstance(value, list)]
        expected_units += 1 + sum(len(value) for value in collections)
    assert len(state_elements) == expected_units
    assert all("state_unit" in item["details"] for item in state_elements)

    collections = next(
        item
        for item in state_elements
        if item["authority"] == "riverhog-catalog" and item["title"].endswith(": collections")
    )
    collections_page = checked.files[collections["dossier"]].decode()
    assert "### Columns" in collections_page
    assert "### Table constraints" in collections_page
    assert "`description_search`" in collections_page
    assert "`ck_collections_archive_root_sha256`" in collections_page

    pydantic_element = next(
        item
        for item in elements
        if item["interface"] == "python"
        and "schema" in atlas.pointer_value(projection, item["pointers"][0])["contract"]
    )
    pydantic_page = checked.files[pydantic_element["dossier"]].decode()
    assert "#### Validated model schema" in pydantic_page
    assert "### Fields" in pydantic_page

    sources_page = checked.files["riverhog-v1/evidence/sources.md"].decode()
    assert "tests/fixtures/state/v1_0001/riverhog.postgresql.sql" in sources_page


def test_http_semantics_are_owned_once_and_operation_bindings_remain_structural() -> None:
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
        assert "### Structural operation bindings" in page
        assert "not executed qualification" in page


def test_collection_list_audit_exposes_scoped_tests_and_unestablished_progression() -> None:
    checked = checked_atlas()
    operation = next(
        item
        for item in checked.root["elements"]
        if item["interface"] == "http-operations"
        and item["authority"] == "riverhog"
        and item["details"]["operation_id"] == "list_collections"
    )
    page = checked.files[operation["dossier"]].decode()
    assert "### Progression evidence and open obligations" in page
    assert "riverhog-read-collection-progression/v1" in page
    sources = checked.files["riverhog-v1/evidence/sources.md"].decode()
    assert "no executed qualification result or CI attestation" in sources
    assert "Shared token codec" in sources
    assert "does not prove each route supplies those bindings correctly" in sources
    assert "uses a fake client and does not establish general output parity" in sources


def test_collection_list_has_a_readable_mutual_http_client_cli_audit_path() -> None:
    checked = checked_atlas()
    selected = [
        next(item for item in checked.root["elements"] if item["title"] == title)
        for title in (
            "GET /v1/collections",
            "riverhog_client.ApiClient.list_collections",
            "piggity collection list",
        )
    ]
    ids = {item["id"] for item in selected}
    for item, implementation in zip(
        selected,
        (
            collection_routes.list_collections,
            ApiClient.list_collections,
            piggity.main.collection_list_cmd,
        ),
        strict=True,
    ):
        assert ids - {item["id"]} <= set(item["related_element_ids"])
        page = checked.files[item["dossier"]].decode()
        source_links = {
            (
                (REPO_ROOT / "qualification/contracts" / item["dossier"])
                .parent.joinpath(unquote(target))
                .resolve(),
                int(line),
            )
            for target, line in re.findall(r"\]\(([^)]+)#L(\d+)\)", page)
        }
        definition = inspect.unwrap(implementation)
        assert (
            Path(inspect.getsourcefile(definition)),
            inspect.getsourcelines(definition)[1],
        ) in source_links
        for other in selected:
            if other is not item:
                assert f"]({navigation._relative_link(item['dossier'], other['dossier'])})" in page

    reading_path = (
        checked.files[selected[0]["dossier"]].decode().split("## Maintained corroboration", 1)[0]
    )
    parameter = next(line for line in reading_path.splitlines() if "`page_size` | query" in line)
    assert "| no | `25` |" in parameter
    assert "minimum=1; maximum=100" in parameter
    response = next(line for line in reading_path.splitlines() if "`200` |" in line)
    assert "application/json" in response
    assert (
        "[ListCollectionsResponse](../http-schemas/schemas-listcollectionsresponse.md)" in response
    )
    for status, code in (
        (400, "bad_request"),
        (401, "unauthorized"),
        (403, "forbidden"),
        (500, "internal_error"),
    ):
        response = next(line for line in reading_path.splitlines() if f"`{status}` |" in line)
        assert "[ErrorResponse](../http-schemas/schemas-errorresponse.md)" in response
        assert f"`{code}`" in response


@pytest.mark.parametrize("invalid_binding", ("missing-member", "export", "no-binding"))
def test_operation_client_binding_requires_a_python_member(invalid_binding: str) -> None:
    checked = checked_atlas()
    elements = deepcopy(checked.root["elements"])
    trace = deepcopy(checked.root["trace"])
    record = next(
        item
        for item in trace["operation_qualification"]["records"]
        if item["application"] == "riverhog" and item["operation_id"] == "list_collections"
    )
    if invalid_binding == "no-binding":
        record["client_bindings"] = []
    else:
        record["client_bindings"][0]["public_identity"] = (
            "example.Client.inherited_method"
            if invalid_binding == "missing-member"
            else "riverhog_client.ApiClient"
        )

    with pytest.raises(atlas.ContractAtlasError, match="lacks a Python"):
        discovery._link_operation_qualification(elements, trace)


def test_inherited_client_method_routes_to_http_and_its_actual_definition() -> None:
    checked = checked_atlas()
    selected = [
        next(item for item in checked.root["elements"] if item["title"] == title)
        for title in (
            "GET /v1/collection-processing-claims",
            "riverhog_client.ApiClient.list_processing_claims",
        )
    ]
    http, client = selected
    assert client["details"]["unit"] == "member"
    assert client["details"]["owner"] == "riverhog_client.ApiClient"
    for item, other in ((http, client), (client, http)):
        assert other["id"] in item["related_element_ids"]
        page = checked.files[item["dossier"]].decode()
        assert f"]({navigation._relative_link(item['dossier'], other['dossier'])})" in page

    definition = inspect.unwrap(ApiClient.list_processing_claims)
    location = {
        "path": Path(inspect.getsourcefile(definition)).relative_to(REPO_ROOT).as_posix(),
        "line": inspect.getsourcelines(definition)[1],
    }
    label = f"{location['path']}::{definition.__qualname__}"
    assert navigation._repository_source_link(client["dossier"], location, label) in (
        checked.files[client["dossier"]].decode()
    )


@pytest.mark.parametrize(
    "target",
    ("../../../tests/example.py#L8", "../../../tests/other.py#L7", "../../../tests/example.py"),
)
def test_repository_links_accept_only_trace_recorded_file_and_line(target: str) -> None:
    root = "riverhog-v1/index.md"
    allowed = {"../../tests/example.py#L7"}
    assert atlas._reachable_atlas_documents(
        root, {root: b"[test](../../../tests/example.py#L7)\n"}, repository_sources=allowed
    ) == {root}
    with pytest.raises(atlas.ContractAtlasError, match="unresolved local link"):
        atlas._reachable_atlas_documents(
            root, {root: f"[test]({target})\n".encode()}, repository_sources=allowed
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


def test_cli_authority_resolution_fails_closed() -> None:
    checked = checked_atlas()
    projection = checked.root["projection"]
    elements = {item["id"]: item for item in checked.root["elements"]}
    command = next(
        item
        for item in elements.values()
        if item["interface"] == "cli" and item["title"] == "piggity collection list"
    )
    result_pointer = next(
        pointer for pointer in command["pointers"] if pointer.endswith("/result_contract")
    )
    result = atlas.pointer_value(projection, result_pointer)
    authority = result["success"][0]["stdout"]["json"]
    kwargs = {
        "element": command,
        "pointer": f"{result_pointer}/success/0/stdout",
        "path": command["dossier"],
        "projection": projection,
        "elements_by_id": elements,
    }

    with pytest.raises(atlas.ContractAtlasError, match="no atlas resolver"):
        atlas._cli_authority_reference({"kind": "unknown"}, **kwargs)

    inapplicable = {**authority, "method": "POST"}
    with pytest.raises(atlas.ContractAtlasError, match="resolves to 0"):
        atlas._cli_authority_reference(inapplicable, **kwargs)

    target = next(
        item
        for item in elements.values()
        if item["interface"] == "http-operations"
        and item.get("details", {}).get("operation_id") == "list_collections"
    )
    ambiguous = {**elements, "duplicate-for-test": {**target, "id": "duplicate-for-test"}}
    with pytest.raises(atlas.ContractAtlasError, match="resolves to 2"):
        atlas._cli_authority_reference(authority, **{**kwargs, "elements_by_id": ambiguous})


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
