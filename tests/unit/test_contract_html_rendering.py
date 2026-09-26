from __future__ import annotations

import copy
import re
import sys
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, cast

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from contract_atlas.html_rendering import (  # noqa: E402
    _authority_file,
    _element_file,
    _hash,
    _inventory_file,
    contract_body,
    render_contract,
    validate_render,
)
from contract_atlas.model import ContractAtlasError, canonical_bytes, canonical_sha256  # noqa: E402
from contract_atlas.records import load_bundle  # noqa: E402


class VisibleFacts(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.code_pointer: str | None = None
        self.row_id: str | None = None
        self.codes: dict[str, str] = {}
        self.rows: dict[str, str] = {}
        self.row_links: dict[str, list[str]] = {}
        self.start_tags: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.start_tags.append(tag)
        values = dict(attrs)
        if tag == "code" and values.get("data-value-pointer") is not None:
            self.code_pointer = values["data-value-pointer"]
            assert self.code_pointer is not None
            self.codes[self.code_pointer] = ""
        if tag == "tr" and values.get("data-element") is not None:
            self.row_id = values["data-element"]
            assert self.row_id is not None
            self.rows[self.row_id] = ""
            self.row_links[self.row_id] = []
        if tag == "a" and self.row_id is not None and values.get("href"):
            self.row_links[self.row_id].append(str(values["href"]))

    def handle_data(self, data: str) -> None:
        if self.code_pointer is not None:
            self.codes[self.code_pointer] += data
        if self.row_id is not None:
            self.rows[self.row_id] += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "code":
            self.code_pointer = None
        if tag == "tr":
            self.row_id = None


@pytest.fixture(scope="module")
def rendered_candidate() -> tuple[dict[str, object], dict[str, object], dict[str, bytes]]:
    bundle = load_bundle(REPO_ROOT / "qualification/contracts/riverhog-v1.json")
    closure, audit = bundle.closure, bundle.audit
    return closure, audit, render_contract(closure, audit)


def test_whole_candidate_html_has_reachable_pages_and_exact_literal(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, _audit, files = rendered_candidate
    validate_render(files)
    elements = cast(list[dict[str, object]], closure["elements"])
    assert len([path for path in files if path.endswith(".html")]) >= len(elements)

    element = next(item for item in elements if item["title"] == "gogurt_core.GOGURT_ROUTE_PATTERN")
    pointer = cast(list[str], element["pointers"])[0] + "/contract/value"
    page = VisibleFacts()
    page.feed(files["riverhog-v1/" + _element_file(str(element["id"]))].decode())
    expected = cast(dict[str, Any], closure["external_contract"])["python"][
        "gogurt_core.GOGURT_ROUTE_PATTERN"
    ]["contract"]["value"]
    assert page.codes[pointer] == expected
    assert "&#40;" not in page.codes[pointer]


def test_authority_map_and_known_human_interface_families(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, _audit, files = rendered_candidate
    elements = cast(list[dict[str, object]], closure["elements"])
    root = files["riverhog-v1/index.html"].decode()
    authorities = {str(element["authority"]) for element in elements}
    interfaces = {(str(element["authority"]), str(element["interface"])) for element in elements}
    assert '<div id="authority-cards" class="authority-cards">' in root
    assert root.count('<article class="authority-card" data-authority=') == len(authorities)
    assert root.index('data-authority="release"') < root.index('data-authority="riverhog"')
    assert 'id="authority-filter"' in root
    assert "Generated v1 candidate; no freeze or qualification" not in root
    assert "Is this exactly the external contract" not in root
    for authority in authorities:
        assert root.count(f'href="{_authority_file(authority)}"') == 1
    for authority, interface in interfaces:
        assert f'href="{_inventory_file(authority, interface)}"' in root

    def human(element: dict[str, object]) -> str:
        page = files["riverhog-v1/" + _element_file(str(element["id"]))].decode()
        return page.split('<div class="human-contract">', 1)[1].split('<details class="exact">', 1)[
            0
        ]

    samples = {
        "http-operations": ("<h3>Operation</h3>", "Parameters", "Responses"),
        "http-schemas": ("Schema", "Fields", "Required"),
        "cli": ("<h3>Command</h3>", "Arguments and options", "Result contract"),
        "python": ("Python declaration", "kind"),
        "durable-state": ("Table:", "Column", "Nullable"),
        "process-protocol-operations": ("Process protocol operation", "Request", "Response"),
        "process-protocol-schemas": ("Schema", "Fields"),
        "configuration": ("Schema", "Fields"),
        "configuration-environment": ("Environment setting", "Default expressions"),
        "compatibility-guarantees": ('class="promise"',),
        "extent": ('class="promise"',),
        "runtime-images": ("Publication and compatibility facts", "platforms"),
        "publication-policies": ('class="promise"',),
    }
    for interface, phrases in samples.items():
        candidates = [item for item in elements if item["interface"] == interface]
        matching = next(
            (item for item in candidates if all(phrase in human(item) for phrase in phrases)), None
        )
        assert matching is not None, interface
    nested_command = next(
        item for item in elements if item["title"] == "a-riverhog-cli app key create"
    )
    assert "--allow" in human(nested_command)
    assert "Success outcomes" in human(nested_command)
    tree = files["riverhog-v1/" + _inventory_file(str(nested_command["authority"]), "cli")].decode()
    assert 'class="command-tree"' in tree
    assert ">create</a>" in tree


def test_extent_marker_routes_through_every_selection_scope(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, audit, files = rendered_candidate
    elements = cast(list[dict[str, object]], closure["elements"])
    owners = {
        pointer: element for element in elements for pointer in cast(list[str], element["pointers"])
    }
    witness = next(
        item
        for item in cast(dict[str, Any], audit["trace"])["segmented_extent_witnesses"]
        if item["association_status"] == "candidate" and item["result_reference"] is None
    )
    subject = str(witness["subject_pointers"][0])
    pointer = max(
        (item for item in owners if subject == item or subject.startswith(item + "/")),
        key=len,
    )
    element = owners[pointer]
    authority = str(element["authority"])
    interface = str(element["interface"])
    identity = str(element["id"])
    root = files["riverhog-v1/index.html"].decode()
    authority_page = files["riverhog-v1/" + _authority_file(authority)].decode()
    interface_page = files["riverhog-v1/" + _inventory_file(authority, interface)].decode()
    element_page = files["riverhog-v1/" + _element_file(identity)].decode()
    assert f'href="{_authority_file(authority)}#audit-scope"' in root
    assert f'href="{_inventory_file(authority, interface)}#audit-scope"' in authority_page
    assert f'href="{_element_file(identity)}#audit"' in interface_page
    assert "Open extent qualification" in element_page
    assert 'class="audit-marker"' in interface_page
    assert "📦 Extent:" in files["riverhog-v1/audit-key.html"].decode()


def test_broad_policy_applications_retain_element_and_authority_routes(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, audit, files = rendered_candidate
    policy = "compatibility/python-api/v1"
    overlays = cast(list[dict[str, object]], audit["element_overlays"])
    chosen_overlay = next(item for item in overlays if policy in item["policy_ids"])
    chosen = next(
        item
        for item in cast(list[dict[str, object]], closure["elements"])
        if item["id"] == chosen_overlay["id"]
    )
    root = f"p-{_hash(policy)}.html"
    child = f"p-{_hash(policy)}-a-{_hash(str(chosen['authority']))}.html"
    assert f'href="{child}"' in files["riverhog-v1/" + root].decode()
    assert f'href="{_element_file(str(chosen["id"]))}"' in files["riverhog-v1/" + child].decode()


def test_policy_definitions_have_one_authority_page_and_keep_audit_application_routes(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, audit, files = rendered_candidate
    root = files["riverhog-v1/index.html"].decode()
    assert "policies.html" not in files
    assert "Governing policies" not in root
    assert root.count(">Exact Contract Closure</a>") == 1
    elements = cast(list[dict[str, object]], closure["elements"])
    owners = {
        pointer: element for element in elements for pointer in cast(list[str], element["pointers"])
    }
    for records in cast(dict[str, list[dict[str, object]]], audit["policies"]).values():
        for record in records:
            pointer = str(record["definition_pointer"])
            owner = owners[pointer]
            element_path = _element_file(str(owner["id"]))
            assert "riverhog-v1/" + element_path in files
            application_path = f"p-{_hash(str(record['id']))}.html"
            assert f'href="{element_path}"' in files["riverhog-v1/" + application_path].decode()
            assert f'href="{application_path}"' in files["riverhog-v1/" + element_path].decode()
    release_policies = [
        item
        for item in elements
        if item["authority"] == "release" and item["interface"] == "publication-policies"
    ]
    assert len(release_policies) == 3
    release_index = files[
        "riverhog-v1/" + _inventory_file("release", "publication-policies")
    ].decode()
    for item in release_policies:
        assert f'href="{_element_file(str(item["id"]))}"' in release_index


def test_accounting_summarizes_discovery_and_routes_exact_records_to_audit_json(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    _closure, audit, files = rendered_candidate
    accounting = files["riverhog-v1/accounting.html"]
    assert len(accounting) < 35_000
    assert not any(path.startswith("riverhog-v1/accounting-") for path in files)
    page = accounting.decode()
    for heading in ("Discovery anomalies", "Projection coverage", "Protected channels"):
        assert heading in page
    for key in cast(dict[str, object], audit["discovery"])["anomalies"]:
        assert key.replace("_", " ") in page
    assert 'href="../riverhog-v1-audit.json"' in page
    assert "Record 1" not in page


def test_audit_references_use_one_qualification_route(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    _closure, _audit, files = rendered_candidate
    root = files["riverhog-v1/index.html"].decode()
    sources = files["riverhog-v1/sources.html"].decode()
    assert 'href="sources.html">Sources and qualifications</a>' in root
    assert 'href="qualifications.html"' not in root
    assert 'href="qualifications.html">Recorded qualifications</a>' in sources


def test_interface_lists_use_local_element_names_without_repeated_prefixes(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, _audit, files = rendered_candidate
    for element in cast(list[dict[str, object]], closure["elements"]):
        if element["authority"] != "release":
            continue
        title = str(element["title"])
        if ": " not in title:
            continue
        local_name = title.split(": ", 1)[1]
        inventory = files[
            "riverhog-v1/" + _inventory_file(str(element["authority"]), str(element["interface"]))
        ].decode()
        assert f">{local_name}</a>" in inventory
        assert f">{title}</a>" not in inventory
    scheme = next(
        element
        for element in cast(list[dict[str, object]], closure["elements"])
        if element["title"] == "securitySchemes: HTTPBearer"
    )
    inventory = files[
        "riverhog-v1/" + _inventory_file(str(scheme["authority"]), str(scheme["interface"]))
    ].decode()
    assert ">HTTPBearer</a>" in inventory
    assert ">securitySchemes: HTTPBearer</a>" not in inventory


def test_selection_inventories_use_local_names_and_skip_empty_shape_filler(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    _closure, audit, files = rendered_candidate
    python_page = files[
        "riverhog-v1/" + _inventory_file("riverhog-application-access", "python")
    ].decode()
    schema_page = files["riverhog-v1/" + _inventory_file("riverhog", "http-schemas")].decode()
    durable_page = files[
        "riverhog-v1/" + _inventory_file("a-riverhog-cli-local", "durable-state")
    ].decode()
    cli_page = files["riverhog-v1/" + _inventory_file("a-riverhog-cli", "cli")].decode()
    environment_page = files[
        "riverhog-v1/" + _inventory_file("a-gogurt-linux-listener", "configuration-environment")
    ].decode()
    assert ">ApplicationAccess</a>" in python_page
    assert ">as_access</a>" in python_page
    assert ">AppListOut</a>" in schema_page
    assert ">desired_collections</a>" in durable_page
    assert ">0 fields<" not in schema_page
    assert ">1 fields<" not in schema_page
    assert ">0 constraints<" not in durable_page
    assert " parameters</span>" not in cli_page
    assert "environment-string" not in environment_page
    assert (
        audit["presentation"]["extent_marker"]["meaning"]
        in files["riverhog-v1/audit-key.html"].decode()
    )


def test_root_audit_and_documentation_references_coexist_without_empty_chrome(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, audit, files = rendered_candidate
    root = files["riverhog-v1/index.html"].decode()
    assert "Audit references" in root
    assert "Documentation references" not in root
    assert 'id="docs-mode"' not in root
    assert "<noscript>" in root

    selected = cast(list[dict[str, object]], closure["elements"])[0]
    documentation = {
        "format": "riverhog-contract-documentation-record/v1",
        "closure_sha256": canonical_sha256(closure),
        "release_scope": "unit fixture",
        "build_scope": "unit fixture",
        "source_revision": "unit fixture",
        "explanations": [],
        "guides": [
            {
                "id": "one-guide",
                "title": "One guide",
                "text": "Fixture guidance.",
                "subjects": [selected["id"]],
            }
        ],
    }
    combined = render_contract(closure, audit, documentation)
    validate_render(combined)
    combined_root = combined["riverhog-v1/index.html"].decode()
    assert "Audit references" in combined_root
    assert "Documentation references" in combined_root
    assert 'id="audit-mode"' in combined_root
    assert 'id="docs-mode"' in combined_root
    assert "Fixture guidance." not in combined_root


def test_process_protocol_context_routes_to_its_existing_semantic_interfaces(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, _audit, files = rendered_candidate
    boundaries = cast(dict[str, Any], closure["boundaries"])
    protocol = boundaries["process_extensions"][0]
    path = f"extension-{_hash('process-protocol:' + protocol['name'])}.html"
    page = files["riverhog-v1/" + path].decode()
    assert "Semantic interfaces" in page
    for authority, interface in (
        (protocol["contract_owner"], "python"),
        (protocol["binding_support"], "process-protocol"),
        (protocol["binding_support"], "process-protocol-operations"),
        (protocol["binding_support"], "process-protocol-schemas"),
    ):
        assert f'href="{_inventory_file(authority, interface)}"' in page
    assert "Supplied implementations" in page
    assert f'href="{path}"' in files["riverhog-v1/relationships.html"].decode()


def test_every_declared_leaf_value_uses_jcs_without_omission_or_mutation(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, _audit, files = rendered_candidate

    def leaves(value: object, pointer: str) -> dict[str, str]:
        if isinstance(value, dict) and value:
            return {
                key: visible
                for name, child in value.items()
                for key, visible in leaves(
                    child, pointer + "/" + str(name).replace("~", "~0").replace("/", "~1")
                ).items()
            }
        if isinstance(value, list) and value:
            return {
                key: visible
                for index, child in enumerate(value)
                for key, visible in leaves(child, pointer + f"/{index}").items()
            }
        if isinstance(value, str):
            return {
                pointer: canonical_bytes(value).decode("utf-8")
                if any(ord(char) < 32 for char in value)
                else value
            }
        return {pointer: canonical_bytes(value).decode("utf-8")}

    for element in cast(list[dict[str, object]], closure["elements"]):
        page = VisibleFacts()
        page.feed(files["riverhog-v1/" + _element_file(str(element["id"]))].decode())
        expected: dict[str, str] = {}
        for pointer in cast(list[str], element["pointers"]):
            current: Any = closure
            for part in pointer.split("/")[1:]:
                key = part.replace("~1", "/").replace("~0", "~")
                current = current[int(key)] if isinstance(current, list) else current[key]
            expected.update(leaves(current, pointer))
        observed = {
            pointer: visible
            for pointer, visible in page.codes.items()
            if pointer.startswith("/external_contract/")
        }
        assert observed == expected, element["id"]


def test_python_kinds_and_compatibility_promises_are_visible_in_inventories(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, _audit, files = rendered_candidate
    elements = cast(list[dict[str, object]], closure["elements"])
    python_page = VisibleFacts()
    python_page.feed(
        files["riverhog-v1/" + _inventory_file("riverhog-application-access", "python")].decode()
    )
    expected_kinds = {
        "riverhog_application_access.ApplicationAccess": "class",
        "riverhog_application_access.MonthlyDownloadQuotaBytes": "type-alias",
        "riverhog_application_access.access_covers": "function",
    }
    python_values = cast(dict[str, Any], closure["external_contract"])["python"]
    for public_name, kind in expected_kinds.items():
        element = next(
            item
            for item in elements
            if item["pointers"] == [f"/external_contract/python/{public_name}"]
        )
        identity = str(element["id"])
        assert python_values[public_name]["contract"]["kind"] == kind
        assert kind in python_page.rows[identity]
        assert _element_file(identity) in python_page.row_links[identity]

    compatibility_page = VisibleFacts()
    compatibility_page.feed(
        files["riverhog-v1/" + _inventory_file("release", "compatibility-guarantees")].decode()
    )
    promises = cast(dict[str, Any], closure["external_contract"])["release"]["compatibility"]
    assert len(promises) == 9
    for element in elements:
        if element["interface"] != "compatibility-guarantees":
            continue
        identity = str(element["id"])
        pointer = cast(list[str], element["pointers"])[0]
        key = pointer.rsplit("/", 1)[-1]
        assert promises[key] in compatibility_page.rows[identity]
        assert _element_file(identity) in compatibility_page.row_links[identity]


def test_inventory_comparison_facts_follow_changed_owned_values(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, _audit, _files = rendered_candidate
    changed = copy.deepcopy(closure)
    elements = cast(list[dict[str, object]], changed["elements"])
    selected = [
        element
        for element in elements
        if element["title"]
        in {"riverhog_application_access.ApplicationAccess", "Compatibility: cli"}
    ]
    assert len(selected) == 2
    for element in selected:
        element["related_element_ids"] = []
    changed["elements"] = selected
    cast(dict[str, Any], changed["external_contract"])["python"][
        "riverhog_application_access.ApplicationAccess"
    ]["contract"]["kind"] = "changed-kind"
    cast(dict[str, Any], changed["external_contract"])["release"]["compatibility"]["cli"] = (
        "Changed compatibility promise in fixture."
    )
    files = render_contract(changed)
    validate_render(files)
    python_page = VisibleFacts()
    python_page.feed(
        files["riverhog-v1/" + _inventory_file("riverhog-application-access", "python")].decode()
    )
    compatibility_page = VisibleFacts()
    compatibility_page.feed(
        files["riverhog-v1/" + _inventory_file("release", "compatibility-guarantees")].decode()
    )
    python_id = next(
        str(element["id"])
        for element in selected
        if element["title"] == "riverhog_application_access.ApplicationAccess"
    )
    compatibility_id = next(
        str(element["id"]) for element in selected if element["title"] == "Compatibility: cli"
    )
    assert "changed-kind" in python_page.rows[python_id]
    assert "Changed compatibility promise in fixture." in compatibility_page.rows[compatibility_id]


def test_documentation_examples_bind_exact_subjects_without_changing_contract_body(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, _audit, _files = rendered_candidate
    chosen = next(
        item
        for item in cast(list[dict[str, object]], closure["elements"])
        if item["title"] == "gogurt_core.GOGURT_ROUTE_PATTERN"
    )
    selected = copy.deepcopy(closure)
    selected["elements"] = [chosen]
    explanation = "This example links the exact pattern; it adds no new rule."
    guide = "Read the pattern, then check its owning source and qualification evidence."
    documentation = {
        "format": "riverhog-contract-documentation-record/v1",
        "closure_sha256": canonical_sha256(selected),
        "release_scope": "v1 candidate fixture",
        "build_scope": "unit fixture",
        "source_revision": "unit fixture",
        "explanations": [{"element_id": chosen["id"], "text": explanation}],
        "guides": [
            {
                "id": "read-one-pattern",
                "title": "Read one pattern",
                "text": guide,
                "subjects": [chosen["id"]],
            }
        ],
    }
    files = render_contract(selected, documentation=documentation)
    validate_render(files)
    page = files["riverhog-v1/" + _element_file(str(chosen["id"]))].decode()
    assert re.sub(r">\s+<", "><", contract_body(selected, chosen)) in re.sub(r">\s+<", "><", page)
    assert explanation in page
    assert 'id="audit-mode"' not in page
    assert 'id="docs-mode" type="checkbox"' in page
    guide_page = next(
        payload.decode()
        for path, payload in files.items()
        if path.endswith(".html") and "/g-" in path
    )
    assert guide in guide_page
    assert _element_file(str(chosen["id"])) in guide_page

    stale = copy.deepcopy(documentation)
    stale["closure_sha256"] = "0" * 64
    with pytest.raises(ContractAtlasError, match="stale"):
        render_contract(selected, documentation=stale)


def test_html_literal_escaping_keeps_visible_changed_value(
    rendered_candidate: tuple[dict[str, object], dict[str, object], dict[str, bytes]],
) -> None:
    closure, _audit, _files = rendered_candidate
    changed = copy.deepcopy(closure)
    element = next(
        item
        for item in cast(list[dict[str, object]], changed["elements"])
        if item["title"] == "gogurt_core.GOGURT_ROUTE_PATTERN"
    )
    pattern = '<script>](?:"quoted" & more)</script>'
    cast(dict[str, Any], changed["external_contract"])["python"][
        "gogurt_core.GOGURT_ROUTE_PATTERN"
    ]["contract"]["value"] = pattern
    body = contract_body(changed, element)
    visible = VisibleFacts()
    visible.feed(body)
    pointer = cast(list[str], element["pointers"])[0] + "/contract/value"
    assert visible.codes[pointer] == pattern
    assert "script" not in visible.start_tags

    changed_pattern = pattern + "\r\n"
    cast(dict[str, Any], changed["external_contract"])["python"][
        "gogurt_core.GOGURT_ROUTE_PATTERN"
    ]["contract"]["value"] = changed_pattern
    escaped = VisibleFacts()
    escaped.feed(contract_body(changed, element))
    assert escaped.codes[pointer] == canonical_bytes(changed_pattern).decode("utf-8")
