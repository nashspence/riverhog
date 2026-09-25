from __future__ import annotations

import copy
import json
import sys
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, cast

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from contract_atlas.html_rendering import (  # noqa: E402
    _element_file,
    _inventory_file,
    contract_body,
    render_contract,
    validate_render,
)
from contract_atlas.model import ContractAtlasError, canonical_sha256  # noqa: E402
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


def test_every_declared_leaf_value_is_visible_without_omission_or_mutation(
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
                pointer: json.dumps(value, ensure_ascii=False)
                if any(ord(char) < 32 for char in value)
                else value
            }
        return {
            pointer: json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        }

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
    assert contract_body(selected, chosen).replace("><", ">\n<") in page
    assert explanation in page
    assert 'id="audit-mode" type="checkbox" disabled' in page
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
    assert escaped.codes[pointer] == json.dumps(changed_pattern, ensure_ascii=False)
