"""Browser behavior of the checked v1 candidate and one documentation fixture."""

from __future__ import annotations

import copy
import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import pytest
from playwright.sync_api import Browser, sync_playwright

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from contract_atlas.html_rendering import (  # noqa: E402
    _authority_file,
    _element_file,
    _inventory_file,
    render_contract,
)
from contract_atlas.model import canonical_bytes, canonical_sha256  # noqa: E402
from contract_atlas.records import load_bundle  # noqa: E402
from contract_pages import build_pages  # noqa: E402


class _QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, _format: str, *_args: object) -> None:
        pass


@pytest.fixture(scope="module")
def candidate_site(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, str, str]:
    root = tmp_path_factory.mktemp("contract-site")
    build_pages(REPO_ROOT / "qualification/contracts", root, "0" * 40)
    bundle = load_bundle(REPO_ROOT / "qualification/contracts/riverhog-v1.json")
    element = next(
        item
        for item in bundle.closure["elements"]
        if item["title"] == "gogurt_core.GOGURT_ROUTE_PATTERN"
    )
    selected = copy.deepcopy(bundle.closure)
    selected["elements"] = [element]
    documentation = {
        "format": "riverhog-contract-documentation-record/v1",
        "closure_sha256": canonical_sha256(selected),
        "release_scope": "browser fixture",
        "build_scope": "browser fixture",
        "source_revision": "browser fixture",
        "explanations": [{"element_id": element["id"], "text": "Fixture explanation."}],
        "guides": [
            {
                "id": "fixture-guide",
                "title": "Fixture guide",
                "text": "Follow the exact declaration.",
                "subjects": [element["id"]],
            }
        ],
    }
    for relative, payload in render_contract(selected, documentation=documentation).items():
        path = root / "fixture" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    (root / "fixture/riverhog-v1.json").write_bytes(canonical_bytes(selected))
    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(_QuietHandler, directory=str(root)))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_port}"
        yield (
            base,
            _element_file(str(element["id"])),
            str(
                selected["external_contract"]["python"]["gogurt_core.GOGURT_ROUTE_PATTERN"][
                    "contract"
                ]["value"]
            ),
        )
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.fixture(scope="module")
def browser() -> Browser:
    with sync_playwright() as playwright:
        instance = playwright.chromium.launch(headless=True)
        try:
            yield instance
        finally:
            instance.close()


def test_candidate_modes_history_direct_links_and_no_js(
    candidate_site: tuple[str, str, str], browser: Browser
) -> None:
    base, element_file, literal = candidate_site
    path = f"/contract-candidate/riverhog-v1/{element_file}"
    context = browser.new_context(viewport={"width": 1280, "height": 800})
    page = context.new_page()
    page.goto(base + path)
    assert page.locator("#contract").is_visible()
    assert literal in page.locator(".human-contract").inner_text()
    page.locator("details.exact summary").click()
    assert page.locator('[data-value-pointer$="/contract/value"]').inner_text() == literal
    contract_html = page.locator("#contract").inner_html()
    assert not page.locator("#audit").is_visible()
    page.locator("#audit-mode").focus()
    page.keyboard.press("Space")
    assert page.locator("#audit-mode").is_checked()
    assert page.locator("#audit").is_visible()
    assert "audit=1" in page.url
    assert page.locator("#contract").inner_html() == contract_html
    page.locator('a[data-source-link="exact-commit"]').first.wait_for()
    assert "/blob/" + "0" * 40 + "/" in page.locator(
        'a[data-source-link="exact-commit"]'
    ).first.get_attribute("href")

    page.locator("header p").first.locator("a").nth(2).click()
    assert "/contract-candidate/riverhog-v1/i-" in page.url
    assert "audit=1" in page.url
    page.go_back()
    assert page.url.endswith(element_file + "?audit=1")
    assert page.locator("#audit").is_visible()
    page.go_back()
    assert page.url.endswith(element_file)
    assert not page.locator("#audit").is_visible()

    context.close()

    no_js = browser.new_context(java_script_enabled=False, viewport={"width": 390, "height": 800})
    page = no_js.new_page()
    page.goto(base + path)
    assert page.locator("#contract").is_visible()
    assert literal in page.locator(".human-contract").inner_text()
    page.locator("details.exact summary").click()
    assert page.locator('[data-value-pointer$="/contract/value"]').inner_text() == literal
    page.locator("header p").first.locator("a").first.click()
    assert page.url.endswith("/contract-candidate/riverhog-v1/index.html")
    assert page.get_by_role("heading", name="Riverhog v1 Contract Render").is_visible()
    assert page.locator("noscript summary").filter(has_text="Audit references").is_visible()
    no_js.close()


def test_authority_to_extent_marker_navigation(
    candidate_site: tuple[str, str, str], browser: Browser
) -> None:
    base, _unused_element_file, _literal = candidate_site
    bundle = load_bundle(REPO_ROOT / "qualification/contracts/riverhog-v1.json")
    owners = {
        pointer: element
        for element in bundle.closure["elements"]
        for pointer in element["pointers"]
    }
    witness = next(
        item
        for item in bundle.audit["trace"]["segmented_extent_witnesses"]
        if item["association_status"] == "candidate" and item["result_reference"] is None
    )
    subject = witness["subject_pointers"][0]
    owner_pointer = max(
        (pointer for pointer in owners if subject == pointer or subject.startswith(pointer + "/")),
        key=len,
    )
    element = owners[owner_pointer]
    authority = str(element["authority"])
    interface = str(element["interface"])
    context = browser.new_context(viewport={"width": 390, "height": 800})
    page = context.new_page()
    page.goto(f"{base}/contract-candidate/riverhog-v1/index.html?audit=1")
    root_marker = page.locator(f'a.audit-marker[href="{_authority_file(authority)}#audit-scope"]')
    assert root_marker.is_visible()
    page.locator(f'a[href="{_authority_file(authority)}"]').first.click()
    assert page.get_by_role("heading", name="Interfaces").is_visible()
    assert page.locator(
        f'a.audit-marker[href="{_inventory_file(authority, interface)}#audit-scope"]'
    ).is_visible()
    page.locator(f'a[href="{_inventory_file(authority, interface)}"]').first.click()
    assert page.locator(
        f'a.audit-marker[href="{_element_file(str(element["id"]))}#audit"]'
    ).is_visible()
    page.locator(f'a[href="{_element_file(str(element["id"]))}"]').first.click()
    assert page.locator("#audit").is_visible()
    assert page.get_by_role("heading", name="Open extent qualification").is_visible()
    page.locator("#audit a[href^='q-']").first.click()
    assert page.get_by_text("Association: candidate; executed result: none.").is_visible()
    direct_audit_url = page.url.split("?", 1)[0]
    page.goto(direct_audit_url)
    assert page.get_by_role("heading", name="Open extent qualification").is_visible()
    context.close()


def test_documentation_fixture_mode_keeps_the_contract_visible(
    candidate_site: tuple[str, str, str], browser: Browser
) -> None:
    base, element_file, _literal = candidate_site
    context = browser.new_context()
    page = context.new_page()
    page.goto(f"{base}/fixture/riverhog-v1/{element_file}")
    contract_html = page.locator("#contract").inner_html()
    assert page.locator("#audit-mode").count() == 0
    assert not page.locator("#documentation").is_visible()
    page.locator("#docs-mode").check()
    assert page.locator("#documentation").is_visible()
    assert "Fixture explanation." in page.locator("#documentation").inner_text()
    assert page.locator("#contract").inner_html() == contract_html
    assert "docs=1" in page.url
    context.close()


def test_selection_and_matrix_tables_use_labeled_cards_at_phone_and_desktop_width(
    candidate_site: tuple[str, str, str], browser: Browser
) -> None:
    base, _element_file_name, _literal = candidate_site
    bundle = load_bundle(REPO_ROOT / "qualification/contracts/riverhog-v1.json")
    schema = next(
        item
        for item in bundle.closure["elements"]
        if item["title"] == "schemas: AddCollectionUploadTagsRequest"
    )
    schema_without_description = next(
        item for item in bundle.closure["elements"] if item["title"] == "schemas: AppKeyCreatedOut"
    )
    context = browser.new_context(viewport={"width": 390, "height": 844})
    page = context.new_page()
    page.goto(f"{base}/contract-candidate/riverhog-v1/index.html")
    cards = page.locator(".authority-cards > article")
    assert cards.count() > 1
    assert cards.first.evaluate("node => getComputedStyle(node).borderStyle") == "solid"
    assert cards.first.locator(":scope > h3 > a").count() >= 1
    assert cards.first.locator(":scope > .authority-interfaces a").count() >= 1
    title_box = cards.first.locator("h3").bounding_box()
    description_box = cards.first.locator(".authority-description").bounding_box()
    assert title_box is not None and description_box is not None
    assert description_box["y"] >= title_box["y"] + title_box["height"]
    page.set_viewport_size({"width": 1280, "height": 844})
    assert cards.first.evaluate("node => getComputedStyle(node).borderStyle") == "solid"
    title_box = cards.first.locator("h3").bounding_box()
    description_box = cards.first.locator(".authority-description").bounding_box()
    assert title_box is not None and description_box is not None
    assert description_box["y"] >= title_box["y"] + title_box["height"]
    page.set_viewport_size({"width": 390, "height": 844})
    assert page.evaluate(
        "document.documentElement.scrollWidth <= document.documentElement.clientWidth + 2"
    )
    for path in (
        _authority_file("riverhog"),
        _inventory_file("riverhog-application-access", "python"),
        _inventory_file("riverhog", "http-schemas"),
        _inventory_file("riverhog", "http-operations"),
        _inventory_file("riverhog-catalog", "durable-state"),
        "source-authorities.html?audit=1",
        _element_file(
            str(
                next(
                    item["id"]
                    for item in bundle.closure["elements"]
                    if "/external_contract/extents/rules/bounded-segment~1v1" in item["pointers"]
                )
            )
        ),
        _element_file(str(schema["id"])),
    ):
        page.goto(f"{base}/contract-candidate/riverhog-v1/{path}")
        table = page.locator("table").first
        assert table.count() == 1, path
        assert table.evaluate("node => getComputedStyle(node).display") == "block", path
        assert (
            table.locator("tbody").first.evaluate("node => getComputedStyle(node).display")
            == "grid"
        ), path
        assert (
            table.locator("tbody tr").first.evaluate("node => getComputedStyle(node).display")
            == "block"
        ), path
        if table.locator("thead").count():
            assert table.locator("thead").first.is_hidden(), path
            assert all(
                cell.get_attribute("data-label")
                for cell in table.locator("tbody tr").first.locator("td").all()
            ), path
        assert page.evaluate(
            "document.documentElement.scrollWidth <= document.documentElement.clientWidth + 2"
        ), path
    page.set_viewport_size({"width": 1280, "height": 844})
    for path in (
        _inventory_file("riverhog", "http-operations"),
        "source-authorities.html?audit=1",
        _element_file(str(schema["id"])),
    ):
        page.goto(f"{base}/contract-candidate/riverhog-v1/{path}")
        table = page.locator("table").first
        assert table.locator("thead").first.is_hidden(), path
        assert (
            table.locator("tbody").first.evaluate("node => getComputedStyle(node).display")
            == "grid"
        ), path
        assert (
            table.locator("tbody tr").first.evaluate("node => getComputedStyle(node).display")
            == "block"
        ), path
        assert page.evaluate(
            "document.documentElement.scrollWidth <= document.documentElement.clientWidth + 2"
        ), path
    page.goto(
        f"{base}/contract-candidate/riverhog-v1/"
        + _element_file(str(schema_without_description["id"]))
    )
    assert page.locator('table td[data-label="Description"]').first.is_hidden()
    page.set_viewport_size({"width": 390, "height": 844})
    page.goto(f"{base}/contract-candidate/riverhog-v1/" + _inventory_file("a-riverhog-cli", "cli"))
    assert page.locator(".command-tree").is_visible()
    assert page.locator(".command-tree a").count() > 1
    assert page.evaluate(
        "document.documentElement.scrollWidth <= document.documentElement.clientWidth + 2"
    )
    context.close()


def test_authority_filter_dark_mode_and_accounting_navigation(
    candidate_site: tuple[str, str, str], browser: Browser
) -> None:
    base, _element_file_name, _literal = candidate_site
    context = browser.new_context(viewport={"width": 390, "height": 844}, color_scheme="dark")
    page = context.new_page()
    page.goto(f"{base}/contract-candidate/riverhog-v1/index.html")
    cards = page.locator("#authority-cards > .authority-card")
    assert cards.first.get_attribute("data-authority") == "release"
    assert page.evaluate("getComputedStyle(document.documentElement).colorScheme") == "dark"
    assert (
        page.evaluate("getComputedStyle(document.documentElement).backgroundColor")
        != "rgb(255, 255, 255)"
    )
    assert (
        page.evaluate("getComputedStyle(document.querySelector('a')).color") != "rgb(23, 78, 114)"
    )
    authority_filter = page.get_by_role("searchbox", name="Filter by authority name")
    authority_filter.fill("riverhog-catalog")
    assert page.locator("#authority-filter-count").inner_text().startswith("1 of ")
    assert page.locator("#authority-cards > .authority-card:visible").count() == 1
    authority_filter.fill("")
    assert page.locator("#authority-cards > .authority-card:visible").count() == cards.count()
    page.goto(f"{base}/contract-candidate/riverhog-v1/accounting.html?audit=1")
    assert page.get_by_role("heading", name="Accounting checks").is_visible()
    assert page.get_by_role("heading", name="Discovery anomalies").is_visible()
    assert page.get_by_role("link", name="exact bound Audit Record").is_visible()
    context.close()
