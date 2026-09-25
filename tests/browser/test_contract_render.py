"""Browser behavior of the checked v1 candidate and one documentation fixture."""

from __future__ import annotations

import copy
import json
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

from contract_atlas.html_rendering import _element_file, render_contract  # noqa: E402
from contract_atlas.model import canonical_sha256  # noqa: E402
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
    (root / "fixture/riverhog-v1.json").write_text(json.dumps(selected), encoding="utf-8")
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


def test_candidate_modes_history_direct_links_zoom_and_no_js(
    candidate_site: tuple[str, str, str], browser: Browser
) -> None:
    base, element_file, literal = candidate_site
    path = f"/contract-candidate/riverhog-v1/{element_file}"
    context = browser.new_context(viewport={"width": 1280, "height": 800})
    page = context.new_page()
    page.goto(base + path)
    assert page.locator("#contract").is_visible()
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

    page.locator("header a").nth(1).click()
    assert "/contract-candidate/riverhog-v1/i-" in page.url
    assert "audit=1" in page.url
    page.go_back()
    assert page.url.endswith(element_file + "?audit=1")
    assert page.locator("#audit").is_visible()
    page.go_back()
    assert page.url.endswith(element_file)
    assert not page.locator("#audit").is_visible()

    page.set_viewport_size({"width": 640, "height": 800})
    page.evaluate("document.documentElement.style.zoom = '2'")
    assert page.evaluate(
        "document.documentElement.scrollWidth <= document.documentElement.clientWidth + 2"
    )
    context.close()

    no_js = browser.new_context(java_script_enabled=False, viewport={"width": 640, "height": 800})
    page = no_js.new_page()
    page.goto(base + path)
    assert page.locator("#contract").is_visible()
    assert page.locator('[data-value-pointer$="/contract/value"]').inner_text() == literal
    page.locator("header a").first.click()
    assert page.url.endswith("/contract-candidate/riverhog-v1/index.html")
    assert page.get_by_role("heading", name="Riverhog v1 Contract Render").is_visible()
    no_js.close()


def test_documentation_fixture_mode_keeps_the_contract_visible(
    candidate_site: tuple[str, str, str], browser: Browser
) -> None:
    base, element_file, _literal = candidate_site
    context = browser.new_context()
    page = context.new_page()
    page.goto(f"{base}/fixture/riverhog-v1/{element_file}")
    contract_html = page.locator("#contract").inner_html()
    assert page.locator("#audit-mode").is_disabled()
    assert not page.locator("#documentation").is_visible()
    page.locator("#docs-mode").check()
    assert page.locator("#documentation").is_visible()
    assert "Fixture explanation." in page.locator("#documentation").inner_text()
    assert page.locator("#contract").inner_html() == contract_html
    assert "docs=1" in page.url
    context.close()
