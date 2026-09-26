"""Parser help is a bound documentation view, never a Closure input."""

from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from contract_atlas.cli_documentation import build_cli_documentation_record  # noqa: E402
from contract_atlas.html_rendering import (  # noqa: E402
    _element_file,
    _inventory_file,
    render_contract,
)
from contract_atlas.model import ContractAtlasError, canonical_sha256  # noqa: E402
from contract_atlas.records import load_closure  # noqa: E402
from contract_freeze import _cli_parsers  # noqa: E402


@pytest.fixture(scope="module")
def cli_documentation() -> tuple[dict[str, object], dict[str, object]]:
    closure = load_closure(REPO_ROOT / "qualification/contracts/riverhog-v1.json")
    return closure, build_cli_documentation_record(closure, _cli_parsers())


def test_help_covers_every_discovered_command_for_click_and_argparse(
    cli_documentation: tuple[dict[str, object], dict[str, object]],
) -> None:
    closure, document = cli_documentation
    commands = document["cli_commands"]
    ids = {item["id"] for item in closure["elements"] if item["interface"] == "cli"}
    assert {item["element_id"] for item in commands} == ids
    assert document["closure_sha256"] == canonical_sha256(closure)
    roots = {
        authority: next(
            item
            for item in commands
            if item["element_id"].startswith(f"cli:{authority}:{authority}:")
        )
        for authority in ("a-riverhog-cli", "a-riverhog-ftp-spool")
    }
    for authority in ("a-riverhog-cli", "a-riverhog-ftp-spool"):
        record = roots[authority]
        assert record["synopsis"].lower().startswith("usage:")
        assert record["parameters"]
        assert record["subcommands"]
    assert roots["a-riverhog-cli"]["description"]
    assert any(item["help"] for item in roots["a-riverhog-cli"]["parameters"])
    assert any(item["help"] for item in roots["a-riverhog-ftp-spool"]["parameters"])


def test_parser_help_drift_changes_documentation_without_changing_closure(
    cli_documentation: tuple[dict[str, object], dict[str, object]],
) -> None:
    closure, original = cli_documentation
    parsers = _cli_parsers()
    parser = parsers["a-riverhog-ftp-spool"]
    parser.description = "A changed parser description."
    changed = build_cli_documentation_record(closure, parsers)
    assert changed["closure_sha256"] == original["closure_sha256"]
    assert changed["cli_commands"] != original["cli_commands"]
    assert any(
        item["description"] == "A changed parser description." for item in changed["cli_commands"]
    )


def test_cli_help_is_only_shown_in_documentation_mode_and_subjects_are_exact(
    cli_documentation: tuple[dict[str, object], dict[str, object]],
) -> None:
    closure, document = cli_documentation
    files = render_contract(closure, documentation=document)
    root = files["riverhog-v1/index.html"].decode()
    assert 'id="docs-mode"' in root
    css = files["riverhog-v1/style.css"].decode()
    assert ".audit,.documentation,.audit-cue,.docs-cue{display:none}" in css
    element = next(
        item
        for item in closure["elements"]
        if item["interface"] == "cli" and item["title"] == "a-riverhog-cli"
    )
    page = files["riverhog-v1/" + _element_file(element["id"])].decode()
    assert 'class="documentation" id="documentation"' in page
    assert "CLI help · noncontractual" in page
    assert "Command-line client for Riverhog." in page
    inventory = files["riverhog-v1/" + _inventory_file("a-riverhog-cli", "cli")].decode()
    assert 'class="docs-cue"' in inventory
    assert "Command-line client for Riverhog." not in str(closure["external_contract"]["cli"])

    stale = copy.deepcopy(document)
    stale["closure_sha256"] = "0" * 64
    with pytest.raises(ContractAtlasError, match="stale"):
        render_contract(closure, documentation=stale)
    stale = copy.deepcopy(document)
    stale["cli_commands"][0]["element_id"] = "unknown"
    with pytest.raises(ContractAtlasError, match="stale subject"):
        render_contract(closure, documentation=stale)
    stale = copy.deepcopy(document)
    stale["cli_commands"].pop()
    with pytest.raises(ContractAtlasError, match="every command"):
        render_contract(closure, documentation=stale)
