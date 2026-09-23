from __future__ import annotations

import json
from pathlib import Path

from a_riverhog_cli.main import app
from typer.testing import CliRunner

RUNNER = CliRunner()


def test_local_provenance_observer_introspection_has_human_json_parity() -> None:
    result = RUNNER.invoke(app, ["local", "provenance-observer", "list", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["format"] == "riverhog-provenance-observer-provider-list/v1"
    names = {item["name"] for item in payload["providers"]}
    assert {
        "a-riverhog-linux-provenance-observer",
        "a-riverhog-macos-provenance-observer",
        "a-riverhog-windows-provenance-observer",
    } <= names
    human = RUNNER.invoke(app, ["local", "provenance-observer", "list"])
    assert human.exit_code == 0
    assert "provenance observers:" in human.stdout
    assert "a-riverhog-linux-provenance-observer" in human.stdout


def test_local_provenance_observer_show_reports_exact_contract_identity() -> None:
    result = RUNNER.invoke(
        app,
        ["local", "provenance-observer", "show", "a-riverhog-linux-provenance-observer", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["observer_id"] == "a-riverhog-linux-provenance-observer/v1"
    assert payload["contract_id"] == "riverhog-provenance-linux-observation/v1"
    assert len(payload["contract_sha256"]) == 64
    assert payload["schema_ids"]
    human = RUNNER.invoke(
        app,
        ["local", "provenance-observer", "show", "a-riverhog-linux-provenance-observer"],
    )
    assert human.exit_code == 0
    assert payload["contract_sha256"] in human.stdout


def test_upload_requires_explicit_capture_composition_before_opening_session(
    tmp_path: Path,
) -> None:
    root = tmp_path / "collection"
    root.mkdir()
    (root / "payload.bin").write_bytes(b"payload")

    result = RUNNER.invoke(app, ["collection", "upload", "start", str(root)])

    assert result.exit_code != 0
    assert "provenance capture requires" in result.output


def test_upload_dry_run_reports_the_exact_selected_provider_in_both_outputs(
    tmp_path: Path,
) -> None:
    root = tmp_path / "collection"
    root.mkdir()
    (root / "payload.bin").write_bytes(b"payload")
    arguments = [
        "collection",
        "upload",
        "start",
        str(root),
        "--provenance-observer",
        "a-riverhog-linux-provenance-observer",
        "--dry-run",
    ]

    result = RUNNER.invoke(app, [*arguments, "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["provenance_observer"]["name"] == "a-riverhog-linux-provenance-observer"
    human = RUNNER.invoke(app, arguments)
    assert human.exit_code == 0
    assert "provenance observer: a-riverhog-linux-provenance-observer" in human.stdout
    assert payload["provenance_observer"]["contract_sha256"] in human.stdout


def test_upload_provider_environment_selection_is_connected(
    monkeypatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "collection"
    root.mkdir()
    (root / "payload.bin").write_bytes(b"payload")
    monkeypatch.setenv("A_RIVERHOG_CLI_PROVENANCE_OBSERVER", "a-riverhog-linux-provenance-observer")

    result = RUNNER.invoke(
        app,
        ["collection", "upload", "start", str(root), "--dry-run", "--json"],
    )

    assert result.exit_code == 0
    assert (
        json.loads(result.stdout)["provenance_observer"]["name"]
        == "a-riverhog-linux-provenance-observer"
    )
