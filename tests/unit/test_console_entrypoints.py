from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from a_riverhog_cli.main import app as a_riverhog_cli_app
from a_riverhog_ftp_spool.app import build_parser as build_adapter_parser
from a_stove0_cli.main import app as stove0_app
from typer.main import get_command

LIFECYCLE_EVENT_LIST_COMMANDS = (
    ("a-riverhog-cli", "event", "list", "--help"),
    ("stove0", "event", "list", "--help"),
)

PAGED_LIST_COMMANDS = (
    ("a-riverhog-cli", "collection", "list", "--help"),
    ("a-riverhog-cli", "collection", "upload", "list", "--help"),
    ("a-riverhog-cli", "collection", "provenance", "list", "--help"),
    ("a-riverhog-cli", "find", "--help"),
    ("a-riverhog-cli", "archive", "copy-job", "list", "--help"),
    ("a-riverhog-cli", "archive", "store", "list", "--help"),
    ("a-riverhog-cli", "retrieval", "cache", "list", "--help"),
    ("a-riverhog-cli", "app", "list", "--help"),
    ("a-riverhog-cli", "app", "key", "list", "--help"),
    ("a-riverhog-cli", "app", "key", "access", "list", "--help"),
    ("a-riverhog-cli", "app", "key", "quota", "list", "--help"),
    ("a-riverhog-cli", "local", "list", "--help"),
    ("stove0", "work", "list", "--help"),
    ("stove0", "evaluation", "list", "--help"),
    ("stove0", "admission", "list", "--help"),
)

BOUNDED_LIST_COMMANDS = (
    ("a-riverhog-cli", "collection", "tag", "list", "--help"),
    ("a-riverhog-cli", "local", "provenance-observer", "list", "--help"),
    ("stove0", "recipe", "list", "--help"),
    ("stove0", "admission", "policy", "list", "--help"),
    ("stove0", "departure", "policy", "list", "--help"),
)

SIMPLE_PAGED_LIST_COMMANDS = (("stove0", "departure", "list", "--help"),)

QUERY_PAGED_LIST_COMMANDS = (("a-riverhog-cli", "tag", "list", "--help"),)


def _run_help(command: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update({"COLUMNS": "240", "NO_COLOR": "1", "TERM": "dumb"})
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        env=environment,
        text=True,
        timeout=10,
    )


@pytest.fixture(scope="module")
def checked_console_closure() -> dict[str, Any]:
    return json.loads(
        (
            Path(__file__).resolve().parents[2] / "qualification/contracts/riverhog-v1.json"
        ).read_text(encoding="utf-8")
    )


@pytest.fixture(scope="module")
def published_console_scripts(checked_console_closure: dict[str, Any]) -> dict[str, str]:
    closure = checked_console_closure
    components = closure["boundaries"]["components"]
    published = {
        name: component["distribution"]
        for component in components
        for name in component["console_scripts"]
    }
    assert set(published) == set(closure["external_contract"]["cli"])
    return published


def test_published_console_entrypoint_help_is_side_effect_free(
    published_console_scripts: dict[str, str], tmp_path: Path
) -> None:
    for command in sorted(published_console_scripts):
        executable = Path(sys.executable).parent / command
        assert executable.is_file(), f"published console script is not installed: {command}"
        home = tmp_path / command
        home.mkdir()
        environment = os.environ.copy()
        environment.update(
            {
                "HOME": str(home),
                "XDG_CONFIG_HOME": str(home),
                "XDG_CACHE_HOME": str(home),
                "COLUMNS": "240",
                "NO_COLOR": "1",
                "TERM": "dumb",
            }
        )
        completed = subprocess.run(
            [str(executable), "--help"],
            cwd=home,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert completed.returncode == 0, (command, completed.stderr)
        assert "usage" in completed.stdout.casefold(), command
        assert not list(home.iterdir()), f"{command} --help wrote into its home or cwd"


def test_published_console_entrypoint_reports_installed_version(
    published_console_scripts: dict[str, str],
    checked_console_closure: dict[str, Any],
) -> None:
    versioned = {
        name
        for name, root in checked_console_closure["external_contract"]["cli"].items()
        if any(control["id"] == "version" for control in root["terminating_controls"])
    }
    for command, distribution in sorted(published_console_scripts.items()):
        if command not in versioned:
            continue
        executable = Path(sys.executable).parent / command
        assert executable.is_file(), f"published console script is not installed: {command}"
        completed = subprocess.run(
            [str(executable), "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert completed.returncode == 0, (command, completed.stderr)
        assert completed.stdout.strip() == importlib.metadata.version(distribution)


@pytest.mark.parametrize(
    "command",
    LIFECYCLE_EVENT_LIST_COMMANDS,
)
def test_lifecycle_event_cli_help_uses_the_shared_contract(command: tuple[str, ...]) -> None:
    completed = _run_help(command)

    assert completed.returncode == 0, completed.stderr
    for option in ("--after", "--limit"):
        assert option in completed.stdout
    if command[0] == "a-riverhog-cli":
        assert "--json" in completed.stdout


@pytest.mark.parametrize(
    "command",
    PAGED_LIST_COMMANDS,
)
def test_paged_list_cli_help_uses_the_shared_contract(command: tuple[str, ...]) -> None:
    completed = _run_help(command)

    assert completed.returncode == 0, completed.stderr
    for option in ("--page-size", "--page-token", "--sort", "--order", "--query"):
        assert option in completed.stdout
    if command[0] == "a-riverhog-cli":
        assert "--json" in completed.stdout


@pytest.mark.parametrize("command", BOUNDED_LIST_COMMANDS)
def test_bounded_list_cli_help_uses_the_shared_output_contract(command: tuple[str, ...]) -> None:
    completed = _run_help(command)

    assert completed.returncode == 0, completed.stderr
    if command[0] == "a-riverhog-cli":
        for option in ("--ids", "--json"):
            assert option in completed.stdout


@pytest.mark.parametrize("command", QUERY_PAGED_LIST_COMMANDS)
def test_query_paged_list_cli_help_uses_its_exact_contract(command: tuple[str, ...]) -> None:
    completed = _run_help(command)

    assert completed.returncode == 0, completed.stderr
    for option in ("--page-size", "--page-token", "--query", "--ids", "--json"):
        assert option in completed.stdout


@pytest.mark.parametrize("command", SIMPLE_PAGED_LIST_COMMANDS)
def test_simple_paged_list_cli_help_uses_its_exact_contract(command: tuple[str, ...]) -> None:
    completed = _run_help(command)
    assert completed.returncode == 0, completed.stderr
    for option in ("--page-size", "--page-token"):
        assert option in completed.stdout


def test_stove0_declares_its_shared_json_projection_once_at_the_root() -> None:
    completed = _run_help(("stove0", "--help"))

    assert completed.returncode == 0, completed.stderr
    assert "--json" in completed.stdout


def test_retrieval_cache_list_emits_actionable_composite_selectors() -> None:
    completed = _run_help(("a-riverhog-cli", "retrieval", "cache", "list", "--help"))

    assert completed.returncode == 0, completed.stderr
    assert "--selectors" in completed.stdout
    for option in (
        "--collection",
        "--source-store",
        "--state",
        "--protection",
        "--expires-before",
        "--expires-after",
    ):
        assert option in completed.stdout


def _typer_list_commands(command: Any, prefix: tuple[str, ...]) -> Iterator[tuple[str, ...]]:
    for name, child in getattr(command, "commands", {}).items():
        path = (*prefix, str(name))
        if name == "list":
            yield path
        yield from _typer_list_commands(child, path)


def _argparse_list_commands(
    parser: argparse.ArgumentParser,
    prefix: tuple[str, ...],
) -> Iterator[tuple[str, ...]]:
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        for name, child in action.choices.items():
            path = (*prefix, name)
            if name == "list":
                yield path
            yield from _argparse_list_commands(child, path)


def test_every_official_list_command_has_one_declared_convention() -> None:
    discovered = {
        *_typer_list_commands(get_command(a_riverhog_cli_app), ("a-riverhog-cli",)),
        *_typer_list_commands(get_command(stove0_app), ("stove0",)),
        *_argparse_list_commands(build_adapter_parser(), ("a-riverhog-ftp-spool",)),
    }
    classified = {
        command[:-1]
        for command in (
            *LIFECYCLE_EVENT_LIST_COMMANDS,
            *PAGED_LIST_COMMANDS,
            *BOUNDED_LIST_COMMANDS,
            *SIMPLE_PAGED_LIST_COMMANDS,
            *QUERY_PAGED_LIST_COMMANDS,
        )
        if command[-2] == "list"
    }

    assert discovered == classified
