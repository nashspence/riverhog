from __future__ import annotations

import argparse
import importlib.metadata
import os
import subprocess
from collections.abc import Iterator
from typing import Any

import pytest
from piggity.main import app as piggity_app
from riverhog_ftp_adapter.app import build_parser as build_adapter_parser
from stove0_cli.main import app as stove0_app
from typer.main import get_command

CONSOLE_DISTRIBUTIONS = {
    "piggity": "piggity",
    "riverhog-api": "riverhog-server",
    "riverhog-recover": "riverhog-recover",
    "riverhog-ftp-adapter": "riverhog-ftp-adapter",
    "stove0": "stove0-client",
    "stove0-server": "stove0-server",
    "stove0-exiftool-observer": "stove0-exiftool-observer",
    "stove0-ffprobe-sampling-observer": "stove0-ffprobe-sampling-observer",
    "stove0-nvenc-av1-opus-target": "stove0-nvenc-av1-opus-target",
    "stove0-nvenc-av1-opus-review-sampler": "stove0-nvenc-av1-opus-review-sampler",
    "stove0-opus-target": "stove0-opus-target",
    "stove0-opus-review-sampler": "stove0-opus-review-sampler",
    "stove0-review-materialize-target": "stove0-review-materialize-target",
    "stove0-review-rclone-effect-target": "stove0-review-rclone-effect-target",
    "stove0-review-sampler-conformance": "stove0-review-sampler-support",
    "stove0-review-sampler-schemas": "stove0-review-sampler-support",
    "gogurt": "gogurt",
    "mango-fish": "mango-fish",
}

LIFECYCLE_EVENT_LIST_COMMANDS = (
    ("piggity", "event", "list", "--help"),
    ("stove0", "event", "list", "--help"),
)

PAGED_LIST_COMMANDS = (
    ("piggity", "collection", "list", "--help"),
    ("piggity", "collection", "upload", "list", "--help"),
    ("piggity", "collection", "provenance", "list", "--help"),
    ("piggity", "find", "--help"),
    ("piggity", "archive", "copy", "list", "--help"),
    ("piggity", "archive", "store", "list", "--help"),
    ("piggity", "retrieval", "cache", "list", "--help"),
    ("piggity", "app", "list", "--help"),
    ("piggity", "app", "key", "list", "--help"),
    ("piggity", "app", "key", "access", "list", "--help"),
    ("piggity", "app", "key", "quota", "list", "--help"),
    ("piggity", "local", "list", "--help"),
    ("stove0", "work", "list", "--help"),
    ("stove0", "evaluation", "list", "--help"),
    ("stove0", "admission", "list", "--help"),
)

BOUNDED_LIST_COMMANDS = (
    ("piggity", "collection", "tag", "list", "--help"),
    ("piggity", "local", "provenance-observer", "list", "--help"),
    ("stove0", "recipe", "list", "--help"),
    ("stove0", "admission", "policy", "list", "--help"),
)

QUERY_PAGED_LIST_COMMANDS = (("piggity", "tag", "list", "--help"),)


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


@pytest.mark.parametrize("command", CONSOLE_DISTRIBUTIONS)
def test_published_console_entrypoint_help_is_side_effect_free(command: str) -> None:
    completed = _run_help((command, "--help"))

    assert completed.returncode == 0, completed.stderr
    assert "usage" in completed.stdout.casefold()


@pytest.mark.parametrize("command,distribution", CONSOLE_DISTRIBUTIONS.items())
def test_published_console_entrypoint_reports_installed_version(
    command: str,
    distribution: str,
) -> None:
    completed = subprocess.run(
        [command, "--version"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr
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
    if command[0] == "piggity":
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
    if command[0] == "piggity":
        assert "--json" in completed.stdout


@pytest.mark.parametrize("command", BOUNDED_LIST_COMMANDS)
def test_bounded_list_cli_help_uses_the_shared_output_contract(command: tuple[str, ...]) -> None:
    completed = _run_help(command)

    assert completed.returncode == 0, completed.stderr
    if command[0] == "piggity":
        for option in ("--ids", "--json"):
            assert option in completed.stdout


@pytest.mark.parametrize("command", QUERY_PAGED_LIST_COMMANDS)
def test_query_paged_list_cli_help_uses_its_exact_contract(command: tuple[str, ...]) -> None:
    completed = _run_help(command)

    assert completed.returncode == 0, completed.stderr
    for option in ("--page-size", "--page-token", "--query", "--ids", "--json"):
        assert option in completed.stdout


def test_stove0_declares_its_shared_json_projection_once_at_the_root() -> None:
    completed = _run_help(("stove0", "--help"))

    assert completed.returncode == 0, completed.stderr
    assert "--json" in completed.stdout


def test_retrieval_cache_list_emits_actionable_composite_selectors() -> None:
    completed = _run_help(("piggity", "retrieval", "cache", "list", "--help"))

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
        *_typer_list_commands(get_command(piggity_app), ("piggity",)),
        *_typer_list_commands(get_command(stove0_app), ("stove0",)),
        *_argparse_list_commands(build_adapter_parser(), ("riverhog-ftp-adapter",)),
    }
    classified = {
        command[:-1]
        for command in (
            *LIFECYCLE_EVENT_LIST_COMMANDS,
            *PAGED_LIST_COMMANDS,
            *BOUNDED_LIST_COMMANDS,
            *QUERY_PAGED_LIST_COMMANDS,
        )
        if command[-2] == "list"
    }

    assert discovered == classified
