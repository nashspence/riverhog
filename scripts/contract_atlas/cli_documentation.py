"""Noncontractual CLI documentation derived from executable parser objects."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from typing import Any, cast

from .human_contract import command_path
from .model import ContractAtlasError, canonical_sha256


def _prose(value: object) -> str:
    return " ".join(value.split()) if isinstance(value, str) else ""


def _argparse_commands(
    parser: argparse.ArgumentParser, path: tuple[str, ...], summary: str = ""
) -> dict[tuple[str, ...], dict[str, object]]:
    subparsers = [
        action for action in parser._actions if isinstance(action, argparse._SubParsersAction)
    ]
    if len(subparsers) > 1:
        raise ContractAtlasError(f"CLI has multiple subparser groups: {' '.join(path)}")
    child_summaries = (
        {choice.dest: _prose(choice.help) for choice in subparsers[0]._choices_actions}
        if subparsers
        else {}
    )
    children = (
        cast(Mapping[str, argparse.ArgumentParser], subparsers[0].choices) if subparsers else {}
    )
    parameters: list[dict[str, str]] = []
    formatter = parser._get_formatter()
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            continue
        if action.help is argparse.SUPPRESS:
            continue
        display = ", ".join(action.option_strings) if action.option_strings else action.dest
        metavar = (
            ""
            if action.nargs == 0
            else action.metavar or (action.dest.upper() if action.option_strings else action.dest)
        )
        help_text = _prose(formatter._expand_help(action)) if isinstance(action.help, str) else ""
        parameters.append(
            {
                "name": str(action.dest),
                "display": display,
                "metavar": str(metavar),
                "help": help_text,
            }
        )
    records: dict[tuple[str, ...], dict[str, object]] = {
        path: {
            "synopsis": _prose(parser.format_usage()),
            "description": _prose(parser.description),
            "summary": summary,
            "epilog": _prose(parser.epilog),
            "parameters": parameters,
            "subcommands": [
                {"name": name, "summary": child_summaries.get(name, "")}
                for name in sorted(children)
            ],
        }
    }
    for name, child in sorted(children.items()):
        records.update(_argparse_commands(child, (*path, name), child_summaries.get(name, "")))
    return records


def _click_commands(
    command: Any, path: tuple[str, ...], summary: str = ""
) -> dict[tuple[str, ...], dict[str, object]]:
    context = command.make_context(" ".join(path), [], resilient_parsing=True)
    children = getattr(command, "commands", None)
    children = children if isinstance(children, Mapping) else {}
    parameters: list[dict[str, str]] = []
    for parameter in command.params:
        help_record = (
            parameter.get_help_record(context) if hasattr(parameter, "get_help_record") else None
        )
        display, help_text = help_record if help_record is not None else ("", "")
        if not display:
            display = ", ".join(getattr(parameter, "opts", ())) or str(parameter.name)
        metavar = "" if getattr(parameter, "is_flag", False) else parameter.make_metavar(context)
        parameters.append(
            {
                "name": str(parameter.name),
                "display": _prose(display),
                "metavar": _prose(metavar),
                "help": _prose(help_text) or _prose(getattr(parameter, "help", None)),
            }
        )
    records: dict[tuple[str, ...], dict[str, object]] = {
        path: {
            "synopsis": _prose(command.get_usage(context)),
            "description": _prose(getattr(command, "help", None)),
            "summary": summary or _prose(getattr(command, "short_help", None)),
            "epilog": _prose(getattr(command, "epilog", None)),
            "parameters": parameters,
            "subcommands": [
                {"name": name, "summary": _prose(child.get_short_help_str())}
                for name, child in sorted(children.items())
            ],
        }
    }
    for name, child in sorted(children.items()):
        records.update(_click_commands(child, (*path, name), _prose(child.get_short_help_str())))
    return records


def build_cli_documentation_record(
    closure: Mapping[str, object], parsers: Mapping[str, object]
) -> dict[str, object]:
    """Bind parser help to every discovered CLI element, without changing Closure."""

    authorities = set(
        cast(Mapping[str, object], cast(Mapping[str, object], closure["external_contract"])["cli"])
    )
    if set(parsers) != authorities:
        raise ContractAtlasError(
            f"CLI parser/documentation authorities differ: {sorted(set(parsers) ^ authorities)}"
        )
    by_path: dict[tuple[str, ...], dict[str, object]] = {}
    for name, parser in sorted(parsers.items()):
        path = (name,)
        records = (
            _argparse_commands(parser, path)
            if isinstance(parser, argparse.ArgumentParser)
            else _click_commands(parser, path)
        )
        by_path.update(records)
    elements = cast(Sequence[Mapping[str, object]], closure["elements"])
    cli_elements = {
        command_path(cast(Sequence[str], element["pointers"])[0]): str(element["id"])
        for element in elements
        if element["interface"] == "cli"
    }
    if set(by_path) != set(cli_elements):
        missing = sorted(set(cli_elements) - set(by_path))
        extra = sorted(set(by_path) - set(cli_elements))
        raise ContractAtlasError(
            f"CLI help and Closure commands differ: missing={missing}, extra={extra}"
        )
    return {
        "format": "riverhog-contract-documentation-record/v2",
        "closure_sha256": canonical_sha256(closure),
        "release_scope": "v1",
        "build_scope": "checked-workspace",
        "source_revision": "checked-workspace",
        "explanations": [],
        "guides": [],
        "cli_commands": [
            {"element_id": cli_elements[path], **by_path[path]} for path in sorted(by_path)
        ],
    }
