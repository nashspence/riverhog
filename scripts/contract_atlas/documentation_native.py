"""Bounded source-owned prose destinations and independent native extraction."""

from __future__ import annotations

import argparse
import ast
import importlib
import inspect
from collections import Counter
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from markdown_it import MarkdownIt
from release_documentation_lib import apply_cli, markdown_text

from .documentation_markdown import native_markdown, plain_tokens, selected_content
from .model import ContractAtlasError, _encoded_json, canonical_bytes, canonical_sha256


@lru_cache(maxsize=512)
def _declarations(source: bytes) -> frozenset[str]:
    declarations: Counter[str] = Counter()

    def visit(node: ast.AST, parents: tuple[str, ...] = ()) -> None:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            parents = (*parents, node.name)
            declarations[".".join(parents)] += 1
        for child in ast.iter_child_nodes(node):
            visit(child, parents)

    visit(ast.parse(source))
    return frozenset(name for name, count in declarations.items() if count == 1)


def owned_python_declaration(value: object, root: Path | None = None) -> str | None:
    """Writable only for one real declaration in this selected source tree."""
    if not (inspect.isfunction(value) or inspect.isclass(value)):
        return None
    root = Path(__file__).resolve().parents[2] if root is None else root
    try:
        path = Path(inspect.getsourcefile(value) or "").resolve()
        relative = path.relative_to(root.resolve())
    except (TypeError, ValueError):
        return None
    if (
        not path.is_file()
        or not relative.parts
        or relative.parts[0] not in {"packages", "riverhog", "some-implementations"}
    ):
        return None
    return relative.as_posix() if value.__qualname__ in _declarations(path.read_bytes()) else None


def public_object(surface: Mapping[str, Any]) -> object:
    """Resolve the export/member already owned by Python discovery, not an author import path."""
    module = importlib.import_module(surface["module"])
    if surface["unit"] == "export":
        return getattr(module, surface["name"])
    owner = surface["owner"].removeprefix(surface["module"] + ".")
    value: Any = module
    for part in owner.split("."):
        value = getattr(value, part)
    member = inspect.getattr_static(value, surface["name"])
    if isinstance(member, (classmethod, staticmethod)):
        return member.__func__
    if isinstance(member, property):
        return member.fget
    return member


def delivery_markdown(source: str, tag: str, *, plain: bool = False) -> str:
    """Installed Markdown resolves links in its reviewed, immutable version context."""
    base = f"https://nashspence.github.io/riverhog/{tag}/riverhog-v1/"
    md = MarkdownIt("commonmark", {"html": False})
    tokens = md.parse(source)
    for token in tokens:
        for child in token.children or []:
            for attribute in ("href", "src"):
                value = child.attrGet(attribute)
                if value is not None:
                    child.attrSet(attribute, urljoin(base, str(value)))
    return (plain_tokens(tokens) if plain else native_markdown(tokens)).strip()


def native_slots(
    compiled: Mapping[str, Any], requirements: Mapping[str, Any], tag: str = "v1.0.0"
) -> dict[str, Any]:
    """Slot keys originate in typed discovery. The corpus cannot select a patch destination."""
    slots: dict[str, Any] = {"cli": {}, "openapi": {}, "python": {}, "metadata": {}, "oci": {}}
    by_element: dict[str, list[tuple[str, Any]]] = {}
    for key, row in requirements["subjects"].items():
        by_element.setdefault(row["target"]["element_id"], []).append((key, row))
    for key, row in sorted(requirements["subjects"].items()):
        entry = compiled["resolved"].get(key)
        if entry is None:
            continue
        content = selected_content(compiled, entry)
        prose = {
            "summary": entry["summary"],
            "plain": delivery_markdown(content.get("markdown", ""), tag, plain=True),
            "markdown": delivery_markdown(content.get("markdown", ""), tag),
        }
        for destination in row["destinations"]:
            kind = destination["kind"]
            if kind == "cli":
                path = " ".join(destination["command"])
                command = slots["cli"].setdefault(
                    path, {"summary": "", "plain": "", "parameters": {}}
                )
                if "parameter" in destination:
                    command["parameters"][destination["parameter"]] = prose["summary"]
                else:
                    command.update(summary=prose["summary"], plain=prose["plain"])
            elif kind == "openapi":
                slots[kind].setdefault(destination["authority"], {})[destination["pointer"]] = prose
            elif kind == "python":
                # The canonical function/class docstring also carries its named member prose.
                members = []
                for member_key, member_row in sorted(
                    by_element[row["target"]["element_id"]],
                    key=lambda item: canonical_bytes(
                        {k: v for k, v in item[1]["target"].items() if k != "element_id"}
                    ),
                ):
                    member_entry = compiled["resolved"].get(member_key)
                    member = member_row["target"].get("member")
                    if member is not None and member_entry is not None:
                        members.append(
                            member["kind"] + " " + member["key"] + ": " + member_entry["summary"]
                        )
                    elif member_row["target"].get("pointer") and member_entry is not None:
                        members.append(
                            member_row["target"]["pointer"] + ": " + member_entry["summary"]
                        )
                slots[kind][destination["identity"]] = {
                    **prose,
                    "capability": destination["capability"],
                    "docstring": prose["summary"]
                    + "\n\n"
                    + prose["plain"]
                    + ("\n\n" + "\n".join(members) if members else ""),
                    "distribution": destination["distribution"],
                    "module": destination["module"],
                }
            else:
                slots[kind][destination["name"]] = prose
    return slots


def apply_cli_prose(
    parsers: Mapping[str, Any], compiled: Mapping[str, Any], requirements: Mapping[str, Any]
) -> None:
    commands = native_slots(compiled, requirements)["cli"]
    for executable, parser in parsers.items():
        apply_cli(parser, commands, executable)


def expected_outputs(slots: Mapping[str, Any]) -> dict[str, Any]:
    """Expected editorial readouts are destination-specific, distinct from actual extraction."""
    values: dict[str, Any] = {}
    for command, slot in sorted(slots["cli"].items()):
        if slot["summary"]:
            values["cli/" + command + "/command"] = {
                "summary": " ".join(slot["summary"].split()),
                "plain": " ".join(slot["plain"].split()),
            }
        for name, summary in sorted(slot["parameters"].items()):
            values["cli/" + command + "/parameter/" + name] = " ".join(summary.split())
    for authority, fields in sorted(slots["openapi"].items()):
        for pointer, slot in sorted(fields.items()):
            values["openapi/" + authority + pointer] = markdown_text(slot["summary"]) + (
                "\n\n" + slot["markdown"] if slot["markdown"] else ""
            )
    for identity, slot in sorted(slots["python"].items()):
        values["python/" + identity] = {"capability": slot["capability"], "text": slot["docstring"]}
    for name, slot in sorted(slots["metadata"].items()):
        values["metadata/" + name] = {
            "summary": slot["summary"],
            "description": slot["markdown"] or markdown_text(slot["summary"]),
        }
    for name, slot in sorted(slots["oci"].items()):
        values["oci/" + name] = slot["summary"]
    return values


def extract_cli(parsers: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """Observe the installed native parser and full help, without the compiled expected strings."""
    observations: dict[str, Any] = {}
    readouts: dict[str, str] = {}

    def walk(parser: Any, path: tuple[str, ...]) -> None:
        name = " ".join(path)
        if isinstance(parser, argparse.ArgumentParser):
            help_text = parser.format_help()
            # Recover exact formatter-delivered command text rather than Python %-programs.
            formatter = parser._get_formatter()
            description = formatter._format_text(parser.description or "").strip()
            epilog = formatter._format_text(parser.epilog or "").strip()
            observations["cli/" + name + "/command"] = {
                "summary": " ".join(description.split()),
                "plain": " ".join(epilog.split()),
            }
            for action in parser._actions:
                if action.help is argparse.SUPPRESS:
                    continue
                if isinstance(action, argparse._SubParsersAction):
                    for child_name, child in sorted(action.choices.items()):
                        walk(child, (*path, child_name))
                elif isinstance(action.help, str):
                    observations["cli/" + name + "/parameter/" + action.dest] = " ".join(
                        formatter._expand_help(action).split()
                    )
        else:
            context = parser.make_context(name, [], resilient_parsing=True)
            help_text = parser.get_help(context)
            text = parser.help or ""
            summary, _, plain = text.partition("\n\n")
            observations["cli/" + name + "/command"] = {
                "summary": " ".join(summary.split()),
                "plain": " ".join(plain.split()),
            }
            for parameter in parser.params:
                if not getattr(parameter, "hidden", False):
                    observations["cli/" + name + "/parameter/" + parameter.name] = " ".join(
                        (getattr(parameter, "help", "") or "").split()
                    )
            for child_name, child in sorted(getattr(parser, "commands", {}).items()):
                walk(child, (*path, child_name))
        readouts[name] = help_text

    for executable, parser in sorted(parsers.items()):
        walk(parser, (executable,))
    return observations, readouts


def select_observations(expected: Mapping[str, Any], observed: Mapping[str, Any]) -> dict[str, Any]:
    return {name: observed[name] for name in expected if name in observed}


def semantic_frame(value: Any) -> dict[str, Any]:
    """Lossless native values use the same explicit exact-integer codec as Closure."""
    encoded, paths = _encoded_json(value)
    return {"value": encoded, "unsafe_integer_paths": paths}


def native_semantics(values: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "cli": semantic_frame(values["cli"]),
        "python": semantic_frame(values["python"]),
        "openapi": {name: semantic_frame(value) for name, value in values["openapi"].items()},
    }


def verify_semantic_parity(expected: object, observed: object, *, destination: str) -> None:
    if canonical_sha256(expected) != canonical_sha256(observed):
        raise ContractAtlasError(f"prepared {destination} changed source-owned semantic facts")
