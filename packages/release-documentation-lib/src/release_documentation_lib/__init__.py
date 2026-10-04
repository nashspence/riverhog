"""Native adapters for verified, package-local release prose."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

__all__ = [
    "DocumentationError",
    "apply_cli",
    "annotate_openapi",
    "attach_openapi",
    "load_resource",
    "markdown_text",
    "prepare_typer",
]


class DocumentationError(RuntimeError):
    """A prepared package has absent, corrupt or unowned documentation."""


def markdown_text(text: str) -> str:
    """Represent a literal summary as Markdown text, without HTML or markup execution."""
    return re.sub(r"([\\`*_{}\[\]<>!#|.&+()\-])", r"\\\1", text)


def load_resource(path: str | Path, expected_sha256: str) -> dict[str, Any]:
    """Read locally sealed resource bytes. No development fallback or network lookup."""
    try:
        payload = Path(path).read_bytes()
        value = json.loads(payload)
    except (OSError, ValueError) as exc:
        raise DocumentationError("required package-local release prose is unavailable") from exc
    if (
        hashlib.sha256(payload).hexdigest() != expected_sha256
        or not isinstance(value, dict)
        or value.get("format") != "riverhog-package-documentation/v1"
    ):
        raise DocumentationError("required package-local release prose is corrupt")
    return value


def _argparse_text(text: str) -> str:
    # argparse interpolates descriptions only when the prog placeholder is present.
    return text.replace("%", "%%") if "%(prog)" in text else text


def prepare_typer(
    app: Any, commands: Mapping[str, Any], root: str, factory: Callable[[Any], Any]
) -> None:
    """Scope native command construction to one prepared application instance."""
    if not hasattr(app, "registered_commands"):
        raise DocumentationError("prepared Typer destination is absent")

    original = factory(app)
    native_class = type(original)

    def initialize(self: Any, *args: Any, **kwargs: Any) -> None:
        native_class.__init__(self, *args, **kwargs)
        apply_cli(self, commands, root)

    # Use Typer's native command-class extension point. The application and its
    # callbacks keep their identities, signatures and native exception handling.
    documented_class = type("DocumentedCommand", (native_class,), {"__init__": initialize})
    if hasattr(original, "commands"):
        app.info.cls = documented_class
        if app.registered_callback is not None:
            app.registered_callback.cls = documented_class
    else:
        app.registered_commands[0].cls = documented_class


def apply_cli(parser: Any, commands: Mapping[str, Any], root: str) -> Any:
    """Annotate native objects without changing their parsing or callback behavior."""

    def apply(node: Any, path: tuple[str, ...]) -> None:
        command = commands.get(" ".join(path))
        if isinstance(node, argparse.ArgumentParser):
            actions = {
                a.dest: a for a in node._actions if not isinstance(a, argparse._SubParsersAction)
            }
            groups = [a for a in node._actions if isinstance(a, argparse._SubParsersAction)]
            children = groups[0].choices if groups else {}
            if command:
                for name in command["parameters"]:
                    if name not in actions or actions[name].help is argparse.SUPPRESS:
                        raise DocumentationError(
                            "release prose exposes an unknown or suppressed argument"
                        )
                node.description = _argparse_text(command["summary"])
                node.epilog = _argparse_text(command.get("plain", ""))
                for name, summary in command["parameters"].items():
                    actions[name].help = summary.replace("%", "%%")
            for group in groups:
                for choice in group._choices_actions:
                    entry = commands.get(" ".join((*path, choice.dest)))
                    if entry:
                        choice.help = entry["summary"].replace("%", "%%")
        else:
            children = getattr(node, "commands", {})
            if command:
                parameters = {p.name: p for p in node.params}
                for name in command["parameters"]:
                    if name not in parameters or getattr(parameters[name], "hidden", False):
                        raise DocumentationError(
                            "release prose exposes an unknown or hidden Click input"
                        )
                node.help = command["summary"] + "\n\n" + command.get("plain", "")
                node.short_help = command["summary"]
                node.epilog = None
                if hasattr(node, "rich_markup_mode"):
                    node.rich_markup_mode = None
                for name, summary in command["parameters"].items():
                    parameters[name].help = summary
        for name, child in sorted(children.items()):
            apply(child, (*path, name))

    apply(parser, (root,))
    return parser


def _pointer(document: dict[str, Any], pointer: str) -> Any:
    value: Any = document
    for component in pointer.split("/")[1:]:
        component = component.replace("~1", "/").replace("~0", "~")
        value = value[int(component)] if isinstance(value, list) else value[component]
    return value


def annotate_openapi(document: dict[str, Any], slots: Mapping[str, Any]) -> dict[str, Any]:
    """Source-generated owned annotation destinations, never an authored JSON patch."""
    result = copy.deepcopy(document)
    for pointer, entry in sorted(slots.items()):
        try:
            node = _pointer(result, pointer)
        except (KeyError, IndexError, ValueError) as exc:
            raise DocumentationError("prepared OpenAPI destination is absent") from exc
        if not isinstance(node, dict):
            raise DocumentationError("prepared OpenAPI destination is not an annotation owner")
        if pointer.startswith("/paths/") and pointer.count("/") == 3:
            node["summary"] = entry["summary"]
        node["description"] = markdown_text(entry["summary"]) + (
            "\n\n" + entry["markdown"] if entry.get("markdown") else ""
        )
    return result


def attach_openapi(app: Any, slots: Mapping[str, Any]) -> Any:
    """Keep ASGI routes and behavior native; annotate the actual served schema lazily."""
    native = app.openapi

    def documented_openapi() -> dict[str, Any]:
        return annotate_openapi(native(), slots)

    app.openapi = documented_openapi
    return app
