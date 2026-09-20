#!/usr/bin/env python3
"""Check or render non-binding guidance; never execute its related tests."""

from __future__ import annotations

import argparse
import ast
import re
import sys
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SOURCE = Path("guidance/heuristics.toml")
OUTPUT = Path("guidance/README.md")
SCHEMA = "riverhog-nonbinding-guidance/v1"
ID = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\\Z")
NOTICE = (
    "These are guiding heuristics only: non-binding and non-contractual. "
    "Recording, adopting, using, testing, or rendering a heuristic does not create, "
    "extend, interpret, override, or relax an external contract. Contract meaning "
    "comes only from independently designated contract authorities.\n\n"
    "Only deliberately adopted, currently useful guidance belongs on reviewed main. "
    "Draft copies and inferred candidates are not adoption. A heuristic may inform "
    "a design discussion, including one about future contracts; any contract change "
    "is a separate decision through its own authority. Departures from guidance need "
    "no waiver. Independent contracts, checks, and repository requirements still apply.\n\n"
    "Related tests are contextual links, not evidence of adoption, complete coverage, "
    "or a passing run. Their independent roles are unchanged. An empty list means "
    "no related test is recorded, not that guidance is invalid."
)


class GuidanceError(ValueError):
    """The register or its references are malformed or stale."""


@dataclass(frozen=True)
class RelatedTest:
    node: str
    note: str


@dataclass(frozen=True)
class Heuristic:
    id: str
    guidance: str
    scope: str
    rationale: str
    related_tests: tuple[RelatedTest, ...]


def _fields(value: object, expected: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise GuidanceError(f"{label}: expected exactly {sorted(expected)}")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GuidanceError(f"{label}: expected nonempty text")
    return value.strip()


def _list(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise GuidanceError(f"{label}: expected a list")
    return value


def test_parts(node: str) -> tuple[PurePosixPath, list[str]]:
    """Accept static, unparameterized pytest function or class-method references."""
    path_text, *symbols = node.split("::")
    path = PurePosixPath(path_text)
    if (
        not symbols
        or path.is_absolute()
        or ".." in path.parts
        or path.as_posix() != path_text
        or "\\" in path_text
        or ":" in path_text
        or path.suffix != ".py"
        or not path.name.startswith("test_")
        or not all(symbol.isidentifier() for symbol in symbols)
        or not symbols[-1].startswith("test_")
    ):
        raise GuidanceError(f"invalid base test node: {node}")
    return path, symbols


def parse(text: str) -> tuple[Heuristic, ...]:
    """The format has no field capable of assigning contractual status."""
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise GuidanceError(str(exc)) from exc
    root = _fields(data, {"schema", "heuristics"}, "register")
    if root["schema"] != SCHEMA:
        raise GuidanceError(f"schema must be {SCHEMA!r}")
    result = []
    ids: set[str] = set()
    for raw in _list(root["heuristics"], "heuristics"):
        record = _fields(
            raw, {"id", "guidance", "scope", "rationale", "related_tests"}, "heuristic"
        )
        identity = _text(record["id"], "id")
        if not ID.fullmatch(identity) or identity in ids:
            raise GuidanceError(f"invalid or duplicate heuristic id: {identity}")
        ids.add(identity)
        tests = []
        nodes: set[str] = set()
        for raw_test in _list(record["related_tests"], f"{identity}.related_tests"):
            test = _fields(raw_test, {"node", "note"}, f"{identity}.related_test")
            node = _text(test["node"], "node")
            test_parts(node)
            if node in nodes:
                raise GuidanceError(f"{identity}: duplicate test reference: {node}")
            nodes.add(node)
            tests.append(RelatedTest(node, _text(test["note"], "note")))
        result.append(
            Heuristic(
                identity,
                _text(record["guidance"], f"{identity}.guidance"),
                _text(record["scope"], f"{identity}.scope"),
                _text(record["rationale"], f"{identity}.rationale"),
                tuple(tests),
            )
        )
    return tuple(result)


def check_references(records: Sequence[Heuristic], root: Path) -> None:
    """Check declared symbols without importing files, collecting, or running tests."""
    root = root.resolve()
    trees: dict[Path, ast.Module] = {}
    for record in records:
        for test in record.related_tests:
            relative, symbols = test_parts(test.node)
            path = (root / relative).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise GuidanceError(f"{record.id}: missing or out-of-root test: {test.node}")
            if path not in trees:
                try:
                    trees[path] = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
                except (OSError, UnicodeError, SyntaxError) as exc:
                    raise GuidanceError(f"cannot parse related test: {test.node}: {exc}") from exc
            body = trees[path].body
            for index, name in enumerate(symbols):
                matches = [item for item in body if getattr(item, "name", None) == name]
                final = index == len(symbols) - 1
                if len(matches) != 1:
                    raise GuidanceError(f"{record.id}: missing or ambiguous test: {test.node}")
                match = matches[0]
                if final:
                    if not isinstance(match, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        raise GuidanceError(f"not a test function: {test.node}")
                elif isinstance(match, ast.ClassDef) and name.startswith("Test"):
                    body = match.body
                else:
                    raise GuidanceError(f"not a test class: {test.node}")


def render(records: Sequence[Heuristic]) -> str:
    """Render one plain page; no authority graph, coverage score, or execution state."""
    lines = [
        "# Guiding heuristics — non-binding",
        "",
        "Generated from [heuristics.toml](heuristics.toml); edit that register, not this page.",
        "",
        NOTICE,
        "",
        "Additions and changes are reviewed as guidance only. Remove retired entries; Git keeps "
        "their history. `make guidance` checks bookkeeping, not adherence to the guidance.",
        "",
    ]
    for record in sorted(records, key=lambda item: item.id):
        lines += [
            f"## {record.id} (non-binding)",
            "",
            record.guidance,
            "",
            f"Scope: {record.scope}",
            "",
            f"Rationale: {record.rationale}",
            "",
            "Related tests (context only):",
            "",
        ]
        if not record.related_tests:
            lines += ["No related test is recorded.", ""]
        for test in record.related_tests:
            path, _ = test_parts(test.node)
            # Links deliberately omit line numbers: moving a test need not churn the page.
            lines.append(f"- [`{test.node}`](../{path.as_posix()}) — {test.note}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "update"))
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    try:
        root = args.root.resolve()
        source, output = root / SOURCE, root / OUTPUT
        if source.resolve() != source or output.resolve() != output:
            raise GuidanceError("register and output paths must not be redirected by symlinks")
        records = parse(source.read_text(encoding="utf-8"))
        check_references(records, root)
        expected = render(records)
        if args.command == "update":
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(expected, encoding="utf-8")
        elif not output.is_file() or output.read_text(encoding="utf-8") != expected:
            raise GuidanceError("rendering is stale; run `make guidance-update` and review")
    except (GuidanceError, OSError, UnicodeError) as exc:
        print(f"guidance metadata check failed: {exc}", file=sys.stderr)
        return 2
    print(f"guidance metadata {args.command}: {len(records)} records; no related tests executed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
