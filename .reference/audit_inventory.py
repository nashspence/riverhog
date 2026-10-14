"""Read-only reference inventory. Does not import or execute repository modules.

This captures source facts, not semantic classifications or passing conformance.
Production integration must reuse the maintained discovery/collection machinery.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import tomllib
from typing import Any

SCOPE = Path("some-implementations/stove0")


def load_toml(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def require_exact_coverage(discovered: set[str], exercised: list[str]) -> None:
    """A test membership check, not a declaration of runtime or product semantics."""
    if not discovered:
        raise ValueError("empty discovery cannot establish coverage")
    if len(exercised) != len(set(exercised)):
        raise ValueError("duplicate coverage subjects")
    missing, stale = discovered - set(exercised), set(exercised) - discovered
    if missing or stale:
        raise ValueError(f"coverage mismatch: missing={sorted(missing)} stale={sorted(stale)}")


def source_facts(path: Path, root: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text, filename=str(path))
    values: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, str):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        values.setdefault(target.id, set()).add(node.value.value)
    constants = {name: next(iter(items)) for name, items in values.items() if len(items) == 1}

    def literal(node: ast.AST) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            return constants.get(node.id)
        if isinstance(node, ast.JoinedStr):
            pieces: list[str] = []
            for item in node.values:
                if isinstance(item, ast.FormattedValue):
                    if item.conversion != -1 or item.format_spec is not None:
                        return None
                    value = literal(item.value)
                else:
                    value = literal(item)
                if value is None:
                    return None
                pieces.append(value)
            return "".join(pieces)
        return None

    arguments, environment, imports = [], [], []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append("." * node.level + (node.module or ""))
        elif isinstance(node, ast.Call):
            name = ast.unparse(node.func)
            if isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
                names = [literal(item) for item in node.args]
                arguments.append({"line": node.lineno, "names": names,
                                  "expression": ast.unparse(node)})
            if name in {"os.getenv", "os.environ.get"} and node.args:
                environment.append({"line": node.lineno, "name": literal(node.args[0]),
                                    "expression": ast.unparse(node)})
    return {"path": path.relative_to(root).as_posix(),
            "sha256": hashlib.sha256(text.encode()).hexdigest(),
            "arguments": sorted(arguments, key=lambda item: item["line"]),
            "environment": sorted(environment, key=lambda item: item["line"]),
            "imports": sorted(set(imports))}


def inventory(root: Path) -> dict[str, Any]:
    root = root.resolve()
    workspace = load_toml(root / "pyproject.toml")["tool"]["uv"]["workspace"]
    members: set[Path] = set()
    for pattern in workspace["members"]:
        for member in root.glob(pattern):
            if any(member.match(exclude) for exclude in workspace.get("exclude", [])):
                continue
            project = member / "pyproject.toml"
            if not project.is_file():
                raise ValueError(f"workspace member lacks pyproject.toml: {member}")
            members.add(project)
    scoped = {p for p in members if p.is_relative_to(root / SCOPE)}
    physical = set((root / SCOPE).rglob("pyproject.toml"))
    if scoped != physical:
        raise ValueError("scoped workspace and physical project inventory differ")
    release = load_toml(root / "release.toml")
    release_roles: dict[str, list[str]] = {}
    for role, paths in release["python"].items():
        for path in paths:
            if Path(path).is_relative_to(SCOPE):
                release_roles.setdefault(path, []).append(role)
    discovered_paths = {p.parent.relative_to(root).as_posix() for p in scoped}
    require_exact_coverage(discovered_paths, list(release_roles))
    if any(len(roles) != 1 for roles in release_roles.values()):
        raise ValueError("a scoped distribution has multiple release roles")
    projects: list[dict[str, Any]] = []
    names: set[str] = set()
    commands: set[str] = set()
    for path in sorted(scoped):
        project = load_toml(path)["project"]
        name = project["name"]
        if name in names:
            raise ValueError(f"duplicate distribution: {name}")
        names.add(name)
        rel = path.parent.relative_to(root).as_posix()
        entrypoints = []
        for command, target in sorted(project.get("scripts", {}).items()):
            if command in commands:
                raise ValueError(f"duplicate executable: {command}")
            commands.add(command)
            module, separator, symbol = target.partition(":")
            if not separator or not symbol:
                raise ValueError(f"unresolved executable target: {target}")
            module_path = path.parent / "src" / Path(*module.split("."))
            candidates = [module_path.with_suffix(".py"), module_path / "__init__.py"]
            found = [candidate for candidate in candidates if candidate.is_file()]
            if len(found) != 1:
                raise ValueError(f"unresolved executable source: {target}")
            facts = source_facts(found[0], root)
            entrypoints.append({"command": command, "target": target, "source": facts})
        projects.append({"distribution": name, "path": rel,
                         "release_role": release_roles[rel][0],
                         "scripts": entrypoints,
                         "entry_point_groups": project.get("entry-points", {}),
                         "dependencies": sorted(project.get("dependencies", []))})
    images = {name: value for name, value in release["images"]["runtime"].items()
              if set(value["distributions"]) & names}
    return {"kind": "reference-source-observations-not-conformance",
            "scope": SCOPE.as_posix(), "project_count": len(projects),
            "executable_count": len(commands), "image_count": len(images),
            "projects": projects, "images": images,
            "limitations": ["Source syntax is not behavior or complete call-graph discovery.",
                            "Role semantics require reviewed contracts and runtime boundaries.",
                            "This reference helper is not a production discovery registry."]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = inventory(args.root)
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
