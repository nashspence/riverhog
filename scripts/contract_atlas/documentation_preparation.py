"""Fixed native adapters staged before package backends; no authored source patch language."""

from __future__ import annotations

import ast
import hashlib
import importlib
import inspect
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from release_documentation_lib import markdown_text

from .documentation_native import native_slots, owned_python_declaration, public_object
from .model import ContractAtlasError, canonical_bytes, canonical_sha256, pointer_value

PREPARATION_FORMAT = "riverhog-documentation-source-preparation/v1"
PREPARATION_FILE = ".release-documentation.json"


def _location(root: Path, value: Any) -> str | None:
    try:
        path = Path(inspect.getsourcefile(value) or "").resolve()
        relative = path.relative_to(root.resolve())
    except (TypeError, ValueError):
        return None
    if path.is_file() and relative.parts[0] in {"packages", "riverhog", "some-implementations"}:
        return relative.as_posix()
    return None


def preparation_plan(
    root: Path,
    closure: Mapping[str, Any],
    compiled: Mapping[str, Any],
    requirements: Mapping[str, Any],
    tag: str,
) -> dict[str, Any]:
    """Resolve only discovery-owned objects/factories and their exact source declarations."""
    import contract_freeze
    import operation_qualification
    import release

    slots = native_slots(compiled, requirements, tag)
    modules: dict[str, Any] = {}

    def module(path: str) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            modules.setdefault(
                path,
                {
                    "source_sha256": hashlib.sha256((root / path).read_bytes()).hexdigest(),
                    "python": {},
                    "references": {},
                    "cli": {},
                    "openapi": {},
                    "typer": {},
                },
            ),
        )

    for element in closure["elements"]:
        if element["interface"] != "python" or element["title"] not in slots["python"]:
            continue
        slot = slots["python"][element["title"]]
        surface = cast(Mapping[str, Any], pointer_value(closure, element["pointers"][0]))
        value: Any = public_object(surface)
        location = owned_python_declaration(value, root)
        writable = location is not None
        slot["capability"] = "docstring" if writable else "reference"
        if writable:
            assert location is not None
            qualified = value.__qualname__
            previous = module(location)["python"].get(qualified)
            if previous is not None and previous["docstring"] != slot["docstring"]:
                raise ContractAtlasError(
                    f"identical native Python object has conflicting release prose: "
                    f"{location}:{qualified}"
                )
            module(location)["python"][qualified] = slot
        else:
            owning_module = importlib.import_module(surface["module"])
            module_path = _location(root, owning_module)
            if module_path is None:
                raise ContractAtlasError(
                    "public Python reference has no owned package resource location"
                )
            module(module_path)["references"][element["title"]] = slot
    for executable, module_name in contract_freeze.CLI_MODULES.items():
        native = importlib.import_module(module_name)
        path = _location(root, native)
        if path is None:
            raise ContractAtlasError("CLI source does not belong to the selected repository")
        commands = {
            name: value
            for name, value in slots["cli"].items()
            if name == executable or name.startswith(executable + " ")
        }
        if not commands:
            continue
        if hasattr(native, "app") and hasattr(native.app, "registered_commands"):
            module(path)["typer"]["app"] = {"root": executable, "commands": commands}
        else:
            builders = [
                (name, value)
                for name, value in vars(native).items()
                if name in {"_parser", "parser", "build_parser"}
                and inspect.isfunction(value)
                and value.__module__ == native.__name__
            ]
            if len(builders) != 1:
                raise ContractAtlasError(f"CLI requires one native parser builder: {executable}")
            module(path)["cli"][builders[0][0]] = {"root": executable, "commands": commands}
    for application in operation_qualification.application_surfaces():
        fields = slots["openapi"].get(application.name)
        if not fields:
            continue
        path = _location(root, application.factory)
        if path is None or application.factory is None:
            raise ContractAtlasError("OpenAPI factory source ownership is unavailable")
        module(path)["openapi"][application.factory.__qualname__] = fields
    projects = release.validate_release_contract(root)
    metadata = {
        project.path: slots["metadata"][project.name]
        for project in projects
        if project.name in slots["metadata"]
    }
    return {
        "format": PREPARATION_FORMAT,
        "tag": tag,
        "closure_sha256": canonical_sha256(closure),
        "compiled_sha256": canonical_sha256(compiled),
        "requirements_sha256": canonical_sha256(requirements),
        "modules": modules,
        "metadata": metadata,
        "slots": slots,
    }


def _stage_module(
    source: bytes, instructions: Mapping[str, Any], resource: str, digest: str
) -> bytes:
    text = source.decode("utf-8")
    tree = ast.parse(text)
    lines = text.splitlines(keepends=True)
    starts = [0]
    for line in lines:
        starts.append(starts[-1] + len(line))
    edits: list[tuple[int, int, str]] = []
    source_context: list[str] = []
    found: set[tuple[str, str]] = set()

    def position(line: int | None, column: int | None) -> int:
        assert line is not None and column is not None
        # AST columns are UTF-8 byte offsets, not Unicode character indices.
        return starts[line - 1] + len(lines[line - 1].encode()[:column].decode())

    def visit(node: ast.AST, parents: tuple[str, ...] = ()) -> None:
        path = (
            (*parents, node.name)
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            else parents
        )
        name = ".".join(path)
        if (
            isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            and name in instructions["python"]
        ):
            found.add(("python", name))
            prose = instructions["python"][name]["docstring"]
            first = node.body[0]
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                # Existing declarations can carry promises mixed with editorial
                # help. Keep their exact source context in the prepared source;
                # the release corpus supplies the independently reviewed display.
                source_context.extend(
                    ["# Native declaration context: " + name]
                    + ["# " + line for line in first.value.value.splitlines()]
                )
                edits.append(
                    (
                        position(first.lineno, first.col_offset),
                        position(first.end_lineno, first.end_col_offset),
                        repr(prose),
                    )
                )
            else:
                prefix = lines[first.lineno - 1].encode()[: first.col_offset].decode()
                if prefix.strip():
                    offset = position(first.lineno, first.col_offset)
                    edits.append((offset, offset, repr(prose) + "; "))
                else:
                    offset = starts[first.lineno - 1]
                    edits.append((offset, offset, " " * first.col_offset + repr(prose) + "\n"))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for kind in ("cli", "openapi"):
                if name not in instructions[kind]:
                    continue
                found.add((kind, name))

                # Returns belonging to this declaration only; nested callbacks are untouched.
                def returns(current: ast.AST, *, kind: str = kind) -> None:
                    if current is not node and isinstance(
                        current, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
                    ):
                        return
                    if isinstance(current, ast.Return) and current.value is not None:
                        start = position(current.value.lineno, current.value.col_offset)
                        end = position(current.value.end_lineno, current.value.end_col_offset)
                        original = text[start:end]
                        expression = (
                            f"_riverhog_release_docs.apply_cli(({original}), "
                            f"_riverhog_release_resource['cli'][{name!r}]['commands'], "
                            f"_riverhog_release_resource['cli'][{name!r}]['root'])"
                            if kind == "cli"
                            else f"_riverhog_release_docs.attach_openapi(({original}), "
                            f"_riverhog_release_resource['openapi'][{name!r}])"
                        )
                        edits.append((start, end, expression))
                    for child in ast.iter_child_nodes(current):
                        returns(child)

                returns(node)
        for child in ast.iter_child_nodes(node):
            visit(child, path)

    visit(tree)
    required = {
        (kind, name) for kind in ("python", "cli", "openapi") for name in instructions[kind]
    }
    if found != required:
        raise ContractAtlasError(f"native source declaration mismatch: {sorted(required - found)}")
    for start, end, replacement in sorted(edits, reverse=True):
        text = text[:start] + replacement + text[end:]
    hook = (
        "\n"
        + "\n".join(source_context)
        + "\n# Verified release documentation: generated during exact source preparation.\n"
        "import release_documentation_lib as _riverhog_release_docs\n"
        f"_riverhog_release_resource = _riverhog_release_docs.load_resource("
        f"__file__.rsplit('/', 1)[0] + '/{resource}', {digest!r})\n"
    )
    # Windows uses native separators; pathlib handles both without a runtime Git dependency.
    hook = hook.replace(
        "__file__.rsplit('/', 1)[0] + '/" + resource + "'",
        "__import__('pathlib').Path(__file__).with_name(" + repr(resource) + ")",
    )
    initialization = ""
    if instructions["cli"] or instructions["openapi"]:
        # Factories may be called during module initialization. Load their sealed
        # annotations before ordinary statements, after the legal future prefix.
        tree = ast.parse(text)
        prefix = 0
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module == "__future__":
                prefix = node.end_lineno or prefix
            elif (
                isinstance(node, ast.Expr)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
            ):
                prefix = node.end_lineno or prefix
            else:
                break
        offset = sum(len(line) for line in text.splitlines(keepends=True)[:prefix])
        text = text[:offset] + hook + "\n" + text[offset:]
        hook = ""
    for variable in sorted(instructions["typer"]):
        initialization += (
            f"_riverhog_release_docs.prepare_typer({variable}, "
            f"_riverhog_release_resource['typer'][{variable!r}]['commands'], "
            f"_riverhog_release_resource['typer'][{variable!r}]['root'], "
            "__import__('typer.main', fromlist=['get_command']).get_command)\n"
        )
    # Native direct module invocation must initialize resources before __main__.
    tree = ast.parse(text)
    main = next(
        (
            node
            for node in tree.body
            if isinstance(node, ast.If)
            and isinstance(node.test, ast.Compare)
            and isinstance(node.test.left, ast.Name)
            and node.test.left.id == "__name__"
        ),
        None,
    )
    offset = (
        sum(len(line) for line in text.splitlines(keepends=True)[: main.lineno - 1])
        if main
        else len(text)
    )
    text = text[:offset] + hook + initialization + "\n" + text[offset:]
    ast.parse(text)
    return text.encode()


def stage_documentation(
    destination: Path, plan: Mapping[str, Any], *, source_files: Mapping[str, bytes] | None = None
) -> dict[str, Any]:
    """Apply a compiler-produced plan to a versioned Git snapshot before any backend runs."""
    if plan.get("format") != PREPARATION_FORMAT:
        raise ContractAtlasError("unsupported documentation source preparation")
    from .documentation_markdown import path_name

    def owned_path(name: str, *, root_allowed: bool = False) -> Path:
        if not (root_allowed and name == "."):
            path_name(name)
        path = destination / name
        if not path.resolve().is_relative_to(destination.resolve()) or any(
            parent.is_symlink() for parent in (path, *path.parents) if parent != destination.parent
        ):
            raise ContractAtlasError("documentation staging destination escapes prepared source")
        return path

    for name, instructions in plan["modules"].items():
        path = owned_path(name)
        if (
            path.suffix != ".py"
            or hashlib.sha256(path.read_bytes()).hexdigest() != instructions["source_sha256"]
        ):
            raise ContractAtlasError(f"documentation source changed before staging: {name}")
    for name in plan["metadata"]:
        owned_path(name, root_allowed=True)
    for name in source_files or {}:
        path_name(name)
    output: dict[str, str] = {}
    if "source_capture" in plan:
        from .documentation import source_ledger

        if source_files is None or source_ledger(source_files) != plan["source_capture"]["files"]:
            raise ContractAtlasError(
                "source archive does not have its exact captured Markdown/assets"
            )
        for name, raw in source_files.items():
            path = destination / "release-documentation" / plan["tag"] / name
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                raise ContractAtlasError("documentation capture would overwrite prepared source")
            path.write_bytes(raw)
            output[path.relative_to(destination).as_posix()] = hashlib.sha256(raw).hexdigest()
    for name, instructions in sorted(plan["modules"].items()):
        path = destination / name
        if hashlib.sha256(path.read_bytes()).hexdigest() != instructions["source_sha256"]:
            raise ContractAtlasError(f"documentation source changed before staging: {name}")
        resource = "_release_documentation_" + canonical_sha256(name)[:16] + ".json"
        resource_path = path.with_name(resource)
        if resource_path.exists():
            raise ContractAtlasError("generated documentation resource would overwrite source")
        raw = canonical_bytes({"format": "riverhog-package-documentation/v1", **instructions})
        resource_path.write_bytes(raw)
        path.write_bytes(
            _stage_module(
                path.read_bytes(), instructions, resource, hashlib.sha256(raw).hexdigest()
            )
        )
        output[resource_path.relative_to(destination).as_posix()] = hashlib.sha256(raw).hexdigest()
        output[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    for name, prose in sorted(plan["metadata"].items()):
        path = destination / name / "pyproject.toml"
        text = path.read_text()

        def summary(_match: re.Match[str], prose: Mapping[str, str] = prose) -> str:
            return "description = " + json.dumps(prose["summary"], ensure_ascii=False)

        def readme(_match: re.Match[str], prose: Mapping[str, str] = prose) -> str:
            return (
                "readme = { text = "
                + json.dumps(
                    prose["markdown"] or markdown_text(prose["summary"]), ensure_ascii=False
                )
                + ', content-type = "text/markdown" }'
            )

        text, count = re.subn(
            r"^description = .*$",
            summary,
            text,
            count=1,
            flags=re.M,
        )
        text, readmes = re.subn(
            r"^readme = .*$",
            readme,
            text,
            count=1,
            flags=re.M,
        )
        if count != 1 or readmes != 1:
            raise ContractAtlasError("package metadata has no exact static prose destination")
        path.write_text(text)
        output[path.relative_to(destination).as_posix()] = hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
    record = {
        "format": PREPARATION_FORMAT,
        "plan_sha256": canonical_sha256(plan),
        "plan": plan,
        "files": output,
    }
    (destination / PREPARATION_FILE).write_bytes(canonical_bytes(record))
    return record


def prepared_package_prose(root: Path, project_path: str) -> Mapping[str, Any] | None:
    """Validate the narrow metadata exception against exact generated staging bytes."""
    record_path = root / PREPARATION_FILE
    if not record_path.exists():
        return None
    record = json.loads(record_path.read_bytes())
    if record.get("format") != PREPARATION_FORMAT or record.get("plan_sha256") != canonical_sha256(
        record.get("plan")
    ):
        raise ContractAtlasError("prepared documentation plan identity differs")
    for name, digest in record["files"].items():
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != digest:
            raise ContractAtlasError(f"prepared documentation bytes changed: {name}")
    return cast(Mapping[str, Any] | None, record["plan"]["metadata"].get(project_path))
