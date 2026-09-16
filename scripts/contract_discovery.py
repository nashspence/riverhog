"""Implementation-led discovery helpers for the Riverhog v1 freeze projection."""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import re
import tomllib
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

EXCEPTION_SCHEMA = "riverhog-contract-freeze-exceptions/v1"
ENVIRONMENT_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]+$")
CONFIGURATION_LIKE_NAME = re.compile(r"^[A-Z][A-Z0-9_]+$")
PROJECT_CONFIGURATION_NAME = re.compile(
    r"^(?:GOGURT|MANGO|PIGGITY|RIVERHOG|STOVE0|VCRUNCH)_[A-Z0-9_]+$"
)


class DiscoveryError(RuntimeError):
    """A release-exposed construct could not be resolved exactly."""


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def _qualified_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _qualified_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return ""


def _walk_scope(node: ast.AST) -> list[ast.AST]:
    """Walk one lexical scope without entering child function or class scopes."""

    result: list[ast.AST] = []

    class Visitor(ast.NodeVisitor):
        def generic_visit(self, current: ast.AST) -> None:
            if current is not node and isinstance(
                current, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
            ):
                return
            result.append(current)
            super().generic_visit(current)

    Visitor().visit(node)
    return result


def _string_values(node: ast.AST, values: Mapping[str, set[str]]) -> set[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value}
    if isinstance(node, ast.Name):
        return set(values.get(node.id, set()))
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _string_values(node.left, values)
        right = _string_values(node.right, values)
        return {a + b for a in left for b in right}
    if isinstance(node, ast.JoinedStr):
        parts: list[set[str]] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append({value.value})
            elif isinstance(value, ast.FormattedValue):
                parts.append(_string_values(value.value, values))
            else:
                return set()
        rendered = {""}
        for part in parts:
            rendered = {prefix + suffix for prefix in rendered for suffix in part}
        return rendered
    if isinstance(node, ast.IfExp):
        return _string_values(node.body, values) | _string_values(node.orelse, values)
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "format"
    ):
        templates = _string_values(node.func.value, values)
        positional = [_string_values(item, values) for item in node.args]
        keywords = {
            str(item.arg): _string_values(item.value, values)
            for item in node.keywords
            if item.arg is not None
        }
        if (
            not templates
            or any(not item for item in positional)
            or any(not item for item in keywords.values())
        ):
            return set()
        combinations: list[tuple[list[str], dict[str, str]]] = [([], {})]
        for candidates in positional:
            combinations = [
                ([*args, candidate], dict(kwargs))
                for args, kwargs in combinations
                for candidate in candidates
            ]
        for name, candidates in keywords.items():
            combinations = [
                (list(args), {**kwargs, name: candidate})
                for args, kwargs in combinations
                for candidate in candidates
            ]
        result: set[str] = set()
        for template in templates:
            for args, kwargs in combinations:
                try:
                    result.add(template.format(*args, **kwargs))
                except (IndexError, KeyError, ValueError):
                    continue
        return result
    return set()


def _assigned_values(
    scope: ast.AST,
    initial: Mapping[str, set[str]],
) -> dict[str, set[str]]:
    values = {name: set(items) for name, items in initial.items()}
    assignments = [
        node for node in _walk_scope(scope) if isinstance(node, (ast.Assign, ast.AnnAssign))
    ]
    for _ in range(len(assignments) + 1):
        changed = False
        for assignment in assignments:
            expression = assignment.value
            if expression is None:
                continue
            resolved = _string_values(expression, values)
            if not resolved:
                continue
            targets = (
                assignment.targets if isinstance(assignment, ast.Assign) else [assignment.target]
            )
            for target in targets:
                if not isinstance(target, ast.Name):
                    continue
                before = len(values.get(target.id, set()))
                values.setdefault(target.id, set()).update(resolved)
                changed |= len(values[target.id]) != before
        if not changed:
            break
    return values


def _function_parameters(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    return [
        argument.arg
        for argument in [
            *node.args.posonlyargs,
            *node.args.args,
            *node.args.kwonlyargs,
        ]
    ]


def _module_string_values(tree: ast.Module) -> dict[str, set[str]]:
    module = ast.Module(
        body=[
            node
            for node in tree.body
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        ],
        type_ignores=[],
    )
    return _assigned_values(module, {})


def _lexical_scopes(tree: ast.Module) -> dict[str, ast.AST]:
    scopes: dict[str, ast.AST] = {"<module>": tree}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            scopes[node.name] = node
        elif isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    scopes[f"{node.name}.{child.name}"] = child
    return scopes


def _scope_environments(
    tree: ast.Module,
) -> tuple[dict[str, set[str]], dict[str, dict[str, set[str]]]]:
    module_values = _module_string_values(tree)
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    parameters: dict[str, dict[str, set[str]]] = {
        name: {parameter: set() for parameter in _function_parameters(node)}
        for name, node in functions.items()
    }
    scopes = _lexical_scopes(tree)
    for _ in range(max(2, len(functions) * 2 + 1)):
        changed = False
        environments = {
            name: _assigned_values(scope, module_values | parameters.get(name, {}))
            for name, scope in scopes.items()
        }
        for caller, scope in scopes.items():
            caller_values = environments[caller]
            for current in _walk_scope(scope):
                if not isinstance(current, ast.Call) or not isinstance(current.func, ast.Name):
                    continue
                callee = functions.get(current.func.id)
                if callee is None:
                    continue
                names = _function_parameters(callee)
                supplied: dict[str, ast.AST] = {
                    names[index]: value
                    for index, value in enumerate(current.args)
                    if index < len(names)
                }
                supplied.update(
                    {
                        str(keyword.arg): keyword.value
                        for keyword in current.keywords
                        if keyword.arg in names
                    }
                )
                for parameter, expression in supplied.items():
                    resolved = _string_values(expression, caller_values)
                    before = len(parameters[callee.name][parameter])
                    parameters[callee.name][parameter].update(resolved)
                    changed |= len(parameters[callee.name][parameter]) != before
        if not changed:
            return module_values, environments
    raise DiscoveryError("configuration call-graph string resolution did not converge")


def _environment_access(node: ast.AST) -> tuple[str, ast.AST, ast.AST | None] | None:
    if isinstance(node, ast.Subscript) and _qualified_name(node.value) == "os.environ":
        return "read", node.slice, None
    if not isinstance(node, ast.Call):
        return None
    name = _qualified_name(node.func)
    if name in {"os.getenv", "os.environ.get", "os.environ.pop"} and node.args:
        default = node.args[1] if len(node.args) > 1 else None
        return ("remove-after-read" if name.endswith(".pop") else "read", node.args[0], default)
    # A mapping read with an exact environment-shaped key is also a configuration
    # read. This catches injected environments without assuming the mapping's name.
    if (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and _qualified_name(node.func.value).rsplit(".", 1)[-1]
        in {"env", "environ", "environment", "values"}
        and node.args
    ):
        return "read", node.args[0], node.args[1] if len(node.args) > 1 else None
    return None


def _scope_index(tree: ast.Module) -> dict[int, str]:
    return {
        id(current): name
        for name, scope in _lexical_scopes(tree).items()
        for current in _walk_scope(scope)
    }


def _source_module(source_root: Path, source: Path) -> tuple[str, bool]:
    parts = list(source.relative_to(source_root).with_suffix("").parts)
    is_package = parts[-1] == "__init__"
    if is_package:
        parts.pop()
    return ".".join(parts), is_package


def _import_aliases(tree: ast.Module, module: str, is_package: bool) -> dict[str, str]:
    aliases: dict[str, str] = {}
    package = module if is_package else module.rpartition(".")[0]
    for node in tree.body:
        if isinstance(node, ast.Import):
            for imported in node.names:
                local = imported.asname or imported.name.split(".", 1)[0]
                aliases[local] = imported.name if imported.asname else local
        elif isinstance(node, ast.ImportFrom):
            imported_module = node.module or ""
            if node.level:
                imported_module = importlib.util.resolve_name(
                    f"{'.' * node.level}{imported_module}", package
                )
            for imported in node.names:
                if imported.name == "*":
                    continue
                local = imported.asname or imported.name
                aliases[local] = f"{imported_module}.{imported.name}"
    return aliases


def _expanded_name(node: ast.AST, aliases: Mapping[str, str]) -> str:
    raw = _qualified_name(node)
    first, separator, remainder = raw.partition(".")
    if first not in aliases:
        return raw
    return aliases[first] + (f".{remainder}" if separator else "")


def _configuration_authority(
    node: ast.AST,
    *,
    module: str,
    scope: str,
    aliases: Mapping[str, str],
    module_symbols: set[str],
) -> tuple[str, str] | None:
    raw = _qualified_name(node)
    if raw == "cls" and "." in scope:
        return module, scope.split(".", 1)[0]
    expanded = _expanded_name(node, aliases)
    if not expanded:
        return None
    if expanded == raw and "." not in raw and raw in module_symbols:
        return module, raw
    if expanded == raw and "." not in raw:
        return None
    authority_module, separator, qualname = expanded.rpartition(".")
    if not separator:
        return None
    return authority_module, qualname


def discover_configuration_documents(
    root: Path,
    projects: Sequence[Any],
) -> list[dict[str, object]]:
    """Detect runtime configuration validators in every release distribution."""

    result: list[dict[str, object]] = []
    for project in projects:
        source_root = root / str(project.path) / "src"
        for source in sorted(source_root.glob("**/*.py")):
            module, is_package = _source_module(source_root, source)
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            aliases = _import_aliases(tree, module, is_package)
            scopes = _scope_index(tree)
            module_symbols = {
                node.name
                for node in tree.body
                if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            }
            module_symbols.update(
                target.id
                for node in tree.body
                if isinstance(node, (ast.Assign, ast.AnnAssign))
                for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
                if isinstance(target, ast.Name)
            )
            seen: defaultdict[tuple[str, str], int] = defaultdict(int)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                validator = _expanded_name(node.func, aliases)
                input_shape: str
                authority_node: ast.AST
                if validator == "config_validation.validate_json_schema":
                    if len(node.args) <= 1:
                        authority_node = ast.Constant(None)
                    else:
                        authority_node = node.args[1]
                    input_shape = "json-schema"
                elif isinstance(node.func, ast.Attribute) and node.func.attr in {
                    "model_validate",
                    "model_validate_json",
                }:
                    authority_node = node.func.value
                    raw_authority = _qualified_name(authority_node).rsplit(".", 1)[-1]
                    scope = scopes.get(id(node), "<module>")
                    context = f"{module}.{scope}".casefold()
                    if not (
                        raw_authority.endswith(("Config", "Catalog"))
                        or (raw_authority in {"cls", "model", "schema"} and "config" in context)
                    ):
                        continue
                    input_shape = (
                        "model-json" if node.func.attr == "model_validate_json" else "model"
                    )
                else:
                    continue
                scope = scopes.get(id(node), "<module>")
                expression = ast.unparse(node)
                occurrence_key = (scope, expression)
                ordinal = seen[occurrence_key]
                seen[occurrence_key] += 1
                authority = _configuration_authority(
                    authority_node,
                    module=module,
                    scope=scope,
                    aliases=aliases,
                    module_symbols=module_symbols,
                )
                relative = source.relative_to(root).as_posix()
                basis = f"{project.name}\0{relative}\0{scope}\0{expression}\0{ordinal}"
                result.append(
                    {
                        "id": f"configuration-document-read:{project.name}:{_digest(basis)}",
                        "kind": "configuration-document-read",
                        "consumer": project.name,
                        "path": relative,
                        "scope": scope,
                        "line": node.lineno,
                        "validator": validator,
                        "input_shape": input_shape,
                        "expression": expression,
                        "authority_module": authority[0] if authority is not None else None,
                        "authority_qualname": authority[1] if authority is not None else None,
                    }
                )
    ids = [str(item["id"]) for item in result]
    if len(ids) != len(set(ids)):
        raise DiscoveryError("configuration-document detection identities are not unique")
    return sorted(result, key=lambda item: str(item["id"]))


def _callables(
    trees: Mapping[str, ast.Module],
) -> tuple[
    dict[str, tuple[str, ...]],
    dict[tuple[str, str], str],
    dict[tuple[str, str], tuple[str, ...]],
]:
    parameters: dict[str, tuple[str, ...]] = {}
    local_names: dict[tuple[str, str], str] = {}
    class_bases: dict[tuple[str, str], tuple[str, ...]] = {}
    for module, tree in trees.items():
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                symbol = f"{module}.{node.name}"
                parameters[symbol] = tuple(_function_parameters(node))
                local_names[(module, node.name)] = symbol
            elif isinstance(node, ast.ClassDef):
                class_bases[(module, node.name)] = tuple(
                    name for base in node.bases if (name := _qualified_name(base))
                )
                for child in node.body:
                    if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    symbol = f"{module}.{node.name}.{child.name}"
                    parameters[symbol] = tuple(_function_parameters(child))
                    if child.name == "__init__":
                        local_names[(module, node.name)] = symbol
    return parameters, local_names, class_bases


def _resolve_callee(
    node: ast.Call,
    *,
    module: str,
    scope: str,
    aliases: Mapping[str, str],
    local_names: Mapping[tuple[str, str], str],
    parameters: Mapping[str, tuple[str, ...]],
    class_bases: Mapping[tuple[str, str], tuple[str, ...]],
) -> tuple[str, bool] | None:
    if (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "__init__"
        and isinstance(node.func.value, ast.Call)
        and _qualified_name(node.func.value.func) == "super"
        and "." in scope
    ):
        class_name = scope.split(".", 1)[0]
        for base in class_bases.get((module, class_name), ()):
            first, separator, remainder = base.partition(".")
            resolved_base = aliases.get(first)
            if resolved_base is not None:
                resolved_base += f".{remainder}" if separator else ""
            local_initializer = local_names.get((module, base))
            for candidate in (
                *((f"{resolved_base}.__init__",) if resolved_base is not None else ()),
                *((local_initializer,) if local_initializer is not None else ()),
            ):
                if candidate in parameters:
                    return candidate, True
        return None
    raw = _qualified_name(node.func)
    if not raw:
        return None
    first, separator, remainder = raw.partition(".")
    if first in aliases:
        symbol = aliases[first] + (f".{remainder}" if separator else "")
    elif (module, raw) in local_names:
        symbol = local_names[(module, raw)]
    elif raw.startswith(("self.", "cls.")) and "." in scope:
        class_name = scope.split(".", 1)[0]
        symbol = f"{module}.{class_name}.{remainder}"
    else:
        symbol = f"{module}.{raw}"
    if symbol not in parameters and f"{symbol}.__init__" in parameters:
        symbol = f"{symbol}.__init__"
    if symbol not in parameters:
        return None
    bound_method = symbol.rsplit(".", 1)[-1] == "__init__" or raw.startswith(("self.", "cls."))
    return symbol, bound_method


def _project_call_hints(source_root: Path) -> dict[tuple[str, str], set[str]]:
    parsed: list[tuple[Path, str, bool, ast.Module]] = []
    trees: dict[str, ast.Module] = {}
    for source in sorted(source_root.glob("**/*.py")):
        module, is_package = _source_module(source_root, source)
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        parsed.append((source, module, is_package, tree))
        trees[module] = tree
    parameters, local_names, class_bases = _callables(trees)
    hints: defaultdict[tuple[str, str], set[str]] = defaultdict(set)
    for _source, module, is_package, tree in parsed:
        module_values = _module_string_values(tree)
        scopes = _lexical_scopes(tree)
        aliases = _import_aliases(tree, module, is_package)
        for scope_name, scope in scopes.items():
            scope_values = _assigned_values(scope, module_values)
            for node in _walk_scope(scope):
                if not isinstance(node, ast.Call):
                    continue
                resolved_callee = _resolve_callee(
                    node,
                    module=module,
                    scope=scope_name,
                    aliases=aliases,
                    local_names=local_names,
                    parameters=parameters,
                    class_bases=class_bases,
                )
                if resolved_callee is None:
                    continue
                symbol, bound_method = resolved_callee
                names = list(parameters[symbol])
                if bound_method and names and names[0] in {"self", "cls"}:
                    names.pop(0)
                for index, argument in enumerate(node.args):
                    if index < len(names):
                        hints[(symbol, names[index])].update(_string_values(argument, scope_values))
                for keyword in node.keywords:
                    if keyword.arg in names:
                        hints[(symbol, str(keyword.arg))].update(
                            _string_values(keyword.value, scope_values)
                        )
    return dict(hints)


def discover_environment_reads(
    root: Path,
    projects: Sequence[Any],
) -> list[dict[str, object]]:
    """Detect every environment read before deciding whether it is contractual."""

    result: list[dict[str, object]] = []
    for project in projects:
        source_root = root / str(project.path) / "src"
        call_hints = _project_call_hints(source_root)
        for source in sorted(source_root.glob("**/*.py")):
            module, _is_package = _source_module(source_root, source)
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            _module_values, environments = _scope_environments(tree)
            scopes = _scope_index(tree)
            seen: defaultdict[tuple[str, str], int] = defaultdict(int)
            for node in ast.walk(tree):
                access = _environment_access(node)
                if access is None:
                    continue
                operation, name_expression, default_expression = access
                scope = scopes.get(id(node), "<module>")
                values = {name: set(items) for name, items in environments.get(scope, {}).items()}
                if isinstance(name_expression, ast.Name):
                    values.setdefault(name_expression.id, set()).update(
                        call_hints.get((f"{module}.{scope}", name_expression.id), set())
                    )
                resolved = sorted(
                    value
                    for value in _string_values(name_expression, values)
                    if ENVIRONMENT_NAME.fullmatch(value)
                )
                # Generic Mapping.get calls enter the detector only when their key
                # resolves to an environment-shaped name. Direct os.environ access
                # remains detected even when its name is unresolved.
                direct = isinstance(node, ast.Subscript) or (
                    isinstance(node, ast.Call)
                    and _qualified_name(node.func)
                    in {"os.getenv", "os.environ.get", "os.environ.pop"}
                )
                if not direct and not any(
                    CONFIGURATION_LIKE_NAME.fullmatch(value) for value in resolved
                ):
                    continue
                if not direct:
                    receiver = (
                        _qualified_name(node.func.value).rsplit(".", 1)[-1]
                        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                        else ""
                    )
                    resolved = [
                        value
                        for value in resolved
                        if CONFIGURATION_LIKE_NAME.fullmatch(value)
                        and (receiver != "values" or PROJECT_CONFIGURATION_NAME.fullmatch(value))
                    ]
                    if not resolved:
                        continue
                expression = ast.unparse(node)
                occurrence_key = (scope, expression)
                ordinal = seen[occurrence_key]
                seen[occurrence_key] += 1
                relative = source.relative_to(root).as_posix()
                basis = f"{project.name}\0{relative}\0{scope}\0{expression}\0{ordinal}"
                result.append(
                    {
                        "id": f"configuration-read:{project.name}:{_digest(basis)}",
                        "kind": "configuration-read",
                        "consumer": project.name,
                        "path": relative,
                        "scope": scope,
                        "operation": operation,
                        "expression": expression,
                        "name_expression": ast.unparse(name_expression),
                        "default_expression": (
                            ast.unparse(default_expression)
                            if default_expression is not None
                            else "unset"
                        ),
                        "resolved_names": resolved,
                    }
                )
    ids = [str(item["id"]) for item in result]
    if len(ids) != len(set(ids)):
        raise DiscoveryError("environment detection identities are not unique")
    return sorted(result, key=lambda item: str(item["id"]))


def python_package_detections(
    root: Path,
    projects: Sequence[Any],
) -> list[dict[str, object]]:
    """Detect every importable root carried by every release distribution."""

    result: list[dict[str, object]] = []
    for project in projects:
        pyproject = root / str(project.path) / "pyproject.toml"
        config = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        packages = (
            config.get("tool", {})
            .get("hatch", {})
            .get("build", {})
            .get("targets", {})
            .get("wheel", {})
            .get("packages", [])
        )
        if not isinstance(packages, list) or not packages:
            raise DiscoveryError(
                f"release distribution has no importable package roots: {project.name}"
            )
        for package_path in packages:
            if not isinstance(package_path, str) or not package_path.startswith("src/"):
                raise DiscoveryError(
                    f"release distribution has an unsupported package root: {project.name}"
                )
            package_root = root / str(project.path) / package_path
            initializer = package_root / "__init__.py"
            root_module = package_path.removeprefix("src/").replace("/", ".")
            if not initializer.is_file():
                raise DiscoveryError(
                    f"release package root is not importable: {project.name}:{root_module}"
                )
            for current in sorted(package_root.glob("**/__init__.py")):
                relative = current.parent.relative_to(package_root)
                module = ".".join((root_module, *relative.parts)) if relative.parts else root_module
                result.append(
                    {
                        "id": f"python-package:{project.name}:{module}",
                        "kind": "python-package",
                        "distribution": project.name,
                        "module": module,
                        "path": current.relative_to(root).as_posix(),
                    }
                )
    ids = [str(item["id"]) for item in result]
    if len(ids) != len(set(ids)):
        raise DiscoveryError("release Python package detections are not unique")
    return sorted(result, key=lambda item: str(item["id"]))


def load_exceptions(path: Path) -> dict[str, list[dict[str, str]]]:
    """Load the narrow audit overlay without allowing it to define candidates."""

    document = tomllib.loads(path.read_text(encoding="utf-8"))
    if set(document) != {"schema", "resolution"}:
        raise DiscoveryError("contract-freeze exception overlay has unexpected fields")
    if document["schema"] != EXCEPTION_SCHEMA:
        raise DiscoveryError("contract-freeze exception overlay has another schema")
    values = document["resolution"]
    if not isinstance(values, list):
        raise DiscoveryError("contract-freeze resolution exceptions must be a list")
    result: dict[str, list[dict[str, str]]] = {"resolution": []}
    for index, item in enumerate(values):
        if not isinstance(item, dict) or set(item) != {
            "detection_id",
            "source_authority_id",
            "reason",
        }:
            raise DiscoveryError(f"contract-freeze resolution exception {index} is incomplete")
        normalized = {str(key): str(value) for key, value in item.items()}
        if any(not value.strip() for value in normalized.values()):
            raise DiscoveryError(f"contract-freeze resolution exception {index} is blank")
        result["resolution"].append(normalized)
    identities = [item["detection_id"] for item in result["resolution"]]
    if len(identities) != len(set(identities)):
        raise DiscoveryError("contract-freeze resolution exceptions repeat an identity")

    return result


__all__ = [
    "DiscoveryError",
    "EXCEPTION_SCHEMA",
    "discover_configuration_documents",
    "discover_environment_reads",
    "load_exceptions",
    "python_package_detections",
]
