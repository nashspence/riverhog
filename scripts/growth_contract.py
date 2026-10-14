"""Project operation-owned declarations and derive their source/audit associations."""

from __future__ import annotations

import hashlib
import importlib
import inspect
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import Binding, growth_binding, project_bindings

from scripts.validation_identity import source_identity


def resolve_operation(subject: str) -> Callable[..., Any]:
    pieces = subject.split(".")
    for count in range(len(pieces) - 1, 0, -1):
        name = ".".join(pieces[:count])
        try:
            value: object = importlib.import_module(name)
        except ModuleNotFoundError as error:
            if error.name != name and not name.startswith(str(error.name) + "."):
                raise
            continue
        for attribute in pieces[count:]:
            value = (
                getattr(value, attribute)
                if inspect.ismodule(value)
                else inspect.getattr_static(value, attribute)
            )
            if isinstance(value, (classmethod, staticmethod)):
                value = value.__func__
            elif isinstance(value, property):
                value = value.fget
        if not callable(value):
            raise ValueError("growth subject does not resolve to an executable operation")
        return value
    raise ValueError("growth subject has no current import authority")


def _declared_operations(packages: Mapping[str, Path]) -> tuple[Callable[..., Any], ...]:
    """Find declaration-bearing bodies in shipped source, including private bodies."""
    result = []
    for package, initializer in sorted(packages.items()):
        for path in sorted(initializer.parent.glob("*.py")):
            if b"declare_growth" not in path.read_bytes():
                continue
            module_name = package if path.name == "__init__.py" else f"{package}.{path.stem}"
            module = importlib.import_module(module_name)
            if Path(str(module.__file__)).resolve() != path.resolve():
                raise ValueError("growth module is outside its shipped source authority")
            pending = list(vars(module).values())
            seen_classes: set[int] = set()
            while pending:
                value = pending.pop()
                if inspect.isclass(value) and value.__module__ == module_name:
                    if id(value) not in seen_classes:
                        seen_classes.add(id(value))
                        pending.extend(vars(value).values())
                    continue
                if isinstance(value, (classmethod, staticmethod)):
                    value = value.__func__
                elif isinstance(value, property):
                    value = value.fget
                if not inspect.isfunction(value) or not hasattr(value, "__riverhog_growth__"):
                    continue
                if Path(inspect.getfile(value)).resolve() != path.resolve():
                    continue
                result.append(value)
    return tuple(result)


def source_bindings(
    surfaces: Mapping[str, Mapping[str, object]], *, packages: Mapping[str, Path] | None = None
) -> tuple[Binding, ...]:
    operations = []
    for subject, surface in surfaces.items():
        contract = surface["contract"]
        if isinstance(contract, Mapping) and contract.get("kind") in {
            "function",
            "method",
            "classmethod",
            "staticmethod",
            "property",
        }:
            operations.append(resolve_operation(subject))
    operations.extend(_declared_operations(packages or {}))
    result: dict[str, Binding] = {}
    for operation in operations:
        if not hasattr(operation, "__riverhog_growth__"):
            continue
        binding = growth_binding(operation)
        identity = binding.declaration.id
        if identity in result and result[identity].operation is not operation:
            raise ValueError("growth declaration has conflicting discovered operation owners")
        result[identity] = binding
    return tuple(result[key] for key in sorted(result))


def declaration_projection(
    surfaces: Mapping[str, Mapping[str, object]], *, packages: Mapping[str, Path] | None = None
) -> dict[str, object]:
    return project_bindings(source_bindings(surfaces, packages=packages))


def source_analysis(
    root: Path,
    surfaces: Mapping[str, Mapping[str, object]],
    projection: Mapping[str, object],
    *,
    packages: Mapping[str, Path] | None = None,
) -> dict[str, object]:
    bindings = source_bindings(surfaces, packages=packages)
    if declaration_projection(surfaces, packages=packages) != projection:
        raise ValueError("growth projection differs from its current source declarations")
    pending = list(bindings)
    owned: dict[str, Binding] = {}
    while pending:
        binding = pending.pop()
        identity = binding.declaration.id
        if identity in owned:
            if owned[identity].operation is not binding.operation:
                raise ValueError("growth declaration identity has conflicting source bindings")
            continue
        owned[identity] = binding
        for subject in binding.declaration.subjects:
            if resolve_operation(subject) is not binding.operation:
                raise ValueError("growth declaration has a stale subject binding")
        pending.extend(growth_binding(operation) for _, operation in binding.delegates)
    declarations = {}
    for identity, binding in sorted(owned.items()):
        path = Path(inspect.getfile(binding.operation)).resolve()
        if not path.is_relative_to(root.resolve()):
            raise ValueError("growth declaration source is outside the authoritative checkout")
        declarations[identity] = {
            "implementation": f"{binding.operation.__module__}.{binding.operation.__qualname__}",
            "source": {
                "path": path.relative_to(root.resolve()).as_posix(),
                "symbol": binding.operation.__qualname__,
                "line": inspect.getsourcelines(binding.operation)[1],
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            },
            "declaration_sha256": hashlib.sha256(
                canonical_json_bytes(binding.project())
            ).hexdigest(),
            "delegates": {
                name: growth_binding(operation).declaration.id
                for name, operation in binding.delegates
            },
        }
    coverage = {}
    for subject, surface in sorted(surfaces.items()):
        contract = surface["contract"]
        kind = contract.get("kind") if isinstance(contract, Mapping) else None
        if kind in {"constant", "type-alias"}:
            coverage[subject] = {"status": "nonexecutable", "reason": f"declared {kind}"}
        elif kind in {"function", "method", "classmethod", "staticmethod", "property"}:
            operation = resolve_operation(subject)
            if hasattr(operation, "__riverhog_growth__"):
                binding = growth_binding(operation)
                coverage[subject] = {
                    "status": "owns" if subject in binding.declaration.subjects else "inherits",
                    "declaration": binding.declaration.id,
                    "wrapper": "same resolved callable; no additional wrapper body",
                }
            else:
                coverage[subject] = {"status": "unestablished", "reason": "no source declaration"}
        else:
            coverage[subject] = {
                "status": "unestablished",
                "reason": "executable disposition not established",
            }
    return {
        "format": "riverhog-growth-source-analysis/v1",
        "projection_sha256": hashlib.sha256(canonical_json_bytes(projection)).hexdigest(),
        "declarations": declarations,
        "coverage": coverage,
        "evidence_status": "not-executed",
        "source_identity": source_identity(root),
    }
