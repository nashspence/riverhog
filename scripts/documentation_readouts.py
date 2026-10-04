#!/usr/bin/env python3
"""Extract native documentation from installed prepared wheels, never editable source."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import subprocess
import sys
from email.parser import Parser
from pathlib import Path
from typing import Any, cast

# Only trusted coordinator tooling is imported here. Component imports must resolve
# inside the isolated interpreter, and cannot see source-tree implementation paths.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from contract_atlas.documentation_native import (  # noqa: E402
    extract_cli,
    native_semantics,
    public_object,
    semantic_frame,
)
from contract_atlas.model import ContractAtlasError, canonical_bytes  # noqa: E402


def installed(module: str) -> Any:
    value = importlib.import_module(module)
    if value.__file__ is None:
        raise ContractAtlasError("installed native module has no package file")
    path = Path(value.__file__).resolve()
    if not path.is_relative_to(Path(sys.prefix).resolve()):
        raise ContractAtlasError(f"native readout imported noninstalled implementation: {module}")
    return value


def extract(request: dict[str, Any]) -> dict[str, Any]:
    import contract_freeze
    import operation_qualification
    from starlette.testclient import TestClient

    for name in request["modules"]:
        installed(name)
    parsers = contract_freeze._cli_parsers()
    observed, helps = extract_cli(parsers)
    journeys = {}
    for name in sorted(parsers):
        command = (
            Path(sys.prefix)
            / ("Scripts" if os.name == "nt" else "bin")
            / (name + ".exe" if os.name == "nt" else name)
        )
        result = subprocess.run(
            [str(command), "--help"],
            text=True,
            capture_output=True,
            timeout=60,
            env={**os.environ, "NO_COLOR": "1"},
        )
        if result.returncode != 0 or not result.stdout.strip():
            raise ContractAtlasError(f"actual installed CLI help journey failed: {name}")
        journeys[name] = {
            "argv": [name, "--help"],
            "exit": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    semantics = {
        "cli": {
            name: contract_freeze._argparse_command(parser, name=name)
            if isinstance(parser, argparse.ArgumentParser)
            else contract_freeze._click_command(parser, name=name)
            for name, parser in parsers.items()
        },
        "openapi": {},
        "python": {},
    }
    readouts: dict[str, Any] = {"cli": helps, "openapi": {}, "python": {}, "metadata": {}}
    for identity, surface in request["python"].items():
        native = public_object(surface["surface"])
        module = installed(surface["surface"]["module"])
        if surface["capability"] == "reference":
            resource = module._riverhog_release_resource["references"][identity]
            value = {"capability": "reference", "text": resource["docstring"]}
        else:
            value = {"capability": "docstring", "text": native.__doc__}
        observed["python/" + identity] = value
        readouts["python"][identity] = value
        semantics["python"][identity] = contract_freeze._python_export(native)
    for surface in operation_qualification.application_surfaces():
        # Extract the actual HTTP response, not the expected annotations or a copied schema.
        if surface.app.openapi_url is None:
            raise ContractAtlasError("prepared service has no served OpenAPI route")
        # Schema extraction exercises HTTP dispatch without claiming a service
        # restart/lifecycle qualification or launching archive background work.
        client = TestClient(surface.app)
        try:
            response = client.get(surface.app.openapi_url)
        finally:
            client.close()
        if response.status_code != 200:
            raise ContractAtlasError("prepared service did not serve its OpenAPI document")
        document = response.json()
        readouts["openapi"][surface.name] = semantic_frame(document)
        semantics["openapi"][surface.name] = document
        for pointer in request["openapi"].get(surface.name, []):
            node: Any = document
            for part in pointer.split("/")[1:]:
                key = part.replace("~1", "/").replace("~0", "~")
                node = node[int(key)] if isinstance(node, list) else node[key]
            observed["openapi/" + surface.name + pointer] = node.get("description")
    import importlib.metadata

    for name in request["metadata"]:
        distribution = importlib.metadata.distribution(name)
        metadata = distribution.metadata
        raw = distribution.read_text("METADATA")
        if raw is None or not Path(str(distribution.locate_file(""))).resolve().is_relative_to(
            Path(sys.prefix).resolve()
        ):
            raise ContractAtlasError("prepared distribution metadata is not installed")
        readouts["metadata"][name] = raw
        observed["metadata/" + name] = {
            "summary": metadata["Summary"],
            "description": cast(str, Parser().parsestr(raw).get_payload()).rstrip("\n"),
        }
    return {
        "observed": observed,
        "readouts": readouts,
        "semantics": native_semantics(semantics),
        "interpreter": sys.version,
        "installed_modules": sorted(request["modules"]),
        "cli_journeys": journeys,
        "http_scope": "actual schema HTTP dispatch; application lifespan not performed",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.write_bytes(canonical_bytes(extract(json.loads(args.request.read_bytes()))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
