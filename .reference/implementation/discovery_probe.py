"""Reference-only, source-derived coverage snapshot; no product work is executed.

Every inventory below comes from an existing Riverhog discovery/qualification
owner. This program records their outputs; it does not classify runtime safety,
execute workloads, infer adoption, or create a second hand-maintained registry.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import platform
import sqlite3
import subprocess
import sys
import time
import tomllib
import traceback
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def git(repo: Path, *arguments: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *arguments], text=True).strip()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def emit(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def input_snapshot(repo: Path) -> dict[str, object]:
    """Bind actual tracked product bytes without creating another source registry."""
    paths = sorted(
        path for path in git(repo, "ls-files", "-z", "--", ".", ":!.reference").split("\0") if path
    )
    result = hashlib.sha256()
    for relative in paths:
        path = repo / relative
        if path.is_symlink():
            contents = hashlib.sha256(os.fsencode(os.readlink(path))).digest()
            kind = b"symlink"
        else:
            with path.open("rb") as stream:
                contents = hashlib.file_digest(stream, "sha256").digest()
            kind = b"file"
        for value in (os.fsencode(relative), kind, contents):
            result.update(len(value).to_bytes(8, "big"))
            result.update(value)
    return {"files": len(paths), "sha256": result.hexdigest()}


def checked_source(repo: Path, source_sha: str) -> dict[str, object]:
    if git(repo, "rev-parse", "HEAD") != source_sha:
        raise ValueError("the exact requested source is not checked out")
    if git(repo, "diff", "--name-only", source_sha, "--", ".", ":!.reference"):
        raise ValueError("product paths differ from the selected source")
    if git(repo, "ls-files", "--others", "--exclude-standard", "-z", "--", ".", ":!.reference"):
        raise ValueError("untracked product paths are present")
    return {
        "head": source_sha,
        "tree": git(repo, "rev-parse", "HEAD^{tree}"),
        "status": git(repo, "status", "--porcelain=v1"),
        "tracked_input_snapshot": input_snapshot(repo),
    }


def prepare_output(repo: Path, output: Path, source_sha: str) -> dict[str, object]:
    if not repo.is_dir():
        raise ValueError("the product checkout is not a directory")
    if output.is_relative_to(repo):
        raise ValueError("probe evidence must be staged outside the product checkout")
    before = checked_source(repo, source_sha)
    output.mkdir(parents=True, exist_ok=False)
    return before


def verify_final_source(
    repo: Path, source_sha: str, before: dict[str, object], metadata: dict[str, Any]
) -> None:
    try:
        after = checked_source(repo, source_sha)
        metadata["source_after"] = after
        if after["tracked_input_snapshot"] != before["tracked_input_snapshot"]:
            raise ValueError("tracked product inputs changed during discovery")
    except BaseException as error:
        metadata["status"] = "failed"
        metadata["final_source_verification"] = "failed"
        metadata["final_source_error"] = f"{type(error).__name__}: {error}"
        raise
    metadata["final_source_verification"] = "observed"


def run(repo: Path, output: Path, source_sha: str) -> dict[str, object]:
    repo, output = repo.resolve(), output.resolve()
    before = prepare_output(repo, output, source_sha)
    sys.path.insert(0, str(repo))
    sys.path.insert(0, str(repo / "scripts"))
    metadata: dict[str, Any] = {
        "format": "riverhog-reference-coverage-observation/v1",
        "source_sha": source_sha,
        "source_tree": git(repo, "rev-parse", "HEAD^{tree}"),
        "source_status_before": git(repo, "status", "--porcelain=v1"),
        "source_before": before,
        "ownership_requirement": (
            "Caller exclusively owns product paths throughout discovery. Concurrent .reference "
            "writers are permitted. Boundary hashes cannot detect transient edits reverted "
            "between observations. Git-ignored caches are outside the checkout-clean check."
        ),
        "started_at": datetime.now(UTC).isoformat(),
        "python": platform.python_version(),
        "interpreter": sys.executable,
        "sqlite": sqlite3.sqlite_version,
        "command": sys.argv,
        "cwd": str(Path.cwd()),
        "uv_lock_sha256": digest(repo / "uv.lock"),
        "probe_sha256": digest(Path(__file__)),
        "stages": [],
        "scope": "Discovery/projection only. No runtime workload or installed proof executed.",
    }
    raw: dict[str, Any] = {}

    def stage(name: str, operation: Callable[[], Any]) -> Any:
        started = time.perf_counter()
        try:
            result = operation()
        except BaseException as error:
            metadata["stages"].append(
                {
                    "name": name,
                    "elapsed_seconds": time.perf_counter() - started,
                    "status": "failed",
                    "error": f"{type(error).__name__}: {error}",
                }
            )
            raise
        metadata["stages"].append(
            {"name": name, "elapsed_seconds": time.perf_counter() - started, "status": "observed"}
        )
        print(f"observed {name}", flush=True)
        return result

    started = time.perf_counter()
    try:
        import yaml

        from scripts import (
            ci_qualification,
            contract_discovery,
            contract_freeze,
            growth_contract,
            operation_qualification,
            release,
            state_contract,
        )
        from scripts.ci_unit import CI_SHARDS
        from scripts.validation_numeric import numeric_policy_identity, qualification_numeric_policy

        owned = {
            name: Path(str(importlib.import_module(name).__file__)).resolve()
            for name in (
                "riverhog_core",
                "riverhog_client",
                "riverhog_provenance",
                "state_schema",
                "a_riverhog_recovery_tool",
                "stove0_core",
                "gogurt_listener_runtime",
            )
        }
        if any(not path.is_relative_to(repo) for path in owned.values()):
            raise ValueError("an imported product owner belongs to another checkout")
        metadata["loaded_owners"] = {name: str(path) for name, path in owned.items()}
        numeric = qualification_numeric_policy()
        metadata["numeric_policy"] = numeric_policy_identity(numeric)
        config = tomllib.loads((repo / "release.toml").read_text())
        projects = stage(
            "release.validate_release_contract", lambda: release.validate_release_contract(repo)
        )
        raw["projects"] = [asdict(project) for project in projects]
        raw["publication"] = stage(
            "release.publication_contract", lambda: release.publication_contract(repo, projects)
        )
        packages = stage(
            "contract_discovery.python_package_detections",
            lambda: contract_discovery.python_package_detections(repo, projects),
        )
        raw["packages"] = packages
        raw["configuration_documents"] = stage(
            "contract_discovery.discover_configuration_documents",
            lambda: contract_discovery.discover_configuration_documents(repo, projects),
        )
        raw["environment_reads"] = stage(
            "contract_discovery.discover_environment_reads",
            lambda: contract_discovery.discover_environment_reads(repo, projects),
        )
        raw["state"] = stage(
            "state_contract.project_owner",
            lambda: [state_contract.project_owner(owner) for owner in config["state"]["owners"]],
        )
        raw["state_declarations"] = config["state"]["owners"]
        operations = stage(
            "operation_qualification.operation_matrix", operation_qualification.operation_matrix
        )
        raw["operations"] = [asdict(operation) for operation in operations]
        surfaces = stage(
            "contract_freeze._python_surfaces", lambda: contract_freeze._python_surfaces(projects)
        )
        package_paths = {str(item["module"]): repo / str(item["path"]) for item in packages}
        projection = stage(
            "growth_contract.declaration_projection",
            lambda: growth_contract.declaration_projection(
                surfaces, numeric_policy=numeric, packages=package_paths
            ),
        )
        analysis = stage(
            "growth_contract.source_analysis",
            lambda: growth_contract.source_analysis(
                repo, surfaces, projection, numeric_policy=numeric, packages=package_paths
            ),
        )
        emit(output / "coverage_growth_projection.json", projection)
        emit(output / "coverage_growth_sources.json", analysis)
        raw["qualification_workloads"] = stage(
            "ci_qualification.validation_workloads",
            lambda: [asdict(work) for work in ci_qualification.validation_workloads()],
        )
        workflow = yaml.safe_load((repo / ".github/workflows/ci.yml").read_text())
        raw["qualification_routes"] = {
            "repository_targets": ci_qualification.REPOSITORY_TARGETS,
            "compose_lanes": ci_qualification.COMPOSE_LANES,
            "image_groups": ci_qualification.IMAGE_GROUPS,
            "native_test_inputs": ci_qualification.NATIVE_TESTS,
            "native_runners": workflow["jobs"]["client-platforms"]["strategy"]["matrix"]["os"],
            "gate_needs": workflow["jobs"]["gate"]["needs"],
            "unit_shards": CI_SHARDS,
            "platforms": config["platforms"],
        }
        declarations = analysis["declarations"]
        summary: dict[str, object] = {
            "format": "riverhog-reference-coverage-summary/v1",
            "source_sha": source_sha,
            "release_distributions": len(projects),
            "distribution_roles": dict(Counter(project.role for project in projects)),
            "python_package_detections": len(packages),
            "public_python_surfaces": len(surfaces),
            "configuration_validator_detections": len(raw["configuration_documents"]),
            "environment_read_detections": len(raw["environment_reads"]),
            "declared_state_owners": len(raw["state"]),
            "state_classifications": dict(
                Counter(owner["classification"] for owner in raw["state"])
            ),
            "operations": operation_qualification._summary(operations),
            "growth_declarations_including_reachable_delegates": len(declarations),
            "growth_delegate_edges": sum(
                len(value["delegates"]) for value in declarations.values()
            ),
            "growth_surface_status": dict(
                Counter(item["status"] for item in analysis["coverage"].values())
            ),
            "growth_evidence_status": analysis["evidence_status"],
            "qualification_workloads": len(raw["qualification_workloads"]),
            "runtime_images": len(raw["publication"]["runtime_images"]),
            "installation_roots": sorted(raw["publication"]["installation_roots"]),
            "scope": metadata["scope"],
        }
        emit(output / "coverage_summary.json", summary)
        emit(output / "coverage_discovery.json", raw)
        metadata["status"] = "observed"
    except BaseException:
        metadata["status"] = "failed"
        metadata["traceback"] = traceback.format_exc()
        emit(output / "coverage_partial.json", raw)
        raise
    finally:
        metadata["elapsed_seconds"] = time.perf_counter() - started
        metadata["source_status_after"] = git(repo, "status", "--porcelain=v1")
        metadata["source_head_after"] = git(repo, "rev-parse", "HEAD")
        try:
            verify_final_source(repo, source_sha, before, metadata)
        finally:
            emit(output / "coverage_execution.json", metadata)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.repo, args.output, args.source_sha), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
