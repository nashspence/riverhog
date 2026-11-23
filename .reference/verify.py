"""Reproduce the focused reference against its exact isolated experiment source."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

SOURCE = "0b25c7c67f7b3ee8787d8d753d4b6d585e8cea1f"
PREVIOUS_REFERENCE = "3dce6780c4883ad96cb32ff3f7ef80883fef6528"
PHYSICAL = "riverhog/src/riverhog_core/services/physical_execution.py"
WORKER = "packages/retained-resource/src/retained_resource/worker.py"
NATIVE = "packages/retained-resource/native/worker.c"
TESTS = (
    "packages/retained-resource/tests",
    "tests/unit/test_archive_upload_execution.py",
    "tests/unit/test_physical_workspace_execution.py",
    "tests/unit/test_archive_payload_input.py",
    "tests/unit/test_archive_upload_terminal_registry.py",
    "tests/unit/test_discovery_retirement_registry.py",
    "tests/unit/test_journal_retirement_registry.py",
    "tests/unit/test_physical_workspace_execution_growth.py",
    "tests/unit/test_ci_qualification.py",
    "tests/unit/test_native_make_evidence.py",
    "tests/unit/test_workspace_boundaries.py::test_implementation_projects_do_not_cross_owner_boundaries",
    "tests/unit/test_workspace_boundaries.py::test_shared_packages_do_not_import_implementation_projects",
    "tests/unit/test_workspace_boundaries.py::test_core_dependency_graphs_are_acyclic",
    "tests/unit/test_dependency_policy.py",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument(
        "--broader", action="store_true", help="include all workspace-boundary tests"
    )
    args = parser.parse_args()
    root = Path.cwd().resolve()
    packet = Path(__file__).resolve().parent
    output = args.evidence.resolve()
    if output == root or root in output.parents:
        parser.error("put evidence outside the isolated source checkout")
    if sys.implementation.name != "cpython" or sys.version_info[:3] != (3, 12, 3):
        parser.error("use the repository's pinned CPython 3.12.3 environment")
    if sys.platform != "linux":
        parser.error("this exact before/after qualification requires Linux")
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if source != SOURCE:
        parser.error(f"expected isolated source {SOURCE}; found {source}")
    manifest = json.loads((packet / "proposal-manifest.json").read_text())
    if manifest["source_sha"] != SOURCE:
        parser.error("proposal manifest names a different experiment source")
    patch_sha256 = hashlib.sha256((packet / "implementation.patch").read_bytes()).hexdigest()
    if patch_sha256 != manifest["patch_sha256"]:
        parser.error("implementation patch differs from its recorded digest")
    expected = set(manifest["files"])
    tracked = subprocess.check_output(["git", "diff", "--name-only", "HEAD"], text=True)
    untracked = subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard"], text=True
    )
    observed = set(tracked.splitlines()) | set(untracked.splitlines())
    if observed != expected:
        parser.error(f"proposal paths differ: {sorted(observed ^ expected)}")
    for relative, entry in manifest["files"].items():
        if hashlib.sha256((root / relative).read_bytes()).hexdigest() != entry["sha256"]:
            parser.error(f"proposal content differs: {relative}")
    uv_version = subprocess.check_output(["uv", "--version"], text=True).strip()
    if uv_version.split()[1] != "0.11.24":
        parser.error(f"use the repository's pinned uv 0.11.24; found {uv_version}")
    output.mkdir(parents=True, exist_ok=False)
    report = {
        "source": SOURCE,
        "previous_reference": PREVIOUS_REFERENCE,
        "suite_scope": "broader" if args.broader else "focused",
        "patch_sha256": manifest["patch_sha256"],
        "python": sys.version,
        "uv_version": uv_version,
        "uv_executable": shutil.which("uv"),
        "uv_cache_dir": os.environ.get("UV_CACHE_DIR"),
        "verifier_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "platform": platform.platform(),
        "libc": platform.libc_ver(),
        "checks": [],
        "baseline_observations": [],
        "previous_startup_observations": [],
    }

    def save() -> None:
        (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")

    def run(
        name: str,
        command: list[str],
        *,
        expected_code: int = 0,
        cwd: Path = root,
        input_text: str | None = None,
    ) -> str:
        started = time.monotonic()
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=os.environ.copy(),
            text=True,
            input=input_text,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=1800 if args.broader else 900,
        )
        elapsed = time.monotonic() - started
        (output / f"{name}.log").write_text(completed.stdout)
        report["checks"].append(
            {
                "name": name,
                "command": command,
                "cwd": str(cwd),
                "exit_code": completed.returncode,
                "expected_exit_code": expected_code,
                "elapsed_seconds": round(elapsed, 3),
            }
        )
        save()
        print(f"{name}: exit {completed.returncode}, {elapsed:.3f}s", flush=True)
        if completed.returncode != expected_code:
            print(completed.stdout, flush=True)
            raise SystemExit(f"unexpected result; see {output / (name + '.log')}")
        return completed.stdout

    baseline = output / "original-physical-execution.py"
    baseline.write_bytes(subprocess.check_output(["git", "show", f"{SOURCE}:{PHYSICAL}"]))
    report["baseline_owner_sha256"] = hashlib.sha256(baseline.read_bytes()).hexdigest()
    # Execute the unchanged owner, not a synthetic bad-backend implementation.
    for kind, tail, outcome in itertools.product(
        ("collection", "workspace"), ("target", "thread-local"), ("success", "failure")
    ):
        name = f"baseline-{kind}-{tail}-{outcome}"
        command = [
            sys.executable,
            "-m",
            "tests.support.physical_exit_probe",
            "--kind",
            kind,
            "--tail",
            tail,
            "--outcome",
            outcome,
            "--implementation",
            str(baseline),
            "--lease-root",
            str(output / (name + "-lease")),
            "--expect",
            "unsafe",
        ]
        observation = json.loads(run(name, command).splitlines()[-1])
        report["baseline_observations"].append(observation)
        save()
    rejected = run(
        "baseline-required-safety-fails",
        [
            sys.executable,
            "-m",
            "tests.support.physical_exit_probe",
            "--kind",
            "workspace",
            "--tail",
            "target",
            "--outcome",
            "success",
            "--implementation",
            str(baseline),
            "--expect",
            "safe",
        ],
        expected_code=1,
    )
    assert '"safe": false' in rejected and "AssertionError" in rejected

    # Materialize the exact prior proposed Python wrapper and Core owner from
    # their immutable reference, never a synthetic waiting replacement. Its
    # native implementation must remain identical for this causal comparison.
    prior_manifest = json.loads(
        subprocess.check_output(
            ["git", "show", f"{PREVIOUS_REFERENCE}:.reference/proposal-manifest.json"]
        )
    )
    prior_patch = subprocess.check_output(
        ["git", "show", f"{PREVIOUS_REFERENCE}:.reference/implementation.patch"]
    )
    assert hashlib.sha256(prior_patch).hexdigest() == prior_manifest["patch_sha256"]
    assert manifest["files"][NATIVE]["sha256"] == prior_manifest["files"][NATIVE]["sha256"]
    previous = output / "previous-proposal"
    (previous / PHYSICAL).parent.mkdir(parents=True)
    (previous / PHYSICAL).write_bytes(
        subprocess.check_output(["git", "show", f"{prior_manifest['source_sha']}:{PHYSICAL}"])
    )
    run("previous-proposal-init", ["git", "init", "--quiet"], cwd=previous)
    run(
        "previous-proposal-materialize",
        ["git", "apply", f"--include={WORKER}", f"--include={PHYSICAL}"],
        cwd=previous,
        input_text=prior_patch.decode(),
    )
    for path in (WORKER, PHYSICAL):
        assert (
            hashlib.sha256((previous / path).read_bytes()).hexdigest()
            == prior_manifest["files"][path]["sha256"]
        )
    report["previous_proposal"] = {
        "reference_sha": PREVIOUS_REFERENCE,
        "source_sha": prior_manifest["source_sha"],
        "patch_sha256": prior_manifest["patch_sha256"],
        "files_verified": {
            path: prior_manifest["files"][path] for path in (WORKER, PHYSICAL, NATIVE)
        },
    }
    for kind in ("collection", "workspace"):
        command = [
            sys.executable,
            "-m",
            "tests.support.physical_bootstrap_probe",
            "--mode",
            "held",
            "--kind",
            kind,
            "--worker",
            str(previous / WORKER),
            "--implementation",
            str(previous / PHYSICAL),
            "--expect",
            "unsafe",
        ]
        observation = json.loads(run(f"previous-startup-{kind}", command).splitlines()[-1])
        report["previous_startup_observations"].append(observation)
        save()
    command[-1] = "safe"
    rejected = run("previous-startup-required-safety-fails", command, expected_code=1)
    assert '"safe": false' in rejected and "AssertionError" in rejected

    python_files = sorted(path for path in expected if path.endswith((".py", ".pyi")))
    run("diff-check", ["git", "diff", "--check"])
    run("ruff", [sys.executable, "-m", "ruff", "check", *python_files])
    run("format", [sys.executable, "-m", "ruff", "format", "--check", *python_files])
    run(
        "mypy",
        [
            sys.executable,
            "-m",
            "mypy",
            "packages/retained-resource/src/retained_resource",
            PHYSICAL,
            "riverhog/src/riverhog_core/services/physical_workspace_growth.py",
        ],
    )
    # Use the unchanged source-owned numeric resource identity and evidence model.
    # Legacy JUnit supports record_property while warnings remain errors.
    sys.path.insert(0, str(root))
    from scripts.validation_numeric import bind_numeric_environment, qualification_numeric_policy

    conditions = dict(os.environ)
    bind_numeric_environment(conditions, qualification_numeric_policy())
    os.environ.update(conditions)
    tests = TESTS
    if args.broader:
        tests = tuple(
            path for path in TESTS if not path.startswith("tests/unit/test_workspace_boundaries.py")
        )
        tests += ("tests/unit/test_workspace_boundaries.py",)
    scope = "broader" if args.broader else "focused"
    run(
        f"{scope}-tests",
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-o",
            "junit_family=legacy",
            f"--junitxml={output / (scope + '-tests.xml')}",
            "--maxfail=1",
            *tests,
        ],
    )
    for relative, entry in manifest["files"].items():
        if hashlib.sha256((root / relative).read_bytes()).hexdigest() != entry["sha256"]:
            raise SystemExit(f"source changed during validation: {relative}")
    final_source = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    final_paths = set(
        subprocess.check_output(["git", "diff", "--name-only", "HEAD"], text=True).splitlines()
    ) | set(
        subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard"], text=True
        ).splitlines()
    )
    if final_source != SOURCE or final_paths != expected:
        raise SystemExit("source identity or proposed path set changed during validation")
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != report["verifier_sha256"]:
        raise SystemExit("verifier changed during validation")
    report["completed"] = True
    save()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
