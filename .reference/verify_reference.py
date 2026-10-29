"""Check the patch and execute only its two newly authored files in isolation."""
from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

BASE = "209481c189f7387557a85e1c249184642816e103"
ROOT = Path(__file__).resolve().parents[1]
NEW = ("scripts/validation_timeouts.py", "tests/unit/test_validation_timeouts.py")
MIGRATED = ("tests/unit/test_api_client_downloads.py", "tests/unit/test_api_client_uploads.py")


class UndoWatchdogMigration(ast.NodeTransformer):
    def __init__(self) -> None:
        self.replacements = 0

    def visit_ImportFrom(self, node: ast.ImportFrom):
        return None if node.module == "scripts.validation_timeouts" else node

    def visit_Assign(self, node: ast.Assign):
        if (
            len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "timeouts"
        ):
            assert ast.unparse(node.value) == "ValidationTimeouts.from_environment()"
            return None
        return self.generic_visit(node)

    def visit_keyword(self, node: ast.keyword):
        if node.arg == "timeout" and ast.unparse(node.value) == "timeouts.thread_seconds(2)":
            self.replacements += 1
            node.value = ast.Constant(value=2)
        return self.generic_visit(node)


def main() -> int:
    patch = (ROOT / ".reference/first-slice.patch").read_bytes()
    paths = re.findall(rb"^diff --git a/(\S+) b/\S+$", patch, flags=re.MULTILINE)
    if set(path.decode() for path in paths) != set(NEW + MIGRATED) or len(paths) != 4:
        raise ValueError("unexpected patch scope")
    report = {"audited_base": BASE, "repository_tests_executed": False, "runs": []}
    with tempfile.TemporaryDirectory(prefix="riverhog-reference-") as directory:
        temporary = Path(directory)
        patched = temporary / "patch-check"
        patched.mkdir()
        originals = {}
        for name in MIGRATED:
            raw = subprocess.check_output(["git", "show", BASE + ":" + name], cwd=ROOT)
            originals[name] = raw.decode()
            destination = patched / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(raw)
        for options in (("--check",), ()):
            subprocess.run(
                ["git", "apply", "--whitespace=error-all", *options, "-"],
                input=patch, cwd=patched, check=True, capture_output=True,
            )
        count = 0
        for name in NEW + MIGRATED:
            ast.parse((patched / name).read_text(), filename=name)
        for name in MIGRATED:
            before = ast.parse(originals[name])
            after = ast.parse((patched / name).read_text())
            normalizer = UndoWatchdogMigration()
            after = normalizer.visit(after)
            if ast.dump(before, include_attributes=False) != ast.dump(after, include_attributes=False):
                raise ValueError("migration changes more than watchdog plumbing: " + name)
            count += normalizer.replacements
        if count != 7:
            raise ValueError("expected seven incidental thread wait sites")
        report["migration_ast_equivalence"] = {"files": 2, "watchdog_sites": count}
        isolated = temporary / "isolated"
        for name in NEW:
            destination = isolated / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes((patched / name).read_bytes())
        (isolated / "scripts/__init__.py").write_text("")
        for scale in ("1", "4", "0.5"):
            process = subprocess.run(
                [sys.executable, "-m", "unittest", "discover", "-s", "tests/unit", "-v"],
                cwd=isolated,
                env={**os.environ, "PYTHONPATH": str(isolated),
                     "RIVERHOG_VALIDATION_TIMEOUT_SCALE": scale},
                capture_output=True, text=True, timeout=60 * float(scale), check=False,
            )
            match = re.search(r"Ran (\d+) tests in ([0-9.]+)s", process.stderr)
            if process.returncode or match is None or int(match[1]) != 16:
                raise RuntimeError(process.stdout + process.stderr)
            report["runs"].append({
                "scale": scale, "tests": int(match[1]), "exit_status": process.returncode,
                "elapsed_seconds_observed": float(match[2]),
            })
    report["interpreter"] = sys.version.split()[0]
    report["limits"] = "No repository imports, pytest, installed artifacts, database, browser or CI run"
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
