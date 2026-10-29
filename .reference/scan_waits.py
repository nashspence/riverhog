"""Read-only lexical candidate discovery; not a semantic classification or policy gate."""
from __future__ import annotations

import argparse
import json
import subprocess
from collections import Counter
from pathlib import Path

PATTERN = (
    r"timeout|deadline|monotonic|perf_counter|Execution Time|"
    r"\.(wait|wait_for|join|result|communicate|sleep)\(|"
    r"(start_period|start_interval|healthcheck|retries):|"
    r"sleep [0-9]|--max-time|--connect-timeout|--wait|"
    r"range\([0-9]|seq [0-9]|\.goto\(|new_context\("
)
PATHS = ("*.py", "*.sh", "*.ps1", "*.yaml", "*.yml", "*.toml", "Makefile")


def family(path: str) -> str:
    parts = Path(path).parts
    if "tests" in parts or path == "conftest.py":
        return "tests-and-harnesses"
    if parts[0] == "scripts":
        return "validation-and-other-tooling"
    if parts[0] == ".github" or "compose" in Path(path).name or path == "Makefile":
        return "orchestration"
    return "runtime-or-other-context"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--ref", default="209481c189f7387557a85e1c249184642816e103")
    parser.add_argument("--details", action="store_true")
    args = parser.parse_args()
    if args.ref.startswith("-"):
        parser.error("ref must not start with an option prefix")
    sha = subprocess.check_output(
        ["git", "rev-parse", "--verify", args.ref + "^{commit}"], cwd=args.root, text=True
    ).strip()
    result = subprocess.run(
        ["git", "grep", "-n", "-i", "-I", "-E", PATTERN, sha, "--", *PATHS],
        cwd=args.root, text=True, capture_output=True, check=False,
    )
    if result.returncode not in (0, 1):
        raise RuntimeError(result.stderr)
    rows = []
    counts: Counter[str] = Counter()
    for line in result.stdout.splitlines():
        _ref, path, number, excerpt = line.split(":", 3)
        if path.startswith((".reference/", "third_party/")):
            continue
        counts[path] += 1
        rows.append({"path": path, "line": int(number), "text": excerpt})
    groups = Counter(family(path) for path in counts)
    output = {
        "source_sha": sha,
        "method": "lexical candidates; false positives and omissions require manual review",
        "candidate_files": len(counts),
        "candidate_lines": len(rows),
        "file_families": dict(sorted(groups.items())),
        "files": [{"path": path, "lines": count} for path, count in sorted(counts.items())],
    }
    if args.details:
        output["matches"] = rows
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
