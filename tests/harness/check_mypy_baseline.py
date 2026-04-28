from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
BASELINE_PATH = ROOT_DIR / "tests" / "harness" / "mypy-baseline.txt"
SIGNATURE_PATTERN = re.compile(
    r"^(?P<path>.+?):(?P<line>\d+): error: (?P<message>.+)  \[(?P<code>[^\]]+)\]$"
)
IMPORT_MESSAGE_PATTERNS = (
    re.compile(
        r'^Cannot find implementation or library stub for module named "(?P<module>.+)"$'
    ),
    re.compile(
        r'^Skipping analyzing "(?P<module>.+)": module is installed, but missing '
        r'library stubs or py.typed marker$'
    ),
    re.compile(r'^Library stubs not installed for "(?P<module>.+)"$'),
)
MYPY_COMMAND = [
    "mypy",
    "src",
    "--show-error-codes",
    "--hide-error-context",
    "--no-error-summary",
    "--no-color-output",
]


def normalized_signature(line: str) -> str:
    match = SIGNATURE_PATTERN.match(line)
    if match is None:
        raise ValueError(f"unrecognized mypy output line: {line}")
    path = match.group("path")
    code = match.group("code")
    message = match.group("message")
    if code in {"import-not-found", "import-untyped"}:
        for pattern in IMPORT_MESSAGE_PATTERNS:
            import_match = pattern.match(message)
            if import_match is not None:
                module = import_match.group("module")
                return f"{path}\t[import-missing-typing]\t{module}"
    return f"{path}\t[{code}]\t{message}"


def load_baseline() -> Counter[str]:
    signatures: list[str] = []
    for raw_line in BASELINE_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        signatures.append(line)
    return Counter(signatures)


def write_baseline(signatures: Counter[str]) -> None:
    lines = [
        "# Normalized mypy regression baseline.",
        "# Format: path<TAB>[error-code]<TAB>message",
    ]
    lines.extend(sorted(signatures.elements()))
    BASELINE_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_mypy() -> tuple[Counter[str], str]:
    result = subprocess.run(
        MYPY_COMMAND,
        cwd=ROOT_DIR,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.stderr.strip():
        raise RuntimeError(f"mypy wrote unexpected stderr output:\n{result.stderr.rstrip()}")
    if result.returncode not in {0, 1}:
        raise RuntimeError(
            f"mypy exited with status {result.returncode}:\n{result.stdout.rstrip()}"
        )

    signatures: list[str] = []
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if not line or ": note: " in line:
            continue
        signatures.append(normalized_signature(line))
    return Counter(signatures), result.stdout


def regression_lines(
    baseline: Counter[str], current: Counter[str]
) -> list[tuple[str, int, int]]:
    regressions: list[tuple[str, int, int]] = []
    for signature in sorted(current):
        baseline_count = baseline.get(signature, 0)
        current_count = current[signature]
        if current_count > baseline_count:
            regressions.append((signature, baseline_count, current_count))
    return regressions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check strict mypy output against the checked-in regression baseline."
    )
    parser.add_argument(
        "--write-baseline",
        action="store_true",
        help="replace the checked-in baseline with the current normalized mypy output",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    current, raw_output = run_mypy()
    if args.write_baseline:
        write_baseline(current)
        print(
            f"wrote mypy baseline with {sum(current.values())} normalized signatures to "
            f"{BASELINE_PATH.relative_to(ROOT_DIR)}"
        )
        return 0

    baseline = load_baseline()
    regressions = regression_lines(baseline, current)
    if regressions:
        print("mypy regression baseline check failed.")
        print("New or expanded normalized signatures:")
        for signature, baseline_count, current_count in regressions:
            print(f"- {signature} (baseline {baseline_count}, current {current_count})")
        print()
        print("Raw mypy output:")
        print(raw_output.rstrip())
        return 1

    print(
        "mypy baseline check passed: "
        f"{sum(current.values())} current normalized signatures within "
        f"{sum(baseline.values())} baseline signatures."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
