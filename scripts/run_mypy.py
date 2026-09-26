"""Type-check every uv workspace source root and the configured support scripts."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def mypy_paths(root: Path) -> list[str]:
    config = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    sources: set[str] = set()
    for pattern in config["tool"]["uv"]["workspace"]["members"]:
        for member in root.glob(pattern):
            if not (member / "pyproject.toml").is_file():
                raise ValueError(f"workspace member lacks pyproject.toml: {member}")
            if (member / "src").is_dir():
                sources.add((member / "src").relative_to(root).as_posix())
    return [*sorted(sources), *config["tool"]["mypy"]["files"]]


def main(argv: list[str]) -> int:
    return subprocess.run(
        [sys.executable, "-m", "mypy", *mypy_paths(ROOT), *argv], cwd=ROOT, check=False
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
