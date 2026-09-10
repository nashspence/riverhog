"""Test-only private-layout instrumentation for offline read-isolation proof."""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def set_access(
    root: Path,
    allowed: frozenset[str] | None,
    prefixes: tuple[str, ...],
) -> None:
    for path_file in root.glob("objects/*/*/*/path"):
        logical = path_file.read_text(encoding="utf-8")
        object_dir = path_file.parent
        current = object_dir / "current"
        if not current.is_file():
            continue
        revision = current.read_text(encoding="utf-8")
        payload = object_dir / "revisions" / revision / "payload.data"
        if not payload.is_file():
            raise RuntimeError(f"current filesystem object has no payload: {logical}")
        permitted = allowed is None or logical in allowed or logical.startswith(prefixes)
        os.chmod(payload, 0o600 if permitted else 0o000)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("paths", nargs="*")
    parser.add_argument("--prefix", action="append", default=[])
    parser.add_argument("--restore", action="store_true")
    args = parser.parse_args()
    set_access(
        args.root,
        None if args.restore else frozenset(args.paths),
        tuple(args.prefix),
    )


if __name__ == "__main__":
    main()
