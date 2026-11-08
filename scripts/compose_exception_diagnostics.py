"""Bounded public-source locations from failed qualification container tracebacks."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import deque
from collections.abc import Iterable
from pathlib import Path
from typing import BinaryIO

MAX_INPUT_BYTES = 1024 * 1024
MAX_LINE_BYTES = 4096
MAX_LINES = 4096
MAX_FRAMES = 12
MAX_ANNOTATION_BYTES = 8192
OWNERS = ("riverhog", "ftp", "processing", "minisign", "opentimestamps")
ERROR_TYPES = frozenset(
    {
        "AssertionError",
        "AttributeError",
        "BadRequest",
        "Conflict",
        "ConnectionError",
        "ExceptionGroup",
        "Forbidden",
        "ImportError",
        "IndexError",
        "IntegrityError",
        "InvalidRequestError",
        "KeyError",
        "ModuleNotFoundError",
        "NameError",
        "NotFound",
        "OperationalError",
        "OSError",
        "PendingRollbackError",
        "ProgrammingError",
        "RuntimeError",
        "StaleDataError",
        "TimeoutError",
        "TypeError",
        "ValueError",
    }
)
FRAME = re.compile(r'File "([^"\r\n]+)", line ([1-9][0-9]{0,7}), in [A-Za-z_<>]')
ERROR = re.compile(
    r"(?:^|\|\s*)(?:[A-Za-z_][A-Za-z0-9_]*\.)*(" + "|".join(sorted(ERROR_TYPES)) + r"):"
)
PREFIX = "::error title=Compose exception locations::"


class PublicSources:
    """Accept only exact tracked Python locations, never a log-supplied path."""

    def __init__(self, root: Path, files: Iterable[str]) -> None:
        self.root = root
        self.by_name: dict[str, list[tuple[str, str]]] = {}
        self.line_counts: dict[str, int] = {}
        for name in files:
            path = Path(name)
            if (
                path.is_absolute()
                or ".." in path.parts
                or path.suffix != ".py"
                or len(name) > 240
                or any(ord(char) < 32 for char in name)
            ):
                continue
            if "/src/" in name:
                suffix = name.rsplit("/src/", 1)[1]
            elif name.startswith(("scripts/", "tests/harness/")):
                suffix = name
            else:
                continue
            self.by_name.setdefault(path.name, []).append((suffix, name))

    def resolve(self, location: str, line: int) -> dict[str, str | int] | None:
        normalized = location.replace("\\", "/")
        for suffix, name in self.by_name.get(normalized.rsplit("/", 1)[-1], ()):
            if normalized != suffix and not normalized.endswith("/" + suffix):
                continue
            if name not in self.line_counts:
                try:
                    path = (self.root / name).resolve()
                    if not path.is_relative_to(self.root.resolve()):
                        continue
                    with path.open("rb") as source:
                        self.line_counts[name] = sum(1 for _ in source)
                except OSError:
                    self.line_counts[name] = 0
            if 0 < line <= self.line_counts[name]:
                return {"path": name, "line": line}
        return None


def observe(stream: BinaryIO, sources: PublicSources, owner: str) -> dict[str, object] | None:
    if owner not in OWNERS:
        raise ValueError("unknown qualification owner")
    frames: deque[dict[str, str | int]] = deque(maxlen=MAX_FRAMES)
    types: set[str] = set()
    consumed = lines = 0
    oversized = truncated = False
    while consumed < MAX_INPUT_BYTES and lines < MAX_LINES:
        raw = stream.readline(min(MAX_LINE_BYTES + 1, MAX_INPUT_BYTES - consumed))
        if not raw:
            break
        consumed += len(raw)
        complete = raw.endswith(b"\n") or len(raw) <= MAX_LINE_BYTES
        if oversized or len(raw) > MAX_LINE_BYTES:
            truncated = True
            oversized = not complete
            if complete:
                lines += 1
            continue
        lines += 1
        text = raw.decode("utf-8", errors="replace")
        match = FRAME.search(text)
        if match:
            frame = sources.resolve(match[1], int(match[2]))
            if frame is not None:
                if frame in frames:
                    frames.remove(frame)
                frames.append(frame)
        error = ERROR.search(text)
        if error:
            types.add(error[1])
    truncated = truncated or consumed >= MAX_INPUT_BYTES or lines >= MAX_LINES
    if not frames and not types:
        return None
    return {
        "format": "riverhog-compose-exception-locations/v1",
        "owner": owner,
        "frames": list(frames),
        "error_types": sorted(types),
        "sample_truncated": truncated,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner", required=True, choices=OWNERS)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    listed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--", "*.py"],
        capture_output=True,
        check=False,
    )
    if listed.returncode:
        return 0
    sources = PublicSources(root, listed.stdout.decode("utf-8", errors="replace").split("\0"))
    result = observe(sys.stdin.buffer, sources, args.owner)
    if result is not None:
        body = json.dumps(result, sort_keys=True, separators=(",", ":"))
        if len(body.encode()) <= MAX_ANNOTATION_BYTES:
            print(PREFIX + body, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
