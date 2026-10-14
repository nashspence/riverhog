"""Execute an isolated, exact pinned route body with counting dependency doubles.

Characterization only, not a product/ASGI/network test. Remove decorators and
postpone annotations; the function body is intact. No other product code executes.
"""
from __future__ import annotations

import argparse
import ast
import asyncio
import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

AUDITED = "e0036f80410a83cc5eaa9d63c17650ffae72e2bd"
PATH = "riverhog/src/riverhog_api/routers/collections.py"
FUNCTION = "put_collection_upload_session_unit"


def characterize(repo: Path) -> dict:
    source = subprocess.check_output(
        ["git", "-C", str(repo), "show", f"{AUDITED}:{PATH}"], text=True)
    blob = hashlib.sha1(b"blob " + str(len(source.encode())).encode()
                        + b"\0" + source.encode()).hexdigest()
    node = next(n for n in ast.parse(source).body
                if isinstance(n, ast.AsyncFunctionDef) and n.name == FUNCTION)
    node.decorator_list = []
    module = ast.Module(body=[ast.ImportFrom(
        module="__future__", names=[ast.alias(name="annotations")], level=0), node],
        type_ignores=[])
    ast.fix_missing_locations(module)
    events = []
    class Unavailable(Exception):
        pass
    class Request:
        headers = {"content-length": "32"}
        async def stream(self):
            events.append("source-read:32")
            yield b"x" * 32
    class Uploads:
        def require_access(self, *args):
            events.append("authorize")
        def get_unit(self, *args):
            events.append("get-unit-metadata")
            return {"payload_bytes": "32"}
        def upload_unit(self, *args, **kwargs):
            events.append("storage-unavailable")
            raise Unavailable()
    async def thread(function, *args, **kwargs):
        return function(*args, **kwargs)
    namespace = {"run_in_threadpool": thread,
                 "parse_scalar": lambda _, value: int(value),
                 "parse_quoted_sha256_identity": lambda value: value}
    exec(compile(module, str(repo / PATH), "exec"), namespace)
    async def run():
        try:
            await namespace[FUNCTION](1, "volume", 0, Request(),
                SimpleNamespace(collection_uploads=Uploads()), None, "a" * 64)
        except Unavailable:
            return
        raise AssertionError("expected storage wait was not reached")
    asyncio.run(run())
    assert events == ["authorize", "get-unit-metadata",
                      "source-read:32", "storage-unavailable"], events
    return {"source_ref": AUDITED, "path": PATH, "git_blob": blob,
            "function": FUNCTION, "lines": [node.lineno, node.end_lineno],
            "events": events, "consumed_before_storage_wait": 32,
            "interpretation": "current gap; NOT a no-consumption acceptance pass"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(characterize(args.repo), indent=2))
