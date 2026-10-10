"""Instrument the installed NVENC executable's admission boundary, without a GPU.

Only live Riverhog authority checks and the first native-tool execution are
replaced. The image's parser, service, durable queue, dispatcher, file admission,
HTTP binding, preflight and shutdown are the maintained implementations. This is
control qualification; it makes no claim about real GPU encoding or custody.
"""

from __future__ import annotations

import fcntl
import os
import threading
from pathlib import Path
from typing import Any

from a_stove0_nvenc_av1_opus_target import app as target_app
from a_stove0_nvenc_av1_opus_target import target as nvenc_target
from riverhog_canonical_json import canonical_json_bytes
from stove0_target_support import TargetExecutionInapplicable

ROOT = Path("/fixture")
LEASE = Path(os.environ["A_STOVE0_NVENC_AV1_OPUS_TARGET_EXECUTION_LEASE_FILE"])
STOP = threading.Event()
SERVICES: list[Any] = []
ORIGINAL = target_app.NvencAv1OpusTargetService


def write(name: str, document: dict[str, Any]) -> None:
    temporary = ROOT / (name + ".tmp")
    temporary.write_bytes(canonical_json_bytes(document))
    temporary.replace(ROOT / name)


def monitor(service: Any, admission: Any) -> None:
    while not STOP.is_set():
        try:
            inode = os.fstat(admission._descriptor)
        except OSError:
            if admission._descriptor < 0:
                return
            raise
        write(
            "control.json",
            {
                "payload_consumers": service._dispatch.payload_count,
                "sessions": len(service._sessions),
                "workspace_entries": len(tuple(service.workspace_root.iterdir())),
                "lease_device": inode.st_dev,
                "lease_inode": inode.st_ino,
            },
        )
        STOP.wait(0.02)


def construct(**kwargs: Any) -> Any:
    service = ORIGINAL(**kwargs)
    SERVICES.append(service)
    threading.Thread(
        target=monitor, args=(service, kwargs["execution_admission"]), daemon=True
    ).start()
    return service


def guarded_native_boundary(_command: str) -> str:
    with LEASE.open("r+b") as independent:
        try:
            fcntl.flock(independent, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            raise AssertionError("Native execution began without retaining its cooperative lease")
    write("native-boundary.json", {"entered": True, "lease_retained": True})
    raise TargetExecutionInapplicable("fixture-stop", "Reached the guarded native boundary.")


def main() -> int:
    ORIGINAL._validate_live_authority = lambda *_args, **_kwargs: None
    target_app.NvencAv1OpusTargetService = construct
    nvenc_target.tool_version = guarded_native_boundary
    try:
        return target_app.target_main(["--host", "0.0.0.0", "--port", "8080"])
    finally:
        STOP.set()


if __name__ == "__main__":
    raise SystemExit(main())
