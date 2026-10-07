"""Private local launcher: an EOF cancels consumers; the inherited owner stays held."""

from __future__ import annotations

import ctypes
import json
import os
import select
import signal
import subprocess
import sys
import time
from types import FrameType


def _stop_group(process: subprocess.Popen[bytes]) -> None:
    started = time.monotonic()
    killed = False
    while True:
        process.poll()
        if process.returncode is not None:
            # Linux subreaper adopts the tool's descendants. Reap only after
            # Popen has reaped its own child, preserving the actual tool result.
            while True:
                try:
                    child, _ = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:
                    break
                if child == 0:
                    break
        try:
            os.killpg(process.pid, signal.SIGKILL if killed else signal.SIGTERM)
        except ProcessLookupError:
            return
        if time.monotonic() - started >= 5:
            killed = True
        time.sleep(0.02)


def main() -> int:
    control = int(sys.argv[1])
    canceled = False

    def cancel(_signal: int, _frame: FrameType | None) -> None:
        nonlocal canceled
        canceled = True

    signal.signal(signal.SIGTERM, cancel)
    signal.signal(signal.SIGINT, cancel)
    if sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
            raise OSError(ctypes.get_errno(), "payload subreaper initialization failed")
    with os.fdopen(control, "rb", buffering=0) as channel:
        # Commands/capabilities never enter the launcher's argv or durable state.
        line = bytearray()
        while not line.endswith(b"\n"):
            byte = channel.read(1)
            if not byte:
                return 125
            line.extend(byte)
        command = json.loads(line)
        if select.select([channel], [], [], 0)[0] or canceled:
            return 125
        process = subprocess.Popen(command, start_new_session=True)
        try:
            while process.poll() is None:
                if canceled or select.select([channel], [], [], 0.05)[0]:
                    break
        finally:
            _stop_group(process)
            process.wait()
        return process.returncode if process.returncode >= 0 else 128 - process.returncode


if __name__ == "__main__":
    raise SystemExit(main())
