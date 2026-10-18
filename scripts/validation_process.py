"""Join interrupted validation owners through their cleanup, independently of speed goals."""

from __future__ import annotations

import os
import signal
import subprocess
import threading
from collections.abc import Sequence
from pathlib import Path
from types import FrameType


class ValidationInterrupted(KeyboardInterrupt):
    def __init__(self, signum: int) -> None:
        self.signum = signum
        super().__init__(signum)


def run_validation_child(command: Sequence[str], *, cwd: Path, env: dict[str, str]) -> int:
    """Execute once; cancellation waits for the owner instead of declaring it stopped.

    POSIX children share an owned process group so the cancellation reaches the
    command's active descendants. Cleanup is not bounded by an advisory goal.
    Signal handlers belong only to the main thread and are always restored.
    """
    child: subprocess.Popen[bytes] | None = None
    handlers = {}
    signals = (signal.SIGINT, signal.SIGTERM)
    requested_signal: int | None = None

    def interrupt(signum: int, _frame: FrameType | None) -> None:
        nonlocal requested_signal
        # Python can deliver a signal between acquiring a subprocess lock and
        # entering its protected block. Raising there can strand the lock and
        # deadlock the required join. Record intent; act at a safe boundary.
        if requested_signal is None:
            requested_signal = signum

    try:
        if threading.current_thread() is threading.main_thread():
            for installed_signal in signals:
                handlers[installed_signal] = signal.signal(installed_signal, interrupt)
        child = subprocess.Popen(command, cwd=cwd, env=env, start_new_session=os.name == "posix")
        try:
            while True:
                if requested_signal is not None:
                    raise ValidationInterrupted(requested_signal)
                try:
                    status = child.wait(timeout=0.1)
                except subprocess.TimeoutExpired:
                    continue
                if requested_signal is not None:
                    raise ValidationInterrupted(requested_signal)
                return status
        except KeyboardInterrupt as error:
            signum = error.signum if isinstance(error, ValidationInterrupted) else signal.SIGINT
            # Repeated cancellation cannot skip joining the command's cleanup.
            for owned_signal in handlers:
                signal.signal(owned_signal, signal.SIG_IGN)
            if child.poll() is None:
                try:
                    if os.name == "posix":
                        os.killpg(child.pid, signum)
                    elif signum == signal.SIGINT:
                        # The Windows console also delivers Ctrl-C to the child.
                        pass
                    else:
                        child.terminate()
                except ProcessLookupError:
                    pass
            child.wait()
            raise ValidationInterrupted(signum) from error
    finally:
        for restored_signal, handler in handlers.items():
            signal.signal(restored_signal, handler)
