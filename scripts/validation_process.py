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

    def interrupt(signum: int, _frame: FrameType | None) -> None:
        raise ValidationInterrupted(signum)

    try:
        if threading.current_thread() is threading.main_thread():
            for installed_signal in signals:
                handlers[installed_signal] = signal.signal(installed_signal, interrupt)
        child = subprocess.Popen(command, cwd=cwd, env=env, start_new_session=os.name == "posix")
        try:
            return child.wait()
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
