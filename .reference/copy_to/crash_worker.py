"""Subprocess-only fault injector. SIGKILL bypasses Python and DB cleanup."""
from __future__ import annotations
import os
import signal
import sys
from harness import Harness


def kill() -> None:
    os.kill(os.getpid(), signal.SIGKILL)


def main() -> None:
    path, phase = sys.argv[1:]
    h = Harness(path, create=False, fault=lambda p: kill() if p == phase else None)
    if phase in {"before_accept_commit", "after_accept_commit"}:
        with h.sessions.begin() as session:
            h.accept(session)
            if phase == "before_accept_commit":
                kill()
        kill()
    elif phase in {"before_publish_commit", "after_publish_commit"}:
        with h.sessions.begin() as session:
            h.publish(session)
            if phase == "before_publish_commit":
                kill()
        kill()
    elif phase == "normal":
        h.service.process_due(h.sessions)
    else:
        with h.sessions.begin() as session:
            h.service.handoff_one(session, 1, "copy-a")
            if phase == "before_handoff_commit":
                kill()
        if phase == "after_handoff_commit":
            kill()
    h.close()


if __name__ == "__main__":
    main()
