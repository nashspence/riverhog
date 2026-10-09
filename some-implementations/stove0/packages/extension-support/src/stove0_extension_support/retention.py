"""Process-owned periodic maintenance of component-local terminal records."""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path

_LOG = logging.getLogger(__name__)


class TerminalStateRetention:
    """Scan lazily outside control handlers, yielding between finite batches.

    The owning component decides eligibility and removes one exact job under
    its own state fence. No result, cancellation or effect policy lives here.
    """

    def __init__(
        self,
        root: Path,
        prune: Callable[[Path, float], object],
        *,
        interval_seconds: float,
    ) -> None:
        self.root, self.prune = root, prune
        self.interval_seconds = interval_seconds
        self._stop, self._wake = threading.Event(), threading.Event()
        self._thread = threading.Thread(target=self._loop, name="extension-retention", daemon=True)
        self._thread.start()

    def wake(self) -> None:
        self._wake.set()

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        self._thread.join()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                with os.scandir(self.root) as entries:
                    for ordinal, entry in enumerate(entries, 1):
                        if self._stop.is_set():
                            return
                        if entry.name.endswith(".status.json"):
                            self.prune(Path(entry.path), time.time())
                        if ordinal % 32 == 0 and self._stop.wait(0.01):
                            return
            except Exception as error:
                _LOG.warning("terminal retention interrupted cause=%s", type(error).__name__)
            self._wake.wait(self.interval_seconds)
            self._wake.clear()
