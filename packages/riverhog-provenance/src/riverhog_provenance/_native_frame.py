"""Private parsed current-frame custody within one original Native executor."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ._native_epoch import EpochContext
from ._native_work import WorkAmount
from .journal import JournalFrame


@dataclass(frozen=True, slots=True)
class NativeFrame(JournalFrame):
    """Retain one exact frame's read-only parsed view and content digest.

    Only the private conductor and its children borrow the parsed dictionary;
    they must not mutate or expose it. Public journal frames keep their pure
    decoding behavior. This is data reuse within original executor custody,
    not source, worker, catalog or semantic authorization.
    """

    _epoch: EpochContext = field(repr=False, compare=False)
    _document: dict[str, Any] | None = field(default=None, init=False, repr=False, compare=False)
    _sha256: str | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if type(self.json_bytes) is not bytes:
            raise TypeError("Native frame custody requires immutable exact bytes")
        self._epoch.require_usable()

    @property
    def document(self) -> dict[str, Any]:
        self._epoch.require_usable()
        document = self._document
        if document is None:
            document = super(NativeFrame, self).document
            object.__setattr__(self, "_document", document)
        return document

    @property
    def sha256(self) -> str:
        self._epoch.require_usable()
        digest = self._sha256
        if digest is None:
            work = self._epoch.work
            if work is not None and self.json_bytes:
                work.consume(WorkAmount(octets=len(self.json_bytes)))
            digest = super(NativeFrame, self).sha256
            object.__setattr__(self, "_sha256", digest)
        return digest


def own_frame(frame: JournalFrame, epoch: EpochContext) -> NativeFrame:
    """Never borrow another executor's parsed view or prior digest."""
    epoch.require_usable()
    if isinstance(frame, NativeFrame) and frame._epoch is epoch:
        return frame
    return NativeFrame(frame.json_bytes, epoch)
