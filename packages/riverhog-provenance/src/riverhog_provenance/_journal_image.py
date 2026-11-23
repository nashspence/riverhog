"""Original bounded baseline capture of one quiescent retained Native image.

The current retained private format carries control and semantic rows together.
An image root preserves that exact Native frontier; the composing Core owner
still owns its full generation envelope, grant, cursor and selection outcome.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING

from retained_resource import Borrow, Group

from ._generation_storage import NativeGenerationStorage
from ._native_storage import NativeStorageLease
from .errors import IncompleteSourceError, JournalValidationCapacity
from .journal_store import InvalidJournalWorkspace

if TYPE_CHECKING:
    from collections.abc import Iterator

    from ._generation_roots import GenerationReader
    from ._native_epoch import EpochContext
    from .journal_session import JournalValidationSession, JournalValidationSource


@dataclass(frozen=True, slots=True)
class JournalImage:
    """Exact Native image identity; not a selected application recovery frontier."""

    identity: str
    store_id: str
    root: str
    binding: bytes
    attempt_id: str
    worker_epoch: int
    checkpoint_version: int
    page_size: int
    page_count: int


class JournalImageCapture:
    """Caller-retained passive frame for a single baseline, including failed opens.

    Capture holds the original executor and working storage across bounded page
    steps. Its file and borrowed descriptor remain reachable before acquisition.
    Closing receives them before releasing that hold. The immutable candidate
    row remains with the generation owner, including failed/ambiguous writes;
    resource closure supplies no authority to retire or select it.
    """

    def __init__(self, session: JournalValidationSession) -> None:
        self.session = session
        self.epoch = session._ledger.epoch
        self.files = Group(1)
        self.file = self.files.resource("file")
        self.borrow = Borrow(self.file)
        self.lease = NativeStorageLease(session._store._storage)
        self.phase = "new"
        self.image: JournalImage | None = None
        self.pages_captured = 0
        self.octets_read = 0
        self._gate = Lock()
        self._held = False
        self._retained = False
        self._storage: NativeGenerationStorage | None = None
        self._source: JournalValidationSource | None = None

    @contextmanager
    def _operation(self) -> Iterator[None]:
        if not self._gate.acquire(blocking=False):
            raise BlockingIOError("original Native image operation has not returned")
        try:
            yield
        finally:
            self._gate.release()

    def _require_current(self) -> None:
        self.session._require_open()
        self.epoch.require_capture(self)
        source = self._source
        if (
            source is None
            or source.binding != self.session._source_binding
            or source.verify_current() is not True
        ):
            raise IncompleteSourceError("retained validation source is no longer current")

    def begin(self, storage: NativeGenerationStorage, identity: str, *, image_octets: int) -> None:
        """Start only after the composing owner's full original grant.

        image_octets is optional capture admission, never a source-size limit.
        Refusal must be received before useful execution continues. Ordinary
        streams are not spooled or converted into recoverable sources.
        """
        with self._operation():
            if self.phase != "new":
                raise RuntimeError("Native baseline capture is single-attempt")
            if not isinstance(storage, NativeGenerationStorage):
                raise TypeError("capture requires its separate original generation storage")
            if type(image_octets) is not int or image_octets < 1:
                raise ValueError("capture requires positive original image admission")
            self.phase = "opening"
            try:
                self.session._require_open()
                if self.session._source is None:
                    raise ValueError("ordinary stream capture requires no implicit spool")
                self.epoch.retain_capture(self)
                self._held = True
                self.session._store._storage.retain(self.session._store)
                self._retained = True
                self._source = self.session._source
                self._require_current()
                before = self.session._ledger.load()
                if before.checkpoint.phase == "done":
                    raise ValueError("completed validation does not require baseline capture")
                self._storage, self._identity = storage, identity
                self._binding = self.session._prefix.binding
                self._checkpoint = before.checkpoint
                with closing(self.epoch.db.execute("PRAGMA page_size")) as rows:
                    self._page_size = rows.fetchone()[0]
                with closing(self.epoch.db.execute("PRAGMA page_count")) as rows:
                    self._page_count = rows.fetchone()[0]
                with closing(self.epoch.db.execute("PRAGMA journal_mode")) as rows:
                    mode = rows.fetchone()
                if mode != ("delete",):
                    raise InvalidJournalWorkspace("baseline requires a received DELETE image")
                if storage.objects.page_size != self._page_size:
                    raise ValueError("generation and working image page widths differ")
                if self._page_count * self._page_size > image_octets:
                    raise JournalValidationCapacity("optional Native baseline admission exhausted")
                path = self.session._store._storage.directory / "snapshot.sqlite3"
                self.file.acquire(
                    os.fsencode(path) if os.name == "posix" else str(path),
                    private_owned=os.name == "posix",
                )
                self.borrow.acquire()
                if os.fstat(self.borrow.fileno()).st_size != self._page_count * self._page_size:
                    raise InvalidJournalWorkspace("working image extent differs")
                with storage.execution():
                    storage.epoch.apply(
                        lambda epoch: storage.roots.begin(identity), receive=lambda result: None
                    )
                self.phase = "capturing"
            except BaseException:
                self.phase = "failed-retained"
                raise

    def advance(
        self, *, page_limit: int, check: Callable[[], None] | None = None
    ) -> JournalImage | None:
        """Copy at most page_limit original pages and seal only the full image."""
        with self._operation():
            if self.phase != "capturing":
                raise RuntimeError("Native baseline capture is not active")
            if type(page_limit) is not int or page_limit < 1:
                raise ValueError("capture requires positive bounded page admission")
            storage = self._storage
            assert storage is not None
            try:
                self._require_current()
                for _ in range(min(page_limit, self._page_count - self.pages_captured)):
                    if check is not None:
                        if check() is not None:
                            raise TypeError("Native capture check must return None or raise")
                    remaining = self._page_size
                    parts = []
                    while remaining:
                        part = os.read(self.borrow.fileno(), remaining)
                        if not part:
                            raise InvalidJournalWorkspace("working image ended before its extent")
                        self.octets_read += len(part)
                        parts.append(part)
                        remaining -= len(part)
                    data = b"".join(parts)
                    number = self.pages_captured + 1

                    def apply_page(
                        epoch: EpochContext, number: int = number, data: bytes = data
                    ) -> None:
                        storage.roots.replace(self._identity, number, data)

                    with storage.execution(check):
                        storage.epoch.apply(apply_page, receive=lambda result: None)
                    self.pages_captured = number
                if self.pages_captured != self._page_count:
                    return None
                if os.read(self.borrow.fileno(), 1):
                    self.octets_read += 1
                    raise InvalidJournalWorkspace("working image grew during original capture")
                self._require_current()
                with storage.execution(check):
                    root = storage.epoch.apply(
                        lambda epoch: storage.roots.freeze(self._identity),
                        receive=lambda result: None,
                    )
                self.image = JournalImage(
                    self._identity,
                    storage.store_id,
                    root,
                    self._binding,
                    self.session.attempt_id,
                    self.session._ledger.worker_epoch,
                    self._checkpoint.version,
                    self._page_size,
                    self._page_count,
                )
                self.phase = "captured"
                return self.image
            except BaseException:
                self.phase = "failed-retained"
                raise

    @property
    def received(self) -> bool:
        return self.phase == "received"

    def close(self) -> None:
        """Retry the same original receiving; intent never releases a hold."""
        with self._operation():
            if self.received:
                return
            self.phase = "receiving"
            self.borrow.close()
            if not self.files.close_step(self.files.capacity):
                raise BlockingIOError("original capture files have not received")
            if self._retained:
                self.lease.close()
                self._retained = False
            if self._held:
                self.epoch.receive_capture(self)
                self._held = False
            self.phase = "received"


class JournalImageRestore:
    """Passive original file/reader receiving for a fresh private working image."""

    def __init__(self, storage: NativeGenerationStorage, image: JournalImage) -> None:
        if type(image) is not JournalImage:
            raise TypeError("restore requires its exact Native image descriptor")
        self.storage, self.image = storage, image
        self.files = Group(2 if os.name == "posix" else 1)
        self.file = self.files.resource("file")
        self.directory = self.files.resource("directory") if os.name == "posix" else None
        self.borrow = Borrow(self.file)
        self.reader: GenerationReader | None = None
        self.phase = "new"
        self.pages_restored = 0
        self.octets_written = 0
        self._gate = Lock()

    @contextmanager
    def _operation(self) -> Iterator[None]:
        if not self._gate.acquire(blocking=False):
            raise BlockingIOError("original Native restore operation has not returned")
        try:
            yield
        finally:
            self._gate.release()

    def begin(self, directory: Path, *, image_octets: int) -> None:
        """Borrow the exact immutable root before exclusive destination effects."""
        with self._operation():
            if self.phase != "new":
                raise RuntimeError("Native image restore is single-attempt")
            self.phase = "opening"
            try:
                image = self.image
                if (
                    image.store_id != self.storage.store_id
                    or type(image.page_count) is not int
                    or image.page_count < 1
                    or image.page_size != self.storage.objects.page_size
                ):
                    raise InvalidJournalWorkspace("Native image identity or extent differs")
                if type(image_octets) is not int or image_octets < 1:
                    raise ValueError("restore requires positive original image admission")
                if image.page_count * image.page_size > image_octets:
                    raise JournalValidationCapacity("optional Native restore admission exhausted")
                if directory.is_symlink() or not directory.is_dir():
                    raise InvalidJournalWorkspace("fresh private restore directory is unavailable")
                self.reader = self.storage.roots.borrow(image.identity, expected_root=image.root)
                if self.directory is not None:
                    self.directory.acquire(os.fsencode(directory), private_owned=True)
                path = directory / "snapshot.sqlite3"
                self.file.acquire(
                    os.fsencode(path) if os.name == "posix" else str(path),
                    writable=True,
                    create=True,
                    exclusive=True,
                    private_owned=os.name == "posix",
                )
                self.borrow.acquire()
                self.phase = "restoring"
            except BaseException:
                self.phase = "failed-retained"
                raise

    def advance(self, *, page_limit: int, check: Callable[[], None] | None = None) -> bool:
        """Receive consecutive pages within one explicit bounded reader step."""
        with self._operation():
            if self.phase != "restoring":
                raise RuntimeError("Native image restore is not active")
            try:
                assert self.reader is not None
                batch = self.reader.read(
                    after=self.pages_restored, page_limit=page_limit, check=check
                )
                for number, data in batch.pages:
                    if (
                        number != self.pages_restored + 1
                        or number > self.image.page_count
                        or len(data) != self.image.page_size
                    ):
                        raise InvalidJournalWorkspace("Native image page closure differs")
                    pending = memoryview(data)
                    while pending:
                        if check is not None and check() is not None:
                            raise TypeError("Native restore check must return None or raise")
                        written = os.write(self.borrow.fileno(), pending)
                        if written <= 0:
                            raise OSError("original Native restore write made no progress")
                        self.octets_written += written
                        pending = pending[written:]
                    self.pages_restored = number
                if batch.more:
                    return False
                if self.pages_restored != self.image.page_count:
                    raise InvalidJournalWorkspace("Native image page closure is incomplete")
                self.file.sync()
                if self.directory is not None:
                    self.directory.sync()
                self.phase = "restored"
                return True
            except BaseException:
                self.phase = "failed-retained"
                raise

    @property
    def received(self) -> bool:
        return self.phase == "received"

    def close(self) -> None:
        with self._operation():
            if self.received:
                return
            self.phase = "receiving"
            self.borrow.close()
            if not self.files.close_step(self.files.capacity):
                raise BlockingIOError("original Native restore files have not received")
            if self.reader is not None:
                self.reader.close()
            self.phase = "received"
