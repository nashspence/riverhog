"""Private local-filesystem barriers for one prepared-primary construction."""

from __future__ import annotations

import hashlib
import os
import stat
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from riverhog_client._prepared_primary_types import (
    PrimaryByteFile,
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStateError,
    primary_file_name,
)

_BLOCK = 128 * 1024


def _stamp(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _private_regular(value: os.stat_result) -> None:
    if not stat.S_ISREG(value.st_mode) or value.st_mode & 0o077:
        raise PrimaryStateError("prepared-primary file is not private regular state")
    if value.st_uid != os.geteuid():
        raise PrimaryStateError("prepared-primary file has another owner")


class PrimaryFiles:
    """One nonblocking process owner on a local fsync/link-capable filesystem.

    This handle is released only after its caller has quiesced every worker.
    It never takes over by age, unlinks a lock, scans a backlog or invents state.
    Application initialization and current-format recognition remain separate.
    """

    def __init__(self, root: Path, *, initialize: bool) -> None:
        if os.name != "posix":
            raise PrimaryStateError("prepared-primary state requires a qualified local filesystem")
        import fcntl

        self.root = root.absolute()
        self._directory = -1
        self._owner = -1
        self._owner_root = self
        for parent in (self.root, *self.root.parents):
            if parent.is_symlink():
                raise PrimaryStateError("prepared-primary state cannot traverse symlinks")
        if initialize:
            self.root.mkdir(mode=0o700)
            parent_fd = os.open(self.root.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        try:
            self._directory = os.open(
                self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
            )
            directory = os.fstat(self._directory)
            if directory.st_mode & 0o077 or directory.st_uid != os.geteuid():
                raise PrimaryStateError("prepared-primary directory is not private owned state")
            flags = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
            if initialize:
                flags |= os.O_CREAT | os.O_EXCL
            self._owner = os.open(".owner", flags, 0o600, dir_fd=self._directory)
            _private_regular(os.fstat(self._owner))
            try:
                fcntl.flock(self._owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise PrimaryStateBusy("prepared-primary construction has a live owner") from exc
            if initialize:
                os.fsync(self._owner)
                self.sync()
            self._identity = directory.st_dev, directory.st_ino
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> PrimaryFiles:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owner >= 0:
            os.close(self._owner)
            self._owner = -1
        if self._directory >= 0:
            os.close(self._directory)
            self._directory = -1

    def check_current(self) -> None:
        if self._owner < 0 or self._directory < 0:
            raise PrimaryStateError("prepared-primary construction owner is closed")
        value = self.root.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(value.st_mode)
            or value.st_mode & 0o077
            or value.st_uid != os.geteuid()
            or (value.st_dev, value.st_ino) != self._identity
        ):
            raise PrimaryStateError("prepared-primary construction directory changed")
        if self._owner_root is not self:
            self._owner_root.check_current()
        held = os.fstat(self._owner)
        linked = os.stat(".owner", dir_fd=self._owner_root._directory, follow_symlinks=False)
        if (held.st_dev, held.st_ino) != (linked.st_dev, linked.st_ino):
            raise PrimaryStateError("prepared-primary construction owner changed")

    def child(self, name: str, *, initialize: bool) -> PrimaryFiles:
        """Open one private child while retaining the same construction owner."""
        self.check_current()
        primary_file_name(name)
        if initialize:
            os.mkdir(name, mode=0o700, dir_fd=self._directory)
            self.sync()
        child = object.__new__(PrimaryFiles)
        child.root = self.root / name
        child._directory = -1
        child._owner = -1
        child._owner_root = self._owner_root
        try:
            child._directory = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=self._directory,
            )
            directory = os.fstat(child._directory)
            if directory.st_mode & 0o077 or directory.st_uid != os.geteuid():
                raise PrimaryStateError("prepared-primary child is not private owned state")
            child._identity = directory.st_dev, directory.st_ino
            child._owner = os.dup(self._owner)
            child.check_current()
            return child
        except BaseException:
            child.close()
            raise

    def sync(self) -> None:
        if self._directory < 0:
            raise PrimaryStateError("prepared-primary construction directory is closed")
        os.fsync(self._directory)

    def _open(self, name: str) -> int:
        self.check_current()
        descriptor = os.open(
            primary_file_name(name),
            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=self._directory,
        )
        try:
            _private_regular(os.fstat(descriptor))
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def put(
        self,
        name: str,
        chunks: Iterable[bytes],
        *,
        byte_limit: int,
        expected: tuple[int, str] | None = None,
        replace: bool = False,
        temporary_name: str | None = None,
    ) -> PrimaryByteFile:
        self.check_current()
        primary_file_name(name)
        if type(byte_limit) is not int or byte_limit < 0:
            raise ValueError("prepared-primary byte admission must be explicit and nonnegative")
        temporary = (
            ".candidate-" + uuid.uuid4().hex
            if temporary_name is None
            else primary_file_name(temporary_name)
        )
        if temporary == name:
            raise ValueError("prepared-primary candidate must differ from its target")
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=self._directory,
        )
        size = 0
        digest = hashlib.sha256()
        try:
            try:
                for chunk in chunks:
                    if not isinstance(chunk, bytes):
                        raise TypeError("prepared-primary chunks must be bytes")
                    if len(chunk) > byte_limit - size:
                        raise PrimaryStateCapacity(
                            "prepared-primary durable-byte admission exhausted"
                        )
                    for offset in range(0, len(chunk), _BLOCK):
                        fragment = memoryview(chunk)[offset : offset + _BLOCK]
                        while fragment:
                            written = os.write(descriptor, fragment)
                            if written <= 0:
                                raise OSError("short write retaining prepared-primary state")
                            fragment = fragment[written:]
                    size += len(chunk)
                    digest.update(chunk)
                result = PrimaryByteFile(name, size, digest.hexdigest())
                if expected is not None and (size, result.sha256) != expected:
                    raise PrimaryStateError(
                        "prepared-primary bytes differ from the expected identity"
                    )
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self.check_current()
            if replace:
                try:
                    target = self._open(name)
                except FileNotFoundError:
                    pass
                else:
                    os.close(target)
                os.replace(temporary, name, src_dir_fd=self._directory, dst_dir_fd=self._directory)
            else:
                os.link(
                    temporary,
                    name,
                    src_dir_fd=self._directory,
                    dst_dir_fd=self._directory,
                    follow_symlinks=False,
                )
            self.sync()
            return result
        finally:
            try:
                os.unlink(temporary, dir_fd=self._directory)
            except FileNotFoundError:
                pass
            self.sync()

    def put_control(
        self, name: str, body: bytes, *, byte_limit: int, expected: bytes | None = None
    ) -> None:
        """Install immutable bytes, or compare/switch one exact prior encoding."""
        primary_file_name(name)
        if type(byte_limit) is not int or byte_limit < 0:
            raise ValueError("prepared-primary control admission must be nonnegative")
        if not isinstance(body, bytes) or (
            expected is not None and not isinstance(expected, bytes)
        ):
            raise TypeError("prepared-primary controls must be original bytes")
        if len(body) > byte_limit:
            raise PrimaryStateCapacity("prepared-primary control exceeds explicit admission")
        if expected is not None:
            try:
                previous = self.read_control(name, byte_limit=byte_limit)
            except FileNotFoundError:
                raise PrimaryStateError("prepared-primary control comparison changed") from None
            if previous != expected:
                raise PrimaryStateError("prepared-primary control comparison changed")
        self.put(
            name,
            (body,),
            byte_limit=byte_limit,
            replace=expected is not None,
            temporary_name="candidate-" + name,
        )

    def remove_control(self, name: str, *, expected: bytes) -> None:
        """Delete only the exact control encoding under the construction owner."""
        if not isinstance(expected, bytes):
            raise TypeError("prepared-primary controls must be original bytes")
        try:
            previous = self.read_control(name, byte_limit=len(expected))
        except FileNotFoundError:
            self.sync()
            return
        if previous != expected:
            raise PrimaryStateError("prepared-primary control comparison changed")
        self.remove(name)

    def read_control(self, name: str, *, byte_limit: int) -> bytes:
        if type(byte_limit) is not int or byte_limit < 0:
            raise ValueError("prepared-primary control admission must be explicit and nonnegative")
        descriptor = self._open(name)
        with os.fdopen(descriptor, "rb", buffering=0) as stream:
            before = os.fstat(stream.fileno())
            if before.st_size > byte_limit:
                raise PrimaryStateCapacity("prepared-primary control exceeds explicit admission")
            value = stream.read(before.st_size + 1)
            if len(value) != before.st_size or _stamp(before) != _stamp(os.fstat(stream.fileno())):
                raise PrimaryStateError("prepared-primary control changed during read")
        self.check_current()
        return value

    @contextmanager
    def extent_reader(self, name: str, *, byte_count: int) -> Iterator[BinaryIO]:
        """Stable extent only; the consuming owner must check its framed commitment."""
        descriptor = self._open(name)
        with os.fdopen(descriptor, "rb", buffering=8192) as stream:
            before = os.fstat(stream.fileno())
            if before.st_size != byte_count:
                raise PrimaryStateError("prepared-primary extent differs from its committed length")
            yield stream
            if _stamp(before) != _stamp(os.fstat(stream.fileno())):
                raise PrimaryStateError("prepared-primary extent changed during traversal")
            self.check_current()

    @contextmanager
    def verified(self, file: PrimaryByteFile) -> Iterator[BinaryIO]:
        """Verify all original bytes before exposing a replay reader; keep its inode open."""
        descriptor = self._open(file.name)
        with os.fdopen(descriptor, "rb", buffering=0) as stream:
            before = os.fstat(stream.fileno())
            if before.st_size != file.bytes:
                raise PrimaryStateError("prepared-primary file differs from its committed length")
            digest = hashlib.sha256()
            size = 0
            while chunk := stream.read(_BLOCK):
                size += len(chunk)
                if size > file.bytes:
                    raise PrimaryStateError("prepared-primary file exceeds its committed length")
                digest.update(chunk)
            if (
                size != file.bytes
                or digest.hexdigest() != file.sha256
                or _stamp(before) != _stamp(os.fstat(stream.fileno()))
            ):
                raise PrimaryStateError("prepared-primary file differs from its committed identity")
            self.check_current()
            stream.seek(0)
            yield stream
            if _stamp(before) != _stamp(os.fstat(stream.fileno())):
                raise PrimaryStateError("prepared-primary file changed during replay")
            self.check_current()

    def exists(self, name: str) -> bool:
        try:
            descriptor = self._open(name)
        except FileNotFoundError:
            return False
        os.close(descriptor)
        return True

    def remove(self, name: str) -> None:
        self.check_current()
        try:
            descriptor = self._open(name)
        except FileNotFoundError:
            self.sync()
            return
        os.close(descriptor)
        os.unlink(name, dir_fd=self._directory)
        self.sync()
