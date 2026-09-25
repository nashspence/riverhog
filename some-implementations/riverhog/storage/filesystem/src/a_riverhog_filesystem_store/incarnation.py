"""Explicit, durable identity for one Linux filesystem storage root."""

from __future__ import annotations

import argparse
import fcntl
import os
import stat
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from riverhog_storage_adapter_protocol import (
    StorageAdapterRejection,
    validate_storage_incarnation_id,
)

_MARKER = ".riverhog-incarnation"
_LOCK = ".riverhog-incarnation.lock"
_MAGIC = b"riverhog-storage-incarnation/v1\n"


class StorageIncarnationError(StorageAdapterRejection):
    """The root does not supply trustworthy incarnation evidence."""

    def __init__(self, message: str) -> None:
        super().__init__("provider_unavailable", message)


@contextmanager
def _open_root(root: Path) -> Iterator[int]:
    if root.is_symlink():
        raise ValueError("filesystem storage root must not be a symbolic link")
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        yield fd
    finally:
        os.close(fd)


def _read_marker(root_fd: int) -> str:
    marker_fd = os.open(
        _MARKER,
        os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
        dir_fd=root_fd,
    )
    with os.fdopen(marker_fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise StorageIncarnationError("filesystem incarnation marker is not a regular file")
        raw = stream.read(128)
    if len(raw) != len(_MAGIC) + 37 or not raw.startswith(_MAGIC) or raw[-1:] != b"\n":
        raise StorageIncarnationError("filesystem incarnation marker is invalid")
    try:
        return validate_storage_incarnation_id(raw[len(_MAGIC) : -1].decode("ascii"))
    except (UnicodeError, ValueError) as exc:
        raise StorageIncarnationError("filesystem incarnation marker is invalid") from exc


def read_storage_incarnation_fd(root_fd: int) -> str:
    """Read the identity from the same pinned root used for object effects."""

    try:
        return _read_marker(root_fd)
    except FileNotFoundError as exc:
        raise StorageIncarnationError("filesystem storage root has no incarnation marker") from exc


def read_storage_incarnation(root: Path) -> str:
    with _open_root(root) as root_fd:
        return read_storage_incarnation_fd(root_fd)


def provision_storage_root(root: Path) -> str:
    """Mark an existing empty root once; ordinary startup never provisions it."""

    with _open_root(root) as root_fd:
        lock_fd = os.open(
            _LOCK,
            os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=root_fd,
        )
        with os.fdopen(lock_fd, "rb") as lock:
            if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
                raise StorageIncarnationError("filesystem incarnation lock is not a regular file")
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                existing = _read_marker(root_fd)
            except FileNotFoundError:
                pass
            else:
                os.fsync(root_fd)
                return existing
            if set(os.listdir(root_fd)) - {_LOCK}:
                raise StorageIncarnationError("refuse to provision an unmarked nonempty root")
            candidate = validate_storage_incarnation_id(str(uuid4()))
            temporary = f"{_MARKER}.{uuid4().hex}.tmp"
            temporary_fd = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                0o600,
                dir_fd=root_fd,
            )
            with os.fdopen(temporary_fd, "wb") as stream:
                stream.write(_MAGIC + candidate.encode("ascii") + b"\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.link(
                temporary,
                _MARKER,
                src_dir_fd=root_fd,
                dst_dir_fd=root_fd,
                follow_symlinks=False,
            )
            os.fsync(root_fd)
            os.unlink(temporary, dir_fd=root_fd)
            os.fsync(root_fd)
            return read_storage_incarnation_fd(root_fd)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m a_riverhog_filesystem_store.incarnation")
    parser.add_argument("root", type=Path, help="existing empty storage root to provision")
    args = parser.parse_args(argv)
    print(provision_storage_root(args.root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
