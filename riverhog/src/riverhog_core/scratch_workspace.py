"""Private process-owned scratch, reclaimed only after its kernel lease is free."""

from __future__ import annotations

import contextlib
import errno
import os
import re
import shutil
import sqlite3
import stat
import tempfile
import uuid
from collections.abc import Iterator
from pathlib import Path

from riverhog_protocol.errors import ServiceUnavailable

_OPERATION = re.compile(r"operation-[0-9a-f]{32}")
_MINIMUM_FREE_BYTES = 1024 * 1024


@contextlib.contextmanager
def capacity_errors() -> Iterator[None]:
    """Filesystem/quota exhaustion is retryable capacity failure, never bad evidence."""
    try:
        yield
    except OSError as error:
        if error.errno not in {errno.ENOSPC, errno.EDQUOT}:
            raise
        raise ServiceUnavailable("temporary workspace capacity exhausted") from None
    except sqlite3.DatabaseError as error:
        if getattr(error, "sqlite_errorcode", 0) & 255 != sqlite3.SQLITE_FULL:
            raise
        raise ServiceUnavailable("temporary workspace capacity exhausted") from None


@contextlib.contextmanager
def scratch_directory(*, prefix: str) -> Iterator[str]:
    # Capacity is the configured filesystem/quota. This physical reserve governs
    # admission of an operation, not metadata, member count, or collection extent.
    with capacity_errors():
        if shutil.disk_usage(tempfile.gettempdir()).free < _MINIMUM_FREE_BYTES:
            raise ServiceUnavailable("temporary workspace capacity exhausted")
        with tempfile.TemporaryDirectory(prefix=prefix) as directory:
            yield directory


@contextlib.contextmanager
def process_workspace(base: Path | None = None) -> Iterator[Path]:
    """Lease private disk scratch for one native server process.

    Creation and reclamation share a coordinator. A kernel lease protects live
    owners across independent processes; process death releases it without PID
    guessing. Unknown entries, foreign owners, and symlinks are never reclaimed.
    """
    import fcntl

    @contextlib.contextmanager
    def coordinated(descriptor: int) -> Iterator[None]:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)

    base = Path(tempfile.gettempdir()) if base is None else base
    root = base / f"riverhog-runtime-scratch-{os.getuid()}"
    with capacity_errors(), contextlib.ExitStack() as resources:
        root.mkdir(mode=0o700, exist_ok=True)
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        resources.callback(os.close, root_fd)
        identity = os.fstat(root_fd)
        if identity.st_uid != os.getuid() or identity.st_mode & 0o777 != 0o700:
            raise RuntimeError("runtime scratch must be a private directory owned by this user")
        coordinator = os.open(
            ".coordinator", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=root_fd
        )
        resources.callback(os.close, coordinator)
        identity = os.fstat(coordinator)
        if not stat.S_ISREG(identity.st_mode) or identity.st_uid != os.getuid():
            raise RuntimeError("runtime scratch coordinator must be owned by this user")
        with coordinated(coordinator):
            for entry in root.iterdir():
                if not _OPERATION.fullmatch(entry.name) or entry.is_symlink() or not entry.is_dir():
                    continue
                with contextlib.ExitStack() as owner_resources:
                    owner = os.open(entry, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                    owner_resources.callback(os.close, owner)
                    identity = os.fstat(owner)
                    if identity.st_uid != os.getuid() or identity.st_mode & 0o777 != 0o700:
                        continue
                    try:
                        identity = os.stat(".lease", dir_fd=owner, follow_symlinks=False)
                    except FileNotFoundError:
                        # An interrupted creation cannot still be adding its
                        # lease: creation holds this same coordinator.
                        pass
                    else:
                        if not stat.S_ISREG(identity.st_mode) or identity.st_uid != os.getuid():
                            continue
                        lease = os.open(".lease", os.O_RDWR | os.O_NOFOLLOW, dir_fd=owner)
                        owner_resources.callback(os.close, lease)
                        try:
                            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            continue
                    shutil.rmtree(entry)
            directory = root / ("operation-" + uuid.uuid4().hex)
            directory.mkdir(mode=0o700)
            try:
                lease = os.open(directory / ".lease", os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
            except BaseException:
                shutil.rmtree(directory)
                raise
            resources.callback(os.close, lease)
            fcntl.flock(lease, fcntl.LOCK_EX)

        def remove_owner() -> None:
            with coordinated(coordinator):
                shutil.rmtree(directory)

        # Reclaim normally while retaining the lease. ExitStack still releases
        # every descriptor if removal fails, allowing the next start to retry.
        resources.callback(remove_owner)
        old_environment, old_tempdir = os.environ.get("TMPDIR"), tempfile.tempdir
        os.environ["TMPDIR"] = str(directory)
        tempfile.tempdir = str(directory)
        try:
            yield directory
        finally:
            tempfile.tempdir = old_tempdir
            if old_environment is None:
                os.environ.pop("TMPDIR", None)
            else:
                os.environ["TMPDIR"] = old_environment
