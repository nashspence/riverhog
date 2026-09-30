"""Durable, exclusive publication of an already complete recovery directory."""

from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path


def _sync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _sync_tree(staging: Path) -> None:
    for root, directories, files in os.walk(staging, topdown=False, followlinks=False):
        for name in (*directories, *files):
            if (Path(root) / name).is_symlink():
                raise ValueError("recovery staging contains a symbolic link")
        _sync_directory(Path(root))


def _exclusive_rename(source: Path, destination: Path) -> None:
    if sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        rename = libc.renameat2
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        # AT_FDCWD and RENAME_NOREPLACE are Linux's public renameat2 values.
        status = rename(-100, os.fsencode(source), -100, os.fsencode(destination), 1)
    elif sys.platform == "darwin":
        libc = ctypes.CDLL(None, use_errno=True)
        rename = libc.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        # RENAME_EXCL is Darwin's exclusive rename flag.
        status = rename(os.fsencode(source), os.fsencode(destination), 4)
    elif os.name == "nt":
        # Windows rename refuses an existing destination directory.
        os.rename(source, destination)
        return
    else:
        raise OSError("exclusive directory rename is unavailable on this platform")
    if status != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(destination))


def publish_complete_recovery(staging: Path, destination: Path) -> None:
    """Make a fsynced recovery visible once, never replacing an existing path."""

    _sync_tree(staging)
    _exclusive_rename(staging, destination)
    _sync_directory(destination.parent)


__all__ = ["publish_complete_recovery"]
