"""Neutral retained extent turn; current private-file semantics, no Core imports.

This reference corresponds to workspace_reclamation.reclaim_retired_files, but
turn retention is mandatory before advance. Disposal authority remains external.
"""
from __future__ import annotations

import os
import stat
import threading
from dataclasses import dataclass
from pathlib import Path

from . import Group


@dataclass(frozen=True)
class Progress:
    removed_octets: int
    removed_files: int
    remaining_octets: int
    remaining_files: int


def _private(st, directory=False):
    expected = 0o700 if directory else 0o600
    if (not (stat.S_ISDIR(st.st_mode) if directory else stat.S_ISREG(st.st_mode))
            or st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != expected
            or (not directory and st.st_nlink != 1)):
        raise RuntimeError("original private custody is required")


class ExtentTurn:
    """One admitted synchronous extent; caller retains this before advance.

    Four retained records (directory, directory borrow, file, file borrow)
    within an explicit six-record grant. No automatic garbage cleanup.
    Outer Core/Native owner retains its independent parent lease and charge.
    """
    def __init__(self, directory: Path, *, names: tuple[str, ...], octet_budget: int):
        if type(octet_budget) is not int or octet_budget < 1:
            raise ValueError("positive extent grant required")
        if not isinstance(names, tuple):
            raise TypeError("exact tuple of names required")
        for name in names:
            if not isinstance(name,str) or name in {"", ".", ".."} or '/' in name or '\0' in name:
                raise ValueError("exact single filename required")
        ordered = sorted(os.fsencode(n) for n in names)
        if any(a == b for a,b in zip(ordered,ordered[1:])):
            raise ValueError("duplicate filename")
        self.directory = os.fsencode(directory)
        self.names, self.budget = names, octet_budget
        self.handles = Group(6)
        # All receiving owners exist before any acquisition, including file data.
        self.dir_owner = self.handles.resource("directory")
        self.dir_borrow = self.handles.borrow(self.dir_owner)
        self.file_owner = self.handles.resource("file")
        self.file_borrow = self.handles.borrow(self.file_owner)
        self._attempted = False
        self._guard = threading.Lock()
        self.progress: Progress | None = None

    @property
    def received(self):
        return self.handles.received

    def _close_owned(self):
        # Called only after this turn's synchronous raw-descriptor users returned.
        self.file_borrow.close()
        self.dir_borrow.close()
        return self.handles.close_step(6)

    def close(self):
        if not self._guard.acquire(blocking=False):
            raise BlockingIOError("extent execution has not left the original borrow")
        try:
            return self._close_owned()
        finally:
            self._guard.release()

    def _files(self, fd):
        found=[]
        for name in self.names:
            try:
                s=os.stat(name,dir_fd=fd,follow_symlinks=False)
            except FileNotFoundError:
                continue
            _private(s)
            found.append((name,s))
        return found

    def advance(self):
        if not self._guard.acquire(blocking=False):
            raise BlockingIOError("extent turn already executing")
        try:
            if self._attempted or self.received:
                raise RuntimeError("one extent attempt only; close retry is not a second mutation")
            self._attempted = True
            self.dir_owner.acquire(self.directory)
            self.dir_borrow.acquire()
            directory=self.dir_borrow.fileno()
            _private(os.fstat(directory),True)
            before=self._files(directory)
            removed=unlinked=0
            if before:
                name, selected=before[0]
                self.file_owner.acquire(os.fsencode(name),writable=True,parent=self.dir_owner)
                self.file_borrow.acquire()
                fd=self.file_borrow.fileno()
                current=os.fstat(fd); _private(current)
                if (current.st_dev,current.st_ino,current.st_size)!=(selected.st_dev,selected.st_ino,selected.st_size):
                    raise RuntimeError("original retired file was replaced")
                removed=min(current.st_size,self.budget)
                if current.st_size>self.budget:
                    os.ftruncate(fd,current.st_size-self.budget); os.fsync(fd)
                else:
                    check=os.stat(name,dir_fd=directory,follow_symlinks=False)
                    if (check.st_dev,check.st_ino)!=(current.st_dev,current.st_ino):
                        raise RuntimeError("original retired file was replaced")
                    os.unlink(name,dir_fd=directory); unlinked=1; os.fsync(directory)
            after=self._files(directory)
            self.progress=Progress(removed,unlinked,sum(s.st_size for _,s in after),len(after))
        finally:
            try:
                self._close_owned()
            finally:
                self._guard.release()
        return self.progress
