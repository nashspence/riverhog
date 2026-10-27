"""Native private NTFS identity and lifetime guards for prepared-primary containers."""

from __future__ import annotations

import ctypes
import hashlib
import os
import re
from ctypes import wintypes
from pathlib import Path
from typing import Any

from riverhog_client._prepared_primary_types import PrimaryStateBusy, PrimaryStateError

_DWORD = ctypes.c_uint32
_BOOL = ctypes.c_int32
_PTR = ctypes.c_void_p
_HANDLE = wintypes.HANDLE
_READ_CONTROL = 0x20000
_READ_ATTRIBUTES = 0x80
_READ_WRITE = 0xC0000000
_REPARSE = 0x400
_DIRECTORY = 0x10
_UNSUPPORTED_ATTRIBUTES = _REPARSE | 0x1000 | 0x40000 | 0x400000
_FILE_FLAGS = 0x00200000
_DIRECTORY_FLAGS = _FILE_FLAGS | 0x02000000
_FULL_ACCESS = 0x1F01FF
_BASENAME = re.compile(r"primary-[0-9a-f]{64}\.sqlite3\Z")
_DEVICE = re.compile(r"(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)", re.IGNORECASE)


class _SecurityAttributes(ctypes.Structure):
    _fields_ = [("length", _DWORD), ("descriptor", _PTR), ("inherit", _BOOL)]


class _FileInformation(ctypes.Structure):
    _fields_ = [
        ("attributes", _DWORD),
        ("creation", _DWORD * 2),
        ("access", _DWORD * 2),
        ("write", _DWORD * 2),
        ("volume", _DWORD),
        ("size_high", _DWORD),
        ("size_low", _DWORD),
        ("links", _DWORD),
        ("index_high", _DWORD),
        ("index_low", _DWORD),
    ]


class _Acl(ctypes.Structure):
    _fields_ = [
        ("revision", ctypes.c_ubyte),
        ("reserved", ctypes.c_ubyte),
        ("size", ctypes.c_uint16),
        ("count", ctypes.c_uint16),
        ("reserved2", ctypes.c_uint16),
    ]


class _Ace(ctypes.Structure):
    _fields_ = [
        ("kind", ctypes.c_ubyte),
        ("flags", ctypes.c_ubyte),
        ("size", ctypes.c_uint16),
        ("mask", _DWORD),
    ]


def windows_container_path(parent: Path, allocation_descriptor: bytes) -> Path:
    """Derive a fixed ASCII filename from an original, length-framed allocation."""
    if not allocation_descriptor:
        raise ValueError("prepared-primary allocation descriptor is required")
    framed = b"riverhog-prepared-primary-windows/v1\0"
    framed += len(allocation_descriptor).to_bytes(8, "big") + allocation_descriptor
    return parent / ("primary-" + hashlib.sha256(framed).hexdigest() + ".sqlite3")


class _WindowsAPI:
    def __init__(self) -> None:
        if os.name != "nt":
            raise PrimaryStateError("native prepared-primary ownership requires Windows")
        loader = getattr(ctypes, "WinDLL", None)
        if loader is None:
            raise PrimaryStateError("native Windows API loader is unavailable")
        self.kernel: Any = loader("kernel32", use_last_error=True)
        self.security: Any = loader("advapi32", use_last_error=True)
        self._configure()
        self.sid = self._process_sid()

    def _configure(self) -> None:
        def bind(library: Any, name: str, result: Any, *arguments: Any) -> None:
            function = getattr(library, name)
            function.restype = result
            function.argtypes = list(arguments)

        k, a = self.kernel, self.security
        p = ctypes.POINTER
        bind(
            k,
            "CreateFileW",
            _HANDLE,
            wintypes.LPCWSTR,
            _DWORD,
            _DWORD,
            _PTR,
            _DWORD,
            _DWORD,
            _HANDLE,
        )
        bind(k, "CloseHandle", _BOOL, _HANDLE)
        bind(k, "FlushFileBuffers", _BOOL, _HANDLE)
        bind(k, "LocalFree", _PTR, _PTR)
        bind(k, "GetCurrentProcess", _HANDLE)
        bind(k, "GetDriveTypeW", _DWORD, wintypes.LPCWSTR)
        bind(k, "GetFileInformationByHandle", _BOOL, _HANDLE, p(_FileInformation))
        bind(k, "GetFinalPathNameByHandleW", _DWORD, _HANDLE, wintypes.LPWSTR, _DWORD, _DWORD)
        bind(
            k,
            "GetVolumeInformationByHandleW",
            _BOOL,
            _HANDLE,
            wintypes.LPWSTR,
            _DWORD,
            p(_DWORD),
            p(_DWORD),
            p(_DWORD),
            wintypes.LPWSTR,
            _DWORD,
        )
        bind(a, "OpenProcessToken", _BOOL, _HANDLE, _DWORD, p(_HANDLE))
        bind(a, "GetTokenInformation", _BOOL, _HANDLE, ctypes.c_int, _PTR, _DWORD, p(_DWORD))
        bind(a, "ConvertSidToStringSidW", _BOOL, _PTR, p(wintypes.LPWSTR))
        bind(
            a,
            "ConvertStringSecurityDescriptorToSecurityDescriptorW",
            _BOOL,
            wintypes.LPCWSTR,
            _DWORD,
            p(_PTR),
            p(_DWORD),
        )
        bind(
            a,
            "GetSecurityInfo",
            _DWORD,
            _HANDLE,
            ctypes.c_int,
            _DWORD,
            p(_PTR),
            p(_PTR),
            p(_PTR),
            p(_PTR),
            p(_PTR),
        )
        bind(a, "GetSecurityDescriptorControl", _BOOL, _PTR, p(ctypes.c_uint16), p(_DWORD))
        bind(a, "GetAce", _BOOL, _PTR, _DWORD, p(_PTR))

    @staticmethod
    def _required(result: object, operation: str) -> None:
        if not result:
            code = getattr(ctypes, "get_last_error", lambda: 0)()
            raise PrimaryStateError(f"native prepared-primary {operation} failed ({code})")

    def _sid_text(self, sid: _PTR) -> str:
        value = wintypes.LPWSTR()
        self._required(self.security.ConvertSidToStringSidW(sid, ctypes.byref(value)), "SID read")
        try:
            return value.value or ""
        finally:
            self.kernel.LocalFree(ctypes.cast(value, _PTR))

    def _process_sid(self) -> str:
        token = _HANDLE()
        self._required(
            self.security.OpenProcessToken(self.kernel.GetCurrentProcess(), 8, ctypes.byref(token)),
            "process token",
        )
        try:
            size = _DWORD()
            self.security.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
            if size.value < ctypes.sizeof(_PTR) or size.value > 1024 * 1024:
                raise PrimaryStateError("native prepared-primary token identity is unavailable")
            buffer = ctypes.create_string_buffer(size.value)
            self._required(
                self.security.GetTokenInformation(token, 1, buffer, size, ctypes.byref(size)),
                "token identity",
            )
            return self._sid_text(_PTR.from_buffer(buffer))
        finally:
            self.kernel.CloseHandle(token)

    def private_descriptor(self, *, directory: bool = False) -> _PTR:
        inheritance = "OICI" if directory else ""
        sddl = f"O:{self.sid}D:P(A;{inheritance};FA;;;{self.sid})(A;{inheritance};FA;;;SY)"
        result = _PTR()
        self._required(
            self.security.ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl, 1, ctypes.byref(result), None
            ),
            "private security descriptor",
        )
        return result

    def open(
        self,
        path: Path,
        *,
        create: bool = False,
        owner: bool = False,
        directory: bool = False,
        journal: bool = False,
    ) -> int:
        descriptor = self.private_descriptor() if create else None
        attributes = _SecurityAttributes(ctypes.sizeof(_SecurityAttributes), descriptor, False)
        try:
            raw = self.kernel.CreateFileW(
                str(path),
                _READ_WRITE if create or owner else _READ_CONTROL | _READ_ATTRIBUTES,
                0 if owner else (7 if journal else 3),
                ctypes.byref(attributes) if create else None,
                1 if create else 3,
                _DIRECTORY_FLAGS if directory else _FILE_FLAGS,
                None,
            )
            error = getattr(ctypes, "get_last_error", lambda: 0)()
        finally:
            if descriptor is not None:
                self.kernel.LocalFree(descriptor)
        if raw is None or raw == _PTR(-1).value:
            if error in (2, 3):
                raise FileNotFoundError(str(path))
            if error in (80, 183):
                raise FileExistsError(str(path))
            if owner and error == 32:
                raise PrimaryStateBusy("prepared-primary construction has a live owner")
            raise PrimaryStateError(f"native prepared-primary open failed ({error})")
        return int(raw)

    def close(self, handle: int) -> None:
        self._required(self.kernel.CloseHandle(handle), "handle close")

    def information(self, handle: int, path: Path, *, directory: bool) -> tuple[int, int, int]:
        info = _FileInformation()
        self._required(
            self.kernel.GetFileInformationByHandle(handle, ctypes.byref(info)), "file identity"
        )
        if (
            info.attributes & _UNSUPPORTED_ATTRIBUTES
            or bool(info.attributes & _DIRECTORY) != directory
        ):
            raise PrimaryStateError("prepared-primary placement has an unsupported file kind")
        if not directory and info.links != 1:
            raise PrimaryStateError("prepared-primary file has multiple links")
        size = self.kernel.GetFinalPathNameByHandleW(handle, None, 0, 0)
        if not size or size > 32768:
            raise PrimaryStateError("prepared-primary final path is unavailable")
        buffer = ctypes.create_unicode_buffer(size + 1)
        written = self.kernel.GetFinalPathNameByHandleW(handle, buffer, len(buffer), 0)
        if not written or written >= len(buffer):
            raise PrimaryStateError("prepared-primary final path is unavailable")
        if buffer.value.casefold() != ("\\\\?\\" + str(path)).casefold():
            raise PrimaryStateError("prepared-primary path resolved through an unsupported alias")
        return info.volume, info.index_high, info.index_low

    def private(self, handle: int, *, directory: bool = False, inherited: bool = False) -> None:
        owner, acl, descriptor = _PTR(), _PTR(), _PTR()
        status = self.security.GetSecurityInfo(
            handle,
            1,
            5,
            ctypes.byref(owner),
            None,
            ctypes.byref(acl),
            None,
            ctypes.byref(descriptor),
        )
        if status:
            raise PrimaryStateError(f"prepared-primary security read failed ({status})")
        try:
            if not owner.value or self._sid_text(owner) != self.sid or not acl.value:
                raise PrimaryStateError("prepared-primary state is not privately owned")
            control, revision = ctypes.c_uint16(), _DWORD()
            self._required(
                self.security.GetSecurityDescriptorControl(
                    descriptor, ctypes.byref(control), ctypes.byref(revision)
                ),
                "DACL control",
            )
            if not inherited and not control.value & 0x1000:
                raise PrimaryStateError("prepared-primary DACL must be protected")
            header = ctypes.cast(acl, ctypes.POINTER(_Acl)).contents
            grants: set[str] = set()
            if header.count != 2:
                raise PrimaryStateError("prepared-primary private grants are missing")
            for ordinal in range(header.count):
                pointer = _PTR()
                self._required(
                    self.security.GetAce(acl, ordinal, ctypes.byref(pointer)), "DACL entry"
                )
                ace = ctypes.cast(pointer, ctypes.POINTER(_Ace)).contents
                if ace.kind != 0 or ace.size < 16:
                    raise PrimaryStateError("prepared-primary DACL contains an unsupported grant")
                if pointer.value is None:
                    raise PrimaryStateError("prepared-primary DACL entry is unavailable")
                sid = self._sid_text(_PTR(pointer.value + ctypes.sizeof(_Ace)))
                if sid not in (self.sid, "S-1-5-18"):
                    raise PrimaryStateError("prepared-primary DACL grants another principal access")
                if ace.mask & _FULL_ACCESS != _FULL_ACCESS or ace.flags & 8:
                    raise PrimaryStateError("prepared-primary private grant is incomplete")
                if directory and ace.flags & 3 != 3:
                    raise PrimaryStateError(
                        "prepared-primary parent cannot create private sidecars"
                    )
                grants.add(sid)
            if not {self.sid, "S-1-5-18"} <= grants:
                raise PrimaryStateError("prepared-primary owner and SYSTEM grants are required")
        finally:
            self.kernel.LocalFree(descriptor)

    def flush(self, handle: int) -> None:
        self._required(self.kernel.FlushFileBuffers(handle), "file flush")

    def ntfs(self, handle: int, path: Path) -> None:
        if self.kernel.GetDriveTypeW(path.anchor) != 3:
            raise PrimaryStateError("prepared-primary placement requires a local fixed volume")
        filesystem = ctypes.create_unicode_buffer(32)
        self._required(
            self.kernel.GetVolumeInformationByHandleW(
                handle, None, 0, None, None, None, filesystem, len(filesystem)
            ),
            "volume identity",
        )
        if filesystem.value != "NTFS":
            raise PrimaryStateError("prepared-primary placement requires NTFS")


class WindowsContainer:
    """Pin existing ancestry, a permanent exclusive owner and one database identity.

    The caller closes all SQLite connections/readers before releasing this guard.
    The parent is provisioned before initialization and must already give newly
    created files private owner/SYSTEM access.
    """

    vfs = "win32"

    def __init__(self, path: Path, *, initialize: bool) -> None:
        self._api = _WindowsAPI()
        self.path = Path(path)
        self._handles: list[tuple[int, Path, bool, tuple[int, int, int]]] = []
        self._journal_identity: tuple[int, int, int] | None = None
        self._closed = False
        self._validate_path()
        try:
            for parent in reversed(self.path.parents):
                self._pin(parent, directory=True)
            parent_handle = self._handles[-1][0]
            self._api.ntfs(parent_handle, self.path.parent)
            self._api.private(parent_handle, directory=True)
            self._owner = self._pin(Path(str(self.path) + ".owner"), create=initialize, owner=True)
            self._api.private(self._owner)
            if initialize:
                self._api.flush(self._owner)
            self._database = self._pin(self.path, create=initialize)
            self._api.private(self._database)
            if initialize:
                self._api.flush(self._database)
            for suffix in ("-journal", "-wal", "-shm") if initialize else ("-wal", "-shm"):
                try:
                    handle = self._api.open(Path(str(self.path) + suffix), journal=True)
                except FileNotFoundError:
                    continue
                self._api.close(handle)
                raise PrimaryStateError("prepared-primary container has an unexpected sidecar")
            self.check_journal(required=not initialize)
        except BaseException:
            self.close()
            raise

    def _validate_path(self) -> None:
        value = str(self.path)
        if not self.path.is_absolute() or value.startswith(("\\\\", "//")):
            raise PrimaryStateError("prepared-primary placement requires an absolute local path")
        if not _BASENAME.fullmatch(self.path.name):
            raise PrimaryStateError("prepared-primary container basename is not allocation-derived")
        for part in self.path.parts[1:]:
            if (
                not part
                or part in (".", "..")
                or part.endswith((" ", "."))
                or any(ord(char) < 32 or char in ':<>"/\\|?*' for char in part)
                or _DEVICE.match(part)
            ):
                raise PrimaryStateError("prepared-primary placement uses an unsupported name")

    def _pin(
        self, path: Path, *, directory: bool = False, create: bool = False, owner: bool = False
    ) -> int:
        handle = self._api.open(path, create=create, owner=owner, directory=directory)
        try:
            identity = self._api.information(handle, path, directory=directory)
        except BaseException:
            self._api.close(handle)
            raise
        self._handles.append((handle, path, directory, identity))
        return handle

    def check_current(self) -> None:
        if self._closed:
            raise PrimaryStateError("prepared-primary construction owner is closed")
        for handle, path, directory, identity in self._handles:
            if self._api.information(handle, path, directory=directory) != identity:
                raise PrimaryStateError("prepared-primary container identity changed")
        self._api.private(self._handles[-3][0], directory=True)
        self._api.private(self._owner)
        self._api.private(self._database)

    def check_journal(self, *, required: bool, recovering: bool = False) -> None:
        self.check_current()
        path = Path(str(self.path) + "-journal")
        try:
            handle = self._api.open(path, journal=True)
        except FileNotFoundError:
            if required:
                raise PrimaryStateError("prepared-primary durable journal is missing") from None
            return
        try:
            identity = self._api.information(handle, path, directory=False)
            self._api.private(handle, inherited=True)
            if (
                not recovering
                and self._journal_identity is not None
                and identity != self._journal_identity
            ):
                raise PrimaryStateError("prepared-primary journal identity changed")
            self._journal_identity = identity
        finally:
            self._api.close(handle)

    def close(self) -> None:
        self._closed = True
        failures = []
        while self._handles:
            handle, _path, _directory, _identity = self._handles.pop()
            try:
                self._api.close(handle)
            except PrimaryStateError as exc:
                failures.append(exc)
        if failures:
            raise failures[0]
