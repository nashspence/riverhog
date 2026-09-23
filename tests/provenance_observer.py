"""Native observer composition used only by cross-platform repository tests."""

from __future__ import annotations

import sys

from riverhog_provenance import FileStateObserver, UnsupportedPlatformError


def native_provenance_observer() -> FileStateObserver:
    if sys.platform.startswith("linux"):
        from a_riverhog_linux_provenance_observer import LinuxFileStateObserver

        return LinuxFileStateObserver()
    if sys.platform == "darwin":
        from a_riverhog_macos_provenance_observer import MacOSFileStateObserver

        return MacOSFileStateObserver()
    if sys.platform == "win32":
        from a_riverhog_windows_provenance_observer import WindowsFileStateObserver

        return WindowsFileStateObserver()
    raise UnsupportedPlatformError(f"no test provenance observer for {sys.platform!r}")


__all__ = ["native_provenance_observer"]
