"""Native observer composition used only by cross-platform repository tests."""

from __future__ import annotations

import sys

from riverhog_provenance.native_capture import UnsupportedPlatformError
from riverhog_provenance.native_source import NativeFileObserver


def native_provenance_observer() -> NativeFileObserver:
    if sys.platform.startswith("linux"):
        from a_riverhog_linux_provenance_observer import LinuxProvenanceObserver

        return LinuxProvenanceObserver()
    if sys.platform == "darwin":
        from a_riverhog_macos_provenance_observer import MacOSProvenanceObserver

        return MacOSProvenanceObserver()
    if sys.platform == "win32":
        from a_riverhog_windows_provenance_observer import WindowsProvenanceObserver

        return WindowsProvenanceObserver()
    raise UnsupportedPlatformError(f"no test provenance observer for {sys.platform!r}")


__all__ = ["native_provenance_observer"]
