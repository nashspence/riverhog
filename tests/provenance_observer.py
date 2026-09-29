"""Native observer composition used only by cross-platform repository tests."""

from __future__ import annotations

import sys

from riverhog_provenance.native_capture import UnsupportedPlatformError
from riverhog_provenance.native_source import NativeFileObserver
from riverhog_provenance.providers import ResolvedProvenanceObserver, resolve_provenance_observer


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


def native_provenance_provider() -> ResolvedProvenanceObserver:
    if sys.platform.startswith("linux"):
        name = "a-riverhog-linux-provenance-observer"
    elif sys.platform == "darwin":
        name = "a-riverhog-macos-provenance-observer"
    elif sys.platform == "win32":
        name = "a-riverhog-windows-provenance-observer"
    else:
        raise UnsupportedPlatformError(f"no test provenance provider for {sys.platform!r}")
    return resolve_provenance_observer(name)


__all__ = ["native_provenance_observer", "native_provenance_provider"]
