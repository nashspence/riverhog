"""Bounded failure observations for the maintained FTP publication fixture."""

from __future__ import annotations

import json
import sys

import httpx

_STAGES = {
    "fixture",
    "listener-uploads",
    "listener-handoff",
    "spool-health",
    "spool-flush",
    "flush-result",
    "spool-status",
    "source-cleanup",
    "receipt-confirmation",
    "receipt-read",
    "classification",
}
_COUNTS = {"completed", "failed", "claims", "new_receipts"}
_stage = "fixture"
_counts: dict[str, int] = {}


def publication_stage(stage: str) -> None:
    global _stage
    if stage not in _STAGES:
        raise ValueError("unknown publication diagnostic stage")
    _stage = stage
    _counts.clear()


def publication_count(name: str, value: int) -> None:
    if name not in _COUNTS or type(value) is not int or value < 0:
        raise ValueError("invalid publication diagnostic count")
    _counts[name] = value


def install_publication_diagnostics(stage: str) -> None:
    """Annotate failure without copying messages, paths, URLs or response bodies.

    Preserve the original exception hook and process failure. These observations
    locate the failed fixture boundary; they do not establish its root cause.
    """
    publication_stage(stage)
    previous = sys.excepthook

    def failed(error_type, error, traceback):
        category = "unexpected"
        diagnostic: dict[str, object] = {
            "format": "riverhog-ftp-publication-diagnostic/v1",
            "stage": _stage,
            "counts": dict(_counts),
        }
        if isinstance(error, httpx.HTTPStatusError):
            category = "http-status"
            diagnostic["http_status"] = error.response.status_code
        elif isinstance(error, httpx.TimeoutException):
            category = "http-timeout"
        elif isinstance(error, AssertionError):
            category = "assertion"
        elif isinstance(error, OSError):
            category = "filesystem"
        diagnostic["failure_class"] = category
        body = json.dumps(diagnostic, sort_keys=True, separators=(",", ":"))
        escaped = body.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print("::error title=FTP publication qualification::" + escaped, file=sys.stderr)
        previous(error_type, error, traceback)

    sys.excepthook = failed
