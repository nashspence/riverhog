"""Bounded FFprobe stream/container acquisition for the separate stream contract."""

from __future__ import annotations

import hashlib
import os
import selectors
import shutil
import time
from pathlib import Path

from a_stove0_ffprobe_streams_contract_lib import MAX_REPORT_BYTES
from stove0_extension_support import subprocess

_MAX_STDERR_BYTES = 64 * 1024


class StreamProbeError(ValueError):
    pass


def ffprobe_tool_identity(command: str) -> tuple[str, str]:
    executable = shutil.which(command)
    if executable is None:
        raise StreamProbeError("FFprobe executable is unavailable")
    digest = hashlib.sha256()
    with open(executable, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    output = _bounded_command(
        [executable, "-version"],
        maximum_stdout=4096,
        maximum_stderr=4096,
        timeout_seconds=10,
    )
    try:
        version = output.decode("utf-8", "strict").splitlines()[0][:200]
    except (UnicodeDecodeError, IndexError) as exc:
        raise StreamProbeError("FFprobe version is malformed") from exc
    if not version:
        raise StreamProbeError("FFprobe version is empty")
    return version, digest.hexdigest()


def bounded_ffprobe_report(command: str, source: Path, *, timeout_seconds: int) -> bytes:
    """Kill the tool on size/deadline breach rather than buffering unlimited stdout."""
    if not 1 <= timeout_seconds <= 300:
        raise ValueError("FFprobe stream deadline is outside the supported bound")
    executable = shutil.which(command)
    if executable is None:
        raise StreamProbeError("FFprobe executable is unavailable")
    return _bounded_command(
        [executable, "-v", "error", "-show_streams", "-show_format", "-of", "json", str(source)],
        maximum_stdout=MAX_REPORT_BYTES,
        maximum_stderr=_MAX_STDERR_BYTES,
        timeout_seconds=timeout_seconds,
    )


def _bounded_command(
    argv: list[str], *, maximum_stdout: int, maximum_stderr: int, timeout_seconds: int
) -> bytes:
    process = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
    )
    selector = selectors.DefaultSelector()
    output = bytearray()
    error = bytearray()
    deadline = time.monotonic() + timeout_seconds
    try:
        assert process.stdout is not None and process.stderr is not None
        selector.register(process.stdout, selectors.EVENT_READ, output)
        selector.register(process.stderr, selectors.EVENT_READ, error)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(argv, timeout_seconds)
            for key, _ in selector.select(remaining):
                chunk = os.read(key.fd, 64 * 1024)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                target = key.data
                maximum = maximum_stdout if target is output else maximum_stderr
                if len(target) + len(chunk) > maximum:
                    raise StreamProbeError("FFprobe report or diagnostic exceeds its bound")
                target.extend(chunk)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(argv, timeout_seconds)
        returncode = process.wait(timeout=remaining)
        if returncode or error or not output:
            raise StreamProbeError("FFprobe returned failure or an empty report")
        return bytes(output)
    except BaseException:
        process.kill()
        process.wait()
        raise
    finally:
        selector.close()
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()


__all__ = ["StreamProbeError", "bounded_ffprobe_report", "ffprobe_tool_identity"]
