"""Subprocess API for native consumers owned by the current extension attempt."""

from __future__ import annotations

import json
import os
import subprocess as _subprocess
import sys
import threading
from collections.abc import Sequence
from types import TracebackType
from typing import IO, Any, Literal, Self, overload

from stove0_extension_support.consumers import CURRENT_CONSUMERS

PIPE = _subprocess.PIPE
STDOUT = _subprocess.STDOUT
DEVNULL = _subprocess.DEVNULL
CalledProcessError = _subprocess.CalledProcessError
TimeoutExpired = _subprocess.TimeoutExpired
SubprocessError = _subprocess.SubprocessError
CompletedProcess = _subprocess.CompletedProcess


class SupervisedProcess[T: (str, bytes)]:
    def __init__(self, args: Sequence[str], **kwargs: Any) -> None:
        scope = CURRENT_CONSUMERS.get()
        if scope is None:
            raise RuntimeError("supervised payload needs a component consumer scope")
        if os.name != "posix":
            # Supplied native payload components use Linux images. Fail before
            # launch when this deployment has no established native supervisor.
            raise RuntimeError("native payload containment requires a POSIX supervisor")
        if kwargs.get("shell") or kwargs.get("preexec_fn") or kwargs.get("pass_fds"):
            raise ValueError("supervised tools require a direct executable invocation")
        self.args = args
        self._scope = scope
        self._control_lock = threading.Lock()
        self._write: int | None = None
        read, write = os.pipe()
        try:
            self._process: _subprocess.Popen[T] = _subprocess.Popen(
                [sys.executable, "-m", "stove0_extension_support.process_supervisor", str(read)],
                pass_fds=(read, scope.owner.descriptor),
                **kwargs,
            )
            self._write = write
            scope.retain(self)
            document = (
                json.dumps([os.fspath(value) for value in args], ensure_ascii=True).encode() + b"\n"
            )
            with os.fdopen(os.dup(write), "wb") as channel:
                channel.write(document)
        except BaseException:
            if self._write is not None:
                self.terminate()
                self._process.wait()
                scope.stopped(self)
            else:
                os.close(write)
            raise
        finally:
            os.close(read)

    @property
    def pid(self) -> int:
        return self._process.pid

    @property
    def returncode(self) -> int | None:
        return self._process.returncode

    @property
    def stdin(self) -> IO[T] | None:
        return self._process.stdin

    @property
    def stdout(self) -> IO[T] | None:
        return self._process.stdout

    @property
    def stderr(self) -> IO[T] | None:
        return self._process.stderr

    def terminate(self) -> None:
        with self._control_lock:
            if self._write is not None:
                os.close(self._write)
                self._write = None

    def kill(self) -> None:
        # Killing only the guardian would abandon its still-running consumers.
        self.terminate()

    def _stopped(self) -> None:
        if self.returncode is not None:
            self.terminate()
            self._scope.stopped(self)

    def poll(self) -> int | None:
        result = self._process.poll()
        self._stopped()
        return result

    def wait(self, timeout: float | None = None) -> int:
        try:
            return self._process.wait(timeout=timeout)
        finally:
            self._stopped()

    def communicate(self, input: T | None = None, timeout: float | None = None) -> tuple[T, T]:
        try:
            return self._process.communicate(input, timeout)
        finally:
            self._stopped()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.terminate()
        self.wait()
        for stream in (self.stdin, self.stdout, self.stderr):
            if stream is not None:
                stream.close()


type Process[T: (str, bytes)] = _subprocess.Popen[T] | SupervisedProcess[T]


@overload
def Popen(args: Sequence[str], *, text: Literal[True], **kwargs: Any) -> Process[str]: ...


@overload
def Popen(
    args: Sequence[str], *, text: Literal[False] = False, **kwargs: Any
) -> Process[bytes]: ...


def Popen(args: Sequence[str], **kwargs: Any) -> Process[Any]:
    if CURRENT_CONSUMERS.get() is None:
        return _subprocess.Popen(args, **kwargs)
    return SupervisedProcess(args, **kwargs)


@overload
def run(
    args: Sequence[str],
    *,
    text: Literal[True],
    input: str | None = None,
    capture_output: bool = False,
    timeout: float | None = None,
    check: bool = False,
    **kwargs: Any,
) -> _subprocess.CompletedProcess[str]: ...


@overload
def run(
    args: Sequence[str],
    *,
    text: Literal[False] = False,
    input: bytes | None = None,
    capture_output: bool = False,
    timeout: float | None = None,
    check: bool = False,
    **kwargs: Any,
) -> _subprocess.CompletedProcess[bytes]: ...


def run(
    args: Sequence[str],
    *,
    input: str | bytes | None = None,
    capture_output: bool = False,
    timeout: float | None = None,
    check: bool = False,
    **kwargs: Any,
) -> _subprocess.CompletedProcess[Any]:
    if CURRENT_CONSUMERS.get() is None:
        return _subprocess.run(
            args, input=input, capture_output=capture_output, timeout=timeout, check=check, **kwargs
        )
    if input is not None:
        if "stdin" in kwargs:
            raise ValueError("stdin and input cannot both be supplied")
        kwargs["stdin"] = PIPE
    if capture_output:
        if "stdout" in kwargs or "stderr" in kwargs:
            raise ValueError("capture_output and output streams cannot both be supplied")
        kwargs["stdout"] = PIPE
        kwargs["stderr"] = PIPE
    with Popen(args, **kwargs) as process:
        try:
            stdout, stderr = process.communicate(input, timeout=timeout)
        except BaseException:
            process.kill()
            process.communicate()
            raise
        if check and process.returncode:
            raise CalledProcessError(process.returncode, args, output=stdout, stderr=stderr)
        assert process.returncode is not None
        return CompletedProcess(args, process.returncode, stdout, stderr)
