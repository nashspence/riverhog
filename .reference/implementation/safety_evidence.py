"""One actual Native retained-session SIGKILL witness for the #1000 handoff.

This is source-test scaffolding, not a product recovery owner or an evidence
registry. It reuses the existing test-owned construction/receiving primitive.
The injected hooks stop immediately around a real SQLite source/parent commit;
all framing, SQL, fencing, cold loading and validation execute current product
code. SIGKILL is process-death evidence, never OS-crash or power-loss evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import selectors
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Literal

CHECKOUT = Path(__file__).resolve().parents[2]
if str(CHECKOUT) not in sys.path:
    sys.path.insert(0, str(CHECKOUT))

from riverhog_provenance import _session_checkpoint as checkpoint_owner  # noqa: E402
from riverhog_provenance import _session_input as input_owner  # noqa: E402
from riverhog_provenance import iter_journal_frames  # noqa: E402
from riverhog_provenance.journal_session import (  # noqa: E402
    JournalValidationBudget,
    JournalValidationSession,
)
from riverhog_provenance.replay_binding import JournalReplayBinding  # noqa: E402
from riverhog_provenance_contracts import ContractCatalog  # noqa: E402

from tests.support.native_construction import JournalConstructions  # noqa: E402

Cut = Literal["before_source_commit", "after_source_commit"]
CUTS: tuple[Cut, ...] = ("before_source_commit", "after_source_commit")
ATTEMPT = "8" * 32
WINDOW = 7


class ExactSource:
    """The same finite, captured-source contract used by retained Native tests."""

    def __init__(self, raw: bytes, catalog: ContractCatalog, *, version: str = "7") -> None:
        self.raw = raw
        frames = tuple(iter_journal_frames((raw,)))
        self.binding = JournalReplayBinding(
            attempt_id="reference-kill-original-capture",
            directory_id="reference-kill-original-source",
            journal_id=frames[0].document["journal_id"],
            source_bytes=str(len(raw)),
            source_sha256=hashlib.sha256(raw).hexdigest(),
            source_version=version,
            frame_count=str(len(frames)),
            catalog_sha256=catalog.checkpoint_sha256(),
            require_profiles=True,
        )
        self.current = True
        self.calls: list[tuple[str, int, int]] = []

    def read_window(self, offset: int, maximum_octets: int) -> bytes:
        self.calls.append(("window", offset, maximum_octets))
        return self.raw[offset : offset + maximum_octets]

    def read_exact(self, offset: int, octets: int) -> bytes:
        assert 0 <= offset <= offset + octets <= len(self.raw)
        self.calls.append(("exact", offset, octets))
        return self.raw[offset : offset + octets]

    def verify_current(self) -> bool:
        return self.current


def open_session(
    directory: Path,
    source: ExactSource,
    catalog: ContractCatalog,
    originals: JournalConstructions,
    *,
    create: bool,
    epoch: int,
) -> JournalValidationSession:
    return JournalValidationSession.open_retained(
        directory,
        source,
        attempt_id=ATTEMPT,
        worker_epoch=epoch,
        create=create,
        catalog=catalog,
        construction=originals.retain(),
    )


def _handshake(session: JournalValidationSession, source: ExactSource, cut: Cut) -> None:
    # Every hook is after the real owner performed the relevant mutation. The
    # before-commit hook is inside the transaction; the after hook is outside it.
    print(
        json.dumps(
            {
                "cut": cut,
                "pid": os.getpid(),
                "in_transaction": session._store.db.in_transaction,
                "cached_source_cursor": session._input.cursor.byte_offset,
                "source_sha256": source.binding.source_sha256,
                "source_calls": source.calls,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    # The parent never sends a byte. A closed pipe is a failed fixture, not an
    # instruction to continue past the requested fault window.
    os.read(sys.stdin.fileno(), 1)
    raise AssertionError("parent released the fault window without killing the original child")


def _child(directory: Path, raw_path: Path, cut: Cut) -> int:
    catalog = ContractCatalog()
    source = ExactSource(raw_path.read_bytes(), catalog)
    originals = JournalConstructions()
    session = open_session(directory, source, catalog, originals, create=True, epoch=0)
    if cut == "before_source_commit":
        publish = checkpoint_owner.ValidationLedger._publish

        def before_commit(
            self: checkpoint_owner.ValidationLedger,
            before: checkpoint_owner.ValidationSnapshot,
            after: checkpoint_owner.ValidationCheckpoint,
            *,
            prefix: tuple[str, bytes, str] | None = None,
        ) -> checkpoint_owner.ValidationSnapshot:
            result = publish(self, before, after, prefix=prefix)
            _handshake(session, source, cut)
            return result

        checkpoint_owner.ValidationLedger._publish = before_commit  # type: ignore[method-assign]
    else:
        source_publish = input_owner.RetainedInput._publish

        def after_commit(
            self: input_owner.RetainedInput,
            before: checkpoint_owner.ValidationSnapshot,
            after: input_owner._InputState,
        ) -> checkpoint_owner.ValidationSnapshot:
            result = source_publish(self, before, after)
            _handshake(session, source, cut)
            return result

        input_owner.RetainedInput._publish = after_commit  # type: ignore[method-assign]
    session.advance(JournalValidationBudget(rows=1, source_octets=WINDOW))
    raise AssertionError("actual Native source commit did not reach the selected hook")


def _read_handshake(process: subprocess.Popen[bytes]) -> dict[str, object]:
    assert process.stdout is not None
    deadline = time.monotonic() + 30
    data = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while b"\n" not in data:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not selector.select(remaining):
                raise TimeoutError("actual Native child did not reach its fault window")
            part = os.read(process.stdout.fileno(), 4096)
            if not part:
                raise RuntimeError("actual Native child exited before its fault window")
            data.extend(part)
            if len(data) > 16384:
                raise RuntimeError("fault handshake exceeded its fixed fixture envelope")
    marker = json.loads(data)
    if type(marker) is not dict:
        raise RuntimeError("fault handshake is not an object")
    return marker


def run_retained_journal_kill(workspace: Path, journal: bytes, cut: Cut) -> dict[str, object]:
    """Kill/reap one actual child, then verify exact cold Native continuation.

    `workspace` is an absent, test-owned path outside the checkout. Failure does
    not erase retained files. Caller owns their eventual test-fixture disposal.
    No provider, shared service or descendant worker is started by this fixture.
    """
    if os.name != "posix" or cut not in CUTS:
        raise ValueError("this witness requires POSIX SIGKILL and an exact supported cut")
    workspace.mkdir(mode=0o700)
    directory = workspace / "retained-session"
    directory.mkdir(mode=0o700)
    raw_path = workspace / "original-journal"
    with raw_path.open("xb") as output:
        output.write(journal)
    raw_path.chmod(0o600)
    started = time.perf_counter_ns()
    process = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--child",
            str(directory),
            str(raw_path),
            cut,
        ],
        cwd=CHECKOUT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        marker = _read_handshake(process)
        assert marker["pid"] == process.pid and marker["cut"] == cut
        assert marker["source_sha256"] == hashlib.sha256(journal).hexdigest()
        assert marker["in_transaction"] is (cut == "before_source_commit")
        assert marker["cached_source_cursor"] == (0 if cut == "before_source_commit" else WINDOW)
        assert marker["source_calls"] == [["window", 0, WINDOW]]
        process.kill()
        stdout, stderr = process.communicate(timeout=30)
        assert process.returncode == -signal.SIGKILL, (process.returncode, stdout, stderr)
        assert not stdout and not stderr, (stdout, stderr)
    finally:
        # Retain the Popen until the original child is reaped and its streams are
        # closed. If receiving itself fails, propagate it; do not erase workspace.
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=30)
    killed_ns = time.perf_counter_ns() - started
    catalog = ContractCatalog()
    source = ExactSource(journal, catalog)
    originals = JournalConstructions()
    resumed_started = time.perf_counter_ns()
    try:
        with open_session(directory, source, catalog, originals, create=False, epoch=1) as resumed:
            cursor = resumed._input.cursor.byte_offset
            expected_cursor = 0 if cut == "before_source_commit" else WINDOW
            assert cursor == expected_cursor
            calls = 0
            for _ in range(100000):
                calls += 1
                if resumed.advance(JournalValidationBudget(rows=64)).complete:
                    break
            else:
                raise AssertionError("small retained fixture did not finish")
            with resumed.result() as result:
                digest = result.journal_sha256
                materialized = result.materialize()
            assert digest == hashlib.sha256(journal).hexdigest()
        assert source.calls and source.calls[0][0:2] == ("window", expected_cursor)
    finally:
        originals.close()
    resumed_ns = time.perf_counter_ns() - resumed_started
    return {
        "evidence_class": "actual-posix-process-kill-current-native-owner",
        "cut": cut,
        "signal": "SIGKILL",
        "child_returncode": process.returncode,
        "original_child_reaped": process.returncode is not None,
        "original_parent_constructions_received": not originals.frames,
        "source_sha256": digest,
        "source_bytes": len(journal),
        "source_calls_before_kill": marker["source_calls"],
        "resumed_source_cursor": cursor,
        "source_calls_after_reopen": source.calls,
        "completion_advance_calls": calls,
        "result_sha256": hashlib.sha256(
            json.dumps(materialized, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "kill_and_receive_elapsed_ns": killed_ns,
        "cold_resume_and_receive_elapsed_ns": resumed_ns,
        "workspace": str(workspace),
        "scope_excludes": [
            "adaptive-cutover",
            "os-crash",
            "power-loss",
            "Windows",
            "provider-effects",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", action="store_true", required=True)
    parser.add_argument("directory", type=Path)
    parser.add_argument("raw_path", type=Path)
    parser.add_argument("cut", choices=CUTS)
    args = parser.parse_args()
    return _child(args.directory, args.raw_path, args.cut)


if __name__ == "__main__":
    raise SystemExit(main())
