from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import sys
from collections.abc import Iterator
from contextlib import closing, contextmanager
from copy import copy
from dataclasses import replace
from pathlib import Path
from tempfile import mkdtemp

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import _session_input as input_owner
from riverhog_provenance import framing as framing_owner
from riverhog_provenance import journal_session as session_owner
from riverhog_provenance._changed_assertions import read_changed_assertion
from riverhog_provenance._entry_replay import EntryReplay, ReplayBudget
from riverhog_provenance.journal import JournalSummary
from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationSession,
    _checkpoint_progress,
)
from riverhog_provenance.replay_binding import JournalReplayBinding
from test_child_admission_growth import octets
from test_entry_replay_advance_growth import AdvanceConnection, advance_inputs, advance_observer
from test_entry_replay_graph_growth import graph_histories as graph_histories
from test_entry_replay_graph_growth import initialize_graph_cache
from test_entry_replay_growth import REGIMES
from test_entry_replay_growth import admission_histories as admission_histories
from test_semantic_edge import observed_work
from test_semantic_owner import snapshot


class SessionConnection(AdvanceConnection):
    @contextmanager
    def observe(self):
        with super().observe() as metrics:
            metrics.update(
                {
                    name: 0
                    for name in (
                        "source_verifications",
                        "source_windows",
                        "source_atoms",
                        "caller_pulls",
                        "source_port_octets",
                        "framing_visited_octets",
                        "framing_consumed_octets",
                        "framing_returned_positions",
                        "framing_retained_content_octets",
                        "caller_copied_octets",
                        "returned_handles",
                        "opened_databases",
                    )
                }
            )
            yield metrics


class FiniteSource:
    def __init__(self, raw, frame, frame_count, catalog, *, window=65536, directory_width=64):
        self.raw, self.window = raw, window
        self.binding = JournalReplayBinding(
            attempt_id="exact-source-capture",
            directory_id="d" * directory_width,
            journal_id=frame.document["journal_id"],
            source_bytes=str(len(raw)),
            source_sha256=hashlib.sha256(raw).hexdigest(),
            source_version="7",
            frame_count=str(frame_count),
            catalog_sha256=catalog.checkpoint_sha256(),
            require_profiles=True,
        )
        self.calls = []
        self.current = True

    def verify_current(self):
        self.calls.append(("verify",))
        return self.current

    def read_window(self, offset, maximum_octets):
        self.calls.append(("window", offset, maximum_octets))
        return self.raw[offset : offset + min(maximum_octets, self.window)]

    def read_exact(self, offset, length):
        self.calls.append(("atom", offset, length))
        assert 0 <= offset <= offset + length <= len(self.raw)
        return self.raw[offset : offset + length]


class SourcePorts:
    """Count actual port results while executing their provider-owned bodies normally."""

    def __init__(self, source, db):
        self.source, self.db = source, db

    @property
    def binding(self):
        return self.source.binding

    def invoke(self, kind, function, *args):
        recording = self.db.recording
        if recording:
            self.db.metrics[kind] += 1
            self.db.recording = False
        try:
            result = function(*args)
        finally:
            self.db.recording = recording
        if recording and type(result) is bytes:
            self.db.metrics["source_port_octets"] += len(result)
        return result

    def verify_current(self):
        return self.invoke("source_verifications", self.source.verify_current)

    def read_window(self, offset, maximum_octets):
        return self.invoke("source_windows", self.source.read_window, offset, maximum_octets)

    def read_exact(self, offset, length):
        return self.invoke("source_atoms", self.source.read_exact, offset, length)


class CallerChunks(Iterator):
    def __init__(self, chunks, db=None):
        self.chunks, self.position, self.db = tuple(chunks), 0, db

    def __next__(self):
        recording = self.db is not None and self.db.recording
        if recording:
            self.db.metrics["caller_pulls"] += 1
        if self.position == len(self.chunks):
            raise StopIteration
        result = self.chunks[self.position]
        self.position += 1
        if recording:
            self.db.metrics["source_port_octets"] += len(result)
        return result


@contextmanager
def session_observer(session, dimensions, operation, *, failed_operations=()):
    db = session._store.db
    framing = framing_owner.advance_journal_framing
    borrow = JournalSummary.borrow
    real_octets, real_connect = framing_owner._octets, sqlite3.connect
    visited = [0]
    session._observed_readers = []

    def scan(raw):
        for value in real_octets(raw):
            if db.recording:
                visited[0] += 1
            yield value

    def connect(*args, **kwargs):
        if db.recording:
            db.metrics["opened_databases"] += 1
        return real_connect(*args, **kwargs)

    real_scan = session_owner._ChunkInput.scan
    real_frame = session_owner._ChunkInput.frame

    def caller_scan(current, octets):
        if not db.recording:
            return real_scan(current, octets)
        code = real_scan.__code__
        handled, previous, before = [0], sys.gettrace(), len(current.buffer)

        def trace(frame, event, _argument):
            if frame.f_code is not code:
                return None
            if event == "line" and not handled[0] and "piece" in frame.f_locals:
                piece = frame.f_locals["piece"]
                assert type(piece) is bytes
                handled[0] = len(piece)
            return trace

        sys.settrace(trace)
        try:
            return real_scan(current, octets)
        finally:
            sys.settrace(previous)
            db.metrics["caller_copied_octets"] += handled[0] + 2 * (len(current.buffer) - before)

    def caller_frame(current, octets):
        result = real_frame(current, octets)
        if db.recording:
            assert len(result.json_bytes) == len(current.buffer)
            db.metrics["caller_copied_octets"] += len(result.json_bytes)
        return result

    @contextmanager
    def kernel(child, args):
        if child is framing:
            start, offset = visited[0], args[0].byte_offset

            def complete(result):
                return {
                    "visited_octets": visited[0] - start,
                    "consumed_octets": result.cursor.byte_offset - offset,
                    "returned_positions": len(result.positions),
                    "retained_content_octets": 0,
                }

            yield complete
        elif child is borrow:
            before, statements, opened = (
                args[0]._store._storage._readers,
                db.metrics["sql_statements"],
                db.metrics["opened_databases"],
            )

            def complete(result):
                session._observed_readers.append(result)
                assert result is not args[0] and result._store._storage is args[0]._store._storage
                assert result._store._storage._readers == before + 1
                return {
                    "returned_handles": 1,
                    "sql_statements": db.metrics["sql_statements"] - statements,
                    "opened_databases": db.metrics["opened_databases"] - opened,
                }

            yield complete
        else:
            yield None

    def primitive(child, _args, _result, work):
        if child is framing:
            for name in (
                "visited_octets",
                "consumed_octets",
                "returned_positions",
                "retained_content_octets",
            ):
                db.metrics["framing_" + name] += work[name]
        if child is borrow:
            db.metrics["returned_handles"] += work["returned_handles"]
        return work

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(framing_owner, "_octets", scan)
        patch.setattr(sqlite3, "connect", connect)
        patch.setattr(session_owner._ChunkInput, "scan", caller_scan)
        patch.setattr(session_owner._ChunkInput, "frame", caller_frame)
        with advance_observer(
            session._replay,
            dimensions,
            root_operation=operation,
            extra_owners={
                session_owner._checkpoint_progress: session_owner,
                framing: session_owner,
            },
            operation_aliases={framing: (input_owner, framing_owner)},
            kernel_hook=kernel,
            failed_operations=failed_operations,
            primitive_hook=primitive,
        ) as observed:
            yield observed


def progress_dimensions(session, before):
    accepted = session_owner.PrefixState.from_json(before.prefix[2])
    return {
        "metadata_row_octets": 1024
        + 2
        * octets(
            (
                before,
                session._ledger.binding,
                str(session._ledger.worker_epoch),
                session._input.cursor.byte_offset,
            )
        ),
        "padding_blocks": 1 if accepted.byte_count % 64 < 56 else 2,
    }


def selected_row(session, before, budget):
    selected = read_changed_assertion(
        session._store.db, after=before.checkpoint.row_after, byte_budget=budget.octets
    )
    return None if selected is None or selected.raw is None else json.loads(selected.raw)


def session_dimensions(session, budget, operation, *, cold):
    before = session._ledger.load()
    state = before.checkpoint
    original = snapshot(session._store.db)
    width = 16384 + 2 * octets(
        (
            before,
            session._ledger.binding,
            session._source_binding,
            getattr(session._input, "state", None),
        )
    )
    ledger = {
        "retained_rows": sum(
            len(original[name]) for name in ("journal_validation", "journal_prefix", "frames")
        ),
        "metadata_row_octets": width,
    }
    if operation is JournalValidationSession.result:
        return {"borrow_calls": 1, "borrow_snapshot_octets": session._summary.journal_bytes}, before
    retained = session._source is not None
    progress = operation is JournalValidationSession.progress
    finish = not progress and (
        state.phase == "terminal" or state.phase == "done" and session._summary is None
    )
    values = {
        "retained_rows": sum(len(rows) for rows in original.values()),
        "metadata_row_octets": width,
        "source_verifications": int(retained) * (1 + int(finish)),
    }
    inputs = {"load": ledger, "progress": progress_dimensions(session, before)}
    active = {"load", "progress"}
    if not progress:
        values.update(
            input_publications=0,
            input_atom_octets=0,
            source_windows=0,
            source_atoms=0,
            caller_pulls=0,
            source_window_octets=0,
            source_atom_octets=0,
            caller_pull_octets=0,
            caller_slice_octets=0,
        )
        accepted = session_owner.PrefixState.from_json(before.prefix[2])
        if state.phase not in {"frame", "terminal", "done"}:
            cached = session._replay._current
            case = {"before": before, "cached": cached, "candidate": cached.frame}
            kind = state.phase.removesuffix("-admission")
            if kind in {
                "entry",
                "targets",
                "staging",
                "retire",
                "seed",
                "generic",
                "row",
                "finish",
            }:
                from test_entry_replay_child_growth import family_for

                case["family"] = family_for(kind)
            if state.phase in {"row-admit", "lifecycle"}:
                selected = (
                    cached.row
                    if state.phase == "lifecycle"
                    else selected_row(session, before, budget)
                )
                case["row"] = selected
            if state.phase == "publish":
                publication = session._prefix.prepare_publication(cached.prepared)
                # Anchor finalization happens only at the exact selected prefix.
                expected = session._prefix.expected_anchor
                padding = (
                    0
                    if not expected
                    or accepted.matched_anchor
                    or publication.after.previous != expected["through"]
                    else (1 if publication.after.byte_count % 64 < 56 else 2)
                )
                case["dimensions"] = {
                    "retained_rows": values["retained_rows"],
                    "checkpoint_retained_rows": ledger["retained_rows"],
                    "metadata_row_octets": width,
                    "retained_frames": state.frame_ordinal,
                    "frame_octets": state.frame_octets,
                    "padding_blocks": padding,
                }
                inputs["progress"]["padding_blocks"] = (
                    1 if publication.after.byte_count % 64 < 56 else 2
                )
            inputs["entry_advance"], _ = advance_inputs(
                session._replay, case, ReplayBudget(budget.rows, budget.octets), cold=cold
            )
            active.add("entry_advance")
        elif state.phase == "frame":
            source_input = session._input
            sync = (
                retained
                and source_input.state.pending is not None
                and source_input.state.pending.ordinal != state.frame_ordinal
            )
            if sync:
                values["input_publications"] = 1
                active.update(("fence", "publication"))
            elif source_input.pending:
                frame_octets = (
                    source_input.state.pending.json_bytes if retained else len(source_input.buffer)
                )
                values["input_atom_octets"] = frame_octets
                values["source_atoms"] = int(retained)
                values["source_atom_octets"] = frame_octets if retained else 0
                inputs["entry_admission"] = {
                    "retained_rows": values["retained_rows"],
                    "checkpoint_retained_rows": ledger["retained_rows"],
                    "metadata_row_octets": width,
                    "retained_frames": state.frame_ordinal,
                    "frame_octets": frame_octets + 2,
                    "initial_tail_octets": accepted.byte_count % 64,
                    "source_blocks": (accepted.byte_count % 64 + frame_octets + 2) // 64,
                }
                active.add("entry_admission")
            elif source_input.complete:
                active.update(("fence", "publication"))
            else:
                raw = b""
                at_end = False
                if retained:
                    source = (
                        session._source.source
                        if isinstance(session._source, SourcePorts)
                        else session._source
                    )
                    remaining = len(source.raw) - source_input.cursor.byte_offset
                    size = min(
                        remaining,
                        budget.source_octets,
                        source_input._available(before, budget.octets),
                    )
                    raw = source.raw[
                        source_input.cursor.byte_offset : source_input.cursor.byte_offset
                        + min(size, source.window)
                    ]
                    values["source_windows"] = int(bool(size))
                    values["source_window_octets"] = len(raw)
                    values["input_publications"] = 1
                    at_end = len(raw) == remaining
                    active.update(("fence", "publication"))
                else:
                    chunk, offset, eof = source_input.chunk, source_input.offset, source_input.eof
                    if offset == len(chunk) and not eof:
                        values["caller_pulls"] = 1
                        chunks = source_input.chunks
                        if chunks.position < len(chunks.chunks):
                            chunk, offset = chunks.chunks[chunks.position], 0
                            values["caller_pull_octets"] = len(chunk)
                        else:
                            eof = True
                    raw = chunk[offset : offset + min(budget.source_octets, 65536)]
                    at_end = eof
                    values["caller_slice_octets"] = len(raw)
                empty_pull = (
                    not retained
                    and values["caller_pulls"]
                    and values["caller_pull_octets"] == 0
                    and not at_end
                )
                if not empty_pull:
                    step = framing_owner.advance_journal_framing(
                        source_input.cursor,
                        raw,
                        byte_budget=budget.octets if retained else budget.source_octets,
                        frame_budget=1,
                        at_end=at_end,
                    )
                    inputs["framing"] = {
                        "consumed_octets": step.cursor.byte_offset
                        - source_input.cursor.byte_offset,
                        "byte_budget": budget.octets if retained else budget.source_octets,
                        "frame_budget": 1,
                    }
                    active.add("framing")
        if finish:
            active.add("padding")
            inputs["padding"] = {"block_octets": 64}
            if state.phase != "done":
                active.update(("fence", "publication"))
        inputs.update(fence=ledger, publication=ledger)
    for tag, child in growth_binding(operation).delegates:
        calls = int(tag in active)
        if tag == "padding" and finish:
            calls = inputs["progress"]["padding_blocks"] * (1 + int(retained))
        values[tag + "_calls"] = calls
        for item in growth_binding(child).declaration.dimensions:
            values[tag + "_" + item.name] = inputs.get(tag, {}).get(item.name, 0)
    return values, before


def rejected_input_dimensions(session, budget, kind):
    values, before = session_dimensions(
        session, budget, JournalValidationSession.progress, cold=False
    )
    assert session._source is not None and before.checkpoint.phase == "frame"
    assert session._summary is None and session._failure is None
    prefix_octets = sum(len(v.encode()) if isinstance(v, str) else len(v) for v in before.prefix)
    minimum = 32768 + 16 * (
        len(session._ledger.binding) + prefix_octets + len(before.checkpoint.to_json().encode())
    )
    if kind == "metadata":
        assert budget.octets < minimum
    elif kind == "atom":
        pending = session._input.state.pending
        assert pending is not None and pending.ordinal == before.checkpoint.frame_ordinal
        assert minimum <= budget.octets < 2 * pending.json_bytes + 65536
    else:
        raise ValueError("unknown retained input rejection")
    dimensions = {
        item.name: values.get(item.name, 0)
        for item in growth_binding(JournalValidationSession.advance).declaration.dimensions
    }
    dimensions["progress_calls"] = 0
    assert dimensions["load_calls"] == dimensions["source_verifications"] == 1
    return dimensions, before


SESSION_CASES = (
    (REGIMES[0], "stream", 65536, None),
    (REGIMES[0], "retained", 65536, None),
    (REGIMES[0], "retained", 7, None),
    (REGIMES[3], "retained", 65536, None),
    (REGIMES[5], "retained", 65536, None),
    (REGIMES[6], "retained", 65536, None),
    (REGIMES[8], "retained", 65536, None),
    (REGIMES[12], "retained", 65536, None),
    (REGIMES[10], "retained", 65536, 55),
    (REGIMES[10], "stream", 65536, 56),
    (REGIMES[0], "retained", 65536, None, "graph-correction"),
)
ATTEMPT = "e" * 32


def finite_source(case, catalog, *, window):
    source = FiniteSource(
        case["raw"],
        case["candidate"],
        case.get("source_frame_count", case["state"].frame_count + 1),
        catalog,
        window=window,
    )
    if case["regime"][5] == 16384:
        from riverhog_provenance._journal_prefix import JournalPrefix
        from riverhog_provenance.journal_store import JournalStore

        with closing(JournalStore()) as store:
            prefix = JournalPrefix(
                store.db,
                attempt_id=ATTEMPT,
                source_binding=source.binding.to_json(),
                catalog=catalog,
                require_profiles=True,
            )
            added = 16384 - len(prefix.binding)
            assert added > 0
            source.binding = replace(
                source.binding, directory_id=source.binding.directory_id + "d" * added
            )
            prefix = JournalPrefix(
                store.db,
                attempt_id=ATTEMPT,
                source_binding=source.binding.to_json(),
                catalog=catalog,
                require_profiles=True,
            )
            assert len(prefix.binding) == 16384
    return source


def session_cases(root, catalog, build, graphs, *, specifications=SESSION_CASES):
    from copy import deepcopy

    from riverhog_provenance import encode_entry, parse_journal, validate_journal

    for regime in sorted({spec[0] for spec in specifications}, key=lambda value: value[0]):
        build(regime)
    cases = []
    for ordinal, specification in enumerate(specifications):
        regime, mode, window, tail, *variants = specification
        case = {**build(regime), "regime": regime}
        if variants:
            assert variants == ["graph-correction"]
            raw = graphs((17, 1, 2, 64))[0]["raw"]
            frames = parse_journal(raw)
            case.update(raw=raw, candidate=frames[-1], source_frame_count=len(frames))
        if tail is not None:
            prefix = case["raw"][: -len(case["candidate"].encoded)]
            document = deepcopy(case["candidate"].document)
            for padding in range(64):
                document["body"]["purpose"] = "urn:test:session-padding:" + "x" * padding
                raw = encode_entry(document, catalog=catalog)
                if (len(prefix) + len(raw)) % 64 == tail:
                    case["raw"], case["candidate"] = prefix + raw, parse_journal(raw)[0]
                    break
            else:
                pytest.fail("native session padding boundary was not found")
        source = finite_source(case, catalog, window=window)
        if mode == "retained":
            directory = root / f"native-session-{ordinal}"
            directory.mkdir(mode=0o700)
            session = JournalValidationSession.open_retained(
                directory,
                source,
                attempt_id=ATTEMPT,
                worker_epoch=regime[4],
                create=True,
                catalog=catalog,
            )
        else:
            session = JournalValidationSession((), attempt_id=ATTEMPT, catalog=catalog)
            session._input.chunks = CallerChunks((b"", case["raw"], b""))
        seen = set()
        try:
            budget = JournalValidationBudget(rows=64)
            for units in range(1000000):
                before = session._ledger.load()
                state = before.checkpoint
                accepted = session_owner.PrefixState.from_json(before.prefix[2])
                current = session._replay._current
                selected = None
                if state.phase == "frame":
                    pending = (
                        session._input.state.pending
                        if mode == "retained"
                        else session._input.pending
                    )
                    selected = (
                        bool(pending),
                        bool(
                            pending
                            and mode == "retained"
                            and pending.ordinal != state.frame_ordinal
                        ),
                        session._input.complete,
                        session._input.cursor.frame_start is None,
                        session._input.cursor.byte_offset == len(case["raw"]),
                        getattr(session._input, "eof", False),
                    )
                    if (
                        mode == "stream"
                        and session._input.offset == len(session._input.chunk)
                        and not session._input.eof
                    ):
                        chunks = session._input.chunks
                        next_kind = (
                            "eof"
                            if chunks.position == len(chunks.chunks)
                            else "data"
                            if chunks.chunks[chunks.position]
                            else "empty"
                        )
                        selected += (next_kind,)
                elif state.phase in {
                    "entry",
                    "targets",
                    "staging",
                    "retire",
                    "seed",
                    "generic",
                    "row",
                    "finish",
                }:
                    child = session._replay._child(state, current, state.phase, state.child_id)
                    child_state = child.load()
                    selected = (
                        child_state.done,
                        getattr(child, "kind", None),
                        getattr(child, "operation", None),
                        child_state.phase,
                    )
                    if getattr(child, "operation", None) == "row" and child_state.phase == "edge":
                        edge = child._edge_work(child_state, child._prefix())
                        selected += (edge.load().phase, edge.checker.progress().phase)
                padding = (
                    accepted.byte_count + (state.frame_octets if state.phase == "publish" else 0)
                ) % 64 >= 56
                key = state.phase, selected, padding
                if key not in seen:
                    seen.add(key)
                    path = root / f"session-{ordinal}-checkpoint-{len(seen)}.sqlite3"
                    with closing(sqlite3.connect(path)) as saved:
                        session._store.db.backup(saved)
                    stream_state = (
                        None
                        if mode == "retained"
                        else {
                            name: bytes(getattr(session._input, name))
                            if name == "buffer"
                            else getattr(session._input, name)
                            for name in (
                                "chunk",
                                "offset",
                                "cursor",
                                "buffer",
                                "pending",
                                "eof",
                                "complete",
                            )
                        }
                    )
                    remaining = (
                        ()
                        if mode == "retained"
                        else session._input.chunks.chunks[session._input.chunks.position :]
                    )
                    cases.append(
                        {
                            **case,
                            "path": path,
                            "mode": mode,
                            "window": window,
                            "binding": source.binding,
                            "before": before,
                            "key": key,
                            "cached": None
                            if current is None
                            else replace(current, child_kind=None, child_id=None, child=None),
                            "stream_state": stream_state,
                            "remaining": remaining,
                        }
                    )
                if state.phase == "done":
                    break
                changed = session.advance(budget)
                assert changed.version >= before.checkpoint.version
                if units % 4096 == 0:
                    (root / "session-trajectory-progress.json").write_text(
                        json.dumps(
                            {
                                "case_index": ordinal,
                                "units": units,
                                "phase": state.phase,
                                "source_offset": session._input.cursor.byte_offset,
                            }
                        )
                        + "\n"
                    )
            else:
                pytest.fail("finite native session trajectory did not complete")
            with (
                session.result() as actual,
                validate_journal(case["raw"], catalog=catalog) as expected,
            ):
                assert actual.materialize() == expected.materialize()
                assert actual.journal_bytes == len(case["raw"])
                assert actual.journal_sha256 == hashlib.sha256(case["raw"]).hexdigest()
        finally:
            session.close()
    return cases


@contextmanager
def session_fixture(case, catalog, *, cold, measured=False, summary=False):
    # Reuse the immutable source fact already verified by the native trajectory.
    source = object.__new__(FiniteSource)
    source.raw, source.window, source.binding = case["raw"], case["window"], case["binding"]
    source.calls, source.current = [], True
    owned_directory = None
    if case["mode"] == "retained":
        owned_directory = Path(mkdtemp(prefix="session-unit-", dir=case["path"].parent))
        with closing(sqlite3.connect(case["path"])) as seed:
            with closing(sqlite3.connect(owned_directory / "snapshot.sqlite3")) as destination:
                seed.backup(destination)
        session = JournalValidationSession.open_retained(
            owned_directory,
            source,
            attempt_id=ATTEMPT,
            worker_epoch=case["regime"][4] + 1,
            catalog=catalog,
        )
    else:
        session = JournalValidationSession((), attempt_id=ATTEMPT, catalog=catalog)
        with closing(sqlite3.connect(case["path"])) as seed:
            seed.backup(session._store.db)
        for name, value in case["stream_state"].items():
            setattr(session._input, name, bytearray(value) if name == "buffer" else value)
        session._input.chunks = CallerChunks(case["remaining"])
    try:
        initialize_graph_cache(session._store)
        before = session._ledger.load()
        session._store.frame_count = before.checkpoint.frame_ordinal
        if case["cached"] is not None:
            session._replay._current = replace(case["cached"])
        if not cold and session._replay._current is not None:
            state = before.checkpoint
            if state.phase in {
                "entry",
                "targets",
                "staging",
                "retire",
                "seed",
                "generic",
                "row",
                "finish",
            }:
                session._replay._start(before, ReplayBudget())
                session._replay._child(state, session._replay._current, state.phase, state.child_id)
        if measured:
            epoch = session._ledger.worker_epoch
            path = session._store._storage.directory / "snapshot.sqlite3"
            session._store.db.close()
            session._store._storage.connection = sqlite3.connect(path, factory=SessionConnection)
            session._store.db.execute("PRAGMA synchronous=FULL").close()
            session._store.db.execute("PRAGMA cache_size=-64").close()
            # Rebind native owners to the same fresh pager, preserving exact immutable controls.
            from riverhog_provenance._journal_prefix import JournalPrefix
            from riverhog_provenance._session_checkpoint import ValidationLedger

            prefix = session._prefix
            session._prefix = JournalPrefix(
                session._store.db,
                attempt_id=ATTEMPT,
                source_binding=json.loads(prefix.binding)["source_binding"],
                catalog=catalog,
                require_profiles=True,
            )
            session._ledger = ValidationLedger(
                session._store.db,
                attempt_id=ATTEMPT,
                binding=session._prefix.binding,
                worker_epoch=epoch,
            )
            session._replay = EntryReplay(
                session._store, prefix=session._prefix, ledger=session._ledger, catalog=catalog
            )
            if case["mode"] == "retained":
                session._input = input_owner.RetainedInput(session._ledger, source)
                ports = SourcePorts(source, session._store.db)
                session._source = session._input.source = ports
            else:
                session._input.chunks.db = session._store.db
            session._replay._current = None if cold else copy(case["cached"])
            if (
                not cold
                and session._replay._current is not None
                and before.checkpoint.phase
                in {"entry", "targets", "staging", "retire", "seed", "generic", "row", "finish"}
            ):
                session._replay._start(before, ReplayBudget())
                session._replay._child(
                    before.checkpoint,
                    session._replay._current,
                    before.checkpoint.phase,
                    before.checkpoint.child_id,
                )
        if summary:
            assert before.checkpoint.phase == "done"
            session._finish(before)
        yield session
    finally:
        storage = session._store._storage
        for reader in getattr(session, "_observed_readers", ()):
            reader.close()
        session.close()
        if owned_directory is not None:
            assert storage._closed
            shutil.rmtree(owned_directory)


def invoke(session, operation, budget):
    if operation is _checkpoint_progress:
        return operation(
            session.attempt_id,
            session._ledger,
            session._ledger.load(),
            session._input.cursor.byte_offset,
        )
    if operation is JournalValidationSession.advance:
        return operation(session, budget)
    return operation(session)


def checkpoint_samples(root, catalog, cases, *, operation, samples, early=False):
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    for ordinal, case in enumerate(cases):
        phase = case["before"].checkpoint.phase
        result_operation = operation is JournalValidationSession.result
        if result_operation and phase != "done":
            continue
        if operation in {_checkpoint_progress, JournalValidationSession.progress} and phase not in {
            "frame",
            "publish",
            "terminal",
            "done",
            "lifecycle",
        }:
            continue
        variants = (
            ((False, True),)
            if result_operation
            else (
                ((False, False), (False, True))
                if phase == "done" and operation is JournalValidationSession.advance
                else ((False, False), (True, False))
                if phase not in {"frame", "terminal", "done"}
                and operation is JournalValidationSession.advance
                else ((False, False),)
            )
        )
        quanta = (
            (1, 16, 64)
            if ordinal % 11 == 0 and not early and operation is JournalValidationSession.advance
            else (16,)
        )
        for rows in quanta:
            budget = JournalValidationBudget(
                rows=rows, source_octets=7 if ordinal % 17 == 0 else 65536
            )
            for cold, summary in variants:
                with session_fixture(case, catalog, cold=cold, summary=summary) as setup:
                    if operation is _checkpoint_progress:
                        dimensions = progress_dimensions(setup, setup._ledger.load())
                    else:
                        dimensions, _ = session_dimensions(setup, budget, operation, cold=cold)
                with session_fixture(
                    case, catalog, cold=cold, summary=summary, measured=True
                ) as current:
                    with session_fixture(
                        case, catalog, cold=cold, summary=summary, measured=True
                    ) as oracle:
                        with pytest.MonkeyPatch.context() as patch:
                            from types import SimpleNamespace

                            from riverhog_provenance import _entry_replay as replay_owner

                            patch.setattr(
                                replay_owner, "uuid4", lambda: SimpleNamespace(hex="f" * 32)
                            )
                            expected = invoke(oracle, operation, budget)
                        expected_rows = snapshot(oracle._store.db)
                        if result_operation:
                            expected_metadata = (
                                expected.journal_id,
                                expected.journal_sha256,
                                expected.journal_bytes,
                            )
                            expected.close()
                    before = current._ledger.load()
                    original = snapshot(current._store.db)
                    if current._source is not None:
                        current._source.source.calls.clear()
                    with pytest.MonkeyPatch.context() as patch:
                        from types import SimpleNamespace

                        from riverhog_provenance import _entry_replay as replay_owner

                        patch.setattr(replay_owner, "uuid4", lambda: SimpleNamespace(hex="f" * 32))
                        with session_observer(current, dimensions, operation) as (
                            observed,
                            complete,
                        ):
                            with current._store.db.observe():
                                if operation is _checkpoint_progress:
                                    # The helper receives the already loaded native parent.
                                    result = operation(
                                        current.attempt_id,
                                        current._ledger,
                                        before,
                                        current._input.cursor.byte_offset,
                                    )
                                else:
                                    result = invoke(current, operation, budget)
                    db = current._store.db
                    work = observed_work(db, {key: 0 for key in db.metrics}, 0, 0)
                    work.update(
                        changed_rows=db.metrics["changed_rows"],
                        indexed_containers=db.all_node_containers,
                        index_python_opcode_events=db.all_node_opcodes,
                    )
                    sample = complete(observed, work)
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
                    assert snapshot(db) == expected_rows
                    if result_operation:
                        try:
                            assert (
                                result.journal_id,
                                result.journal_sha256,
                                result.journal_bytes,
                            ) == expected_metadata
                            current.close()
                            result._store.require_open()
                            assert len(result.frames) == int(case["binding"].frame_count)
                        finally:
                            result.close()
                    else:
                        assert result == expected
                        assert (
                            result.accepted_sha256
                            == hashlib.sha256(case["raw"][: result.accepted_octets]).hexdigest()
                        )
                        if operation in {_checkpoint_progress, JournalValidationSession.progress}:
                            assert snapshot(db) == original
                    for tag, children in sample["delegates"].items():
                        expected_calls = dimensions.get(
                            tag + "_calls", dimensions.get("padding_blocks")
                        )
                        assert len(children) == expected_calls
                    if len(samples) == 1 or len(samples) % 32 == 0:
                        (root / (operation.__name__ + "-growth-progress.json")).write_text(
                            json.dumps(
                                {
                                    "samples": len(samples),
                                    "phase": phase,
                                    "case_index": ordinal,
                                    "cases": len(cases),
                                    "cold": cold,
                                }
                            )
                            + "\n"
                        )
    assert samples


@pytest.fixture(scope="module")
def native_session_checkpoints(tmp_path_factory, catalog, admission_histories, graph_histories):
    root = tmp_path_factory.mktemp("native-session-checkpoints")
    return root, session_cases(root, catalog, admission_histories, graph_histories)


@pytest.mark.growth_witness(
    "riverhog-native-checkpoint-progress-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session._checkpoint_progress",
)
def test_actual_checkpoint_progress_encoding(catalog, native_session_checkpoints, record_property):
    root, cases = native_session_checkpoints
    samples = []
    record_property("growth_samples", samples)
    checkpoint_samples(root, catalog, cases, operation=_checkpoint_progress, samples=samples)


@pytest.mark.growth_witness(
    "riverhog-native-session-advance-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.advance",
)
def test_actual_session_source_and_native_advance(
    catalog, native_session_checkpoints, record_property
):
    root, cases = native_session_checkpoints
    samples = []
    record_property("growth_samples", samples)
    checkpoint_samples(
        root, catalog, cases, operation=JournalValidationSession.advance, samples=samples
    )


@pytest.mark.growth_witness(
    "riverhog-native-session-progress-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.progress",
)
def test_actual_session_progress(catalog, native_session_checkpoints, record_property):
    root, cases = native_session_checkpoints
    samples = []
    record_property("growth_samples", samples)
    checkpoint_samples(
        root, catalog, cases, operation=JournalValidationSession.progress, samples=samples
    )


@pytest.mark.growth_witness(
    "riverhog-native-session-result-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session.JournalValidationSession.result",
)
def test_actual_session_result_borrow(catalog, native_session_checkpoints, record_property):
    root, cases = native_session_checkpoints
    samples = []
    record_property("growth_samples", samples)
    checkpoint_samples(
        root, catalog, cases, operation=JournalValidationSession.result, samples=samples
    )


def rejected_recording_dimensions(session, budget):
    dimensions, before = session_dimensions(
        session, budget, JournalValidationSession.advance, cold=False
    )
    assert before.checkpoint.phase == "recording"
    assert session._failure is None and session._replay._current is not None
    dimensions["progress_calls"] = 0
    dimensions["entry_advance_fence_calls"] = 0
    dimensions["entry_advance_publication_calls"] = 0
    assert dimensions["entry_advance_calls"] == dimensions["entry_advance_recording_calls"] == 1
    return dimensions, before
