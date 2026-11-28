"""Small plaintext proofs for independent root-bound preparation and its real caller.

These tests exercise the authenticated-plaintext object boundary. Official age
and complete archive recovery remain covered by test_recovery.py.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import a_riverhog_recovery_tool._provenance_reader as archive_read
import a_riverhog_recovery_tool.recovery as recovery
import pytest
from riverhog_archive_contracts import (
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    PROVENANCE_BINDING_PAGE_MEMBERS_MAX,
    PROVENANCE_BINDINGS_FORMAT,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryDocument,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    ProvenancePayload,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    RecordPage,
    RecordSetCommitment,
    binding_tree_commitment,
    history_record_page_object_path,
    member_history_object_path,
    ordered_provenance_commitment,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance import assertion, create_journal


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _urn(value: int) -> str:
    return f"urn:uuid:00000000-0000-4000-8000-{value:012x}"


@dataclass
class ArchiveCase:
    root: ProvenanceRootDocument
    objects: dict[str, bytes]
    journals: dict[str, bytes]
    descriptors: tuple[ProvenanceVolumeDocument | ProvenanceTerminalDocument, ...]
    bindings: tuple[MemberHistoryBinding, ...]
    structure_paths: tuple[str, ...]


def make_archive_case(
    *,
    journals: int = 2,
    segments: int = 2,
    bindings: int = 1,
    semantic_journals: bool = False,
) -> ArchiveCase:
    """Build bounded structural inputs; optionally use real valid canonical journals."""
    assert journals > 0 and segments > 0 and bindings > 0
    generation, artifact_set = "a" * 64, "b" * 64
    journal_bytes: dict[str, bytes] = {}
    for index in range(journals):
        journal_id = _urn(index + 1)
        if semantic_journals:
            who = _urn(1000 + index)
            raw = create_journal(
                {
                    "agents": [
                        assertion(
                            "agent",
                            who,
                            object_id=who,
                            assertion_id=_urn(2000 + index),
                            kind="software",
                            name="prepared-reader-fixture",
                        )
                    ]
                },
                recorded_by_agent_id=who,
                journal_id=journal_id,
                entry_id=_urn(3000 + index),
                recorded_at="2026-01-01T00:00:00Z",
            )
        else:
            raw = f"journal-{index:04d}|".encode() * max(4, segments)
        journal_bytes[journal_id] = raw

    objects: dict[str, bytes] = {}
    final_bindings = []
    ids = tuple(journal_bytes)
    for index in range(bindings):
        journal_id = ids[index % len(ids)]
        raw = journal_bytes[journal_id]
        anchor = HistoryJournalAnchor(
            journal_id, _urn(3000 + index % len(ids)), 0, "d" * 64, _sha(raw), len(raw)
        )
        selected = MemberHistoryRoot(anchor, "bound")
        roots = RecordSetCommitment(MEMBER_HISTORY_ROOTS_SCHEMA)
        roots.update(selected.key, selected.to_mapping())
        roots_ref = roots.ref()
        imports_ref = RecordSetCommitment(MEMBER_HISTORY_IMPORTS_SCHEMA).ref()
        history = MemberHistoryDocument(
            f"{index + 1:064x}",
            1,
            "c" * 64,
            MemberHistoryPrimary(anchor, _urn(4000 + index)),
            roots_ref,
            imports_ref,
        )
        document = history.to_json_bytes()
        objects[member_history_object_path(history.identity)] = document
        for page in (
            RecordPage(
                roots_ref, 0, ({"key": selected.key, "value": selected.to_mapping()},), False
            ),
            RecordPage(roots_ref, 1, (), True),
            RecordPage(imports_ref, 0, (), True),
        ):
            objects[
                history_record_page_object_path(page.authority.records_sha256, page.ordinal)
            ] = page.to_json_bytes()
        final_bindings.append(
            MemberHistoryBinding(
                history.artifact_id, history.bytes, history.sha256, history.identity, len(document)
            )
        )
    structure_paths = tuple(sorted(objects))
    descriptors: list[ProvenanceVolumeDocument | ProvenanceTerminalDocument] = []

    def add(payload: bytes, kind: str, **extra: Any) -> None:
        sequence = len(descriptors)
        document = ProvenanceVolumeDocument(
            generation,
            artifact_set,
            sequence,
            ProvenancePayload(kind, sequence, len(payload), _sha(payload)),
            **extra,
        )
        descriptors.append(document)
        objects[document.metadata_path] = document.to_json_bytes()
        objects[document.payload.path] = payload

    page_size = PROVENANCE_BINDING_PAGE_MEMBERS_MAX
    for start in range(0, len(final_bindings), page_size):
        page = final_bindings[start : start + page_size]
        add(
            canonical_json_bytes(
                {
                    "format": PROVENANCE_BINDINGS_FORMAT,
                    "bindings": [row.to_mapping() for row in page],
                }
            ),
            "bindings",
            first_artifact_id=page[0].artifact_id,
            last_artifact_id=page[-1].artifact_id,
            binding_count=len(page),
        )
    for journal_id, raw in journal_bytes.items():
        boundaries = [len(raw) * index // segments for index in range(segments + 1)]
        assert all(a < b for a, b in zip(boundaries, boundaries[1:], strict=False))
        for start, stop in zip(boundaries, boundaries[1:], strict=False):
            add(
                raw[start:stop],
                "journal",
                journal_id=journal_id,
                journal_offset=start,
                journal_bytes=len(raw),
                journal_sha256=_sha(raw),
            )
    terminal = ProvenanceTerminalDocument(generation, artifact_set, len(descriptors))
    descriptors.append(terminal)
    objects[terminal.metadata_path] = terminal.to_json_bytes()
    root = ProvenanceRootDocument(
        generation,
        artifact_set,
        _urn(5000),
        binding_count=bindings,
        binding_tree_sha256=binding_tree_commitment(final_bindings).root_sha256,
        journal_count=journals,
        ordered_volume_sha256=ordered_provenance_commitment(descriptors),
    )
    objects["provenance/root.json.age"] = root.to_json_bytes()
    return ArchiveCase(
        root, objects, journal_bytes, tuple(descriptors), tuple(final_bindings), structure_paths
    )


class PlaintextSource:
    """Observe real callback consumption; retain iterators to rule out GC cleanup."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.opens: Counter[str] = Counter()
        self.closed: Counter[str] = Counter()
        self.streams: list[Iterator[bytes]] = []
        self.failure: str | None = None

    def __call__(self, path: str) -> Iterator[bytes]:
        def body() -> Iterator[bytes]:
            self.opens[path] += 1
            try:
                raw = self.objects[path]
                middle = max(1, len(raw) // 2)
                yield raw[:middle]
                if path == self.failure:
                    raise OSError("injected object read failure")
                yield raw[middle:]
            finally:
                self.closed[path] += 1

        stream = body()
        self.streams.append(stream)
        return stream

    iter_plaintext = __call__

    def read_bounded(self, path: str, maximum: int) -> bytes:
        stream = self(path)
        try:
            raw = b"".join(stream)
            if len(raw) > maximum:
                raise ValueError("fixture object exceeds bound")
            return raw
        finally:
            stream.close()


def _reader(case: ArchiveCase, source: PlaintextSource, **changes: str):
    return archive_read.CanonicalProvenanceArchiveReader(
        source,
        expected_root_sha256=changes.get("root", case.root.identity),
        archive_generation=changes.get("generation", case.root.archive_generation),
        artifact_set_sha256=changes.get("artifact_set", case.root.artifact_set_sha256),
    )


def _selection(case: ArchiveCase, source: PlaintextSource):
    return SimpleNamespace(
        encrypted=source,
        manifest=SimpleNamespace(
            provenance=SimpleNamespace(
                identity=case.root.identity, root=SimpleNamespace(path="provenance/root.json.age")
            ),
            archive_generation=case.root.archive_generation,
            artifact_set_sha256=case.root.artifact_set_sha256,
            artifacts=case.root.binding_count,
        ),
    )


@pytest.mark.parametrize(
    "journals,segments,bindings",
    [(1, 1, 1), (4, 1, 1), (2, 7, 1), (2, 1, 17), (1, 1, 513)],
)
def test_complete_prepared_reads_visit_source_descriptors_once(
    tmp_path: Path, journals: int, segments: int, bindings: int
) -> None:
    case = make_archive_case(journals=journals, segments=segments, bindings=bindings)
    source = PlaintextSource(case.objects)
    with _reader(case, source).prepare(workspace=tmp_path) as prepared:
        assert prepared.scan().root == case.root
        assert prepared.root_bytes == case.root.to_json_bytes()
        headers = list(prepared.iter_journal_headers())
        assert headers == [
            (journal_id, len(raw), _sha(raw)) for journal_id, raw in case.journals.items()
        ]
        for journal_id, raw in case.journals.items():
            assert prepared.journal_metadata(journal_id) == (len(raw), _sha(raw))
            assert b"".join(prepared.iter_journal_range(journal_id)) == raw
        assert list(prepared.iter_bindings()) == [row.to_mapping() for row in case.bindings]
        for path in case.structure_paths[:3]:
            assert b"".join(prepared.read_structure(path)) == case.objects[path]
        # Repeat metadata lookups in this attempt; no bounded byte-cache loophole.
        for journal_id in reversed(case.journals):
            prepared.journal_metadata(journal_id)
        assert source.opens["provenance/root.json.age"] == 1
        assert all(source.opens[document.metadata_path] == 1 for document in case.descriptors)
    assert source.closed == source.opens
    assert list(tmp_path.iterdir()) == []


def test_prepared_root_is_an_attempt_snapshot_and_next_attempt_rechecks(tmp_path: Path) -> None:
    case = make_archive_case()
    source = PlaintextSource(case.objects)
    reader = _reader(case, source)
    with reader.prepare(workspace=tmp_path) as prepared:
        case.objects["provenance/root.json.age"] = b"{}"
        assert prepared.root_bytes == case.root.to_json_bytes()
        assert prepared.scan().root == case.root
        assert source.opens["provenance/root.json.age"] == 1
    with pytest.raises(archive_read.ProvenanceArchiveReadError, match="root"):
        with reader.prepare(workspace=tmp_path):
            pytest.fail("a changed root cannot create a new prepared attempt")
    assert source.opens["provenance/root.json.age"] == 2
    assert source.closed == source.opens


@pytest.mark.parametrize("field", ["root", "generation", "artifact_set"])
def test_prepare_rejects_wrong_selected_authority(tmp_path: Path, field: str) -> None:
    case = make_archive_case()
    source = PlaintextSource(case.objects)
    with pytest.raises(archive_read.ProvenanceArchiveReadError):
        with _reader(case, source, **{field: "f" * 64}).prepare(workspace=tmp_path):
            pytest.fail("wrong selected authority became ready")
    assert source.closed == source.opens
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("fault", ["missing-terminal", "wrong-sequence", "truncated-journal"])
def test_incomplete_descriptor_sequence_cannot_become_ready(tmp_path: Path, fault: str) -> None:
    case = make_archive_case()
    if fault == "missing-terminal":
        del case.objects[case.descriptors[-1].metadata_path]
    else:
        document = case.descriptors[-2]
        assert isinstance(document, ProvenanceVolumeDocument)
        changed = (
            replace(
                document,
                sequence=document.sequence + 1,
                payload=replace(document.payload, sequence=document.sequence + 1),
            )
            if fault == "wrong-sequence"
            else replace(document, journal_bytes=document.journal_bytes + 1)
        )
        case.objects[document.metadata_path] = changed.to_json_bytes()
    source = PlaintextSource(case.objects)
    with pytest.raises((archive_read.ProvenanceArchiveReadError, ValueError, KeyError)):
        with _reader(case, source).prepare(workspace=tmp_path):
            pytest.fail("an incomplete sequence became ready")
    assert source.closed == source.opens
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("fault", ["changed", "missing", "interrupted"])
def test_prepared_payload_is_checked_fresh_and_failed_stream_is_received(
    tmp_path: Path, fault: str
) -> None:
    case = make_archive_case(journals=1, segments=2)
    source = PlaintextSource(case.objects)
    document = next(
        row
        for row in case.descriptors
        if isinstance(row, ProvenanceVolumeDocument) and row.payload.kind == "journal"
    )
    original = case.objects[document.payload.path]
    with _reader(case, source).prepare(workspace=tmp_path) as prepared:
        if fault == "changed":
            case.objects[document.payload.path] = original[:-1] + b"!"
        elif fault == "missing":
            del case.objects[document.payload.path]
        else:
            source.failure = document.payload.path
        with pytest.raises((archive_read.ProvenanceArchiveReadError, KeyError, OSError)):
            list(prepared.iter_journal_range(next(iter(case.journals))))
        assert source.closed == source.opens
        case.objects[document.payload.path] = original
        source.failure = None
        assert b"".join(prepared.iter_journal_range(next(iter(case.journals)))) == next(
            iter(case.journals.values())
        )


def test_prepared_structure_is_selected_and_checked_without_descriptor_rescan(
    tmp_path: Path,
) -> None:
    case = make_archive_case(bindings=17)
    source = PlaintextSource(case.objects)
    path = member_history_object_path(case.bindings[0].history_sha256)
    untouched = member_history_object_path(case.bindings[-1].history_sha256)
    with _reader(case, source).prepare(workspace=tmp_path) as prepared:
        assert b"".join(prepared.read_structure(path)) == case.objects[path]
        case.objects[path] = b"{}"
        with pytest.raises((archive_read.ProvenanceArchiveReadError, ValueError)):
            list(prepared.read_structure(path))
        assert source.opens[path] == 2
        assert source.opens[untouched] == 0
        assert all(source.opens[row.metadata_path] == 1 for row in case.descriptors)
    assert source.closed == source.opens


def test_bounded_read_explicitly_closes_retained_oversized_source() -> None:
    closed: list[bool] = []

    def chunks() -> Iterator[bytes]:
        try:
            yield b"too large"
            yield b"never reached"
        finally:
            closed.append(True)

    retained = chunks()
    with pytest.raises(archive_read.ProvenanceArchiveReadError, match="bounded"):
        archive_read._read_bounded(retained, 1)
    assert closed == [True]


def test_partial_index_iterator_cannot_outlive_prepared_owner(tmp_path: Path) -> None:
    case = make_archive_case(journals=4)
    source = PlaintextSource(case.objects)
    with _reader(case, source).prepare(workspace=tmp_path) as prepared:
        headers = prepared.iter_journal_headers()
        assert next(headers)[0] == next(iter(case.journals))
    with pytest.raises((RuntimeError, sqlite3.ProgrammingError)):
        next(headers)
    headers.close()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "journals,segments,bindings", [(1, 1, 1), (4, 1, 1), (1, 3, 1), (1, 1, 17)]
)
def test_actual_stage_provenance_reuses_preparation_and_receives_owned_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    journals: int,
    segments: int,
    bindings: int,
) -> None:
    case = make_archive_case(
        journals=journals, segments=segments, bindings=bindings, semantic_journals=True
    )
    source = PlaintextSource(case.objects)
    index_rows = 0
    journal_queries: Counter[str] = Counter()
    connect = sqlite3.connect

    class ObservedCursor(sqlite3.Cursor):
        def execute(self, statement, parameters=(), /):
            if statement.lstrip().upper().startswith("SELECT"):
                selected = [value for value in parameters if value in case.journals]
                if selected:
                    journal_queries.update(selected)
                    plan = sqlite3.Connection.execute(
                        self.connection, "EXPLAIN QUERY PLAN " + statement, parameters
                    )
                    try:
                        details = [row[3] for row in plan.fetchall()]
                    finally:
                        plan.close()
                    # Matching returned rows alone would miss SQL filtering a
                    # whole-index scan. Require a seek on the requested journal,
                    # without binding this regression to an index name.
                    assert any("SEARCH " in row and "journal=?" in row for row in details)
                    assert not any("SCAN " in row for row in details)
            return super().execute(statement, parameters)

        def fetchone(self):
            nonlocal index_rows
            row = super().fetchone()
            index_rows += row is not None
            return row

        def fetchmany(self, size=None):
            nonlocal index_rows
            rows = super().fetchmany() if size is None else super().fetchmany(size)
            index_rows += len(rows)
            return rows

        def fetchall(self):
            nonlocal index_rows
            rows = super().fetchall()
            index_rows += len(rows)
            return rows

        def __next__(self):
            nonlocal index_rows
            row = super().__next__()
            index_rows += 1
            return row

    class ObservedConnection(sqlite3.Connection):
        def cursor(self, *args, **kwargs):
            kwargs.setdefault("factory", ObservedCursor)
            return super().cursor(*args, **kwargs)

        def execute(self, statement, parameters=(), /):
            return self.cursor().execute(statement, parameters)

    def observed_connect(database: Any, *args: Any, **kwargs: Any):
        if Path(str(database)).name == "metadata.sqlite3":
            kwargs["factory"] = ObservedConnection
        return connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", observed_connect)
    staging, scratch = tmp_path / "output", tmp_path / "scratch"
    staging.mkdir()
    scratch.mkdir()
    with recovery._stage_provenance(_selection(case, source), staging=staging, scratch=scratch) as (
        prepared,
        state,
        count,
    ):
        assert count == len(case.journals)
        rows = state.execute("SELECT journal_id, bytes, sha256, path FROM journals").fetchall()
        assert len(rows) == count
        for journal_id, byte_count, digest, path in rows:
            raw = case.journals[journal_id]
            assert (byte_count, digest, Path(path).read_bytes()) == (len(raw), _sha(raw), raw)
        assert (staging / "structure" / "provenance-root.json").read_bytes() == prepared.root_bytes
        assert list(prepared.iter_bindings()) == [row.to_mapping() for row in case.bindings]
        selected = case.structure_paths[0]
        assert b"".join(prepared.read_structure(selected)) == case.objects[selected]
        assert all(source.opens[path] == 0 for path in case.structure_paths[1:])
        assert source.opens["provenance/root.json.age"] == 1
        assert all(source.opens[row.metadata_path] == 1 for row in case.descriptors)
        # Header enumeration and journal staging together visit journal rows at
        # most twice; complete binding verification/exposure has the same bound.
        # Neither another journal nor a selected structure may multiply that work.
        assert 0 < index_rows <= 2 * (len(case.descriptors) - 1)
        assert journal_queries == Counter({journal_id: 1 for journal_id in case.journals})
    with pytest.raises(sqlite3.ProgrammingError):
        state.execute("SELECT 1")
    assert source.closed == source.opens


def test_actual_stage_failure_closes_unreturned_journal_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = make_archive_case(journals=1, semantic_journals=True)
    source = PlaintextSource(case.objects)
    source.failure = next(
        row.payload.path
        for row in case.descriptors
        if isinstance(row, ProvenanceVolumeDocument) and row.payload.kind == "journal"
    )
    staging, scratch = tmp_path / "output", tmp_path / "scratch"
    staging.mkdir()
    scratch.mkdir()
    connections: list[sqlite3.Connection] = []
    connect = sqlite3.connect

    def tracked_connect(database: Any, *args: Any, **kwargs: Any):
        connection = connect(database, *args, **kwargs)
        if Path(str(database)).name == "journals.sqlite3":
            connections.append(connection)
        return connection

    monkeypatch.setattr(recovery.sqlite3, "connect", tracked_connect)
    with pytest.raises(OSError, match="injected object read failure"):
        with recovery._stage_provenance(_selection(case, source), staging=staging, scratch=scratch):
            pytest.fail("partial journal was accepted")
    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError):
        connections[0].execute("SELECT 1")
    assert source.closed == source.opens


def test_prepared_ranges_preserve_exact_bounds(tmp_path: Path) -> None:
    case = make_archive_case(journals=1, segments=7)
    source = PlaintextSource(case.objects)
    journal_id, raw = next(iter(case.journals.items()))
    with _reader(case, source).prepare(workspace=tmp_path) as prepared:
        assert b"".join(prepared.iter_journal_range(journal_id, offset=3, size=17)) == raw[3:20]
        assert b"".join(prepared.iter_journal_range(journal_id, offset=len(raw), size=0)) == b""
        for arguments in ({"offset": -1}, {"size": -1}, {"offset": len(raw) + 1}):
            with pytest.raises(archive_read.ProvenanceArchiveReadError):
                list(prepared.iter_journal_range(journal_id, **arguments))


def test_binding_tree_is_verified_before_any_binding_is_exposed(tmp_path: Path) -> None:
    case = make_archive_case(bindings=17)
    case.root = replace(case.root, binding_tree_sha256="f" * 64)
    case.objects["provenance/root.json.age"] = case.root.to_json_bytes()
    source = PlaintextSource(case.objects)
    with _reader(case, source).prepare(workspace=tmp_path) as prepared:
        stream = prepared.iter_bindings()
        with pytest.raises(archive_read.ProvenanceArchiveReadError, match="tree"):
            next(stream)
    assert source.closed == source.opens


@pytest.mark.parametrize("target", ["cursor", "connection"])
@pytest.mark.parametrize("after_effect", [False, True])
def test_uncertain_close_retains_original_storage_until_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str, after_effect: bool
) -> None:
    case = make_archive_case(journals=2)
    source = PlaintextSource(case.objects)
    cursors = []
    connections = []
    connect = sqlite3.connect

    class TrackedCursor(sqlite3.Cursor):
        failures = 0
        close_calls = 0

        def close(self):
            self.close_calls += 1
            if self.failures:
                self.failures -= 1
                if after_effect:
                    super().close()
                raise OSError("cursor close unconfirmed")
            return super().close()

    class TrackedConnection(sqlite3.Connection):
        failures = 0
        close_calls = 0

        def execute(self, statement, parameters=(), /):
            cursor = self.cursor(factory=TrackedCursor)
            if statement.startswith("SELECT kind, body FROM volumes"):
                cursors.append(cursor)
            return cursor.execute(statement, parameters)

        def executescript(self, statement, /):
            return self.cursor(factory=TrackedCursor).executescript(statement)

        def close(self):
            self.close_calls += 1
            if self.failures:
                self.failures -= 1
                if after_effect:
                    super().close()
                raise OSError("connection close unconfirmed")
            return super().close()

    def tracked_connect(database, *args, **kwargs):
        connection = connect(database, *args, **kwargs, factory=TrackedConnection)
        connections.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    prepared = _reader(case, source).prepare(workspace=tmp_path)
    with pytest.raises(OSError, match=f"{target} close unconfirmed"):
        with prepared:
            headers = prepared.iter_journal_headers()
            next(headers)
            assert len(connections) == len(cursors) == 1
            if target == "cursor":
                cursors[0].failures = 1
            else:
                connections[0].failures = 1
    assert list(tmp_path.iterdir()), "unconfirmed receiving must retain original scratch"
    assert connections[0].close_calls == (0 if target == "cursor" else 1)
    with pytest.raises(RuntimeError):
        prepared.scan()
    prepared.close()
    assert list(tmp_path.iterdir()) == []
    assert connections[0].close_calls == (1 if target == "cursor" else 2)
    headers.close()


def test_two_or_more_buffered_bindings_cannot_escape_a_closed_owner(tmp_path: Path) -> None:
    case = make_archive_case(bindings=3)
    source = PlaintextSource(case.objects)
    with _reader(case, source).prepare(workspace=tmp_path) as prepared:
        stream = prepared.iter_bindings()
        assert next(stream) == case.bindings[0].to_mapping()
        prepared.close()
        with pytest.raises(RuntimeError, match="not open|closed"):
            next(stream)
        stream.close()
    assert source.closed == source.opens
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("operation", ["journal", "structure"])
def test_source_callback_close_prevents_buffered_payload_or_structure_yield(
    tmp_path: Path, operation: str
) -> None:
    case = make_archive_case(journals=1)
    source = PlaintextSource(case.objects)
    if operation == "journal":
        target = next(
            row.payload.path
            for row in case.descriptors
            if isinstance(row, ProvenanceVolumeDocument) and row.payload.kind == "journal"
        )
    else:
        target = member_history_object_path(case.bindings[0].history_sha256)
    owner = []
    triggered = []

    def read(path: str) -> Iterator[bytes]:
        stream = source(path)
        try:
            for chunk in stream:
                if path == target and not triggered:
                    triggered.append(path)
                    owner[0].close()
                yield chunk
        finally:
            stream.close()

    reader = archive_read.CanonicalProvenanceArchiveReader(
        read,
        expected_root_sha256=case.root.identity,
        archive_generation=case.root.archive_generation,
        artifact_set_sha256=case.root.artifact_set_sha256,
    )
    with reader.prepare(workspace=tmp_path) as prepared:
        owner.append(prepared)
        stream = (
            prepared.iter_journal_range(next(iter(case.journals)))
            if operation == "journal"
            else prepared.read_structure(target)
        )
        with pytest.raises(RuntimeError, match="not open|closed"):
            next(stream)
        stream.close()
    assert triggered == [target]
    assert source.closed == source.opens
    assert list(tmp_path.iterdir()) == []


def test_schema_failure_retains_the_original_unconfirmed_connection_for_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = make_archive_case()
    source = PlaintextSource(case.objects)
    connections = []
    connect = sqlite3.connect

    class SetupFailureConnection(sqlite3.Connection):
        close_calls = 0

        def executescript(self, statement, /):
            cursor = super().executescript(statement)
            cursor.close()
            raise ValueError("injected schema initialization failure")

        def close(self):
            self.close_calls += 1
            if self.close_calls == 1:
                raise OSError("original connection close unconfirmed")
            return super().close()

    def tracked_connect(database, *args, **kwargs):
        connection = connect(database, *args, **kwargs, factory=SetupFailureConnection)
        connections.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    prepared = _reader(case, source).prepare(workspace=tmp_path)
    with pytest.raises(OSError, match="original connection close unconfirmed") as failure:
        with prepared:
            pytest.fail("failed schema setup cannot publish a prepared owner")
    assert isinstance(failure.value.__context__, ValueError)
    assert str(failure.value.__context__) == "injected schema initialization failure"
    assert len(connections) == 1 and connections[0].close_calls == 1
    assert list(tmp_path.iterdir()), "unconfirmed constructor storage must remain owned"
    assert not source.opens
    prepared.close()
    assert connections[0].close_calls == 2
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(sqlite3.ProgrammingError):
        connections[0].execute("SELECT 1")


def test_prepared_index_schema_is_published_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = make_archive_case()
    source = PlaintextSource(case.objects)
    connect = sqlite3.connect
    visible = []
    connections = []

    def tracked(database, *args, **kwargs):
        db = connect(database, *args, **kwargs)
        connections.append(db)

        def observed(statement):
            if statement.lstrip().startswith("CREATE "):
                reader = connect(database)
                try:
                    visible.append(reader.execute("SELECT name FROM sqlite_master").fetchall())
                finally:
                    reader.close()

        db.set_trace_callback(observed)
        return db

    monkeypatch.setattr(sqlite3, "connect", tracked)
    with _reader(case, source).prepare(workspace=tmp_path) as prepared:
        assert visible and all(not schema for schema in visible)
        assert len(connections) == 1 and not connections[0].in_transaction
        assert prepared.scan().root == case.root
    assert list(tmp_path.iterdir()) == []
