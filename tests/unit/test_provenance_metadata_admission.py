from __future__ import annotations

import errno
import shutil
import sqlite3
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import riverhog_core.provenance_metadata_index as owner_module
import riverhog_core.scratch_workspace as scratch
from riverhog_archive_contracts import BOUND_HISTORY_EXTENT, MemberHistoryBinding
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader
from riverhog_core.provenance_metadata_index import ProvenanceMetadataIndex
from riverhog_protocol.errors import ServiceUnavailable

from tests.support.provenance_archive import archive_reader_fixture
from tests.support.sqlite_admission import Capacity, Population

_CONNECT = sqlite3.connect


class Probe:
    def __init__(self):
        self.active = self.peak = self.cursor_active = self.cursor_peak = 0
        self.connections, self.cursors, self.owners, self.calls, self.sql = [], [], [], [], []
        self.statement_failure = None
        self.cursor_failure = None
        self.commit_failure = None
        self.fetch_failure = False
        self.on_fetch = self.on_commit = None
        self.refuse = self.unreturned = False

    def retain(self, owner):
        assert owner._db is self.connections[-1]
        self.owners.append(owner)

    def __call__(self, path, **options):
        self.calls.append((Path(path), options))
        if self.refuse:
            raise Capacity("connection admission refused")
        probe = self

        class Cursor(sqlite3.Cursor):
            released = False
            statement = ""
            close_calls = 0

            def execute(self, statement, parameters=()):
                self.statement = statement
                probe.sql.append(statement)
                if probe.statement_failure and statement.lstrip().startswith(
                    probe.statement_failure
                ):
                    raise InterruptedError("metadata statement interrupted")
                return super().execute(statement, parameters)

            def executescript(self, statement):
                self.statement = statement
                probe.sql.append(statement)
                if probe.statement_failure and statement.lstrip().startswith(
                    probe.statement_failure
                ):
                    raise InterruptedError("metadata schema interrupted")
                return super().executescript(statement)

            def fetchone(self):
                if probe.fetch_failure:
                    raise InterruptedError("metadata fetch interrupted")
                row = super().fetchone()
                if probe.on_fetch:
                    probe.on_fetch()
                return row

            def close(self):
                self.close_calls += 1
                failure = probe.cursor_failure
                if failure and self.statement.startswith(failure[0]):
                    probe.cursor_failure = None
                    if failure[1]:
                        super().close()
                    raise OSError("metadata cursor close unconfirmed")
                super().close()
                if not self.released:
                    self.released = True
                    probe.cursor_active -= 1

        class Connection(sqlite3.Connection):
            released = False
            close_failure = None
            close_calls = 0

            def _new_cursor(self):
                assert any(owner._db is self for owner in probe.owners), "SQL preceded retention"
                cursor = self.cursor(factory=Cursor)
                probe.cursors.append(cursor)
                probe.cursor_active += 1
                probe.cursor_peak = max(probe.cursor_peak, probe.cursor_active)
                return cursor

            def execute(self, statement, parameters=()):
                cursor = self._new_cursor()
                try:
                    return cursor.execute(statement, parameters)
                except BaseException:
                    sqlite3.Cursor.close(cursor)
                    cursor.released = True
                    probe.cursor_active -= 1
                    raise

            def executescript(self, statement):
                cursor = self._new_cursor()
                try:
                    return cursor.executescript(statement)
                except BaseException:
                    sqlite3.Cursor.close(cursor)
                    cursor.released = True
                    probe.cursor_active -= 1
                    raise

            def commit(self):
                failure = probe.commit_failure
                if failure is not None:
                    probe.commit_failure = None
                    if failure:
                        super().commit()
                    raise InterruptedError("metadata commit acknowledgement interrupted")
                super().commit()
                if probe.on_commit:
                    probe.on_commit()

            def close(self):
                self.close_calls += 1
                failure = self.close_failure
                if failure is not None:
                    self.close_failure = None
                    if failure:
                        super().close()
                    raise OSError("metadata connection close unconfirmed")
                super().close()
                if not self.released:
                    self.released = True
                    probe.active -= 1

        db = _CONNECT(path, factory=Connection, **options)
        self.connections.append(db)
        self.active += 1
        self.peak = max(self.peak, self.active)
        if self.unreturned:
            raise OSError("metadata connection construction unconfirmed")
        return db


def reader(probe, *, count=2, member_count=1, retain=None, **ports):
    original, objects, journal_id, text = archive_reader_fixture(
        journal_bytes=b"x" * count, segment_ends=tuple(range(1, count))
    )
    if member_count != 1:
        binding = MemberHistoryBinding.from_mapping(next(original.iter_bindings()))
        history = original.history_descriptor(binding)
        histories = tuple(
            replace(history, artifact_id=f"{ordinal + 1:064x}") for ordinal in range(member_count)
        )
        original, objects, journal_id, text = archive_reader_fixture(
            journal_bytes=b"x" * count,
            segment_ends=tuple(range(1, count)),
            histories=histories,
            history_objects={
                path: raw
                for path, raw in objects.items()
                if path.startswith(("provenance/history/", "provenance/sets/"))
            },
        )
    root = original.scan().root
    reads = []

    def read_object(path):
        reads.append(path)
        yield objects[path]

    selected = CanonicalProvenanceArchiveReader(
        read_object,
        expected_root_sha256=root.identity,
        archive_generation=root.archive_generation,
        artifact_set_sha256=root.artifact_set_sha256,
        metadata_connection_factory=probe,
        on_metadata_storage_open=probe.retain if retain is None else retain,
        **ports,
    )
    return selected, objects, journal_id, text, reads


@pytest.mark.parametrize("count", [1, 129, 385])
def test_metadata_admission_precedes_scan_and_writes_each_descriptor_once(count):
    probe = Probe()
    selected, _, journal_id, text, reads = reader(probe, count=count)
    original_retain = probe.retain

    def retain(index):
        assert not reads and not probe.sql
        assert selected.retained_metadata_storage is index
        original_retain(index)

    selected._on_metadata_storage_open = retain
    with selected.prepared():
        (index,) = probe.owners
        assert probe.calls[0][1] == {"check_same_thread": False}
        assert selected.scan().volume_count == count + 1
        before = list(reads)
        assert list(selected.iter_journal_ids()) == [journal_id]
        assert b"".join(selected.iter_journal_range(journal_id)) == text.encode()
        assert [row["artifact_id"] for row in selected.iter_bindings()] == ["c" * 64]
        assert sum(s.startswith("INSERT INTO volumes") for s in probe.sql) == count + 2
        assert all("provenance/metadata/" not in p for p in reads[len(before) :])
        cursor = probe.connections[0].execute("PRAGMA query_only")
        assert cursor.fetchone() == (1,)
        cursor.close()
        assert probe.cursor_active == 0
    assert index.cleanup_complete and not index._root.exists()
    assert selected.retained_metadata_storage is None
    assert probe.active == 0 and probe.peak == 1
    assert all(c.close_calls == 1 for c in probe.connections)


@pytest.mark.parametrize("unreturned", [False, True])
def test_metadata_refusal_precedes_archive_reads_and_retains_factory_files(unreturned):
    probe = Probe()
    selected, _, _, _, reads = reader(probe)
    probe.refuse, probe.unreturned = not unreturned, unreturned
    with pytest.raises(OSError if unreturned else Capacity):
        with selected.prepared():
            pytest.fail("refused index became ready")
    assert not reads and not probe.sql and not probe.owners
    assert selected.retained_metadata_storage is None
    path = probe.calls[0][0]
    assert path.parent.exists() and probe.active == int(unreturned)
    if unreturned:
        probe.connections[0].close()
    shutil.rmtree(path.parent)


@pytest.mark.parametrize("stage", ["retention", "schema", "scan"])
@pytest.mark.parametrize("after", [None, False, True])
def test_failed_metadata_preparation_retains_actual_owner_and_blocks_fallback(stage, after):
    probe = Probe()
    selected, objects, _, _, _ = reader(probe)

    def retain(index):
        probe.retain(index)
        probe.connections[0].close_failure = after
        if stage == "retention":
            raise InterruptedError("metadata retention interrupted")
        if stage == "schema":
            probe.statement_failure = "PRAGMA cache_size"
        else:
            objects["provenance/root.json.age"] = b"{}"

    selected._on_metadata_storage_open = retain
    with pytest.raises((InterruptedError, OSError, ValueError)):
        with selected.prepared():
            pytest.fail("failed preparation became ready")
    (index,) = probe.owners
    assert index.closing and probe.connections[0].close_calls == 1
    assert index.cleanup_complete == (after is None)
    if after is not None:
        assert selected.retained_metadata_storage is index and index._root.exists()
        for work in (selected.scan, lambda: list(selected.iter_journal_ids())):
            with pytest.raises(RuntimeError, match="closed"):
                work()
        with pytest.raises(RuntimeError, match="closed"):
            with selected.prepared():
                pytest.fail("unknown cleanup was reused")
        assert len(probe.calls) == 1
        selected.close_prepared()
    assert selected.retained_metadata_storage is None and probe.active == 0
    assert not index._root.exists()


@pytest.mark.parametrize("after", [False, True])
def test_metadata_context_does_not_retry_explicit_uncertain_close(after):
    probe = Probe()
    selected, _, _, _, _ = reader(probe)
    with pytest.raises(OSError):
        with selected.prepared():
            index = selected.retained_metadata_storage
            probe.connections[0].close_failure = after
            index.close()
    assert probe.connections[0].close_calls == 1 and probe.active == 1
    assert selected.retained_metadata_storage is index
    selected.close_prepared()
    selected.close_prepared()
    assert probe.connections[0].close_calls == 2 and probe.active == 0


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("stage", ["schema", "insert", "read"])
def test_metadata_cursor_cleanup_remains_before_connection_and_directory(after, stage):
    probe = Probe()
    selected, _, _, _, _ = reader(probe)
    prefix = {
        "schema": "PRAGMA cache_size",
        "insert": "INSERT INTO volumes",
        "read": "SELECT kind, body",
    }[stage]
    probe.cursor_failure = (prefix, after)
    with pytest.raises(OSError):
        with selected.prepared():
            list(selected.iter_journal_ids())
    (index,) = probe.owners
    assert index.closing and selected.retained_metadata_storage is index
    assert probe.connections[0].close_calls == 0 and probe.active == probe.cursor_active == 1
    assert index._root.is_dir()
    cursor = next(c for c in probe.cursors if not c.released)
    assert cursor.close_calls == 1
    selected.close_prepared()
    assert cursor.close_calls == 2 and probe.active == probe.cursor_active == 0
    assert not index._root.exists()


@pytest.mark.parametrize("after", [False, True])
def test_metadata_directory_retry_does_not_reclose_connection(monkeypatch, after):
    probe = Probe()
    selected, _, _, _, _ = reader(probe)
    remove = ProvenanceMetadataIndex._remove_directory
    failures = []

    def interrupted(index):
        if not failures:
            failures.append(index)
            if after:
                remove(index)
            raise OSError("metadata directory removal unconfirmed")
        remove(index)

    monkeypatch.setattr(ProvenanceMetadataIndex, "_remove_directory", interrupted)
    with pytest.raises(OSError):
        with selected.prepared():
            pass
    (index,) = failures
    assert selected.retained_metadata_storage is index and probe.active == 0
    assert not index.cleanup_complete and probe.connections[0].close_calls == 1
    selected.close_prepared()
    assert index.cleanup_complete and probe.connections[0].close_calls == 1


def test_metadata_closed_during_fetch_cannot_emit_or_complete():
    probe = Probe()
    selected, _, _, _, _ = reader(probe)
    with pytest.raises(RuntimeError, match="closed"):
        with selected.prepared():
            index = selected.retained_metadata_storage
            probe.on_fetch = index.close
            list(selected.iter_journal_ids())
    assert probe.active == 0 and index.cleanup_complete
    assert all(c.close_calls == 1 for c in probe.cursors)


@pytest.mark.parametrize("after", [False, True])
def test_metadata_commit_interruption_does_not_handoff_prepared_reader(after):
    probe = Probe()
    selected, _, _, _, _ = reader(probe)
    probe.commit_failure = after
    with pytest.raises(InterruptedError):
        with selected.prepared():
            pytest.fail("unconfirmed commit became ready")
    assert probe.active == probe.cursor_active == 0
    assert selected.retained_metadata_storage is None
    with selected.prepared():
        assert selected.scan().volume_count == 3
    assert probe.active == 0


def test_nested_prepared_scope_shares_owner_and_abandoned_stream_closes_cursor():
    probe = Probe()
    selected, _, _, _, _ = reader(probe)
    with selected.prepared():
        index = selected.retained_metadata_storage
        with selected.prepared():
            assert selected.retained_metadata_storage is index
            stream = selected._descriptors()
            next(stream)
            assert probe.cursor_active == 1
            stream.close()
            assert probe.cursor_active == 0 and probe.active == 1
        assert probe.active == 1
    assert len(probe.connections) == 1 and probe.active == 0


def test_metadata_suspended_stream_cannot_resume_after_parent_cleanup():
    probe = Probe()
    selected, _, _, _, _ = reader(probe)
    with selected.prepared():
        stream = selected._descriptors()
        next(stream)
    assert probe.active == probe.cursor_active == 0
    with pytest.raises(RuntimeError, match="closed"):
        next(stream)
    assert all(c.close_calls == 1 for c in probe.cursors)


def test_metadata_early_callback_cannot_return_closed_parent():
    probe = Probe()
    selected, _, _, _, reads = reader(probe)

    def retain(index):
        probe.retain(index)
        selected.close_prepared()

    selected._on_metadata_storage_open = retain
    with pytest.raises(RuntimeError, match="closed"):
        with selected.prepared():
            pytest.fail("closed admission became ready")
    assert not probe.sql and not reads and probe.active == 0
    assert selected.retained_metadata_storage is None


def test_metadata_default_connect_failure_cleans_only_owned_scratch(monkeypatch):
    original = owner_module.sqlite3.connect
    paths = []

    def refuse(path, **_options):
        paths.append(path)
        raise OSError("known metadata open failure")

    monkeypatch.setattr(owner_module.sqlite3, "connect", refuse)
    with pytest.raises(OSError):
        ProvenanceMetadataIndex()
    assert len(paths) == 1 and not paths[0].parent.exists()
    monkeypatch.setattr(owner_module.sqlite3, "connect", original)


@pytest.mark.parametrize("error_number", [errno.ENOSPC, errno.EDQUOT])
def test_metadata_creation_capacity_failure_is_retryable(monkeypatch, error_number):
    def exhausted(**_options):
        raise OSError(error_number, "private path")

    monkeypatch.setattr(scratch.tempfile, "mkdtemp", exhausted)
    with pytest.raises(ServiceUnavailable, match="temporary workspace capacity exhausted"):
        ProvenanceMetadataIndex()


def test_owned_scratch_capacity_refuses_before_connection(monkeypatch):
    probe = Probe()
    monkeypatch.setattr(scratch.shutil, "disk_usage", lambda _root: SimpleNamespace(free=0))
    with pytest.raises(ServiceUnavailable):
        ProvenanceMetadataIndex(connection_factory=probe)
    assert not probe.calls


def test_cached_reader_preserves_metadata_and_history_verification_factories():
    probe, heads = Probe(), Population(limit=1)
    handles = []
    selected, _, journal_id, _, _ = reader(
        probe,
        history_verification_connection_factory=heads,
        on_history_verification_storage_open=handles.append,
    )
    with selected.cached() as cached:
        with cached.prepared():
            assert list(cached.iter_journal_ids()) == [journal_id]
            binding = MemberHistoryBinding.from_mapping(next(cached.iter_bindings()))
            assert cached.member_history(binding).identity == binding.history_sha256
            assert tuple(cached.history_store().roots(binding, extent=BOUND_HISTORY_EXTENT))
    assert len(probe.connections) == 1 and probe.active == 0
    assert len(handles) == len(heads.connections) == 2 and heads.active == 0
    assert all(owner.cleanup_complete for owner in handles)


def test_multiple_metadata_streams_keep_independent_cursor_custody():
    probe = Probe()
    selected, _, _, _, _ = reader(probe)
    with selected.prepared():
        first, second = selected._descriptors(), selected._descriptors()
        assert next(first) == next(second)
        assert probe.cursor_active == 2
        first.close()
        assert probe.cursor_active == 1
    assert probe.cursor_active == probe.active == 0
    with pytest.raises(RuntimeError, match="closed"):
        next(second)
    assert all(cursor.close_calls == 1 for cursor in probe.cursors)


def test_interrupted_metadata_fetch_releases_known_resources_before_retry():
    probe = Probe()
    selected, _, _, _, _ = reader(probe)
    with pytest.raises(InterruptedError):
        with selected.prepared():
            probe.fetch_failure = True
            list(selected.iter_journal_ids())
    assert selected.retained_metadata_storage is None
    assert probe.active == probe.cursor_active == 0
    probe.fetch_failure = False
    with selected.prepared():
        assert selected.scan().volume_count == 3
    assert probe.active == 0


def test_real_metadata_sqlite_full_is_retryable_capacity_failure():
    probe = Probe()
    selected, _, _, _, reads = reader(probe)

    def retain(index):
        probe.retain(index)
        cursor = probe.connections[-1].execute("PRAGMA max_page_count=2")
        cursor.close()

    selected._on_metadata_storage_open = retain
    with pytest.raises(ServiceUnavailable, match="temporary workspace capacity exhausted"):
        with selected.prepared():
            pytest.fail("full metadata database became ready")
    assert not reads and probe.active == probe.cursor_active == 0
    assert selected.retained_metadata_storage is None
    assert not probe.owners[0]._root.exists()


def test_published_service_composes_all_history_roles_across_cold_and_warm_scope(tmp_path):
    from riverhog_core.catalog_db import session_scope
    from riverhog_core.catalog_workflow_models import CollectionProcessingCapabilityRecord
    from riverhog_core.services.canonical_provenance import SqlAlchemyCanonicalProvenanceService
    from riverhog_protocol.errors import NotFound

    from tests.unit.test_published_provenance_access import _environment, _scope

    metadata, histories, journals, heads, memberships = (
        Probe(),
        Population(),
        Population(),
        Population(),
        Population(),
    )
    history_handles, journal_handles, head_handles, membership_handles = [], [], [], []
    _default, archive, config, registry, *_rest = _environment(tmp_path)
    service = SqlAlchemyCanonicalProvenanceService(
        config,
        registry,
        metadata_connection_factory=metadata,
        on_metadata_storage_open=metadata.retain,
        history_connection_factory=histories,
        on_history_storage_open=history_handles.append,
        provenance_connection_factory=journals,
        on_provenance_storage_open=journal_handles.append,
        history_verification_connection_factory=heads,
        on_history_verification_storage_open=head_handles.append,
        membership_connection_factory=memberships,
        on_membership_storage_open=membership_handles.append,
    )
    principal = _scope(config, archive)
    first = service.list_journals(1, page_size=1, after_journal_id=None, principal=principal)
    assert first["journals"]
    assert metadata.owners and history_handles and journal_handles and head_handles
    assert not membership_handles
    before = (len(histories.attempts), len(journals.attempts), len(heads.attempts))
    again = service.list_journals(1, page_size=1, after_journal_id=None, principal=principal)
    assert again == first
    assert len(metadata.connections) == 2 and len(membership_handles) == 1
    assert (len(histories.attempts), len(journals.attempts), len(heads.attempts)) == before
    assert all(owner.cleanup_complete for owner in metadata.owners)
    assert all(owner.cleanup_complete for owner in head_handles)
    assert all(pop.active == 0 for pop in (metadata, histories, journals, heads, memberships))
    attempts = (len(metadata.calls), len(memberships.attempts))
    with session_scope(service._session_factory) as session:
        session.get(
            CollectionProcessingCapabilityRecord, principal.artifact_scope_capability_id
        ).state = "revoked"
    with pytest.raises(NotFound):
        service.list_journals(1, page_size=1, after_journal_id=None, principal=principal)
    assert (len(metadata.calls), len(memberships.attempts)) == attempts


def test_metadata_close_during_payload_read_prevents_handoff_before_scope_exit():
    probe = Probe()
    selected, _, journal_id, _, _ = reader(probe)
    read = selected._read_object
    enabled = False
    delivered = []

    def closing_source(path):
        for chunk in read(path):
            if enabled and path.startswith("provenance/payloads/"):
                selected.close_prepared()
            yield chunk

    selected._read_object = closing_source
    with pytest.raises(RuntimeError, match="closed"):
        with selected.prepared():
            enabled = True
            stream = selected.iter_journal_range(journal_id)
            try:
                delivered.append(next(stream))
            finally:
                stream.close()
    assert not delivered and probe.active == probe.cursor_active == 0


@pytest.mark.parametrize("replacement", [False, True])
def test_suspended_binding_page_keeps_original_owner_after_close_and_replacement(replacement):
    probe = Probe()
    selected, _, _, _, _ = reader(probe, member_count=2)
    delivered = []
    with selected.prepared():
        stream = selected.iter_bindings()
        delivered.append(next(stream))
    if replacement:
        with selected.prepared():
            with pytest.raises(RuntimeError, match="closed"):
                delivered.append(next(stream))
            assert selected.scan().root.binding_count == 2
    else:
        with pytest.raises(RuntimeError, match="closed"):
            delivered.append(next(stream))
    assert len(delivered) == 1
    assert probe.active == probe.cursor_active == 0
