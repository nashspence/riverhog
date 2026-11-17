"""Causal witnesses for a disposable index's final commit and exact receiving."""

from __future__ import annotations

import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import pytest
from discovery_experiment import (
    PrivateReadIndex,
    ReceivingArchiveCorpus,
    instrumented_reader,
    observe,
)
from riverhog_core import provenance_archive_read as archive_read
from riverhog_core.canonical_discovery_rebuild import _ArchiveCorpus
from riverhog_core.provenance_archive_read import ProvenanceArchiveReadError
from riverhog_core.provenance_catalog import admission_provenance_catalog
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    create_journal,
    validate_journal_chunks,
)

from tests.support.provenance_archive import archive_reader_fixture


def test_real_reader_removes_only_disposable_index_commit_and_preserves_exact_results() -> None:
    _, objects, journal_id, _ = archive_reader_fixture()
    unchanged = observe(objects, journal_id, private=False)
    proposed = observe(objects, journal_id, private=True)
    assert unchanged["results"] == proposed["results"]
    old = unchanged["measurements"]
    new = proposed["measurements"]
    assert old["explicit_commit_requests"] == old["traced_commit_statements"] == 1
    assert new["explicit_commit_requests"] == new["traced_commit_statements"] == 0
    assert old["inserted_descriptors"] == new["inserted_descriptors"] == 4
    assert old["archive_reads"] == new["archive_reads"]
    assert not unchanged["transaction_open_while_reading"]
    assert proposed["transaction_open_while_reading"]
    for result in (unchanged, proposed):
        assert result["rows_ready_before_reads"] and result["query_only"]
        assert result["metadata_reads_after_prepare"] == result["metadata_reads_after_use"] == 4
        assert result["scratch_removed"]
        assert result["measurements"]["original_close_received"]
    if path := os.environ.get("RIVERHOG_DISCOVERY_REPORT"):
        Path(path).write_text(json.dumps({"unchanged": unchanged, "proposed": proposed}, indent=2))


def test_corrupt_terminal_never_exposes_a_partial_index() -> None:
    _, objects, _, _ = archive_reader_fixture()
    path = sorted(path for path in objects if "/metadata/" in path)[-1]
    objects[path] = b"{}"
    reader, observations = instrumented_reader(objects)
    with patch.object(archive_read, "ProvenanceMetadataIndex", PrivateReadIndex):
        with pytest.raises((ProvenanceArchiveReadError, ValueError)):
            with reader.prepared():
                pytest.fail("incomplete archive became a prepared index")
    assert reader.retained_metadata_storage is None
    assert observations.explicit_commit_requests == 0
    assert observations.original_close_received
    assert all(not Path(path).parent.exists() for path in observations.scratch_paths)


def test_missing_authority_is_not_an_empty_prepared_query() -> None:
    _, objects, _, _ = archive_reader_fixture()
    reader, observations = instrumented_reader(objects)
    with patch.object(archive_read, "ProvenanceMetadataIndex", PrivateReadIndex), reader.prepared():
        index = reader.retained_metadata_storage
        assert index is not None
        assert tuple(index.descriptors(journal_id="urn:uuid:missing")) == ()
        with pytest.raises(ProvenanceArchiveReadError, match="journal is absent"):
            tuple(reader.iter_journal_range("urn:uuid:missing"))
    del objects[sorted(path for path in objects if "/metadata/" in path)[0]]
    with patch.object(archive_read, "ProvenanceMetadataIndex", PrivateReadIndex):
        with pytest.raises(KeyError):
            with reader.prepared():
                pytest.fail("missing archive descriptor became an empty index")
    assert observations.explicit_commit_requests == 0


@pytest.mark.parametrize("close_failure", ["before", "after"])
def test_uncertain_original_close_retains_owner_and_blocks_reprepare(close_failure: str) -> None:
    _, objects, _, _ = archive_reader_fixture()
    reader, observations = instrumented_reader(objects, fail_close=close_failure)
    with patch.object(archive_read, "ProvenanceMetadataIndex", PrivateReadIndex):
        with pytest.raises(OSError):
            with reader.prepared():
                original = reader.retained_metadata_storage
                headers = reader.iter_journal_headers()
                next(headers)
        assert reader.retained_metadata_storage is original
        assert original is not None and original.closing
        assert all(Path(path).parent.is_dir() for path in observations.scratch_paths)
        with pytest.raises(RuntimeError, match="closed"):
            with reader.prepared():
                pytest.fail("replacement bypassed unresolved original close")
        reader.close_prepared()
        headers.close()
    assert reader.retained_metadata_storage is None
    assert observations.original_close_attempts == 2
    assert observations.original_close_received
    assert all(not Path(path).parent.exists() for path in observations.scratch_paths)


def test_private_index_remains_query_only_and_owned_across_serial_worker_handoff() -> None:
    _, objects, journal_id, expected = archive_reader_fixture()
    reader, observations = instrumented_reader(objects)

    def body():
        with reader.prepared():
            owner = reader.retained_metadata_storage
            assert owner is not None
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                owner._db.execute("DELETE FROM volumes")
            yield from reader.iter_journal_range(journal_id)

    with (
        patch.object(archive_read, "ProvenanceMetadataIndex", PrivateReadIndex),
        ThreadPoolExecutor(max_workers=1) as first,
        ThreadPoolExecutor(max_workers=1) as second,
    ):
        stream = body()
        left = first.submit(next, stream).result()
        right = second.submit(next, stream).result()
        done = object()
        assert second.submit(next, stream, done).result() is done
        assert left + right == expected.encode()
    assert observations.explicit_commit_requests == 0
    assert observations.original_close_received
    assert reader.retained_metadata_storage is None


@pytest.mark.parametrize("lost_reply", [False, True])
def test_actual_native_eviction_keeps_original_until_received(lost_reply: bool) -> None:
    catalog = admission_provenance_catalog()
    observation = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"exact source"))
    sources = {}
    for _ in range(3):
        raw = create_journal(
            observation.graph_fragment(),
            recorded_by_agent_id=observation.observer_agent_id,
            catalog=catalog,
        )
        with validate_journal_chunks((raw,), catalog=catalog) as summary:
            sources[summary.journal_id] = raw

    class Reader:
        def iter_journal_range(self, identity):
            yield sources[identity]

    class UncertainClose:
        def __init__(self, original):
            self.original, self.calls = original, 0

        def __getattr__(self, name):
            return getattr(self.original, name)

        def close(self):
            self.calls += 1
            if self.calls == 1:
                if lost_reply:
                    self.original.close()
                raise OSError("native original close is unconfirmed")
            self.original.close()

    keys = list(sources)
    for corpus_type in (_ArchiveCorpus, ReceivingArchiveCorpus):
        corpus = corpus_type(Reader())
        original = corpus[keys[0]]
        corpus[keys[1]]
        storage = original._store._storage
        directory = storage.directory
        observed = UncertainClose(storage.connection)
        storage.connection = observed
        try:
            with pytest.raises(OSError, match="unconfirmed"):
                corpus[keys[2]]
            assert not original.storage_received and directory.exists()
            if corpus_type is _ArchiveCorpus:
                assert original not in corpus.cache.values()
                assert len(corpus.cache) == 2
                # The harness retains the lost original to avoid leaking its real handle.
                original.close()
                corpus.__exit__()
            else:
                assert original in corpus.cache.values() and len(corpus.cache) == 3
                with pytest.raises(RuntimeError, match="incomplete"):
                    corpus[keys[0]]
                corpus.close()
                assert not corpus.cache
            assert original.storage_received and not directory.exists()
            assert observed.calls == 2
        finally:
            original.close()
            corpus.__exit__()
