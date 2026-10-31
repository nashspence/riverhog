from __future__ import annotations

from contextlib import nullcontext
from dataclasses import replace

import pytest
from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_core.canonical_discovery_archive_work import (
    StaleArchiveTask,
    archive_journal_header,
    archive_journal_segment_at,
)
from riverhog_core.provenance_archive_read import ProvenanceArchiveReadError
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import validate_journal_chunks

from tests.unit.test_canonical_discovery_corpus_source import case as case
from tests.unit.test_canonical_discovery_corpus_source import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_source import original as original
from tests.unit.test_canonical_discovery_corpus_source import qualified as qualified
from tests.unit.test_canonical_discovery_corpus_source import seed
from tests.unit.test_discovery_validation_owner import unit


def source_for(work, qualified):
    selection, anchors = seed(work.metadata, qualified)
    return ArchiveJournalSource(work.metadata, selection.history.reader, anchors[-1].journal_id)


def forbid_scan(monkeypatch, source):
    def forbidden(*_args, **_kwargs):
        pytest.fail("an exact archive range rescanned metadata or entered the general range reader")

    for name in ("scan", "_descriptors", "iter_journal_range", "journal_metadata"):
        monkeypatch.setattr(source.reader, name, forbidden)


def test_original_decimal_offset_index_seeks_boundaries_and_eof(case, qualified, original):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            source = source_for(work, qualified)
            with work.metadata.read() as (db, key):
                header = archive_journal_header(db, key, source.identity.journal_id)
                assert header.source_bytes == len(original[0])
                for offset in (0, 1, 9, 10, 11, 12, 13, 99, 100, header.source_bytes - 1):
                    selected = archive_journal_segment_at(db, key, header.journal_id, offset)
                    assert selected.journal_offset == (0 if offset < 12 else 12)
                assert (
                    archive_journal_segment_at(db, key, header.journal_id, header.source_bytes)
                    is None
                )
    finally:
        app.close()


def test_authentic_short_windows_and_exact_reads_feed_native_without_rescanning(
    case, qualified, original, monkeypatch
):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            source = source_for(work, qualified)
            forbid_scan(monkeypatch, source)
            assert source.read_window(0, source.identity.source_bytes) == original[0][:12]
            assert source.read_exact(0, source.identity.source_bytes) == original[0]

            def chunks():
                offset = 0
                while offset < source.identity.source_bytes:
                    raw = source.read_window(offset, min(17, source.identity.source_bytes - offset))
                    offset += len(raw)
                    yield raw

            with validate_journal_chunks(chunks(), require_profiles=False) as accepted:
                assert accepted.anchor == original[1].anchor
            assert source.read_exact(source.identity.source_bytes, 0) == b""
            assert source.read_window(source.identity.source_bytes, 0) == b""
    finally:
        app.close()


def test_one_payload_cache_stays_current_and_never_reloads_small_warm_windows(
    case, qualified, original, monkeypatch
):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            source = source_for(work, qualified)
            reads = []
            payload = source.reader.payload_bytes

            def observed(document):
                reads.append(document.sequence)
                return payload(document)

            monkeypatch.setattr(source.reader, "payload_bytes", observed)
            for offset in range(12):
                assert source.read_window(offset, 1) == original[0][offset : offset + 1]
            assert reads == [1]
            assert source.read_window(12, 1) == original[0][12:13]
            assert source.read_window(13, 1) == original[0][13:14]
            assert reads == [1, 2]
            assert source.read_window(0, 1) == original[0][:1]
            assert reads == [1, 2, 1]
            assert len(source._page[1]) == 12
    finally:
        app.close()


@pytest.mark.parametrize("damage", ["worker", "header", "admission", "file", "lease"])
def test_cached_payload_cannot_replace_live_original_custody(case, qualified, monkeypatch, damage):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        disposition = (
            nullcontext() if damage in ("header", "admission") else pytest.raises(StaleArchiveTask)
        )
        with disposition, unit(app, case) as work:
            source = source_for(work, qualified)
            assert source.read_window(0, 1)
            if damage == "worker":
                from riverhog_core import canonical_discovery_corpus_jobs as jobs

                assert jobs.claim_discovery_worker(case.factory, work.metadata.job) is not None
            elif damage == "header":
                with work.metadata.transaction() as (db, _):
                    db.execute(
                        "UPDATE discovery_archive_journals SET source_sha256=?", ("e" * 64,)
                    ).close()
            elif damage == "admission":
                from copy import copy

                work.metadata.admission = copy(work.metadata.admission)
            elif damage == "file":
                (work.metadata.custody.directory / "discovery.sqlite3").unlink()
            else:
                (work.metadata.custody.directory / ".lease").unlink()
            assert not source.verify_current()
            with pytest.raises(StaleArchiveTask):
                source.read_window(0, 1)
    finally:
        app.close()


def test_changed_source_during_payload_read_is_not_cached_or_returned(case, qualified, monkeypatch):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            source = source_for(work, qualified)
            payload = source.reader.payload_bytes

            def changed(document):
                raw = payload(document)
                with work.metadata.transaction() as (db, _):
                    db.execute(
                        "UPDATE discovery_archive_journals SET source_sha256=?", ("e" * 64,)
                    ).close()
                return raw

            monkeypatch.setattr(source.reader, "payload_bytes", changed)
            with pytest.raises(StaleArchiveTask):
                source.read_window(0, 1)
            assert source._page is None
    finally:
        app.close()


@pytest.mark.parametrize("field", ["offset", "digest", "journal"])
def test_index_and_original_descriptor_disagreement_is_rejected(case, qualified, field):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            source = source_for(work, qualified)
            with work.metadata.read() as (db, key):
                descriptor = archive_journal_segment_at(db, key, source.identity.journal_id, 0)
            changes = {
                "offset": {"journal_offset": 1},
                "digest": {"journal_sha256": "e" * 64},
                "journal": {"journal_id": "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"},
            }
            changed = replace(descriptor, **changes[field])
            with work.metadata.transaction() as (db, _):
                db.execute(
                    "UPDATE discovery_archive_journal_segments SET descriptor_json=? "
                    "WHERE sequence=?",
                    (changed.to_json_bytes().decode(), f"{descriptor.sequence:064x}"),
                ).close()
            with pytest.raises(StaleArchiveTask, match="original descriptor"):
                source.read_window(0, 1)
    finally:
        app.close()


@pytest.mark.parametrize("range_", [(-1, 1), (True, 1), (0, -1), (0, True), (100000, 1)])
def test_invalid_ranges_do_not_read_payload(case, qualified, monkeypatch, range_):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            source = source_for(work, qualified)

            def forbidden(*_args):
                pytest.fail("invalid range acquired payload")

            monkeypatch.setattr(source.reader, "payload_bytes", forbidden)
            with pytest.raises(ValueError):
                source.read_window(*range_)
            with pytest.raises(ValueError):
                source.read_exact(*range_)
    finally:
        app.close()


def test_payload_fixity_failure_is_never_cached(case, qualified, monkeypatch):
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            source = source_for(work, qualified)
            read = source.reader._read_object

            def corrupted(path):
                if path.endswith(".bin.age"):
                    yield b"corrupt"
                else:
                    yield from read(path)

            monkeypatch.setattr(source.reader, "_read_object", corrupted)
            with pytest.raises(ProvenanceArchiveReadError, match="exact descriptor"):
                source.read_window(0, 1)
            assert source._page is None
    finally:
        app.close()


@pytest.mark.parametrize("operation", ["read_window", "read_exact"])
def test_worker_loss_during_payload_acquisition_never_returns_bytes(
    case, qualified, monkeypatch, operation
):
    from riverhog_core import canonical_discovery_corpus_jobs as jobs

    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with pytest.raises(StaleArchiveTask), unit(app, case) as work:
            source = source_for(work, qualified)
            payload = source.reader.payload_bytes

            def lost_worker(document):
                raw = payload(document)
                assert jobs.claim_discovery_worker(case.factory, work.metadata.job) is not None
                return raw

            monkeypatch.setattr(source.reader, "payload_bytes", lost_worker)
            with pytest.raises(StaleArchiveTask):
                getattr(source, operation)(0, 1)
            assert not source.verify_current()
        assert app.active_build_ids() == ()
    finally:
        app.close()
