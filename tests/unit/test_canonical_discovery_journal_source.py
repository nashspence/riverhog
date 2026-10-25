from __future__ import annotations

import hashlib
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_archive_framing as framing
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_journal_source import DiscoveryJournalSource
from riverhog_provenance import (
    IncompleteSourceError,
    JournalValidationBudget,
    JournalValidationCapacity,
    JournalValidationSession,
    ProvenanceValidationError,
    iter_journal_frames,
    validate_journal_chunks,
)
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX

from tests.unit.test_canonical_discovery_archive_framing import (
    apply,
    begin,
    framing_case,
    prepare,
)


@contextmanager
def source_case(
    path, raw, *, segments=1, retained=17, byte_budget=65536, catalog=None, journal_id=None
):
    with framing_case(
        path, raw, segments=segments, retained=retained, catalog=catalog, journal_id=journal_id
    ) as (
        case,
        work,
    ):
        begin(case, work)
        while page := prepare(case, work, byte_budget=byte_budget, frame_budget=8192):
            assert apply(case, work, page)
        source = DiscoveryJournalSource(work, directory_id=case.metadata.job.workspace_id)
        yield case, work, source


def test_exact_ranges_use_normalized_pages_without_reading_original_segments(tmp_path, monkeypatch):
    extent = framing.ARCHIVE_CAPTURE_PAGE_OCTETS
    raw = b"\x1e" + b"x" * (extent + 17) + b"\n"
    with source_case(tmp_path / "ranges", raw, segments=1025) as (case, work, source):
        original_calls = []

        def forbidden(*args):
            original_calls.append(args)
            pytest.fail("normalized exact reads fetched original archive fragments")

        monkeypatch.setattr(work.source, "read_window", forbidden)
        assert source.read_exact(1, len(raw) - 2) == raw[1:-1]
        assert source.read_window(extent - 1, 4) == raw[extent - 1 : extent]
        assert source.read_window(extent, 18) == raw[extent : extent + 18]
        assert source.read_exact(len(raw), 0) == b""
        assert source.read_window(len(raw), 0) == b""
        assert not original_calls
        assert source._page is not None and len(source._page.content) <= extent
        assert source.binding.source_bytes == str(len(raw))
        assert source.binding.source_sha256 == hashlib.sha256(raw).hexdigest()
        assert source.binding.journal_id == work.source.identity.journal_id
        assert source.binding.catalog_sha256 == case.metadata.job.catalog_sha256


@pytest.mark.parametrize("raw", [b"x" * 17, b"\x1e{}\n" + b"x" * 17])
def test_deferred_framing_preserves_short_original_fallback_and_native_failure_order(tmp_path, raw):
    with source_case(tmp_path / "deferred", raw, segments=2, byte_budget=1) as (case, work, source):
        captured = source._progress.cursor.byte_offset
        assert source._progress.phase == "deferred" and captured < len(raw)
        window = source.read_window(captured, len(raw) - captured)
        assert window and window == raw[captured : captured + len(window)]
        with pytest.raises(ValueError, match="captured"):
            source.read_exact(captured, 1)
        directory = case.path / "native"
        directory.mkdir(mode=0o700)
        with pytest.raises(ProvenanceValidationError) as original:
            with validate_journal_chunks((raw,), require_profiles=False):
                pytest.fail("malformed original bytes were accepted")
        with JournalValidationSession.open_retained(
            directory,
            source,
            attempt_id=case.metadata.job.workspace_id,
            worker_epoch=case.metadata.job.worker_epoch,
            create=True,
        ) as native:
            with pytest.raises(type(original.value)) as retained:
                for _ in range(10000):
                    if native.advance(JournalValidationBudget(rows=2, source_octets=7)).complete:
                        pytest.fail("deferred framing supplied Native acceptance")
            assert str(retained.value) == str(original.value)


@pytest.mark.parametrize("damage", ["worker", "progress", "identity", "metadata"])
def test_live_currentness_precedes_cached_page_reads(tmp_path, monkeypatch, damage):
    with source_case(tmp_path / "current", b"\x1e{}\n" * 17) as (case, work, source):
        assert source.read_exact(0, 4) == b"\x1e{}\n"
        cached = source._page
        if damage == "worker":
            monkeypatch.setattr(work.source, "verify_current", lambda: False)
        elif damage == "progress":
            case.metadata._db.execute(
                "UPDATE discovery_archive_framing SET version=CAST(version AS INTEGER)+1"
            ).close()
        elif damage == "identity":
            work.source._header = replace(work.source.identity, source_sha256="0" * 64)
        else:
            work.source.metadata = object()
        assert source.verify_current() is False
        with pytest.raises(StaleArchiveTask):
            source.read_exact(0, 4)
        assert source._page is cached
        if damage == "metadata":
            work.source.metadata = case.metadata


def test_changed_or_lost_page_never_returns_a_cold_result(tmp_path):
    with source_case(tmp_path / "page", b"\x1e{}\n" * 17) as (case, _, source):
        case.metadata._db.execute(
            "UPDATE discovery_archive_pages SET content=?", (b"x" * 68,)
        ).close()
        with pytest.raises(StaleArchiveTask, match="retained page"):
            source.read_exact(0, 4)
        assert source._page is None


def test_source_requires_terminal_framing_and_original_owned_identity(tmp_path):
    with framing_case(tmp_path / "not-ready", b"\x1e{}\n") as (case, work):
        begin(case, work)
        with pytest.raises(StaleArchiveTask, match="terminal"):
            DiscoveryJournalSource(work, directory_id="selected")
        assert apply(case, work, prepare(case, work))
        with pytest.raises(ValueError):
            DiscoveryJournalSource(work, directory_id="")
        with case.metadata.transaction():
            with pytest.raises(ValueError, match="write transaction"):
                DiscoveryJournalSource(work, directory_id="selected")


@pytest.mark.parametrize(("offset", "octets"), [(-1, 1), (0, -1), (True, 1), (0, True), (4, 1)])
def test_invalid_ranges_have_no_source_authority(tmp_path, offset, octets):
    with source_case(tmp_path / "invalid", b"\x1e{}\n") as (_, _, source):
        with pytest.raises(ValueError):
            source.read_exact(offset, octets)
        with pytest.raises(ValueError):
            source.read_window(offset, octets)


def test_existing_frame_capacity_and_writer_boundary_remain_explicit(tmp_path):
    raw = b"\x1e" + b"x" * (PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 1) + b"\n"
    # Capacity is checked before source access; this larger original header
    # remains a valid range rather than creating a new total-source limit.
    with source_case(tmp_path / "capacity", raw, segments=17) as (case, _, source):
        with pytest.raises(JournalValidationCapacity):
            source.read_exact(0, PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 1)
        with case.metadata.transaction():
            with pytest.raises(ValueError, match="write transaction"):
                source.read_window(0, 1)
            with pytest.raises(ValueError, match="write transaction"):
                source.read_exact(0, 1)


@pytest.mark.parametrize("exact_header", [True, False])
def test_real_native_session_checks_original_journal_binding_from_normalized_pages(
    tmp_path, exact_header
):
    from tests.support.canonical_discovery_work import corrected_journal_fixture

    fixture, raw, _ = corrected_journal_fixture()
    journal_id = tuple(iter_journal_frames((raw,)))[0].document["journal_id"]
    declared = journal_id if exact_header else "urn:uuid:00000000-0000-4000-8000-000000000001"
    assert exact_header or declared != journal_id
    with source_case(
        tmp_path / "native", raw, segments=17, catalog=fixture.catalog, journal_id=declared
    ) as (case, work, source):
        assert fixture.catalog.checkpoint_sha256() == source.binding.catalog_sha256
        directory = case.path / "native"
        directory.mkdir(mode=0o700)
        with JournalValidationSession.open_retained(
            directory,
            source,
            attempt_id=case.metadata.job.workspace_id,
            worker_epoch=case.metadata.job.worker_epoch,
            create=True,
            catalog=fixture.catalog,
        ) as native:

            def finish():
                for _ in range(100000):
                    if native.advance(JournalValidationBudget(rows=64)).complete:
                        return
                pytest.fail("small normalized journal did not complete")

            if exact_header:
                finish()
                with native.result() as result:
                    assert result.journal_sha256 == hashlib.sha256(raw).hexdigest()
                    assert result.journal_bytes == len(raw)
                    assert result.journal_id == work.source.identity.journal_id
            else:
                with pytest.raises(IncompleteSourceError, match="exact retained binding"):
                    finish()
                with pytest.raises(RuntimeError, match="terminal result"):
                    native.result()


def test_real_worker_handoff_reopens_original_capture_with_the_same_binding(tmp_path):
    from riverhog_core import canonical_discovery_corpus_jobs as jobs
    from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
    from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata

    raw = b"\x1e{}\n" * 17
    with source_case(tmp_path / "handoff", raw, segments=17) as (case, work, source):
        assert source.read_exact(0, 4) == raw[:4]
        binding = source.binding
        previous = case.metadata
        previous.close()
        claimed = jobs.claim_discovery_worker(case.factory, previous.job)
        assert claimed is not None and claimed.worker_epoch > previous.job.worker_epoch
        current = DiscoveryMetadata(case.factory, claimed, case.admission, previous.custody)
        case.metadata = current
        try:
            current.open()
            resumed_work = framing.ArchiveJournalFramingWork(
                ArchiveJournalSource(current, work.source.reader, work.source.identity.journal_id)
            )
            resumed = DiscoveryJournalSource(resumed_work, directory_id=binding.directory_id)
            assert resumed.binding == binding
            assert resumed.read_exact(0, len(raw)) == raw
            assert source.verify_current() is False
            with pytest.raises(StaleArchiveTask):
                source.read_exact(0, 4)
        finally:
            current.close()
            case.metadata = previous
