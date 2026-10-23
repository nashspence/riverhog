from __future__ import annotations

import hashlib

import pytest
from riverhog_core.catalog_models import CollectionUploadProvenanceCaptureSegmentRecord as Segment
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services.upload_journal_source import UploadJournalValidationSource
from riverhog_provenance import (
    JournalValidationSession,
    ProvenanceValidationError,
    validate_journal,
)
from sqlalchemy import delete, insert, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_journal_validation_handoff import complete_capture
from tests.unit.test_staged_journal_range import JOURNAL, Chunk, Journal


def owned(factory, work):
    return UploadJournalValidationSource(
        factory, work, catalog=source_owner.admission_provenance_catalog()
    )


@pytest.mark.parametrize("extra", [0, 65537])
def test_captured_source_windows_and_atoms_use_closed_fixed_pages(tmp_path, extra):
    raw = source(extra, journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        work = complete_capture(factory)
        current = owned(factory, work)
        with factory() as session, session.begin():
            session.execute(delete(Chunk))
        pieces, offset = [], 0
        while offset < len(raw):
            window = current.read_window(offset, min(65536, len(raw) - offset))
            assert 1 <= len(window) <= 65536 and engine.pool.checkedout() == 0
            pieces.append(window)
            offset += len(window)
        assert b"".join(pieces) == raw
        assert current.read_exact(1, len(raw) - 2) == raw[1:-1]
        assert engine.pool.checkedout() == 0 and current.verify_current()


def test_source_cache_retains_only_one_fixed_page(tmp_path, monkeypatch):
    raw = source(65537, journal_id=JOURNAL)
    with retained(tmp_path, raw) as (_, factory):
        work = complete_capture(factory)
        current = owned(factory, work)
        read = source_owner._read_upload_journal_capture_segment
        reads = []

        def observed(*args, **kwargs):
            reads.append(kwargs["ordinal"])
            return read(*args, **kwargs)

        monkeypatch.setattr(source_owner, "_read_upload_journal_capture_segment", observed)
        assert current.read_window(0, 1) == raw[:1]
        assert current.read_window(1, 1) == raw[1:2]
        assert current.read_exact(2, 7) == raw[2:9]
        assert reads == [0]
        assert current.read_window(65536, 1) == raw[65536:65537]
        assert current.read_window(0, 1) == raw[:1]
        assert reads == [0, 1, 0]


def test_deferred_window_reads_one_tiny_transport_chunk_without_range_accumulation(tmp_path):
    raw = source(journal_id=JOURNAL) + b"malformed tail"
    with retained(tmp_path, raw) as (engine, factory):
        work = complete_capture(factory)
        assert work.phase == "deferred" and work.cursor.byte_offset < len(raw)
        split = work.cursor.byte_offset
        rows = [
            {
                "collection_id": 1,
                "journal_id": JOURNAL,
                "ordinal": 0,
                "byte_offset": 0,
                "content": raw[:split],
            },
            *[
                {
                    "collection_id": 1,
                    "journal_id": JOURNAL,
                    "ordinal": ordinal + 1,
                    "byte_offset": split + ordinal,
                    "content": raw[split + ordinal : split + ordinal + 1],
                }
                for ordinal in range(len(raw) - split)
            ],
        ]
        with factory() as session, session.begin():
            session.execute(delete(Chunk))
            session.execute(insert(Chunk), rows)
            session.execute(update(Journal).values(next_chunk_ordinal=len(rows)))
        current = owned(factory, work)
        assert current.read_window(split, len(raw) - split) == raw[split : split + 1]
        assert engine.pool.checkedout() == 0
        with pytest.raises(RuntimeError, match="exceeds the captured"):
            current.read_exact(split, 1)


@pytest.mark.parametrize("fault", ["missing", "digest"])
def test_lost_or_changed_captured_pages_fail_without_acceptance(tmp_path, fault):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        current = owned(factory, complete_capture(factory))
        with factory() as session, session.begin():
            session.execute(
                delete(Segment) if fault == "missing" else update(Segment).values(sha256="0" * 64)
            )
        with pytest.raises(RuntimeError, match="missing source page|digest differs"):
            current.read_window(0, 1)
        assert engine.pool.checkedout() == 0


@pytest.mark.parametrize("method", ["read_window", "read_exact"])
def test_replaced_source_cannot_return_cached_octets(tmp_path, method):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        current = owned(factory, complete_capture(factory))
        current.read_window(0, 1)
        with factory() as session, session.begin():
            session.execute(update(Journal).values(capture_attempt_id="replacement"))
        assert not current.verify_current()
        with pytest.raises(source_owner._StaleUploadJournalValidation):
            getattr(current, method)(0, 1)
        assert engine.pool.checkedout() == 0


@pytest.mark.parametrize("malformed", [False, True])
def test_core_retained_source_preserves_native_result_or_error(tmp_path, malformed):
    raw = source(journal_id=JOURNAL) + (b"bad framing" if malformed else b"")
    failure = None
    try:
        with validate_journal(raw) as expected:
            expected_digest = expected.journal_sha256
    except ProvenanceValidationError as exc:
        failure = str(exc)
    with retained(tmp_path, raw) as (engine, factory):
        current = owned(factory, complete_capture(factory))
        directory = tmp_path / "validation"
        directory.mkdir(mode=0o700)
        with JournalValidationSession.open_retained(
            directory,
            current,
            attempt_id="a" * 32,
            worker_epoch=0,
            create=True,
            catalog=source_owner.admission_provenance_catalog(),
        ) as validation:
            try:
                for _ in range(10000):
                    progress = validation.advance()
                    assert engine.pool.checkedout() == 0
                    if progress.complete:
                        break
                else:
                    pytest.fail("retained source stopped making bounded progress")
            except ProvenanceValidationError as exc:
                assert malformed and str(exc) == failure
            else:
                assert not malformed
                with validation.result() as result:
                    assert result.journal_sha256 == expected_digest
                    assert result.journal_sha256 == hashlib.sha256(raw).hexdigest()
