from __future__ import annotations

import gc
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    JournalFrame,
    JournalValidationCache,
    ProvenanceValidationError,
    append_checkpoint,
    create_journal,
    validate_journal_chunks,
)
from riverhog_provenance_contracts import ContractCatalog


def _journal(payload: bytes = b"opaque bytes") -> tuple[bytes, str]:
    observation = BoundedSourceObserver().observe(BytesSource(payload))
    return (
        create_journal(
            observation.graph_fragment(), recorded_by_agent_id=observation.observer_agent_id
        ),
        observation.observer_agent_id,
    )


def test_current_provider_bytes_and_exact_anchors_fence_cached_validation(monkeypatch) -> None:
    import riverhog_provenance.journal as journal

    catalog = ContractCatalog()
    cache = JournalValidationCache(catalog)
    raw, recorder = _journal()
    reads = validations = 0
    original = validate_journal_chunks

    def validate(chunks, **options: Any):
        nonlocal validations
        validations += 1
        return original(chunks, **options)

    def current_bytes():
        nonlocal reads
        reads += 1
        yield raw

    monkeypatch.setattr(journal, "validate_journal_chunks", validate)
    first = original(current_bytes(), catalog=catalog, cache=cache)
    again = original(
        current_bytes(),
        catalog=catalog,
        cache=cache,
        expected_anchor=first.anchor,
        require_exact_tail=True,
    )
    assert again is not first and again.materialize() == first.materialize()
    assert (reads, validations) == (2, 1)
    wrong = {**first.anchor, "prefix_sha256": "f" * 64}
    with pytest.raises(ProvenanceValidationError, match="anchor mismatch"):
        original(current_bytes(), catalog=catalog, cache=cache, expected_anchor=wrong)
    assert reads == 3 and validations == 1

    def revoked():
        yield raw
        raise PermissionError("current read authority rejected")

    with pytest.raises(PermissionError, match="current read authority"):
        original(revoked(), catalog=catalog, cache=cache)
    with pytest.raises(ProvenanceValidationError):
        original((raw[:-1],), catalog=catalog, cache=cache)
    extended = append_checkpoint(
        raw, recorded_by_agent_id=recorder, purpose="urn:example:checkpoint", catalog=catalog
    )
    full = original((extended,), catalog=catalog, cache=cache)
    assert (
        original((extended,), catalog=catalog, cache=cache, expected_anchor=first.anchor).anchor
        == full.anchor
    )
    with pytest.raises(ProvenanceValidationError, match="exact tail"):
        original(
            (extended,),
            catalog=catalog,
            cache=cache,
            expected_anchor=first.anchor,
            require_exact_tail=True,
        )


def test_validation_mode_and_fixed_catalog_do_not_share_acceptance(monkeypatch) -> None:
    import riverhog_provenance.journal as journal

    catalog = ContractCatalog()
    cache = JournalValidationCache(catalog)
    raw, _ = _journal()
    original = validate_journal_chunks
    calls = []

    def validate(chunks, **options: Any):
        calls.append((options["catalog"], options["require_profiles"]))
        return original(chunks, **options)

    monkeypatch.setattr(journal, "validate_journal_chunks", validate)
    for strict in (False, False, True, True):
        original((raw,), catalog=catalog, require_profiles=strict, cache=cache)
    other = ContractCatalog()
    original((raw,), catalog=other, require_profiles=False, cache=cache)
    assert calls == [(catalog, False), (catalog, True), (other, False)]


def test_cache_capacity_does_not_limit_valid_journals_or_close_active_snapshots() -> None:
    catalog = ContractCatalog()
    cache = JournalValidationCache(catalog, entry_budget=1)
    raw, _ = _journal()
    first = validate_journal_chunks((raw,), catalog=catalog, cache=cache)
    expected = first.materialize()
    other, _ = _journal(b"different bytes")
    validate_journal_chunks((other,), catalog=catalog, cache=cache)
    assert len(cache._entries) == 1 and cache._bytes <= cache.byte_budget
    assert first.materialize() == expected
    snapshot = first.frames.store.path
    del cache
    gc.collect()
    assert snapshot.exists() and first.materialize() == expected
    del first
    gc.collect()
    assert not snapshot.exists()
    uncached = JournalValidationCache(catalog, byte_budget=0)
    for _ in range(2):
        assert (
            validate_journal_chunks((raw,), catalog=catalog, cache=uncached).materialize()
            == expected
        )
    assert not uncached._entries and uncached._bytes == 0


def test_shared_frozen_projection_has_independent_views_and_concurrent_readers() -> None:
    catalog = ContractCatalog()
    cache = JournalValidationCache(catalog)
    raw, _ = _journal()
    first = validate_journal_chunks((raw,), catalog=catalog, cache=cache)
    expected = first.materialize()
    first.frames[0].document["body"].clear()
    first.states[0]["id"] = "changed caller view"
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        first.frames.store.db.execute("DELETE FROM frames")

    anchor = first.anchor

    def read(_):
        selected = validate_journal_chunks(
            (raw,),
            catalog=catalog,
            cache=cache,
            expected_anchor=anchor,
            require_exact_tail=True,
        )
        return selected.materialize()

    with ThreadPoolExecutor(max_workers=4) as workers:
        assert list(workers.map(read, range(12))) == [expected] * 12


def test_failed_validation_and_incomplete_current_reads_are_never_cached() -> None:
    catalog = ContractCatalog()
    cache = JournalValidationCache(catalog)
    raw, _ = _journal()
    for _ in range(2):
        with pytest.raises(ProvenanceValidationError):
            validate_journal_chunks((raw[:-1],), catalog=catalog, cache=cache)
    assert not cache._entries
    validate_journal_chunks((raw,), catalog=catalog, cache=cache)
    with pytest.raises(ProvenanceValidationError, match="empty"):
        validate_journal_chunks((), catalog=catalog, cache=cache)
    assert len(cache._entries) == 1


def test_projected_frame_views_preserve_exact_bytes_and_checked_intake(monkeypatch) -> None:
    import riverhog_provenance.journal as journal

    raw, _ = _journal()
    summary = validate_journal_chunks((raw,))
    projected = summary.frames[0]
    expected_document = projected.document
    expected_reference = projected.reference
    expected_encoded = projected.encoded
    direct = JournalFrame(projected.json_bytes)
    assert direct.document == expected_document and direct.reference == expected_reference
    noncanonical = projected.json_bytes.replace(b"{", b"{ ", 1)
    with pytest.raises(ValueError):
        _ = JournalFrame(noncanonical).document

    def repeated_intake(_):
        raise AssertionError("validated projection repeated byte-intake checks")

    monkeypatch.setattr(journal, "decode_document", repeated_intake)
    for frame in (summary.frames[0], *summary.frames):
        frame.document["body"].clear()
        reference = frame.reference
        reference["entry_id"] = "changed caller reference"
        assert frame.document == expected_document
        assert frame.reference == expected_reference
        assert frame.encoded == expected_encoded
        assert frame.sha256 == expected_reference["json_sha256"]
    with pytest.raises(AssertionError, match="byte-intake"):
        _ = direct.document


def test_default_core_validation_can_reuse_current_bytes_without_an_explicit_catalog() -> None:
    raw, _ = _journal()
    cache = JournalValidationCache()
    first = validate_journal_chunks((raw,), cache=cache)
    again = validate_journal_chunks(
        (raw,), cache=cache, expected_anchor=first.anchor, require_exact_tail=True
    )
    assert again.materialize() == first.materialize()
    assert len(cache._entries) == 1
    assert (
        validate_journal_chunks((raw,), catalog=ContractCatalog(), cache=cache).materialize()
        == first.materialize()
    )
    assert len(cache._entries) == 1
