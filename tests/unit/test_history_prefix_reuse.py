"""Reuse exact overlap authentication, never selected-history or permission decisions."""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_client.processing.history_transfer import CanonicalHistoryTransfer
from riverhog_protocol.errors import Forbidden, NotFound
from riverhog_provenance import BoundedSourceObserver, BytesSource, create_journal, validate_journal


def anchor():
    observed = BoundedSourceObserver().observe(BytesSource(b"source"))
    content = create_journal(
        observed.graph_fragment(), recorded_by_agent_id=observed.observer_agent_id
    )
    selected = HistoryJournalAnchor.from_mapping(validate_journal(content).anchor)
    return content, selected


class PrefixApi:
    def __init__(self, prefix: bytes) -> None:
        self.prefix = prefix
        self.suffix = b"unselected later prefix"
        self.status_reads = 0
        self.stream_reads = 0
        self.suffix_reads = 0
        self.fail_status: Exception | None = None
        self.fail_stream = False

    def get_collection_upload_session_provenance_journal(self, collection_id, journal_id):
        self.status_reads += 1
        if self.fail_status is not None:
            raise self.fail_status
        return SimpleNamespace(
            state="sealed",
            bytes=len(self.prefix) + len(self.suffix),
            sha256=hashlib.sha256(self.prefix + self.suffix).hexdigest(),
        )

    @contextmanager
    def stream_collection_upload_session_provenance_journal(self, collection_id, journal_id):
        self.stream_reads += 1

        def chunks():
            if self.fail_stream:
                raise TimeoutError("interrupted overlap read")
            yield self.prefix
            self.suffix_reads += 1
            yield self.suffix

        yield chunks()


def test_same_overlap_is_verified_once_but_receiver_status_is_always_read() -> None:
    prefix, selected = anchor()
    api = PrefixApi(prefix)
    transfer = CanonicalHistoryTransfer(api, 1)
    for _ in range(128):
        transfer._journal(None, selected)
    assert api.status_reads == 128
    assert api.stream_reads == 1
    assert api.suffix_reads == 0


def test_receiver_extension_or_replacement_requires_fresh_overlap_verification() -> None:
    prefix, selected = anchor()
    api = PrefixApi(prefix)
    transfer = CanonicalHistoryTransfer(api, 1)
    transfer._journal(None, selected)
    api.suffix += b"later extension"
    transfer._journal(None, selected)
    assert api.stream_reads == 2
    api.suffix = b"x" * len(api.suffix)  # equal size is not equal identity
    transfer._journal(None, selected)
    assert api.stream_reads == 3
    api.prefix = b"x" * len(prefix)
    with pytest.raises(ValueError, match="conflicts"):
        transfer._journal(None, selected)


def test_cached_overlap_does_not_accept_changed_selected_prefix() -> None:
    prefix, selected = anchor()
    api = PrefixApi(prefix)
    transfer = CanonicalHistoryTransfer(api, 1)
    transfer._journal(None, selected)
    with pytest.raises(ValueError, match="conflicts"):
        transfer._journal(None, replace(selected, prefix_sha256="f" * 64))
    assert api.stream_reads == 2


def test_failed_overlap_read_is_not_cached() -> None:
    prefix, selected = anchor()
    api = PrefixApi(prefix)
    transfer = CanonicalHistoryTransfer(api, 1)
    api.fail_stream = True
    with pytest.raises(TimeoutError):
        transfer._journal(None, selected)
    api.fail_stream = False
    transfer._journal(None, selected)
    assert api.stream_reads == 2
    transfer._journal(None, selected)
    assert api.stream_reads == 2


def test_receiver_authorization_is_rechecked_even_after_cache_hit() -> None:
    prefix, selected = anchor()
    api = PrefixApi(prefix)
    transfer = CanonicalHistoryTransfer(api, 1)
    transfer._journal(None, selected)
    api.fail_status = Forbidden("permission revoked")
    with pytest.raises(Forbidden):
        transfer._journal(None, selected)
    assert api.stream_reads == 1


def test_new_transfer_has_no_trusted_prefix_state_from_previous_writer() -> None:
    prefix, selected = anchor()
    api = PrefixApi(prefix)
    CanonicalHistoryTransfer(api, 1)._journal(None, selected)
    CanonicalHistoryTransfer(api, 1)._journal(None, selected)
    CanonicalHistoryTransfer(api, 2)._journal(None, selected)
    assert api.stream_reads == 3


def test_prefix_cache_eviction_does_not_limit_history_extent() -> None:
    prefix, selected = anchor()
    api = PrefixApi(prefix)
    transfer = CanonicalHistoryTransfer(api, 1)
    for index in range(140):
        value = replace(selected, journal_id=f"urn:uuid:00000000-0000-4000-8000-{index:012d}")
        transfer._journal(None, value)
    assert len(transfer._verified_prefixes) == 128
    oldest = replace(selected, journal_id="urn:uuid:00000000-0000-4000-8000-000000000000")
    transfer._journal(None, oldest)
    assert api.stream_reads == 141


def test_deleted_receiver_prefix_does_not_succeed_from_cache() -> None:
    prefix, selected = anchor()
    api = PrefixApi(prefix)
    transfer = CanonicalHistoryTransfer(api, 1)
    transfer._journal(None, selected)
    api.fail_status = NotFound("receiver entry removed")

    def upload(*args, **kwargs):
        raise RuntimeError("a new authenticated transfer is required")

    api.upload_collection_upload_session_provenance_journal = upload
    with pytest.raises(RuntimeError, match="new authenticated transfer"):
        transfer._journal(None, selected)
