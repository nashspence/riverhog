"""Exact claim-scoped history transfer, with immutable prefix deduplication."""

from __future__ import annotations

import hashlib
from collections import OrderedDict
from collections.abc import Iterator

from riverhog_archive_contracts import HistoryJournalAnchor, MemberHistoryImport
from riverhog_protocol.errors import NotFound

from riverhog_client.client import ApiClient
from riverhog_client.processing.provenance import ClaimedProvenance

_VERIFIED_PREFIX_CACHE_ENTRIES = 128


class CanonicalHistoryTransfer:
    """Copy only an explicitly accepted closure; never discover ancestry by names."""

    def __init__(self, api: ApiClient, collection_id: int) -> None:
        self.api = api
        self.collection_id = collection_id
        # Cache only successful byte-prefix authentication within this transfer.
        # Destination status and source closure/claim checks still run afresh.
        self._verified_prefixes: OrderedDict[tuple[str, int, str, int, str], None] = OrderedDict()

    def accept(self, source: ClaimedProvenance, *, extent: str) -> MemberHistoryImport:
        with source.history_import(extent=extent) as (imported, proof, closure):
            self.api.stage_collection_upload_session_history_structure(
                self.collection_id, proof.to_json_bytes()
            )
            for content in closure.structure_objects():
                self.api.stage_collection_upload_session_history_structure(
                    self.collection_id, content
                )
            for selected in closure.journal_anchors():
                self._journal(source, selected)
        return imported

    def _journal(self, source: ClaimedProvenance, selected: HistoryJournalAnchor) -> None:
        try:
            old = self.api.get_collection_upload_session_provenance_journal(
                self.collection_id, selected.journal_id
            )
        except NotFound:
            old = None
        if old is not None and old.state == "sealed" and old.bytes >= selected.prefix_bytes:
            if old.bytes == selected.prefix_bytes:
                if old.sha256 != selected.prefix_sha256:
                    raise ValueError("imported journal conflicts with an already accepted prefix")
                return
            # The same source journal may serve multiple exact selected prefixes.
            # Authenticate overlap, preserving each import's independent extent.
            key = (
                selected.journal_id,
                old.bytes,
                old.sha256,
                selected.prefix_bytes,
                selected.prefix_sha256,
            )
            if key in self._verified_prefixes:
                self._verified_prefixes.move_to_end(key)
                return
            digest = hashlib.sha256()
            remaining = selected.prefix_bytes
            with self.api.stream_collection_upload_session_provenance_journal(
                self.collection_id, selected.journal_id
            ) as chunks:
                for chunk in chunks:
                    prefix = chunk[:remaining]
                    digest.update(prefix)
                    remaining -= len(prefix)
                    if remaining == 0:
                        break
            if remaining or digest.hexdigest() != selected.prefix_sha256:
                raise ValueError("imported journal conflicts with an already accepted prefix")
            self._verified_prefixes[key] = None
            if len(self._verified_prefixes) > _VERIFIED_PREFIX_CACHE_ENTRIES:
                self._verified_prefixes.popitem(last=False)
            return
        expected_bytes = selected.prefix_bytes
        expected_sha256 = selected.prefix_sha256
        if old is not None and old.bytes > expected_bytes:
            # Reconcile an interrupted, previously declared longer transfer.
            expected_bytes, expected_sha256 = old.bytes, old.sha256

        def content() -> Iterator[bytes]:
            yield from source.journal_prefix(selected.journal_id, expected_bytes)

        self.api.upload_collection_upload_session_provenance_journal(
            self.collection_id,
            selected.journal_id,
            content=content(),
            byte_count=expected_bytes,
            sha256=expected_sha256,
            selection_role="history-dependency",
        )


__all__ = ["CanonicalHistoryTransfer"]
