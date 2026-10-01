from __future__ import annotations

import hashlib
from contextlib import contextmanager
from dataclasses import replace
from typing import Any

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_client.client import ApiClient
from riverhog_client.processing.history_transfer import CanonicalHistoryTransfer
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalChunkRecord
from riverhog_protocol import (
    CollectionUploadProvenanceJournalCreateDocument,
    CollectionUploadProvenanceJournalStatusDocument,
)
from riverhog_protocol.errors import Conflict
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    append_assertion_batches,
    assertion,
    create_journal,
    reference,
    validate_journal,
)
from sqlalchemy import select

from tests.unit.test_canonical_upload_custody import _construction


class _ConstructionApi:
    upload_collection_upload_session_provenance_journal = (
        ApiClient.upload_collection_upload_session_provenance_journal
    )

    def __init__(self, service: Any) -> None:
        self.service = service
        self.stream_reads = 0

    def get_collection_upload_session_provenance_journal(self, collection_id, journal_id):
        return CollectionUploadProvenanceJournalStatusDocument.model_validate(
            self.service.get_provenance_journal(collection_id, journal_id)
        )

    @contextmanager
    def stream_collection_upload_session_provenance_journal(self, collection_id, journal_id):
        self.stream_reads += 1
        yield self.service.iter_sealed_provenance_journal(collection_id, journal_id)

    def create_collection_upload_session_provenance_journal(
        self, collection_id, journal_id, *, byte_count, sha256, selection_role=None
    ):
        return CollectionUploadProvenanceJournalStatusDocument.model_validate(
            self.service.create_provenance_journal(
                collection_id,
                journal_id,
                CollectionUploadProvenanceJournalCreateDocument.model_validate(
                    {"bytes": str(byte_count), "sha256": sha256, "selection_role": selection_role}
                ),
            )
        )

    def append_collection_upload_session_provenance_journal(
        self, collection_id, journal_id, *, offset, content
    ):
        return CollectionUploadProvenanceJournalStatusDocument.model_validate(
            self.service.append_provenance_journal(
                collection_id, journal_id, offset=offset, content=content
            )
        )

    def seal_collection_upload_session_provenance_journal(self, collection_id, journal_id):
        return CollectionUploadProvenanceJournalStatusDocument.model_validate(
            self.service.seal_provenance_journal(collection_id, journal_id)
        )


def test_longer_authenticated_source_prefix_preserves_early_octets_and_upload_bounds(
    tmp_path,
) -> None:
    f = _construction(tmp_path)
    api = _ConstructionApi(f.service)
    observed = BoundedSourceObserver().observe(BytesSource(b"source"))
    first = create_journal(
        observed.graph_fragment(), recorded_by_agent_id=observed.observer_agent_id
    )
    summary = validate_journal(first)
    later = append_assertion_batches(
        first,
        (
            {
                "extensions": [
                    assertion(
                        "extension",
                        observed.observer_agent_id,
                        subject=reference(observed.state_id, "state"),
                        property="urn:test:bounded-source-record",
                        value={"type": "text", "value": str(index) + "x" * 150000},
                    )
                ]
            }
            for index in range(10)
        ),
        recorded_by_agent_id=observed.observer_agent_id,
    )
    assert len(first) < 1024 * 1024 < len(later)
    early = api.upload_collection_upload_session_provenance_journal(
        1,
        summary.journal_id,
        content=(first,),
        byte_count=len(first),
        sha256=hashlib.sha256(first).hexdigest(),
        selection_role="history-dependency",
    )
    transfer = CanonicalHistoryTransfer(api, 1)
    first_anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
    transfer._journal(None, first_anchor)
    assert api.stream_reads == 0
    with pytest.raises(ValueError, match="already accepted prefix"):
        transfer._journal(None, replace(first_anchor, prefix_sha256="f" * 64))
    complete = api.upload_collection_upload_session_provenance_journal(
        1,
        summary.journal_id,
        content=(later,),
        byte_count=len(later),
        sha256=hashlib.sha256(later).hexdigest(),
        selection_role="history-dependency",
    )
    assert early.anchor.model_dump(mode="json") == summary.anchor
    assert complete.anchor.model_dump(mode="json") == validate_journal(later).anchor
    # A longer receiver prefix still requires exact overlap authentication.
    transfer._journal(None, first_anchor)
    assert api.stream_reads == 1
    with pytest.raises(ValueError, match="already accepted prefix"):
        transfer._journal(None, replace(first_anchor, prefix_sha256="f" * 64))
    with session_scope(f.factory) as session:
        chunks = list(
            session.scalars(
                select(CollectionUploadProvenanceJournalChunkRecord)
                .where(
                    CollectionUploadProvenanceJournalChunkRecord.journal_id == summary.journal_id
                )
                .order_by(CollectionUploadProvenanceJournalChunkRecord.ordinal)
            )
        )
        assert chunks[0].content == first
        assert b"".join(row.content for row in chunks) == later
        assert len(chunks[1].content) == 1024 * 1024
    # Same-sized replacement and role substitution both fail without modifying
    # any accepted source prefix or permitting a primary rewrite.
    with pytest.raises(Conflict, match="authority already differs"):
        api.create_collection_upload_session_provenance_journal(
            1,
            summary.journal_id,
            byte_count=len(later),
            sha256="f" * 64,
            selection_role="history-dependency",
        )
    with pytest.raises(Conflict, match="construction role"):
        api.create_collection_upload_session_provenance_journal(
            1,
            f.primary.journal_id,
            byte_count=len(f.primary.content) + 1,
            sha256="f" * 64,
            selection_role="history-dependency",
        )
