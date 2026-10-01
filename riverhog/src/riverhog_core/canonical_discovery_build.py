"""Shared bounded staging for initial admission and archive-only index rebuilds."""

from __future__ import annotations

from collections.abc import Mapping
from itertools import islice

from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
)
from riverhog_provenance import JournalSummary
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy.orm import Session

from riverhog_core.canonical_discovery_index import (
    stage_assertion_page,
    stage_entry_page,
    stage_member,
    stage_membership_page,
    stage_snapshot_header,
)
from riverhog_core.canonical_discovery_relevance import (
    MemberRelevance,
    relevance_row_keys,
    snapshots_for_relevance,
)
from riverhog_core.canonical_discovery_rows import iter_index_assertions
from riverhog_core.catalog_provenance_index_models import CollectionProvenanceIndexSnapshotRecord


def stage_relevant_member(
    session: Session,
    *,
    build_id: str,
    member: ArtifactMemberIdentityDocument,
    binding: CollectionArtifactProvenanceBindingDocument,
    primary: JournalSummary,
    relevance: MemberRelevance,
    corpus: Mapping[str, JournalSummary],
    catalog: ContractCatalog,
) -> int:
    for summary in snapshots_for_relevance(relevance, corpus=corpus, catalog=catalog):
        snapshot_key = (build_id, summary.journal_id, summary.journal_sha256)
        if session.get(CollectionProvenanceIndexSnapshotRecord, snapshot_key) is not None:
            continue
        stage_snapshot_header(session, build_id=build_id, summary=summary)
        session.flush()
        for start in range(0, len(summary.frames), 128):
            stage_entry_page(session, build_id=build_id, summary=summary, start=start)
        session.flush()
        rows = iter_index_assertions(summary)
        while batch := tuple(islice(rows, 2)):
            stage_assertion_page(session, build_id=build_id, rows=batch)
    stage_member(
        session,
        build_id=build_id,
        artifact_id=member.artifact_id,
        bytes=int(member.bytes),
        sha256=member.sha256,
        journal_id=primary.journal_id,
        prefix_sha256=primary.journal_sha256,
        delivery_association_id=binding.delivery_association_id,
    )
    session.flush()
    memberships = 0
    membership_rows = (
        (member.artifact_id, row_key, scope) for row_key, scope in relevance_row_keys(relevance)
    )
    while membership_batch := tuple(islice(membership_rows, 1024)):
        stage_membership_page(session, build_id=build_id, rows=membership_batch)
        memberships += len(membership_batch)
    return memberships
