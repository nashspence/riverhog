"""Resume a fenced canonical discovery generation using only archive custody."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterator, Mapping

from riverhog_archive_contracts import MemberHistoryBinding
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
)
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_provenance import JournalSummary, MemberHistoryClosure, validate_journal_chunks
from riverhog_provenance_contracts import core_contract
from sqlalchemy import func, select

from riverhog_core.canonical_discovery_build import stage_relevant_member
from riverhog_core.canonical_discovery_index import (
    EXTRACTION_CONTRACT_SHA256,
    StaleIndexBuild,
    begin_index_build,
    complete_index_build,
    publish_index_build,
)
from riverhog_core.canonical_discovery_relevance import member_relevance, snapshots_for_relevance
from riverhog_core.canonical_provenance_archive import PublishedCanonicalProvenance
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionDeletionRecord, CollectionRecord
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexGenerationRecord as Generation,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexMemberRecord as Member,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexMembershipRecord as Membership,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexSnapshotRecord as Snapshot,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexStateRecord as State,
)
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader
from riverhog_core.provenance_catalog import admission_provenance_catalog


class _ArchiveCorpus(Mapping[str, JournalSummary]):
    def __init__(self, reader: CanonicalProvenanceArchiveReader) -> None:
        self.reader = reader
        self.cache: OrderedDict[str, JournalSummary] = OrderedDict()

    def __getitem__(self, journal_id: str) -> JournalSummary:
        if journal_id not in self.cache:
            self.cache[journal_id] = validate_journal_chunks(
                self.reader.iter_journal_range(journal_id),
                catalog=admission_provenance_catalog(),
                require_profiles=False,
            )
        self.cache.move_to_end(journal_id)
        result = self.cache[journal_id]
        if len(self.cache) > 2:
            self.cache.popitem(last=False)
        return result

    def __iter__(self) -> Iterator[str]:
        yield from self.reader.iter_journal_ids()

    def __len__(self) -> int:
        return self.reader.scan().root.journal_count


def rebuild_canonical_index(
    factory: SessionFactory, archives: PublishedCanonicalProvenance, collection_id: int
) -> str:
    """Commit one complete member at a time; keep the old generation until publication.

    Interrupted work resumes at its committed member key. Journal and structural
    reads happen outside catalog transactions, with bounded protected disk caches.
    No upload scratch, media interpreter or existing search projection supplies
    canonical facts.
    """
    catalog = admission_provenance_catalog()
    with session_scope(factory) as session:
        collection = session.get(CollectionRecord, collection_id, with_for_update=True)
        if (
            collection is None
            or not collection.is_published
            or session.get(CollectionDeletionRecord, collection_id) is not None
        ):
            raise StaleIndexBuild("collection is not eligible for archive discovery rebuild")
        count = collection.artifact_count
        state = session.get(State, collection_id, with_for_update=True)
        pending = (
            session.get(Generation, state.pending_build_id)
            if state is not None and state.pending_build_id is not None
            else None
        )
        if (
            state is not None
            and pending is not None
            and pending.expected_epoch == state.epoch
            and pending.archive_root_sha256 == collection.archive_root_sha256
            and pending.provenance_identity == collection.provenance_identity
            and pending.core_contract_sha256 == core_contract().contract_sha256
            and pending.extraction_contract_sha256 == EXTRACTION_CONTRACT_SHA256
        ):
            build_id = pending.build_id
            if pending.complete:
                return publish_index_build(session, build_id=build_id)
            state.phase = "indexing"
            state.failure = None
        else:
            build_id = begin_index_build(session, collection_id=collection_id)
        after_id = session.scalar(
            select(func.max(Member.artifact_id)).where(Member.build_id == build_id)
        )
    try:
        with archives.reader(collection_id).cached() as reader, reader.prepared():
            root = reader.scan().root
            if root.binding_count != count:
                raise StaleIndexBuild("archive member count differs from its publication")
            corpus = _ArchiveCorpus(reader)
            for row in reader.iter_bindings():
                selected = MemberHistoryBinding.from_mapping(row)
                if after_id is not None and selected.artifact_id <= after_id:
                    continue
                history = reader.member_history(selected)
                binding = CollectionArtifactProvenanceBindingDocument.model_validate(
                    {"artifact_id": selected.artifact_id, **history.primary.to_mapping()}
                )
                member = ArtifactMemberIdentityDocument.model_validate(
                    {
                        "artifact_id": selected.artifact_id,
                        "bytes": str(selected.bytes),
                        "sha256": selected.sha256,
                    }
                )
                primary = validate_journal_chunks(
                    reader.iter_journal_range(
                        binding.journal.journal_id, size=int(binding.journal.prefix_bytes)
                    ),
                    catalog=catalog,
                    expected_anchor=binding.journal.model_dump(mode="json"),
                    require_exact_tail=True,
                    require_profiles=False,
                )
                with (
                    MemberHistoryClosure(
                        reader.history_store(),
                        lambda journal_id, size: reader.iter_journal_range(journal_id, size=size),
                        member_role=COLLECTION_MEMBER_ROLE,
                        catalog=catalog,
                    ) as closure,
                    member_relevance(
                        member=member,
                        binding=binding,
                        primary=primary,
                        corpus=corpus,
                        delivery_context_id=root.delivery_context_id,
                        catalog=catalog,
                        history_binding=selected,
                        closure=closure,
                    ) as relevance,
                ):
                    # Finish any required archive reads before taking the write fence.
                    for _snapshot in snapshots_for_relevance(
                        relevance, corpus=corpus, catalog=catalog
                    ):
                        pass
                    with session_scope(factory) as session:
                        stage_relevant_member(
                            session,
                            build_id=build_id,
                            member=member,
                            binding=binding,
                            primary=primary,
                            relevance=relevance,
                            corpus=corpus,
                            catalog=catalog,
                        )
            with session_scope(factory) as session:
                counts = [
                    int(
                        session.scalar(
                            select(func.count())
                            .select_from(model)
                            .where(model.build_id == build_id)
                        )
                        or 0
                    )
                    for model in (Snapshot, Member, Membership)
                ]
                if counts[1] != count:
                    raise StaleIndexBuild("archive rebuild did not cover every member")
                complete_index_build(
                    session,
                    build_id=build_id,
                    expected_snapshots=counts[0],
                    expected_members=count,
                    expected_memberships=counts[2],
                )
                return publish_index_build(session, build_id=build_id)
    except Exception as exc:
        with session_scope(factory) as session:
            state = session.get(State, collection_id, with_for_update=True)
            if state is not None and state.pending_build_id == build_id:
                state.phase = "failed"
                state.failure = str(exc)[:1000]
        raise
