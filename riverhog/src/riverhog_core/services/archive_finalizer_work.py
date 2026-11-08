"""Submit selected archive-finalizer turns through the upload service's owners."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from functools import partial
from typing import Literal, Protocol, cast

from riverhog_protocol.errors import NotFound
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import SessionFactory, catalog_write_contention, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_authority_finalizer import ArchiveAuthorityFinalizer
from riverhog_core.services.archive_finalizer_progress import ArchiveFinalizerProgress
from riverhog_core.services.archive_finalizer_registry import (
    ArchiveFinalizerRegistry,
    FinalizerSubmission,
    RetainedArchiveFinalizer,
)
from riverhog_core.services.archive_initial_description_finalizer import (
    ArchiveInitialDescriptionFinalizer,
)
from riverhog_core.services.archive_initial_tag_finalizer import ArchiveInitialTagFinalizer
from riverhog_core.services.archive_provenance_authority_finalizer import (
    ArchiveProvenanceAuthorityFinalizer,
)
from riverhog_core.services.archive_provenance_journal_writer import ArchiveProvenanceJournalWriter
from riverhog_core.services.archive_provenance_structure_writer import (
    ArchiveProvenanceStructureWriter,
)
from riverhog_core.services.archive_provenance_volume_finalizer import (
    ArchiveProvenanceVolumeFinalizer,
)
from riverhog_core.services.archive_upload_execution import ArchiveUploadExecutionKey
from riverhog_core.services.archive_volume_metadata_finalizer import ArchiveVolumeMetadataFinalizer
from riverhog_core.services.archive_write_admission import Guard, _writer

MetadataFinalizerKind = Literal[
    "archive-volumes",
    "provenance-volumes",
    "provenance-authority",
    "archive-authority",
    "initial-tags",
    "initial-description",
]


class _MetadataFinalizer(RetainedArchiveFinalizer, Protocol):
    def advance(self) -> bool: ...


class _MetadataFactory(Protocol):
    def __call__(
        self,
        factory: SessionFactory,
        *,
        collection_id: int,
        config: RuntimeConfig,
        archive_stores: ArchiveStoreRegistry,
    ) -> _MetadataFinalizer: ...


_METADATA: dict[MetadataFinalizerKind, _MetadataFactory] = {
    "archive-volumes": ArchiveVolumeMetadataFinalizer,
    "provenance-volumes": ArchiveProvenanceVolumeFinalizer,
    "provenance-authority": ArchiveProvenanceAuthorityFinalizer,
    "archive-authority": ArchiveAuthorityFinalizer,
    "initial-tags": ArchiveInitialTagFinalizer,
    "initial-description": ArchiveInitialDescriptionFinalizer,
}


class ArchiveFinalizerWork:
    """Bounded submission, without a task queue, provider waits or another scheduler.

    The catalog read selects original execution labels, not forward authority.
    The semantic finalizer rechecks its selected source, live permissions and
    write admission in the worker. Pending original continuations take priority
    over every fresh selection, including another kind of finalizer.
    """

    def __init__(
        self,
        factory: SessionFactory,
        registry: ArchiveFinalizerRegistry,
        *,
        config: RuntimeConfig,
        archive_stores: ArchiveStoreRegistry,
    ) -> None:
        self._factory = factory
        self._registry = registry
        self._config = config
        self._archive_stores = archive_stores

    def _submit(
        self,
        collection_id: int,
        selection: str,
        create: Callable[[], RetainedArchiveFinalizer],
        advance: Callable[[RetainedArchiveFinalizer], bool],
        receive: Callable[[bool], None],
        *,
        resume_original: bool = True,
        claim_guard: Guard | None = None,
        register_claim: Guard | None = None,
    ) -> FinalizerSubmission:
        if register_claim is not None and resume_original:
            raise ValueError("claim registration requires a fresh finalizer turn")

        def construct() -> ArchiveFinalizerProgress:
            original = create()
            return ArchiveFinalizerProgress(
                self._factory,
                collection_id=collection_id,
                original=original,
                advance=partial(advance, original),
                receive=receive,
            )

        # Parent/control locks cover only local registration, never a provider
        # wait or semantic continuation. A terminal cannot observe absence and
        # then miss a newly installed constructor. Contention defers submission.
        try:
            with session_scope(self._factory) as session:
                _writer(session, defer_if_busy=True)
                parent = session.scalar(
                    select(CollectionUploadRecord.collection_id)
                    .where(CollectionUploadRecord.collection_id == collection_id)
                    .with_for_update(nowait=True)
                )
                if parent is not None and claim_guard is not None:
                    claim_guard(session)
                control = session.get(
                    ArchiveWriteControlRecord,
                    collection_id,
                    with_for_update={"nowait": True},
                    populate_existing=True,
                )
                if control is None:
                    raise NotFound("archive finalizer construction is unavailable")
                construction = control.construction_json
                key = ArchiveUploadExecutionKey(
                    collection_id, hashlib.sha256(construction.encode()).hexdigest()
                )
                target = json.loads(construction)["archive_incarnation_id"]
                if parent is None or control.gate != "open":
                    if not resume_original:
                        return "closed"
                    return self._registry.resume(key, target_incarnation=target)
                result = self._registry.submit(
                    key,
                    target_incarnation=target,
                    selection=selection,
                    create=construct,
                    advance=lambda owner: cast(ArchiveFinalizerProgress, owner).advance(),
                    resume_original=resume_original,
                )
                if result == "submitted" and register_claim is not None:
                    register_claim(session)
                return result
        except OperationalError as exc:
            if catalog_write_contention(exc):
                return "busy"
            raise

    def submit_metadata(
        self,
        collection_id: int,
        *,
        kind: MetadataFinalizerKind,
        receive: Callable[[bool], None],
    ) -> FinalizerSubmission:
        if kind not in _METADATA:
            raise ValueError("unknown archive metadata finalizer")
        create = partial(
            _METADATA[kind],
            self._factory,
            collection_id=collection_id,
            config=self._config,
            archive_stores=self._archive_stores,
        )
        return self._submit(
            collection_id,
            kind,
            create,
            lambda owner: cast(_MetadataFinalizer, owner).advance(),
            receive,
        )

    def submit_structure(
        self,
        collection_id: int,
        *,
        forward_guard: Guard,
        selected_object_id: str | None = None,
        receive: Callable[[bool], None],
    ) -> FinalizerSubmission:
        create = partial(
            ArchiveProvenanceStructureWriter,
            self._factory,
            collection_id=collection_id,
            config=self._config,
            archive_stores=self._archive_stores,
            forward_guard=forward_guard,
        )
        return self._submit(
            collection_id,
            "provenance-structure",
            create,
            lambda owner: cast(ArchiveProvenanceStructureWriter, owner).advance(
                selected_object_id=selected_object_id
            ),
            receive,
        )

    def submit_journal(
        self,
        collection_id: int,
        *,
        forward_guard: Guard,
        journal_id: str,
        offset: int,
        size: int,
        receive: Callable[[bool], None],
    ) -> FinalizerSubmission:
        create = partial(
            ArchiveProvenanceJournalWriter,
            self._factory,
            collection_id=collection_id,
            config=self._config,
            archive_stores=self._archive_stores,
            forward_guard=forward_guard,
        )
        return self._submit(
            collection_id,
            "provenance-journal",
            create,
            lambda owner: cast(ArchiveProvenanceJournalWriter, owner).advance(
                journal_id=journal_id, offset=offset, size=size
            ),
            receive,
        )
