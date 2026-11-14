"""Compose configured native cache writes behind the original payload family."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable

from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteControlRecord,
    ArchiveWriteEffectRecord,
)
from riverhog_core.catalog_cache_write_models import CachePopulationBeginRecord
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadRecord,
    RetrievalCachePopulationRecord,
)
from riverhog_core.runtime_config import RetrievalCacheStoreRegistration
from riverhog_core.services.archive_payload_input import ArchivePayloadRequest
from riverhog_core.services.archive_upload_writes import ArchiveUploadWrites
from riverhog_core.services.archive_write_admission import Guard
from riverhog_core.services.cache_population_begin import CachePopulationBeginAdmission
from riverhog_core.services.cache_population_writes import CachePopulationWriteAdmission
from riverhog_core.services.mirrored_archive_begin import MirroredArchiveBegin
from riverhog_core.services.mirrored_archive_upload_writes import MirroredArchiveUploadWrites
from riverhog_core.stores.storage_adapter_retrieval_cache import StorageAdapterRetrievalCache

type CacheUploadCandidates = Callable[
    [], tuple[tuple[StorageAdapterRetrievalCache, RetrievalCacheStoreRegistration], ...]
]


class ArchivePayloadCache:
    """Bind optional population to one current original construction and volume."""

    def __init__(
        self,
        request: ArchivePayloadRequest,
        *,
        source_store: str,
        candidates: CacheUploadCandidates,
        grants: CacheWriteGrants,
        session_factory: SessionFactory,
    ) -> None:
        self._request = request
        self._source_store = source_store
        self._candidates = candidates
        self._grants = grants
        self._factory = session_factory
        self._owner = f"collection-upload:{request.key.collection_id}:{request.volume_id}"

    def create(self, archive: ArchiveUploadWrites, guard: Guard) -> MirroredArchiveUploadWrites:
        """Called only after the original family has retained its parent pin."""
        request = self._request
        begin = MirroredArchiveBegin(
            self._factory,
            archive=archive,
            cache=CachePopulationBeginAdmission(self._factory),
            candidates=self._candidates(),
            owner=self._owner,
            principal=request.principal,
            grants=self._grants,
            guard=guard,
            validate_population=self._validate_begin,
        )
        return MirroredArchiveUploadWrites(
            archive=archive,
            begin=begin,
            cache=CachePopulationWriteAdmission(self._factory),
            source_store=self._source_store,
            owner=self._owner,
            principal=request.principal,
            grants=self._grants,
            guard=guard,
            validate_population=self.validate_population,
        )

    def _scope(
        self, session: Session, upload: CollectionUploadRecord, object_id: str
    ) -> tuple[ArchiveWriteControlRecord, CollectionArchiveObjectUploadRecord]:
        request = self._request
        control = session.get(ArchiveWriteControlRecord, request.key.collection_id)
        volume = session.get(
            CollectionArchiveObjectUploadRecord,
            (request.key.collection_id, object_id),
            with_for_update=True,
            populate_existing=True,
        )
        if (
            upload.collection_id != request.key.collection_id
            or upload.archive_store != self._source_store
            or upload.archive_incarnation_id != request.target_incarnation
            or not upload.use_cache
            or object_id != request.volume_id
            or control is None
            or control.epoch != request.epoch
            or control.gate != "open"
            or hashlib.sha256(control.construction_json.encode()).hexdigest()
            != request.key.scope_sha256
            or volume is None
            or volume.plan_sha256 != request.plan_sha256
        ):
            raise Conflict("cache population original payload scope changed")
        return control, volume

    def _validate_begin(
        self,
        session: Session,
        upload: CollectionUploadRecord,
        object_id: str,
        expected_bytes: int,
    ) -> None:
        _, volume = self._scope(session, upload, object_id)
        effects = list(
            session.scalars(
                select(ArchiveWriteEffectRecord)
                .where(
                    ArchiveWriteEffectRecord.collection_id == upload.collection_id,
                    ArchiveWriteEffectRecord.object_id == object_id,
                    ArchiveWriteEffectRecord.epoch == self._request.epoch,
                    ArchiveWriteEffectRecord.kind == "begin",
                    ArchiveWriteEffectRecord.state == "armed",
                )
                .with_for_update()
                .limit(2)
            )
        )
        if len(effects) != 1:
            raise Conflict("cache population has no unique original archive begin")
        effect = effects[0]
        if hashlib.sha256(effect.request_json.encode()).hexdigest() != effect.request_sha256:
            raise Conflict("cache population original archive begin commitment changed")
        original = json.loads(effect.request_json)["arguments"]["operation"]["request"]
        if (
            original["object_path"] != volume.object_path
            or int(original["expected_bytes"]) != expected_bytes
        ):
            raise Conflict("cache population differs from the original archive begin")

    def validate_population(
        self,
        session: Session,
        upload: CollectionUploadRecord,
        object_id: str,
        expected_bytes: int,
    ) -> None:
        control, _ = self._scope(session, upload, object_id)
        population = session.get(
            RetrievalCachePopulationRecord, (self._source_store, upload.collection_id, object_id)
        )
        begin = (
            session.get(CachePopulationBeginRecord, population.begin_call_id)
            if population is not None and population.begin_call_id is not None
            else None
        )
        if (
            begin is None
            or begin.source_store != self._source_store
            or begin.collection_id != upload.collection_id
            or begin.object_id != object_id
            or hashlib.sha256(begin.request_json.encode()).hexdigest() != begin.request_sha256
        ):
            raise Conflict("cache population original begin is unavailable")
        original = json.loads(begin.request_json)
        if (
            original["construction"] != json.loads(control.construction_json)
            or original["epoch"] != self._request.epoch
            or original["owner"] != self._owner
            or original["source_incarnation_id"] != self._request.target_incarnation
            or int(original["operation"]["request"]["expected_bytes"]) != expected_bytes
        ):
            raise Conflict("cache population differs from its original payload family")
