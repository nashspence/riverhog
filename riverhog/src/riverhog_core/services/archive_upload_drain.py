"""Bounded catalog facts for an exact selected upload terminal request."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
    ArchiveWriteEffectRecord,
)
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, _writer

EffectModel = (
    type[ArchiveWriteEffectRecord]
    | type[CachePopulationBeginRecord]
    | type[CachePopulationWriteEffectRecord]
)


def _record_query(model: EffectModel, collection_id: int, *, unclosed: bool) -> Select[tuple[str]]:
    query = select(model.id).where(model.collection_id == collection_id)
    if unclosed:
        query = query.where(model.unresolved.is_(True))
    return query.limit(1)


@dataclass(frozen=True, slots=True)
class TerminalWriteRecords:
    """Catalog records alone grant no cleanup, execution closure or slot release.

    Settled reports can still retain receiving/accounting obligations. Live
    dispatchers, retained actors, buffers and cleanup ownership have independent
    predicates even when every unclosed-record field is false.
    """

    collection_id: int
    operation_id: str
    archive_unclosed: bool
    cache_begin_unclosed: bool
    cache_write_unclosed: bool
    archive_retained: bool
    cache_begin_retained: bool
    cache_write_retained: bool


def inspect_terminal_write_records(
    session: Session, collection_id: int, operation_id: str
) -> TerminalWriteRecords:
    """Observe indexed owner facts under the closed construction/terminal lock.

    This does not adopt forward admission's archive-only predicate: an uncertain
    optional cache effect must remain visible to terminal drainage independently
    of whether archive preservation can continue.
    """
    _writer(session)
    upload = session.scalar(
        ArchiveWriteAdmission._upload_query(collection_id).execution_options(populate_existing=True)
    )
    if upload is None:
        raise NotFound("upload terminal construction is unavailable")
    session.get(
        ArchiveWriteControlRecord,
        collection_id,
        with_for_update=True,
        populate_existing=True,
    )
    control = ArchiveWriteAdmission._control(session, upload)
    if control.gate != "closed" or control.terminal_id != operation_id:
        raise Conflict("upload terminal selection is not current")
    terminal = session.get(
        ArchiveUploadTerminalRecord,
        operation_id,
        with_for_update=True,
        populate_existing=True,
    )
    if terminal is None:
        raise NotFound("upload terminal original request is unavailable")
    try:
        value = json.loads(terminal.request_json)
        if (
            terminal.collection_id != collection_id
            or terminal.id != hashlib.sha256(terminal.request_json.encode()).hexdigest()
            or canonical_json_bytes(value).decode() != terminal.request_json
            or set(value) != {"construction", "kind", "principal_id", "request"}
            or canonical_json_bytes(value["construction"]).decode() != control.construction_json
            or value["kind"] != terminal.kind
            or value["principal_id"] != terminal.principal_id
            or control.next_call != terminal.frontier
            or control.epoch == terminal.original_epoch
        ):
            raise Conflict("upload terminal original selection changed")
    except (KeyError, TypeError, ValueError) as exc:
        raise Conflict("upload terminal original request is invalid") from exc

    def retained(model: EffectModel, *, unclosed: bool) -> bool:
        return session.scalar(_record_query(model, collection_id, unclosed=unclosed)) is not None

    return TerminalWriteRecords(
        collection_id=collection_id,
        operation_id=operation_id,
        archive_unclosed=retained(ArchiveWriteEffectRecord, unclosed=True),
        cache_begin_unclosed=retained(CachePopulationBeginRecord, unclosed=True),
        cache_write_unclosed=retained(CachePopulationWriteEffectRecord, unclosed=True),
        archive_retained=retained(ArchiveWriteEffectRecord, unclosed=False),
        cache_begin_retained=retained(CachePopulationBeginRecord, unclosed=False),
        cache_write_retained=retained(CachePopulationWriteEffectRecord, unclosed=False),
    )
