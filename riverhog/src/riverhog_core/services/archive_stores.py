from __future__ import annotations

from collections.abc import Iterator

from http_api_contracts import closed_literal_values
from riverhog_protocol import ArchiveStoreSort, SortOrder
from riverhog_protocol.errors import BadRequest, NotFound, ServiceUnavailable
from sqlalchemy import func, literal, select, union_all
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.app_permissions import ARCHIVES_READ, Principal
from riverhog_core.archive_store_registry import ArchiveStoreBinding, ArchiveStoreRegistry
from riverhog_core.browse import bounded_page, validate_page_size
from riverhog_core.catalog_db import SessionFactory, make_session_factory
from riverhog_core.catalog_models import (
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    CollectionDescriptionPublicationRecord,
    CollectionTagPublicationRecord,
    CollectionTagPublishedNodeRecord,
    StorageIncarnationRecord,
)
from riverhog_core.collection_access import collection_access_filter
from riverhog_core.domain.models import (
    ArchiveDownloadAllowance,
    ArchiveStoreListPage,
    ArchiveStoreSummary,
)
from riverhog_core.ports.download_allowance import DownloadAllowance
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.download_allowances import SqlAlchemyDownloadAllowance

_SORT_FIELDS = closed_literal_values(ArchiveStoreSort)
_SORT_ORDERS = closed_literal_values(SortOrder)


class SqlAlchemyArchiveStoreService:
    def __init__(
        self,
        config: RuntimeConfig,
        archive_stores: ArchiveStoreRegistry,
        *,
        download_allowance: DownloadAllowance | None = None,
        session_factory: SessionFactory | None = None,
    ) -> None:
        self._config = config
        self._archive_stores = archive_stores
        self._session_factory = session_factory or make_session_factory(config.database_url)
        self._download_allowance = download_allowance or SqlAlchemyDownloadAllowance(
            config,
            session_factory=self._session_factory,
        )
        self._read_priorities = {
            name: priority for priority, name in enumerate(config.archive_read_order, start=1)
        }

    def get(
        self,
        store: str,
        *,
        principal: Principal | None = None,
    ) -> ArchiveStoreSummary:
        normalized = store.strip().casefold()
        with read_snapshot(self._session_factory) as session:
            record = session.scalar(
                select(StorageIncarnationRecord).where(
                    StorageIncarnationRecord.kind == "archive",
                    StorageIncarnationRecord.name == normalized,
                )
            )
            if record is None and normalized not in self._config.archive_stores:
                raise NotFound(f"archive store not found: {normalized}")
            aggregates = _store_aggregates(
                session,
                stores=(normalized,),
                principal=principal,
            )
        return self._summary(
            normalized,
            record=record,
            binding=self._usable_binding(normalized),
            aggregate=aggregates.get(normalized, (0, 0, 0)),
            allowances=self._allowances(),
        )

    def list(
        self,
        *,
        page_size: int,
        position: tuple[str | int | bool | bytes | None, ...] | None,
        q: str | None,
        sort: str,
        order: str,
        principal: Principal | None = None,
    ) -> ArchiveStoreListPage:
        validate_page_size(page_size)
        if sort not in _SORT_FIELDS:
            raise BadRequest(f"sort must be one of {', '.join(sorted(_SORT_FIELDS))}")
        if order not in _SORT_ORDERS:
            raise BadRequest("order must be asc or desc")

        needle = q.strip().casefold() if q and q.strip() else None
        with read_snapshot(self._session_factory) as session:
            records = {
                row.name: row
                for row in session.scalars(
                    select(StorageIncarnationRecord).where(
                        StorageIncarnationRecord.kind == "archive"
                    )
                )
            }
            names = tuple(dict.fromkeys((*self._config.archive_stores, *records)))
            aggregates = _store_aggregates(
                session,
                stores=names,
                principal=principal,
            )
        allowances = self._allowances()
        summaries = [
            self._summary(
                name,
                record=records.get(name),
                binding=self._usable_binding(name),
                aggregate=aggregates.get(name, (0, 0, 0)),
                allowances=allowances,
            )
            for name in names
        ]
        if needle is not None:
            summaries = [
                current
                for current in summaries
                if needle in f"{current.store} {current.read_mode or ''}".casefold()
            ]
        summaries.sort(
            key=lambda current: _archive_store_position(current, sort=sort), reverse=order == "desc"
        )
        if position is not None:
            if len(position) != 2:
                raise BadRequest("page token position is invalid")
            summaries = [
                current
                for current in summaries
                if (
                    _archive_store_position(current, sort=sort) > position
                    if order == "asc"
                    else _archive_store_position(current, sort=sort) < position
                )
            ]
        selected, next_position = bounded_page(
            summaries,
            page_size=page_size,
            position_of=lambda current: _archive_store_position(current, sort=sort),
        )
        return ArchiveStoreListPage(
            page_size=page_size,
            next_position=next_position,
            sort=sort,
            order=order,
            query=needle,
            stores=selected,
        )

    def iter_stores(
        self,
        *,
        q: str | None,
        sort: str,
        order: str,
        principal: Principal | None = None,
    ) -> Iterator[ArchiveStoreSummary]:
        position = None
        while True:
            page = self.list(
                page_size=100,
                position=position,
                q=q,
                sort=sort,
                order=order,
                principal=principal,
            )
            yield from page.stores
            if page.next_position is None:
                return
            position = page.next_position

    def _allowances(self) -> dict[str, ArchiveDownloadAllowance]:
        return {current.store: current for current in self._download_allowance.get_statuses()}

    def _summary(
        self,
        name: str,
        *,
        record: StorageIncarnationRecord | None,
        binding: ArchiveStoreBinding | None,
        aggregate: tuple[int, int, int],
        allowances: dict[str, ArchiveDownloadAllowance],
    ) -> ArchiveStoreSummary:
        collections, objects, stored_bytes = aggregate
        configured = name in self._config.archive_stores
        reachable = binding is not None
        admitted = reachable and record is not None and record.state == "bound"
        return ArchiveStoreSummary(
            store=name,
            incarnation_id=record.id if record is not None else None,
            administrative_state=record.state if record is not None else None,
            configured=configured,
            reachable=reachable,
            readable=admitted,
            writable=admitted,
            read_mode=(
                binding.store.read_mode()
                if binding is not None
                else record.last_read_mode
                if record is not None
                else None
            ),
            read_priority=self._read_priorities.get(name, len(self._read_priorities) + 1),
            write_target=configured and name == self._config.archive_write_store,
            collections=collections,
            objects=objects,
            stored_bytes=stored_bytes,
            download_allowance=allowances.get(name),
        )

    def _usable_binding(self, name: str) -> ArchiveStoreBinding | None:
        if name not in self._config.archive_stores:
            return None
        try:
            return self._archive_stores.require(name)
        except (ServiceUnavailable, ValueError):
            return None


def _archive_store_position(
    summary: ArchiveStoreSummary,
    *,
    sort: str,
) -> tuple[str | int, str]:
    value = getattr(summary, sort)
    if value is None:
        value = ""
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        raise RuntimeError("archive-store browse position has an invalid value")
    return value, summary.store


def _store_aggregates(
    session: Session,
    *,
    stores: tuple[str, ...],
    principal: Principal | None,
) -> dict[str, tuple[int, int, int]]:
    if not stores:
        return {}
    copies = (
        select(
            CollectionArchiveCopyRecord.collection_id.label("collection_id"),
            CollectionArchiveCopyRecord.store.label("store"),
        )
        .where(
            CollectionArchiveCopyRecord.store.in_(stores),
            collection_access_filter(
                CollectionArchiveCopyRecord.collection_id,
                principal,
                ARCHIVES_READ,
            ),
        )
        .subquery()
    )
    immutable = select(
        CollectionArchiveObjectRecord.collection_id.label("collection_id"),
        CollectionArchiveObjectRecord.store.label("store"),
        literal(1).label("objects"),
        CollectionArchiveObjectRecord.stored_bytes.label("stored_bytes"),
    )
    descriptions = select(
        CollectionDescriptionPublicationRecord.collection_id.label("collection_id"),
        CollectionDescriptionPublicationRecord.store.label("store"),
        literal(1).label("objects"),
        func.coalesce(CollectionDescriptionPublicationRecord.stored_bytes, 0).label("stored_bytes"),
    ).where(CollectionDescriptionPublicationRecord.object_path.is_not(None))
    tag_heads = select(
        CollectionTagPublicationRecord.collection_id.label("collection_id"),
        CollectionTagPublicationRecord.store.label("store"),
        literal(1).label("objects"),
        func.coalesce(CollectionTagPublicationRecord.head_stored_bytes, 0).label("stored_bytes"),
    ).where(CollectionTagPublicationRecord.head_object_path.is_not(None))
    tag_nodes = select(
        CollectionTagPublishedNodeRecord.collection_id.label("collection_id"),
        CollectionTagPublishedNodeRecord.store.label("store"),
        literal(1).label("objects"),
        CollectionTagPublishedNodeRecord.stored_bytes.label("stored_bytes"),
    )
    owned = union_all(immutable, descriptions, tag_heads, tag_nodes).subquery()
    rows = session.execute(
        select(
            copies.c.store,
            func.count(func.distinct(copies.c.collection_id)),
            func.coalesce(func.sum(owned.c.objects), 0),
            func.coalesce(func.sum(owned.c.stored_bytes), 0),
        )
        .outerjoin(
            owned,
            (owned.c.collection_id == copies.c.collection_id) & (owned.c.store == copies.c.store),
        )
        .group_by(copies.c.store)
    )
    return {
        str(store): (int(collections), int(objects), int(stored_bytes))
        for store, collections, objects, stored_bytes in rows
    }
