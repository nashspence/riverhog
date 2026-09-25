from __future__ import annotations

from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass, field
from functools import lru_cache, partial
from threading import RLock
from typing import Annotated

from fastapi import Depends
from http_api_contracts.browse import BrowseTokenCodec
from riverhog_archive_contracts import ARCHIVE_ENCRYPTION_FORMAT
from riverhog_core.archive_store_registry import ArchiveStoreBinding, ArchiveStoreRegistry
from riverhog_core.catalog_db import (
    SessionFactory,
    dispose_session_factory,
    make_session_factory,
    session_scope,
    validate_db,
)
from riverhog_core.catalog_models import (
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    CollectionRecord,
    CollectionUploadRecord,
)
from riverhog_core.collection_access import SqlAlchemyCollectionAccessService
from riverhog_core.ports.download_allowance import DownloadAllowance
from riverhog_core.runtime_config import (
    RuntimeConfig,
    StorageAdapterRegistration,
)
from riverhog_core.runtime_document import load_runtime_config
from riverhog_core.services.app_keys import SqlAlchemyAppKeyService
from riverhog_core.services.archive_copy_jobs import SqlAlchemyArchiveCopyJobService
from riverhog_core.services.archive_copy_retirements import (
    SqlAlchemyArchiveCopyRetirementService,
)
from riverhog_core.services.archive_stores import SqlAlchemyArchiveStoreService
from riverhog_core.services.catalog_sync import SqlAlchemyCatalogSyncService
from riverhog_core.services.collection_deletions import SqlAlchemyCollectionDeletionService
from riverhog_core.services.collection_descriptions import SqlAlchemyCollectionDescriptionService
from riverhog_core.services.collection_tags import SqlAlchemyCollectionTagService
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_core.services.collection_workflows import SqlAlchemyCollectionWorkflowService
from riverhog_core.services.collections import SqlAlchemyCollectionService
from riverhog_core.services.download_allowances import SqlAlchemyDownloadAllowance
from riverhog_core.services.interfaces import (
    AppKeyService,
    ArchiveCopyJobService,
    ArchiveCopyRetirementService,
    ArchiveStoreService,
    CatalogSyncService,
    CollectionDeletionService,
    CollectionDescriptionService,
    CollectionService,
    CollectionTagService,
    LifecycleEventService,
    ProvenanceService,
    RetrievalService,
    SearchService,
)
from riverhog_core.services.lifecycle_events import SqlAlchemyLifecycleEventService
from riverhog_core.services.provenance import SqlAlchemyProvenanceService
from riverhog_core.services.retrieval import SqlAlchemyRetrievalService
from riverhog_core.services.retrieval_cache import SqlAlchemyRetrievalCache
from riverhog_core.services.search import SqlAlchemySearchService
from riverhog_core.storage_incarnations import (
    StorageBindingConflict,
    StorageName,
    reconcile_storage_incarnations,
)
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterArchiveObjectRangeStore,
    StorageAdapterArchiveResumableObjectStore,
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_core.stores.storage_adapter_archive_store import StorageAdapterArchiveStore
from riverhog_core.stores.storage_adapter_retrieval_cache import StorageAdapterRetrievalCache
from riverhog_core.throughput import ArchiveTransferResources
from riverhog_storage_adapter_protocol import StorageAdapterRejection, validated_storage_adapter
from riverhog_storage_adapter_support import StorageAdapterClient
from sqlalchemy import select


@dataclass(slots=True)
class ServiceContainer:
    app_keys: AppKeyService
    collection_access: SqlAlchemyCollectionAccessService
    collection_tags: CollectionTagService
    collections: CollectionService
    collection_descriptions: CollectionDescriptionService
    collection_uploads: SqlAlchemyCollectionUploadService
    collection_workflows: SqlAlchemyCollectionWorkflowService
    provenance: ProvenanceService
    collection_deletions: CollectionDeletionService
    catalog_sync: CatalogSyncService
    search: SearchService
    archive_copy_jobs: ArchiveCopyJobService
    archive_copy_retirements: ArchiveCopyRetirementService
    archive_stores: ArchiveStoreService
    retrieval: RetrievalService
    lifecycle_events: LifecycleEventService
    download_quotas: DownloadAllowance
    session_factory: SessionFactory
    browse_tokens: BrowseTokenCodec
    storage_adapter_clients: list[StorageAdapterClient] = field(default_factory=list)
    storage_readiness: Callable[[], None] | None = None
    bootstrap_token: str = field(default="", repr=False)

    def close(self) -> None:
        dispose_session_factory(self.session_factory)
        for client in self.storage_adapter_clients:
            client.close()


def _archive_store_registry(
    config: RuntimeConfig,
    *,
    adapters: dict[str, StorageAdapterClient],
    download_allowance: DownloadAllowance,
    unavailable: dict[str, str] | None = None,
) -> ArchiveStoreRegistry:
    return ArchiveStoreRegistry(
        {
            name: _archive_store_binding(config, name, adapter, download_allowance)
            for name, adapter in adapters.items()
        },
        unavailable=unavailable,
        probes={
            name: partial(_probe_storage_adapter, client)
            for name, client in adapters.items()
            if isinstance(client, StorageAdapterClient)
        },
    )


def _archive_store_binding(
    config: RuntimeConfig,
    name: str,
    client: StorageAdapterClient,
    download_allowance: DownloadAllowance,
) -> ArchiveStoreBinding:
    adapter = validated_storage_adapter(client)
    return ArchiveStoreBinding(
        incarnation_id=adapter.descriptor().storage_incarnation_id,
        store=StorageAdapterArchiveStore(
            config,
            name=name,
            adapter=adapter,
            download_allowance=download_allowance,
        ),
        resumable_objects=StorageAdapterArchiveResumableObjectStore(adapter),
        immutable_objects=StorageAdapterImmutableArchiveObjectStore(adapter),
        object_ranges=StorageAdapterArchiveObjectRangeStore(adapter),
    )


def _probe_storage_adapter(client: StorageAdapterClient) -> None:
    client.check_readiness()
    client.refresh_descriptor()


def _adapter_client(registration: StorageAdapterRegistration) -> StorageAdapterClient:
    return StorageAdapterClient.from_token_file(
        registration.base_url,
        token_file=registration.token_file,
        allow_insecure_http=registration.allow_insecure_http,
        timeout=registration.timeout_seconds,
        maximum_connections=registration.maximum_connections,
    )


def _build_default_container(
    config: RuntimeConfig,
    *,
    session_factory: SessionFactory,
    startup_cleanup: ExitStack,
) -> ServiceContainer:
    _require_archive_encryption_bindings(config, session_factory=session_factory)
    throughput_tuning = config.throughput_tuning
    transfer_resources = ArchiveTransferResources.from_tuning(throughput_tuning)
    adapters: dict[str, StorageAdapterClient] = {}
    archive_clients: dict[str, StorageAdapterClient] = {}
    all_clients: list[StorageAdapterClient] = []
    observations: dict[StorageName, str | None] = {}
    read_modes: dict[StorageName, str] = {}
    unavailable: dict[str, str] = {}
    for name, store in config.archive_stores.items():
        client = _adapter_client(store)
        startup_cleanup.callback(client.close)
        all_clients.append(client)
        archive_clients[name] = client
        try:
            client.check_readiness()
            descriptor = client.refresh_descriptor()
            observations[("archive", name)] = descriptor.storage_incarnation_id
            read_modes[("archive", name)] = descriptor.read_mode
            adapters[name] = client
        except StorageAdapterRejection:
            observations[("archive", name)] = None
            unavailable[name] = "adapter unavailable"
    restore_required = [
        name
        for name, client in adapters.items()
        if client.descriptor().read_mode == "restore_required"
    ]
    if restore_required and not config.retrieval_cache_stores:
        raise ValueError(
            "restore-required archive adapters require a retrieval cache adapter: "
            + ", ".join(restore_required)
        )
    cache_clients: dict[str, StorageAdapterClient] = {}
    configured_cache_clients: dict[str, StorageAdapterClient] = {}
    cache_stores: dict[str, StorageAdapterRetrievalCache] = {}
    for name, registration in config.retrieval_cache_stores.items():
        client = _adapter_client(registration.adapter)
        startup_cleanup.callback(client.close)
        all_clients.append(client)
        configured_cache_clients[name] = client
        try:
            client.check_readiness()
            descriptor = client.refresh_descriptor()
            observations[("cache", name)] = descriptor.storage_incarnation_id
            read_modes[("cache", name)] = descriptor.read_mode
        except StorageAdapterRejection:
            observations[("cache", name)] = None
            continue
        cache_clients[name] = client
    reservations = reconcile_storage_incarnations(
        session_factory, observations, read_modes=read_modes
    )
    for name, client in adapters.items():
        client.pin_incarnation(reservations[("archive", name)])
    for name, client in cache_clients.items():
        client.pin_incarnation(reservations[("cache", name)])
    for name, client in cache_clients.items():
        cache_stores[name] = StorageAdapterRetrievalCache(
            name,
            client,
            write_segment_bytes=config.retrieval_cache_write_segment_bytes,
            throughput_tuning=throughput_tuning,
            transfer_resources=transfer_resources,
        )
    retrieval_cache = (
        SqlAlchemyRetrievalCache(
            cache_stores,
            {name: config.retrieval_cache_stores[name] for name in cache_stores},
            session_factory=session_factory,
        )
        if config.retrieval_cache_stores
        else None
    )
    download_allowance = SqlAlchemyDownloadAllowance(
        config,
        session_factory=session_factory,
    )
    archive_stores = _archive_store_registry(
        config,
        adapters=adapters,
        download_allowance=download_allowance,
        unavailable=unavailable,
    )

    recovery_lock = RLock()

    def _replace_rejected_client(kind: str, name: str) -> None:
        clients = archive_clients if kind == "archive" else configured_cache_clients
        old_client = clients[name]
        replacement = _adapter_client(
            config.archive_stores[name]
            if kind == "archive"
            else config.retrieval_cache_stores[name].adapter
        )
        clients[name] = replacement
        all_clients.remove(old_client)
        all_clients.append(replacement)
        old_client.close()

    def _recover(kind: str, name: str | None = None) -> None:
        with recovery_lock:
            clients = archive_clients if kind == "archive" else configured_cache_clients
            admitted = adapters if kind == "archive" else cache_clients
            for candidate in (name,) if name is not None else tuple(clients):
                if candidate not in clients or candidate in admitted:
                    continue
                client = clients[candidate]
                try:
                    client.check_readiness()
                    descriptor = client.refresh_descriptor()
                    if (
                        kind == "archive"
                        and descriptor.read_mode == "restore_required"
                        and not config.retrieval_cache_stores
                    ):
                        continue
                    if kind == "archive":
                        archive_binding = _archive_store_binding(
                            config, candidate, client, download_allowance
                        )
                    else:
                        cache_binding = StorageAdapterRetrievalCache(
                            candidate,
                            client,
                            write_segment_bytes=config.retrieval_cache_write_segment_bytes,
                            throughput_tuning=throughput_tuning,
                            transfer_resources=transfer_resources,
                        )
                    key: StorageName = (kind, candidate)  # type: ignore[assignment]
                    candidate_observations = {
                        **observations,
                        key: descriptor.storage_incarnation_id,
                    }
                    candidate_read_modes = {**read_modes, key: descriptor.read_mode}
                    reservations = reconcile_storage_incarnations(
                        session_factory,
                        candidate_observations,
                        read_modes=candidate_read_modes,
                    )
                    client.pin_incarnation(reservations[key])
                except StorageBindingConflict:
                    _replace_rejected_client(kind, candidate)
                    continue
                except StorageAdapterRejection:
                    continue
                except ValueError:
                    continue
                if kind == "archive":
                    archive_stores.admit(
                        candidate,
                        archive_binding,
                        probe=partial(_probe_storage_adapter, client),
                    )
                    adapters[candidate] = client
                else:
                    assert retrieval_cache is not None
                    retrieval_cache.admit_store(
                        candidate,
                        cache_binding,
                        config.retrieval_cache_stores[candidate],
                    )
                    cache_clients[candidate] = client
                observations[key] = descriptor.storage_incarnation_id
                read_modes[key] = descriptor.read_mode

    archive_stores.set_recovery(lambda name: _recover("archive", name))
    if retrieval_cache is not None:
        retrieval_cache.set_recovery(lambda: _recover("cache"))

    def check_storage_readiness() -> None:
        _recover("archive")
        _recover("cache")
        archive_stores.require(config.archive_write_store)
        usable_names = set(archive_stores.names)
        readable = [
            archive_stores.require(name)
            for name in config.archive_read_order
            if name in usable_names
        ]
        if not readable:
            raise RuntimeError("no verified archive read binding is available")
        if all(binding.store.read_mode() == "restore_required" for binding in readable):
            for client in cache_clients.values():
                try:
                    client.check_readiness()
                    client.refresh_descriptor()
                except StorageAdapterRejection:
                    continue
                break
            else:
                raise RuntimeError("no verified retrieval cache is available")

    return ServiceContainer(
        app_keys=SqlAlchemyAppKeyService(config, session_factory=session_factory),
        collection_access=SqlAlchemyCollectionAccessService(
            config,
            session_factory=session_factory,
        ),
        collection_tags=SqlAlchemyCollectionTagService(
            config, archive_stores, session_factory=session_factory
        ),
        collections=SqlAlchemyCollectionService(config, session_factory=session_factory),
        collection_descriptions=SqlAlchemyCollectionDescriptionService(
            config,
            archive_stores,
            session_factory=session_factory,
        ),
        collection_uploads=SqlAlchemyCollectionUploadService(
            config,
            archive_stores,
            retrieval_cache=retrieval_cache,
            session_factory=session_factory,
            throughput_tuning=throughput_tuning,
            policy=config.volume_policy,
            transfer_resources=transfer_resources,
        ),
        collection_workflows=SqlAlchemyCollectionWorkflowService(
            config, session_factory=session_factory
        ),
        provenance=SqlAlchemyProvenanceService(config, session_factory=session_factory),
        collection_deletions=SqlAlchemyCollectionDeletionService(
            config,
            archive_stores,
            retrieval_cache=retrieval_cache,
            session_factory=session_factory,
        ),
        catalog_sync=SqlAlchemyCatalogSyncService(
            config,
            session_factory=session_factory,
        ),
        search=SqlAlchemySearchService(config, session_factory=session_factory),
        archive_copy_jobs=SqlAlchemyArchiveCopyJobService(
            config,
            archive_stores,
            retrieval_cache=retrieval_cache,
            session_factory=session_factory,
            throughput_tuning=throughput_tuning,
            transfer_resources=transfer_resources,
        ),
        archive_copy_retirements=SqlAlchemyArchiveCopyRetirementService(
            config,
            archive_stores,
            session_factory=session_factory,
        ),
        archive_stores=SqlAlchemyArchiveStoreService(
            config,
            archive_stores,
            download_allowance=download_allowance,
            session_factory=session_factory,
        ),
        retrieval=SqlAlchemyRetrievalService(
            config,
            archive_stores,
            retrieval_cache,
            download_allowance=download_allowance,
            session_factory=session_factory,
            throughput_tuning=throughput_tuning,
            transfer_resources=transfer_resources,
        ),
        lifecycle_events=SqlAlchemyLifecycleEventService(
            config,
            session_factory=session_factory,
        ),
        download_quotas=download_allowance,
        session_factory=session_factory,
        browse_tokens=BrowseTokenCodec(
            config.browse_token_signing_key,
            lifetime_seconds=int(config.browse_token_lifetime.total_seconds()),
        ),
        storage_adapter_clients=all_clients,
        storage_readiness=check_storage_readiness,
        bootstrap_token=config.bootstrap_token,
    )


def _require_archive_encryption_bindings(
    config: RuntimeConfig,
    *,
    session_factory: SessionFactory,
) -> None:
    with session_scope(session_factory) as session:
        bindings = set(
            session.query(CollectionRecord.encryption_format, CollectionRecord.passphrase_id)
        ) | set(
            session.query(
                CollectionUploadRecord.encryption_format,
                CollectionUploadRecord.passphrase_id,
            )
        )
        descriptor_exists = (
            select(CollectionArchiveObjectRecord.object_id)
            .where(
                CollectionArchiveObjectRecord.collection_id
                == CollectionArchiveCopyRecord.collection_id,
                CollectionArchiveObjectRecord.store == CollectionArchiveCopyRecord.store,
                CollectionArchiveObjectRecord.kind == "recovery-descriptor",
            )
            .exists()
        )
        missing_descriptor = session.execute(
            select(
                CollectionArchiveCopyRecord.collection_id,
                CollectionArchiveCopyRecord.store,
            )
            .where(
                CollectionArchiveCopyRecord.state == "uploaded",
                ~descriptor_exists,
            )
            .limit(1)
        ).first()
    for encryption_format, passphrase_id in bindings:
        if encryption_format != ARCHIVE_ENCRYPTION_FORMAT:
            raise ValueError(
                f"unsupported persisted archive encryption format: {encryption_format}"
            )
        config.archive_passphrase_for(passphrase_id)
    if missing_descriptor is not None:
        collection_id, store = missing_descriptor
        raise ValueError(
            "uploaded archive copy has no recovery descriptor: "
            f"collection={collection_id} store={store}"
        )


@lru_cache(maxsize=1)
def default_container() -> ServiceContainer:
    config = load_runtime_config()
    validate_db(config.database_url)
    session_factory = make_session_factory(config.database_url)
    startup_cleanup = ExitStack()
    startup_cleanup.callback(dispose_session_factory, session_factory)
    try:
        container = _build_default_container(
            config,
            session_factory=session_factory,
            startup_cleanup=startup_cleanup,
        )
    except BaseException:
        startup_cleanup.close()
        raise
    startup_cleanup.pop_all()
    return container


def get_container() -> ServiceContainer:
    return default_container()


ContainerDep = Annotated[ServiceContainer, Depends(get_container)]
