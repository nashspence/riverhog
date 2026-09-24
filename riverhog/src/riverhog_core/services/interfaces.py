from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import timedelta
from typing import Protocol

from riverhog_protocol import (
    CatalogSyncChangePage,
    CatalogSyncCheckpoint,
    CatalogSyncCollectionPage,
    PortableCollectionFile,
    PortableCollectionHeader,
    PortableCollectionInventoryPage,
)
from riverhog_protocol.lifecycle_events import RiverhogEventPage

from riverhog_core.app_permissions import ApplicationAccess, Principal
from riverhog_core.domain.models import (
    ArchiveStoreListPage,
    ArchiveStoreSummary,
    CollectionListPage,
    CollectionSummary,
)

JsonObject = dict[str, object]
BrowsePosition = tuple[str | int | bool | bytes | None, ...] | None


class CollectionService(Protocol):
    def get(
        self,
        collection_id: int,
        *,
        principal: Principal | None = None,
    ) -> CollectionSummary: ...
    def list(
        self,
        *,
        page_size: int,
        position: BrowsePosition,
        q: str | None,
        encryption_format: str | None = None,
        passphrase_id: str | None = None,
        tags: Sequence[str] = (),
        sort: str = "id",
        order: str = "asc",
        principal: Principal | None = None,
    ) -> CollectionListPage: ...
    def iter_collections(
        self,
        *,
        q: str | None,
        encryption_format: str | None = None,
        passphrase_id: str | None = None,
        tags: Sequence[str] = (),
        sort: str = "id",
        order: str = "asc",
        principal: Principal | None = None,
    ) -> Iterator[CollectionSummary]: ...
    def list_archive_copies(
        self,
        collection_id: int,
        *,
        page_size: int,
        position: BrowsePosition,
        principal: Principal | None = None,
    ) -> JsonObject: ...
    def iter_archive_copies(
        self,
        collection_id: int,
        *,
        principal: Principal | None = None,
    ) -> Iterator[JsonObject]: ...


class CollectionDescriptionService(Protocol):
    def replace(
        self,
        collection_id: int,
        *,
        description: str | None,
        expected_identity: str,
        principal: Principal,
    ) -> JsonObject: ...

    def requeue_interrupted_for_startup(self, *, limit: int = 100) -> int: ...

    def process_due(self, *, limit: int = 1) -> int: ...


class ProvenanceService(Protocol):
    def list_files(
        self,
        collection_id: int,
        *,
        page_size: int,
        position: BrowsePosition,
        q: str | None,
        status: str | None,
        sort: str,
        order: str,
        principal: Principal,
    ) -> JsonObject: ...
    def iter_files(
        self,
        collection_id: int,
        *,
        q: str | None,
        status: str | None,
        sort: str,
        order: str,
        principal: Principal,
    ) -> Iterator[JsonObject]: ...
    def show_file(
        self,
        collection_id: int,
        path: str,
        *,
        principal: Principal,
    ) -> JsonObject: ...
    def trace_file(
        self,
        collection_id: int,
        path: str,
        *,
        page_size: int,
        position: BrowsePosition,
        principal: Principal,
    ) -> JsonObject: ...
    def iter_trace_file(
        self,
        collection_id: int,
        path: str,
        *,
        principal: Principal,
    ) -> Iterator[JsonObject]: ...
    def journal_metadata(
        self,
        collection_id: int,
        journal_id: str,
        *,
        principal: Principal,
    ) -> tuple[int, str]: ...
    def iter_journal(
        self,
        collection_id: int,
        journal_id: str,
        *,
        principal: Principal,
    ) -> Iterator[bytes]: ...
    def iter_journal_range(
        self,
        collection_id: int,
        journal_id: str,
        *,
        offset: int,
        size: int,
        principal: Principal,
    ) -> Iterator[bytes]: ...
    def list_journal_agents(
        self,
        collection_id: int,
        journal_id: str,
        *,
        page_size: int,
        position: BrowsePosition,
        principal: Principal,
    ) -> JsonObject: ...
    def iter_journal_agents(
        self,
        collection_id: int,
        journal_id: str,
        *,
        principal: Principal,
    ) -> Iterator[JsonObject]: ...
    def request_verification(
        self,
        collection_id: int,
        *,
        principal: Principal,
    ) -> JsonObject: ...
    def get_verification(
        self,
        collection_id: int,
        *,
        principal: Principal,
    ) -> JsonObject: ...
    def cancel_verification(
        self,
        collection_id: int,
        *,
        principal: Principal,
    ) -> JsonObject: ...
    def requeue_interrupted_verifications_for_startup(self) -> int: ...
    def process_due_verifications(self, *, limit: int = 1) -> int: ...


class CollectionTagService(Protocol):
    def list_tags(
        self,
        *,
        page_size: int,
        position: BrowsePosition,
        q: str | None,
        principal: Principal,
    ) -> JsonObject: ...
    def list_collection(
        self,
        collection_id: int,
        *,
        page_size: int,
        position: BrowsePosition,
        expected_revision: int,
        expected_tag_set_identity: str,
        principal: Principal,
    ) -> JsonObject: ...
    def contains(
        self,
        collection_id: int,
        *,
        tag: str,
        revision: int,
        tag_set_identity: str,
        principal: Principal,
    ) -> JsonObject: ...
    def add(
        self,
        collection_id: int,
        *,
        tag: str,
        operation_id: str,
        expected_revision: int,
        expected_tag_set_identity: str,
        principal: Principal,
    ) -> JsonObject: ...
    def remove(
        self,
        collection_id: int,
        *,
        tag: str,
        operation_id: str,
        expected_revision: int,
        expected_tag_set_identity: str,
        principal: Principal,
    ) -> JsonObject: ...
    def requeue_interrupted_for_startup(self, *, limit: int = 100) -> int: ...
    def process_due(self, *, limit: int = 1) -> int: ...


class CollectionDeletionService(Protocol):
    def plan(
        self,
        collection_id: int,
        *,
        principal: Principal | None = None,
        retirement_claim_id: str | None = None,
    ) -> JsonObject: ...
    def delete(
        self,
        collection_id: int,
        *,
        challenge: str,
        initiator: Principal,
        event_context: dict[str, object] | None = None,
        retirement_claim_id: str | None = None,
    ) -> JsonObject: ...
    def process_due(self, *, limit: int = 10) -> int: ...


class CatalogSyncService(Protocol):
    def checkpoint(self, *, principal: Principal) -> CatalogSyncCheckpoint: ...
    def collections(
        self,
        *,
        cursor: str,
        limit: int,
        principal: Principal,
    ) -> CatalogSyncCollectionPage: ...
    def changes(
        self,
        *,
        cursor: str,
        limit: int,
        principal: Principal,
    ) -> CatalogSyncChangePage: ...
    def reap_expired_history(self, *, limit: int | None = None) -> int: ...


class RetrievalService(Protocol):
    def request_cache_accounting_reconciliation_for_startup(self) -> int: ...
    def process_cache_accounting_reconciliation(self, *, limit: int = 100) -> int: ...

    def collection_inventory(
        self,
        collection_id: int,
        *,
        principal: Principal | None = None,
    ) -> tuple[
        PortableCollectionHeader,
        Iterator[PortableCollectionFile],
        str,
        int,
        int,
    ]: ...
    def collection_inventory_page(
        self,
        collection_id: int,
        *,
        cursor: str | None,
        limit: int,
        expected_identity: str | None,
        principal: Principal | None = None,
    ) -> PortableCollectionInventoryPage: ...
    def cache_status(
        self,
        *,
        principal: Principal | None = None,
    ) -> JsonObject: ...
    def list_cache_objects(
        self,
        *,
        page_size: int,
        position: BrowsePosition,
        q: str | None,
        collection_id: int | None = None,
        source_store: str | None = None,
        cache_store: str | None = None,
        state: str | None = None,
        protection: str | None = None,
        expires_before: str | None = None,
        expires_after: str | None = None,
        sort: str,
        order: str,
        principal: Principal | None = None,
    ) -> JsonObject: ...
    def iter_cache_objects(
        self,
        *,
        q: str | None,
        collection_id: int | None = None,
        source_store: str | None = None,
        cache_store: str | None = None,
        state: str | None = None,
        protection: str | None = None,
        expires_before: str | None = None,
        expires_after: str | None = None,
        sort: str,
        order: str,
        principal: Principal | None = None,
    ) -> Iterator[JsonObject]: ...
    def get_cache_object(
        self,
        *,
        collection_id: int,
        source_store: str,
        object_id: str,
        principal: Principal | None = None,
    ) -> JsonObject: ...
    def plan(
        self,
        files: Sequence[tuple[int, str]],
        *,
        idempotency_key: str | None = None,
        lease: timedelta | None = None,
        restore_policy: str = "allow",
        principal: Principal | None = None,
    ) -> JsonObject: ...
    def get_plan(
        self,
        *,
        principal_id: str,
        plan_id: str,
        key_id: str | None = None,
    ) -> JsonObject: ...
    def advance_plan(
        self,
        *,
        principal_id: str,
        plan_id: str,
        key_id: str | None = None,
    ) -> JsonObject: ...
    def list_plan_files(
        self,
        *,
        principal_id: str,
        plan_id: str,
        etag: str,
        start_ordinal: int,
        page_size: int,
        key_id: str | None = None,
    ) -> JsonObject: ...
    def create(
        self,
        *,
        principal_id: str,
        key_id: str | None = None,
        plan_id: str,
        plan_etag: str,
        event_context: dict[str, object] | None = None,
        principal: Principal | None = None,
    ) -> JsonObject: ...
    def get(self, *, principal_id: str, job_id: str, key_id: str | None = None) -> JsonObject: ...
    def renew(
        self,
        *,
        principal_id: str,
        job_id: str,
        lease: timedelta,
        key_id: str | None = None,
    ) -> JsonObject: ...
    def acknowledge(
        self,
        *,
        principal_id: str,
        job_id: str,
        key_id: str | None = None,
    ) -> JsonObject: ...
    def cancel(
        self,
        *,
        principal_id: str,
        job_id: str,
        key_id: str | None = None,
    ) -> JsonObject: ...
    def content_metadata(
        self,
        *,
        principal_id: str,
        job_id: str,
        collection_id: int,
        path: str,
        key_id: str | None = None,
    ) -> tuple[int, str]: ...
    def content(
        self,
        *,
        principal_id: str,
        job_id: str,
        collection_id: int,
        path: str,
        offset: int = 0,
        size: int | None = None,
        key_id: str | None = None,
    ) -> tuple[Iterator[bytes], int, str]: ...
    def process_due(self, *, limit: int = 10) -> int: ...
    def requeue_interrupted_cache_cleanup_for_startup(self) -> int: ...
    def sweep(self, *, limit: int = 100) -> int: ...


class AppKeyService(Protocol):
    def authenticate(self, token: str) -> Principal | None: ...
    def create(
        self,
        *,
        app: str,
        access: Sequence[ApplicationAccess | tuple[str, str]],
        grantor: Principal,
        expires_in: timedelta | None = None,
    ) -> JsonObject: ...
    def rotate(
        self,
        *,
        app: str,
        key_id: str,
        grantor: Principal,
    ) -> JsonObject: ...
    def revoke(self, *, app: str, key_id: str) -> JsonObject: ...
    def replace_access(
        self,
        *,
        app: str,
        key_id: str,
        access: Sequence[ApplicationAccess | tuple[str, str]],
        grantor: Principal,
    ) -> JsonObject: ...
    def add_access(
        self,
        *,
        app: str,
        key_id: str,
        access: ApplicationAccess | tuple[str, str],
        grantor: Principal,
    ) -> JsonObject: ...
    def remove_access(
        self,
        *,
        app: str,
        key_id: str,
        access: ApplicationAccess | tuple[str, str],
    ) -> JsonObject: ...
    def list_access(
        self,
        *,
        page_size: int,
        position: BrowsePosition,
        q: str | None,
        sort: str,
        order: str,
        app: str | None = None,
        key_id: str | None = None,
        permission: str | None = None,
        resource: str | None = None,
        active: bool | None = None,
    ) -> JsonObject: ...
    def iter_access(
        self,
        *,
        q: str | None,
        sort: str,
        order: str,
        app: str | None = None,
        key_id: str | None = None,
        permission: str | None = None,
        resource: str | None = None,
        active: bool | None = None,
    ) -> Iterator[JsonObject]: ...
    def list_apps(
        self,
        *,
        page_size: int,
        position: BrowsePosition,
        q: str | None,
        sort: str,
        order: str,
        active: bool | None = None,
    ) -> JsonObject: ...
    def iter_apps(
        self,
        *,
        q: str | None,
        sort: str,
        order: str,
        active: bool | None = None,
    ) -> Iterator[JsonObject]: ...
    def list_keys(
        self,
        *,
        app: str,
        page_size: int,
        position: BrowsePosition,
        q: str | None,
        sort: str,
        order: str,
        active: bool | None = None,
    ) -> JsonObject: ...
    def iter_keys(
        self,
        *,
        app: str,
        q: str | None,
        sort: str,
        order: str,
        active: bool | None = None,
    ) -> Iterator[JsonObject]: ...


class LifecycleEventService(Protocol):
    def page(
        self,
        *,
        owner_principal_id: str | None,
        after: str | None,
        limit: int,
    ) -> RiverhogEventPage: ...
    def reap_expired_contexts(self) -> int: ...


class SearchService(Protocol):
    def search(
        self,
        *,
        q: str | None,
        page_size: int,
        position: BrowsePosition,
        sort: str,
        order: str,
        collection: int | None = None,
        principal: Principal | None = None,
    ) -> JsonObject: ...
    def iter_files(
        self,
        *,
        q: str | None,
        sort: str,
        order: str,
        collection: int | None = None,
        principal: Principal | None = None,
    ) -> Iterator[JsonObject]: ...


class ArchiveCopyJobService(Protocol):
    def requeue_interrupted_jobs_for_startup(self, *, limit: int = 100) -> int: ...
    def create_or_resume(
        self,
        collection_id: int,
        *,
        destination_store: str,
        source_store: str | None = None,
        initiator: Principal,
        event_context: dict[str, object] | None = None,
    ) -> JsonObject: ...
    def get(
        self,
        collection_id: int,
        *,
        destination_store: str,
        principal: Principal | None = None,
    ) -> JsonObject: ...
    def cancel(
        self,
        collection_id: int,
        *,
        destination_store: str,
        principal: Principal | None = None,
    ) -> JsonObject: ...
    def list(
        self,
        *,
        page_size: int,
        position: BrowsePosition,
        q: str | None,
        sort: str,
        order: str,
        state: str | None = None,
        principal: Principal | None = None,
    ) -> JsonObject: ...
    def iter_jobs(
        self,
        *,
        q: str | None,
        sort: str,
        order: str,
        state: str | None = None,
        principal: Principal | None = None,
    ) -> Iterator[JsonObject]: ...
    def process_due(self, *, limit: int = 1) -> int: ...


class ArchiveCopyRetirementService(Protocol):
    def plan(self, collection_id: int, *, store: str) -> JsonObject: ...
    def retire(self, collection_id: int, *, store: str, challenge: str) -> JsonObject: ...


class ArchiveStoreService(Protocol):
    def get(
        self,
        store: str,
        *,
        principal: Principal | None = None,
    ) -> ArchiveStoreSummary: ...
    def list(
        self,
        *,
        page_size: int,
        position: BrowsePosition,
        q: str | None,
        sort: str,
        order: str,
        principal: Principal | None = None,
    ) -> ArchiveStoreListPage: ...
    def iter_stores(
        self,
        *,
        q: str | None,
        sort: str,
        order: str,
        principal: Principal | None = None,
    ) -> Iterator[ArchiveStoreSummary]: ...
