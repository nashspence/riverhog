"""Published history keeps custody selection, member scope and live read fences."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from threading import Barrier, Lock
from types import SimpleNamespace
from typing import get_args
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from riverhog_age import decrypt_age_scrypt
from riverhog_api.auth import ProvenanceExporter
from riverhog_api.deps import get_container
from riverhog_api.routers.provenance import router
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    HistoryJournalAnchor,
    MemberHistoryStore,
    ProvenanceRootDocument,
    provenance_structure_object_id,
)
from riverhog_core.app_permissions import (
    CATALOG_READ,
    KEYS_MANAGE,
    PROVENANCE_EXPORT,
    PROVENANCE_READ,
    ApplicationAccess,
    Principal,
)
from riverhog_core.archive_store_registry import ArchiveStoreBinding, ArchiveStoreRegistry
from riverhog_core.catalog_db import initialize_db, make_session_factory, session_scope
from riverhog_core.catalog_models import (
    AppKeyRecord,
    ArchiveCopyRetirementRecord,
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    CollectionArtifactProvenanceRecord,
    CollectionArtifactRecord,
    CollectionProvenanceJournalRecord,
    CollectionRecord,
)
from riverhog_core.catalog_workflow_models import (
    CollectionProcessingCapabilityRecord,
    CollectionProcessingClaimRecord,
)
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader
from riverhog_core.provenance_read_cache import ProvenanceReadCache
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.app_keys import SqlAlchemyAppKeyService
from riverhog_core.services.canonical_provenance import SqlAlchemyCanonicalProvenanceService
from riverhog_core.services.download_allowances import SqlAlchemyDownloadAllowance
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_protocol.errors import DownloadAllowanceExceeded, NotFound
from riverhog_provenance import MemberHistoryClosure, validate_journal

from tests.support.qualification.recovery_archive import (
    PASSPHRASE,
    FixtureArchive,
    write_archive,
)
from tests.unit.artifact_scope_fixtures import persisted_artifact_scope
from tests.unit.db_helpers import sqlite_url
from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation

_BOOTSTRAP = Principal(
    id="bootstrap",
    key_id=None,
    access=frozenset({ApplicationAccess(KEYS_MANAGE)}),
    unrestricted_delegation=True,
)
_PERMISSIONS = (CATALOG_READ, PROVENANCE_READ, PROVENANCE_EXPORT)
_NOW = "2026-01-01T00:00:00.000000000Z"


class _ArchiveStore:
    def __init__(self, root: Path, allowance: SqlAlchemyDownloadAllowance, name: str) -> None:
        self.root = root
        self.allowance = allowance
        self.name = name
        self.downloaded = 0
        self.object_reads: Counter[str] = Counter()
        self.before_yield: Callable[[str], None] = lambda _path: None
        self.after_yield: Callable[[str], None] = lambda _path: None

    def iter_archive_object(self, *, object, attribution, **_kwargs) -> Iterator[bytes]:
        relative = object.object_path.removeprefix("collection/")
        ciphertext = (self.root / relative).read_bytes()

        def remote() -> Iterator[bytes]:
            self.object_reads[relative] += 1
            self.downloaded += len(ciphertext)
            yield ciphertext

        received = b"".join(
            self.allowance.track(
                store=self.name,
                expected_bytes=len(ciphertext),
                content=remote(),
                attribution=attribution,
            )
        )
        plaintext = decrypt_age_scrypt(received, PASSPHRASE)
        self.before_yield(relative)
        yield plaintext
        self.after_yield(relative)


def _environment(tmp_path: Path, **fixture_options):
    root = tmp_path / "archive"
    archive = write_archive(root, **fixture_options)
    config = RuntimeConfig.for_testing(database_url=sqlite_url(tmp_path / "catalog.sqlite3"))
    registration = config.archive_store("archive")
    names = ("creation", "preferred", "replica")
    config = replace(
        config,
        archive_stores={
            name: replace(
                registration,
                name=name,
                base_url=f"https://{name}.invalid",
                monthly_download_allowance_bytes=10**9,
                download_safety_buffer_bytes=0,
            )
            for name in names
        },
        archive_write_store="creation",
        archive_read_order=("preferred", "creation", "replica"),
    )
    initialize_db(config.database_url)
    factory = make_session_factory(config.database_url)
    provenance = ProvenanceRootDocument.from_json_bytes(archive.provenance_root)
    with session_scope(factory) as session:
        session.add(
            CollectionRecord(
                id=1,
                creation_idempotency_key="history-access",
                creation_identity_sha256="1" * 64,
                creation_custody_mode="custody-transfer",
                creation_archive_store="creation",
                archive_generation=provenance.archive_generation,
                delivery_context_id=provenance.delivery_context_id,
                artifact_set_identity=provenance.artifact_set_sha256,
                encryption_format="age-v1-scrypt",
                passphrase_id="recovery-test-key-v1",
                provenance_identity=provenance.identity,
                inventory_identity="4" * 64,
                archive_root_sha256=archive.archive_root_sha256,
                created_at=_NOW,
            )
        )
        session.flush()
        for member in archive.history_bindings:
            session.add(
                CollectionArtifactRecord(
                    collection_id=1,
                    artifact_id=member.artifact_id,
                    bytes=member.bytes,
                    sha256=member.sha256,
                )
            )
        session.flush()
        for journal_id, raw in archive.journals.items():
            summary = validate_journal(raw, require_profiles=False)
            tail = HistoryJournalAnchor.from_mapping(summary.anchor)
            session.add(
                CollectionProvenanceJournalRecord(
                    collection_id=1,
                    journal_id=journal_id,
                    bytes=len(raw),
                    sha256=summary.journal_sha256,
                    entries=len(summary.frames),
                    terminal_entry_id=tail.through_entry_id,
                    terminal_sequence=tail.through_sequence,
                    terminal_json_sha256=tail.through_json_sha256,
                )
            )
        session.flush()
        store = MemberHistoryStore(lambda path: (archive.history_objects[path],))
        for binding in archive.history_bindings:
            history = store.descriptor(binding)
            anchor = history.primary.journal
            session.add(
                CollectionArtifactProvenanceRecord(
                    collection_id=1,
                    artifact_id=binding.artifact_id,
                    journal_id=anchor.journal_id,
                    through_entry_id=anchor.through_entry_id,
                    through_sequence=anchor.through_sequence,
                    through_json_sha256=anchor.through_json_sha256,
                    prefix_bytes=anchor.prefix_bytes,
                    prefix_sha256=anchor.prefix_sha256,
                    delivery_association_id=history.primary.delivery_association_id,
                    history_sha256=binding.history_sha256,
                    history_bytes=binding.history_bytes,
                )
            )
        for name in names:
            incarnation = seed_storage_incarnation(session, "archive", name)
            session.add(
                CollectionArchiveCopyRecord(
                    collection_id=1,
                    store=name,
                    incarnation_id=incarnation,
                    state="uploaded",
                    archive_storage_prefix="collection",
                    last_uploaded_at=_NOW,
                    last_verified_at=_NOW,
                )
            )
            session.flush()
            for ordinal, path in enumerate(sorted(root.rglob("*.age"))):
                ciphertext = path.read_bytes()
                plaintext = decrypt_age_scrypt(ciphertext, PASSPHRASE)
                relative = path.relative_to(root).as_posix()
                session.add(
                    CollectionArchiveObjectRecord(
                        collection_id=1,
                        store=name,
                        object_id="fixture-" + hashlib.sha256(relative.encode()).hexdigest(),
                        object_order=ordinal,
                        kind="provenance",
                        object_path="collection/" + relative,
                        plaintext_bytes=len(plaintext),
                        stored_bytes=len(ciphertext),
                        sha256=hashlib.sha256(plaintext).hexdigest(),
                        stored_sha256=hashlib.sha256(ciphertext).hexdigest(),
                        uploaded_at=_NOW,
                        verified_at=_NOW,
                    )
                )
    allowance = SqlAlchemyDownloadAllowance(config)
    stores = {name: _ArchiveStore(root, allowance, name) for name in names}
    bindings = {}
    with session_scope(factory) as session:
        for name in names:
            incarnation = seed_storage_incarnation(session, "archive", name)
            bindings[name] = ArchiveStoreBinding(
                incarnation_id=incarnation,
                store=stores[name],
                resumable_objects=Mock(),
                immutable_objects=Mock(),
                object_ranges=Mock(),
            )
    registry = ArchiveStoreRegistry(bindings)
    service = SqlAlchemyCanonicalProvenanceService(config, registry)
    keys = SqlAlchemyAppKeyService(config)
    credential = keys.create(
        app="history-reader",
        access=tuple(ApplicationAccess(p) for p in _PERMISSIONS),
        grantor=_BOOTSTRAP,
    )
    principal = keys.authenticate(str(credential["token"]))
    assert principal is not None and principal.key_id is not None
    allowance.set_key_quota(app=principal.id, key_id=principal.key_id, monthly_bytes=None)
    return service, archive, config, registry, stores, allowance, keys, principal


def _scope(config: RuntimeConfig, archive: FixtureArchive) -> Principal:
    member = archive.history_bindings[0]
    return persisted_artifact_scope(
        config.database_url,
        access=tuple(ApplicationAccess(p) for p in _PERMISSIONS),
        artifacts=((1, member.artifact_id, member.bytes, member.sha256),),
    )


def test_capability_exposes_only_selected_structure_and_snapshot_prefix(tmp_path: Path) -> None:
    service, archive, config, _registry, _stores, _allowance, _keys, _principal = _environment(
        tmp_path,
        unselected_journal_tail=True,
    )
    principal = _scope(config, archive)
    selected, unrelated = archive.history_bindings[:2]
    selected_history = MemberHistoryStore(lambda path: (archive.history_objects[path],)).descriptor(
        selected
    )
    anchor = selected_history.primary.journal
    page = service.list_journals(1, page_size=1, after_journal_id=None, principal=principal)
    assert page["journals"] == [
        {
            "journal_id": anchor.journal_id,
            "bytes": str(anchor.prefix_bytes),
            "sha256": anchor.prefix_sha256,
        }
    ]
    assert page["next_journal_id"] is None
    assert anchor.prefix_bytes < len(archive.journals[anchor.journal_id])
    assert service.journal_metadata(1, anchor.journal_id, principal=principal) == (
        anchor.prefix_bytes,
        anchor.prefix_sha256,
    )
    assert (
        b"".join(service.iter_journal_range(1, anchor.journal_id, principal=principal))
        == archive.journals[anchor.journal_id][: anchor.prefix_bytes]
    )
    with pytest.raises(NotFound):
        b"".join(
            service.iter_journal_range(
                1, anchor.journal_id, offset=anchor.prefix_bytes, size=1, principal=principal
            )
        )
    with pytest.raises(NotFound):
        service.get_artifact(1, unrelated.artifact_id, principal=principal)
    other_history = MemberHistoryStore(lambda path: (archive.history_objects[path],)).descriptor(
        unrelated
    )
    with pytest.raises(NotFound):
        service.journal_metadata(1, other_history.primary.journal.journal_id, principal=principal)
    with MemberHistoryClosure(
        MemberHistoryStore(lambda path: (archive.history_objects[path],)),
        lambda journal, end: (archive.journals[journal],),
        member_role=COLLECTION_MEMBER_ROLE,
    ) as closure:
        closure.resolve(selected, extent=RETAINED_HISTORY_EXTENT)
        reachable = {
            provenance_structure_object_id(path)
            for path in archive.history_objects
            if closure.contains_structure_object(path)
        }
    for path, raw in archive.history_objects.items():
        object_id = provenance_structure_object_id(path)
        if object_id in reachable:
            assert (
                service.get_structure_object(
                    1, object_id, expected_root=archive.archive_root_sha256, principal=principal
                )
                == raw
            )
        else:
            with pytest.raises(NotFound):
                service.get_structure_object(
                    1, object_id, expected_root=archive.archive_root_sha256, principal=principal
                )


@pytest.mark.parametrize("extent", [BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT])
def test_capability_honors_the_sealed_inherited_extent(tmp_path: Path, extent: str) -> None:
    source = write_archive(
        tmp_path / "source", late_shared_history=True, late_shared_scope="retained"
    )
    service, archive, config, *_ = _environment(
        tmp_path, inherited_history=source, inherited_extent=extent
    )
    principal = _scope(config, archive)
    selected = source.history_bindings[0]
    source_primary = (
        MemberHistoryStore(lambda path: (source.history_objects[path],))
        .descriptor(selected)
        .primary.journal.journal_id
    )
    late = next(
        journal
        for journal in source.journals
        if journal != source_primary
        and journal
        not in {
            MemberHistoryStore(lambda path: (source.history_objects[path],))
            .descriptor(binding)
            .primary.journal.journal_id
            for binding in source.history_bindings
        }
    )
    page = service.list_journals(1, page_size=200, after_journal_id=None, principal=principal)
    visible = {row["journal_id"] for row in page["journals"]}
    assert source_primary in visible
    assert (late in visible) == (extent == RETAINED_HISTORY_EXTENT)
    if extent == BOUND_HISTORY_EXTENT:
        with pytest.raises(NotFound):
            b"".join(service.iter_journal_range(1, late, principal=principal))


@pytest.mark.parametrize("excluded", ["unavailable", "retiring", "incomplete", "wrong-incarnation"])
def test_history_uses_configured_read_order_and_a_healthy_complete_replica(
    tmp_path: Path, excluded: str
) -> None:
    service, archive, _config, registry, stores, _allowance, _keys, principal = _environment(
        tmp_path
    )
    journal = next(iter(archive.journals))
    service.journal_metadata(1, journal, principal=principal)
    assert stores["preferred"].downloaded > 0 and stores["creation"].downloaded == 0
    stores["preferred"].downloaded = 0
    registry._probes["creation"] = lambda: (_ for _ in ()).throw(
        OSError("unavailable creation store")
    )
    if excluded == "unavailable":
        registry._probes["preferred"] = lambda: (_ for _ in ()).throw(
            OSError("unavailable preferred store")
        )
    elif excluded == "wrong-incarnation":
        registry._stores["preferred"] = replace(
            registry._stores["preferred"], incarnation_id="ffffffff-ffff-4fff-8fff-ffffffffffff"
        )
    else:
        with session_scope(service._session_factory) as session:
            copy = session.get(CollectionArchiveCopyRecord, (1, "preferred"))
            assert copy is not None
            if excluded == "incomplete":
                copy.last_verified_at = None
            else:
                session.add(
                    ArchiveCopyRetirementRecord(
                        collection_id=1,
                        store="preferred",
                        incarnation_id=copy.incarnation_id,
                        challenge="retirement",
                        plan_json="{}",
                        started_at=_NOW,
                    )
                )
    assert service.journal_metadata(1, journal, principal=principal) == (
        len(archive.journals[journal]),
        hashlib.sha256(archive.journals[journal]).hexdigest(),
    )
    assert stores["preferred"].downloaded == stores["creation"].downloaded == 0
    assert stores["replica"].downloaded > 0


def test_user_history_downloads_enforce_and_charge_key_quota(tmp_path: Path) -> None:
    service, archive, _config, _registry, stores, allowance, _keys, principal = _environment(
        tmp_path
    )
    assert principal.key_id is not None
    journal = next(iter(archive.journals))
    allowance.set_key_quota(app=principal.id, key_id=principal.key_id, monthly_bytes=0)
    with pytest.raises(DownloadAllowanceExceeded, match="application key"):
        service.journal_metadata(1, journal, principal=principal)
    assert sum(store.downloaded for store in stores.values()) == 0
    assert allowance.get_key_quota(key_id=principal.key_id)["accounted_bytes"] == 0
    allowance.set_key_quota(app=principal.id, key_id=principal.key_id, monthly_bytes=10**7)
    b"".join(service.iter_journal_range(1, journal, principal=principal))
    member = archive.history_bindings[0]
    proof = service.get_history_binding_proof(
        1, member.artifact_id, expected_root=archive.archive_root_sha256, principal=principal
    )
    assert proof
    assert (
        allowance.get_key_quota(key_id=principal.key_id)["accounted_bytes"]
        == sum(store.downloaded for store in stores.values())
        > 0
    )
    before = allowance.get_key_quota(key_id=principal.key_id)["accounted_bytes"]
    # Internal index rebuilding has no initiating application key; store accounting remains active.
    service._archives.reader(1, attribution=None).scan()
    assert allowance.get_key_quota(key_id=principal.key_id)["accounted_bytes"] == before
    assert sum(row.accounted_bytes for row in allowance.get_statuses()) == sum(
        store.downloaded for store in stores.values()
    )


@pytest.mark.parametrize("operation", ["metadata", "stream"])
@pytest.mark.parametrize(
    "transition",
    [
        "unpublish",
        "key-revoked",
        "key-expired",
        "grants-changed",
        "capability-revoked",
        "capability-expired",
        "claim-fence",
    ],
)
def test_live_read_fences_prevent_disclosure_after_archive_io(
    tmp_path: Path, operation: str, transition: str
) -> None:
    service, archive, config, _registry, stores, _allowance, keys, principal = _environment(
        tmp_path
    )
    if transition in ("capability-revoked", "capability-expired", "claim-fence"):
        principal = _scope(config, archive)
    selected = archive.history_bindings[0]
    journal = (
        MemberHistoryStore(lambda path: (archive.history_objects[path],))
        .descriptor(selected)
        .primary.journal.journal_id
    )

    def invalidate(_path: str) -> None:
        stores["preferred"].after_yield = lambda _path: None
        if transition == "grants-changed":
            keys.replace_access(
                app=principal.id,
                key_id=principal.key_id,
                access=(ApplicationAccess(CATALOG_READ),),
                grantor=_BOOTSTRAP,
            )
            return
        with session_scope(service._session_factory) as session:
            if transition == "unpublish":
                session.get(CollectionRecord, 1).is_published = False
            elif transition == "key-revoked":
                session.get(AppKeyRecord, principal.key_id).revoked_at = _NOW
            elif transition == "key-expired":
                session.get(AppKeyRecord, principal.key_id).expires_at = _NOW
            else:
                capability = session.get(
                    CollectionProcessingCapabilityRecord, principal.artifact_scope_capability_id
                )
                if transition == "capability-revoked":
                    capability.state = "revoked"
                elif transition == "capability-expired":
                    capability.expires_at = _NOW
                else:
                    session.get(CollectionProcessingClaimRecord, capability.claim_id).fence += 1

    stores["preferred"].after_yield = invalidate
    with pytest.raises(NotFound):
        if operation == "metadata":
            service.journal_metadata(1, journal, principal=principal)
        else:
            next(service.iter_journal_range(1, journal, principal=principal))


def test_stream_continuation_rechecks_revocation_after_cached_member_closure(
    tmp_path: Path,
) -> None:
    service, archive, config, *_ = _environment(tmp_path, journal_segment_bytes=4096)
    principal = _scope(config, archive)
    selected = archive.history_bindings[0]
    journal = (
        MemberHistoryStore(lambda path: (archive.history_objects[path],))
        .descriptor(selected)
        .primary.journal.journal_id
    )
    stream = service.iter_journal_range(1, journal, principal=principal)
    assert len(archive.journals[journal]) > 4096
    assert next(stream) == archive.journals[journal][:4096]
    with session_scope(service._session_factory) as session:
        session.get(
            CollectionProcessingCapabilityRecord, principal.artifact_scope_capability_id
        ).state = "revoked"
    with pytest.raises(NotFound):
        next(stream)


@pytest.mark.parametrize("members", [4, 128])
def test_scoped_pages_and_ranges_reuse_verified_history_without_repeat_downloads(
    tmp_path: Path, members: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    payloads = {f"{ordinal + 1:064x}": b"member" for ordinal in range(members)}
    service, archive, config, _registry, stores, allowance, *_ = _environment(
        tmp_path, members=payloads, hints={artifact_id: None for artifact_id in payloads}
    )
    principal = persisted_artifact_scope(
        config.database_url,
        access=tuple(ApplicationAccess(p) for p in _PERMISSIONS),
        artifacts=tuple((1, m.artifact_id, m.bytes, m.sha256) for m in archive.history_bindings),
    )
    scans = 0
    original_scan = CanonicalProvenanceArchiveReader._scan

    def scan(self, *args, **kwargs):
        nonlocal scans
        scans += 1
        return original_scan(self, *args, **kwargs)

    monkeypatch.setattr(CanonicalProvenanceArchiveReader, "_scan", scan)
    resolved = 0
    original = MemberHistoryClosure.resolve

    def resolve(self, *args, **kwargs):
        nonlocal resolved
        resolved += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(MemberHistoryClosure, "resolve", resolve)
    first = service.list_journals(1, page_size=1, after_journal_id=None, principal=principal)
    assert len(first["journals"]) == 1 and first["next_journal_id"] is not None
    initial_bytes = sum(store.downloaded for store in stores.values())
    initial_reads = sum(sum(store.object_reads.values()) for store in stores.values())
    assert initial_bytes > 0 and initial_reads > members
    assert max(stores["preferred"].object_reads.values()) == 1
    assert resolved == members
    assert scans == 1
    assert allowance.get_key_quota(key_id=principal.key_id)["accounted_bytes"] == initial_bytes
    assert (
        service.list_journals(1, page_size=1, after_journal_id=None, principal=principal) == first
    )
    continuation = service.list_journals(
        1, page_size=1, after_journal_id=first["next_journal_id"], principal=principal
    )
    assert continuation["journals"][0]["journal_id"] > first["journals"][0]["journal_id"]
    journal = first["journals"][0]
    assert service.journal_metadata(1, journal["journal_id"], principal=principal) == (
        int(journal["bytes"]),
        journal["sha256"],
    )
    assert (
        b"".join(
            service.iter_journal_range(
                1, journal["journal_id"], offset=0, size=1, principal=principal
            )
        )
        == archive.journals[journal["journal_id"]][:1]
    )
    assert resolved == members
    assert scans == 1
    assert sum(store.downloaded for store in stores.values()) == initial_bytes
    assert sum(sum(store.object_reads.values()) for store in stores.values()) == initial_reads
    assert allowance.get_key_quota(key_id=principal.key_id)["accounted_bytes"] == initial_bytes
    with session_scope(service._session_factory) as session:
        session.get(
            CollectionProcessingCapabilityRecord, principal.artifact_scope_capability_id
        ).state = "revoked"
    with pytest.raises(NotFound):
        service.list_journals(1, page_size=1, after_journal_id=None, principal=principal)
    with pytest.raises(NotFound):
        service.journal_metadata(1, journal["journal_id"], principal=principal)
    assert sum(store.downloaded for store in stores.values()) == initial_bytes


def test_warm_membership_never_widens_a_different_member_selection(tmp_path: Path) -> None:
    service, archive, config, *_ = _environment(tmp_path)
    selected = _scope(config, archive)
    service.list_journals(1, page_size=200, after_journal_id=None, principal=selected)
    other = archive.history_bindings[1]
    principal = persisted_artifact_scope(
        config.database_url,
        access=tuple(ApplicationAccess(p) for p in _PERMISSIONS),
        artifacts=((1, other.artifact_id, other.bytes, other.sha256),),
    )
    first = archive.history_bindings[0]
    history = MemberHistoryStore(lambda path: (archive.history_objects[path],)).descriptor(first)
    with pytest.raises(NotFound):
        service.journal_metadata(1, history.primary.journal.journal_id, principal=principal)


def test_one_byte_http_range_shares_metadata_and_body_membership_work(tmp_path: Path) -> None:
    payloads = {f"{ordinal + 1:064x}": b"member" for ordinal in range(4)}
    service, archive, config, _registry, stores, allowance, *_ = _environment(
        tmp_path, members=payloads, hints={artifact_id: None for artifact_id in payloads}
    )
    principal = persisted_artifact_scope(
        config.database_url,
        access=tuple(ApplicationAccess(p) for p in _PERMISSIONS),
        artifacts=tuple((1, m.artifact_id, m.bytes, m.sha256) for m in archive.history_bindings),
    )
    history = MemberHistoryStore(lambda path: (archive.history_objects[path],)).descriptor(
        archive.history_bindings[0]
    )
    anchor = history.primary.journal
    app = FastAPI()
    app.include_router(router, prefix="/v1")
    app.dependency_overrides[get_container] = lambda: SimpleNamespace(provenance=service)
    app.dependency_overrides[get_args(ProvenanceExporter)[1].dependency] = lambda: principal
    with TestClient(app) as client:
        path = f"/v1/collections/1/provenance/journals/{anchor.journal_id}"
        headers = {"Range": "bytes=0-0", "If-Match": f'"{anchor.prefix_sha256}"'}
        response = client.get(path, headers=headers)
        assert response.status_code == 206, response.text
        assert response.content == archive.journals[anchor.journal_id][:1]
        assert response.headers["content-length"] == "1"
        reads = sum(sum(store.object_reads.values()) for store in stores.values())
        downloaded = sum(store.downloaded for store in stores.values())
        assert max(stores["preferred"].object_reads.values()) == 1
        for _ in range(3):
            assert client.get(path, headers=headers).content == response.content
        assert sum(sum(store.object_reads.values()) for store in stores.values()) == reads
        assert sum(store.downloaded for store in stores.values()) == downloaded
        assert allowance.get_key_quota(key_id=principal.key_id)["accounted_bytes"] == downloaded


def test_metadata_index_cache_budget_never_limits_valid_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, archive, config, *_ = _environment(tmp_path)
    principal = _scope(config, archive)
    service._archives._metadata_indexes = ProvenanceReadCache(byte_budget=1, entry_budget=1)
    scans = 0
    original = CanonicalProvenanceArchiveReader._scan

    def scan(self, *args, **kwargs):
        nonlocal scans
        scans += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(CanonicalProvenanceArchiveReader, "_scan", scan)
    first = service.list_journals(1, page_size=200, after_journal_id=None, principal=principal)
    assert (
        service.list_journals(1, page_size=200, after_journal_id=None, principal=principal) == first
    )
    assert scans == 2


def test_warm_metadata_index_does_not_substitute_a_retired_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, archive, config, _registry, stores, *_ = _environment(tmp_path)
    principal = _scope(config, archive)
    scans = 0
    original = CanonicalProvenanceArchiveReader._scan

    def scan(self, *args, **kwargs):
        nonlocal scans
        scans += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(CanonicalProvenanceArchiveReader, "_scan", scan)
    first = service.list_journals(1, page_size=200, after_journal_id=None, principal=principal)
    preferred_bytes = stores["preferred"].downloaded
    assert preferred_bytes > 0 and scans == 1
    with session_scope(service._session_factory) as session:
        for name in ("preferred", "creation"):
            copy = session.get(CollectionArchiveCopyRecord, (1, name))
            assert copy is not None
            copy.last_verified_at = None
    assert (
        service.list_journals(1, page_size=200, after_journal_id=None, principal=principal) == first
    )
    assert scans == 2
    assert stores["preferred"].downloaded == preferred_bytes
    assert stores["replica"].downloaded > 0


def test_membership_cache_retains_only_queries_after_complete_closure_validation(tmp_path: Path):
    from riverhog_archive_contracts import provenance_structure_identity
    from riverhog_provenance import MemberHistoryMembership

    _service, archive, *_ = _environment(tmp_path)
    store = MemberHistoryStore(lambda path: (archive.history_objects[path],))
    with MemberHistoryClosure(
        store,
        lambda journal, end: (archive.journals[journal][:end],),
        member_role=COLLECTION_MEMBER_ROLE,
    ) as closure:
        for binding in archive.history_bindings:
            closure.resolve(binding, extent=RETAINED_HISTORY_EXTENT)
        anchors = list(closure.journal_anchors())
        paths = [
            provenance_structure_identity(raw).relative_path for raw in closure.structure_objects()
        ]
        image = closure.membership_image(max_bytes=64 * 1024)
        assert image is not None and len(image) <= 64 * 1024
        with MemberHistoryMembership(image) as membership:
            assert list(membership.journal_anchors()) == anchors
            for anchor in anchors:
                assert membership.journal_anchor(anchor.journal_id) == anchor
            assert all(membership.contains_structure_object(path) for path in paths)
            assert not membership.contains_structure_object("unselected-history")
        assert closure.membership_image(max_bytes=1) is None
        assert list(closure.journal_anchors()) == anchors


def test_concurrent_authorized_reads_share_immutable_builds_and_own_their_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payloads = {f"{ordinal + 1:064x}": b"member" for ordinal in range(16)}
    service, archive, config, _registry, stores, _allowance, *_ = _environment(
        tmp_path, members=payloads, hints={artifact_id: None for artifact_id in payloads}
    )
    principal = persisted_artifact_scope(
        config.database_url,
        access=tuple(ApplicationAccess(p) for p in _PERMISSIONS),
        artifacts=tuple((1, m.artifact_id, m.bytes, m.sha256) for m in archive.history_bindings),
    )
    lock = Lock()
    scans = resolves = 0
    original_scan = CanonicalProvenanceArchiveReader._scan
    original_resolve = MemberHistoryClosure.resolve
    original_prepared = CanonicalProvenanceArchiveReader.prepared
    connections, scratch_paths = set(), set()

    def scan(self, *args, **kwargs):
        nonlocal scans
        with lock:
            scans += 1
        return original_scan(self, *args, **kwargs)

    def resolve(self, *args, **kwargs):
        nonlocal resolves
        with lock:
            resolves += 1
        return original_resolve(self, *args, **kwargs)

    @contextmanager
    def prepared(self):
        with original_prepared(self) as ready:
            cursor = ready._prepared_db.execute("PRAGMA database_list")
            try:
                path = Path(cursor.fetchone()[2])
            finally:
                cursor.close()
            with lock:
                connections.add(id(ready._prepared_db))
                scratch_paths.add(path)
            yield ready

    monkeypatch.setattr(CanonicalProvenanceArchiveReader, "_scan", scan)
    monkeypatch.setattr(MemberHistoryClosure, "resolve", resolve)
    monkeypatch.setattr(CanonicalProvenanceArchiveReader, "prepared", prepared)
    for cache in (service._archives._metadata_indexes, service._memberships):
        barrier = Barrier(4)
        original = cache.get_or_load

        def together(key, load, *, barrier=barrier, original=original):
            barrier.wait(timeout=30)
            return original(key, load)

        monkeypatch.setattr(cache, "get_or_load", together)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [
            pool.submit(
                service.list_journals, 1, page_size=1, after_journal_id=None, principal=principal
            )
            for _ in range(4)
        ]
        results = [future.result(timeout=30) for future in futures]
    assert results == [results[0]] * 4
    assert scans == 1 and resolves == 16
    assert len(connections) == 4
    assert all(not path.exists() for path in scratch_paths)
    assert max(stores["preferred"].object_reads.values()) == 1


@pytest.mark.parametrize("failed_stage", ["metadata", "history"])
def test_failed_immutable_build_is_cleaned_and_retried_with_live_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_stage: str
) -> None:
    service, archive, config, _registry, _stores, _allowance, *_ = _environment(tmp_path)
    principal = _scope(config, archive)
    owner = CanonicalProvenanceArchiveReader if failed_stage == "metadata" else MemberHistoryClosure
    method = "_scan" if failed_stage == "metadata" else "resolve"
    original = getattr(owner, method)
    failed = False

    def once(self, *args, **kwargs):
        nonlocal failed
        result = original(self, *args, **kwargs)
        if not failed:
            failed = True
            raise OSError("immutable builder interrupted")
        return result

    monkeypatch.setattr(owner, method, once)
    with pytest.raises(OSError, match="immutable builder interrupted"):
        service.list_journals(1, page_size=1, after_journal_id=None, principal=principal)
    assert service.list_journals(1, page_size=1, after_journal_id=None, principal=principal)[
        "journals"
    ]
    with session_scope(service._session_factory) as session:
        session.get(
            CollectionProcessingCapabilityRecord, principal.artifact_scope_capability_id
        ).state = "revoked"
    with pytest.raises(NotFound):
        service.list_journals(1, page_size=1, after_journal_id=None, principal=principal)
