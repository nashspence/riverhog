"""Shared-store replay and terminal-control witnesses over an owned storage factory."""

from __future__ import annotations

import hashlib
from pathlib import Path

from riverhog_client import PrimaryScope, PrimaryStore, PrimaryStoreLimits
from riverhog_client._prepared_primary_documents import (
    PrimaryAllocation,
    PrimarySession,
    primary_control_bytes,
)
from riverhog_client._prepared_primary_storage import PrimaryStorageFactory
from riverhog_client._prepared_primary_store import PrimaryPreparation
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
)

from tests.support.prepared_primary import custody_receipt

SCOPE = PrimaryScope(service="fixture-storage-service", account="fixture-storage-account")
LIMITS = PrimaryStoreLimits(
    pending_bytes=1024 * 1024,
    total_bytes=4 * 1024 * 1024,
    preparation_bytes=256 * 1024,
    control_bytes=8192,
)
ALLOCATION = PrimaryAllocation.model_validate(
    {
        "allocation_id": "shared-store-replay",
        "scope": SCOPE,
        "control_record_bytes": str(LIMITS.control_bytes),
    }
)
DESCRIPTOR = primary_control_bytes(ALLOCATION)


def witness(path: Path, factory: PrimaryStorageFactory) -> None:
    """Use original sealed controls with synthetic octets, without claiming Native authorship."""
    create, request, decision = b"exact creation", b"exact member retry", b"exact decision"
    journal = b"original retained journal\x00" * 800
    member = ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": "9" * 64,
            "bytes": "7",
            "sha256": hashlib.sha256(b"payload").hexdigest(),
        }
    )
    primary = CollectionArtifactProvenanceBindingDocument.model_validate(
        {
            "artifact_id": member.artifact_id,
            "journal": {
                "journal_id": "urn:uuid:aaaabbbb-1234-4234-8234-123456789abc",
                "through": {
                    "entry_id": "urn:uuid:aaaabbbb-4321-4321-8321-123456789abc",
                    "sequence": "0",
                    "json_sha256": "a" * 64,
                },
                "prefix_bytes": str(len(journal)),
                "prefix_sha256": hashlib.sha256(journal).hexdigest(),
            },
            "delivery_association_id": "urn:uuid:bbbbaaaa-1234-4234-8234-123456789abc",
        }
    )

    def opened(*, initialize: bool = False) -> PrimaryStore:
        return PrimaryStore(
            path,
            allocation_id=ALLOCATION.allocation_id,
            scope=SCOPE,
            limits=LIMITS,
            initialize=initialize,
            storage_factory=factory,
        )

    with opened(initialize=True) as store:
        assert store.allocation == ALLOCATION
        assert store.session_for(create) is None
        store.bind_session(
            PrimarySession(
                allocation_sha256=hashlib.sha256(DESCRIPTOR).hexdigest(),
                create_request={
                    "bytes": str(len(create)),
                    "sha256": hashlib.sha256(create).hexdigest(),
                },
                collection_id="42",
                delivery_context_id="urn:uuid:ccccaaaa-1234-4234-8234-123456789abc",
                server_construction_identity_sha256="a" * 64,
                attribution_construction_identity_sha256="b" * 64,
                completion_requirement_sha256=None,
            )
        )
        original = store.prepare_or_load(
            member,
            request,
            lambda _grant: PrimaryPreparation(
                b"original observation", b"original graph", journal, decision, primary, None
            ),
        )
        before = store.usage

    def no_author(_grant):
        raise AssertionError("cold replay authored a replacement primary")

    with opened() as store:
        assert store.session_for(create) == store.session
        assert store.prepare_or_load(member, request, no_author) == original
        assert store.usage == before
        with store.byte_reader(original, "journal") as stream:
            assert stream.read() == journal
        acknowledged = store.accept_decision(original, decision)
        store.accept_custody(
            acknowledged, custody_receipt(42, member.model_dump(mode="json"), primary)
        )
        complete = store.reclaim(member)
        if not complete:
            assert store._usage == before

    with opened() as store:
        for _ in range(64):
            if store.reclaim(member):
                break
            assert store._usage == before
        else:
            raise AssertionError("bounded shared-store fixture reclaim did not finish")
        assert store.load_controls(member).compact
        assert store.usage.pending_bytes == 0
        assert store.usage.sequence == before.sequence + 1
        assert store.usage.control_bytes == before.control_bytes
        after = store.usage

    with opened() as store:
        assert store.load_controls(member).compact
        assert store.reclaim(member)
        assert store.usage == after
