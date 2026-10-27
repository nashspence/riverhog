"""Explicit publication-state fixtures for component boundaries."""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from riverhog_client import PrimaryScope, PrimaryStateError, PrimaryStatePolicy, PrimaryStore
from riverhog_client._prepared_primary_closing import (
    advance_closing,
    advance_custody,
    freeze_inventory,
)
from riverhog_client._prepared_primary_documents import PrimarySession, primary_control_bytes
from riverhog_client._prepared_primary_store import PrimaryPreparation
from riverhog_client._prepared_primary_types import PrimaryFileSpace
from riverhog_client.producer import ProducerArtifactIdentity, ProducerFile
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
    CollectionUploadCustodyObjectDocument,
    CollectionUploadProvenanceCustodyObjectDocument,
)

PRIMARY_STATE_POLICY = PrimaryStatePolicy(
    scope=PrimaryScope(service="fixture-service", account="fixture-account")
)
PRIMARY_STATE_DOCUMENT = PRIMARY_STATE_POLICY.model_dump(mode="json")


def inventory_from_rows(rows: Iterable[Mapping[str, Any]]):
    for row in sorted(rows, key=lambda row: str(row["artifact_id"])):
        yield ProducerArtifactIdentity(row["artifact_id"], int(row["bytes"]), str(row["sha256"]))


def custody_receipt(collection_id, row, primary, requirement=None):
    return CollectionUploadArtifactCustodyReceiptDocument.seal(
        collection_id=collection_id,
        artifact_id=row["artifact_id"],
        bytes=int(row["bytes"]),
        sha256=row["sha256"],
        primary=primary,
        completion_requirement_sha256=requirement,
        archive_objects=(
            CollectionUploadCustodyObjectDocument(
                volume_id="pack-" + "0" * 64, sealed_receipt_sha256="a" * 64
            ),
        ),
        provenance_objects=(
            CollectionUploadProvenanceCustodyObjectDocument(
                object_id="fixture-primary",
                relative_path="provenance/payloads/fixture.bin.age",
                plaintext_bytes=str(primary.journal.prefix_bytes),
                plaintext_sha256=primary.journal.prefix_sha256,
                sealed_receipt_sha256="b" * 64,
            ),
        ),
    )


def settle_stub_publication(
    store: PrimaryStore, files: Iterable[ProducerFile], *, collection_id: int
) -> None:
    """Model the producer acknowledgement contract without claiming a Native execution."""
    create = b"fixture-complete-creation-inputs"
    if store.session_for(create) is None:
        store.bind_session(
            PrimarySession.model_validate(
                {
                    "allocation_sha256": hashlib.sha256(
                        primary_control_bytes(store.allocation)
                    ).hexdigest(),
                    "create_request": {
                        "bytes": str(len(create)),
                        "sha256": hashlib.sha256(create).hexdigest(),
                    },
                    "collection_id": str(collection_id),
                    "delivery_context_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
                    "server_construction_identity_sha256": "c" * 64,
                    "attribution_construction_identity_sha256": "d" * 64,
                    "completion_requirement_sha256": None,
                }
            )
        )
    members = []
    journal = b"fixture retained primary"
    for source in files:
        content = source.source.read_bytes()
        member = ArtifactMemberIdentityDocument.model_validate(
            {
                "artifact_id": source.artifact_id,
                "bytes": str(len(content)),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )
        members.append(member)
        primary = CollectionArtifactProvenanceBindingDocument.model_validate(
            {
                "artifact_id": member.artifact_id,
                "journal": {
                    "journal_id": "urn:uuid:"
                    + str(uuid.uuid5(uuid.NAMESPACE_OID, str(member.artifact_id))),
                    "through": {
                        "entry_id": "urn:uuid:22222222-2222-4222-8222-222222222222",
                        "sequence": "0",
                        "json_sha256": "e" * 64,
                    },
                    "prefix_bytes": str(len(journal)),
                    "prefix_sha256": hashlib.sha256(journal).hexdigest(),
                },
                "delivery_association_id": "urn:uuid:33333333-3333-4333-8333-333333333333",
            }
        )
        record = store.prepare_or_load(
            member,
            b"fixture-exact-member-inputs",
            lambda _grant, primary=primary: PrimaryPreparation(
                b"fixture-observation", b"{}", journal, b"fixture-decision", primary, None
            ),
        )
        store.accept_decision(record, b"fixture-decision")
        store.accept_custody(
            record,
            custody_receipt(collection_id, member.model_dump(mode="json"), primary),
        )
        store.reclaim(member)

    def no_replay(_member):
        raise AssertionError("stub publication lost a retained acknowledgement")

    freeze_inventory(store, sorted(members, key=lambda member: str(member.artifact_id)))
    assert advance_closing(store, limit=max(1, len(members)), settle=no_replay)
    assert advance_custody(store, limit=max(1, len(members)), settle=no_replay)


class SqlitePrimaryTestGuard:
    """SQL-only fixture; native ownership and NTFS are qualified separately."""

    vfs = "unix"

    def __init__(self, path: Path):
        self.path = path
        self.closed = False

    def check_current(self):
        if self.closed:
            raise PrimaryStateError("fixture owner closed")

    def check_journal(self, *, required, recovering=False):
        self.check_current()
        if required:
            assert Path(str(self.path) + "-journal").is_file()

    def file_space(self):
        self.check_current()

        def space(path):
            info = path.stat()
            return PrimaryFileSpace(info.st_size, info.st_blocks * 512)

        return space(self.path), space(Path(str(self.path) + "-journal"))

    def close(self):
        self.closed = True
