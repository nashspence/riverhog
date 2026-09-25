from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from a_riverhog_opentimestamps_witness import proof
from a_riverhog_opentimestamps_witness.schema import upgrade_state, validate_state
from a_riverhog_opentimestamps_witness.store import WitnessStore
from opentimestamps.core.notary import PendingAttestation
from opentimestamps.core.serialize import BytesSerializationContext
from opentimestamps.core.timestamp import Timestamp
from riverhog_protocol import (
    CatalogSyncChangePage,
    CatalogSyncCheckpoint,
    CatalogSyncCollectionPage,
    CatalogSyncDeparture,
    CatalogSyncDescriptor,
)
from riverhog_protocol.errors import CatalogSyncViewChanged

URL = "https://calendar.example"


class Api:
    view_changed = False

    def create_catalog_sync_checkpoint(self) -> CatalogSyncCheckpoint:
        return CatalogSyncCheckpoint(
            source_identity="a" * 64,
            authorization_view_identity="b" * 64,
            catalog_cursor="catalog",
        )

    def list_catalog_sync_collections(
        self, cursor: str, *, limit: int = 100
    ) -> CatalogSyncCollectionPage:
        assert cursor == "catalog"
        return CatalogSyncCollectionPage(
            source_identity="a" * 64,
            authorization_view_identity="b" * 64,
            collections=[
                CatalogSyncDescriptor(
                    collection_id="42",
                    archive_root_sha256="c" * 64,
                    content_identity="d" * 64,
                    description=None,
                    description_revision=0,
                    description_identity="e" * 64,
                    tag_revision=1,
                    tag_set_identity="f" * 64,
                    revision="3",
                )
            ],
            changes_cursor="changes",
        )

    def list_catalog_sync_changes(self, cursor: str, *, limit: int = 100) -> CatalogSyncChangePage:
        if self.view_changed:
            raise CatalogSyncViewChanged("view changed")
        assert cursor == "changes"
        return CatalogSyncChangePage(
            source_identity="a" * 64,
            authorization_view_identity="b" * 64,
            changes=[
                CatalogSyncDeparture(cause="collection_deleted", collection_id="42", revision="4")
            ],
            next_cursor="later",
            caught_up=True,
            through_revision="4",
        )


class Calendar:
    def __init__(self) -> None:
        self.calls = 0

    def request(self, kind: str, url: str, commitment: bytes, max_bytes: int) -> bytes:
        self.calls += 1
        assert kind == "submit" and url == URL and max_bytes == proof.MAX_PROOF
        node = Timestamp(commitment)
        node.attestations.add(PendingAttestation(URL))
        context = BytesSerializationContext()
        node.serialize(context)
        return context.getbytes()


def test_proof_work_is_durable_before_network_and_survives_departure(tmp_path: Path) -> None:
    path = tmp_path / "ots.db"
    assert upgrade_state(path).condition == "current"
    assert validate_state(path).condition == "current"
    store = WitnessStore(path, (URL,))
    api = Api()
    store.ingest_once(api, now=100)
    store.ingest_once(api, now=100)
    with sqlite3.connect(path) as db:
        digest, state = db.execute("SELECT digest, job_state FROM statements").fetchone()
    initial = proof.load_job(state, bytes.fromhex(digest))
    assert initial.proof is None
    assert (
        initial.submission_commitment
        == hashlib.sha256(bytes.fromhex(digest) + initial.nonce).digest()
    )
    calendar = Calendar()
    assert store.mature_once(calendar, now=100) == digest
    assert calendar.calls == 1
    evidence = store.evidence(digest)
    assert isinstance(evidence["proof"], bytes)
    assert len(evidence["proof_revisions"]) == 1
    assert evidence["job"].nonce == initial.nonce
    store.ingest_once(api, now=101)
    api.view_changed = True
    assert store.ingest_once(api, now=102).kind == "reset"
    store.rebaseline()
    with sqlite3.connect(path) as db:
        assert (
            db.execute("SELECT departure_cause FROM observations").fetchone()[0]
            == "collection_deleted"
        )
        assert db.execute("SELECT COUNT(*) FROM proof_history").fetchone()[0] == 1
    assert store.evidence(digest)["proof"] == evidence["proof"]
