from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path

import pytest
from a_riverhog_minisign_witness.minisign import MinisignSigner, public_key_identity
from a_riverhog_minisign_witness.schema import upgrade_state, validate_state
from a_riverhog_minisign_witness.store import Signature, WitnessStore
from riverhog_protocol import (
    CatalogSyncChangePage,
    CatalogSyncCheckpoint,
    CatalogSyncCollectionPage,
    CatalogSyncDeparture,
    CatalogSyncDescriptor,
    CatalogSyncUpsert,
)
from riverhog_protocol.errors import CatalogSyncViewChanged


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
        assert cursor in {"changes", "later"}
        return CatalogSyncChangePage(
            source_identity="a" * 64,
            authorization_view_identity="b" * 64,
            changes=[
                CatalogSyncDeparture(cause="visibility_lost", collection_id="42", revision="4")
            ]
            if cursor == "changes"
            else [],
            next_cursor="later",
            caught_up=True,
            through_revision="4",
        )


class Signer:
    calls = 0

    def sign(self, statement: bytes) -> Signature:
        self.calls += 1
        assert statement.startswith(b"a-riverhog-collection-witness/v1\n")
        return Signature(b"signed", "sha256:" + "1" * 64)


def test_schema_and_independent_evidence_survive_departure_and_view_reset(tmp_path: Path) -> None:
    path = tmp_path / "minisign.db"
    assert upgrade_state(path).condition == "current"
    assert validate_state(path).condition == "current"
    store = WitnessStore(path)
    api = Api()
    assert store.ingest_once(api, now=100).kind == "checkpoint"
    assert store.ingest_once(api, now=100).kind == "catalog"
    with sqlite3.connect(path) as db:
        digest = db.execute("SELECT digest FROM statements").fetchone()[0]
        assert db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
    assert store.sign_once(Signer(), now=100) == digest
    assert store.evidence(digest)["signature"] == b"signed"
    assert store.ingest_once(api, now=100).kind == "changes"
    with sqlite3.connect(path) as db:
        assert (
            db.execute("SELECT departure_cause FROM observations").fetchone()[0]
            == "visibility_lost"
        )
    api.view_changed = True
    assert store.ingest_once(api, now=100).kind == "reset"
    assert store.progress().position.phase == "reset_required"
    store.rebaseline()
    assert store.progress().generation == 1
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM statements").fetchone()[0] == 1
    assert store.evidence(digest)["signature"] == b"signed"


def test_equal_revision_conflict_rolls_back_cursor_and_evidence(tmp_path: Path) -> None:
    path = tmp_path / "minisign.db"
    upgrade_state(path)
    store = WitnessStore(path)
    api = Api()
    store.ingest_once(api, now=100)
    store.ingest_once(api, now=100)
    before = store.progress()

    class ConflictApi(Api):
        def list_catalog_sync_changes(
            self, cursor: str, *, limit: int = 100
        ) -> CatalogSyncChangePage:
            return CatalogSyncChangePage(
                source_identity="a" * 64,
                authorization_view_identity="b" * 64,
                changes=[
                    CatalogSyncUpsert(
                        collection_id="42",
                        archive_root_sha256="9" * 64,
                        content_identity="d" * 64,
                        description=None,
                        description_revision=0,
                        description_identity="e" * 64,
                        tag_revision=1,
                        tag_set_identity="f" * 64,
                        revision="3",
                    )
                ],
                next_cursor="later",
                caught_up=True,
                through_revision="3",
            )

    with pytest.raises(ValueError, match="equal-revision"):
        store.ingest_once(ConflictApi(), now=101)
    assert store.progress() == before
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM statements").fetchone()[0] == 1


def test_real_minisign_key_signs_exact_statement(tmp_path: Path) -> None:
    try:
        executable = subprocess.run(
            ["mise", "which", "minisign"], check=True, capture_output=True, text=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("locked Minisign tool is unavailable")
    secret = tmp_path / "secret.key"
    public = tmp_path / "public.key"
    subprocess.run(
        [executable, "-G", "-W", "-s", str(secret), "-p", str(public)],
        check=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    signer = MinisignSigner(secret, public, executable=executable)
    statement = b"a-riverhog-collection-witness/v1\nexample\n"
    signature = signer.sign(statement)
    assert signature.key_identity == public_key_identity(public)
    message = tmp_path / "statement"
    output = tmp_path / "statement.minisig"
    message.write_bytes(statement)
    output.write_bytes(signature.payload)
    assert (
        subprocess.run(
            [executable, "-Vm", str(message), "-p", str(public), "-x", str(output), "-q"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
        == 0
    )
    message.write_bytes(statement + b"tampered")
    assert (
        subprocess.run(
            [executable, "-Vm", str(message), "-p", str(public), "-x", str(output), "-q"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
        != 0
    )
