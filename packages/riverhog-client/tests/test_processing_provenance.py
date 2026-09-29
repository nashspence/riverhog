from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from riverhog_client.processing import ClaimedArtifact, ClaimedCollectionReader
from riverhog_protocol.collection_workflows import CollectionRootIdentity


class ProvenanceApi:
    def __init__(self) -> None:
        self.root = CollectionRootIdentity(1, "a" * 64, "b" * 64)
        self.artifact = ClaimedArtifact(self.root, "c" * 64, 3, hashlib.sha256(b"abc").hexdigest())
        self.journal_id = "urn:uuid:11111111-1111-4111-8111-111111111111"
        self.entry_id = "urn:uuid:22222222-2222-4222-8222-222222222222"
        self.association_id = "urn:uuid:33333333-3333-4333-8333-333333333333"
        self.journal = b"journal"
        self.calls: list[str] = []

    def get_collection(self, collection_id: int) -> dict[str, Any]:
        assert collection_id == 1
        return {
            "id": "1",
            "archive_root_sha256": self.root.archive_root_sha256,
            "artifact_set_identity": self.root.artifact_set_identity,
        }

    def get_collection_artifact_provenance(
        self, collection_id: int, artifact_id: str
    ) -> dict[str, Any]:
        self.calls.append("detail")
        return {
            "collection_id": str(collection_id),
            "archive_root_sha256": self.root.archive_root_sha256,
            "artifact": {
                "artifact_id": artifact_id,
                "bytes": "3",
                "sha256": self.artifact.sha256,
            },
            "binding": {
                "artifact_id": artifact_id,
                "journal": {
                    "journal_id": self.journal_id,
                    "through": {
                        "entry_id": self.entry_id,
                        "sequence": "0",
                        "json_sha256": "d" * 64,
                    },
                    "prefix_sha256": "e" * 64,
                    "prefix_bytes": "7",
                },
                "delivery_association_id": self.association_id,
            },
        }

    def list_collection_provenance_journals(
        self,
        collection_id: int,
        *,
        page_size: int,
        after_journal_id: str | None,
        archive_root_sha256: str | None,
    ) -> dict[str, Any]:
        assert collection_id == 1 and page_size == 200
        assert after_journal_id is None
        assert archive_root_sha256 is None
        self.calls.append("journals")
        return {
            "collection_id": "1",
            "archive_root_sha256": self.root.archive_root_sha256,
            "journals": [
                {
                    "journal_id": self.journal_id,
                    "bytes": str(len(self.journal)),
                    "sha256": hashlib.sha256(self.journal).hexdigest(),
                }
            ],
            "next_journal_id": None,
        }

    @contextmanager
    def stream_collection_provenance_journal(
        self,
        collection_id: int,
        journal_id: str,
        *,
        expected_bytes: int,
        expected_sha256: str,
    ) -> Iterator[Iterator[bytes]]:
        assert collection_id == 1 and journal_id == self.journal_id
        assert expected_bytes == len(self.journal)
        assert expected_sha256 == hashlib.sha256(self.journal).hexdigest()
        self.calls.append("stream")
        yield iter((self.journal,))


def _reader(api: ProvenanceApi) -> ClaimedCollectionReader:
    return ClaimedCollectionReader(
        api,
        inputs=(api.root,),
        work_id="f" * 64,
        claim_id="claim",
        fence=1,
    )


def test_claimed_provenance_verifies_member_root_and_streams_exact_journal() -> None:
    api = ProvenanceApi()
    view = _reader(api).provenance(api.artifact)
    assert view.binding.artifact_id == api.artifact.artifact_id
    journals = tuple(view.iter_journals())
    assert len(journals) == 1
    with view.stream_journal(journals[0]) as chunks:
        assert b"".join(chunks) == api.journal
    assert api.calls == ["detail", "journals", "stream"]


def test_claimed_provenance_rejects_unselected_artifact_and_changed_root() -> None:
    api = ProvenanceApi()
    reader = _reader(api)
    other = ClaimedArtifact(
        CollectionRootIdentity(2, "a" * 64, "b" * 64),
        api.artifact.artifact_id,
        api.artifact.bytes,
        api.artifact.sha256,
    )
    with pytest.raises(PermissionError):
        reader.provenance(other)
    api.root = CollectionRootIdentity(1, "d" * 64, "b" * 64)
    with pytest.raises(RuntimeError, match="claimed collection root changed"):
        reader.provenance(api.artifact)
