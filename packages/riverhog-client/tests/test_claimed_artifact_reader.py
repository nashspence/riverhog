from __future__ import annotations

from riverhog_client.processing.reader import ClaimedCollectionReader
from riverhog_protocol import PortableCollectionInventoryPage
from riverhog_protocol.collection_workflows import CollectionRootIdentity


class _InventoryApi:
    def __init__(self, root: CollectionRootIdentity) -> None:
        self.root = root

    def get_collection(self, collection_id: int) -> dict[str, object]:
        assert collection_id == self.root.collection_id
        return {
            "id": str(collection_id),
            "archive_root_sha256": self.root.archive_root_sha256,
            "artifact_set_identity": self.root.artifact_set_identity,
        }

    def get_portable_collection_inventory(
        self,
        collection_id: int,
        *,
        cursor: str | None,
        limit: int,
        inventory_identity: str | None,
    ) -> PortableCollectionInventoryPage:
        assert collection_id == self.root.collection_id
        assert cursor is None and inventory_identity is None and limit == 1000
        return PortableCollectionInventoryPage.model_validate(
            {
                "authority": {
                    "header": {
                        "collection": "7",
                        "artifact_set_identity": self.root.artifact_set_identity,
                        "encryption_format": "age-v1-scrypt",
                        "passphrase_id": "test-passphrase-1",
                        "provenance_identity": "c" * 64,
                    },
                    "inventory_identity": "d" * 64,
                    "artifact_count": "2",
                    "artifact_bytes": "8",
                },
                "artifacts": [
                    {"artifact_id": "1" * 64, "bytes": "4", "sha256": "a" * 64},
                    {"artifact_id": "2" * 64, "bytes": "4", "sha256": "a" * 64},
                ],
                "complete": True,
            }
        )


def test_claimed_inventory_preserves_same_byte_members_as_distinct_ids() -> None:
    root = CollectionRootIdentity(7, "a" * 64, "b" * 64)
    reader = ClaimedCollectionReader(
        _InventoryApi(root),
        inputs=(root,),
        work_id="e" * 64,
        claim_id="claim-1",
        fence=1,
    )
    members = tuple(reader.iter_inventory())
    assert [member.artifact_id for member in members] == ["1" * 64, "2" * 64]
    assert members[0].sha256 == members[1].sha256
    assert members[0].key != members[1].key
    assert "path" not in members[0].as_dict()
