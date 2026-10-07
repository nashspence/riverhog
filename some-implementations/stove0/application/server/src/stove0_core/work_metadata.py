"""One bounded inventory continuation over exact Riverhog roots or child selection."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Protocol

from riverhog_protocol.portable_collection import PortableCollectionInventoryPage
from stove0_protocol import (
    ArtifactSelectionRef,
    BranchWorkBinding,
    JoinWorkBinding,
    WorkArtifactSubject,
    WorkIdentity,
    canonical_json_sha256,
)

if TYPE_CHECKING:
    from stove0_core.persistence import SqlAlchemyStateStore


class RiverhogInventoryPort(Protocol):
    def get_collection(self, collection_id: int) -> dict[str, Any]: ...

    def get_portable_collection_inventory(
        self,
        collection_id: int,
        *,
        cursor: str | None = None,
        limit: int = 100,
        inventory_identity: str | None = None,
    ) -> PortableCollectionInventoryPage: ...


class WorkInventory:
    def __init__(self, state: SqlAlchemyStateStore, riverhog: RiverhogInventoryPort) -> None:
        self.state, self.riverhog = state, riverhog

    def step(self, work: WorkIdentity) -> ArtifactSelectionRef | None:
        binding = work.fork_join
        if isinstance(binding, BranchWorkBinding):
            reference = self.state.load_selection_ref(binding.artifact_selection_sha256)
            if reference is None:
                raise ValueError("child recipe lost its exact parent-bound selection")
            return reference
        if isinstance(binding, JoinWorkBinding):
            raise ValueError("a join consumes explicit producer output selections")
        builder_id = canonical_json_sha256(
            {"format": "stove0-work-inventory-builder/v1", "work_id": work.work_id}
        )
        builders = self.state.metadata_selections
        row = builders.ensure(
            builder_id,
            binding={
                "work_id": work.work_id,
                "roots": [root.model_dump(mode="json") for root in work.inputs],
            },
            source={"root_ordinal": 0, "cursor": None, "inventory_identity": None},
        )
        if row["state"] != "collecting":
            return builders.seal_step(builder_id)
        source = json.loads(row["source_json"])
        root_ordinal = source["root_ordinal"]
        if root_ordinal >= len(work.inputs):
            builders.append(
                builder_id,
                expected_revision=row["revision"],
                subjects=(),
                source=source,
                complete=True,
            )
            return None
        root = work.inputs[root_ordinal]
        current = self.riverhog.get_collection(root.collection_id)
        if (current.get("archive_root_sha256"), current.get("artifact_set_identity")) != (
            root.archive_root_sha256,
            root.artifact_set_identity,
        ):
            raise ValueError("Riverhog root changed during inventory continuation")
        page = self.riverhog.get_portable_collection_inventory(
            root.collection_id,
            cursor=source["cursor"],
            limit=1000,
            inventory_identity=source["inventory_identity"],
        )
        if (
            source["inventory_identity"] is not None
            and page.authority.inventory_identity != source["inventory_identity"]
        ):
            raise ValueError("Riverhog inventory authority changed during continuation")
        subjects = tuple(
            WorkArtifactSubject.model_validate(
                {
                    "id": "a-"
                    + canonical_json_sha256(
                        {
                            "collection_id": root.collection_id,
                            "artifact_id": str(artifact.artifact_id),
                        }
                    )[:32],
                    "role": "stove0.source/v1",
                    "collection": root,
                    "artifact_id": artifact.artifact_id,
                    "bytes": str(artifact.bytes),
                    "sha256": artifact.sha256,
                }
            )
            for artifact in page.artifacts
        )
        if not page.complete and (
            not subjects or page.next_cursor is None or page.next_cursor == source["cursor"]
        ):
            raise ValueError("Riverhog inventory continuation made no progress")
        continuation = (
            {"root_ordinal": root_ordinal + 1, "cursor": None, "inventory_identity": None}
            if page.complete
            else {
                "root_ordinal": root_ordinal,
                "cursor": page.next_cursor,
                "inventory_identity": page.authority.inventory_identity,
            }
        )
        builders.append(
            builder_id,
            expected_revision=row["revision"],
            subjects=subjects,
            source=continuation,
            complete=page.complete and root_ordinal + 1 == len(work.inputs),
        )
        return None
