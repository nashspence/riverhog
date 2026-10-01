"""Root-verified artifact readouts for finite public qualification corpora."""

from collections.abc import Iterator
from typing import Any

from riverhog_client.processing import ClaimedArtifact
from riverhog_client.processing.provenance import ClaimedProvenance
from riverhog_protocol import ArtifactId
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_protocol.collection_workflows import CollectionRootIdentity
from riverhog_protocol.paths import parse_collection_id_parameter
from riverhog_protocol.provenance_transport import MaterializationHintDocument
from riverhog_provenance import selected_delivery_occurrence


def root_bound_artifacts(
    api: Any, collection_id: int | str
) -> Iterator[tuple[Any, tuple[str, ...] | None]]:
    collection_id = parse_collection_id_parameter(collection_id)
    collection = api.get_collection(collection_id)
    root = CollectionRootIdentity(
        collection_id, collection["archive_root_sha256"], collection["artifact_set_identity"]
    )

    def verify_root() -> None:
        current = api.get_collection(collection_id)
        if (current["archive_root_sha256"], current["artifact_set_identity"]) != (
            root.archive_root_sha256,
            root.artifact_set_identity,
        ):
            raise ValueError("qualification artifact lookup changed its archive root")

    cursor = None
    inventory_identity = None
    while True:
        page = api.get_portable_collection_inventory(
            collection_id, cursor=cursor, limit=1000, inventory_identity=inventory_identity
        )
        if inventory_identity is None:
            inventory_identity = page.authority.inventory_identity
        elif inventory_identity != page.authority.inventory_identity:
            raise ValueError("qualification inventory changed while resolving artifact IDs")
        for member in page.artifacts:
            accepted = ClaimedProvenance(
                api,
                artifact=ClaimedArtifact(
                    root, ArtifactId(member.artifact_id), int(member.bytes), member.sha256
                ),
                verify_root=verify_root,
                heartbeat=lambda: None,
            )
            accepted.source_binding_proof()
            _state, occurrence = selected_delivery_occurrence(
                accepted.bound_summary(),
                binding=accepted.binding.model_dump(mode="json"),
                artifact_id=str(member.artifact_id),
                byte_count=int(member.bytes),
                sha256=member.sha256,
                member_role=COLLECTION_MEMBER_ROLE,
            )
            hint = occurrence.get("materialization_hint")
            components = (
                None
                if hint is None
                else tuple(MaterializationHintDocument.model_validate(hint).components)
            )
            yield member, components
        if page.complete:
            break
        if page.next_cursor is None or page.next_cursor == cursor:
            raise ValueError("qualification inventory continuation does not advance")
        cursor = page.next_cursor
    verify_root()
