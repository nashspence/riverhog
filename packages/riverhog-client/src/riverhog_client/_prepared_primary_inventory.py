"""Canonical owner inventory validation above backend-owned ordering scratch."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.artifact_identity import ArtifactId

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_store import PrimaryStore
    from riverhog_client.producer import ProducerArtifactIdentity


def ordered_primary_inventory(
    store: PrimaryStore, members: Iterable[ProducerArtifactIdentity]
) -> Iterator[ProducerArtifactIdentity]:
    """Order once in admitted derived scratch, with O(N log N) indexed work."""
    from riverhog_client.producer import ProducerArtifactIdentity

    def rows() -> Iterator[tuple[str, int, str]]:
        for supplied in members:
            member = ArtifactMemberIdentityDocument.model_validate(
                {
                    "artifact_id": supplied.artifact_id,
                    "bytes": str(supplied.bytes),
                    "sha256": supplied.sha256,
                }
            )
            yield str(member.artifact_id), member.bytes, member.sha256

    with store._mutex:
        store._admit_ordering()
        with store._files.ordered_rows(
            rows(), byte_limit=store.limits.inventory_order_bytes
        ) as selected:
            for key, byte_count, digest in selected.rows():
                yield ProducerArtifactIdentity(ArtifactId(key), byte_count, digest)
