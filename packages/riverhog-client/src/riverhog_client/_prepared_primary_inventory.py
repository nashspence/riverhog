"""Canonical owner inventory validation above backend-owned ordering scratch."""

from __future__ import annotations

from collections.abc import Generator, Iterable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.artifact_identity import ArtifactId

from riverhog_client._prepared_primary_storage import RetainedPrimaryOrdering

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_store import PrimaryStore
    from riverhog_client.producer import ProducerArtifactIdentity


def ordered_primary_inventory(
    store: PrimaryStore, members: Iterable[ProducerArtifactIdentity]
) -> Iterator[ProducerArtifactIdentity]:
    """Order once in admitted derived scratch, with O(N log N) indexed work."""
    from riverhog_client.producer import ProducerArtifactIdentity

    with store._mutex:
        store._admit_ordering()
        with store._files.ordered_rows(
            _rows(members), byte_limit=store.limits.inventory_order_bytes
        ) as selected:
            for key, byte_count, digest in selected.rows():
                yield ProducerArtifactIdentity(ArtifactId(key), byte_count, digest)


def _rows(members: Iterable[ProducerArtifactIdentity]) -> Iterator[tuple[str, int, str]]:
    for supplied in members:
        member = ArtifactMemberIdentityDocument.model_validate(
            {
                "artifact_id": supplied.artifact_id,
                "bytes": str(supplied.bytes),
                "sha256": supplied.sha256,
            }
        )
        yield str(member.artifact_id), member.bytes, member.sha256


@dataclass(frozen=True, slots=True)
class PreparedPrimaryInventory:
    """One canonical derivation, replayable only under its original live store.

    The handle retains no members, pager, cursor or selected ordering lease.
    Opening never rebuilds or prepares storage. Explicit cleanup, replacement
    or owner closure invalidates it; cold execution must validate original
    source authority and prepare a new derivation.
    """

    _store: PrimaryStore
    _ordering: RetainedPrimaryOrdering

    @property
    def count(self) -> int:
        return self._ordering.count

    @contextmanager
    def open(self) -> Iterator[Iterator[ProducerArtifactIdentity]]:
        from riverhog_client.producer import ProducerArtifactIdentity

        def members() -> Generator[ProducerArtifactIdentity, None, None]:
            for key, byte_count, digest in selected.rows():
                yield ProducerArtifactIdentity(ArtifactId(key), byte_count, digest)

        with self._store._mutex:
            self._store._admit_ordering()
            with self._ordering.open() as selected:
                with closing(members()) as replay:
                    yield replay


def prepare_primary_inventory(
    store: PrimaryStore, members: Iterable[ProducerArtifactIdentity]
) -> PreparedPrimaryInventory:
    """Validate and order once in execution, retaining only a derived replay handle."""
    with store._mutex:
        store._admit_ordering()
        with store._files.ordered_rows(
            _rows(members), byte_limit=store.limits.inventory_order_bytes
        ) as selected:
            retained = selected.retain()
    return PreparedPrimaryInventory(store, retained)
