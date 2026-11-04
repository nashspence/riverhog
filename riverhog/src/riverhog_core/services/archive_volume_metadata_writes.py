"""Volume format publication through a retained original immutable write actor."""

from __future__ import annotations

from riverhog_archive_contracts import CollectionArchiveTerminalDocument

from riverhog_core.archive_root import ArchiveRootPublisher, SealedArchiveVolumeMetadata
from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.ports.archive_objects import (
    ArchiveObjectWriteExecutor,
    ImmutableArchiveObjectObserver,
)
from riverhog_core.services.archive_volume_metadata_checkpoint import ArchiveVolumeMetadataSource
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites


class ArchiveVolumeMetadataWrites(ImmutableArchiveWrites):
    """Own volume format inputs while the shared actor retains exact invocation state."""

    def __init__(
        self,
        factory: SessionFactory,
        *,
        source: ArchiveVolumeMetadataSource,
        epoch: str,
        grants: ArchiveWriteGrants,
        executor: ArchiveObjectWriteExecutor,
        observer: ImmutableArchiveObjectObserver,
    ) -> None:
        super().__init__(
            factory,
            source=source,
            epoch=epoch,
            grants=grants,
            executor=executor,
            observer=observer,
        )
        self._volume_source = source

    def publish(self, *, passphrase: str, scrypt_log_n: int) -> SealedArchiveVolumeMetadata:
        """Resume original results before spending another encryption step."""
        received = self.resume_original()
        if received is not None:
            return self._volume_source.publication.sealed_receipt(received)
        publisher = ArchiveRootPublisher(
            object_store=self, passphrase=passphrase, scrypt_log_n=scrypt_log_n
        )
        document = self._volume_source.document
        if isinstance(document, CollectionArchiveTerminalDocument):
            return publisher.publish_terminal_metadata(
                archive_storage_prefix=self._volume_source.publication.archive_storage_prefix,
                document=document,
            )
        return publisher.publish_volume_metadata(
            archive_storage_prefix=self._volume_source.publication.archive_storage_prefix,
            document=document,
        )
