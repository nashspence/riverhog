"""Archive reads over encrypted bytes produced by the real upload services."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator, Mapping

from riverhog_age import decrypt_age_scrypt
from riverhog_core.ports.archive_store import ArchiveArtifactRead, ArchiveObjectUploadReceipt

from tests.unit.archive_object_fixtures import MemoryArchiveStore


class UploadedArchiveStore(MemoryArchiveStore):
    """Keep publication tests on stored bytes rather than a prebuilt archive fixture."""

    def __init__(
        self,
        *,
        passphrases: Mapping[str, str],
        read_stored: Callable[[str], bytes] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.passphrases = passphrases
        self.read_stored = read_stored or self.objects.__getitem__

    def iter_archive_object(
        self, *, collection_id, object, passphrase_id, attribution=None
    ) -> Iterator[bytes]:
        stored = self.read_stored(object.object_path)
        assert len(stored) == object.stored_bytes
        assert hashlib.sha256(stored).hexdigest() == object.stored_sha256
        plaintext = (
            stored
            if object.kind == "recovery-descriptor"
            else decrypt_age_scrypt(stored, self.passphrases[passphrase_id])
        )
        assert len(plaintext) == object.plaintext_bytes
        assert hashlib.sha256(plaintext).hexdigest() == object.sha256
        self.read.append(object.object_id)
        yield plaintext

    def iter_stored_archive_object(
        self, *, collection_id, object, attribution=None
    ) -> Iterator[bytes]:
        stored = self.read_stored(object.object_path)
        assert len(stored) == object.stored_bytes
        assert hashlib.sha256(stored).hexdigest() == object.stored_sha256
        self.read.append(object.object_id)
        yield stored

    def read_archive_artifact(self, *, collection_id, object, passphrase_id) -> ArchiveArtifactRead:
        content = b"".join(
            self.iter_archive_object(
                collection_id=collection_id, object=object, passphrase_id=passphrase_id
            )
        )
        return ArchiveArtifactRead(
            receipt=ArchiveObjectUploadReceipt(
                object_id=object.object_id,
                kind=object.kind,
                object_path=object.object_path,
                plaintext_bytes=object.plaintext_bytes,
                stored_bytes=object.stored_bytes,
                sha256=object.sha256,
                stored_sha256=object.stored_sha256,
                revision=object.revision,
                uploaded_at="2026-10-01T00:00:00.000000000Z",
                verified_at="2026-10-01T00:00:00.000000000Z",
            ),
            content=content,
        )
