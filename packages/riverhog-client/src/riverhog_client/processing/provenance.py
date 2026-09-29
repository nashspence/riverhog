"""Claim-bound reads of exact canonical provenance, separate from payload retrieval."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import Any, Protocol

from riverhog_canonical_json import parse_scalar
from riverhog_protocol import (
    ArtifactId,
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
)
from riverhog_provenance_contracts import require_canonical_uuid_urn

from riverhog_client.processing.models import ClaimedArtifact


class ClaimedProvenanceApi(Protocol):
    def get_collection_artifact_provenance(
        self, collection_id: int, artifact_id: ArtifactId
    ) -> dict[str, Any]: ...

    def list_collection_provenance_journals(
        self,
        collection_id: int,
        *,
        page_size: int,
        after_journal_id: str | None,
        archive_root_sha256: str | None,
    ) -> dict[str, Any]: ...

    def stream_collection_provenance_journal(
        self,
        collection_id: int,
        journal_id: str,
        *,
        expected_bytes: int,
        expected_sha256: str,
    ) -> AbstractContextManager[Iterator[bytes]]: ...


@dataclass(frozen=True, slots=True)
class ProvenanceJournal:
    journal_id: str
    bytes: int
    sha256: str


class ClaimedProvenance:
    """One exact member binding and root-pinned, paged journal corpus."""

    def __init__(
        self,
        api: ClaimedProvenanceApi,
        *,
        artifact: ClaimedArtifact,
        verify_root: Callable[[], None],
        heartbeat: Callable[[], None],
    ) -> None:
        self.api = api
        self.artifact = artifact
        self.verify_root = verify_root
        self.heartbeat = heartbeat
        self.verify_root()
        self.heartbeat()
        value = api.get_collection_artifact_provenance(
            artifact.root.collection_id, artifact.artifact_id
        )
        if (
            not isinstance(value, Mapping)
            or value.get("collection_id") != str(artifact.root.collection_id)
            or value.get("archive_root_sha256") != artifact.root.archive_root_sha256
        ):
            raise RuntimeError("provenance detail differs from the claimed collection root")
        member = ArtifactMemberIdentityDocument.model_validate(value.get("artifact"))
        if (
            member.artifact_id != artifact.artifact_id
            or int(member.bytes) != artifact.bytes
            or member.sha256 != artifact.sha256
        ):
            raise RuntimeError("provenance detail differs from the claimed artifact")
        binding = CollectionArtifactProvenanceBindingDocument.model_validate(value.get("binding"))
        if binding.artifact_id != artifact.artifact_id:
            raise RuntimeError("provenance binding names another artifact")
        self.binding = binding
        self.verify_root()

    def iter_journals(self) -> Iterator[ProvenanceJournal]:
        after: str | None = None
        while True:
            self.heartbeat()
            self.verify_root()
            page = self.api.list_collection_provenance_journals(
                self.artifact.root.collection_id,
                page_size=200,
                after_journal_id=after,
                archive_root_sha256=(
                    self.artifact.root.archive_root_sha256 if after is not None else None
                ),
            )
            if (
                not isinstance(page, Mapping)
                or page.get("collection_id") != str(self.artifact.root.collection_id)
                or page.get("archive_root_sha256") != self.artifact.root.archive_root_sha256
            ):
                raise RuntimeError("provenance journal page differs from the claimed root")
            rows = page.get("journals")
            if not isinstance(rows, list) or len(rows) > 200:
                raise RuntimeError("provenance journal page has invalid extent")
            for row in rows:
                if not isinstance(row, Mapping):
                    raise RuntimeError("provenance journal summary is invalid")
                raw_id = row.get("journal_id")
                if not isinstance(raw_id, str):
                    raise RuntimeError("provenance journal identity is invalid")
                journal_id = require_canonical_uuid_urn(raw_id)
                size = parse_scalar("nonnegative", row.get("bytes"))
                sha256 = row.get("sha256")
                if (
                    size < 1
                    or not isinstance(sha256, str)
                    or len(sha256) != 64
                    or any(character not in "0123456789abcdef" for character in sha256)
                    or (after is not None and journal_id <= after)
                ):
                    raise RuntimeError("provenance journal summary is not canonical")
                after = journal_id
                yield ProvenanceJournal(journal_id, size, sha256)
            next_id = page.get("next_journal_id")
            if next_id is None:
                self.verify_root()
                return
            if not rows or next_id != after:
                raise RuntimeError("provenance journal continuation is invalid")

    @contextmanager
    def stream_journal(self, journal: ProvenanceJournal) -> Iterator[Iterator[bytes]]:
        self.heartbeat()
        self.verify_root()
        with self.api.stream_collection_provenance_journal(
            self.artifact.root.collection_id,
            journal.journal_id,
            expected_bytes=journal.bytes,
            expected_sha256=journal.sha256,
        ) as chunks:

            def guarded() -> Iterator[bytes]:
                for chunk in chunks:
                    self.heartbeat()
                    yield chunk

            yield guarded()
        self.verify_root()


__all__ = ["ClaimedProvenance", "ClaimedProvenanceApi", "ProvenanceJournal"]
