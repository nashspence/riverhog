"""Claim-bound reads of exact canonical provenance, separate from payload retrieval."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import Any, Protocol

from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryImport,
    MemberHistoryStore,
    SourceMemberHistoryBindingProof,
    provenance_structure_object_id,
)
from riverhog_canonical_json import canonical_json_bytes, parse_scalar
from riverhog_protocol import (
    ArtifactId,
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
)
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_provenance import (
    JournalSummary,
    MemberHistoryClosure,
    external_reference,
    selected_delivery_occurrence,
    validate_journal_chunks,
)
from riverhog_provenance_contracts import ExternalReference, require_canonical_uuid_urn

from riverhog_client.processing.models import ClaimedArtifact


class ClaimedProvenanceApi(Protocol):
    def get_collection_provenance_structure(
        self, collection_id: int, object_id: str, *, archive_root_sha256: str
    ) -> bytes: ...
    def get_collection_artifact_history_binding_proof(
        self, collection_id: int, artifact_id: ArtifactId, *, archive_root_sha256: str
    ) -> SourceMemberHistoryBindingProof: ...
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
        end: int | None,
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
        self.history_binding = MemberHistoryBinding.from_mapping(value.get("history_binding"))
        self.history = self.history_binding.verify_descriptor(
            canonical_json_bytes(value.get("member_history"))
        )
        if (
            (self.history.artifact_id, self.history.bytes, self.history.sha256)
            != (artifact.artifact_id, artifact.bytes, artifact.sha256)
            or self.history.primary.journal.to_mapping() != binding.journal.model_dump(mode="json")
            or self.history.primary.delivery_association_id != binding.delivery_association_id
        ):
            raise RuntimeError("member history differs from its exact primary/member binding")
        self.history_store = MemberHistoryStore(self._read_structure)
        self.verify_root()

    def _read_structure(self, path: str) -> Iterator[bytes]:
        self.heartbeat()
        self.verify_root()
        content = self.api.get_collection_provenance_structure(
            self.artifact.root.collection_id,
            provenance_structure_object_id(path),
            archive_root_sha256=self.artifact.root.archive_root_sha256,
        )
        self.verify_root()
        yield content

    def iter_bound_summaries(self) -> Iterator[JournalSummary]:
        """Read each independently selected bound snapshot, preserving local corrections."""

        for selected in self.history_store.roots(self.history_binding, extent=BOUND_HISTORY_EXTENT):
            yield self.summary_at_anchor(selected.journal)

    def source_binding_proof(self) -> SourceMemberHistoryBindingProof:
        self.heartbeat()
        self.verify_root()
        proof = self.api.get_collection_artifact_history_binding_proof(
            self.artifact.root.collection_id,
            self.artifact.artifact_id,
            archive_root_sha256=self.artifact.root.archive_root_sha256,
        )
        if proof.binding != self.history_binding:
            raise RuntimeError("source proof substitutes another member history")
        self.verify_root()
        return proof

    def history_import(self, *, extent: str) -> MemberHistoryImport:
        """Bind an explicitly accepted transfer to the exact delivered input State."""

        # Validation precedes accepting a transfer. Missing supplemental records
        # fail here, including when the caller chose the broader retained extent.
        with self.history_closure(extent=extent):
            proof = self.source_binding_proof()
            primary = self.bound_summary()
            state, _ = selected_delivery_occurrence(
                primary,
                binding={
                    "artifact_id": self.history.artifact_id,
                    **self.history.primary.to_mapping(),
                },
                artifact_id=self.history.artifact_id,
                byte_count=self.history.bytes,
                sha256=self.history.sha256,
                member_role=COLLECTION_MEMBER_ROLE,
            )
            return MemberHistoryImport.from_mapping(
                {
                    "source": {
                        "identity": proof.source_identity,
                        "collection_id": str(proof.collection_id),
                        "archive_root_sha256": self.artifact.root.archive_root_sha256,
                        "artifact_set_identity": self.artifact.root.artifact_set_identity,
                        "artifact_id": str(self.artifact.artifact_id),
                    },
                    "history_sha256": self.history_binding.history_sha256,
                    "source_binding_proof_sha256": proof.identity,
                    "extent": extent,
                    "input_state": external_reference(primary, state["id"]),
                }
            )

    def _read_journal(self, journal_id: str, end: int | None) -> Iterator[bytes]:
        selected = next((row for row in self.iter_journals() if row.journal_id == journal_id), None)
        if selected is None:
            raise ValueError("required history journal is outside the selected archive")
        with self.stream_journal(selected, end=end) as chunks:
            yield from chunks

    def journal_prefix(self, journal_id: str, byte_count: int) -> Iterator[bytes]:
        """Read an exact selected dependency from the claim-pinned archive."""
        yield from self._read_journal(journal_id, byte_count)

    @contextmanager
    def history_closure(self, *, extent: str) -> Iterator[MemberHistoryClosure]:
        """Retain exact history without borrowing other collections' live authority."""

        with MemberHistoryClosure(
            self.history_store, self._read_journal, member_role=COLLECTION_MEMBER_ROLE
        ) as closure:
            closure.resolve(self.history_binding, extent=extent)
            self.verify_root()
            yield closure
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
    def stream_journal(
        self, journal: ProvenanceJournal, *, end: int | None = None
    ) -> Iterator[Iterator[bytes]]:
        self.heartbeat()
        self.verify_root()
        with self.api.stream_collection_provenance_journal(
            self.artifact.root.collection_id,
            journal.journal_id,
            expected_bytes=journal.bytes,
            expected_sha256=journal.sha256,
            end=end,
        ) as chunks:

            def guarded() -> Iterator[bytes]:
                for chunk in chunks:
                    self.heartbeat()
                    yield chunk

            yield guarded()
        self.verify_root()

    def bound_summary(self) -> JournalSummary:
        """Validate the exact primary prefix selected by this member binding."""

        return self.summary_at_anchor(self.history.primary.journal)

    def summary_at_anchor(self, anchor: HistoryJournalAnchor) -> JournalSummary:
        journal = next(
            (item for item in self.iter_journals() if item.journal_id == anchor.journal_id),
            None,
        )
        if journal is None or anchor.prefix_bytes > journal.bytes:
            raise RuntimeError("selected provenance journal is absent or shorter than its anchor")
        with self.stream_journal(journal, end=int(anchor.prefix_bytes)) as chunks:
            return validate_journal_chunks(
                chunks,
                expected_anchor=anchor.to_mapping(),
                require_exact_tail=True,
                require_profiles=False,
            )

    def resolve_external_reference(self, reference: Mapping[str, Any]) -> dict[str, Any]:
        """Verify an exact foreign assertion in this collection's frozen corpus."""

        selected = ExternalReference.model_validate(reference)
        journal = next(
            (item for item in self.iter_journals() if item.journal_id == selected.journal_id),
            None,
        )
        if journal is None:
            raise ValueError("referenced canonical journal is absent from the selected root")
        with self.stream_journal(journal) as chunks:
            summary = validate_journal_chunks(chunks, require_profiles=False)
        if summary.journal_id != selected.journal_id:
            raise ValueError("referenced canonical journal identity differs")
        expected_entry = selected.entry.model_dump(mode="json")
        frame = next((item for item in summary.frames if item.reference == expected_entry), None)
        if frame is None:
            raise ValueError("referenced canonical entry anchor is absent")
        for rows in frame.document["body"].get("assertions", {}).values():
            for row in rows:
                if row["assertion_id"] != selected.assertion_id:
                    continue
                if row["id"] != selected.object_id or row["type"] != selected.object_type:
                    raise ValueError("referenced canonical assertion differs")
                return {
                    "journal_id": selected.journal_id,
                    "entry": expected_entry,
                    "assertion_id": selected.assertion_id,
                    "object_id": selected.object_id,
                    "object_type": selected.object_type,
                }
        raise ValueError("referenced canonical assertion is absent")


__all__ = ["ClaimedProvenance", "ClaimedProvenanceApi", "ProvenanceJournal"]
