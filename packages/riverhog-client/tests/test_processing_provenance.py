from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from typing import Any

import pytest
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    ArchiveProvenanceIdentity,
    CollectionArchiveManifest,
    CollectionArtifactSetIdentity,
    MemberHistoryBuilder,
    MemberHistoryImport,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    MemberHistoryStore,
    ProvenanceRootDocument,
    ProvenanceRootIdentity,
    SourceMemberHistoryBindingProof,
    binding_tree_commitment,
    provenance_structure_identity,
    provenance_structure_object_id,
)
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_client.processing import ClaimedArtifact, ClaimedCollectionReader
from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_protocol.collection_workflows import CollectionRootIdentity
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    MemberHistoryClosure,
    append_correction,
    assertion,
    create_journal,
    external_reference,
    validate_journal,
)


class ProvenanceApi:
    def __init__(self) -> None:
        self.root = CollectionRootIdentity(1, "a" * 64, "b" * 64)
        self.artifact = ClaimedArtifact(self.root, "c" * 64, 3, hashlib.sha256(b"abc").hexdigest())
        source = BoundedSourceObserver().observe(BytesSource(b"abc"))
        produced = build_member_journal(
            member=ArtifactMemberIdentityDocument.model_validate(
                {
                    "artifact_id": self.artifact.artifact_id,
                    "bytes": "3",
                    "sha256": self.artifact.sha256,
                }
            ),
            observation=source,
            delivery_context_id="urn:uuid:44444444-4444-4444-8444-444444444444",
            attribution=ProducerAttribution(
                "fixture", "fixture/v1", "v1", "event", "fixture", {}, "f" * 64
            ),
            materialization_hint=None,
        )
        self.journal = produced.content
        self.summary = validate_journal(self.journal, require_profiles=False)
        self.journal_id = self.summary.journal_id
        self.journals = {self.journal_id: self.journal}
        self.entry_id = self.summary.tail.reference["entry_id"]
        self.association_id = produced.binding.delivery_association_id
        self.binding = produced.binding
        with MemberHistoryBuilder(
            artifact_id=self.artifact.artifact_id,
            bytes=3,
            sha256=self.artifact.sha256,
            primary=MemberHistoryPrimary.from_mapping(
                {
                    "journal": produced.binding.journal.model_dump(mode="json"),
                    "delivery_association_id": self.association_id,
                }
            ),
        ) as builder:
            self.history_binding, self.history = builder.seal()
            self.objects = {
                provenance_structure_identity(raw).object_id: raw for raw in builder.objects()
            }
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
            "binding": self.binding.model_dump(mode="json"),
            "history_binding": self.history_binding.to_mapping(),
            "member_history": self.history.to_mapping(),
        }

    def get_collection_provenance_structure(
        self, collection_id: int, object_id: str, *, archive_root_sha256: str
    ) -> bytes:
        assert collection_id == 1 and archive_root_sha256 == self.root.archive_root_sha256
        return self.objects[object_id]

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
                    "journal_id": identity,
                    "bytes": str(len(content)),
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
                for identity, content in sorted(self.journals.items())
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
        end: int | None = None,
    ) -> Iterator[Iterator[bytes]]:
        content = self.journals[journal_id]
        assert collection_id == 1
        assert expected_bytes == len(content)
        assert expected_sha256 == hashlib.sha256(content).hexdigest()
        self.calls.append("stream")
        yield iter((content[:end],))


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
    assert view.bound_summary().anchor == api.summary.anchor
    with view.stream_journal(journals[0]) as chunks:
        assert b"".join(chunks) == api.journal
    assert api.calls == ["detail", "journals", "journals", "stream", "stream"]


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


def test_claimed_provenance_resolves_only_exact_foreign_assertions() -> None:
    api = ProvenanceApi()
    view = _reader(api).provenance(api.artifact)
    reference = external_reference(api.summary, api.summary.states[0]["id"])
    resolved = view.resolve_external_reference(reference)
    assert resolved == {key: value for key, value in reference.items() if key != "scope"}
    changed = {**reference, "object_id": api.summary.graph["occurrences"][0]["id"]}
    with pytest.raises(ValueError, match="differs"):
        view.resolve_external_reference(changed)


def _add_selected(api: ProvenanceApi, *, inclusion: str = "bound", retract: bool = False) -> str:
    who = api.summary.graph["agents"][0]["id"]
    row = assertion(
        "extension",
        who,
        subject=external_reference(api.summary, api.summary.states[0]["id"]),
        property="urn:test:late-claim",
        value={"type": "text", "value": "late"},
    )
    content = create_journal(
        {"agents": api.summary.graph["agents"], "extensions": [row]},
        recorded_by_agent_id=who,
    )
    snapshot = validate_journal(content, require_profiles=False)
    if retract:
        content = append_correction(
            content,
            reason="retire the local late claim",
            retracts=[{"entry": snapshot.tail.reference, "assertion_id": row["assertion_id"]}],
            recorded_by_agent_id=who,
        )
        snapshot = validate_journal(content, require_profiles=False)
    api.journals[snapshot.journal_id] = content
    with MemberHistoryBuilder(
        artifact_id=api.history.artifact_id,
        bytes=api.history.bytes,
        sha256=api.history.sha256,
        primary=api.history.primary,
    ) as builder:
        builder.add_root(
            MemberHistoryRoot.from_mapping({"journal": snapshot.anchor, "inclusion": inclusion})
        )
        api.history_binding, api.history = builder.seal()
        for raw in builder.objects():
            api.objects[provenance_structure_identity(raw).object_id] = raw
    return snapshot.journal_id


def _source_proof(api: ProvenanceApi) -> SourceMemberHistoryBindingProof:
    provenance = ProvenanceRootDocument(
        archive_generation="1" * 64,
        artifact_set_sha256=api.root.artifact_set_identity,
        delivery_context_id=api.summary.graph["delivery_associations"][0]["delivery_context_id"],
        binding_count=1,
        binding_tree_sha256=binding_tree_commitment((api.history_binding,)).root_sha256,
        journal_count=len(api.journals),
        ordered_volume_sha256="e" * 64,
    )
    archive = CollectionArchiveManifest(
        archive_generation=provenance.archive_generation,
        artifact_set=CollectionArtifactSetIdentity(
            1, api.artifact.bytes, provenance.artifact_set_sha256
        ),
        ordered_volume_sha256="f" * 64,
        provenance=ArchiveProvenanceIdentity(
            provenance.identity,
            ProvenanceRootIdentity(
                id="provenance-root",
                kind="provenance-root",
                path="provenance/root.json.age",
                plaintext_bytes=len(provenance.to_json_bytes()),
                sha256=provenance.identity,
                stored_bytes=1234,
                stored_sha256="0" * 64,
            ),
        ),
    )
    return SourceMemberHistoryBindingProof(
        "a" * 64, 1, archive.to_json_bytes(), provenance.to_json_bytes(), api.history_binding, 0, ()
    )


def test_history_closure_keeps_retracted_late_bytes_and_does_not_widen_extent() -> None:
    api = ProvenanceApi()
    late = _add_selected(api, inclusion="retained", retract=True)
    view = _reader(api).provenance(api.artifact)
    with view.history_closure(extent=BOUND_HISTORY_EXTENT) as closure:
        assert [anchor.journal_id for anchor in closure.journal_anchors()] == [api.journal_id]
        assert len(tuple(closure.structure_objects())) >= 3
    with view.history_closure(extent=RETAINED_HISTORY_EXTENT) as closure:
        assert {anchor.journal_id for anchor in closure.journal_anchors()} == {api.journal_id, late}
        selected = next(a for a in closure.journal_anchors() if a.journal_id == late)
        summary = closure.summary_at(selected)
        assert not summary.graph.get("extensions")
        assert any(
            frame.document["body"].get("assertions", {}).get("extensions")
            for frame in summary.frames
        )
    api.journals.pop(late)
    with view.history_closure(extent=BOUND_HISTORY_EXTENT):
        pass
    with pytest.raises(ValueError, match="outside the selected archive"):
        with view.history_closure(extent=RETAINED_HISTORY_EXTENT):
            pass


def test_derivative_import_retains_late_input_history_and_checks_exact_source_state() -> None:
    source = ProvenanceApi()
    late = _add_selected(source)
    proof = _source_proof(source)
    imported = MemberHistoryImport(
        proof.source_identity,
        proof.collection_id,
        hashlib.sha256(proof.archive_root).hexdigest(),
        source.root.artifact_set_identity,
        source.history.artifact_id,
        source.history.identity,
        proof.identity,
        BOUND_HISTORY_EXTENT,
        external_reference(source.summary, source.summary.states[0]["id"]),
    )
    derivative = ProvenanceApi()
    objects = {**source.objects, **derivative.objects}
    objects[provenance_structure_identity(proof.to_json_bytes()).object_id] = proof.to_json_bytes()
    journals = {**source.journals, **derivative.journals}
    with MemberHistoryBuilder(
        artifact_id=derivative.history.artifact_id,
        bytes=derivative.history.bytes,
        sha256=derivative.history.sha256,
        primary=derivative.history.primary,
    ) as builder:
        builder.add_import(imported)
        binding, _ = builder.seal()
        for raw in builder.objects():
            objects[provenance_structure_identity(raw).object_id] = raw
    store = MemberHistoryStore(lambda path: (objects[provenance_structure_object_id(path)],))
    with MemberHistoryClosure(
        store, lambda identity, end: (journals[identity][:end],), member_role=COLLECTION_MEMBER_ROLE
    ) as closure:
        closure.resolve(binding, extent=BOUND_HISTORY_EXTENT)
        assert {a.journal_id for a in closure.journal_anchors()} == {
            source.journal_id,
            late,
            derivative.journal_id,
        }
        transferred = tuple(closure.structure_objects())
        assert proof.to_json_bytes() in transferred
        assert source.history.to_json_bytes() in transferred
        assert late in {a.journal_id for a in closure.journal_anchors()}
    wrong = replace(
        imported,
        input_state={**imported.input_state, "object_id": derivative.summary.states[0]["id"]},
    )
    with MemberHistoryBuilder(
        artifact_id=derivative.history.artifact_id,
        bytes=derivative.history.bytes,
        sha256=derivative.history.sha256,
        primary=derivative.history.primary,
    ) as builder:
        builder.add_import(wrong)
        binding, _ = builder.seal()
        for raw in builder.objects():
            objects[provenance_structure_identity(raw).object_id] = raw
    with pytest.raises(ValueError, match="differs from the source primary"):
        with MemberHistoryClosure(
            store,
            lambda identity, end: (journals[identity][:end],),
            member_role=COLLECTION_MEMBER_ROLE,
        ) as closure:
            closure.resolve(binding, extent=BOUND_HISTORY_EXTENT)
