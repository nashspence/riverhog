"""Encrypted pathless archive fixture for independent recovery qualification."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from riverhog_age import encrypt_age_scrypt
from riverhog_archive_contracts import (
    ARCHIVE_ENCRYPTION_FORMAT,
    BOUND_HISTORY_EXTENT,
    PROVENANCE_BINDINGS_FORMAT,
    RETAINED_HISTORY_EXTENT,
    ArchiveRootCiphertextIdentity,
    CollectionEncryptionBinding,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryBuilder,
    MemberHistoryDocument,
    MemberHistoryImport,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    MemberHistoryStore,
    ProvenancePayload,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    RecoveryDescriptor,
    SourceMemberHistoryBindingProof,
    binding_tree_commitment,
    format_archive_sequence,
    ordered_provenance_commitment,
    provenance_structure_identity,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_core.archive_manifest import (
    build_collection_archive_authority,
    build_collection_archive_terminal_document,
)
from riverhog_core.domain.archive import (
    ArchiveArtifact,
    SealedPackVolume,
    SealedProvenanceObject,
    SealedRawVolume,
    StoredArchivePart,
    VerifiedRawArtifact,
)
from riverhog_core.pack_volume import iter_render_pack_upload_unit, plan_pack_volume
from riverhog_core.raw_verification import raw_artifact_ordered_volume_commitment
from riverhog_protocol import (
    COLLECTION_DESCRIPTION_RELATIVE_PATH,
    COLLECTION_TAG_HEAD_RELATIVE_PATH,
    ArtifactMemberIdentityDocument,
    CollectionDescriptionDocument,
    CollectionTagHeadDocument,
    CollectionTagSet,
    MemoryCollectionTagNodeStore,
    collection_tag_node_path,
)
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_MEMBER_ROLE,
    collection_production_contract,
)
from riverhog_protocol.manifest import artifact_set_identity
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    MemberHistoryClosure,
    append_assertions,
    assertion,
    create_journal,
    external_reference,
    new_id,
    validate_journal,
)
from riverhog_provenance_contracts import ContractCatalog

from tests.fixtures.archive import age_state_json

PASSPHRASE = "correct horse battery archive"
PASSPHRASE_ID = "recovery-test-key-v1"
GENERATION = "a" * 64


@dataclass(frozen=True, slots=True)
class FixtureArchive:
    members: Mapping[str, bytes]
    hints: Mapping[str, tuple[str, ...] | None]
    journals: Mapping[str, bytes]
    archive_root_sha256: str
    description_sha256: str | None
    tag_head_sha256: str
    history_objects: Mapping[str, bytes]
    history_bindings: tuple[MemberHistoryBinding, ...]
    archive_root: bytes
    provenance_root: bytes


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _part(plaintext: bytes, ciphertext: bytes) -> tuple[StoredArchivePart, ...]:
    return (
        StoredArchivePart(
            number=1,
            plaintext_start=0,
            plaintext_bytes=len(plaintext),
            plaintext_sha256=_sha256(plaintext),
            stored_bytes=len(ciphertext),
            stored_sha256=_sha256(ciphertext),
        ),
    )


def _encrypt(value: bytes, passphrase: str) -> bytes:
    return encrypt_age_scrypt(value, passphrase, log_n=1)


def _inherited_selection(
    source: FixtureArchive,
    extent: Literal["bound-and-required-history", "complete-retained-history"],
) -> tuple[MemberHistoryImport, dict[str, bytes], dict[str, bytes]]:
    binding = source.history_bindings[0]
    proof_tree = binding_tree_commitment(
        source.history_bindings, target_artifact_id=binding.artifact_id
    )
    assert proof_tree.target_index is not None
    proof = SourceMemberHistoryBindingProof(
        source_identity="e3" * 32,
        collection_id=1,
        archive_root=source.archive_root,
        provenance_root=source.provenance_root,
        binding=binding,
        index=proof_tree.target_index,
        siblings=proof_tree.target_siblings,
    )
    history = MemberHistoryDocument.from_json_bytes(
        next(
            value
            for value in source.history_objects.values()
            if hashlib.sha256(value).hexdigest() == binding.history_sha256
        )
    )
    primary = validate_journal(
        source.journals[history.primary.journal.journal_id],
        catalog=ContractCatalog((collection_production_contract(),)),
    )
    state = primary.graph_validation.objects[history.primary.delivery_association_id]["state"]
    imported = MemberHistoryImport(
        source_identity="e3" * 32,
        source_collection_id=1,
        source_archive_root_sha256=source.archive_root_sha256,
        source_artifact_set_sha256=ProvenanceRootDocument.from_json_bytes(
            source.provenance_root
        ).artifact_set_sha256,
        source_artifact_id=binding.artifact_id,
        source_history_sha256=binding.history_sha256,
        source_binding_proof_sha256=proof.identity,
        extent=extent,
        input_state=external_reference(primary, state["object_id"]),
    )
    with MemberHistoryClosure(
        MemberHistoryStore(lambda path: (source.history_objects[path],)),
        lambda journal_id, end: (source.journals[journal_id],),
        member_role=COLLECTION_MEMBER_ROLE,
    ) as closure:
        closure.resolve(binding, extent=extent)
        objects = {
            provenance_structure_identity(raw).relative_path: raw
            for raw in closure.structure_objects()
        }
        journals = {
            anchor.journal_id: source.journals[anchor.journal_id][: anchor.prefix_bytes]
            for anchor in closure.journal_anchors()
        }
    objects[provenance_structure_identity(proof.to_json_bytes()).relative_path] = (
        proof.to_json_bytes()
    )
    return imported, objects, journals


def write_archive(
    root: Path,
    *,
    passphrase: str = PASSPHRASE,
    passphrase_id: str = PASSPHRASE_ID,
    description: str | None = "Recovered fixture collection",
    description_document: bool = True,
    tags: Sequence[str] = (),
    hints: Mapping[str, tuple[str, ...] | None] | None = None,
    late_shared_history: bool = False,
    late_shared_scope: Literal["bound", "retained"] = "bound",
    inherited_history: FixtureArchive | None = None,
    inherited_extent: Literal[
        "bound-and-required-history", "complete-retained-history"
    ] = RETAINED_HISTORY_EXTENT,
    unselected_journal_tail: bool = False,
    journal_segment_bytes: int | None = None,
    members: Mapping[str, bytes] | None = None,
) -> FixtureArchive:
    root.mkdir(parents=True, exist_ok=True)
    contents = (
        dict(members)
        if members is not None
        else {
            "1" * 64: b"alpha\n",
            "2" * 64: b"beta\n",
            "3" * 64: b"first-second",
        }
    )
    suggested: dict[str, tuple[str, ...] | None] = {
        "1" * 64: ("notes", "alpha.txt"),
        "2" * 64: ("notes", "beta.txt"),
        "3" * 64: None,
    }
    suggested.update(hints or {})
    artifacts = tuple(
        ArchiveArtifact(artifact_id, len(content), _sha256(content))
        for artifact_id, content in sorted(contents.items())
    )
    artifact_set_sha256 = artifact_set_identity(
        ArtifactMemberIdentityDocument.model_validate(
            {"artifact_id": row.artifact_id, "bytes": str(row.bytes), "sha256": row.sha256}
        )
        for row in artifacts
    )
    delivery_context_id = new_id()
    attribution = ProducerAttribution(
        producer_app="recovery-fixture",
        adapter_id="recovery-fixture/v1",
        adapter_version="1.0.0",
        source_event_id="recovery-fixture-event",
        ingest_source="fixture",
        source_context={"fixture": True},
        construction_identity="f" * 64,
    )
    bindings = []
    journals: dict[str, bytes] = {}
    for row in artifacts:
        observed = BoundedSourceObserver().observe(BytesSource(contents[row.artifact_id]))
        produced = build_member_journal(
            member=ArtifactMemberIdentityDocument.model_validate(
                {"artifact_id": row.artifact_id, "bytes": str(row.bytes), "sha256": row.sha256}
            ),
            observation=observed,
            delivery_context_id=delivery_context_id,
            attribution=attribution,
            materialization_hint=suggested[row.artifact_id],
        )
        bindings.append(produced.binding)
        journals[produced.journal_id] = produced.content
        if unselected_journal_tail:
            who = new_id()
            journals[produced.journal_id] = append_assertions(
                produced.content,
                {
                    "agents": [
                        assertion(
                            "agent", who, object_id=who, kind="software", name="later recorder"
                        )
                    ]
                },
                recorded_by_agent_id=who,
                catalog=ContractCatalog((collection_production_contract(),)),
            )
    history_objects: dict[str, bytes] = {}
    inherited = None
    if inherited_history is not None:
        if inherited_extent not in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT):
            raise ValueError("invalid inherited test history extent")
        inherited, inherited_objects, inherited_journals = _inherited_selection(
            inherited_history, inherited_extent
        )
        history_objects.update(inherited_objects)
        journals.update(inherited_journals)
    final_bindings: list[MemberHistoryBinding] = []
    supplemental = None
    if late_shared_history:
        agent_id = new_id()
        primary = validate_journal(
            journals[bindings[0].journal.journal_id],
            catalog=ContractCatalog((collection_production_contract(),)),
        )
        state = primary.graph_validation.objects[bindings[0].delivery_association_id]["state"]
        recorded = create_journal(
            {
                "agents": [
                    assertion(
                        "agent",
                        agent_id,
                        object_id=agent_id,
                        kind="software",
                        name="fixture-recorder",
                    )
                ],
                "extensions": [
                    assertion(
                        "extension",
                        agent_id,
                        subject=external_reference(primary, state["object_id"]),
                        property="urn:test:late-operation-fact",
                        value={"type": "text", "value": "required late evidence"},
                    )
                ],
            },
            recorded_by_agent_id=agent_id,
        )
        supplemental = validate_journal(recorded)
        journals[supplemental.journal_id] = recorded
    for member, binding in zip(artifacts, bindings, strict=True):
        with MemberHistoryBuilder(
            artifact_id=member.artifact_id,
            bytes=member.bytes,
            sha256=member.sha256,
            primary=MemberHistoryPrimary.from_mapping(
                {
                    "journal": binding.journal.model_dump(mode="json"),
                    "delivery_association_id": binding.delivery_association_id,
                }
            ),
        ) as builder:
            if inherited is not None and member.artifact_id == artifacts[0].artifact_id:
                builder.add_import(inherited)
            if supplemental is not None:
                builder.add_root(
                    MemberHistoryRoot(
                        HistoryJournalAnchor.from_mapping(supplemental.anchor), late_shared_scope
                    )
                )
            selected, history = builder.seal()
            final_bindings.append(selected)
            for raw in (
                history.to_json_bytes(),
                *(
                    page.to_json_bytes()
                    for authority in (history.roots, history.imports)
                    for page in builder.pages(authority)
                ),
            ):
                history_objects[provenance_structure_identity(raw).relative_path] = raw
    binding_page = canonical_json_bytes(
        {
            "format": PROVENANCE_BINDINGS_FORMAT,
            "bindings": [selected.to_mapping() for selected in final_bindings],
        }
    )
    provenance_docs: list[ProvenanceVolumeDocument] = []
    archive_objects: dict[str, bytes] = {}
    provenance_docs.append(
        ProvenanceVolumeDocument(
            archive_generation=GENERATION,
            artifact_set_sha256=artifact_set_sha256,
            sequence=0,
            payload=ProvenancePayload("bindings", 0, len(binding_page), _sha256(binding_page)),
            first_artifact_id=artifacts[0].artifact_id,
            last_artifact_id=artifacts[-1].artifact_id,
            binding_count=len(bindings),
        )
    )
    payloads: list[bytes] = [binding_page]
    for journal_id, raw in sorted(journals.items()):
        segment_bytes = len(raw) if journal_segment_bytes is None else journal_segment_bytes
        for offset in range(0, len(raw), segment_bytes):
            segment = raw[offset : offset + segment_bytes]
            sequence = len(provenance_docs)
            provenance_docs.append(
                ProvenanceVolumeDocument(
                    archive_generation=GENERATION,
                    artifact_set_sha256=artifact_set_sha256,
                    sequence=sequence,
                    payload=ProvenancePayload("journal", sequence, len(segment), _sha256(segment)),
                    journal_id=journal_id,
                    journal_offset=offset,
                    journal_bytes=len(raw),
                    journal_sha256=_sha256(raw),
                )
            )
            payloads.append(segment)
    for document, payload in zip(provenance_docs, payloads, strict=True):
        archive_objects[document.metadata_path] = _encrypt(document.to_json_bytes(), passphrase)
        archive_objects[document.payload.path] = _encrypt(payload, passphrase)
    provenance_terminal = ProvenanceTerminalDocument(
        archive_generation=GENERATION,
        artifact_set_sha256=artifact_set_sha256,
        sequence=len(provenance_docs),
    )
    archive_objects[provenance_terminal.metadata_path] = _encrypt(
        provenance_terminal.to_json_bytes(), passphrase
    )
    provenance_root = ProvenanceRootDocument(
        archive_generation=GENERATION,
        artifact_set_sha256=artifact_set_sha256,
        delivery_context_id=delivery_context_id,
        binding_count=len(bindings),
        binding_tree_sha256=binding_tree_commitment(final_bindings).root_sha256,
        journal_count=len(journals),
        ordered_volume_sha256=ordered_provenance_commitment(
            (*provenance_docs, provenance_terminal)
        ),
    )
    provenance_root_raw = provenance_root.to_json_bytes()
    provenance_root_ciphertext = _encrypt(provenance_root_raw, passphrase)
    archive_objects["provenance/root.json.age"] = provenance_root_ciphertext
    provenance_object = SealedProvenanceObject(
        object_id="provenance-root",
        kind="provenance-root",
        relative_path="provenance/root.json.age",
        plaintext_bytes=len(provenance_root_raw),
        plaintext_sha256=provenance_root.identity,
        stored_bytes=len(provenance_root_ciphertext),
        stored_sha256=_sha256(provenance_root_ciphertext),
        revision="fixture-provenance-root",
        completed_at="2026-08-08T00:00:00Z",
    )

    pack_artifacts = artifacts[:2] if members is None else artifacts
    pack = plan_pack_volume(pack_artifacts, sequence=0)
    pack_plaintext = b"".join(
        iter_render_pack_upload_unit(pack, 0, lambda artifact_id: (contents[artifact_id],))
    )
    pack_ciphertext = _encrypt(pack_plaintext, passphrase)
    sealed_pack = SealedPackVolume(
        volume_id=pack.volume_id,
        sequence=0,
        relative_path=f"volumes/{pack.volume_id}.tar.age",
        artifacts=len(pack_artifacts),
        source_bytes=sum(row.bytes for row in pack_artifacts),
        plaintext_bytes=len(pack_plaintext),
        age_state_json=age_state_json(len(pack_plaintext)),
        index_sha256=pack.index_sha256,
        plan_sha256=pack.plan_sha256,
        parts=_part(pack_plaintext, pack_ciphertext),
        revision="fixture-pack",
        completed_at="2026-08-08T00:00:00Z",
    )
    archive_objects[sealed_pack.relative_path] = pack_ciphertext
    raw_volumes = []
    verified_raw_artifacts: tuple[VerifiedRawArtifact, ...] = ()
    if members is None:
        raw_artifact = artifacts[-1]
        offset = 0
        for sequence, plaintext in enumerate((b"first-", b"second"), start=1):
            volume_id = f"segment-{format_archive_sequence(sequence)}"
            ciphertext = _encrypt(plaintext, passphrase)
            sealed = SealedRawVolume(
                volume_id=volume_id,
                sequence=sequence,
                relative_path=f"volumes/{volume_id}.bin.age",
                artifact_id=raw_artifact.artifact_id,
                artifact_offset=offset,
                plaintext_bytes=len(plaintext),
                artifact_bytes=raw_artifact.bytes,
                artifact_sha256=raw_artifact.sha256,
                age_state_json=age_state_json(len(plaintext)),
                parts=_part(plaintext, ciphertext),
                revision=f"fixture-segment-{sequence}",
                completed_at="2026-08-08T00:00:00Z",
            )
            raw_volumes.append(sealed)
            archive_objects[sealed.relative_path] = ciphertext
            offset += len(plaintext)
        verified_raw = VerifiedRawArtifact(
            artifact_id=raw_artifact.artifact_id,
            bytes=raw_artifact.bytes,
            sha256=raw_artifact.sha256,
            ordered_volume_sha256=raw_artifact_ordered_volume_commitment(
                artifact=raw_artifact, volumes=raw_volumes
            ),
            verified_at="2026-08-08T00:00:00Z",
        )
        verified_raw_artifacts = (verified_raw,)
    manifest_raw, volumes = build_collection_archive_authority(
        archive_generation=GENERATION,
        artifacts=artifacts,
        packs=((pack, sealed_pack),),
        raw_volumes=raw_volumes,
        verified_raw_artifacts=verified_raw_artifacts,
        provenance_identity=provenance_root.identity,
        provenance_objects=(provenance_object,),
    )
    root_sha256 = _sha256(manifest_raw)
    manifest_ciphertext = _encrypt(manifest_raw, passphrase)
    archive_objects["manifest.json.age"] = manifest_ciphertext
    descriptor = RecoveryDescriptor(
        encryption=CollectionEncryptionBinding(ARCHIVE_ENCRYPTION_FORMAT, passphrase_id),
        root=ArchiveRootCiphertextIdentity(
            "manifest.json.age", len(manifest_ciphertext), _sha256(manifest_ciphertext)
        ),
    )
    archive_objects["recovery.json"] = descriptor.to_json_bytes()
    for archive_document in volumes:
        archive_objects[
            f"metadata/volume-{format_archive_sequence(archive_document.volume.sequence)}.json.age"
        ] = _encrypt(archive_document.to_json_bytes(), passphrase)
    archive_terminal = build_collection_archive_terminal_document(
        archive_generation=GENERATION,
        artifact_set_sha256=artifact_set_sha256,
        sequence=len(volumes),
    )
    archive_objects[
        f"metadata/volume-{format_archive_sequence(archive_terminal.sequence)}.json.age"
    ] = _encrypt(archive_terminal.to_json_bytes(), passphrase)

    description_sha256 = None
    if description_document:
        description_raw = CollectionDescriptionDocument.seal(
            archive_root_sha256=root_sha256, revision=1, description=description
        ).to_json_bytes()
        description_sha256 = _sha256(description_raw)
        archive_objects[COLLECTION_DESCRIPTION_RELATIVE_PATH] = _encrypt(
            description_raw, passphrase
        )
    tag_store = MemoryCollectionTagNodeStore()
    tag_set = CollectionTagSet(tag_store)
    for tag in tags:
        tag_set = tag_set.insert(tag)
    head_raw = CollectionTagHeadDocument.seal(
        archive_root_sha256=root_sha256,
        revision=1,
        root_sha256=tag_set.root.root_sha256,
    ).to_json_bytes()
    archive_objects[COLLECTION_TAG_HEAD_RELATIVE_PATH] = _encrypt(head_raw, passphrase)
    for digest, raw in tag_store.nodes.items():
        archive_objects[collection_tag_node_path(digest)] = _encrypt(raw, passphrase)
    for relative, raw in history_objects.items():
        archive_objects[relative] = _encrypt(raw, passphrase)
    for relative, content in archive_objects.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return FixtureArchive(
        contents,
        suggested,
        journals,
        root_sha256,
        description_sha256,
        _sha256(head_raw),
        history_objects,
        tuple(final_bindings),
        manifest_raw,
        provenance_root_raw,
    )


__all__ = ["FixtureArchive", "PASSPHRASE", "PASSPHRASE_ID", "write_archive"]
