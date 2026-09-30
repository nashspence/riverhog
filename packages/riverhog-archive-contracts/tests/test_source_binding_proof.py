from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from riverhog_archive_contracts import (
    ArchiveProvenanceIdentity,
    CollectionArchiveManifest,
    CollectionArtifactSetIdentity,
    MemberHistoryBinding,
    MemberHistoryImport,
    ProvenanceRootDocument,
    ProvenanceRootIdentity,
    SourceMemberHistoryBindingProof,
    binding_tree_commitment,
)
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json


def _proof() -> SourceMemberHistoryBindingProof:
    bindings = tuple(
        MemberHistoryBinding(f"{index:064x}", index, "b" * 64, f"{index + 11:064x}", 512)
        for index in range(9)
    )
    tree = binding_tree_commitment(bindings, target_artifact_id=bindings[4].artifact_id)
    provenance = ProvenanceRootDocument(
        archive_generation="c" * 64,
        artifact_set_sha256="d" * 64,
        delivery_context_id="urn:uuid:11111111-1111-4111-8111-111111111111",
        binding_count=len(bindings),
        binding_tree_sha256=tree.root_sha256,
        journal_count=2,
        ordered_volume_sha256="e" * 64,
    )
    archive = CollectionArchiveManifest(
        archive_generation=provenance.archive_generation,
        artifact_set=CollectionArtifactSetIdentity(
            len(bindings), 36, provenance.artifact_set_sha256
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
                stored_bytes=len(provenance.to_json_bytes()) + 128,
                stored_sha256="0" * 64,
            ),
        ),
    )
    assert tree.target_index is not None
    return SourceMemberHistoryBindingProof(
        source_identity="a" * 64,
        collection_id=7,
        archive_root=archive.to_json_bytes(),
        provenance_root=provenance.to_json_bytes(),
        binding=bindings[4],
        index=tree.target_index,
        siblings=tree.target_siblings,
    )


def _import(proof: SourceMemberHistoryBindingProof) -> MemberHistoryImport:
    archive = CollectionArchiveManifest.from_json_bytes(proof.archive_root)
    return MemberHistoryImport(
        source_identity=proof.source_identity,
        source_collection_id=proof.collection_id,
        source_archive_root_sha256=hashlib.sha256(proof.archive_root).hexdigest(),
        source_artifact_set_sha256=archive.artifact_set_sha256,
        source_artifact_id=proof.binding.artifact_id,
        source_history_sha256=proof.binding.history_sha256,
        source_binding_proof_sha256=proof.identity,
        extent="bound-and-required-history",
        input_state={
            "scope": "external",
            "journal_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "entry": {
                "entry_id": "urn:uuid:22222222-2222-4222-8222-222222222222",
                "sequence": "0",
                "json_sha256": "f" * 64,
            },
            "assertion_id": "urn:uuid:33333333-3333-4333-8333-333333333333",
            "object_id": "urn:uuid:44444444-4444-4444-8444-444444444444",
            "object_type": "state",
        },
    )


def test_source_proof_authenticates_one_exact_history_without_disclosing_siblings() -> None:
    proof = _proof()
    raw = proof.to_json_bytes()
    assert SourceMemberHistoryBindingProof.from_json_bytes(raw) == proof
    proof.verify_import(_import(proof))
    # The selected leaf is present; neither a sibling artifact ID nor H is disclosed.
    assert proof.binding.artifact_id.encode() in raw
    assert f"{3:064x}".encode() not in raw
    assert f"{14:064x}".encode() not in raw
    with pytest.raises(ValueError, match="source root"):
        replace(proof, binding=replace(proof.binding, history_sha256="0" * 64))
    with pytest.raises(ValueError, match="source root"):
        replace(proof, index=5)
    with pytest.raises(ValueError, match="incomplete"):
        replace(proof, siblings=proof.siblings[:-1])


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_identity", "1" * 64),
        ("source_collection_id", 8),
        ("source_archive_root_sha256", "1" * 64),
        ("source_artifact_set_sha256", "1" * 64),
        ("source_artifact_id", "1" * 64),
        ("source_history_sha256", "1" * 64),
        ("source_binding_proof_sha256", "1" * 64),
    ],
)
def test_proof_cannot_be_rebound_to_another_source_selection(field: str, value: object) -> None:
    proof = _proof()
    with pytest.raises(ValueError, match="source binding proof differs"):
        proof.verify_import(replace(_import(proof), **{field: value}))


def test_proof_rejects_mismatched_root_authority_and_noncanonical_preimages() -> None:
    proof = _proof()
    provenance = ProvenanceRootDocument.from_json_bytes(proof.provenance_root)
    with pytest.raises(ValueError, match="root authority"):
        replace(
            proof, provenance_root=replace(provenance, artifact_set_sha256="a" * 64).to_json_bytes()
        )
    with pytest.raises(ValueError, match="canonical"):
        replace(proof, archive_root=proof.archive_root + b"\n")
    value = require_canonical_json(proof.to_json_bytes())
    value["archive_root_base64"] += "="
    with pytest.raises(ValueError):
        SourceMemberHistoryBindingProof.from_json_bytes(canonical_json_bytes(value))
    value["source_identity"] = 1
    with pytest.raises(ValueError):
        SourceMemberHistoryBindingProof.from_json_bytes(canonical_json_bytes(value))
