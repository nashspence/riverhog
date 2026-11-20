from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from riverhog_client._prepared_primary_documents import (
    ByteFile,
    PreparedPrimary,
    PrimaryCustody,
    PrimaryDecisionAccepted,
    primary_control_bytes,
)
from riverhog_client.canonical_production import (
    ProducerAttribution,
    build_member_journal,
    member_materialization_decision,
)
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
    CollectionUploadCustodyObjectDocument,
    CollectionUploadProvenanceCustodyObjectDocument,
)
from riverhog_provenance import BoundedSourceObserver, BytesSource


def _file(name: str, content: bytes) -> ByteFile:
    return ByteFile(name=name, bytes=str(len(content)), sha256=hashlib.sha256(content).hexdigest())


@pytest.fixture(scope="module")
def original(tmp_path_factory):
    root = tmp_path_factory.mktemp("prepared-control")
    # A module fixture precedes the function-scoped provenance identity isolation.
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("RIVERHOG_PROVENANCE_STATE_HOME", str(root / "user-state"))
        content = b"same measured member"
        member = ArtifactMemberIdentityDocument.model_validate(
            {
                "artifact_id": "a3" * 32,
                "bytes": str(len(content)),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )
        observation = BoundedSourceObserver().observe(BytesSource(content))
        produced = build_member_journal(
            member=member,
            observation=observation,
            delivery_context_id="urn:uuid:ba123103-fd14-4813-b2d9-b83859e36f31",
            attribution=ProducerAttribution(
                "fixture", "fixture/v1", "1", "event", "fixture", {}, "f2" * 32
            ),
            materialization_hint=None,
        )
        decision = member_materialization_decision(
            artifact_id=member.artifact_id,
            materialization_hint=None,
            allow_missing_materialization_hint=True,
        )
        prepared = PreparedPrimary.seal(
            session_sha256="1" * 64,
            member=member,
            request=_file("request", b"fixture inputs"),
            observation=_file("observation", b"fixture selection"),
            graph=_file("graph", observation.graph_json),
            journal=_file("journal", produced.content),
            decision=_file("decision", primary_control_bytes(decision)),
            primary=produced.binding,
            completion_requirement_sha256=None,
        )
        receipt = CollectionUploadArtifactCustodyReceiptDocument.seal(
            collection_id=42,
            artifact_id=member.artifact_id,
            bytes=member.bytes,
            sha256=member.sha256,
            primary=produced.binding,
            completion_requirement_sha256=None,
            archive_objects=(
                CollectionUploadCustodyObjectDocument(
                    volume_id="pack-" + "0" * 64, sealed_receipt_sha256="f" * 64
                ),
            ),
            provenance_objects=(
                CollectionUploadProvenanceCustodyObjectDocument(
                    object_id="original-primary",
                    relative_path="provenance/payloads/original.bin.age",
                    plaintext_bytes=str(len(produced.content)),
                    plaintext_sha256=prepared.journal.sha256,
                    sealed_receipt_sha256="e" * 64,
                ),
            ),
        )
        yield prepared, receipt


def _retained(prepared, receipt):
    return PrimaryCustody.from_receipt(42, prepared, receipt)


def test_custody_binds_the_original_primary_and_survives_absent_large_files(
    original, tmp_path: Path
) -> None:
    prepared, receipt = original
    retained = _retained(prepared, receipt)
    raw = primary_control_bytes(retained)
    loaded = PrimaryCustody.model_validate_json(raw)
    assert loaded == retained
    assert loaded.prepared_sha256 == hashlib.sha256(primary_control_bytes(prepared)).hexdigest()
    assert loaded.receipt_sha256 == receipt.receipt_sha256
    # Compact controls carry commitments; parsing them does not open retired preimages.
    assert list(tmp_path.iterdir()) == []
    accepted = PrimaryDecisionAccepted(
        prepared_sha256=retained.prepared_sha256,
        decision=prepared.decision.commitment(),
        response=prepared.decision.commitment(),
    )
    assert PrimaryDecisionAccepted.model_validate_json(primary_control_bytes(accepted)) == accepted


@pytest.mark.parametrize("changed", ["primary", "completion", "member", "collection"])
def test_a_self_consistent_receipt_cannot_acknowledge_another_preparation(
    original, changed: str
) -> None:
    from riverhog_canonical_json import canonical_json_sha256

    prepared, receipt = original
    value = receipt.model_dump(mode="json")
    if changed == "primary":
        value["primary"]["delivery_association_id"] = (
            "urn:uuid:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        )
    elif changed == "completion":
        value["completion_requirement_sha256"] = "f" * 64
    elif changed == "member":
        value["bytes"] = str(receipt.bytes + 1)
    else:
        value["collection_id"] = "43"
    value["receipt_sha256"] = canonical_json_sha256(
        {key: item for key, item in value.items() if key != "receipt_sha256"}
    )
    other = CollectionUploadArtifactCustodyReceiptDocument.model_validate(value)
    with pytest.raises(ValueError):
        _retained(prepared, other)


@pytest.mark.parametrize(
    "changed", ["member", "length", "digest", "shared-file", "unknown", "format"]
)
def test_invalid_or_ambiguous_preparation_controls_fail_closed(original, changed: str) -> None:
    prepared, _receipt = original
    value = prepared.model_dump(mode="json")
    if changed == "member":
        value["member"]["artifact_id"] = "b4" * 32
    elif changed == "length":
        value["journal"]["bytes"] = str(prepared.journal.bytes + 1)
    elif changed == "digest":
        value["journal"]["sha256"] = "f" * 64
    elif changed == "shared-file":
        value["decision"]["name"] = value["journal"]["name"]
    elif changed == "unknown":
        value["fallback"] = True
    else:
        value["format"] = "unknown"
    with pytest.raises(ValueError):
        PreparedPrimary.model_validate(value)


def test_custody_and_materialization_acceptance_remain_independent(original) -> None:
    prepared, receipt = original
    retained = _retained(prepared, receipt)
    decision = prepared.decision.commitment()
    response = decision.model_dump(mode="json")
    response["sha256"] = "a" * 64
    with pytest.raises(ValueError):
        PrimaryDecisionAccepted(
            prepared_sha256=retained.prepared_sha256,
            decision=decision,
            response=response,
        )
    # A valid receipt is retained even though no successful decision acknowledgement exists.
    assert (
        PrimaryCustody.model_validate_json(primary_control_bytes(retained)).receipt_sha256
        == receipt.receipt_sha256
    )


def test_unvalidated_receipt_changes_cannot_be_compacted(original) -> None:
    prepared, receipt = original
    changed = receipt.model_copy(update={"archive_object_count": receipt.archive_object_count + 1})
    with pytest.raises(ValueError, match="receipt identity"):
        _retained(prepared, changed)


def test_changed_saved_receipt_commitment_is_rejected(original) -> None:
    prepared, receipt = original
    retained = _retained(prepared, receipt)
    value = retained.model_dump(mode="json")
    value["receipt_sha256"] = "1" * 64
    assert value["receipt_sha256"] != retained.receipt_sha256
    with pytest.raises(ValueError, match="custody proof identity"):
        PrimaryCustody.model_validate(value)


def test_unvalidated_manifest_change_cannot_be_blessed_by_a_genuine_receipt(original) -> None:
    prepared, receipt = original
    changed = prepared.model_copy(
        update={"graph": prepared.graph.model_copy(update={"sha256": "f" * 64})}
    )
    with pytest.raises(ValueError, match="original manifest integrity"):
        _retained(changed, receipt)
