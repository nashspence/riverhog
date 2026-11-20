from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError, replace

import pytest
from riverhog_age import decrypt_age_scrypt
from riverhog_archive_contracts import (
    PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX,
    ArchiveProvenanceIdentity,
    CollectionArchiveManifest,
    CollectionArtifactSetIdentity,
    MemberHistoryBinding,
    ProvenanceRootIdentity,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    RecordPage,
    SourceMemberHistoryBindingProof,
    format_archive_sequence,
    member_history_object_path,
)
from riverhog_core.archive_formats import (
    PROVENANCE_BINDING_SEGMENT_STORAGE_FORMAT,
    PROVENANCE_HISTORY_STORAGE_FORMAT,
    PROVENANCE_JOURNAL_SEGMENT_STORAGE_FORMAT,
    PROVENANCE_RECORD_PAGE_STORAGE_FORMAT,
    PROVENANCE_ROOT_STORAGE_FORMAT,
    PROVENANCE_SOURCE_PROOF_STORAGE_FORMAT,
    PROVENANCE_TERMINAL_STORAGE_FORMAT,
    PROVENANCE_VOLUME_METADATA_STORAGE_FORMAT,
)
from riverhog_core.archive_provenance import (
    ArchiveProvenancePublication as Publication,
)
from riverhog_core.archive_provenance import ArchiveProvenancePublisher

from tests.support.provenance_archive import archive_reader_fixture
from tests.unit.test_archive_root import MemoryImmutableStore

_KINDS = (
    "bindings",
    "journal",
    "metadata",
    "history",
    "record-page",
    "source-proof",
    "terminal",
    "root",
)
_PREFIX = "archive/fixture"


def _case(kind):
    reader, objects, _, _ = archive_reader_fixture()
    root = reader.scan().root
    binding = MemberHistoryBinding.from_mapping(next(reader.iter_bindings()))
    documents = [
        ProvenanceVolumeDocument.from_json_bytes(
            objects[f"provenance/metadata/volume-{format_archive_sequence(i)}.json.age"]
        )
        for i in (0, 1)
    ]
    prefix = {"archive_storage_prefix": "/" + _PREFIX + "/"}
    selected = None
    if kind in {"bindings", "journal", "metadata"}:
        document = documents[0 if kind == "bindings" else 1]
        payload = objects[document.payload.path]
        method = "publish_volume"
        arguments = dict(prefix, document=document, payload=payload)
        if kind == "metadata":
            publication = Publication.from_volume_metadata(**dict(prefix, document=document))
            selected = "metadata"
            expected = (
                "provenance-volume-" + format_archive_sequence(document.sequence),
                "provenance-volume-metadata",
                document.metadata_path,
                PROVENANCE_VOLUME_METADATA_STORAGE_FORMAT,
                document.to_json_bytes(),
            )
        else:
            publication = Publication.from_volume_payload(**arguments)
            selected = "payload"
            expected = (
                "provenance-payload-" + document.payload.sha256,
                "provenance-bindings" if kind == "bindings" else "provenance-journal-segment",
                document.payload.path,
                PROVENANCE_BINDING_SEGMENT_STORAGE_FORMAT
                if kind == "bindings"
                else PROVENANCE_JOURNAL_SEGMENT_STORAGE_FORMAT,
                payload,
            )
    elif kind == "history":
        raw = objects[member_history_object_path(binding.history_sha256)]
        arguments = dict(prefix, binding=binding, content=raw)
        method = "publish_member_history"
        publication = Publication.from_member_history(**arguments)
        expected = (
            "provenance-history-" + binding.history_sha256,
            "provenance-history",
            member_history_object_path(binding.history_sha256),
            PROVENANCE_HISTORY_STORAGE_FORMAT,
            raw,
        )
    elif kind == "record-page":
        path = next(p for p in objects if p.startswith("provenance/sets/"))
        page = RecordPage.from_json_bytes(objects[path])
        arguments = dict(prefix, page=page)
        method = "publish_record_page"
        publication = Publication.from_record_page(**arguments)
        expected = (
            "provenance-record-page-"
            + page.authority.records_sha256
            + "-"
            + format_archive_sequence(page.ordinal),
            "provenance-record-page",
            path,
            PROVENANCE_RECORD_PAGE_STORAGE_FORMAT,
            objects[path],
        )
    elif kind == "source-proof":
        archive = CollectionArchiveManifest(
            archive_generation=root.archive_generation,
            artifact_set=CollectionArtifactSetIdentity(
                root.binding_count, binding.bytes, root.artifact_set_sha256
            ),
            ordered_volume_sha256="f" * 64,
            provenance=ArchiveProvenanceIdentity(
                root.identity,
                ProvenanceRootIdentity(
                    id="provenance-root",
                    kind="provenance-root",
                    path="provenance/root.json.age",
                    plaintext_bytes=len(root.to_json_bytes()),
                    sha256=root.identity,
                    stored_bytes=1024,
                    stored_sha256="0" * 64,
                ),
            ),
        )
        proof = SourceMemberHistoryBindingProof(
            source_identity="a" * 64,
            collection_id=7,
            archive_root=archive.to_json_bytes(),
            provenance_root=root.to_json_bytes(),
            binding=binding,
            index=0,
            siblings=(),
        )
        arguments = dict(prefix, proof=proof)
        method = "publish_source_binding_proof"
        publication = Publication.from_source_binding_proof(**arguments)
        expected = (
            "provenance-source-proof-" + proof.identity,
            "provenance-source-proof",
            "provenance/source-proofs/" + proof.identity[:2] + "/" + proof.identity + ".json.age",
            PROVENANCE_SOURCE_PROOF_STORAGE_FORMAT,
            proof.to_json_bytes(),
        )
    elif kind == "terminal":
        terminal = ProvenanceTerminalDocument(
            root.archive_generation, root.artifact_set_sha256, reader.scan().volume_count
        )
        arguments = dict(prefix, terminal=terminal)
        method = "publish_terminal"
        publication = Publication.from_terminal(**arguments)
        expected = (
            "provenance-terminal-" + format_archive_sequence(terminal.sequence),
            "provenance-terminal",
            terminal.metadata_path,
            PROVENANCE_TERMINAL_STORAGE_FORMAT,
            terminal.to_json_bytes(),
        )
    else:
        assert kind == "root"
        arguments = dict(prefix, root=root)
        method = "publish_root"
        publication = Publication.from_root(**arguments)
        selected = "root"
        expected = (
            "provenance-root",
            "provenance-root",
            "provenance/root.json.age",
            PROVENANCE_ROOT_STORAGE_FORMAT,
            root.to_json_bytes(),
        )
    return publication, method, arguments, selected, expected


@pytest.mark.parametrize("kind", _KINDS)
def test_actual_provenance_publishers_preserve_each_physical_format_and_original_receipt(
    monkeypatch, kind
):
    publication, method, arguments, selected, expected = _case(kind)
    object_id, object_kind, path, storage_format, plaintext = expected
    assert (
        publication.object_id,
        publication.kind,
        publication.relative_path,
        publication.storage_format,
        publication.plaintext,
    ) == expected
    assert publication.archive_storage_prefix == _PREFIX
    store = MemoryImmutableStore()
    calls = []
    original_put = store.put_immutable_object

    def capture(**kwargs):
        calls.append(kwargs)
        return original_put(**kwargs)

    monkeypatch.setattr(store, "put_immutable_object", capture)
    publisher = ArchiveProvenancePublisher(object_store=store, passphrase="fixture", scrypt_log_n=1)
    first = getattr(publisher, method)(**arguments)
    second = getattr(publisher, method)(**arguments)
    if selected is not None:
        first, second = getattr(first, selected), getattr(second, selected)
    assert first == second
    physical_calls = [c for c in calls if c["object_path"] == _PREFIX + "/" + path]
    assert len(physical_calls) == 2
    assert physical_calls[0]["content"] != physical_calls[1]["content"]
    assert first.stored_sha256 == hashlib.sha256(physical_calls[0]["content"]).hexdigest()
    assert first.stored_sha256 != hashlib.sha256(physical_calls[1]["content"]).hexdigest()
    assert (first.object_id, first.kind, first.relative_path) == (object_id, object_kind, path)
    assert first.plaintext_bytes == len(plaintext)
    assert first.plaintext_sha256 == hashlib.sha256(plaintext).hexdigest()
    assert publication.sealed_receipt(store.objects[publication.object_path].receipt) == first
    for call in physical_calls:
        assert decrypt_age_scrypt(call["content"], "fixture") == plaintext
        assert call["required_identity_assertions"] == {
            "riverhog-format": storage_format,
            "riverhog-plaintext-bytes": str(len(plaintext)),
            "riverhog-plaintext-sha256": hashlib.sha256(plaintext).hexdigest(),
        }
        assert call["content_type"] == (
            "application/vnd." + storage_format.replace("/", ".").replace("+", ".")
        )
        assert call["placement_policy"] == "immediate_default"
        request = publication.request(call["content"])
        publication.require_request(request)
        assert request.stored_sha256 == hashlib.sha256(call["content"]).hexdigest()
        assert request.stored_bytes == len(call["content"])


@pytest.mark.parametrize("kind", _KINDS)
def test_provenance_logical_request_rejects_substituted_identity_and_authority(kind):
    publication, *_ = _case(kind)
    request = publication.request(b"opaque original encrypted bytes")
    other = publication.request(b"another randomized encoding")
    assert request.stored_sha256 != other.stored_sha256
    publication.require_request(request)
    publication.require_request(other)
    changes = [
        {"object_path": "archive/other"},
        {"content_type": "application/octet-stream"},
        {"mode": "replace_current"},
        {"expected_current_stored_sha256": "f" * 64},
        {"placement_policy": "other"},
    ]
    for key in (
        "riverhog-format",
        "riverhog-plaintext-bytes",
        "riverhog-plaintext-sha256",
        "extra",
    ):
        changes.append(
            {
                "required_identity_assertions": dict(
                    request.required_identity_assertions, **{key: "changed"}
                )
            }
        )
    for change in changes:
        with pytest.raises(ValueError, match="request differs from its source"):
            publication.require_request(request.model_copy(update=change))
    assertions = publication.required_identity_assertions
    assertions["riverhog-format"] = "changed"
    assert assertions != publication.required_identity_assertions
    with pytest.raises(FrozenInstanceError):
        publication.plaintext = b"changed"
    assert publication.plaintext not in repr(publication).encode()


@pytest.mark.parametrize("kind", _KINDS)
def test_inconsistent_provenance_physical_receipt_cannot_become_source_data(kind):
    publication, *_ = _case(kind)
    store = MemoryImmutableStore()
    publisher = ArchiveProvenancePublisher(object_store=store, passphrase="fixture", scrypt_log_n=1)
    original = publisher.publish_object(publication)
    receipt = store.objects[publication.object_path].receipt
    assert publication.sealed_receipt(receipt) == original
    for change in ({"object_path": "archive/other"}, {"stored_bytes": 0}):
        with pytest.raises(RuntimeError, match="inconsistent receipt"):
            publication.sealed_receipt(replace(receipt, **change))


def test_early_journal_segment_and_final_volume_reuse_original_encrypted_octets():
    publication, _, arguments, _, _ = _case("journal")
    early = Publication.from_journal_segment(
        archive_storage_prefix=_PREFIX, content=arguments["payload"]
    )
    assert early == publication
    store = MemoryImmutableStore()
    publisher = ArchiveProvenancePublisher(object_store=store, passphrase="fixture", scrypt_log_n=1)
    first = publisher.publish_journal_segment(
        archive_storage_prefix=_PREFIX, content=arguments["payload"]
    )
    original = store.objects[early.object_path].content
    final = publisher.publish_volume(**arguments)
    assert final.payload == first
    assert store.objects[early.object_path].content == original
    assert len(store.objects) == 2


def test_provenance_volume_second_write_failure_preserves_original_payload(monkeypatch):
    publication, _, arguments, _, _ = _case("bindings")
    store = MemoryImmutableStore()
    original_put = store.put_immutable_object
    attempts = []

    def fail_metadata(**kwargs):
        attempts.append(kwargs["object_path"])
        if len(attempts) == 2:
            raise OSError("injected metadata interruption")
        return original_put(**kwargs)

    monkeypatch.setattr(store, "put_immutable_object", fail_metadata)
    publisher = ArchiveProvenancePublisher(object_store=store, passphrase="fixture", scrypt_log_n=1)
    with pytest.raises(OSError, match="metadata interruption"):
        publisher.publish_volume(**arguments)
    assert len(store.objects) == 1
    original = store.objects[publication.object_path]
    result = publisher.publish_volume(**arguments)
    assert result.payload == publication.sealed_receipt(original.receipt)
    assert store.objects[publication.object_path] == original
    assert len(store.objects) == 2 and len(attempts) == 4


def test_journal_publication_accepts_actual_maximum_frame_and_rejects_invalid_input():
    content = b"x" * PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
    publication = Publication.from_journal_segment(archive_storage_prefix=_PREFIX, content=content)
    store = MemoryImmutableStore()
    result = ArchiveProvenancePublisher(
        object_store=store, passphrase="fixture", scrypt_log_n=1
    ).publish_object(publication)
    ciphertext = store.objects[publication.object_path].content
    assert len(ciphertext) > len(content)
    assert decrypt_age_scrypt(ciphertext, "fixture") == content
    assert result.plaintext_bytes == PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
    for invalid in (b"", content + b"x"):
        with pytest.raises(ValueError, match="bounded byte contract"):
            Publication.from_journal_segment(archive_storage_prefix=_PREFIX, content=invalid)
    with pytest.raises(TypeError, match="immutable bytes"):
        Publication.from_journal_segment(
            archive_storage_prefix=_PREFIX, content=bytearray(b"mutable")
        )


def test_provenance_payload_and_history_must_match_exact_source_before_store_work():
    _, _, arguments, _, _ = _case("bindings")
    with pytest.raises(ValueError, match="payload identity changed"):
        Publication.from_volume_payload(**dict(arguments, payload=b"changed"))
    _, _, arguments, _, _ = _case("history")
    with pytest.raises(ValueError):
        Publication.from_member_history(**dict(arguments, content=b"{}"))
