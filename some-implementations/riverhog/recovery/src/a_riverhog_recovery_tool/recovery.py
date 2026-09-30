"""Independent full recovery of one selected encrypted Riverhog archive copy."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from riverhog_archive_contracts import (
    ARCHIVE_ROOT_DOCUMENT_BYTES_MAX,
    PROVENANCE_METADATA_BYTES_MAX,
    RECOVERY_DESCRIPTOR_PATH,
    CollectionArchiveManifest,
    RecoveryDescriptor,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_materialization import (
    DestinationRules,
    MemberAdvice,
    plan_materialization_spooled,
    primary_sidecar_components,
    shared_journal_components,
)
from riverhog_protocol import (
    COLLECTION_DESCRIPTION_RELATIVE_PATH,
    COLLECTION_TAG_HEAD_RELATIVE_PATH,
    CollectionArtifactProvenanceBindingDocument,
    CollectionDescriptionDocument,
    CollectionTagHeadDocument,
    CollectionTagSet,
    CollectionTagSetRoot,
    collection_tag_node_path,
)
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_provenance import (
    selected_delivery_occurrence,
    validate_journal_chunks,
    validate_journal_set_chunks,
)

from ._archive_io import EncryptedArchive, archive_file, sha256_file
from ._metadata import stage_metadata
from ._payloads import stage_payloads
from ._provenance_reader import CanonicalProvenanceArchiveReader
from ._publication import publish_complete_recovery


class RecoveryError(RuntimeError):
    """The selected copy cannot be independently recovered as a complete collection."""


@dataclass(frozen=True, slots=True)
class RecoverySummary:
    output: Path
    artifacts: int
    bytes: int
    volumes: int
    provenance_journals: int
    tag_count: int
    layout_mode: Literal["declared-hints", "id-layout"]
    archive_root_sha256: str
    description_revision: int | None
    tag_revision: int


@dataclass(frozen=True, slots=True)
class _ArchiveSelection:
    archive: Path
    descriptor: RecoveryDescriptor
    encrypted: EncryptedArchive
    manifest: CollectionArchiveManifest
    manifest_bytes: bytes
    archive_root_sha256: str


def read_recovery_descriptor(archive_dir: Path) -> RecoveryDescriptor:
    try:
        archive = archive_dir.expanduser().resolve()
        path = archive_file(archive, RECOVERY_DESCRIPTOR_PATH)
        return RecoveryDescriptor.from_json_bytes(path.read_bytes())
    except (OSError, UnicodeError, ValueError) as exc:
        raise RecoveryError(f"invalid recovery descriptor: {exc}") from exc


def _select_archive(
    archive_dir: Path,
    *,
    passphrases: Mapping[str, str],
    age_command: str,
    scratch: Path,
    expected_archive_root_sha256: str | None = None,
    require_provenance: bool = True,
) -> _ArchiveSelection:
    archive = archive_dir.expanduser().resolve()
    if not archive.is_dir():
        raise RecoveryError("archive directory does not exist")
    descriptor = read_recovery_descriptor(archive)
    passphrase = passphrases.get(descriptor.encryption.passphrase_id)
    if not isinstance(passphrase, str) or not passphrase:
        raise RecoveryError("no passphrase is available for the selected archive key ID")
    root_file = archive_file(archive, descriptor.root.path)
    if sha256_file(root_file) != (
        descriptor.root.stored_bytes,
        descriptor.root.stored_sha256,
    ):
        raise RecoveryError("encrypted root differs from the recovery descriptor")
    encrypted = EncryptedArchive(
        archive, passphrase=passphrase, age_command=age_command, scratch=scratch
    )
    raw = encrypted.read_bounded(descriptor.root.path, ARCHIVE_ROOT_DOCUMENT_BYTES_MAX)
    manifest = CollectionArchiveManifest.from_json_bytes(raw)
    root_sha256 = hashlib.sha256(raw).hexdigest()
    if expected_archive_root_sha256 is not None and root_sha256 != expected_archive_root_sha256:
        raise RecoveryError("selected archive root differs from trusted expectation")
    if require_provenance:
        provenance_file = archive_file(archive, manifest.provenance.root.path)
        if sha256_file(provenance_file) != (
            manifest.provenance.root.stored_bytes,
            manifest.provenance.root.stored_sha256,
        ):
            raise RecoveryError("encrypted provenance root differs from the immutable manifest")
    return _ArchiveSelection(archive, descriptor, encrypted, manifest, raw, root_sha256)


def _metadata_ciphertext_pins(archive: Path) -> tuple[tuple[int, str] | None, tuple[int, str]]:
    description_path = archive / COLLECTION_DESCRIPTION_RELATIVE_PATH
    if description_path.is_symlink():
        raise RecoveryError("description authority traverses a symbolic link")
    description = (
        sha256_file(archive_file(archive, COLLECTION_DESCRIPTION_RELATIVE_PATH))
        if description_path.exists()
        else None
    )
    head = sha256_file(archive_file(archive, COLLECTION_TAG_HEAD_RELATIVE_PATH))
    return description, head


def _destination_rules(output_parent: Path) -> DestinationRules:
    with tempfile.TemporaryDirectory(prefix=".riverhog-rule-probe-", dir=output_parent) as name:
        probe = Path(name)
        (probe / "case").write_bytes(b"x")
        case_sensitive = not (probe / "CASE").exists()
        (probe / "e\u0301").write_bytes(b"x")
        unicode_equivalence = "NFC" if (probe / "\u00e9").exists() else "exact"
    try:
        component_bytes = os.pathconf(output_parent, "PC_NAME_MAX")
        path_bytes = os.pathconf(output_parent, "PC_PATH_MAX")
    except (AttributeError, OSError, ValueError):
        component_bytes = 255
        path_bytes = 240 if os.name == "nt" else 4096
    return DestinationRules(
        windows_names=os.name == "nt",
        case_sensitive=case_sensitive,
        unicode_equivalence=cast(Any, unicode_equivalence),
        component_bytes=max(1, component_bytes),
        relative_path_bytes=max(1, path_bytes),
    )


def _write_exact(staging: Path, components: tuple[str, ...], content: bytes) -> Path:
    if not components or any(
        not part or part in {".", ".."} or "/" in part or "\\" in part for part in components
    ):
        raise RecoveryError("recovery destination is not a safe relative path")
    destination = staging.joinpath(*components)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())
    return destination


def _copy_exact(
    staging: Path,
    components: tuple[str, ...],
    source: Path,
    *,
    expected_bytes: int,
    expected_sha256: str,
) -> Path:
    if sha256_file(source) != (expected_bytes, expected_sha256):
        raise RecoveryError("staged artifact differs from selected archive identity")
    destination = staging.joinpath(*components)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as input_file, destination.open("xb") as output:
        shutil.copyfileobj(input_file, output, length=8 * 1024 * 1024)
        output.flush()
        os.fsync(output.fileno())
    return destination


def _prefix_chunks(path: Path, byte_count: int) -> Iterator[bytes]:
    with path.open("rb") as source:
        remaining = byte_count
        while remaining:
            chunk = source.read(min(8 * 1024 * 1024, remaining))
            if not chunk:
                raise RecoveryError("primary provenance prefix is incomplete")
            remaining -= len(chunk)
            yield chunk


def _stage_provenance(
    selection: _ArchiveSelection,
    *,
    staging: Path,
    scratch: Path,
) -> tuple[CanonicalProvenanceArchiveReader, sqlite3.Connection, int]:
    reader = CanonicalProvenanceArchiveReader(
        selection.encrypted.iter_plaintext,
        expected_root_sha256=selection.manifest.provenance.identity,
        archive_generation=selection.manifest.archive_generation,
        artifact_set_sha256=selection.manifest.artifact_set_sha256,
    )
    scanned = reader.scan()
    if scanned.root.binding_count != selection.manifest.artifacts:
        raise RecoveryError("provenance binding coverage differs from artifact set")
    root_raw = selection.encrypted.read_bounded(
        selection.manifest.provenance.root.path, PROVENANCE_METADATA_BYTES_MAX
    )
    _write_exact(staging, ("structure", "provenance-root.json"), root_raw)
    journals = sqlite3.connect(scratch / "journals.sqlite3")
    journals.execute("DROP TABLE IF EXISTS journals")
    journals.execute(
        "CREATE TABLE journals (journal_id TEXT PRIMARY KEY, bytes INTEGER NOT NULL, "
        "sha256 TEXT NOT NULL, path TEXT NOT NULL)"
    )
    count = 0
    for journal_id, byte_count, sha256 in reader.iter_journal_headers():
        components = shared_journal_components(sha256)
        output = staging.joinpath(*components)
        output.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        copied = 0
        with output.open("xb") as stream:
            for chunk in reader.iter_journal_range(journal_id):
                stream.write(chunk)
                copied += len(chunk)
                digest.update(chunk)
            stream.flush()
            os.fsync(stream.fileno())
        if (copied, digest.hexdigest()) != (byte_count, sha256):
            raise RecoveryError("canonical journal differs from selected archive")
        journals.execute(
            "INSERT INTO journals VALUES (?, ?, ?, ?)",
            (journal_id, byte_count, sha256, str(output)),
        )
        count += 1
    journals.commit()
    # The entire selected set must resolve exact foreign/fork references. Unknown
    # optional profile payloads remain exact even without their application code.
    validate_journal_set_chunks(
        (
            _file_chunks(Path(path))
            for (path,) in journals.execute("SELECT path FROM journals ORDER BY journal_id")
        ),
        require_all_references=True,
        require_profiles=False,
    )
    return reader, journals, count


def _file_chunks(path: Path) -> Iterator[bytes]:
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            yield chunk


def _selected_advice(
    *,
    binding: CollectionArtifactProvenanceBindingDocument,
    byte_count: int,
    sha256: str,
    journals: sqlite3.Connection,
    staging: Path,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    journal = journals.execute(
        "SELECT bytes, path FROM journals WHERE journal_id = ?",
        (binding.journal.journal_id,),
    ).fetchone()
    if journal is None or int(journal[0]) < int(binding.journal.prefix_bytes):
        raise RecoveryError("primary binding selects a missing journal prefix")
    source = Path(str(journal[1]))
    prefix_bytes = int(binding.journal.prefix_bytes)
    summary = validate_journal_chunks(
        _prefix_chunks(source, prefix_bytes),
        expected_anchor=binding.journal.model_dump(mode="json"),
        require_exact_tail=True,
        require_profiles=False,
    )
    state, occurrence = selected_delivery_occurrence(
        summary,
        binding=binding.model_dump(mode="json"),
        artifact_id=binding.artifact_id,
        byte_count=byte_count,
        sha256=sha256,
        member_role=COLLECTION_MEMBER_ROLE,
    )
    sidecar = staging.joinpath(*primary_sidecar_components(binding.artifact_id))
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with sidecar.open("xb") as output:
        for chunk in _prefix_chunks(source, prefix_bytes):
            output.write(chunk)
            digest.update(chunk)
        output.flush()
        os.fsync(output.fileno())
    if digest.hexdigest() != binding.journal.prefix_sha256:
        raise RecoveryError("primary sidecar differs from selected anchor")
    support = {
        "journal": binding.journal.model_dump(mode="json"),
        "delivery_association_id": binding.delivery_association_id,
        "state_id": state["id"],
        "occurrence_id": occurrence["id"],
        "occurrence_assertion_id": occurrence["assertion_id"],
    }
    hint = occurrence.get("materialization_hint")
    if hint is not None and not isinstance(hint, dict):
        raise RecoveryError("selected occurrence has a malformed materialization hint")
    return hint, support


def _stage_mapping(
    *,
    payloads: sqlite3.Connection,
    reader: CanonicalProvenanceArchiveReader,
    journals: sqlite3.Connection,
    staging: Path,
    scratch: Path,
    rules: DestinationRules,
    mode: Literal["declared-hints", "id-layout"],
) -> tuple[int, int]:
    selected = sqlite3.connect(scratch / "selected.sqlite3")
    selected.execute("DROP TABLE IF EXISTS selected")
    selected.execute(
        "CREATE TABLE selected (artifact_id TEXT PRIMARY KEY, bytes INTEGER NOT NULL, "
        "sha256 TEXT NOT NULL, binding_json BLOB NOT NULL, hint_json BLOB, "
        "support_json BLOB NOT NULL)"
    )
    members = payloads.execute(
        "SELECT artifact_id, bytes, sha256 FROM artifacts ORDER BY artifact_id"
    )
    count = 0

    def advice() -> Iterator[MemberAdvice]:
        for artifact_id, raw_hint in selected.execute(
            "SELECT artifact_id, hint_json FROM selected ORDER BY artifact_id"
        ):
            yield MemberAdvice(
                str(artifact_id), json.loads(raw_hint) if raw_hint is not None else None
            )

    for member, raw_binding in zip(members, reader.iter_bindings(), strict=True):
        artifact_id, byte_count, sha256 = str(member[0]), int(member[1]), str(member[2])
        binding = CollectionArtifactProvenanceBindingDocument.model_validate(raw_binding)
        if binding.artifact_id != artifact_id:
            raise RecoveryError("primary binding differs from archive artifact order")
        hint, support = _selected_advice(
            binding=binding,
            byte_count=byte_count,
            sha256=sha256,
            journals=journals,
            staging=staging,
        )
        selected.execute(
            "INSERT INTO selected VALUES (?, ?, ?, ?, ?, ?)",
            (
                artifact_id,
                byte_count,
                sha256,
                canonical_json_bytes(binding.model_dump(mode="json")),
                canonical_json_bytes(hint) if hint is not None else None,
                canonical_json_bytes(support),
            ),
        )
        count += 1
    selected.commit()
    planned = plan_materialization_spooled(advice(), rules=rules, state=selected, mode=mode)
    mapped = 0
    with (staging / "recovery-members.jsonseq").open("xb") as mapping:
        for plan in planned:
            row = selected.execute(
                "SELECT bytes, sha256, binding_json, hint_json, support_json "
                "FROM selected WHERE artifact_id = ?",
                (plan.artifact_id,),
            ).fetchone()
            if row is None:
                raise RecoveryError("materialization plan lost an artifact")
            byte_count, sha256 = int(row[0]), str(row[1])
            _copy_exact(
                staging,
                plan.components,
                scratch / "artifact-spool" / plan.artifact_id,
                expected_bytes=byte_count,
                expected_sha256=sha256,
            )
            record = {
                "artifact_id": plan.artifact_id,
                "bytes": str(byte_count),
                "sha256": sha256,
                "components": list(plan.components),
                "sidecar": list(plan.sidecar_components),
                "reason": plan.reason,
                "materialization_hint": json.loads(row[3]) if row[3] is not None else None,
                "support": json.loads(row[4]),
                "binding": json.loads(row[2]),
            }
            mapping.write(canonical_json_bytes(record) + b"\n")
            mapped += 1
        mapping.flush()
        os.fsync(mapping.fileno())
    selected.close()
    if mapped != count:
        raise RecoveryError("materialization plan is incomplete")
    return count, mapped


def _required_layout_fits(rules: DestinationRules) -> None:
    mandatory = (
        ("metadata", "description-state.json"),
        ("metadata", "description.txt"),
        ("metadata", "description.json"),
        ("metadata", "tags.jsonseq"),
        ("metadata", "tags", "head.json"),
        ("metadata", "tags", "nodes", "f" * 64 + ".bin"),
        ("structure", "archive.json"),
        ("structure", "provenance-root.json"),
        ("recovery-members.jsonseq",),
        ("recovery.json",),
    )
    if any(not rules.fits(path) for path in mandatory):
        raise RecoveryError("destination cannot represent mandatory recovery structure")


def recover_archive(
    archive_dir: Path,
    output_dir: Path,
    *,
    passphrases: Mapping[str, str],
    age_command: str = "age",
    layout_mode: Literal["declared-hints", "id-layout"] = "declared-hints",
    expected_archive_root_sha256: str | None = None,
    expected_description_sha256: str | None = None,
    expected_tag_head_sha256: str | None = None,
) -> RecoverySummary:
    """Restore all four selected-copy semantic contents, then publish completion."""

    if layout_mode not in ("declared-hints", "id-layout"):
        raise RecoveryError("recovery layout mode is unsupported")
    output = output_dir.expanduser().absolute()
    if output.exists() or output.is_symlink():
        raise RecoveryError("recovery output already exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    scratch = output.parent / f".{output.name}.riverhog-recovery"
    if scratch.is_symlink():
        raise RecoveryError("recovery scratch path is a symbolic link")
    scratch.mkdir(mode=0o700, exist_ok=True)
    staging = scratch / "output"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(mode=0o700)
    payload_state: sqlite3.Connection | None = None
    journal_state: sqlite3.Connection | None = None
    try:
        selection = _select_archive(
            archive_dir,
            passphrases=passphrases,
            age_command=age_command,
            scratch=scratch,
            expected_archive_root_sha256=expected_archive_root_sha256,
        )
        metadata_pins_before = _metadata_ciphertext_pins(selection.archive)
        rules = _destination_rules(output.parent)
        _required_layout_fits(rules)
        selected_metadata = stage_metadata(
            read_plaintext=selection.encrypted.read_bounded,
            description_exists=metadata_pins_before[0] is not None,
            archive_root_sha256=selection.archive_root_sha256,
            staging=staging,
        )
        if (
            expected_description_sha256 is not None
            and selected_metadata.description_sha256 != expected_description_sha256
        ):
            raise RecoveryError("selected description differs from trusted expectation")
        if (
            expected_tag_head_sha256 is not None
            and selected_metadata.head_sha256 != expected_tag_head_sha256
        ):
            raise RecoveryError("selected tag head differs from trusted expectation")
        reader, journal_state, journal_count = _stage_provenance(
            selection, staging=staging, scratch=scratch
        )
        payload_summary, payload_state = stage_payloads(
            selection.encrypted, manifest=selection.manifest, scratch=scratch
        )
        covered, mapped = _stage_mapping(
            payloads=payload_state,
            reader=reader,
            journals=journal_state,
            staging=staging,
            scratch=scratch,
            rules=rules,
            mode=layout_mode,
        )
        if covered != payload_summary.artifacts or mapped != covered:
            raise RecoveryError("recovery mapping does not cover every artifact")
        _write_exact(staging, ("structure", "archive.json"), selection.manifest_bytes)
        if _metadata_ciphertext_pins(selection.archive) != metadata_pins_before:
            raise RecoveryError("selected metadata changed during recovery; retry the snapshot")
        receipt = {
            "format": "riverhog-complete-recovery/v1",
            "complete": True,
            "archive_root_sha256": selection.archive_root_sha256,
            "artifact_set_sha256": payload_summary.artifact_set_sha256,
            "provenance_identity": selection.manifest.provenance.identity,
            "metadata_pins": selected_metadata.pins(),
            "metadata_freshness": "selected-copy-heads-not-global-latest",
            "layout_mode": layout_mode,
            "artifacts": payload_summary.artifacts,
            "bytes": str(payload_summary.bytes),
            "volumes": payload_summary.volumes,
            "provenance_journals": journal_count,
            "tags": selected_metadata.tag_count,
            "mapping_sha256": sha256_file(staging / "recovery-members.jsonseq")[1],
        }
        _write_exact(staging, ("recovery.json",), canonical_json_bytes(receipt))
        payload_state.close()
        journal_state.close()
        payload_state = None
        journal_state = None
        if output.exists() or output.is_symlink():
            raise RecoveryError("recovery output appeared before completion")
        publish_complete_recovery(staging, output)
        try:
            shutil.rmtree(scratch)
        except OSError:
            # The completed output is already durable; scratch cleanup cannot
            # turn it back into an incomplete recovery.
            pass
        return RecoverySummary(
            output=output,
            artifacts=payload_summary.artifacts,
            bytes=payload_summary.bytes,
            volumes=payload_summary.volumes,
            provenance_journals=journal_count,
            tag_count=selected_metadata.tag_count,
            layout_mode=layout_mode,
            archive_root_sha256=selection.archive_root_sha256,
            description_revision=(
                selected_metadata.description.revision if selected_metadata.description else None
            ),
            tag_revision=selected_metadata.head.revision,
        )
    except RecoveryError:
        raise
    except (OSError, UnicodeError, ValueError, sqlite3.Error) as exc:
        raise RecoveryError(str(exc)) from exc
    finally:
        if payload_state is not None:
            payload_state.close()
        if journal_state is not None:
            journal_state.close()


class RecoveredCollectionTags:
    """Read a selected exact tag authority without reading collection payloads."""

    def __init__(self, encrypted: EncryptedArchive, head: CollectionTagHeadDocument) -> None:
        self.encrypted = encrypted
        self.head = head

    def iter_tags(self) -> Iterator[str]:
        encrypted = self.encrypted

        class NodeStore:
            def get(self, digest: str) -> bytes:
                from riverhog_protocol import COLLECTION_TAG_NODE_BYTES_MAX

                return encrypted.read_bounded(
                    collection_tag_node_path(digest), COLLECTION_TAG_NODE_BYTES_MAX
                )

            def put(self, digest: str, encoded: bytes) -> None:
                del digest, encoded
                raise TypeError("recovered tags are read-only")

        yield from CollectionTagSet(
            NodeStore(), CollectionTagSetRoot.seal(self.head.root_sha256)
        ).iter_tags()


def recover_collection_tags(
    archive_dir: Path,
    *,
    passphrases: Mapping[str, str],
    age_command: str = "age",
) -> RecoveredCollectionTags:
    with tempfile.TemporaryDirectory(prefix="riverhog-tags-") as name:
        selection = _select_archive(
            archive_dir,
            passphrases=passphrases,
            age_command=age_command,
            scratch=Path(name),
            require_provenance=False,
        )
        raw = selection.encrypted.read_bounded(COLLECTION_TAG_HEAD_RELATIVE_PATH, 64 * 1024)
        head = CollectionTagHeadDocument.from_json_bytes(raw)
        if head.archive_root_sha256 != selection.archive_root_sha256:
            raise RecoveryError("tag head belongs to another archive root")
    # The returned reader uses fresh scratch for each bounded node access.
    return RecoveredCollectionTags(
        EncryptedArchive(
            selection.archive,
            passphrase=selection.encrypted.passphrase,
            age_command=age_command,
            scratch=Path(tempfile.gettempdir()),
        ),
        head,
    )


def recover_collection_description(
    archive_dir: Path,
    *,
    passphrases: Mapping[str, str],
    age_command: str = "age",
) -> CollectionDescriptionDocument | None:
    with tempfile.TemporaryDirectory(prefix="riverhog-description-") as name:
        selection = _select_archive(
            archive_dir,
            passphrases=passphrases,
            age_command=age_command,
            scratch=Path(name),
            require_provenance=False,
        )
        path = selection.archive / COLLECTION_DESCRIPTION_RELATIVE_PATH
        if not path.exists():
            if path.is_symlink():
                raise RecoveryError("description authority traverses a symbolic link")
            return None
        raw = selection.encrypted.read_bounded(
            COLLECTION_DESCRIPTION_RELATIVE_PATH, 4 * 1024 * 1024
        )
        document = CollectionDescriptionDocument.from_json_bytes(raw)
        if document.archive_root_sha256 != selection.archive_root_sha256:
            raise RecoveryError("description belongs to another archive root")
        return document


__all__ = [
    "RecoveredCollectionTags",
    "RecoveryError",
    "RecoverySummary",
    "read_recovery_descriptor",
    "recover_archive",
    "recover_collection_description",
    "recover_collection_tags",
]
