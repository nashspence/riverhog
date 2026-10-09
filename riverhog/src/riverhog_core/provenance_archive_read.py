"""Read root-bound canonical provenance from encrypted archive objects.

The caller supplies authenticated plaintext object streams. This reader checks
the structural root and every bounded volume descriptor before exposing journal
bytes. It retains only one descriptor and one payload segment at a time.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    PAGE_BYTES_MAX,
    PROVENANCE_BINDING_PAGE_MEMBERS_MAX,
    PROVENANCE_BINDINGS_FORMAT,
    PROVENANCE_METADATA_BYTES_MAX,
    PROVENANCE_SEQUENCE_DOMAIN,
    PROVENANCE_TERMINAL_FORMAT,
    RETAINED_HISTORY_EXTENT,
    SOURCE_BINDING_PROOF_BYTES_MAX,
    BindingTreeCommitment,
    MemberHistoryBinding,
    MemberHistoryDocument,
    MemberHistoryImport,
    MemberHistoryRoot,
    MemberHistoryStore,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    RecordPage,
    RecordSetRef,
    SourceMemberHistoryBindingProof,
    binding_tree_commitment,
    format_archive_sequence,
    history_record_page_object_path,
    member_history_object_path,
    provenance_structure_identity,
    provenance_structure_object_path,
    source_binding_proof_object_path,
    update_provenance_commitment,
    validate_member_history_binding_page,
    verify_member_history_sets,
)
from riverhog_canonical_json import require_canonical_json

ObjectReader = Callable[[str], Iterator[bytes]]


class ProvenanceArchiveReadError(ValueError):
    """Archived canonical provenance differs from its selected root."""


@dataclass(frozen=True, slots=True)
class ProvenanceArchiveSummary:
    root: ProvenanceRootDocument
    volume_count: int


def _read_bounded(chunks: Iterator[bytes], maximum: int) -> bytes:
    content = bytearray()
    for chunk in chunks:
        if type(chunk) is not bytes or len(content) + len(chunk) > maximum:
            raise ProvenanceArchiveReadError("provenance object exceeds its bounded contract")
        content.extend(chunk)
    return bytes(content)


class CanonicalProvenanceArchiveReader:
    def __init__(
        self,
        read_object: ObjectReader,
        *,
        expected_root_sha256: str,
        archive_generation: str,
        artifact_set_sha256: str,
    ) -> None:
        self._read_object = read_object
        self._expected_root_sha256 = expected_root_sha256
        self._archive_generation = archive_generation
        self._artifact_set_sha256 = artifact_set_sha256
        self._prepared_db: sqlite3.Connection | None = None
        self._prepared_summary: ProvenanceArchiveSummary | None = None
        self._prepared_cursors: set[sqlite3.Cursor] = set()

    @contextmanager
    def cached(self) -> Iterator[CanonicalProvenanceArchiveReader]:
        """Cache exact immutable object streams on protected disk for one operation.

        The selected root and normal fixity checks remain authoritative. A failed
        or interrupted stream never installs a cache entry.
        """
        with TemporaryDirectory(prefix="riverhog-provenance-read-") as scratch:
            directory = Path(scratch)

            def read(path: str) -> Iterator[bytes]:
                target = directory / hashlib.sha256(path.encode()).hexdigest()
                if not target.exists():
                    pending = target.with_suffix(".pending")
                    try:
                        with os.fdopen(
                            os.open(pending, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "wb"
                        ) as output:
                            for chunk in self._read_object(path):
                                output.write(chunk)
                            output.flush()
                        pending.replace(target)
                    finally:
                        pending.unlink(missing_ok=True)
                with target.open("rb") as source:
                    while chunk := source.read(128 * 1024):
                        yield chunk

            yield CanonicalProvenanceArchiveReader(
                read,
                expected_root_sha256=self._expected_root_sha256,
                archive_generation=self._archive_generation,
                artifact_set_sha256=self._artifact_set_sha256,
            )

    @contextmanager
    def prepared(self) -> Iterator[CanonicalProvenanceArchiveReader]:
        """Verify once and seek immutable volume metadata through a disk index."""
        if self._prepared_db is not None:
            yield self
            return
        with TemporaryDirectory(prefix="riverhog-provenance-metadata-") as scratch:
            # The operation owns this index. Its serial HTTP iterator may
            # advance and close on different worker threads.
            db = sqlite3.connect(Path(scratch) / "metadata.sqlite3", check_same_thread=False)
            db.executescript(
                "PRAGMA cache_size = -512; PRAGMA temp_store = FILE; "
                "CREATE TABLE volumes(sequence TEXT PRIMARY KEY, kind TEXT, "
                "journal TEXT, body BLOB); "
                "CREATE INDEX volume_journals ON volumes(journal, sequence); "
                "CREATE INDEX volume_kinds ON volumes(kind, sequence);"
            )

            def remember(document: ProvenanceVolumeDocument | ProvenanceTerminalDocument) -> None:
                if isinstance(document, ProvenanceTerminalDocument):
                    kind, journal_id = "terminal", None
                else:
                    kind, journal_id = document.payload.kind, document.journal_id
                db.execute(
                    "INSERT INTO volumes VALUES (?, ?, ?, ?)",
                    (
                        format_archive_sequence(document.sequence),
                        kind,
                        journal_id,
                        document.to_json_bytes(),
                    ),
                )

            try:
                summary = self._scan(remember)
                db.commit()
                db.execute("PRAGMA query_only = ON")
                self._prepared_db = db
                self._prepared_summary = summary
                yield self
            finally:
                self._prepared_db = None
                self._prepared_summary = None
                for cursor in self._prepared_cursors:
                    cursor.close()
                self._prepared_cursors.clear()
                db.close()

    def _history_pages(self, authority: RecordSetRef) -> Iterator[RecordPage]:
        """Read a bounded set through its mandatory terminal, with no total cap."""

        for ordinal in range(authority.record_count + 1):
            raw = _read_bounded(
                self._read_object(
                    history_record_page_object_path(authority.records_sha256, ordinal)
                ),
                PAGE_BYTES_MAX,
            )
            page = RecordPage.from_json_bytes(raw)
            if page.authority != authority or page.ordinal != ordinal:
                raise ProvenanceArchiveReadError("member history set page authority changed")
            yield page
            if page.terminal:
                return
        raise ProvenanceArchiveReadError("member history set lacks its terminal")

    def history_store(self) -> MemberHistoryStore:
        return MemberHistoryStore(self._read_object)

    def structure_object(self, object_id: str) -> bytes:
        # The requested hash or record-set authority identifies this bounded
        # history object; its consumer verifies the selected set commitment.
        # Authenticate the selected archive root here; journal/binding reads
        # separately verify their complete volume sequence and terminal.
        self._root()
        raw = _read_bounded(
            self._read_object(provenance_structure_object_path(object_id)), PAGE_BYTES_MAX
        )
        if provenance_structure_identity(raw).object_id != object_id:
            raise ProvenanceArchiveReadError("history structure differs from its identity")
        return raw

    def member_history(self, binding: MemberHistoryBinding) -> MemberHistoryDocument:
        """Verify the exact descriptor and both complete structural selections.

        The binding must come from the root-authenticated final binding page;
        this verifies structure, while claim readers also resolve the journal and
        source-proof closure at the explicitly requested extent.
        """

        raw = _read_bounded(
            self._read_object(member_history_object_path(binding.history_sha256)),
            binding.history_bytes,
        )
        descriptor = binding.verify_descriptor(raw)
        try:
            verify_member_history_sets(
                descriptor,
                root_pages=self._history_pages(descriptor.roots),
                import_pages=self._history_pages(descriptor.imports),
            )
        except ValueError as exc:
            raise ProvenanceArchiveReadError(str(exc)) from exc
        return descriptor

    def iter_selected_history_roots(
        self,
        binding: MemberHistoryBinding,
        *,
        extent: str = BOUND_HISTORY_EXTENT,
    ) -> Iterator[MemberHistoryRoot]:
        """Enumerate exact selected snapshots after verifying the whole descriptor."""

        if extent not in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT):
            raise ProvenanceArchiveReadError("unsupported member history extent")
        descriptor = self.member_history(binding)
        for page in self._history_pages(descriptor.roots):
            for row in page.records:
                selected = MemberHistoryRoot.from_mapping(row["value"])
                if extent == RETAINED_HISTORY_EXTENT or selected.inclusion == "bound":
                    yield selected

    def iter_history_imports(self, binding: MemberHistoryBinding) -> Iterator[MemberHistoryImport]:
        descriptor = self.member_history(binding)
        for page in self._history_pages(descriptor.imports):
            for row in page.records:
                yield MemberHistoryImport.from_mapping(row["value"])

    def source_binding_proof(
        self, imported: MemberHistoryImport
    ) -> SourceMemberHistoryBindingProof:
        raw = _read_bounded(
            self._read_object(
                source_binding_proof_object_path(imported.source_binding_proof_sha256)
            ),
            SOURCE_BINDING_PROOF_BYTES_MAX,
        )
        try:
            proof = SourceMemberHistoryBindingProof.from_json_bytes(raw)
            proof.verify_import(imported)
        except ValueError as exc:
            raise ProvenanceArchiveReadError(str(exc)) from exc
        return proof

    def binding_inclusion(self, artifact_id: str) -> BindingTreeCommitment:
        """Verify and prove one final binding without revealing sibling records."""

        root = self.scan().root
        commitment = binding_tree_commitment(
            (MemberHistoryBinding.from_mapping(row) for row in self.iter_bindings()),
            target_artifact_id=artifact_id,
        )
        if (commitment.count, commitment.root_sha256) != (
            root.binding_count,
            root.binding_tree_sha256,
        ):
            raise ProvenanceArchiveReadError("member binding tree differs from the root")
        return commitment

    def _root(self) -> ProvenanceRootDocument:
        if self._prepared_summary is not None:
            return self._prepared_summary.root
        raw = _read_bounded(
            self._read_object("provenance/root.json.age"), PROVENANCE_METADATA_BYTES_MAX
        )
        if hashlib.sha256(raw).hexdigest() != self._expected_root_sha256:
            raise ProvenanceArchiveReadError("provenance root identity changed")
        root = ProvenanceRootDocument.from_json_bytes(raw)
        if (
            root.archive_generation != self._archive_generation
            or root.artifact_set_sha256 != self._artifact_set_sha256
        ):
            raise ProvenanceArchiveReadError("provenance root names another archive")
        return root

    def _descriptors(
        self,
        *,
        journal_id: str | None = None,
        kind: str | None = None,
    ) -> Iterator[ProvenanceVolumeDocument | ProvenanceTerminalDocument]:
        if self._prepared_db is not None:
            filters = []
            values = []
            if journal_id is not None:
                filters.append("journal = ?")
                values.append(journal_id)
            if kind is not None:
                filters.append("kind = ?")
                values.append(kind)
            where = " WHERE " + " AND ".join(filters) if filters else ""
            cursor = self._prepared_db.execute(
                "SELECT kind, body FROM volumes" + where + " ORDER BY sequence", values
            )
            self._prepared_cursors.add(cursor)
            try:
                for selected_kind, raw in cursor:
                    yield (
                        ProvenanceTerminalDocument.from_json_bytes(raw)
                        if selected_kind == "terminal"
                        else ProvenanceVolumeDocument.from_json_bytes(raw)
                    )
            finally:
                if cursor in self._prepared_cursors:
                    self._prepared_cursors.remove(cursor)
                    cursor.close()
            return
        sequence = 0
        while True:
            path = f"provenance/metadata/volume-{format_archive_sequence(sequence)}.json.age"
            raw = _read_bounded(self._read_object(path), PROVENANCE_METADATA_BYTES_MAX)
            value = require_canonical_json(raw)
            if not isinstance(value, dict):
                raise ProvenanceArchiveReadError("provenance volume is not an object")
            if value.get("format") == PROVENANCE_TERMINAL_FORMAT:
                terminal = ProvenanceTerminalDocument.from_json_bytes(raw)
                if terminal.sequence != sequence:
                    raise ProvenanceArchiveReadError("provenance terminal sequence changed")
                if journal_id is None and kind is None:
                    yield terminal
                return
            document = ProvenanceVolumeDocument.from_json_bytes(raw)
            if document.sequence != sequence:
                raise ProvenanceArchiveReadError("provenance volume sequence changed")
            if (journal_id is None or document.journal_id == journal_id) and (
                kind is None or document.payload.kind == kind
            ):
                yield document
            sequence += 1

    def scan(self) -> ProvenanceArchiveSummary:
        """Verify the complete root-selected descriptor sequence without payload reads."""

        if self._prepared_summary is not None:
            return self._prepared_summary
        return self._scan()

    def _scan(
        self,
        remember: Callable[[ProvenanceVolumeDocument | ProvenanceTerminalDocument], None]
        | None = None,
    ) -> ProvenanceArchiveSummary:

        root = self._root()
        digest = hashlib.sha256(PROVENANCE_SEQUENCE_DOMAIN)
        expected = 0
        binding_count = 0
        journal_count = 0
        last_artifact_id: str | None = None
        current_journal_id: str | None = None
        current_journal_offset = 0
        current_journal_bytes = 0
        current_journal_sha256: str | None = None
        for document in self._descriptors():
            if remember is not None:
                remember(document)
            if document.sequence != expected or (
                document.archive_generation,
                document.artifact_set_sha256,
            ) != (root.archive_generation, root.artifact_set_sha256):
                raise ProvenanceArchiveReadError("provenance volume sequence changes authority")
            update_provenance_commitment(digest, document)
            expected += 1
            if isinstance(document, ProvenanceTerminalDocument):
                if (
                    current_journal_id is not None
                    and current_journal_offset != current_journal_bytes
                ):
                    raise ProvenanceArchiveReadError("last provenance journal is incomplete")
                break
            if document.payload.kind == "bindings":
                if current_journal_id is not None:
                    raise ProvenanceArchiveReadError("binding page follows a journal segment")
                assert document.first_artifact_id is not None
                assert document.last_artifact_id is not None
                assert document.binding_count is not None
                if last_artifact_id is not None and document.first_artifact_id <= last_artifact_id:
                    raise ProvenanceArchiveReadError("provenance binding pages overlap")
                last_artifact_id = document.last_artifact_id
                binding_count += document.binding_count
                continue
            assert document.journal_id is not None
            assert document.journal_offset is not None
            assert document.journal_bytes is not None
            assert document.journal_sha256 is not None
            if document.journal_id != current_journal_id:
                if (
                    current_journal_id is not None
                    and current_journal_offset != current_journal_bytes
                ):
                    raise ProvenanceArchiveReadError("provenance journal is incomplete")
                if document.journal_offset != 0:
                    raise ProvenanceArchiveReadError("provenance journal does not start at zero")
                if current_journal_id is not None and document.journal_id <= current_journal_id:
                    raise ProvenanceArchiveReadError(
                        "provenance journal identities are not ordered"
                    )
                current_journal_id = document.journal_id
                current_journal_bytes = document.journal_bytes
                current_journal_sha256 = document.journal_sha256
                current_journal_offset = 0
                journal_count += 1
            if (
                document.journal_offset != current_journal_offset
                or document.journal_bytes != current_journal_bytes
                or document.journal_sha256 != current_journal_sha256
            ):
                raise ProvenanceArchiveReadError("provenance journal segments are not contiguous")
            current_journal_offset += document.payload.bytes
        if (
            digest.hexdigest() != root.ordered_volume_sha256
            or binding_count != root.binding_count
            or journal_count != root.journal_count
        ):
            raise ProvenanceArchiveReadError("provenance sequence differs from the root")
        return ProvenanceArchiveSummary(root=root, volume_count=expected - 1)

    def iter_journal_range(
        self, journal_id: str, *, offset: int = 0, size: int | None = None
    ) -> Iterator[bytes]:
        """Read exact journal bytes; every touched segment is verified in full."""

        self.scan()
        if (
            type(offset) is not int
            or offset < 0
            or (size is not None and (type(size) is not int or size < 0))
        ):
            raise ProvenanceArchiveReadError("journal range is invalid")
        journal_bytes: int | None = None
        journal_sha256: str | None = None
        full_digest = hashlib.sha256() if offset == 0 and size is None else None
        emitted = 0
        for document in self._descriptors(journal_id=journal_id, kind="journal"):
            if isinstance(document, ProvenanceTerminalDocument):
                break
            if document.payload.kind != "journal" or document.journal_id != journal_id:
                continue
            if journal_bytes is None:
                journal_bytes = document.journal_bytes
                journal_sha256 = document.journal_sha256
            assert document.journal_offset is not None
            assert journal_bytes is not None
            start = document.journal_offset
            end = start + document.payload.bytes
            wanted_end = journal_bytes if size is None else offset + size
            if end <= offset or start >= wanted_end:
                continue
            content = _read_bounded(
                self._read_object(document.payload.path), document.payload.bytes
            )
            if (
                len(content) != document.payload.bytes
                or hashlib.sha256(content).hexdigest() != document.payload.sha256
            ):
                raise ProvenanceArchiveReadError("provenance journal segment changed")
            if full_digest is not None:
                full_digest.update(content)
            begin = max(offset, start) - start
            stop = min(wanted_end, end) - start
            emitted += stop - begin
            yield content[begin:stop]
        if journal_bytes is None or journal_sha256 is None:
            raise ProvenanceArchiveReadError("provenance journal is absent")
        expected = journal_bytes - offset if size is None else size
        if offset > journal_bytes or expected < 0 or offset + expected > journal_bytes:
            raise ProvenanceArchiveReadError("journal range exceeds its exact byte count")
        if emitted != expected:
            raise ProvenanceArchiveReadError("provenance journal range is incomplete")
        if full_digest is not None and full_digest.hexdigest() != journal_sha256:
            raise ProvenanceArchiveReadError("provenance journal digest changed")

    def journal_metadata(self, journal_id: str) -> tuple[int, str]:
        """Read one root-selected journal's exact length and digest authority."""

        self.scan()
        result: tuple[int, str] | None = None
        for document in self._descriptors(journal_id=journal_id, kind="journal"):
            if isinstance(document, ProvenanceTerminalDocument):
                break
            if document.payload.kind != "journal" or document.journal_id != journal_id:
                continue
            assert document.journal_bytes is not None
            assert document.journal_sha256 is not None
            candidate = (document.journal_bytes, document.journal_sha256)
            if result is not None and result != candidate:
                raise ProvenanceArchiveReadError("journal segment authority changed")
            result = candidate
        if result is None:
            raise ProvenanceArchiveReadError("provenance journal is absent")
        return result

    def iter_journal_headers(self) -> Iterator[tuple[str, int, str]]:
        """Enumerate root-selected journal identities and fixity without payloads."""

        self.scan()
        previous: str | None = None
        for document in self._descriptors(kind="journal"):
            if isinstance(document, ProvenanceTerminalDocument):
                return
            if document.payload.kind != "journal" or document.journal_id == previous:
                continue
            assert document.journal_id is not None
            assert document.journal_bytes is not None
            assert document.journal_sha256 is not None
            previous = document.journal_id
            yield previous, document.journal_bytes, document.journal_sha256

    def iter_journal_ids(self) -> Iterator[str]:
        """Enumerate the exact root-selected journal corpus without loading payloads."""

        for journal_id, _bytes, _sha256 in self.iter_journal_headers():
            yield journal_id

    def iter_bindings(self) -> Iterator[dict[str, object]]:
        """Stream exact member-ordered binding pages from the selected archive."""

        self.scan()
        last_id: str | None = None
        for document in self._descriptors(kind="bindings"):
            if isinstance(document, ProvenanceTerminalDocument):
                return
            if document.payload.kind != "bindings":
                continue
            content = _read_bounded(
                self._read_object(document.payload.path), document.payload.bytes
            )
            if (
                len(content) != document.payload.bytes
                or hashlib.sha256(content).hexdigest() != document.payload.sha256
            ):
                raise ProvenanceArchiveReadError("provenance binding page changed")
            value = require_canonical_json(content)
            if not isinstance(value, dict) or set(value) != {"format", "bindings"}:
                raise ProvenanceArchiveReadError("provenance binding page is invalid")
            if value["format"] != PROVENANCE_BINDINGS_FORMAT:
                raise ProvenanceArchiveReadError("provenance binding page format changed")
            raw_bindings = value["bindings"]
            if not isinstance(raw_bindings, list) or not all(
                isinstance(row, dict) for row in raw_bindings
            ):
                raise ProvenanceArchiveReadError("provenance binding page rows are invalid")
            try:
                bindings = validate_member_history_binding_page(
                    cast(list[Mapping[str, Any]], raw_bindings),
                    max_members=PROVENANCE_BINDING_PAGE_MEMBERS_MAX,
                )
            except ValueError as exc:
                raise ProvenanceArchiveReadError(
                    "provenance binding page rows are invalid"
                ) from exc
            if (
                len(bindings) != document.binding_count
                or bindings[0].artifact_id != document.first_artifact_id
                or bindings[-1].artifact_id != document.last_artifact_id
            ):
                raise ProvenanceArchiveReadError("provenance binding page range changed")
            for binding in bindings:
                if last_id is not None and binding.artifact_id <= last_id:
                    raise ProvenanceArchiveReadError("provenance bindings are not member ordered")
                last_id = binding.artifact_id
                yield binding.to_mapping()


__all__ = [
    "CanonicalProvenanceArchiveReader",
    "ProvenanceArchiveReadError",
    "ProvenanceArchiveSummary",
]
