"""Read root-bound canonical provenance from encrypted archive objects.

The caller supplies authenticated plaintext object streams. This reader checks
the structural root and every bounded volume descriptor before exposing journal
bytes. It retains only one descriptor and one payload segment at a time.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, cast

from riverhog_archive_contracts import (
    PROVENANCE_BINDING_PAGE_MEMBERS_MAX,
    PROVENANCE_BINDINGS_FORMAT,
    PROVENANCE_METADATA_BYTES_MAX,
    PROVENANCE_SEQUENCE_DOMAIN,
    PROVENANCE_TERMINAL_FORMAT,
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    ProvenanceVolumeDocument,
    format_archive_sequence,
    update_provenance_commitment,
)
from riverhog_canonical_json import require_canonical_json
from riverhog_protocol import validate_archive_binding_page

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

    def _root(self) -> ProvenanceRootDocument:
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
    ) -> Iterator[ProvenanceVolumeDocument | ProvenanceTerminalDocument]:
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
                yield terminal
                return
            document = ProvenanceVolumeDocument.from_json_bytes(raw)
            if document.sequence != sequence:
                raise ProvenanceArchiveReadError("provenance volume sequence changed")
            yield document
            sequence += 1

    def scan(self) -> ProvenanceArchiveSummary:
        """Verify the complete root-selected descriptor sequence without payload reads."""

        root = self._root()
        digest = hashlib.sha256(PROVENANCE_SEQUENCE_DOMAIN)
        expected = 0
        binding_count = 0
        journal_count = 0
        operation_journal_seen = False
        last_artifact_id: str | None = None
        current_journal_id: str | None = None
        current_journal_offset = 0
        current_journal_bytes = 0
        current_journal_sha256: str | None = None
        for document in self._descriptors():
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
                if document.journal_id == root.operation_journal_id:
                    operation_journal_seen = True
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
            or (root.operation_journal_id is not None and not operation_journal_seen)
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
        for document in self._descriptors():
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
        for document in self._descriptors():
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
        for document in self._descriptors():
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
        for document in self._descriptors():
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
                bindings = validate_archive_binding_page(
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
                yield binding.model_dump(mode="json")


__all__ = [
    "CanonicalProvenanceArchiveReader",
    "ProvenanceArchiveReadError",
    "ProvenanceArchiveSummary",
]
