"""Exact, paged member-history selection over authenticated archive object streams."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator

from .member_history import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    MemberHistoryBinding,
    MemberHistoryDocument,
    MemberHistoryImport,
    MemberHistoryRoot,
    history_record_page_object_path,
    member_history_object_path,
    source_binding_proof_object_path,
    verify_member_history_sets,
)
from .source_binding_proof import SOURCE_BINDING_PROOF_BYTES_MAX, SourceMemberHistoryBindingProof
from .structural_record_set import PAGE_BYTES_MAX, RecordPage, RecordSetRef


def read_bounded_history_object(chunks: Iterable[bytes], maximum: int) -> bytes:
    content = bytearray()
    for chunk in chunks:
        if type(chunk) is not bytes or len(content) + len(chunk) > maximum:
            raise ValueError("history object exceeds its bounded contract")
        content.extend(chunk)
    return bytes(content)


class MemberHistoryStore:
    """Resolve structural selections; canonical claim validation belongs to its consumer.

    A starting binding comes from the enclosing authenticated provenance root.
    An inherited binding comes only from an exact verified source membership proof.
    This reader never follows a network address or chooses a newer journal head.
    """

    def __init__(self, read_object: Callable[[str], Iterable[bytes]]) -> None:
        self.read_object = read_object

    def pages(self, authority: RecordSetRef) -> Iterator[RecordPage]:
        for ordinal in range(authority.record_count + 1):
            page = RecordPage.from_json_bytes(
                read_bounded_history_object(
                    self.read_object(
                        history_record_page_object_path(authority.records_sha256, ordinal)
                    ),
                    PAGE_BYTES_MAX,
                )
            )
            if page.authority != authority or page.ordinal != ordinal:
                raise ValueError("member history page authority changed")
            yield page
            if page.terminal:
                return
        raise ValueError("member history set lacks its terminal")

    def descriptor(self, binding: MemberHistoryBinding) -> MemberHistoryDocument:
        history = binding.verify_descriptor(
            read_bounded_history_object(
                self.read_object(member_history_object_path(binding.history_sha256)),
                binding.history_bytes,
            )
        )
        verify_member_history_sets(
            history, root_pages=self.pages(history.roots), import_pages=self.pages(history.imports)
        )
        return history

    def roots(self, binding: MemberHistoryBinding, *, extent: str) -> Iterator[MemberHistoryRoot]:
        if extent not in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT):
            raise ValueError("unsupported member history extent")
        history = self.descriptor(binding)
        for page in self.pages(history.roots):
            for row in page.records:
                root = MemberHistoryRoot.from_mapping(row["value"])
                if extent == RETAINED_HISTORY_EXTENT or root.inclusion == "bound":
                    yield root

    def imports(self, binding: MemberHistoryBinding) -> Iterator[MemberHistoryImport]:
        history = self.descriptor(binding)
        for page in self.pages(history.imports):
            for row in page.records:
                yield MemberHistoryImport.from_mapping(row["value"])

    def source_proof(self, imported: MemberHistoryImport) -> SourceMemberHistoryBindingProof:
        proof = SourceMemberHistoryBindingProof.from_json_bytes(
            read_bounded_history_object(
                self.read_object(
                    source_binding_proof_object_path(imported.source_binding_proof_sha256)
                ),
                SOURCE_BINDING_PROOF_BYTES_MAX,
            )
        )
        proof.verify_import(imported)
        self.descriptor(proof.binding)
        return proof


__all__ = ["MemberHistoryStore", "read_bounded_history_object"]
