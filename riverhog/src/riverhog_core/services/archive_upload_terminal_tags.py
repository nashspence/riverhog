"""Original tag selector facts retained with one exact terminal operation."""

from __future__ import annotations

from dataclasses import dataclass

from riverhog_protocol import CollectionTagSetRoot
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy.orm import Session

from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalTagRecord
from riverhog_core.catalog_models import (
    CollectionUploadRecord,
    CollectionUploadTagNodeReferenceRecord,
)


@dataclass(frozen=True, slots=True)
class TerminalTagSelector:
    """An original immutable set reference; current access remains caller-owned."""

    root_sha256: str | None
    tag_set_identity: str

    def __post_init__(self) -> None:
        try:
            root = CollectionTagSetRoot.seal(self.root_sha256)
        except ValueError as exc:
            raise Conflict("upload terminal original tag selector is invalid") from exc
        if root.tag_set_identity != self.tag_set_identity:
            raise Conflict("upload terminal original tag selector identity changed")

    def payload(self) -> dict[str, str | None]:
        return {
            "root_sha256": self.root_sha256,
            "tag_set_identity": self.tag_set_identity,
        }


def upload_terminal_tag_selector(upload: CollectionUploadRecord) -> TerminalTagSelector:
    """Select raw context while the caller holds the original upload parent."""
    return TerminalTagSelector(upload.tag_staging_root_sha256, upload.tag_staging_set_identity)


def retain_terminal_tag_selector(
    session: Session,
    upload: CollectionUploadRecord,
    operation_id: str,
    selector: TerminalTagSelector,
) -> None:
    """Pin one already-retained root in the terminal selection transaction.

    Structural edges retain its descendants. No tag walk, provider work or
    permission adjudication runs here. Upload references still protect the root
    while this caller holds its parent and installs the terminal reference.
    """
    if upload_terminal_tag_selector(upload) != selector:
        raise Conflict("upload terminal selected tag set changed")
    if (
        selector.root_sha256 is not None
        and session.get(
            CollectionUploadTagNodeReferenceRecord,
            (upload.collection_id, selector.root_sha256),
        )
        is None
    ):
        raise NotFound("upload terminal original tag root is unavailable")
    session.add(
        ArchiveUploadTerminalTagRecord(
            terminal_id=operation_id,
            root_sha256=selector.root_sha256,
            tag_set_identity=selector.tag_set_identity,
        )
    )


def load_terminal_tag_selector(session: Session, operation_id: str) -> TerminalTagSelector:
    """Read the retained original selector after the caller validates its terminal.

    This point reference survives upload/tag row teardown. It supplies neither
    current read/disposal permission nor a writer-quiescence decision.
    """
    selected = session.get(ArchiveUploadTerminalTagRecord, operation_id, populate_existing=True)
    if selected is None:
        raise NotFound("upload terminal original tag selector is unavailable")
    return TerminalTagSelector(selected.root_sha256, selected.tag_set_identity)
