"""Exact source and mode identity shared by private semantic continuations."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING

from riverhog_provenance_contracts import require_canonical_uuid_urn

from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX

if TYPE_CHECKING:
    from .journal import JournalFrame


@dataclass(frozen=True)
class SemanticSource:
    sha256: str
    sequence: str
    journal_id: str
    catalog_sha256: str
    require_profiles: bool

    def __post_init__(self) -> None:
        for digest in (self.sha256, self.catalog_sha256):
            if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise ValueError("an exact semantic source/catalog identity is required")
        if type(self.sequence) is not str or re.fullmatch(r"0|[1-9][0-9]*", self.sequence) is None:
            raise ValueError("an exact semantic source sequence is required")
        require_canonical_uuid_urn(self.journal_id)
        if type(self.require_profiles) is not bool:
            raise ValueError("an exact profile mode is required")

    @classmethod
    def from_frame(
        cls, frame: JournalFrame, *, catalog_sha256: str, require_profiles: bool
    ) -> SemanticSource:
        if len(frame.json_bytes) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            raise ValueError("semantic source exceeds its owning entry envelope")
        document = frame.document
        return cls(
            frame.sha256,
            document["sequence"],
            document["journal_id"],
            catalog_sha256,
            require_profiles,
        )


def install(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE INDEX semantic_subject ON assertions(assertion_id,object_id,type,active)"
    ).close()
    db.execute(
        "CREATE INDEX semantic_reference_type ON assertions(object_id,type,assertion_id) "
        "WHERE active=1"
    ).close()
