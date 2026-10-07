"""Closed metadata identity shared by selections and planning evidence."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import Field
from riverhog_protocol.exact_scalar import NonnegativeDecimal

from stove0_protocol.models import Sha256, Stove0ProtocolModel

if TYPE_CHECKING:
    from stove0_protocol.fork_join import ArtifactSelection


class ArtifactSelectionRef(Stove0ProtocolModel):
    """Closed reference to a separately retained selection document."""

    selection_sha256: Sha256
    artifact_count: int = Field(ge=0)
    total_bytes: NonnegativeDecimal = Field(ge=0)

    @classmethod
    def from_selection(cls, selection: ArtifactSelection) -> ArtifactSelectionRef:
        return selection.ref()
