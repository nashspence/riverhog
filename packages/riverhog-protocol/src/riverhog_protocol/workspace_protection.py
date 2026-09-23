"""Deployment-declared protection for plaintext processing workspaces."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field

type DeclaredWorkspaceProtection = Annotated[
    Literal["encrypted-at-rest", "memory-backed"],
    Field(
        description=(
            "Deployment declaration for plaintext workspace storage. "
            "Memory-backed storage requires no unencrypted swap. "
            "The runtime does not verify the mount or swap policy."
        )
    ),
]

__all__ = ["DeclaredWorkspaceProtection"]
