"""Exact placement and initial classification of a derived collection."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, model_validator

from riverhog_protocol.collection_tags import CollectionTag
from riverhog_protocol.storage_names import ArchiveStoreName


class OutputCollectionPolicy(BaseModel):
    """Deployment-owned names only; no storage credentials or target configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    format: Literal["riverhog-output-collection-policy/v1"] = "riverhog-output-collection-policy/v1"
    archive_store: ArchiveStoreName | None = None
    use_cache: bool | None = None
    copy_to: tuple[ArchiveStoreName, ...] = ()
    tags: tuple[CollectionTag, ...] = ()

    @model_validator(mode="after")
    def canonical_members(self) -> Self:
        if self.copy_to != tuple(sorted(set(self.copy_to))):
            raise ValueError("output copy_to stores must be unique and ordered")
        if self.archive_store in self.copy_to:
            raise ValueError("output copy_to cannot include its archive store")
        if self.tags != tuple(sorted(set(self.tags), key=lambda value: value.encode("utf-8"))):
            raise ValueError("output classification tags must be unique and ordered")
        return self


__all__ = ["OutputCollectionPolicy"]
