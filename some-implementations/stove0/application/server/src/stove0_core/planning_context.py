"""Invocation-qualified storage keys; semantic work and question IDs stay exact."""

from __future__ import annotations

from typing import Self

from sqlalchemy import CheckConstraint, Column, Index, MetaData, String, Table, Text
from stove0_protocol import canonical_json_sha256


def declare_planning_contexts(metadata: MetaData) -> Table:
    return Table(
        "stove0_planning_contexts",
        metadata,
        Column("context_id", String(64), primary_key=True),
        Column("owner_kind", String(16), nullable=False),
        Column("owner_id", String(64), nullable=False),
        Column("updated_at", Text, nullable=False),
        CheckConstraint(
            "owner_kind IN ('work','preview')", name="ck_stove0_planning_context_owner"
        ),
        Index("ix_stove0_planning_context_owner", "owner_kind", "owner_id"),
        Index("ix_stove0_planning_context_updated", "updated_at", "context_id"),
    )


class PlanningContext:
    def __init__(self, context_id: str | None = None) -> None:
        self.context_id = context_id

    @classmethod
    def owned(cls, owner_kind: str, owner_id: str) -> Self:
        if owner_kind not in {"work", "preview"}:
            raise ValueError("planning requires an ordinary work or preview invocation owner")
        return cls(canonical_json_sha256({"owner_kind": owner_kind, "owner_id": owner_id}))

    def key(self, identity: str) -> str:
        if self.context_id is None:
            return identity
        prefix = self.context_id + ":"
        if ":" in identity:
            if not identity.startswith(prefix):
                raise ValueError("planning storage key belongs to another invocation owner")
            return identity
        return prefix + identity
