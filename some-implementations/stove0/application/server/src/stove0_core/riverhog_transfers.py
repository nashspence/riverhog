"""Resumable metadata transfer checkpoints; no bearer or payload retention."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict
from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
    Index,
    MetaData,
    String,
    Table,
    Text,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine, RowMapping
from stove0_protocol import canonical_json_bytes, canonical_json_sha256

from stove0_core._checkpoint_sha256 import CheckpointSHA256
from stove0_core.planning_context import PlanningContext
from stove0_core.work_state import ConcurrentWorkUpdate


class TransferCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    stage: Literal["collecting", "sealing", "complete"] = "collecting"
    receiving_id: str | None = None
    after_collection_id: int = 0
    after_artifact_id: str = ""
    count: int = 0
    total_bytes: int = 0
    hash_state: str
    disposition_ordinal: int = 0
    evidence_after_sha256: str = ""


def declare_riverhog_transfers(metadata: MetaData) -> Table:
    return Table(
        "stove0_riverhog_transfers",
        metadata,
        Column("transfer_id", String(129), primary_key=True),
        Column(
            "context_id",
            String(64),
            ForeignKey("stove0_planning_contexts.context_id", ondelete="CASCADE"),
            nullable=False,
        ),
        Column("revision", BigInteger, nullable=False),
        Column("binding_json", Text, nullable=False),
        Column("checkpoint_json", Text, nullable=False),
        Index("ix_stove0_riverhog_transfer_owner", "context_id", "transfer_id"),
    )


class RiverhogTransferStore:
    def __init__(self, engine: Engine, table: Table, context: PlanningContext) -> None:
        self.engine, self.table, self.context = engine, table, context

    def ensure(
        self, binding: dict[str, Any], *, domain: bytes = b"riverhog-claim-artifacts/v1\0"
    ) -> tuple[RowMapping, TransferCheckpoint]:
        if self.context.context_id is None:
            raise ValueError("metadata transfers require an invocation owner")
        identity = self.context.key(canonical_json_sha256(binding))
        encoded = canonical_json_bytes(binding).decode("utf-8")
        initial = TransferCheckpoint(hash_state=CheckpointSHA256(domain).export_state())
        insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
        t = self.table
        with self.engine.begin() as connection:
            connection.execute(
                insert(t)
                .values(
                    transfer_id=identity,
                    context_id=self.context.context_id,
                    revision=1,
                    binding_json=encoded,
                    checkpoint_json=canonical_json_bytes(initial.model_dump()).decode("utf-8"),
                )
                .on_conflict_do_nothing()
            )
            row = connection.execute(select(t).where(t.c.transfer_id == identity)).mappings().one()
        if row["binding_json"] != encoded:
            raise ValueError("metadata transfer binding changed")
        return row, TransferCheckpoint.model_validate_json(row["checkpoint_json"])

    def advance(
        self, row: RowMapping, checkpoint: TransferCheckpoint, **changes: Any
    ) -> TransferCheckpoint:
        replacement = TransferCheckpoint.model_validate({**checkpoint.model_dump(), **changes})
        t = self.table
        with self.engine.begin() as connection:
            result = connection.execute(
                update(t)
                .where(t.c.transfer_id == row["transfer_id"], t.c.revision == row["revision"])
                .values(
                    revision=row["revision"] + 1,
                    checkpoint_json=canonical_json_bytes(replacement.model_dump()).decode("utf-8"),
                )
            )
            if result.rowcount != 1:
                raise ConcurrentWorkUpdate("metadata transfer checkpoint changed")
        return replacement
