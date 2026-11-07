"""Operator admission for application-owned prepared-primary state."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from riverhog_client._prepared_primary_documents import PrimaryScope
from riverhog_client._prepared_primary_store import PrimaryStoreLimits


class PrimaryStatePolicy(BaseModel):
    """Configured allocation scope and explicit durable-byte admission."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    scope: PrimaryScope
    pending_bytes: int = Field(default=256 * 1024**2, strict=True, gt=0)
    total_bytes: int = Field(default=2 * 1024**3, strict=True, gt=0)
    preparation_bytes: int = Field(default=64 * 1024**2, strict=True, gt=0)
    control_bytes: int = Field(default=64 * 1024, strict=True, gt=0)
    inventory_order_bytes: int = Field(default=16 * 1024**2, strict=True, ge=4096)

    @model_validator(mode="after")
    def admitted_root(self) -> PrimaryStatePolicy:
        self.limits()
        return self

    def limits(self) -> PrimaryStoreLimits:
        return PrimaryStoreLimits(
            pending_bytes=self.pending_bytes,
            total_bytes=self.total_bytes,
            preparation_bytes=self.preparation_bytes,
            control_bytes=self.control_bytes,
            inventory_order_bytes=self.inventory_order_bytes,
        )
