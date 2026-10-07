"""Paged exact group membership; primary declarations survive empty attachments."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import Field, StrictBool, model_validator
from riverhog_protocol.exact_scalar import NonnegativeDecimal

from stove0_protocol.fork_join import ArtifactSelectionRef
from stove0_protocol.jcs import CommitmentDigest, canonical_json_bytes, canonical_json_sha256
from stove0_protocol.models import RecipeIdentityRef, Sha256, Stove0ProtocolModel
from stove0_protocol.observation_evidence import AcceptedView
from stove0_protocol.predicates import LocalName

INPUT_GROUP_PAGE_MAX = 100


class InputGroupMember(Stove0ProtocolModel):
    primary_id: str = Field(min_length=1)
    associated_id: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def distinct_instances(self) -> Self:
        if self.primary_id == self.associated_id:
            raise ValueError("an input group cannot attach its primary to itself")
        return self


def update_input_group_commitment(
    digest: CommitmentDigest, *, ordinal: int, member: InputGroupMember
) -> None:
    if type(ordinal) is not int or ordinal < 0:
        raise ValueError("group membership ordinal must be nonnegative")
    encoded = canonical_json_bytes(member.model_dump(mode="json"))
    digest.update(b"stove0-input-group-members/v1\x00")
    digest.update(str(ordinal).encode("ascii"))
    digest.update(b"\x00")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)


class InputGroupSetPayload(Stove0ProtocolModel):
    format: Literal["stove0-input-groups/v1"] = "stove0-input-groups/v1"
    work_id: Sha256
    recipe: RecipeIdentityRef
    group_id: LocalName
    inventory: ArtifactSelectionRef
    supports: tuple[AcceptedView, ...]
    group_count: NonnegativeDecimal
    association_count: NonnegativeDecimal
    members_sha256: Sha256

    @model_validator(mode="after")
    def exact_support(self) -> Self:
        keys = [view.view_sha256 for view in self.supports]
        if len(keys) != len(set(keys)) or any(
            view.work_id != self.work_id for view in self.supports
        ):
            raise ValueError("group support must retain distinct exact views of this work")
        # Order is the compiled preference order, rather than a set ordering.
        return self


class InputGroupSet(InputGroupSetPayload):
    group_set_sha256: Sha256

    @model_validator(mode="after")
    def exact_identity(self) -> Self:
        if self.group_set_sha256 != canonical_json_sha256(
            self.model_dump(mode="json", by_alias=True, exclude={"group_set_sha256"})
        ):
            raise ValueError("group set differs from its exact membership and support")
        return self

    @classmethod
    def seal(cls, payload: InputGroupSetPayload) -> Self:
        document = payload.model_dump(mode="json", by_alias=True)
        return cls.model_validate({**document, "group_set_sha256": canonical_json_sha256(document)})


class InputGroupPage(Stove0ProtocolModel):
    authority: InputGroupSet
    start_ordinal: NonnegativeDecimal
    members: tuple[InputGroupMember, ...] = Field(
        max_length=INPUT_GROUP_PAGE_MAX,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-input-group-page",
                "progression": "group-set-bound-ordinal",
            }
        },
    )
    complete: StrictBool

    @model_validator(mode="after")
    def exact_continuation(self) -> Self:
        count = self.authority.group_count + self.authority.association_count
        end = self.start_ordinal + len(self.members)
        if end > count or self.complete != (end == count):
            raise ValueError("group page differs from its exact membership authority")
        if not self.members and not self.complete:
            raise ValueError("incomplete group page makes no progress")
        keys = [(member.primary_id, member.associated_id or "") for member in self.members]
        if keys != sorted(set(keys)):
            raise ValueError("group page members must be distinct and canonically ordered")
        return self
