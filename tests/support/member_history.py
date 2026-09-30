"""Explicit history authoring for current observer and archive fixtures."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from riverhog_archive_contracts import (
    MemberHistoryBuilder,
    MemberHistoryDocument,
    MemberHistoryPrimary,
    MemberHistoryRoot,
)


def member_history_fixture(
    subject: Any, binding: Any, *, roots: Iterable[MemberHistoryRoot] = ()
) -> MemberHistoryDocument:
    with MemberHistoryBuilder(
        artifact_id=subject.artifact_id,
        bytes=int(subject.bytes),
        sha256=subject.sha256,
        primary=MemberHistoryPrimary.from_mapping(
            {
                "journal": binding.journal.model_dump(mode="json"),
                "delivery_association_id": binding.delivery_association_id,
            }
        ),
    ) as builder:
        for root in roots:
            builder.add_root(root)
        return builder.seal()[1]
