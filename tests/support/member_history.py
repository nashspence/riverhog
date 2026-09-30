"""Explicit history authoring for current observer and archive fixtures."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from riverhog_archive_contracts import (
    MemberHistoryBuilder,
    MemberHistoryDocument,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    MemberHistoryStore,
    provenance_structure_identity,
)
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_MEMBER_ROLE,
    collection_production_contract,
)
from riverhog_provenance import MemberHistoryClosure
from riverhog_provenance_contracts import ContractCatalog


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


def member_history_selection_fixture(subject, binding, corpus, *, roots=()):
    """Author complete explicit structural objects for small canonical fixtures."""
    with MemberHistoryBuilder(
        artifact_id=str(subject.artifact_id),
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
        selected, history = builder.seal()
        encoded = [history.to_json_bytes()]
        for authority in (history.roots, history.imports):
            encoded.extend(page.to_json_bytes() for page in builder.pages(authority))
    objects = {provenance_structure_identity(value).relative_path: value for value in encoded}
    closure = MemberHistoryClosure(
        MemberHistoryStore(lambda path: (objects[path],)),
        lambda journal_id, end: (frame.encoded for frame in corpus[journal_id].frames),
        member_role=COLLECTION_MEMBER_ROLE,
        catalog=ContractCatalog((collection_production_contract(),)),
    )
    return selected, closure, objects
